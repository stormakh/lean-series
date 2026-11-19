//! Port of `tsdb/query_cost_test.go`.

mod common;

#[test]
fn test_db_cost_sampling_queryable() {
    {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use common::*;
        use model::labels;
        use model::matcher::{MatchType, Matcher};
        use storage::{Context, Error, LabelQuerier};
        use tsdb::db::default_options as DefaultOptions;

        // Layouts.
        struct Case {
            name: &'static str,
            blocks: Vec<[i64; 2]>,
            head: bool,
            ooo: bool,
            unsupported: bool,
        }
        let c = |name, blocks: Vec<[i64; 2]>, head, ooo, unsupported| Case { name, blocks, head, ooo, unsupported };
        for tc in [
            c("empty", vec![], false, false, false),
            c("in-order head", vec![], true, false, false),
            c("single block", vec![[0, 100]], false, false, false),
            c("adjacent blocks", vec![[0, 50], [50, 100]], false, false, true),
            c("overlapping blocks", vec![[0, 80], [20, 100]], false, false, true),
            c("head and block", vec![[0, 50]], true, false, true),
            c("OOO head", vec![], true, true, true),
        ] {
            let dir = TempDir::new();
            for w in &tc.blocks {
                create_block(dir.path(), &gen_series(1, 1, w[0], w[1]));
            }
            let mut opts = DefaultOptions();
            opts.out_of_order_time_window = 1000;
            let db = new_test_db(vec![with_dir(dir.path()), with_opts(opts)]);
            db.disable_compactions();
            if tc.head {
                let mut app = db.appender(&Context::background());
                app.append(0, &labels::from_strings(&[DEFAULT_LABEL_NAME, "0"]), 80, 1.0).unwrap();
                if tc.ooo {
                    app.append(0, &labels::from_strings(&[DEFAULT_LABEL_NAME, "0"]), 40, 2.0).unwrap();
                }
                assert!(app.commit().is_none());
            }
            let (source, supported) = db.cost_sampling_queryable();
            assert!(supported, "{}", tc.name);
            for chunk_query in [false, true] {
                let res: Result<Box<dyn LabelQuerier>, Error> = if chunk_query {
                    source.chunk_querier(25, 90).map(|q| q as Box<dyn LabelQuerier>)
                } else {
                    source.querier(25, 90).map(|q| q as Box<dyn LabelQuerier>)
                };
                if tc.unsupported {
                    assert!(res.err().unwrap().is(&storage::ERR_COST_SAMPLING_UNSUPPORTED), "{}", tc.name);
                } else {
                    assert!(res.unwrap().close().is_none(), "{}", tc.name);
                }
            }
        }

        // Layout changes.
        {
            let mut opts = DefaultOptions();
            opts.out_of_order_time_window = 1000;
            let db = new_test_db(vec![with_opts(opts)]);
            let metric = labels::from_strings(&[DEFAULT_LABEL_NAME, "0"]);
            let mut app = db.appender(&Context::background());
            app.append(0, &metric, 20, 1.0).unwrap();
            app.append(0, &metric, 80, 2.0).unwrap();
            assert!(app.commit().is_none());
            let (source, supported) = db.cost_sampling_queryable();
            assert!(supported);
            let mut pinned = source.chunk_querier(25, 75).unwrap();

            let mut app = db.appender(&Context::background());
            app.append(0, &metric, 40, 3.0).unwrap();
            assert!(app.commit().is_none());
            // The already-pinned reader retains its in-order path; later openings must reject the newly
            // introduced OOO iterable before reading any chunks.
            {
                let mut set = pinned.select(
                    &Context::background(),
                    false,
                    None,
                    &[Matcher::must_new(MatchType::Equal, DEFAULT_LABEL_NAME, "0")],
                );
                while set.next() {
                    let s = set.at().clone();
                    let mut it = s.iterator(None);
                    while it.next() {}
                    assert!(it.err().is_none());
                }
                assert!(set.err().is_none());
            }
            let q = source.chunk_querier(25, 75);
            assert!(q.err().unwrap().is(&storage::ERR_COST_SAMPLING_UNSUPPORTED));
            let plain = source.querier(25, 75);
            assert!(plain.err().unwrap().is(&storage::ERR_COST_SAMPLING_UNSUPPORTED));
            assert!(pinned.close().is_none());
        }

        // Reader cleanup.
        for failure in ["unsupported", "close failure", "opening failure"] {
            for chunk_query in [false, true] {
                let dir = TempDir::new();
                create_block(dir.path(), &gen_series(1, 1, 0, 50));
                create_block(dir.path(), &gen_series(1, 1, 50, 100));
                let db = new_test_db(vec![with_dir(dir.path()), with_opts(DefaultOptions())]);
                db.disable_compactions();
                let opens = Arc::new(AtomicUsize::new(0));
                let closes = Arc::new(AtomicUsize::new(0));
                let want = Error::msg("reader failure");
                {
                    let (opens, closes, want) = (opens.clone(), closes.clone(), want.clone());
                    db.set_block_chunk_querier_func(Box::new(move |b, mint, maxt| {
                        let n = opens.fetch_add(1, Ordering::SeqCst) + 1;
                        if failure == "opening failure" && n == 2 {
                            return Err(want.clone());
                        }
                        let q = tsdb::querier::new_block_chunk_querier(b, mint, maxt)?;
                        let close_err = if failure == "close failure" { Some(want.clone()) } else { None };
                        Ok(Box::new(CostSamplingCloseQuerier { q, closes: closes.clone(), close_err }))
                    }));
                }
                let (source, _) = db.cost_sampling_queryable();
                let res: Result<Box<dyn LabelQuerier>, Error> = if chunk_query {
                    source.chunk_querier(25, 75).map(|q| q as Box<dyn LabelQuerier>)
                } else {
                    source.querier(25, 75).map(|q| q as Box<dyn LabelQuerier>)
                };
                let err = res.err().expect("an error");
                if failure == "unsupported" {
                    assert!(err.is(&storage::ERR_COST_SAMPLING_UNSUPPORTED));
                    assert_eq!(2, closes.load(Ordering::SeqCst));
                } else {
                    assert!(err.is(&want));
                    assert!(
                        !err.is(&storage::ERR_COST_SAMPLING_UNSUPPORTED),
                        "reader failures must never trigger advisory fallback"
                    );
                    let exp = if failure == "opening failure" { 1 } else { 2 };
                    assert_eq!(exp, closes.load(Ordering::SeqCst));
                }
                assert_eq!(2, opens.load(Ordering::SeqCst));
            }
        }

        struct CostSamplingCloseQuerier {
            q: storage::BoxChunkQuerier,
            closes: Arc<AtomicUsize>,
            close_err: Option<Error>,
        }
        impl LabelQuerier for CostSamplingCloseQuerier {
            fn label_values(
                &self,
                ctx: &Context,
                name: &str,
                hints: Option<&storage::LabelHints>,
                ms: &[Matcher],
            ) -> storage::LabelResult {
                self.q.label_values(ctx, name, hints, ms)
            }
            fn label_names(&self, ctx: &Context, hints: Option<&storage::LabelHints>, ms: &[Matcher]) -> storage::LabelResult {
                self.q.label_names(ctx, hints, ms)
            }
            fn close(&mut self) -> Option<Error> {
                self.closes.fetch_add(1, Ordering::SeqCst);
                Error::join([self.q.close(), self.close_err.clone()])
            }
        }
        impl storage::ChunkQuerier for CostSamplingCloseQuerier {
            fn select(
                &self,
                ctx: &Context,
                sort: bool,
                hints: Option<&storage::SelectHints>,
                ms: &[Matcher],
            ) -> storage::BoxChunkSeriesSet {
                self.q.select(ctx, sort, hints, ms)
            }
        }
    }
}
