//! Port of `promql/matrix_budget_test.go`.

use chunkenc::ValueType;
use model::labels::Labels;
use promql::{PromqlError, QueryBudget, with_test_evaluator};
use storage::chunks::generate_samples;
use storage::{Context, new_list_series};

#[test]
fn test_matrix_iterator_check_context_cancellation_accounting() {
    for quota in [0i64, 30] {
        for billed in [0usize, 7, 20] {
            for step in [0isize, -1] {
                let name = format!("quota={}/billed={}/step={}", quota, billed, step);
                let (ctx, cancel) = Context::background().with_cancel();
                let budget = QueryBudget { max_samples_read: quota, ..Default::default() };
                let series = new_list_series(Labels::empty(), generate_samples(0, 21));
                with_test_evaluator(&ctx, Some(&budget), |ev| {
                    ev.consume_matrix_samples(10, 0).unwrap(); // A previous selector's cost.
                    let mut it = ev.new_matrix_iterator(30);
                    it.reset(series.iterator(None));
                    it.begin_frame(step);
                    ev.guard_check(&it).unwrap();
                    assert_eq!(ValueType::Float, it.seek(-1, -1, 20), "{}", name);
                    // Charge actual buffered consumption, then interrupt the drain.
                    {
                        let mut buf = it.buffer();
                        for _ in 0..billed {
                            assert_eq!(ValueType::Float, buf.next(), "{}", name);
                            let _ = buf.at();
                            ev.consume_matrix_samples(1, step).unwrap();
                        }
                    }
                    cancel.cancel();
                    let err = ev.guard_check(&it).unwrap_err();
                    assert_eq!(
                        Some(&PromqlError::QueryCanceled("query execution".to_string())),
                        PromqlError::find(&err),
                        "{}",
                        name
                    );
                    let want = if step < 0 { 10 } else { 30 };
                    assert_eq!(want, budget.samples_read.get(), "{}: commit only unbilled storage input", name);
                    assert_eq!(want, ev.samples_stats().samples_read, "{}", name);
                    assert_eq!(vec![want], ev.samples_stats().samples_read_per_step, "{}", name);
                });
            }
        }
    }
}
