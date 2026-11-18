//! Quick checks of the scrape path that are not upstream tests.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use model::labels::from_strings;
use scrape::testing::new_test_scrape_loop;
use util::teststorage::{Appendable, Sample, require_equal};

#[test]
fn smoke_append_text_v1_v2() {
    for v2 in [false, true] {
        let app = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| {
            if v2 {
                sl.appendable_v2 = Some(Arc::new(app.clone()));
            } else {
                sl.appendable = Some(Arc::new(app.clone()));
            }
            sl.append_metadata_to_wal = false;
        });
        let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let mut a = sl.appender();
        let (stats, err) = a.append(b"metric_a{foo=\"1\"} 1\nmetric_b 2\n", "text/plain", ts);
        assert!(err.is_none(), "{err:?}");
        assert!(a.commit().is_none());
        assert_eq!(stats.total, 2);
        assert_eq!(stats.added, 2);
        assert_eq!(stats.series_added, 2);
        require_equal(
            &[
                Sample { l: from_strings(&["__name__", "metric_a", "foo", "1"]), t: 1_000_000, v: 1.0, ..Default::default() },
                Sample { l: from_strings(&["__name__", "metric_b"]), t: 1_000_000, v: 2.0, ..Default::default() },
            ],
            &app.result_samples(),
        );
        // Second scrape: metric_b disappears and gets a stale marker.
        let mut a = sl.appender();
        let (stats, err) = a.append(b"metric_a{foo=\"1\"} 3\n", "text/plain", ts + Duration::from_secs(1));
        assert!(err.is_none());
        assert!(a.commit().is_none());
        assert_eq!(stats.series_added, 0);
        let got = app.result_samples();
        assert_eq!(got.len(), 4);
        assert!(got[3].is_stale());
        assert_eq!(got[3].l, from_strings(&["__name__", "metric_b"]));
    }
}

#[test]
fn smoke_run_loop() {
    let app = Appendable::new();
    let (sl, scraper) = new_test_scrape_loop(|sl| {
        sl.appendable = Some(Arc::new(app.clone()));
        sl.fallback_scrape_protocol = "text/plain".to_string();
    });
    scraper.set_scrape_func(|_, w| {
        w.extend_from_slice(b"metric_a 1\n");
        Ok(())
    });
    let sl = Arc::new(sl);
    let s2 = sl.clone();
    let h = std::thread::spawn(move || s2.run(None));
    std::thread::sleep(Duration::from_millis(55));
    sl.stop();
    sl.parent_ctx.cancel();
    h.join().unwrap();
    let got = app.result_samples();
    let n = got.iter().filter(|s| s.l.get("__name__") == "metric_a").count();
    assert!(n >= 4 && n <= 7, "scrapes: {n}");
    let ups = got.iter().filter(|s| s.l.get("__name__") == "up").count();
    assert_eq!(n, ups);
}
