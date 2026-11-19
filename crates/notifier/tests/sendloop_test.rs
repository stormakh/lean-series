// Port of notifier/sendloop_test.go.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use common::{alert, alerts_equal};
use config::default_alertmanager_config;
use discovery::metrics::Registry;
use discovery::Logger;
use notifier::{AlertMetrics, Context, Options, SendLoop, DEFAULT_MAX_BATCH_SIZE};

fn metrics() -> Arc<AlertMetrics> {
    Arc::new(AlertMetrics::new(Some(&Registry::new()), Box::new(|| 0.0)))
}

fn same(a: &[Arc<notifier::Alert>], b: &[&Arc<notifier::Alert>]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| Arc::ptr_eq(x, y))
}

#[test]
fn test_custom_do() {
    const TEST_URL: &str = "http://testurl.com/";
    const TEST_BODY: &str = "testbody";

    let received = Arc::new(AtomicBool::new(false));
    let r = received.clone();
    let opts = Options {
        do_fn: Some(Arc::new(move |_ctx: &Context, _c: Option<&net::Client>, req: net::Request| {
            r.store(true, Ordering::SeqCst);
            assert_eq!(TEST_BODY, String::from_utf8(req.body.clone()).unwrap());
            assert_eq!(TEST_URL, req.url.to_url_string());
            Ok(net::Response { status: 0, status_text: String::new(), header: net::Header::new(), body: Vec::new(), request: req })
        })),
        ..Default::default()
    };
    let h = SendLoop::new("", None, None, Arc::new(opts), Logger::nop(), metrics());

    let _ = h.send_one(&Context::background(), None, TEST_URL, TEST_BODY.as_bytes().to_vec());

    assert!(received.load(Ordering::SeqCst));
}

#[test]
fn test_handler_next_batch() {
    let opts = Options { max_batch_size: DEFAULT_MAX_BATCH_SIZE, ..Default::default() };
    let send_loop = SendLoop::new("http://mock", None, Some(Arc::new(default_alertmanager_config())), Arc::new(opts), Logger::nop(), metrics());

    for i in 0..2 * DEFAULT_MAX_BATCH_SIZE + 1 {
        send_loop.queue_mut().push(Arc::new(alert(&["alertname", &i.to_string()])));
    }
    let expected = send_loop.queue();

    alerts_equal(&expected[0..DEFAULT_MAX_BATCH_SIZE], &send_loop.next_batch()).unwrap();
    alerts_equal(&expected[DEFAULT_MAX_BATCH_SIZE..2 * DEFAULT_MAX_BATCH_SIZE], &send_loop.next_batch()).unwrap();
    alerts_equal(&expected[2 * DEFAULT_MAX_BATCH_SIZE..], &send_loop.next_batch()).unwrap();
    assert!(send_loop.queue().is_empty());
}

#[test]
fn test_add_alerts_to_queue() {
    let alert1 = Arc::new(alert(&["alertname", "existing1"]));
    let alert2 = Arc::new(alert(&["alertname", "existing2"]));

    let s = SendLoop::new("http://foo.bar/", None, None, Arc::new(Options { queue_capacity: 5, ..Default::default() }), Logger::nop(), metrics());
    s.add(&[alert1.clone(), alert2.clone()]);
    assert!(same(&s.queue(), &[&alert1, &alert2]));
    assert_eq!(s.queue().len(), 2);

    let alert3 = Arc::new(alert(&["alertname", "new1"]));
    let alert4 = Arc::new(alert(&["alertname", "new2"]));

    // Add new alerts to the queue, expect 0 dropped.
    s.add(&[alert3.clone(), alert4.clone()]);
    assert_eq!(0.0, s.metrics.dropped.with_label_values(&[&s.alertmanager_url]).get());

    // Verify all new alerts were added to the queue.
    assert!(same(&s.queue(), &[&alert1, &alert2, &alert3, &alert4]));
    assert_eq!(s.queue().len(), 4);
}

#[test]
fn test_add_alerts_to_queue_exceeding_capacity() {
    let alert1 = Arc::new(alert(&["alertname", "alert1"]));
    let alert2 = Arc::new(alert(&["alertname", "alert2"]));

    let s = SendLoop::new("http://foo.bar/", None, None, Arc::new(Options { queue_capacity: 3, ..Default::default() }), Logger::nop(), metrics());
    s.add(&[alert1, alert2.clone()]);

    let alert3 = Arc::new(alert(&["alertname", "alert3"]));
    let alert4 = Arc::new(alert(&["alertname", "alert4"]));

    // Add new alerts to the queue, expect 1 dropped.
    s.add(&[alert3.clone(), alert4.clone()]);
    assert_eq!(1.0, s.metrics.dropped.with_label_values(&[&s.alertmanager_url]).get());

    // Verify all new alerts were added to the queue.
    assert!(same(&s.queue(), &[&alert2, &alert3, &alert4]));
}

#[test]
fn test_add_alerts_to_queue_exceeding_total_capacity() {
    let alert1 = Arc::new(alert(&["alertname", "alert1"]));
    let alert2 = Arc::new(alert(&["alertname", "alert2"]));

    let s = SendLoop::new("http://foo.bar/", None, None, Arc::new(Options { queue_capacity: 3, ..Default::default() }), Logger::nop(), metrics());
    s.add(&[alert1, alert2]);

    let alert3 = Arc::new(alert(&["alertname", "alert3"]));
    let alert4 = Arc::new(alert(&["alertname", "alert4"]));
    let alert5 = Arc::new(alert(&["alertname", "alert5"]));
    let alert6 = Arc::new(alert(&["alertname", "alert6"]));

    // Add new alerts to the queue, expect 3 dropped: 1 from the new batch + 2 from existing queued items.
    s.add(&[alert3, alert4.clone(), alert5.clone(), alert6.clone()]);
    assert_eq!(3.0, s.metrics.dropped.with_label_values(&[&s.alertmanager_url]).get());

    // Verify all new alerts were added to the queue.
    assert!(same(&s.queue(), &[&alert4, &alert5, &alert6]));
}

#[test]
fn test_next_batch_alerts_from_queue() {
    let s = SendLoop::new("http://foo.bar/", None, None, Arc::new(Options { queue_capacity: 5, max_batch_size: 3, ..Default::default() }), Logger::nop(), metrics());

    let alert1 = Arc::new(alert(&["alertname", "alert1"]));
    let alert2 = Arc::new(alert(&["alertname", "alert2"]));
    let alert3 = Arc::new(alert(&["alertname", "alert3"]));
    s.add(&[alert1.clone(), alert2.clone(), alert3.clone()]);

    // Test batch-size alerts in the queue.
    assert!(same(&s.next_batch(), &[&alert1, &alert2, &alert3]));
    assert!(s.next_batch().is_empty());

    // Test full queue.
    let alert4 = Arc::new(alert(&["alertname", "alert4"]));
    let alert5 = Arc::new(alert(&["alertname", "alert5"]));
    s.add(&[alert1.clone(), alert2.clone(), alert3.clone(), alert4.clone(), alert5.clone()]);
    assert!(same(&s.next_batch(), &[&alert1, &alert2, &alert3]));
    assert!(same(&s.next_batch(), &[&alert4, &alert5]));
    assert!(s.next_batch().is_empty());
}

#[test]
fn test_metrics() {
    const URL: &str = "http://alertmanager:9093";

    // Use a single registry throughout the test - this is critical to catch registry conflicts.
    let reg = Registry::new();
    let metrics = Arc::new(AlertMetrics::new(Some(&reg), Box::new(|| 0.0)));
    let opts = Arc::new(Options { queue_capacity: 10, max_batch_size: DEFAULT_MAX_BATCH_SIZE, ..Default::default() });
    let cfg = Some(Arc::new(default_alertmanager_config()));

    // Create the first send loop - this initializes metrics with the alertmanager URL label.
    let send_loop1 = SendLoop::new(URL, None, cfg.clone(), opts.clone(), Logger::nop(), metrics.clone());

    // Verify metrics are initialized.
    assert_eq!(0.0, metrics.dropped.with_label_values(&[URL]).get());
    assert_eq!(0.0, metrics.sent.with_label_values(&[URL]).get());

    // Stop the send loop - this should clean up all metrics.
    send_loop1.stop();
    // adapted: Go relies on a registry conflict panic to notice leftover series; check the series are gone.
    assert_eq!(0, metrics.dropped.len());
    assert_eq!(0, metrics.sent.len());

    // Create a second send loop with the same URL - this should NOT panic or conflict because metrics were
    // properly cleaned up.
    let send_loop2 = SendLoop::new(URL, None, cfg, opts, Logger::nop(), metrics.clone());

    // Verify metrics are re-initialized correctly.
    assert_eq!(0.0, metrics.dropped.with_label_values(&[URL]).get());
    assert_eq!(0.0, metrics.sent.with_label_values(&[URL]).get());
    send_loop2.stop();
}
