//! Port of discovery/refresh/refresh_test.go.
use std::sync::Arc;
use std::time::Duration;

use discovery::refresh::{Discovery, Options};
use discovery::{CancelToken, DiscovererMetrics, Discoverer, Group, Recv, Registry};
use model::labelset::LabelSet;

#[test]
fn test_refresh() {
    let tg1 = vec![Arc::new(Group {
        source: "tg".to_string(),
        targets: vec![LabelSet::from_pairs(&[("t1", "v1")]), LabelSet::from_pairs(&[("t2", "v2")])],
        labels: LabelSet::from_pairs(&[("l1", "lv1")]),
    })];
    let tg2 = vec![Arc::new(Group { source: "tg".to_string(), ..Default::default() })];

    let mut i = 0;
    let (t1, t2) = (tg1.clone(), tg2.clone());
    let refresh = move |_: &CancelToken| -> Result<Vec<Arc<Group>>, String> {
        i += 1;
        match i {
            1 => Ok(t1.clone()),
            2 => Ok(t2.clone()),
            _ => Err("some error".to_string()),
        }
    };
    let interval = Duration::from_millis(1);

    let metrics = discovery::new_refresh_metrics(&Registry::new());
    metrics.register().unwrap();

    let mut d = Discovery::new(Options {
        logger: discovery::Logger::nop(),
        mech: "test",
        set_name: "test-refresh",
        interval,
        refresh_f: refresh,
        metrics_instantiator: metrics.as_ref(),
    });

    let (tx, rx) = discovery::channel();
    let ctx = CancelToken::new();
    let c2 = ctx.clone();
    let h = std::thread::spawn(move || d.run(&c2, tx));

    match rx.recv() {
        Recv::Update(tg) => assert_eq!(tg1, tg),
        other => panic!("unexpected {other:?}"),
    }
    match rx.recv() {
        Recv::Update(tg) => assert_eq!(tg2, tg),
        other => panic!("unexpected {other:?}"),
    }
    match rx.recv_timeout(2 * interval) {
        Recv::Timeout => {}
        Recv::Update(_) => panic!("Unexpected target group"),
        other => panic!("unexpected {other:?}"),
    }
    // adapted: Go's goleak check at exit; here the discoverer thread must stop when cancelled.
    ctx.cancel();
    h.join().unwrap();
    metrics.unregister();
}
