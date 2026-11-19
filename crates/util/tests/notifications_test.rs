//! Port of upstream/util/notifications/notifications_test.go.
use std::sync::mpsc::Receiver;
use std::thread::JoinHandle;
use std::time::Duration;
use util::notifications::{Notification, Notifications};

fn collect(sub: Receiver<Notification>) -> JoinHandle<Vec<Notification>> {
    std::thread::spawn(move || {
        let mut received = Vec::new();
        for n in sub {
            received.push(n);
        }
        received
    })
}

// TestNotificationLifecycle tests adding, modifying, and deleting notifications.
#[test]
fn test_notification_lifecycle() {
    let notifs = Notifications::new(10);

    // Add a notification.
    notifs.add_notification("Test Notification 1");

    // Check if the notification was added.
    let notifications = notifs.get();
    assert_eq!(notifications.len(), 1, "Expected 1 notification after addition.");
    assert_eq!("Test Notification 1", notifications[0].text, "Notification text mismatch.");
    assert!(notifications[0].active, "Expected notification to be active.");

    // Modify the notification.
    notifs.add_notification("Test Notification 1");
    let notifications = notifs.get();
    assert_eq!(notifications.len(), 1, "Expected 1 notification after modification.");

    // Delete the notification.
    notifs.delete_notification("Test Notification 1");
    let notifications = notifs.get();
    assert!(notifications.is_empty(), "Expected no notifications after deletion.");
}

// TestSubscriberReceivesNotifications tests that a subscriber receives notifications, including modifications and deletions.
#[test]
fn test_subscriber_receives_notifications() {
    let notifs = Notifications::new(10);

    // Subscribe to notifications.
    let sub = notifs.sub();
    assert!(sub.is_some());
    let (sub, unsubscribe) = sub.unwrap_or_else(|| unreachable!());

    // Thread to listen for notifications.
    let wg = collect(sub);

    // Add notifications.
    notifs.add_notification("Test Notification 1");
    notifs.add_notification("Test Notification 2");

    // Modify a notification.
    notifs.add_notification("Test Notification 1");

    // Delete a notification.
    notifs.delete_notification("Test Notification 2");

    // Wait for notifications to propagate.
    std::thread::sleep(Duration::from_millis(100));

    unsubscribe.unsubscribe();
    let received_notifications = wg.join().expect("join"); // Wait for the subscriber thread to finish.

    // Verify that we received the expected number of notifications.
    assert_eq!(received_notifications.len(), 4, "Expected 4 notifications (2 active, 1 modified, 1 deleted).");

    // Check the content and state of received notifications.
    let expected = [
        ("Test Notification 1", true),
        ("Test Notification 2", true),
        ("Test Notification 1", true),
        ("Test Notification 2", false),
    ];

    for (i, n) in received_notifications.iter().enumerate() {
        assert_eq!(expected[i].0, n.text, "Notification text mismatch at index {i}.");
        assert_eq!(expected[i].1, n.active, "Notification active state mismatch at index {i}.");
    }
}

// TestMultipleSubscribers tests that multiple subscribers receive notifications independently.
#[test]
fn test_multiple_subscribers() {
    let notifs = Notifications::new(10);

    // Subscribe two subscribers to notifications.
    let s1 = notifs.sub();
    assert!(s1.is_some());
    let (sub1, unsubscribe1) = s1.unwrap_or_else(|| unreachable!());

    let s2 = notifs.sub();
    assert!(s2.is_some());
    let (sub2, unsubscribe2) = s2.unwrap_or_else(|| unreachable!());

    let wg1 = collect(sub1);
    let wg2 = collect(sub2);

    // Add and delete notifications.
    notifs.add_notification("Test Notification 1");
    notifs.delete_notification("Test Notification 1");

    // Wait for notifications to propagate.
    std::thread::sleep(Duration::from_millis(100));

    // Unsubscribe both.
    unsubscribe1.unsubscribe();
    unsubscribe2.unsubscribe();

    let received_sub1 = wg1.join().expect("join");
    let received_sub2 = wg2.join().expect("join");

    // Both subscribers should have received the same 2 notifications.
    assert_eq!(received_sub1.len(), 2, "Expected 2 notifications for subscriber 1.");
    assert_eq!(received_sub2.len(), 2, "Expected 2 notifications for subscriber 2.");

    // Verify that both subscribers received the same notifications.
    for i in 0..2 {
        assert_eq!(received_sub1[i], received_sub2[i], "Subscriber notification mismatch at index {i}.");
    }
}

// TestUnsubscribe tests that unsubscribing prevents further notifications from being received.
#[test]
fn test_unsubscribe() {
    let notifs = Notifications::new(10);

    // Subscribe to notifications.
    let s = notifs.sub();
    assert!(s.is_some());
    let (sub, unsubscribe) = s.unwrap_or_else(|| unreachable!());

    let wg = collect(sub);

    // Add a notification and then unsubscribe.
    notifs.add_notification("Test Notification 1");
    std::thread::sleep(Duration::from_millis(100)); // Allow time for notification delivery.
    unsubscribe.unsubscribe(); // Unsubscribe.

    // Add another notification after unsubscribing.
    notifs.add_notification("Test Notification 2");

    // Wait for the subscriber thread to finish.
    let received_notifications = wg.join().expect("join");

    // Only the first notification should have been received.
    assert_eq!(received_notifications.len(), 1, "Expected 1 notification before unsubscribe.");
    assert_eq!("Test Notification 1", received_notifications[0].text, "Unexpected notification text.");
}

// TestMaxSubscribers tests that exceeding the max subscribers limit prevents additional subscriptions.
#[test]
fn test_max_subscribers() {
    let max_subscribers = 2;
    let notifs = Notifications::new(max_subscribers);

    // Subscribe the maximum number of subscribers.
    let s1 = notifs.sub();
    assert!(s1.is_some(), "Expected first subscription to succeed.");
    let (_r1, unsubscribe1) = s1.unwrap_or_else(|| unreachable!());

    let s2 = notifs.sub();
    assert!(s2.is_some(), "Expected second subscription to succeed.");
    let (_r2, unsubscribe2) = s2.unwrap_or_else(|| unreachable!());

    // Try to subscribe more than the max allowed.
    let s3 = notifs.sub();
    assert!(s3.is_none(), "Expected third subscription to fail due to max subscriber limit.");

    // Unsubscribe one subscriber and try again.
    unsubscribe1.unsubscribe();

    let s4 = notifs.sub();
    assert!(s4.is_some(), "Expected subscription to succeed after unsubscribing a subscriber.");
    let (_r4, unsubscribe4) = s4.unwrap_or_else(|| unreachable!());

    // Clean up the subscriptions.
    unsubscribe2.unsubscribe();
    unsubscribe4.unsubscribe();
}
