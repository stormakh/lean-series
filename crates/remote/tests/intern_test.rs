//! Port of `storage/remote/intern_test.go`.

use std::sync::Arc;

use remote::intern::Pool;

#[test]
fn test_intern() {
    let interner = Pool::new();
    let test_string = "TestIntern";
    interner.intern(test_string);
    let refs = interner.refs(test_string);
    assert!(refs.is_some());
    assert_eq!(Some(1), refs, "expected refs to be 1 but it was {refs:?}");
}

#[test]
fn test_intern_multi_ref() {
    let interner = Pool::new();
    let test_string = "TestIntern_MultiRef";

    interner.intern(test_string);
    let refs = interner.refs(test_string);
    assert!(refs.is_some());
    assert_eq!(Some(1), refs, "expected refs to be 1 but it was {refs:?}");

    interner.intern(test_string);
    let refs = interner.refs(test_string);
    assert!(refs.is_some());
    assert_eq!(Some(2), refs, "expected refs to be 2 but it was {refs:?}");
}

#[test]
fn test_intern_delete_ref() {
    let interner = Pool::new();
    let test_string = "TestIntern_DeleteRef";

    interner.intern(test_string);
    let refs = interner.refs(test_string);
    assert!(refs.is_some());
    assert_eq!(Some(1), refs, "expected refs to be 1 but it was {refs:?}");

    interner.release(test_string);
    assert!(interner.refs(test_string).is_none());
}

#[test]
fn test_intern_multi_ref_concurrent() {
    let interner = Arc::new(Pool::new());
    let test_string = "TestIntern_MultiRef_Concurrent";

    interner.intern(test_string);
    let refs = interner.refs(test_string);
    assert!(refs.is_some());
    assert_eq!(Some(1), refs, "wrong interned refs count");

    for _ in 0..1000 {
        let i = interner.clone();
        let released = std::thread::spawn(move || i.release(test_string));
        interner.intern(test_string);
        released.join().unwrap();
    }

    let refs = interner.refs(test_string);
    assert!(refs.is_some());
    assert_eq!(Some(1), refs, "wrong interned refs count");
}
