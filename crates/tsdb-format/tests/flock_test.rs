//! Port of tsdb/fileutil/flock_test.go.

use std::fs;
use std::path::PathBuf;

use tsdb_format::fileutil::flock;

fn temp_dir(name: &str) -> PathBuf {
    let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("flock_test-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn test_locking() {
    let dir = temp_dir("test_flock");
    let file_name = dir.join("LOCK");

    assert!(
        fs::metadata(&file_name).is_err(),
        "File {file_name:?} unexpectedly exists."
    );

    let (lock, existed) = flock(&file_name);
    let mut lock = lock.unwrap_or_else(|e| panic!("Error locking file {file_name:?}: {e}"));
    assert!(
        !existed,
        "File {file_name:?} reported as existing during locking."
    );

    // File must now exist.
    fs::metadata(&file_name).expect("Could not stat file expected to exist");

    // Try to lock again.
    let (locked_again, existed) = flock(&file_name);
    assert!(locked_again.is_err(), "File {file_name:?} locked twice.");
    // Go: require.Nil(lockedAgain): the error result carries no releaser.
    assert!(existed, "Existing file {file_name:?} not recognized.");

    lock.release().expect("Error releasing lock");

    // File must still exist.
    fs::metadata(&file_name).expect("Could not stat file expected to exist");

    // Lock existing file.
    let (lock, existed) = flock(&file_name);
    let mut lock = lock.expect("Error locking file");
    assert!(existed, "Existing file {file_name:?} not recognized.");

    lock.release().expect("Error releasing lock");
    fs::remove_dir_all(&dir).unwrap();
}
