//! Port of upstream/util/runtime/statfs_unix_test.go.
use util::runtime::{fs_size, fs_type};

#[test]
fn test_fs_type() {
    let path = std::env::current_dir().expect("getwd");
    let path = path.to_string_lossy();

    // A real path must yield a non-zero filesystem type.
    let fs_type_v = fs_type(&path);
    assert_ne!("0", fs_type_v);

    let fs_type_v = fs_type("/no/where/to/be/found");
    assert_eq!("0", fs_type_v);

    let fs_type_v = fs_type("  %% not event a real path\n\n");
    assert_eq!("0", fs_type_v);
}

#[test]
fn test_fs_size() {
    let path = std::env::current_dir().expect("getwd");
    let path = path.to_string_lossy();

    let size = fs_size(&path);
    assert!(size > 0);

    let size = fs_size("/no/where/to/be/found");
    assert_eq!(0u64, size);

    let size = fs_size("  %% not event a real path\n\n");
    assert_eq!(0u64, size);
}
