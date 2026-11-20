//! Links the functions that the server runs after startup next to each other (`hot-symbols.txt`, made by
//! `bench/hot-code.sh`). The kernel maps code in 64 KB blocks, so packed hot code keeps the mapped (PSS) part of the
//! 13 MB of code small. Symbols that no longer exist are ignored. Needs LLD (the default linker of Rust on Linux).

fn main() {
    let dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
    let file = format!("{dir}/hot-symbols.txt");
    println!("cargo:rerun-if-changed={file}");
    // A fixed load address: no run-time relocations, so the 500 KB of pointers in .data.rel.ro (vtables, string
    // tables) stay clean file pages instead of private dirty copies, and .rela.dyn (600 KB) is not read at startup.
    println!("cargo:rustc-link-arg-bin=prometheus=-no-pie");
    if std::path::Path::new(&file).exists() {
        println!("cargo:rustc-link-arg-bin=prometheus=-Wl,--symbol-ordering-file={file}");
        println!("cargo:rustc-link-arg-bin=prometheus=-Wl,--no-warn-symbol-ordering");
    }
}
