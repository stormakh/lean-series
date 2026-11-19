//! Prints the errors of `rulefmt::parse_file` for each file argument (as the Go check program does).
fn main() {
    let p = promql::parser::Parser::new(promql::parser::Options::default());
    for f in std::env::args().skip(1) {
        let (_, errs) = rules::rulefmt::parse_file(&f, false, model::labels::ValidationScheme::Utf8, &p, &rules::log::Logger::nop());
        println!("== {}", std::path::Path::new(&f).file_name().unwrap().to_string_lossy());
        for e in errs {
            println!("{e}");
        }
    }
}
