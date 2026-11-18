//! Prints the Rust view of config files in the format of scripts/e2e/config/gocfg (Go), for comparison:
//! `cfgdump load|agent|scrape <files>`.
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args[1].as_str();
    let logger = discovery::Logger::nop();
    for f in &args[2..] {
        println!("=== {f}");
        let c = match config::load_file(f, mode == "agent", &logger) {
            Ok(c) => c,
            Err(e) => {
                println!("ERR: {e}");
                continue;
            }
        };
        match mode {
            "scrape" => match c.get_scrape_configs() {
                Ok(scs) => print!("{}", yaml::marshal(&scs).unwrap_or_else(|e| format!("MERR: {e}\n"))),
                Err(e) => println!("ERR: {e}"),
            },
            _ => print!("{}", c.to_yaml_string()),
        }
    }
}
