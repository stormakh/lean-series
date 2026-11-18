mod e2e;

fn main() {
    if std::env::args().nth(1).as_deref() == Some("e2e") {
        std::process::exit(if e2e::run() { 0 } else { 1 });
    }
    let args: Vec<String> = std::env::args().collect();
    let mode = args[1].as_str();
    for f in &args[2..] {
        let b = std::fs::read_to_string(f).unwrap();
        println!("=== {f}");
        match mode {
            "any" | "anystrict" => {
                let r: Result<yaml::Value, _> = if mode == "any" { yaml::unmarshal(&b) } else { yaml::unmarshal_strict(&b) };
                match r {
                    Err(e) => println!("ERR: {e}"),
                    Ok(v) => match yaml::marshal(&v) {
                        Ok(s) => print!("{s}"),
                        Err(e) => println!("MERR: {e}"),
                    },
                }
            }
            "http" => match net::load_http_config(&b) {
                Err(e) => println!("ERR: {e}"),
                Ok(c) => print!("{}", c.to_yaml_string()),
            },
            _ => {}
        }
    }
}
