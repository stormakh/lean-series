//! Times requests of a URL with the scrape HTTP client (diagnostics).
fn main() {
    let url = std::env::args().nth(1).expect("url");
    let c = net::new_client_from_config(&net::HttpClientConfig::default_config(), &net::HttpClientOptions::default()).unwrap();
    for _ in 0..5 {
        let t = std::time::Instant::now();
        let mut r = net::Request::get(&url).unwrap();
        r.header.add("Accept-Encoding", "gzip");
        r.timeout = Some(std::time::Duration::from_secs(4));
        let resp = c.do_request(r).unwrap();
        println!("{} bytes in {:?}", resp.body.len(), t.elapsed());
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}
