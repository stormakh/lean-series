#!/bin/bash
# differential check of HTTPClientConfig decode + marshal: Go (prometheus/common) vs Rust (crates/net)
cd "$(dirname "$0")"
python3 mkhin.py
cp /home/kamrot/code/prometheus-rs/tools/gopath/pkg/mod/github.com/prometheus/common@v0.72.1/config/testdata/http.conf.*.y*ml hin/
chmod u+w hin/*
~/.cargo/bin/cargo build -q --manifest-path rcmp/Cargo.toml 2>&1 | grep -E "^error" -A5 | head -30
./gocmp/gocmp http hin/* > go_http.txt 2>&1
./rcmp/target/debug/rcmp http hin/* > rs_http.txt 2>&1
diff go_http.txt rs_http.txt && echo SAME
wc -l go_http.txt
