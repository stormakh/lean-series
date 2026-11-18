#!/bin/bash
# starts the Go test servers (ports 9600-9604), runs the Rust e2e checks, stops the servers
cd "$(dirname "$0")"
TD=/home/kamrot/code/prometheus-rs/tools/gopath/pkg/mod/github.com/prometheus/common@v0.72.1/config/testdata
./e2esrv/e2esrv "$TD" > e2esrv.log 2>&1 &
echo $! > e2esrv.pid
sleep 1
ss -ltn | grep -cE "127.0.0.1:960[0-4]"
./rcmp/target/debug/rcmp e2e
rc=$?
kill "$(cat e2esrv.pid)"
wait 2>/dev/null
rm -f e2esrv.pid
ss -ltn | grep -cE "127.0.0.1:960[0-4]"
exit $rc
