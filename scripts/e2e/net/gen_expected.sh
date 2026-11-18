#!/bin/bash
# regenerates crates/net/tests/testdata/go_compat/expected.txt with Go (prometheus/common LoadHTTPConfig + String)
cd "$(dirname "$0")/../../../crates/net/tests/testdata/go_compat"
../../../../../bench/tmp/w0-net/gocmp/gocmp http *.yml | sed 's#^=== #=== #' > expected.txt
wc -l expected.txt
