#!/usr/bin/env bash
# End-to-end comparison of the cloud SD providers: real Prometheus 3.16.0 and prometheus-rs (examples/sd_dump)
# discover from the same recorded API responses (mock.py); compare.py diffs the __meta_* and __address__ labels.
# Ports 9891-9895. State in bench/tmp/w2-sd-cloud/e2e. Everything started here is stopped on exit.
set -euo pipefail
R=$(cd "$(dirname "$0")/../.." && pwd)
E="$R/bench/sd-cloud-e2e"
S="$R/bench/tmp/w2-sd-cloud/e2e"
PROM=${PROM:-$R/upstream-bin/prometheus-3.16.0.linux-amd64/prometheus}
[ -x "$PROM" ] || PROM=$R/upstream-bin/prometheus-3.16.0.linux-amd64/prometheus
mkdir -p "$S"
find "$S" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
mkdir -p "$S/home" "$S/data"

# Test CA, a leaf for the proxied hosts, an RSA key for OCI and a service account for GCE.
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$S/ca.key" -out "$S/ca.pem" -days 2 -subj "/CN=sd-e2e-ca" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -keyout "$S/leaf.key" -out "$S/leaf.csr" -subj "/CN=sd-e2e" 2>/dev/null
printf 'subjectAltName=DNS:compute.googleapis.com,DNS:oauth2.googleapis.com,DNS:management.azure.com,DNS:login.microsoftonline.com,DNS:identity.us-ashburn-1.oci.oraclecloud.com,DNS:iaas.us-ashburn-1.oraclecloud.com\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n' > "$S/leaf.ext"
openssl x509 -req -in "$S/leaf.csr" -CA "$S/ca.pem" -CAkey "$S/ca.key" -CAcreateserial -out "$S/leaf.pem" -days 2 -extfile "$S/leaf.ext" 2>/dev/null
openssl genrsa -out "$S/oci_key.pem" 2048 2>/dev/null
python3 - "$S" <<'PY'
import json, sys
s = sys.argv[1]
key = open(f'{s}/oci_key.pem').read()
json.dump({'type': 'service_account', 'project_id': 'test-project', 'private_key_id': 'e2e', 'private_key': key,
           'client_email': 'e2e@test-project.iam.gserviceaccount.com', 'client_id': '1',
           'token_uri': 'https://oauth2.googleapis.com/token'}, open(f'{s}/sa.json', 'w'))
PY
sed "s#@STATE@#$S#g" "$E/jobs.yml" > "$S/jobs.yml"
{ echo "global: {scrape_interval: 1h, scrape_timeout: 10s}"; echo "scrape_configs:"; sed 's/^/  /' "$S/jobs.yml"; } > "$S/prometheus.yml"

(cd "$R" && ~/.cargo/bin/cargo build -q -p discovery --example sd_dump)

pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

python3 "$E/mock.py" "$S" 9895 9894 9893 9892 > "$S/mock.out" 2>&1 & pids+=($!)
for _ in $(seq 50); do grep -q ready "$S/mock.out" 2>/dev/null && break; sleep 0.1; done

# The same environment for both: an isolated HOME, the proxy for the TLS APIs, the test CA, credentials.
export HOME="$S/home" HTTPS_PROXY=http://127.0.0.1:9892 HTTP_PROXY= NO_PROXY=127.0.0.1,localhost SSL_CERT_FILE="$S/ca.pem" \
  GOOGLE_APPLICATION_CREDENTIALS="$S/sa.json" AWS_EC2_METADATA_DISABLED=true AWS_ENDPOINT_URL_EC2=http://127.0.0.1:9894 \
  AWS_ACCESS_KEY_ID= AWS_SECRET_ACCESS_KEY= AWS_PROFILE= AWS_REGION=

"$PROM" --config.file="$S/prometheus.yml" --storage.tsdb.path="$S/data" --web.listen-address=127.0.0.1:9891 > "$S/prometheus.log" 2>&1 & pids+=($!)
jobs_n=$(grep -c '^- job_name' "$S/jobs.yml")
for _ in $(seq 60); do
  sleep 1
  curl -sf 'http://127.0.0.1:9891/api/v1/targets?state=any' > "$S/prom-targets.json" || continue
  got=$(python3 -c "import json,sys;d=json.load(open('$S/prom-targets.json'))['data'];print(len({t['discoveredLabels']['job'] for t in d['activeTargets']+d.get('droppedTargets',[])}))")
  [ "$got" -ge "$jobs_n" ] && break
done
sleep 3
curl -sf 'http://127.0.0.1:9891/api/v1/targets?state=any' > "$S/prom-targets.json"

"$R/target/debug/examples/sd_dump" "$S/jobs.yml" 20 > "$S/rs-targets.json" 2> "$S/rs.log"

python3 "$E/compare.py" "$S/prom-targets.json" "$S/rs-targets.json" "$S/jobs.yml"
