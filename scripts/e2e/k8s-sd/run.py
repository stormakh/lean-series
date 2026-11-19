#!/usr/bin/env python3
"""End-to-end check of Kubernetes SD: the Rust port (example k8s_sd_dump) against Prometheus 3.16.0 on the same
fake API server (fake_k8s_api.py).

Phase A (initial lists and watch-list streams, no changes): every target group per source, with target order,
from `promtool check service-discovery` vs the Rust discoverer.
Phase B (watch updates): the fake server changes objects after 6 s; the final targets of a running Prometheus
server (/api/v1/targets?state=any, discoveredLabels) vs the final groups of the Rust discoverer.

usage: run.py   (from the repository root; uses ports 9800 and 9801 and bench/tmp/w2-sd-k8s)
"""
import json, os, subprocess, sys, time, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
HERE = os.path.dirname(os.path.abspath(__file__))
TMP = os.path.join(ROOT, 'bench/tmp/w2-sd-k8s')
PROM = os.path.join(ROOT, 'upstream-bin/prometheus-3.16.0.linux-amd64')
EXTRA = {'job', '__scheme__', '__metrics_path__', '__scrape_interval__', '__scrape_timeout__'}
os.makedirs(TMP, exist_ok=True)

sd_text = open(os.path.join(HERE, 'sd.yml')).read().replace('KUBECONFIG_PATH', os.path.join(HERE, 'kubeconfig.yml'))
sd_path = os.path.join(TMP, 'sd.yml')
open(sd_path, 'w').write(sd_text)
# split the YAML list into entries (each starts with "- role:")
entries = []
for line in sd_text.splitlines():
    if line.startswith('#'):
        continue
    if line.startswith('- '):
        entries.append([])
    if entries:
        entries[-1].append(line)
roles = [e[0].split(':', 1)[1].strip() for e in entries]
cfg = ['global:', '  scrape_interval: 1h', 'scrape_configs:']
for i, e in enumerate(entries):
    cfg += [f'  - job_name: k{i}', '    relabel_configs:', '      - action: labelmap', '        regex: __meta_kubernetes_(.+)', '    kubernetes_sd_configs:']
    cfg += ['      ' + l for l in e]
prom_cfg = os.path.join(TMP, 'prometheus.yml')
open(prom_cfg, 'w').write('\n'.join(cfg) + '\n')


def source_key(role, ls):
    ns = ls.get('__meta_kubernetes_namespace', '')
    name = {'node': 'node_name', 'pod': 'pod_name', 'service': 'service_name', 'endpoints': 'endpoints_name', 'endpointslice': 'endpointslice_name', 'ingress': 'ingress_name'}[role]
    return ns + '/' + ls.get('__meta_kubernetes_' + name, '')


def strip(ls):
    # the scrape config adds job, __scheme__, __scrape_interval__, ... ; the discoverers give __address__ and
    # __meta_* (and instance for nodes)
    # __meta_* (and instance for nodes); Prometheus' labels.Labels drop empty values, so the comparison does too
    return {k: v for k, v in ls.items() if (k in ('__address__', 'instance') or k.startswith('__meta_')) and v != ''}


def start_api(mutate, log):
    open(log, 'w').close()
    p = subprocess.Popen([sys.executable, os.path.join(HERE, 'fake_k8s_api.py'), '--port', '9800', '--mutate-after', str(mutate), '--log', log], stdout=subprocess.PIPE, text=True)
    p.stdout.readline()
    return p


def rust_dump(secs):
    exe = os.path.join(ROOT, 'target/debug/examples/k8s_sd_dump')
    r = subprocess.run([exe, sd_path, str(secs)], capture_output=True, text=True, check=True)
    open(os.path.join(TMP, 'rust-stderr.log'), 'a').write(r.stderr)
    out = r.stdout
    return [json.loads(l) for l in out.splitlines() if l.strip()]


fails = 0


def check(what, ok, detail=''):
    global fails
    print(('PASS ' if ok else 'FAIL ') + what + ('' if ok else '\n  ' + detail))
    if not ok:
        fails += 1


subprocess.run([os.path.expanduser('~/.cargo/bin/cargo'), 'build', '-q', '-p', 'discovery', '--example', 'k8s_sd_dump'], cwd=ROOT, check=True)

# ---- phase A
api = start_api(0, os.path.join(TMP, 'api-a.log'))
try:
    procs = {}
    for i in range(len(entries)):
        procs[i] = subprocess.Popen([os.path.join(PROM, 'promtool'), 'check', 'service-discovery', prom_cfg, f'k{i}', '--timeout=5s'], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    rust = rust_dump(5)
    go = {i: json.loads(p.communicate()[0] or '[]') for i, p in procs.items()}
finally:
    api.terminate()
    api.wait()
for i, role in enumerate(roles):
    gseq = {}
    for r in go[i]:
        d = strip(r['discoveredLabels'])
        gseq.setdefault(source_key(role, d), []).append(d)
    last = {}
    for u in rust:
        if u['cfg'] == i:
            last[u['source']] = [strip(t) for t in u['targets']]
    rseq = {}
    for src, ts in last.items():
        if ts:
            rseq[source_key(role, ts[0])] = ts
    bad = []
    for k in sorted(set(gseq) | set(rseq)):
        g, r = gseq.get(k, []), rseq.get(k, [])
        if not r or len(g) % len(r):
            bad.append((k, g, r))
            continue
        want = r * (len(g) // len(r))
        if role in ('endpoints', 'endpointslice'):
            # Go appends the targets of uncovered container ports per pod in map order (random): compare those
            # roles as sorted lists
            key = lambda t: json.dumps(t, sort_keys=True)
            g, want = sorted(g, key=key), sorted(want, key=key)
        if g != want:
            bad.append((k, g, want))
    ntargets = sum(len(v) for v in rseq.values())
    detail = ''
    for k, g, r in bad[:2]:
        detail += f'source {k}: prometheus {len(g)} targets, rust {len(r)}; first differences: '
        detail += json.dumps([{l: (x.get(l), y.get(l)) for l in set(x) | set(y) if x.get(l) != y.get(l)} for x, y in zip(g, r)])[:1500] + '\n  '
    check(f'A job k{i} ({role}): {len(rseq)} sources, {ntargets} targets, same groups and target order', not bad, detail)

# ---- phase B
data = os.path.join(TMP, 'data')
subprocess.run(['rm', '-rf', data])
api = start_api(6, os.path.join(TMP, 'api-b.log'))
prom = subprocess.Popen([os.path.join(PROM, 'prometheus'), f'--config.file={prom_cfg}', f'--storage.tsdb.path={data}', '--web.listen-address=127.0.0.1:9801'], stdout=subprocess.DEVNULL, stderr=open(os.path.join(TMP, 'prom.log'), 'w'))
try:
    rust = rust_dump(18)
    # Prometheus applies discovery updates to the scrape manager at most every 5 s
    time.sleep(8)
    tj = json.load(urllib.request.urlopen('http://127.0.0.1:9801/api/v1/targets?state=any'))
finally:
    prom.terminate()
    prom.wait()
    api.terminate()
    api.wait()
json.dump({'rust': rust, 'prometheus': tj}, open(os.path.join(TMP, 'phase-b.json'), 'w'))
gfinal = {}
for t in tj['data']['activeTargets'] + tj['data']['droppedTargets']:
    gfinal.setdefault(int(t['scrapePool'][1:]), set()).add(json.dumps(strip(t['discoveredLabels']), sort_keys=True))
rfinal_groups = {}
for u in rust:
    rfinal_groups[(u['cfg'], u['source'])] = u['targets']
for i, role in enumerate(roles):
    r = {json.dumps(strip(t), sort_keys=True) for (c, _), ts in rfinal_groups.items() if c == i for t in ts}
    g = gfinal.get(i, set())
    check(f'B job k{i} ({role}): {len(r)} final targets after watch updates, same as Prometheus', r == g, f'only rust: {sorted(r - g)[:2]}\n  only prometheus: {sorted(g - r)[:2]}')

log = [json.loads(l) for l in open(os.path.join(TMP, 'api-b.log'))]
rust_ua = [l for l in log if (l['ua'] or '').startswith('Prometheus/3.16.0') and l['accept'] == 'application/json']
check('B rust requests carry the bearer token and the kubeconfig token', any(l['auth'].startswith('Bearer secre') for l in rust_ua) and any(l['auth'].startswith('Bearer kubec') for l in rust_ua), str(rust_ua[:3]))
print('FAILS', fails)
sys.exit(1 if fails else 0)
