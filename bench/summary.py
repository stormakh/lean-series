#!/usr/bin/env python3
"""Summarize a bench/results/host-* run: memory and CPU per phase, request latency. usage: summary.py <run_dir>"""
import csv, json, os, sys
d = sys.argv[1]
rows = list(csv.DictReader(open(os.path.join(d, 'samples.csv'))))
HZ = os.sysconf('SC_CLK_TCK')
out = {}
for ph in ('idle', 'load', 'after'):
    s = [r for r in rows if r['phase'] == ph]
    if len(s) < 2: continue
    pss = [int(r['pss_kb']) for r in s]
    dt = int(s[-1]['epoch']) - int(s[0]['epoch'])
    cpu = (int(s[-1]['cpu_ticks']) - int(s[0]['cpu_ticks'])) / HZ
    out[ph] = {'pss_mb_avg': round(sum(pss) / len(pss) / 1024, 2), 'pss_mb_max': round(max(pss) / 1024, 2),
               'cpu_pct': round(100 * cpu / dt, 3), 'cpu_s': cpu, 'seconds': dt}
rq = os.path.join(d, 'req.jsonl')
if os.path.exists(rq):
    r = [json.loads(l) for l in open(rq)]
    ok = [x for x in r if x['status'] == 200]
    ms = sorted(x['ms'] for x in ok if x['path'].startswith('/api/v1/query'))
    q = lambda p: ms[min(len(ms) - 1, int(p * len(ms)))] if ms else None
    out['requests'] = {'total': len(r), 'failed': len(r) - len(ok), 'query_p50_ms': q(.5), 'query_p95_ms': q(.95),
                       'query_p99_ms': q(.99), 'bytes': sum(x.get('bytes', 0) for x in r)}
    if 'load' in out:
        out['load']['cpu_ms_per_request'] = round(1000 * out['load']['cpu_s'] / max(1, len(r)), 3)
json.dump(out, open(os.path.join(d, 'summary.json'), 'w'), indent=1)
print(json.dumps(out, indent=1))
