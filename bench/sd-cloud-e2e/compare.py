#!/usr/bin/env python3
"""Compares the discovered __address__ and __meta_* labels of Prometheus (/api/v1/targets?state=any) and
prometheus-rs (examples/sd_dump), per job. usage: compare.py <prom-targets.json> <rs-targets.json> <jobs.yml>"""
import json, re, sys

prom = json.load(open(sys.argv[1]))['data']
rs = json.load(open(sys.argv[2]))
jobs = re.findall(r'^- job_name: (\S+)', open(sys.argv[3]).read(), re.M)


def keep(labels):
    # Prometheus drops empty label values when it builds a target; that is the scrape layer, not SD
    return tuple(sorted((k, v) for k, v in labels.items() if v != '' and (k == '__address__' or k.startswith('__meta_'))))


p = {}
for t in prom['activeTargets'] + prom.get('droppedTargets', []):
    p.setdefault(t['discoveredLabels']['job'], set()).add(keep(t['discoveredLabels']))
ok = True
for j in jobs:
    # a target without __address__ is dropped by the scrape layer (not shown by the API)
    a, b = p.get(j, set()), {keep(t) for t in rs.get(j, []) if t.get('__address__')}
    if a == b and a:
        print(f'OK    {j}: {len(a)} targets, {sum(len(t) for t in a)} labels identical')
        continue
    addr = lambda ts: {dict(t).get('__address__') for t in ts}
    if a and a <= b and addr(a) == addr(b):
        # Prometheus keeps one target per final label set: targets with the same __address__ in one job collapse
        print(f'OK    {j}: {len(a)} targets identical; prometheus-rs also has {len(b - a)} with a duplicate __address__ that the scrape pool merges')
        continue
    ok = False
    print(f'DIFF  {j}: prometheus {len(a)} targets, prometheus-rs {len(b)} targets')
    for t in sorted(a - b):
        print('  only prometheus:   ', dict(t))
    for t in sorted(b - a):
        print('  only prometheus-rs:', dict(t))
sys.exit(0 if ok else 1)
