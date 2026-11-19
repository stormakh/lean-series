#!/usr/bin/env python3
"""Compares the alerts that real Prometheus and prometheus-rs sent to the fake Alertmanagers.

usage: compare.py <go.log> <rs.log>
Each log line is `<path>\t<content-type>\t<json body>`. For each sender, the last state of every alert (by labels)
is taken. Times differ between the two runs, so `startsAt`/`endsAt` are checked for format and replaced by
"<time>". Everything else (paths, content type, keys and their order, labels, annotations, generatorURL) must be
equal. Prints the normalized bodies and exits 1 on a difference.
"""
import json, re, sys

TIME = re.compile(r'^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(Z|[+-]\d\d:\d\d)$')


def load(path):
    alerts, meta = {}, set()
    for line in open(path, encoding='utf-8'):
        p, ct, body = line.rstrip('\n').split('\t', 2)
        meta.add((p, ct))
        for a in json.loads(body, object_pairs_hook=list):
            d = dict(a)
            norm = []
            for k, v in a:
                if k in ('startsAt', 'endsAt'):
                    assert TIME.match(v), f'bad time {v!r} in {path}'
                    v = '<time>'
                norm.append((k, v))
            key = json.dumps(d['labels'])
            alerts[key] = norm
    return meta, [alerts[k] for k in sorted(alerts)]


def main():
    gm, ga = load(sys.argv[1])
    rm, ra = load(sys.argv[2])
    ok = True
    print('go paths:', sorted(gm))
    print('rs paths:', sorted(rm))
    if gm != rm:
        ok = False
        print('DIFF: paths or content types differ')
    print(f'alerts: go={len(ga)} rs={len(ra)}')
    for g, r in zip(ga, ra):
        gs, rs = json.dumps(g), json.dumps(r)
        if gs == rs:
            print('same:', gs)
        else:
            ok = False
            print('DIFF go:', gs)
            print('     rs:', rs)
    if len(ga) != len(ra):
        ok = False
    print('RESULT', 'equal' if ok else 'different')
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
