#!/usr/bin/env python3
"""Compares samples received over remote write (JSON lines from rw_e2e) with what a Prometheus API returns.

usage: compare.py <received.jsonl> <api-response.json> [--from-ms N] [--to-ms N]
The API response is from /api/v1/query?query={__name__=~".+"}[<range>] (a matrix). Stale markers in the
received samples are dropped (range selectors never return them). Floats compare as Go 'f' -1 strings;
histograms compare by count and sum. Prints a summary and exits 1 on any difference.
"""
import json, sys


def key(metric):
    return tuple(sorted(metric.items()))


def main():
    rec_path, api_path = sys.argv[1], sys.argv[2]
    lo = int(sys.argv[sys.argv.index('--from-ms') + 1]) if '--from-ms' in sys.argv else -(2 ** 63)
    hi = int(sys.argv[sys.argv.index('--to-ms') + 1]) if '--to-ms' in sys.argv else 2 ** 63
    got = set()
    stale = 0
    for line in open(rec_path):
        line = line.strip()
        if not line:
            continue
        s = json.loads(line)
        if s.get('stale'):
            stale += 1
            continue
        if not (lo <= s['t'] <= hi):
            continue
        val = ('f', s['v']) if 'v' in s else ('h', s['hcount'], s['hsum'])
        got.add((key(s['metric']), s['t'], val))
    want = set()
    api = json.load(open(api_path))
    assert api['status'] == 'success', api
    for series in api['data']['result']:
        k = key(series['metric'])
        for t, v in series.get('values', []):
            ms = round(float(t) * 1000)
            if lo <= ms <= hi:
                want.add((k, ms, ('f', v)))
        for t, h in series.get('histograms', []):
            ms = round(float(t) * 1000)
            if lo <= ms <= hi:
                want.add((k, ms, ('h', h['count'], h['sum'])))
    missing = want - got
    extra = got - want
    series = len({k for k, _, _ in want})
    hist = sum(1 for x in want if x[2][0] == 'h')
    print(f'{rec_path}: expected {len(want)} samples ({hist} histograms) in {series} series, '
          f'received {len(got)} (+{stale} stale markers); missing {len(missing)}, extra {len(extra)}')
    for x in sorted(missing)[:5]:
        print('  missing', x)
    for x in sorted(extra)[:5]:
        print('  extra  ', x)
    sys.exit(0 if not missing and not extra and want else 1)


if __name__ == '__main__':
    main()
