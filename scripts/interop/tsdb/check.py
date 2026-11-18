#!/usr/bin/env python3
"""Helpers of scripts/interop/tsdb/run.sh.

  check.py gen-om <file> <end_ms>     write an OpenMetrics file with 5 h of samples ending at end_ms (for
                                      `promtool tsdb create-blocks-from openmetrics`)
  check.py same <a.txt> <b.txt>       compare two dumps in `promtool tsdb dump` form (order-insensitive)
  check.py api <dump.txt> <url>       query a running Prometheus for every series and sample of the dump and
                                      compare the float values (bit-exact) and histogram count/sum
"""
import json, math, struct, sys, urllib.parse, urllib.request


def gen_om(path, end_ms):
    end = end_ms // 1000
    start = end - 5 * 3600
    with open(path, 'w') as f:
        f.write('# HELP interop_counter A counter.\n# TYPE interop_counter counter\n')
        for inst in range(3):
            for t in range(start, end, 30):
                f.write('interop_counter_total{instance="i%d"} %d %d\n' % (inst, (t - start) * (inst + 1), t))
        f.write('# HELP interop_gauge A gauge.\n# TYPE interop_gauge gauge\n')
        for t in range(start, end, 30):
            f.write('interop_gauge{kind="sine"} %r %d\n' % (math.sin(t / 600.0) * 1e3, t))
            f.write('interop_gauge{kind="tiny"} %r %d\n' % (1.0 / (t % 997 + 3), t))
        f.write('# EOF\n')


def parse_labels(s):
    """Parses Go's labels.String(): {a="b", c="d"}. Returns (pairs tuple, rest of the line)."""
    assert s[0] == '{', s[:40]
    i, pairs = 1, []
    while s[i] != '}':
        j = s.index('=', i)
        name = s[i:j]
        assert s[j + 1] == '"'
        k, val = j + 2, []
        while s[k] != '"':
            if s[k] == '\\':
                nxt = s[k + 1]
                val.append({'n': '\n', 't': '\t', '\\': '\\', '"': '"'}.get(nxt, nxt))
                k += 2
            else:
                val.append(s[k])
                k += 1
        pairs.append((name, ''.join(val)))
        i = k + 1
        if s[i] == ',':
            i += 2  # ", "
    return tuple(sorted(pairs)), s[i + 2:]


def read_dump(path):
    """{labels: {t: value text}}"""
    out = {}
    for line in open(path):
        line = line.rstrip('\n')
        if not line:
            continue
        lbls, rest = parse_labels(line)
        val, ts = rest.rsplit(' ', 1)
        out.setdefault(lbls, {})[int(ts)] = val
    return out


def same(a, b):
    da, db = read_dump(a), read_dump(b)
    bad = 0
    for k in sorted(set(da) | set(db)):
        if da.get(k) != db.get(k):
            bad += 1
            if bad <= 5:
                sa, sb = da.get(k, {}), db.get(k, {})
                diff = [(t, sa.get(t), sb.get(t)) for t in sorted(set(sa) | set(sb)) if sa.get(t) != sb.get(t)]
                print('DIFF', dict(k), 'samples %d vs %d' % (len(sa), len(sb)), diff[:3])
    n = sum(len(v) for v in da.values())
    print('series %d vs %d, samples %d vs %d, differing series %d' % (
        len(da), len(db), n, sum(len(v) for v in db.values()), bad))
    return bad == 0 and len(da) > 0


def bits(x):
    return struct.unpack('<Q', struct.pack('<d', x))[0]


def hist_count_sum(text):
    # Go histogram String(): {count:N, sum:S, ...}
    parts = text.strip('{}').split(', ')
    c = float(parts[0].split(':', 1)[1])
    s = float(parts[1].split(':', 1)[1])
    return c, s


def api(dump, url):
    d = read_dump(dump)
    ts = [t for v in d.values() for t in v]
    mint, maxt = min(ts), max(ts)
    q = '{__name__=~".+"}[%dms]' % (maxt - mint + 1)
    u = url + '/api/v1/query?' + urllib.parse.urlencode({'query': q, 'time': '%.3f' % (maxt / 1000.0)})
    res = json.load(urllib.request.urlopen(u, timeout=120))
    assert res['status'] == 'success', res
    got = {}
    for r in res['data']['result']:
        k = tuple(sorted(r['metric'].items()))
        m = got.setdefault(k, {})
        for t, v in r.get('values', []):
            m[round(float(t) * 1000)] = ('f', float(v))
        for t, h in r.get('histograms', []):
            m[round(float(t) * 1000)] = ('h', float(h['count']), float(h['sum']))
    bad = checked = 0
    for k, samples in d.items():
        g = got.get(k, {})
        for t, v in samples.items():
            if v.startswith('{'):
                want = ('h',) + hist_count_sum(v)
                ok = g.get(t) == want or (g.get(t) and g[t][0] == 'h' and g[t][1] == want[1] and
                                          (g[t][2] == want[2] or (math.isnan(g[t][2]) and math.isnan(want[2]))))
            else:
                f = float(v)
                if math.isnan(f):
                    continue  # stale markers are not returned by range selectors
                ok = t in g and g[t][0] == 'f' and bits(g[t][1]) == bits(f)
            checked += 1
            if not ok:
                bad += 1
                if bad <= 5:
                    print('API DIFF', dict(k), t, v, g.get(t))
    extra = sum(1 for k in got if k not in d)
    print('api: checked %d samples of %d series, %d differ, %d extra series' % (checked, len(d), bad, extra))
    return bad == 0 and extra == 0 and checked > 0


if __name__ == '__main__':
    cmd = sys.argv[1]
    if cmd == 'gen-om':
        gen_om(sys.argv[2], int(sys.argv[3]))
    elif cmd == 'same':
        sys.exit(0 if same(sys.argv[2], sys.argv[3]) else 1)
    elif cmd == 'api':
        sys.exit(0 if api(sys.argv[2], sys.argv[3]) else 1)
    else:
        sys.exit(__doc__)
