#!/usr/bin/env python3
"""Turn the upstream `go test -json ./...` run into tests-data/ledger.json.

One entry per top-level Go test function that passed upstream, plus one entry per promqltest file
(TestEvaluations/testdata/<file>.test/line_<n>: one per eval command; the Rust runner prints
`test promqltest::<file>_line_<n> ... ok` lines itself). Each entry names the Rust test that must exist for it:
  crates/<crate>/tests/<go_test_file_stem>.rs  ->  #[test] fn <rust>()
where <rust> is the Go name in snake_case (TestFooBar -> test_foo_bar). The crate is chosen per package in CRATE.
usage: scripts/ledger.py <(zcat tests-data/upstream-test.jsonl.gz) tests-data/ledger.json
"""
import json, os, re, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UP = os.path.join(ROOT, 'upstream')
MOD = 'github.com/prometheus/prometheus/'

# Go package prefix -> Rust crate. First match wins.
CRATE = [
    ('model/textparse', 'textparse'), ('model/', 'model'), ('promql', 'promql'),
    ('tsdb/chunkenc', 'chunkenc'), ('tsdb/chunks', 'tsdb-format'), ('tsdb/index', 'tsdb-format'),
    ('tsdb/record', 'tsdb-format'), ('tsdb/wlog', 'tsdb-format'), ('tsdb/tombstones', 'tsdb-format'),
    ('tsdb/encoding', 'tsdb-format'), ('tsdb/fileutil', 'tsdb-format'),
    ('tsdb/agent', 'agent'), ('tsdb', 'tsdb'), ('storage/remote', 'remote'), ('storage', 'storage'),
    ('scrape', 'scrape'), ('discovery', 'discovery'), ('rules', 'rules'), ('notifier', 'notifier'),
    ('config', 'config'), ('web', 'web'), ('cmd/promtool', 'promtool'), ('cmd/prometheus', 'server'),
    ('util', 'util'), ('tracing', 'util'), ('schema', 'model'), ('documentation', 'discovery'), ('internal', 'util'),
    ('prompb', 'remote'), ('template', 'rules'),
    ('', 'other'),
]

def snake(name):
    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    return re.sub(r'[^0-9a-zA-Z]+', '_', s).strip('_').lower()

def crate_of(pkg):
    for p, c in CRATE:
        if pkg.startswith(p):
            return c

_files = {}
def file_of(pkg, test):
    d = os.path.join(UP, pkg)
    if d not in _files:
        m = {}
        for f in sorted(os.listdir(d)) if os.path.isdir(d) else []:
            if f.endswith('_test.go'):
                for line in open(os.path.join(d, f), encoding='utf-8', errors='replace'):
                    g = re.match(r'func (Test\w+)\(', line)
                    if g:
                        m.setdefault(g.group(1), f[:-3])
        _files[d] = m
    return _files[d].get(test, '')

def main(src, dst):
    status = {}
    for line in open(src):
        try:
            e = json.loads(line)
        except ValueError:
            continue
        t, a = e.get('Test'), e.get('Action')
        if not t or a not in ('pass', 'fail', 'skip'):
            continue
        pkg = e['Package'][len(MOD):] if e['Package'].startswith(MOD) else e['Package']
        top = t.split('/')[0]
        if '/' in t and not (top == 'TestEvaluations' and pkg == 'promql'):
            continue
        status[(pkg, t)] = a
    out = []
    for (pkg, t), a in sorted(status.items()):
        if t.startswith('TestEvaluations/'):
            parts = t.split('/')  # TestEvaluations/testdata/<file>.test[/line_<n>/<expr>]
            if len(parts) < 4:
                continue
            name = snake(parts[2].replace('.test', '')) + '_' + parts[3]
            fstem = 'promqltest'
        else:
            name, fstem = snake(t), file_of(pkg, t)
        out.append({'package': pkg, 'go': t, 'file': fstem, 'crate': crate_of(pkg), 'rust': name, 'upstream': a})
    json.dump(out, open(dst, 'w'), indent=0)
    by = {}
    for e in out:
        k = (e['crate'], e['upstream'])
        by[k] = by.get(k, 0) + 1
    for (c, a), n in sorted(by.items()):
        print(f'{c:12} {a:5} {n}')
    print('total', len(out), 'passing', sum(e['upstream'] == 'pass' for e in out))

if __name__ == '__main__':
    main(*sys.argv[1:3])
