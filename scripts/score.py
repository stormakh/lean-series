#!/usr/bin/env python3
"""Score the Rust port against tests-data/ledger.json.

usage: scripts/score.py [crate ...] [--release] [--list-missing]
For each crate (default: every crate that has ledger entries and exists in crates/), runs
`cargo test -p <crate> --no-fail-fast` and matches each passing `test <name> ... ok` line under
`Running tests/<file>.rs` to the ledger entry (crate, file, rust). Prints `SCORE <crate> passed/total` per crate and
`SCORE total passed/total`. Only ledger entries that passed upstream count in the total.
A test that panics, fails, or is #[ignore]d does not count. Extra Rust tests that are not in the ledger are ignored.
"""
import json, os, re, subprocess, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CARGO = os.path.expanduser('~/.cargo/bin/cargo')

def main():
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    release = '--release' in sys.argv
    ledger = [e for e in json.load(open(os.path.join(ROOT, 'tests-data/ledger.json'))) if e['upstream'] == 'pass']
    crates = args or sorted({e['crate'] for e in ledger if os.path.isdir(os.path.join(ROOT, 'crates', e['crate']))})
    total_ok = total = 0
    missing = []
    for c in crates:
        want = {(e['file'], e['rust']) for e in ledger if e['crate'] == c}
        cmd = [CARGO, 'test', '-p', c, '--no-fail-fast'] + (['--release'] if release else [])
        p = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)  # one pipe keeps order
        cur, ok = None, set()
        for line in p.stdout.splitlines():
            m = re.search(r'Running tests/([\w\-]+)\.rs', line)
            if m:
                cur = m.group(1)
                continue
            if 'Running ' in line:
                cur = None
                continue
            m = re.match(r'test (\S+) \.\.\. ok$', line.strip())
            if m and cur:
                ok.add((cur, m.group(1).split('::')[-1]))
        got = want & ok
        total_ok += len(got)
        total += len(want)
        missing += [(c, f, n) for f, n in sorted(want - ok)]
        print(f'SCORE {c} {len(got)}/{len(want)}' + ('' if p.returncode == 0 else '  (cargo test exit %d)' % p.returncode))
    print(f'SCORE total {total_ok}/{total}')
    if '--list-missing' in sys.argv:
        for c, f, n in missing:
            print(f'missing {c} tests/{f}.rs {n}')

if __name__ == '__main__':
    main()
