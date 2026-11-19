#!/usr/bin/env python3
# Generates EXPECTED_ERRORS of crates/config/tests/config_test.rs from upstream/config/config_test.go:
# (file, error text, SD providers the file needs). Usage: scripts/gen/config_bad_table.py <repo root>
import re, os, sys
root = sys.argv[1]
src = open(os.path.join(root, 'upstream/config/config_test.go')).read()
block = re.search(r'^var expectedErrors = .*?^}\n', src, re.S | re.M).group(0)
rows = re.findall(r'filename:\s*("(?:[^"\\]|\\.)*"|`[^`]*`),\s*\n\s*errMsg:\s*("(?:[^"\\]|\\.)*"|`[^`]*`),', block)
def lit(s):
    body = s[1:-1]
    if s[0] == '"':
        assert '\\' not in body.replace('\\"', ''), body
        body = body.replace('\\"', '"')
    return 'r#"' + body + '"#'
out = []
for fn, msg in rows:
    name = fn[1:-1]
    text = open(os.path.join(root, 'upstream/config/testdata', name), 'rb').read().decode('latin1')
    ks = sorted(set(re.findall(r'([a-z0-9_]+)_sd_configs', text)) - {'file', 'http'})
    if name == 'unknown_attr.bad.yml':
        ks = []  # "consult_sd_configs" is the unknown field under test, not a provider
    prov = '&[' + ', '.join('"%s"' % k for k in ks) + ']'
    out.append('    (%s, %s, %s),' % (lit(fn), lit(msg), prov))
print(len(rows), file=sys.stderr)
allp = sorted({p for r in out for p in re.findall(r'"([a-z0-9_]+)"', r.split('&[')[1])})
print('providers:', ', '.join(allp), file=sys.stderr)
print('\n'.join(out))
