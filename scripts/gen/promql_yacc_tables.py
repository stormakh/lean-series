#!/usr/bin/env python3
"""Generates crates/promql/src/parser/generated.rs from upstream promql/parser/generated_parser.y.go
(the goyacc output) and generated_parser.y (the grammar, for the rule texts).

usage: scripts/gen/promql_yacc_tables.py

The Rust parser runs the same LALR tables with a port of the goyacc driver, so it accepts the same language
and recovers from errors in the same way (error texts depend on the parser state). Rerun after an upstream bump,
then check the rule texts that crates/promql/src/parser/actions.rs expects (a unit test compares them).
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC = os.path.join(ROOT, 'upstream/promql/parser/generated_parser.y.go')
GRAMMAR = os.path.join(ROOT, 'upstream/promql/parser/generated_parser.y')
OUT = os.path.join(ROOT, 'crates/promql/src/parser/generated.rs')


def int_array(src, name):
    m = re.search(r'var %s = \[\.\.\.\](int\d*)\{(.*?)\n\}' % name, src, re.S)
    if not m:
        sys.exit('table %s not found' % name)
    return [int(x) for x in re.findall(r'-?\d+', m.group(2))]


def str_array(src, name):
    m = re.search(r'var %s = \[\.\.\.\]string\{(.*?)\n\}' % name, src, re.S)
    if not m:
        sys.exit('table %s not found' % name)
    return re.findall(r'"((?:[^"\\]|\\.)*)"', m.group(1))


def const(src, name):
    m = re.search(r'const %s = (-?\d+)' % name, src)
    if not m:
        sys.exit('const %s not found' % name)
    return int(m.group(1))


def grammar_rules(text):
    """Returns [(lhs, [symbols])] in goyacc rule order (rule 1 first)."""
    body = text.split('\n%%', 2)[1]
    toks = []
    i, n = 0, len(body)
    while i < n:
        c = body[i]
        if c.isspace():
            i += 1
        elif body.startswith('//', i):
            i = body.index('\n', i)
        elif body.startswith('/*', i):
            i = body.index('*/', i) + 2
        elif c == '{':
            # Skip a Go action block, minding strings, runes and comments.
            depth = 0
            while True:
                c = body[i]
                if c == '{':
                    depth += 1
                    i += 1
                elif c == '}':
                    depth -= 1
                    i += 1
                    if depth == 0:
                        break
                elif c in '"\'`':
                    q = c
                    i += 1
                    while body[i] != q:
                        i += 2 if body[i] == '\\' and q != '`' else 1
                    i += 1
                elif body.startswith('//', i):
                    i = body.index('\n', i)
                elif body.startswith('/*', i):
                    i = body.index('*/', i) + 2
                else:
                    i += 1
            toks.append('{}')
        elif c in ':|;':
            toks.append(c)
            i += 1
        elif body.startswith('%prec', i):
            toks.append('%prec')
            i += 5
        else:
            m = re.match(r'[A-Za-z_][A-Za-z0-9_]*', body[i:])
            if not m:
                sys.exit('grammar: unexpected %r' % body[i:i + 20])
            toks.append(m.group(0))
            i += len(m.group(0))
    rules = []
    lhs, cur, j = None, None, 0
    while j < len(toks):
        t = toks[j]
        if j + 1 < len(toks) and toks[j + 1] == ':' and t not in ('{}', ':', '|', ';', '%prec'):
            if cur is not None:
                rules.append((lhs, cur))
            lhs, cur = t, []
            j += 2
            continue
        if t == '|':
            rules.append((lhs, cur))
            cur = []
        elif t == ';':
            rules.append((lhs, cur))
            cur = None
        elif t == '%prec':
            j += 1
        elif t != '{}':
            cur.append(t)
        j += 1
    if cur is not None:
        rules.append((lhs, cur))
    return rules


def rust_array(name, typ, vals):
    lines = []
    for k in range(0, len(vals), 16):
        lines.append('    ' + ', '.join(str(v) for v in vals[k:k + 16]) + ',')
    return 'pub(crate) static %s: [%s; %d] = [\n%s\n];\n' % (name, typ, len(vals), '\n'.join(lines))


def main():
    src = open(SRC).read()
    consts = re.findall(r'^const ([A-Za-z_][A-Za-z0-9_]*) = (5\d{4})$', src, re.M)
    toknames = str_array(src, 'yyToknames')
    tables = {n: int_array(src, n) for n in
              ('yyExca', 'yyAct', 'yyPact', 'yyPgo', 'yyR1', 'yyR2', 'yyChk', 'yyDef', 'yyTok1', 'yyTok2', 'yyTok3')}
    rules = grammar_rules(open(GRAMMAR).read())
    r1, r2 = tables['yyR1'], tables['yyR2']
    if len(rules) + 1 != len(r2):
        sys.exit('grammar has %d rules, tables %d' % (len(rules), len(r2) - 1))
    # Check rule lengths and that rules with the same left side share one nonterminal number.
    nt = {}
    for k, (lhs, syms) in enumerate(rules, 1):
        if len(syms) != r2[k]:
            sys.exit('rule %d (%s: %s) length %d, table says %d' % (k, lhs, ' '.join(syms), len(syms), r2[k]))
        if nt.setdefault(lhs, r1[k]) != r1[k]:
            sys.exit('rule %d: nonterminal number mismatch for %s' % (k, lhs))

    out = ['// Code generated by scripts/gen/promql_yacc_tables.py from upstream promql/parser/generated_parser.y.go.',
           '// DO NOT EDIT.',
           '',
           '#![allow(non_upper_case_globals, dead_code)]',
           '',
           'use super::lex::ItemType;',
           '']
    for name, val in consts:
        out.append('pub const %s: ItemType = ItemType(%s);' % (name, val))
    out.append('')
    for name, val in (('YY_EOF_CODE', const(src, 'yyEofCode')), ('YY_ERR_CODE', const(src, 'yyErrCode')),
                      ('YY_PRIVATE', const(src, 'yyPrivate')), ('YY_LAST', const(src, 'yyLast')),
                      ('YY_FLAG', const(src, 'yyFlag'))):
        out.append('pub(crate) const %s: i32 = %d;' % (name, val))
    out.append('')
    out.append('pub(crate) static YY_TOKNAMES: [&str; %d] = [%s];' % (
        len(toknames), ', '.join('"%s"' % t for t in toknames)))
    out.append('')
    for name, rname in (('yyExca', 'YY_EXCA'), ('yyAct', 'YY_ACT'), ('yyPact', 'YY_PACT'), ('yyPgo', 'YY_PGO'),
                        ('yyR1', 'YY_R1'), ('yyR2', 'YY_R2'), ('yyChk', 'YY_CHK'), ('yyDef', 'YY_DEF'),
                        ('yyTok1', 'YY_TOK1'), ('yyTok2', 'YY_TOK2'), ('yyTok3', 'YY_TOK3')):
        out.append(rust_array(rname, 'i16', tables[name]))
    out.append('/// The grammar rules by goyacc rule number (index 0 is the accept rule).')
    out.append('pub(crate) static RULES: [&str; %d] = [' % (len(rules) + 1))
    out.append('    "$accept: start $end",')
    for lhs, syms in rules:
        out.append('    "%s:%s",' % (lhs, ''.join(' ' + s for s in syms)))
    out.append('];')
    open(OUT, 'w').write('\n'.join(out) + '\n')
    print('wrote %s: %d rules, %d states' % (os.path.relpath(OUT, ROOT), len(rules), len(tables['yyPact'])))


if __name__ == '__main__':
    main()
