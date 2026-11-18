package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/grafana/regexp"
	"github.com/grafana/regexp/syntax"
)

func dump(re *syntax.Regexp) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d/%d/%v/%d/%d/%d/%q(", re.Op, re.Flags, re.Rune, re.Min, re.Max, re.Cap, re.Name)
	for i, s := range re.Sub {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(dump(s))
	}
	b.WriteString(")")
	return b.String()
}

func lines(fn string) []string {
	f, _ := os.Open(fn)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	var out []string
	for sc.Scan() {
		s, err := strconv.Unquote(sc.Text())
		if err != nil {
			panic(sc.Text())
		}
		out = append(out, s)
	}
	return out
}

type rec struct {
	P       string   `json:"p"`
	Tree    string   `json:"tree"`
	TreeP   string   `json:"tree_perl"`
	Compile string   `json:"compile"`
	Match   []bool   `json:"match"`
	Find    [][]int  `json:"find"`
	Repl    []string `json:"repl"`
}

func main() {
	pats := lines(os.Args[1])
	vals := lines(os.Args[2])
	enc := json.NewEncoder(os.Stdout)
	for _, p := range pats {
		if !utf8.ValidString(p) {
			continue
		}
		r := rec{P: p}
		if t, err := syntax.Parse(p, syntax.Perl|syntax.DotNL); err != nil {
			r.Tree = "ERR " + err.Error()
		} else {
			r.Tree = dump(t)
		}
		if t, err := syntax.Parse(p, syntax.Perl); err != nil {
			r.TreeP = "ERR " + err.Error()
		} else {
			r.TreeP = dump(t)
		}
		re, err := regexp.Compile("^(?s:" + p + ")$")
		if err != nil {
			r.Compile = err.Error()
		} else {
			for _, v := range vals {
				r.Match = append(r.Match, re.MatchString(v))
			}
		}
		re2, err := regexp.Compile(p)
		if err == nil {
			for _, v := range vals {
				r.Find = append(r.Find, re2.FindStringSubmatchIndex(v))
				r.Repl = append(r.Repl, re2.ReplaceAllString(v, "<$1|${name}|$0>"))
			}
		}
		enc.Encode(r)
	}
}
