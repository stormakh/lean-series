#!/usr/bin/env python3
"""Compares the API v1 JSON of prometheus-rs (web_e2e example) with real Prometheus 3.16.0.

Usage: web_e2e_compare.py <rust base URL> <go base URL>

Both servers scrape themselves (job "prometheus") and the same node_exporter (job "node"). Values that differ by
nature (timestamps, sample values, scrape durations, the self-scrape address, build data) are masked; the shape,
key order, types, label sets and everything else must match. Prints one line per check and exits 1 on a difference.
"""
import json
import sys
import time
import urllib.parse
import urllib.request

RS, GO = sys.argv[1], sys.argv[2]


def get(base, path, params=None):
    url = base + path
    if params:
        url += "?" + urllib.parse.urlencode(params, doseq=True)
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            return r.status, r.headers.get("Content-Type"), r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.headers.get("Content-Type"), e.read().decode()


def keys_shape(v):
    """The structure with key order, types; strings and numbers replaced by their type."""
    if isinstance(v, dict):
        return [(k, keys_shape(x)) for k, x in v.items()]
    if isinstance(v, list):
        return [keys_shape(x) for x in v]
    return type(v).__name__


fails = 0


def report(name, ok, detail=""):
    global fails
    print(("OK   " if ok else "DIFF ") + name + ("" if ok else "\n     " + detail[:2000]))
    if not ok:
        fails += 1


def compare(name, path, params=None, norm=None, shape_only=False):
    s1, c1, b1 = get(RS, path, params)
    s2, c2, b2 = get(GO, path, params)
    if s1 != s2 or c1 != c2:
        report(name, False, f"status/content-type rust={s1} {c1} go={s2} {c2}\nrust={b1[:500]}\ngo={b2[:500]}")
        return
    if not (c1 or "").startswith("application/json"):
        report(name, b1 == b2, f"rust={b1[:500]!r}\ngo  ={b2[:500]!r}")
        return
    j1 = json.loads(b1, object_pairs_hook=dict)
    j2 = json.loads(b2, object_pairs_hook=dict)
    if norm:
        j1, j2 = norm(j1), norm(j2)
    if shape_only:
        j1, j2 = keys_shape(j1), keys_shape(j2)
    a = json.dumps(j1, sort_keys=False)
    b = json.dumps(j2, sort_keys=False)
    report(name, a == b, f"rust={a[:1500]}\ngo  ={b[:1500]}")


def walk(v, f):
    if isinstance(v, dict):
        return {k: f(k, walk(x, f)) for k, x in v.items()}
    if isinstance(v, list):
        return [walk(x, f) for x in v]
    return v


SELF_RS = RS.split("//")[1]
SELF_GO = GO.split("//")[1]


def mask_self(v):
    if isinstance(v, str):
        return v.replace(SELF_RS, "SELF").replace(SELF_GO, "SELF")
    if isinstance(v, dict):
        return {mask_self(k): mask_self(x) for k, x in v.items()}
    if isinstance(v, list):
        return [mask_self(x) for x in v]
    return v


def mask_values(j):
    """Masks sample timestamps and values in query results; keeps label sets."""
    d = j.get("data", {})
    res = d.get("result")
    if isinstance(res, list) and all(isinstance(s, dict) for s in res):
        for s in res:
            if "value" in s:
                s["value"] = ["T", "V"]
            if "values" in s:
                s["values"] = ["N"]
    elif res is not None:
        d["result"] = ["T", "V"]
    return j


def sort_result(j):
    res = j.get("data", {}).get("result")
    if isinstance(res, list) and all(isinstance(s, dict) for s in res):
        res.sort(key=lambda s: json.dumps(s.get("metric", {}), sort_keys=True))
    return j


def only_common_series(j1_metrics):
    return j1_metrics


def main():
    for _ in range(60):
        s1, _, b1 = get(RS, "/api/v1/query", {"query": "up"})
        s2, _, b2 = get(GO, "/api/v1/query", {"query": "up"})
        if s1 == 200 and s2 == 200 and len(json.loads(b1)["data"]["result"]) == 2 and len(json.loads(b2)["data"]["result"]) == 2:
            break
        time.sleep(1)
    now = time.time()

    # Queries with label sets that do not depend on the server implementation.
    q_node = 'node_cpu_seconds_total{mode="idle"}'
    for name, params in [
        ("query up", {"query": "up"}),
        ("query node cpu", {"query": q_node}),
        ("query scalar", {"query": "1+1", "time": "1700000000.123"}),
        ("query string", {"query": '"hello"', "time": "1700000000"}),
        ("query sum by", {"query": "sum by (job) (up)"}),
        ("query rate", {"query": "sum by (mode) (rate(node_cpu_seconds_total[1m]))"}),
        ("query exact scalar values", {"query": "vector(1.5e-7) or vector(1e21) or vector(0.1)", "time": "100"}),
        ("query count", {"query": "count(node_cpu_seconds_total)"}),
    ]:
        compare(name + " (labels, masked values)", "/api/v1/query", params, lambda j: mask_values(sort_result(mask_self(j))))
    compare("query exact values", "/api/v1/query", {"query": "vector(1.5e-7) + vector(0)", "time": "100.5"})
    compare("query scalar exact", "/api/v1/query", {"query": "1/3", "time": "1700000000.123"})
    compare("query NaN/Inf", "/api/v1/query", {"query": "vector(1/0) or label_replace(vector(-1/0),'a','b','','') or label_replace(vector(0/0),'a','c','','')", "time": "5"})
    compare("query_range shape", "/api/v1/query_range", {"query": "up", "start": now - 60, "end": now, "step": "15"},
            lambda j: mask_values(sort_result(mask_self(j))))
    compare("query_range constant values", "/api/v1/query_range", {"query": "vector(2)", "start": "1000", "end": "1060.5", "step": "15"})
    compare("query_range bad step", "/api/v1/query_range", {"query": "up", "start": "0", "end": "10", "step": "0"})
    compare("query bad expr", "/api/v1/query", {"query": "sum("})
    compare("query bad time", "/api/v1/query", {"query": "up", "time": "x"})
    compare("query_range too many points", "/api/v1/query_range", {"query": "up", "start": "0", "end": "100000", "step": "1"})
    compare("query range vector", "/api/v1/query", {"query": "vector(3)[1m:20s]", "time": "100"})

    def series_norm(j):
        if isinstance(j.get("data"), list):
            j["data"] = sorted(mask_self(j["data"]), key=lambda m: json.dumps(m, sort_keys=True))
        return j

    compare("series node_cpu", "/api/v1/series", {"match[]": "node_cpu_seconds_total", "start": now - 30, "end": now}, series_norm)
    compare("series up", "/api/v1/series", {"match[]": "up"}, series_norm)
    compare("series two matchers", "/api/v1/series", {"match[]": ["up", 'node_load1']}, series_norm)
    compare("series no match", "/api/v1/series", {})
    compare("series limit", "/api/v1/series", {"match[]": "node_cpu_seconds_total", "limit": "2"}, lambda j: {"status": j["status"], "n": len(j["data"]), "warnings": j.get("warnings")})

    def node_only(j):
        return j

    compare("label values job", "/api/v1/label/job/values")
    compare("label values mode", "/api/v1/label/mode/values", {"match[]": "node_cpu_seconds_total"})
    compare("label values cpu", "/api/v1/label/cpu/values")
    compare("label values with limit", "/api/v1/label/job/values", {"limit": "1"})
    compare("label values bad name", "/api/v1/label/U__a_2e_/values")
    compare("labels for node", "/api/v1/labels", {"match[]": 'node_cpu_seconds_total'})
    compare("labels for up", "/api/v1/labels", {"match[]": "up"})

    def md_norm(j):
        # Node metadata only (self metrics differ by implementation).
        d = j.get("data", {})
        j["data"] = {k: v for k, v in d.items() if k.startswith("node_")}
        return j

    compare("metadata (node_ families)", "/api/v1/metadata", {}, md_norm)
    compare("metadata one metric", "/api/v1/metadata", {"metric": "node_load1"})
    compare("metadata limit bad", "/api/v1/metadata", {"limit": "x"})
    compare("targets/metadata node_load1", "/api/v1/targets/metadata", {"metric": "node_load1"}, mask_self)

    def targets_norm(j):
        d = j["data"]
        for t in d["activeTargets"]:
            t["lastScrape"] = "T"
            t["lastScrapeDuration"] = 0
            t["lastError"] = t["lastError"]
        d["activeTargets"].sort(key=lambda t: t["scrapePool"])
        return mask_self(j)

    compare("targets (volatile fields masked)", "/api/v1/targets", {}, targets_norm)
    compare("targets state=dropped", "/api/v1/targets", {"state": "dropped"})
    compare("scrape_pools", "/api/v1/scrape_pools")
    compare("status/config", "/api/v1/status/config", {}, mask_self)
    compare("status/buildinfo shape", "/api/v1/status/buildinfo", {}, shape_only=True)
    compare("status/runtimeinfo shape", "/api/v1/status/runtimeinfo", {}, shape_only=True)
    compare("status/tsdb shape", "/api/v1/status/tsdb", {}, lambda j: {"status": j["status"], "keys": list(j["data"].keys()), "head": list(j["data"]["headStats"].keys())})
    compare("status/walreplay", "/api/v1/status/walreplay")
    compare("format_query", "/api/v1/format_query", {"query": "sum(rate(foo{bar='baz'}[5m])) by (x)"})
    compare("parse_query", "/api/v1/parse_query", {"query": "sum by (x) (rate(foo{bar='baz'}[5m] offset 1m)) > bool 2"})
    compare("query_exemplars empty", "/api/v1/query_exemplars", {"query": "up", "start": "0", "end": now})
    compare("admin disabled", "/api/v1/admin/tsdb/snapshot")
    compare("unknown endpoint", "/api/v1/nope")
    print(f"{fails} differences")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
