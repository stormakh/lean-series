#!/usr/bin/env python3
"""Small API load check: N clients send dashboard-like queries for S seconds; prints requests/s and latency.

Usage: web_api_load.py <base URL> [clients=20] [seconds=20]
"""
import sys
import threading
import time
import urllib.parse
import urllib.request

BASE = sys.argv[1]
N = int(sys.argv[2]) if len(sys.argv) > 2 else 20
S = float(sys.argv[3]) if len(sys.argv) > 3 else 20

QUERIES = [
    ("/api/v1/query_range", {"query": 'sum by (mode) (rate(node_cpu_seconds_total[1m]))', "step": "15"}),
    ("/api/v1/query_range", {"query": 'node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes', "step": "15"}),
    ("/api/v1/query", {"query": 'node_cpu_seconds_total'}),
    ("/api/v1/series", {"match[]": 'node_cpu_seconds_total'}),
    ("/api/v1/label/__name__/values", {}),
]

lat = []
lock = threading.Lock()
stop = time.time() + S


def worker(i):
    k = i
    while time.time() < stop:
        path, p = QUERIES[k % len(QUERIES)]
        k += 1
        p = dict(p)
        if path.endswith("query_range"):
            now = time.time()
            p["start"] = now - 3600
            p["end"] = now
        url = BASE + path + "?" + urllib.parse.urlencode(p)
        req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
        t = time.time()
        with urllib.request.urlopen(req) as r:
            r.read()
        with lock:
            lat.append(time.time() - t)


ts = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
for t in ts:
    t.start()
for t in ts:
    t.join()
lat.sort()
print(f"{BASE}: {len(lat)/S:.0f} req/s, p50 {lat[len(lat)//2]*1000:.1f} ms, p99 {lat[int(len(lat)*0.99)]*1000:.1f} ms")
