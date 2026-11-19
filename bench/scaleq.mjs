// Runs bench/scale-queries.txt <rounds> times, one request at a time, and prints per-query median ms as JSON.
// usage: node bench/scaleq.mjs <queries.txt> <base_url> <rounds> <out.json>
import http from 'node:http';
import fs from 'node:fs';
const [file, base, rounds, outFile] = process.argv.slice(2);
const u = new URL(base);
const qs = fs.readFileSync(file, 'utf8').split('\n').filter((l) => l && !l.startsWith('#')).map((l) => { const i = l.indexOf(' '); return [l.slice(0, i), l.slice(i + 1)]; });
const agent = new http.Agent({ keepAlive: true, maxSockets: 1 });
const enc = (b) => b.split('&').map((kv) => { const i = kv.indexOf('='); return kv.slice(0, i + 1) + encodeURIComponent(kv.slice(i + 1)); }).join('&');
function post(path, body) {
  const get = /\/label\/[^/]+\/values$/.test(path);
  if (get) { path = path + '?' + body; body = ''; }
  return new Promise((ok, bad) => {
    const t = performance.now();
    const req = http.request({ host: u.hostname, port: u.port, method: get ? 'GET' : 'POST', path, agent, headers: { 'content-type': 'application/x-www-form-urlencoded', 'content-length': Buffer.byteLength(body) } }, (res) => {
      let n = 0; res.on('data', (c) => (n += c.length)); res.on('end', () => ok({ status: res.statusCode, ms: performance.now() - t, bytes: n }));
    });
    req.on('error', bad); req.end(body);
  });
}
const res = qs.map(([p, b]) => ({ path: p, body: b, ms: [], status: 0, bytes: 0 }));
for (let r = 0; r < +rounds; r++) {
  for (const q of res) {
    const now = Math.floor(Date.now() / 1000);
    const body = enc(q.body.replace(/NOW-(\d+)/g, (_, d) => String(now - +d)).replace(/NOW/g, String(now)));
    const x = await post(q.path, body);
    q.ms.push(x.ms); q.status = x.status; q.bytes = x.bytes;
  }
}
const med = (a) => { const s = [...a].sort((x, y) => x - y); return +s[Math.floor(s.length / 2)].toFixed(2); };
const out = res.map((q) => ({ path: q.path, body: q.body, status: q.status, bytes: q.bytes, median_ms: med(q.ms) }));
fs.writeFileSync(outFile, JSON.stringify(out, null, 1));
agent.destroy();
