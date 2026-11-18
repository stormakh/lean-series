// Query load for workload "host": N viewers of the Node Exporter Full dashboard, sent straight to the Prometheus API.
// It replays the requests that Lean Sight sent to Prometheus for one refresh (tests-data/host-requests.jsonl,
// recorded by bench/capture-host.sh), with the time window moved to "now".
// usage: node bench/promload.mjs <requests.jsonl> <base_url> <viewers> <minutes> <out.jsonl> [refresh_s=10]
// Per viewer: the 3 label-value requests once, then every refresh_s seconds the refresh set (instant and range
// queries), at most 6 requests at a time (as Lean Sight's connection pool per viewer), started with a random offset.
// Output: one JSON line per request {t, v, path, status, ms, bytes}. Summary on stderr.
import http from 'node:http';
import fs from 'node:fs';

const [recFile, base, viewersArg, minutesArg, outFile, refreshArg = '10'] = process.argv.slice(2);
const VIEWERS = +viewersArg, END = Date.now() + +minutesArg * 60000, REFRESH = +refreshArg * 1000;
const url = new URL(base);
const out = fs.createWriteStream(outFile);
const rows = fs.readFileSync(recFile, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
const open = rows.filter((r) => r.url.startsWith('/api/v1/label/'));
// one refresh: the queries of the first refresh (rows until the first expr repeats)
const refresh = [];
for (const r of rows.filter((r) => r.url.startsWith('/api/v1/query'))) {
  const key = r.url.split('?')[0] + (r.reqBody || r.url);
  const k2 = key.replace(/(start|end|time)=\d+(\.\d+)?/g, '');
  if (refresh.some((x) => x.k2 === k2)) break;
  refresh.push({ ...r, k2 });
}
const agent = new http.Agent({ keepAlive: true, maxSockets: 6 * VIEWERS });

function retime(s, now) {
  // Lean Sight sends unix seconds; keep the recorded window length
  const m = { start: /start=(\d+)/.exec(s), end: /end=(\d+)/.exec(s) };
  if (m.start && m.end) {
    const span = +m.end[1] - +m.start[1];
    return s.replace(/start=\d+/, `start=${now - span}`).replace(/end=\d+/, `end=${now}`);
  }
  return s.replace(/time=\d+(\.\d+)?/, `time=${now}`);
}

function send(v, r, now) {
  return new Promise((done) => {
    const path = r.url.includes('?') ? retime(r.url, now) : r.url;
    const body = r.reqBody ? retime(r.reqBody, now) : null;
    const t = performance.now();
    const headers = { 'accept-encoding': 'gzip' };
    if (body) { headers['content-type'] = 'application/x-www-form-urlencoded'; headers['content-length'] = Buffer.byteLength(body); }
    const req = http.request({ host: url.hostname, port: url.port, method: r.method, path, headers, agent }, (res) => {
      let n = 0;
      res.on('data', (c) => (n += c.length));
      res.on('end', () => { out.write(JSON.stringify({ t: Date.now(), v, path: path.split('?')[0], status: res.statusCode, ms: +(performance.now() - t).toFixed(2), bytes: n }) + '\n'); done(); });
    });
    req.on('error', (e) => { out.write(JSON.stringify({ t: Date.now(), v, path: path.split('?')[0], status: 0, ms: 0, err: e.code }) + '\n'); done(); });
    if (body) req.write(body);
    req.end();
  });
}

async function pool(v, list, now) {
  let i = 0;
  const worker = async () => { while (i < list.length) await send(v, list[i++], now); };
  await Promise.all(Array.from({ length: Math.min(6, list.length) }, worker));
}

async function viewer(v) {
  await new Promise((r) => setTimeout(r, Math.random() * REFRESH));
  await pool(v, open, Math.floor(Date.now() / 1000));
  while (Date.now() < END) {
    const t = Date.now();
    await pool(v, refresh, Math.floor(t / 1000));
    await new Promise((r) => setTimeout(r, Math.max(0, REFRESH - (Date.now() - t))));
  }
}

console.error(`promload: ${VIEWERS} viewers, ${open.length} open + ${refresh.length} per refresh, every ${REFRESH / 1000} s`);
await Promise.all(Array.from({ length: VIEWERS }, (_, v) => viewer(v)));
out.end();
agent.destroy();
console.error('done');
