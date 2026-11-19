#!/usr/bin/env python3
"""Recorded cloud API responses for the SD end-to-end comparison (real Prometheus 3.16.0 vs prometheus-rs).

usage: mock.py <state-dir> <aws-port> <ecs-ec2-port> <openstack-port> <proxy-port>
- aws-port: AWS APIs for the `endpoint:` overrides (EC2/RDS/ElastiCache Query from fixtures/aws/<svc>_<Action>.xml,
  Lightsail JSON from fixtures/aws/lightsail_<Op>.json, ECS and MSK from the recorded fixtures/recorded/*.txt).
- ecs-ec2-port: the EC2 calls of the ECS role (AWS_ENDPOINT_URL_EC2), from fixtures/recorded/ecs-refresh.txt.
- openstack-port: Keystone v3 + Nova + Neutron + Octavia, bodies from upstream mock_test.go; the catalog points here.
- proxy-port: an HTTP CONNECT proxy that terminates TLS with <state-dir>/leaf.pem (signed by <state-dir>/ca.pem) and
  serves Google (OAuth, compute), Azure (AAD, ARM) and OCI from fixtures/{gce,azure,oci}.
Every request is logged to <state-dir>/requests.log.
"""
import json, os, re, ssl, sys, threading, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
FIX = os.path.join(HERE, 'fixtures')
ROOT = os.path.dirname(os.path.dirname(HERE))
STATE = sys.argv[1]
AWS_PORT, ECS_EC2_PORT, OS_PORT, PROXY_PORT = (int(a) for a in sys.argv[2:6])
LOG = open(os.path.join(STATE, 'requests.log'), 'a', buffering=1)


def maybe(*p):
    try:
        with open(os.path.join(FIX, *p), 'rb') as f:
            return f.read()
    except FileNotFoundError:
        return None


def q_dict(query):
    return dict(urllib.parse.parse_qsl(query, keep_blank_values=True))


def same_body(a, b):
    try:
        return json.loads(a) == json.loads(b)
    except Exception:  # noqa
        return q_dict(a) == q_dict(b)


# ---------------- recorded request/response files (### METHOD PATH [X-Amz-Target: T] / >>> body / <<< body) ----------------

def load_recorded(name):
    rules = []
    text = (maybe('recorded', name) or b'').decode()
    for block in text.split('\n### ')[0:]:
        block = block.lstrip('# ').strip('\n')
        if not block:
            continue
        lines = block.split('\n')
        head = lines[0]
        target = ''
        if ' X-Amz-Target:' in head:
            head, target = head.split(' X-Amz-Target:', 1)
            target = target.strip()
        method, url = head.split(' ', 1)
        path, _, query = url.strip().partition('?')
        req = '\n'.join(l[4:] for l in lines if l.startswith('>>> '))
        at = next((i for i, l in enumerate(lines) if l.startswith('<<< ')), len(lines))
        resp = '\n'.join([l[4:] for l in lines[at:at + 1]] + lines[at + 1:]).strip()
        rules.append({'method': method, 'path': urllib.parse.unquote(path), 'query': q_dict(query), 'target': target, 'req': req, 'resp': resp})
    return rules


ECS = load_recorded('ecs-refresh.txt')
MSK = load_recorded('msk-refresh.txt')


def match_recorded(rules, method, path, query, headers, body):
    target = headers.get('x-amz-target', '')
    cands = [r for r in rules if r['method'] == method and r['path'] == urllib.parse.unquote(path) and r['target'] == target]
    if target == '' and method == 'POST':
        action = q_dict(body.decode()).get('Action')
        cands = [r for r in cands if q_dict(r['req']).get('Action') == action]
    exact = [r for r in cands if (not r['req'] or same_body(r['req'], body.decode() or '{}')) and (method == 'POST' or r['query'] == q_dict(query))]
    pick = (exact or cands or [None])[0]
    if pick is None:
        return None
    resp = pick['resp']
    ctype = 'text/xml;charset=UTF-8' if resp.lstrip().startswith('<') else 'application/x-amz-json-1.1' if target else 'application/json'
    return 200, ctype, resp.encode()


# ---------------- AWS ----------------

def aws_error(code, msg):
    return 400, 'text/xml', f'<ErrorResponse><Error><Type>Sender</Type><Code>{code}</Code><Message>{msg}</Message></Error><RequestId>e2e</RequestId></ErrorResponse>'.encode()


def aws(method, path, query, headers, body):
    target = headers.get('x-amz-target', '')
    if target.startswith('Lightsail'):
        b = maybe('aws', 'lightsail_' + target.split('.')[-1] + '.json')
        if b is not None:
            return 200, 'application/x-amz-json-1.1', b
    if method == 'POST' and not target:
        form = q_dict(body.decode())
        svc = {'2016-11-15': 'ec2', '2014-10-31': 'rds', '2015-02-02': 'elasticache'}.get(form.get('Version'), 'x')
        b = maybe('aws', f"{svc}_{form.get('Action')}.xml")
        if b is not None:
            return 200, 'text/xml;charset=UTF-8', b
    r = match_recorded(ECS if target else MSK, method, path, query, headers, body)
    if r:
        return r
    return aws_error('InvalidAction', target or path)


def ecs_ec2(method, path, query, headers, body):
    return match_recorded(ECS, method, path, query, headers, body) or aws_error('InvalidAction', 'no recorded EC2 response')


# ---------------- OpenStack ----------------

MOCK_GO = open(os.path.join(ROOT, 'upstream/discovery/openstack/mock_test.go')).read()


def go_literal(marker):
    at = MOCK_GO.index(marker)
    s = MOCK_GO.index('`', at) + 1
    return MOCK_GO[s:MOCK_GO.index('`', s)]


def openstack(method, path, query, headers, body):
    ep = f'http://127.0.0.1:{OS_PORT}/'
    if path == '/v3/auth/tokens':
        return 201, 'application/json', (go_literal('m.Mux.HandleFunc("/v3/auth/tokens",') % (ep, ep, ep)).encode(), {'X-Subject-Token': 'cbc36478b0bd8e67e89469c7749d4127'}
    lists = {
        '/os-hypervisors/detail': 'const hypervisorListBody =',
        '/servers/detail': 'const serverListBody =',
        '/v2.0/floatingips': 'const listOutput =',
        '/v2.0/ports': 'const portsListBody =',
        '/v2.0/lbaas/loadbalancers': 'const lbListBody =',
        '/v2.0/lbaas/listeners': 'const listenerListBody =',
    }
    if path in lists:
        return 200, 'application/json', go_literal(lists[path]).encode()
    return 200, 'application/json', (go_literal('m.Mux.HandleFunc("/",') % (ep + 'v3/', ep + 'v2.0/')).encode()


# ---------------- TLS hosts behind the proxy ----------------

OCI = json.loads(maybe('oci', 'recorded_responses.json') or b'{"requests":[]}')['requests']
AZURE = json.loads(maybe('azure', 'azure-recorded.json') or b'{"requests":[]}')['requests']


def oci(host, method, path, query, headers, body):
    kind = 'identity' if host.startswith('identity.') else 'iaas'
    q = q_dict(query)
    for r in OCI:
        if r.get('host') == kind and r.get('path') == path and (r.get('query') or {}) == q:
            h = {'opc-request-id': 'e2e'}
            h.update(r.get('headers') or {})
            return r.get('status', 200), 'application/json', json.dumps(r['body']).encode(), h
    return 404, 'application/json', json.dumps({'code': 'NotAuthorizedOrNotFound', 'message': 'no fixture'}).encode()


def azure(host, method, path, query, headers, body):
    q = q_dict(query)
    best = None
    for r in AZURE:
        u = urllib.parse.urlsplit(r['url'])
        if u.hostname != host or r['method'] != method or urllib.parse.unquote(u.path).lower() != urllib.parse.unquote(path).lower():
            continue
        if q_dict(u.query) == q:
            best = r
            break
        best = best or r
    if best is None:
        return 404, 'application/json', json.dumps({'error': {'code': 'NotFound', 'message': path}}).encode()
    return best.get('status', 200), 'application/json', json.dumps(best['body']).encode()


def tls_host(host, method, path, query, headers, body):
    host = host.split(':')[0]
    if host in ('management.azure.com', 'login.microsoftonline.com'):
        return azure(host, method, path, query, headers, body)
    if host == 'oauth2.googleapis.com':
        return 200, 'application/json', json.dumps({'access_token': 'e2e-token', 'token_type': 'Bearer', 'expires_in': 3600}).encode()
    if host == 'compute.googleapis.com':
        return 200, 'application/json; charset=UTF-8', maybe('gce', 'instances_list.json')
    if host.endswith('oraclecloud.com'):
        return oci(host, method, path, query, headers, body)
    return 404, 'text/plain', b'unknown host ' + host.encode()


# ---------------- servers ----------------

def respond(h, res):
    status, ctype, body = res[0], res[1], res[2]
    extra = res[3] if len(res) > 3 else {}
    h.send_response(status)
    h.send_header('Content-Type', ctype)
    for k, v in extra.items():
        h.send_header(k, v)
    h.send_header('Content-Length', str(len(body)))
    h.end_headers()
    h.wfile.write(body)


def make_handler(route, scheme, tls_for=None):
    class H(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *a):
            pass

        def handle_any(self):
            n = int(self.headers.get('Content-Length') or 0)
            body = self.rfile.read(n) if n else b''
            u = urllib.parse.urlsplit(self.path)
            headers = {k.lower(): v for k, v in self.headers.items()}
            host = tls_for or headers.get('host', '')
            try:
                res = route(host, self.command, u.path, u.query, headers, body)
            except Exception as e:  # noqa
                res = (500, 'text/plain', repr(e).encode())
            LOG.write(f'{scheme} {self.command} {host}{self.path} -> {res[0]} ua={headers.get("user-agent", "")[:50]!r} target={headers.get("x-amz-target", "")} body={body[:160]!r}\n')
            respond(self, res)

        do_GET = do_POST = do_PUT = do_DELETE = handle_any

        def do_CONNECT(self):
            # TLS-terminating proxy: answer 200, wrap the socket, serve the requests for that host
            host = self.path
            self.send_response(200, 'Connection established')
            self.end_headers()
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(os.path.join(STATE, 'leaf.pem'), os.path.join(STATE, 'leaf.key'))
            try:
                conn = ctx.wrap_socket(self.connection, server_side=True)
            except Exception as e:  # noqa
                LOG.write(f'tls handshake failed for {host}: {e}\n')
                self.close_connection = True
                return
            inner = make_handler(lambda h, m, p, q, hd, b: tls_host(host, m, p, q, hd, b), 'https', host.split(':')[0])
            try:
                inner(conn, self.client_address, self.server)
            except Exception as e:  # noqa
                LOG.write(f'tls conn error {host}: {e}\n')
            self.close_connection = True
    return H


def serve(port, route, scheme):
    s = ThreadingHTTPServer(('127.0.0.1', port), make_handler(route, scheme))
    s.daemon_threads = True
    threading.Thread(target=s.serve_forever, daemon=True).start()


serve(AWS_PORT, lambda h, m, p, q, hd, b: aws(m, p, q, hd, b), 'aws')
serve(ECS_EC2_PORT, lambda h, m, p, q, hd, b: ecs_ec2(m, p, q, hd, b), 'ecs-ec2')
serve(OS_PORT, lambda h, m, p, q, hd, b: openstack(m, p, q, hd, b), 'openstack')
serve(PROXY_PORT, lambda h, m, p, q, hd, b: tls_host(h, m, p, q, hd, b), 'proxy')
print('ready', flush=True)
threading.Event().wait()
