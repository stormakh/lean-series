#!/usr/bin/env python3
"""A fake Kubernetes API server for the end-to-end check of Kubernetes SD (Rust port vs Prometheus 3.16.0).

Serves JSON list and watch (with resourceVersion, bookmarks, and the watch-list stream that client-go
v0.37 tries first: sendInitialEvents + the "k8s.io/initial-events-end" bookmark) for nodes, namespaces,
pods, services, endpoints, endpointslices, ingresses, replicasets and jobs, from realistic objects.
After --mutate-after seconds it applies a fixed list of changes (add, update, delete), so that watches
are exercised too.

usage: fake_k8s_api.py --port 9800 [--mutate-after 8] [--log FILE]
"""
import argparse, copy, json, re, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

LOCK = threading.Condition()
RV = [100]
# resource -> {(ns, name): obj}
STORE = {}
# (rv, resource, ns, type, obj)
EVENTS = []
LOG = None

GROUPS = {
    'pods': '/api/v1', 'nodes': '/api/v1', 'services': '/api/v1', 'endpoints': '/api/v1', 'namespaces': '/api/v1',
    'endpointslices': '/apis/discovery.k8s.io/v1', 'ingresses': '/apis/networking.k8s.io/v1',
    'replicasets': '/apis/apps/v1', 'jobs': '/apis/batch/v1',
}
KINDS = {'pods': ('Pod', 'v1'), 'nodes': ('Node', 'v1'), 'services': ('Service', 'v1'), 'endpoints': ('Endpoints', 'v1'),
         'namespaces': ('Namespace', 'v1'), 'endpointslices': ('EndpointSlice', 'discovery.k8s.io/v1'),
         'ingresses': ('Ingress', 'networking.k8s.io/v1'), 'replicasets': ('ReplicaSet', 'apps/v1'), 'jobs': ('Job', 'batch/v1')}


def meta(name, ns=None, labels=None, annotations=None, owner=None, uid=None):
    m = {'name': name, 'uid': uid or ('uid-' + name), 'creationTimestamp': '2026-11-18T10:00:00Z'}
    if ns:
        m['namespace'] = ns
    if labels:
        m['labels'] = labels
    if annotations:
        m['annotations'] = annotations
    if owner:
        kind, oname = owner
        m['ownerReferences'] = [{'apiVersion': 'v1', 'kind': kind, 'name': oname, 'uid': 'uid-' + oname, 'controller': True, 'blockOwnerDeletion': True}]
    m['managedFields'] = [{'manager': 'kube-controller-manager', 'operation': 'Update', 'apiVersion': 'v1'}]
    return m


def put(resource, obj, typ=None):
    """Stores an object with a new resource version and records a watch event (caller holds LOCK)."""
    RV[0] += 1
    obj = copy.deepcopy(obj)
    obj['metadata']['resourceVersion'] = str(RV[0])
    kind, api = KINDS[resource]
    obj['kind'], obj['apiVersion'] = kind, api
    key = (obj['metadata'].get('namespace', ''), obj['metadata']['name'])
    prev = STORE.setdefault(resource, {}).get(key)
    STORE[resource][key] = obj
    EVENTS.append((RV[0], resource, key[0], typ or ('MODIFIED' if prev else 'ADDED'), obj, prev))
    LOCK.notify_all()


def delete(resource, ns, name):
    RV[0] += 1
    obj = STORE[resource].pop((ns, name))
    obj = copy.deepcopy(obj)
    obj['metadata']['resourceVersion'] = str(RV[0])
    EVENTS.append((RV[0], resource, ns, 'DELETED', obj, None))
    LOCK.notify_all()


def node(name, addresses, labels, conditions, provider='', annotations=None):
    return {'metadata': meta(name, labels=labels, annotations=annotations),
            'spec': {'providerID': provider, 'podCIDR': '10.244.0.0/24'},
            'status': {'addresses': [{'type': t, 'address': a} for t, a in addresses],
                       'conditions': [{'type': t, 'status': s, 'reason': 'x', 'lastHeartbeatTime': '2026-11-18T10:00:00Z'} for t, s in conditions],
                       'daemonEndpoints': {'kubeletEndpoint': {'Port': 10250}},
                       'nodeInfo': {'kubeletVersion': 'v1.34.1', 'osImage': 'Ubuntu'}}}


def container(name, image, ports):
    return {'name': name, 'image': image, 'resources': {}, 'ports': [dict({'containerPort': p, 'protocol': proto}, **({'name': n} if n else {})) for n, p, proto in ports]}


def pod(name, ns, node_name, ip, containers, init=None, owner=None, labels=None, annotations=None, phase='Running', ready='True'):
    statuses = [{'name': c['name'], 'ready': True, 'restartCount': 0, 'image': c['image'], 'containerID': 'containerd://' + name + '-' + c['name']} for c in containers]
    st = {'phase': phase, 'hostIP': '10.0.0.1' if node_name == 'node-1' else '10.0.0.2', 'conditions': [{'type': 'Initialized', 'status': 'True'}, {'type': 'Ready', 'status': ready}],
          'containerStatuses': statuses, 'qosClass': 'BestEffort'}
    if ip:
        st['podIP'] = ip
        st['podIPs'] = [{'ip': ip}]
    if init:
        st['initContainerStatuses'] = [{'name': c['name'], 'ready': False, 'containerID': 'containerd://' + name + '-init-' + c['name']} for c in init]
    spec = {'nodeName': node_name, 'containers': containers, 'restartPolicy': 'Always'}
    if init:
        spec['initContainers'] = init
    return {'metadata': meta(name, ns, labels, annotations, owner), 'spec': spec, 'status': st}


def service(name, ns, typ, ports, cluster_ip='10.96.0.10', labels=None, annotations=None, extra=None, lb_ips=None):
    spec = {'type': typ, 'ports': [{'name': n, 'protocol': proto, 'port': p, 'targetPort': p} for n, p, proto in ports], 'selector': {'app': name}}
    if typ != 'ExternalName':
        spec['clusterIP'] = cluster_ip
        spec['clusterIPs'] = [cluster_ip]
    spec.update(extra or {})
    status = {'loadBalancer': {'ingress': [{'ip': i} for i in lb_ips]} if lb_ips else {}}
    return {'metadata': meta(name, ns, labels, annotations), 'spec': spec, 'status': status}


def objects():
    with LOCK:
        for n in [
            node('node-1', [('InternalIP', '10.0.0.1'), ('Hostname', 'node-1'), ('ExternalIP', '34.1.2.3')], {'kubernetes.io/hostname': 'node-1', 'node-role.kubernetes.io/control-plane': '', 'tier': 'gold'},
                 [('MemoryPressure', 'False'), ('DiskPressure', 'False'), ('Ready', 'True')], 'aws:///eu-west-1a/i-0001', {'node.alpha.kubernetes.io/ttl': '0'}),
            node('node-2', [('InternalDNS', 'node-2.internal'), ('ExternalIP', '34.1.2.4')], {'kubernetes.io/hostname': 'node-2', 'tier': 'silver'}, [('Ready', 'False')], 'aws:///eu-west-1b/i-0002'),
            node('node-3', [], {'tier': 'gold'}, [('Ready', 'Unknown')]),
            node('node-4', [('InternalIP', 'fd00::4'), ('InternalIP', '10.0.0.4')], {'tier': 'gold'}, [('Ready', 'True'), ('PIDPressure', 'False')]),
        ]:
            put('nodes', n)
        put('namespaces', {'metadata': meta('default', labels={'kubernetes.io/metadata.name': 'default', 'team': 'web'}, annotations={'owner': 'alice@example.com'}), 'spec': {'finalizers': ['kubernetes']}, 'status': {'phase': 'Active'}})
        put('namespaces', {'metadata': meta('monitoring', labels={'kubernetes.io/metadata.name': 'monitoring'}), 'status': {'phase': 'Active'}})
        put('namespaces', {'metadata': meta('kube-system'), 'status': {'phase': 'Active'}})
        put('replicasets', {'metadata': meta('web-7d9f8', 'default', {'app': 'web'}, owner=('Deployment', 'web')), 'spec': {'replicas': 2}})
        put('replicasets', {'metadata': meta('orphan-rs', 'default'), 'spec': {'replicas': 1}})
        put('jobs', {'metadata': meta('nightly-29001', 'default', owner=('CronJob', 'nightly')), 'spec': {}})
        put('jobs', {'metadata': meta('once', 'default'), 'spec': {}})

        app = container('app', 'registry.example.com/web:1.2.3', [('http', 8080, 'TCP'), ('metrics', 9090, 'TCP')])
        side = container('sidecar', 'envoy:1.30', [])
        init = [container('init-db', 'busybox:1.36', [('initport', 7000, 'TCP')])]
        put('pods', pod('web-7d9f8-abcde', 'default', 'node-1', '10.244.1.5', [app, side], init, ('ReplicaSet', 'web-7d9f8'), {'app': 'web', 'pod-template-hash': '7d9f8'}, {'prometheus.io/scrape': 'true', 'kubectl.kubernetes.io/restartedAt': '2026-11-18T09:00:00Z'}))
        put('pods', pod('web-7d9f8-fghij', 'default', 'node-2', '10.244.2.7', [app, side], None, ('ReplicaSet', 'web-7d9f8'), {'app': 'web'}, ready='False'))
        put('pods', pod('web-7d9f8-pend', 'default', 'node-2', None, [app], None, ('ReplicaSet', 'web-7d9f8'), {'app': 'web'}, phase='Pending', ready='False'))
        put('pods', pod('nightly-29001-xyz', 'default', 'node-1', '10.244.1.9', [container('job', 'batch:2', [])], None, ('Job', 'nightly-29001'), {'job-name': 'nightly-29001'}, phase='Succeeded', ready='False'))
        put('pods', pod('once-q', 'default', 'node-4', '10.244.4.2', [container('job', 'batch:2', [('', 8000, 'UDP')])], None, ('Job', 'once')))
        put('pods', pod('orphan-rs-1', 'default', 'node-4', '10.244.4.3', [container('o', 'o:1', [('web', 80, 'TCP')])], None, ('ReplicaSet', 'orphan-rs')))
        put('pods', pod('node-exporter-1', 'monitoring', 'node-1', 'fd00:10:244::5', [container('node-exporter', 'quay.io/prometheus/node-exporter:v1.12.1', [('metrics', 9100, 'TCP')])], None, ('DaemonSet', 'node-exporter'), {'app': 'node-exporter'}))
        put('pods', pod('coredns-1', 'kube-system', 'node-3', '10.244.3.2', [container('coredns', 'coredns:1.11', [('dns', 53, 'UDP'), ('dns-tcp', 53, 'TCP'), ('metrics', 9153, 'TCP')])], None, None, {'k8s-app': 'kube-dns'}))

        put('services', service('web', 'default', 'ClusterIP', [('http', 80, 'TCP'), ('metrics', 9090, 'TCP')], '10.96.10.1', {'app': 'web'}, {'prometheus.io/port': '9090'}))
        put('services', service('lb', 'default', 'LoadBalancer', [('https', 443, 'TCP')], '10.96.10.2', extra={'loadBalancerIP': '1.1.1.1'}, lb_ips=['35.1.1.1', '', '35.1.1.2']))
        put('services', service('lb-old', 'default', 'LoadBalancer', [('', 8443, 'TCP')], '10.96.10.3', extra={'loadBalancerIP': '1.1.1.9'}))
        put('services', service('ext', 'default', 'ExternalName', [('pg', 5432, 'TCP')], extra={'externalName': 'db.example.com'}))
        put('services', service('dual', 'default', 'ClusterIP', [('http', 80, 'TCP')], '10.96.10.4', extra={'ipFamilyPolicy': 'PreferDualStack', 'ipFamilies': ['IPv4', 'IPv6']}))
        put('services', service('node-exporter', 'monitoring', 'ClusterIP', [('metrics', 9100, 'TCP')], 'None', {'app': 'node-exporter'}))
        put('services', service('kube-dns', 'kube-system', 'ClusterIP', [('dns', 53, 'UDP'), ('metrics', 9153, 'TCP')], '10.96.0.10', {'k8s-app': 'kube-dns'}))

        def ref(kind, ns, name):
            return {'kind': kind, 'namespace': ns, 'name': name, 'uid': 'uid-' + name}
        put('endpoints', {'metadata': meta('web', 'default', {'app': 'web'}, {'endpoints.kubernetes.io/last-change-trigger-time': '2026-11-18T10:00:00Z'}),
                          'subsets': [{'addresses': [{'ip': '10.244.1.5', 'nodeName': 'node-1', 'targetRef': ref('Pod', 'default', 'web-7d9f8-abcde')}],
                                       'notReadyAddresses': [{'ip': '10.244.2.7', 'nodeName': 'node-2', 'targetRef': ref('Pod', 'default', 'web-7d9f8-fghij')}],
                                       'ports': [{'name': 'http', 'port': 8080, 'protocol': 'TCP'}]}]})
        put('endpoints', {'metadata': meta('node-exporter', 'monitoring', {'app': 'node-exporter'}),
                          'subsets': [{'addresses': [{'ip': 'fd00:10:244::5', 'hostname': 'ne-1', 'nodeName': 'node-1', 'targetRef': ref('Pod', 'monitoring', 'node-exporter-1')}], 'ports': [{'name': 'metrics', 'port': 9100, 'protocol': 'TCP'}]}]})
        put('endpoints', {'metadata': meta('kubelet', 'kube-system', {'k8s-app': 'kubelet'}),
                          'subsets': [{'addresses': [{'ip': '10.0.0.1', 'targetRef': ref('Node', '', 'node-1')}, {'ip': '10.0.0.4', 'targetRef': ref('Node', '', 'node-4')}],
                                       'ports': [{'name': 'https-metrics', 'port': 10250, 'protocol': 'TCP'}, {'name': 'cadvisor', 'port': 4194, 'protocol': 'TCP'}]}]})
        put('endpoints', {'metadata': meta('external', 'default'), 'subsets': [{'addresses': [{'ip': '192.168.5.5'}], 'ports': [{'port': 3306, 'protocol': 'TCP'}]}]})

        put('endpointslices', {'metadata': meta('web-x8k2p', 'default', {'kubernetes.io/service-name': 'web', 'endpointslice.kubernetes.io/managed-by': 'endpointslice-controller.k8s.io'}),
                               'addressType': 'IPv4',
                               'endpoints': [{'addresses': ['10.244.1.5'], 'conditions': {'ready': True, 'serving': True, 'terminating': False}, 'nodeName': 'node-1', 'zone': 'eu-west-1a', 'targetRef': ref('Pod', 'default', 'web-7d9f8-abcde')},
                                             {'addresses': ['10.244.2.7'], 'conditions': {'ready': False, 'serving': False, 'terminating': True}, 'nodeName': 'node-2', 'hostname': 'web-2', 'targetRef': ref('Pod', 'default', 'web-7d9f8-fghij'), 'deprecatedTopology': {'topology.kubernetes.io/zone': 'eu-west-1b'}}],
                               'ports': [{'name': 'http', 'port': 8080, 'protocol': 'TCP', 'appProtocol': 'http'}, {'name': 'metrics', 'port': 9090, 'protocol': 'TCP'}]})
        put('endpointslices', {'metadata': meta('dual-v4', 'default', {'kubernetes.io/service-name': 'dual'}), 'addressType': 'IPv4',
                               'endpoints': [{'addresses': ['10.244.4.3'], 'conditions': {'ready': True}, 'targetRef': ref('Pod', 'default', 'orphan-rs-1'), 'nodeName': 'node-4'}], 'ports': [{'name': 'http', 'port': 80, 'protocol': 'TCP'}]})
        put('endpointslices', {'metadata': meta('dual-v6', 'default', {'kubernetes.io/service-name': 'dual'}), 'addressType': 'IPv6',
                               'endpoints': [{'addresses': ['fd00:10:244::9'], 'conditions': {'ready': True}}], 'ports': [{'name': 'http', 'port': 80, 'protocol': 'TCP'}]})
        put('endpointslices', {'metadata': meta('node-exporter-abc', 'monitoring', {'kubernetes.io/service-name': 'node-exporter'}), 'addressType': 'IPv6',
                               'endpoints': [{'addresses': ['fd00:10:244::5'], 'conditions': {'ready': True}, 'nodeName': 'node-1', 'targetRef': ref('Pod', 'monitoring', 'node-exporter-1')}],
                               'ports': [{'name': 'metrics', 'port': 9100, 'protocol': 'TCP'}]})
        put('endpointslices', {'metadata': meta('kubelet-1', 'kube-system', {'kubernetes.io/service-name': 'kubelet'}), 'addressType': 'IPv4',
                               'endpoints': [{'addresses': ['10.0.0.1'], 'targetRef': ref('Node', '', 'node-1')}], 'ports': [{'name': 'https-metrics', 'port': 10250, 'protocol': 'TCP'}, {}]})

        put('ingresses', {'metadata': meta('web', 'default', {'app': 'web'}, {'nginx.ingress.kubernetes.io/rewrite-target': '/'}),
                          'spec': {'ingressClassName': 'nginx', 'tls': [{'hosts': ['*.example.com'], 'secretName': 'tls'}],
                                   'rules': [{'host': 'a.example.com', 'http': {'paths': [{'path': '/', 'pathType': 'Prefix', 'backend': {}}, {'path': '/api', 'pathType': 'Prefix', 'backend': {}}]}},
                                             {'host': 'b.other.org'}, {'host': 'x.y.example.com', 'http': {'paths': [{'pathType': 'Prefix', 'backend': {}}]}}]}})
        put('ingresses', {'metadata': meta('grafana', 'monitoring'), 'spec': {'rules': [{'host': 'grafana.local', 'http': {'paths': []}}]}})


def mutate():
    """The changes applied after the initial sync, 0.5 s apart (so that every client has seen one change before
    the next; the endpoints roles read pods from another informer, and the order of two informers is a race in
    Go as well)."""
    def step(f):
        with LOCK:
            f()
        time.sleep(0.5)

    app = container('app', 'registry.example.com/web:1.2.4', [('http', 8080, 'TCP'), ('metrics', 9090, 'TCP')])
    step(lambda: put('pods', pod('web-7d9f8-new', 'default', 'node-4', '10.244.4.9', [app], None, ('ReplicaSet', 'web-7d9f8'), {'app': 'web'})))

    def failed():
        p = copy.deepcopy(STORE['pods'][('default', 'web-7d9f8-fghij')])
        p['status']['phase'] = 'Failed'
        put('pods', p)
    step(failed)
    step(lambda: delete('pods', 'default', 'nightly-29001-xyz'))

    def svc():
        s = copy.deepcopy(STORE['services'][('default', 'web')])
        s['metadata']['labels']['version'] = 'v2'
        put('services', s)
    step(svc)
    step(lambda: delete('endpointslices', 'monitoring', 'node-exporter-abc'))
    step(lambda: delete('endpoints', 'default', 'external'))

    def ns():
        n = copy.deepcopy(STORE['namespaces'][('', 'default')])
        n['metadata']['labels']['team'] = 'platform'
        put('namespaces', n)
    step(ns)

    def nd():
        n = copy.deepcopy(STORE['nodes'][('', 'node-1')])
        n['metadata']['labels']['tier'] = 'platinum'
        put('nodes', n)
    step(nd)

    def ing():
        i = copy.deepcopy(STORE['ingresses'][('default', 'web')])
        i['spec']['rules'].append({'host': 'c.example.com', 'http': {'paths': [{'path': '/c'}]}})
        put('ingresses', i)
    step(ing)
    step(lambda: delete('nodes', '', 'node-2'))


def parse_selector(s):
    reqs = []
    for part in [p.strip() for p in (s or '').split(',') if p.strip()]:
        m = re.match(r'^([^!=]+?)\s*(==|=|!=)\s*(.*)$', part)
        if m:
            reqs.append((m.group(1), m.group(2), m.group(3)))
        elif part.startswith('!'):
            reqs.append((part[1:], '!', None))
        else:
            reqs.append((part, '?', None))
    return reqs


def matches(obj, reqs):
    ls = obj['metadata'].get('labels', {})
    for k, op, v in reqs:
        if op in ('=', '==') and ls.get(k) != v:
            return False
        if op == '!=' and ls.get(k) == v:
            return False
        if op == '!' and k in ls:
            return False
        if op == '?' and k not in ls:
            return False
    return True


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, fmt, *args):
        pass

    def log(self, what):
        if LOG:
            with open(LOG, 'a') as f:
                f.write(json.dumps({'t': round(time.time(), 3), 'what': what, 'path': self.path, 'ua': self.headers.get('User-Agent'),
                                    'accept': self.headers.get('Accept'), 'auth': self.headers.get('Authorization', '')[:12]}) + '\n')

    def send_json(self, code, obj):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def chunk(self, obj):
        b = (json.dumps(obj) + '\n').encode()
        self.wfile.write(b'%x\r\n%s\r\n' % (len(b), b))
        self.wfile.flush()

    def do_GET(self):
        u = urlparse(self.path)
        q = {k: v[-1] for k, v in parse_qs(u.query, keep_blank_values=True).items()}
        m = re.match(r'^(/api/v1|/apis/[^/]+/v1)(?:/namespaces/([^/]+))?/([a-z]+)$', u.path)
        if not m or m.group(3) not in GROUPS or GROUPS[m.group(3)] != m.group(1):
            self.log('404')
            return self.send_json(404, {'kind': 'Status', 'apiVersion': 'v1', 'status': 'Failure', 'message': 'the server could not find the requested resource', 'reason': 'NotFound', 'code': 404})
        ns, res = m.group(2) or '', m.group(3)
        reqs = parse_selector(q.get('labelSelector'))
        if q.get('watch') in ('true', '1'):
            return self.watch(res, ns, reqs, q)
        self.log('list')
        with LOCK:
            items = [o for (ons, _), o in sorted(STORE.get(res, {}).items()) if (not ns or ons == ns) and matches(o, reqs)]
            kind, api = KINDS[res]
            self.send_json(200, {'kind': kind + 'List', 'apiVersion': api, 'metadata': {'resourceVersion': str(RV[0])}, 'items': items})

    def watch(self, res, ns, reqs, q):
        initial = q.get('sendInitialEvents') == 'true'
        self.log('watchlist' if initial else 'watch')
        timeout = int(q.get('timeoutSeconds') or 300)
        bookmarks = q.get('allowWatchBookmarks') == 'true'
        kind, api = KINDS[res]
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Transfer-Encoding', 'chunked')
        self.end_headers()
        end = time.time() + timeout
        try:
            with LOCK:
                rv_q = q.get('resourceVersion', '')
                if initial or rv_q in ('', '0'):
                    for (ons, _), o in sorted(STORE.get(res, {}).items()):
                        if (not ns or ons == ns) and matches(o, reqs):
                            self.chunk({'type': 'ADDED', 'object': o})
                    pos = RV[0]
                    if initial:
                        self.chunk({'type': 'BOOKMARK', 'object': {'kind': kind, 'apiVersion': api, 'metadata': {'resourceVersion': str(pos), 'annotations': {'k8s.io/initial-events-end': 'true'}}}})
                else:
                    pos = int(rv_q)
            last_bm = time.time()
            while time.time() < end:
                with LOCK:
                    evs = [e for e in EVENTS if e[0] > pos and e[1] == res and (not ns or e[2] == ns)]
                    if not evs:
                        LOCK.wait(timeout=1.0)
                        evs = [e for e in EVENTS if e[0] > pos and e[1] == res and (not ns or e[2] == ns)]
                    if EVENTS:
                        pos = max(pos, max(e[0] for e in EVENTS if e[1] == res) if any(e[1] == res for e in EVENTS) else pos)
                for rv, _, _, typ, o, prev in evs:
                    # as the API server: a change that makes an object (not) match the selector is ADDED (DELETED)
                    now, before = matches(o, reqs), prev is not None and matches(prev, reqs)
                    if typ == 'MODIFIED' and now != before:
                        typ = 'ADDED' if now else 'DELETED'
                        if not now:
                            now = True
                    if now:
                        self.chunk({'type': typ, 'object': o})
                if bookmarks and time.time() - last_bm > 5:
                    self.chunk({'type': 'BOOKMARK', 'object': {'kind': kind, 'apiVersion': api, 'metadata': {'resourceVersion': str(pos)}}})
                    last_bm = time.time()
            self.wfile.write(b'0\r\n\r\n')
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass


def main():
    global LOG
    ap = argparse.ArgumentParser()
    ap.add_argument('--port', type=int, default=9800)
    ap.add_argument('--mutate-after', type=float, default=0)
    ap.add_argument('--log')
    a = ap.parse_args()
    LOG = a.log
    objects()
    if a.mutate_after > 0:
        threading.Timer(a.mutate_after, mutate).start()
    ThreadingHTTPServer.request_queue_size = 256
    srv = ThreadingHTTPServer(('127.0.0.1', a.port), Handler)
    srv.daemon_threads = True
    print(f'fake k8s api on 127.0.0.1:{a.port}', flush=True)
    srv.serve_forever()


if __name__ == '__main__':
    main()
