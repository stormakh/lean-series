#!/usr/bin/env python3
"""A fake Alertmanager: answers every POST with 200 and appends `<path>\t<body>` lines to a file.
usage: fake_am.py <port> <out-file>"""
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

port, out = int(sys.argv[1]), sys.argv[2]


class H(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
        with open(out, 'ab') as f:
            f.write(self.path.encode() + b'\t' + self.headers.get('Content-Type', '').encode() + b'\t' + body.replace(b'\n', b' ') + b'\n')
        self.send_response(200)
        self.end_headers()

    def log_message(self, *a):
        pass


ThreadingHTTPServer(('127.0.0.1', port), H).serve_forever()
