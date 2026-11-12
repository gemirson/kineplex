#!/usr/bin/env python3
"""
KinePlex Cluster Manager UI Server
Inicia um servidor web local para o front-end e fornece proxy reverso para o cluster Rust na porta 8080.
"""

import http.server
import socketserver
import urllib.request
import urllib.error
import os
import sys

PORT = 3000
BACKEND_URL = os.environ.get("KINEPLEX_BACKEND_URL", "http://localhost:8080")
DIRECTORY = os.path.dirname(os.path.abspath(__file__))

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, DELETE')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.send_header('Cache-Control', 'no-store, must-revalidate')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        if self.path.startswith('/api/'):
            self.proxy_to_backend('GET')
        else:
            super().do_GET()

    def do_POST(self):
        if self.path.startswith('/api/'):
            self.proxy_to_backend('POST')
        else:
            self.send_error(404, "Not Found")

    def proxy_to_backend(self, method):
        target_url = f"{BACKEND_URL}{self.path}"
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length) if content_length > 0 else None

        req = urllib.request.Request(target_url, data=body, method=method)
        for key, val in self.headers.items():
            if key.lower() not in ['host', 'content-length']:
                req.add_header(key, val)

        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                self.send_response(response.status)
                for header, val in response.getheaders():
                    if header.lower() not in ['access-control-allow-origin', 'transfer-encoding']:
                        self.send_header(header, val)
                self.end_headers()
                self.wfile.write(response.read())
        except urllib.error.HTTPError as e:
            self.send_response(e.code)
            self.end_headers()
            self.wfile.write(e.read())
        except Exception as e:
            self.send_response(502)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            error_msg = f'{{"error": "Backend offline ou inalcançável em {BACKEND_URL}", "detail": "{str(e)}"}}'
            self.wfile.write(error_msg.encode('utf-8'))

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else PORT
    os.chdir(DIRECTORY)
    socketserver.TCPServer.allow_reuse_address = True
    
    with socketserver.TCPServer(("", port), Handler) as httpd:
        print(f"\n=======================================================")
        print(f"  🚀 KINEPLEX CLUSTER MANAGEMENT UI ATIVO")
        print(f"  🌐 URL Local: http://localhost:{port}")
        print(f"  🔗 Proxy Reverso para Daemon Rust: {BACKEND_URL}")
        print(f"=======================================================\n")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServidor encerrado com sucesso.")
