from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .store import LineageStore


class DataTraceXHandler(BaseHTTPRequestHandler):
    store: LineageStore

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/health":
            self._json({"status": "ok", "stats": self.store.stats()})
            return

        if path.startswith("/lineage/nodes/"):
            urn = unquote(path.removeprefix("/lineage/nodes/"))
            query = parse_qs(parsed.query)
            direction = query.get("direction", ["both"])[0]
            self._json(self.store.lineage_for_node(urn, direction=direction))
            return

        if path.startswith("/lineage/runs/"):
            run_id = unquote(path.removeprefix("/lineage/runs/"))
            self._json(self.store.run_detail(run_id))
            return

        if path == "/search":
            query = parse_qs(parsed.query)
            uri = query.get("uri", [""])[0]
            self._json(self.store.search_uri(uri))
            return

        self._json({"error": "not found", "path": path}, status=404)

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def serve(store: LineageStore, host: str = "127.0.0.1", port: int = 8765) -> ThreadingHTTPServer:
    class Handler(DataTraceXHandler):
        pass

    Handler.store = store
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"DataTraceX listening on http://{host}:{port}")
    server.serve_forever()
    return server
