from __future__ import annotations

import json
import os
from datetime import datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import psycopg
from neo4j import GraphDatabase
from psycopg.rows import dict_row

from .review.api import ReviewQueueService
from .settings import AppSettings


class LineageWebApp:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.neo4j_driver = GraphDatabase.driver(
            settings.neo4j.uri,
            auth=(settings.neo4j.user, settings.neo4j.password),
        )
        self.review_service = ReviewQueueService(settings.postgres.dsn)

    def close(self) -> None:
        self.neo4j_driver.close()

    def stats(self) -> dict[str, Any]:
        with psycopg.connect(self.settings.postgres.dsn, row_factory=dict_row) as conn:
            return {
                "entities": _scalar(conn, "SELECT COUNT(*) FROM entity"),
                "edges": _scalar(conn, "SELECT COUNT(*) FROM lineage_edge"),
                "evidence": _scalar(conn, "SELECT COUNT(*) FROM evidence"),
                "runs": _scalar(conn, "SELECT COUNT(*) FROM run"),
                "candidates": {
                    row["status"]: row["count"]
                    for row in conn.execute(
                        "SELECT status, COUNT(*) AS count FROM lineage_candidate GROUP BY status"
                    ).fetchall()
                },
                "edge_kinds": {
                    row["kind"]: row["count"]
                    for row in conn.execute(
                        "SELECT kind, COUNT(*) AS count FROM lineage_edge GROUP BY kind ORDER BY count DESC"
                    ).fetchall()
                },
            }

    def search(self, query: str, limit: int = 50) -> list[dict[str, Any]]:
        like = f"%{query}%"
        with psycopg.connect(self.settings.postgres.dsn, row_factory=dict_row) as conn:
            rows = conn.execute(
                """
                SELECT urn, kind, source_system, name, qualified_name
                FROM entity
                WHERE urn ILIKE %s OR name ILIKE %s OR COALESCE(qualified_name, '') ILIKE %s
                ORDER BY kind, name
                LIMIT %s
                """,
                (like, like, like, limit),
            ).fetchall()
        return [_jsonable(dict(row)) for row in rows]

    def lineage(self, urn: str, depth: int = 2, path_limit: int = 600) -> dict[str, Any]:
        depth = max(1, min(depth, 5))
        query = f"""
        MATCH (root:Entity {{urn: $urn}})
        CALL {{
          WITH root
          MATCH p=(root)-[*1..{depth}]-(n:Entity)
          WITH p LIMIT $path_limit
          UNWIND nodes(p) AS node
          RETURN collect(DISTINCT node {{
            .urn, .kind, .source_system, .external_id, .qualified_name, .name
          }}) AS nodes
        }}
        CALL {{
          WITH root
          MATCH p=(root)-[*1..{depth}]-(n:Entity)
          WITH p LIMIT $path_limit
          UNWIND relationships(p) AS rel
          RETURN collect(DISTINCT {{
            edge_id: rel.edge_id,
            source: startNode(rel).urn,
            target: endNode(rel).urn,
            type: type(rel),
            confidence: rel.confidence,
            edge_scope: rel.edge_scope,
            source_system: rel.source_system,
            attrs_json: rel.attrs_json
          }}) AS links
        }}
        RETURN nodes, links
        """
        with self.neo4j_driver.session() as session:
            record = session.run(query, urn=urn, path_limit=path_limit).single()
        if not record:
            return {"root": urn, "nodes": [], "links": []}
        nodes = record["nodes"]
        links = record["links"]
        if not any(node.get("urn") == urn for node in nodes):
            nodes.append({"urn": urn, "kind": "unknown", "name": urn, "source_system": "unknown"})
        return {"root": urn, "nodes": nodes, "links": links}

    def candidates(self, status: str = "pending", limit: int = 50) -> list[dict[str, Any]]:
        return [_jsonable(row) for row in self.review_service.list_candidates(status=status, limit=limit)]

    def candidate_detail(self, candidate_id: str) -> dict[str, Any]:
        return _jsonable(self.review_service.candidate_detail(candidate_id))

    def edit_candidate(self, candidate_id: str, payload: dict[str, Any], reviewer: str = "web") -> dict[str, Any]:
        return _jsonable(self.review_service.edit_candidate(candidate_id, payload, reviewer=reviewer))

    def accept_candidate(self, candidate_id: str, reviewer: str = "web") -> dict[str, Any]:
        return self.review_service.accept_candidate(candidate_id, reviewer=reviewer, comment="accepted from web")

    def reject_candidate(self, candidate_id: str, reviewer: str = "web") -> dict[str, Any]:
        return self.review_service.reject_candidate(candidate_id, reviewer=reviewer, comment="rejected from web")


class WebHandler(BaseHTTPRequestHandler):
    app: LineageWebApp

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/" or parsed.path.startswith("/lineage/nodes/"):
            self._html(INDEX_HTML)
            return
        if parsed.path == "/api/stats":
            self._json(self.app.stats())
            return
        if parsed.path == "/api/search":
            qs = parse_qs(parsed.query)
            self._json(self.app.search(qs.get("q", [""])[0], limit=int(qs.get("limit", ["50"])[0])))
            return
        if parsed.path == "/api/lineage":
            qs = parse_qs(parsed.query)
            urn = qs.get("urn", [""])[0]
            if not urn:
                self._json({"error": "urn required"}, status=400)
                return
            self._json(self.app.lineage(urn, depth=int(qs.get("depth", ["2"])[0])))
            return
        if parsed.path == "/api/candidates":
            qs = parse_qs(parsed.query)
            self._json(self.app.candidates(qs.get("status", ["pending"])[0]))
            return
        parts = [unquote(part) for part in parsed.path.strip("/").split("/")]
        if len(parts) == 3 and parts[:2] == ["api", "candidates"]:
            try:
                self._json(self.app.candidate_detail(parts[2]))
            except KeyError:
                self._json({"error": "candidate not found"}, status=404)
            return
        self._json({"error": "not found"}, status=404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        parts = [unquote(part) for part in parsed.path.strip("/").split("/")]
        if len(parts) == 4 and parts[:2] == ["api", "candidates"] and parts[3] in {"accept", "reject", "edit"}:
            candidate_id = parts[2]
            try:
                if parts[3] == "accept":
                    self._json(self.app.accept_candidate(candidate_id))
                elif parts[3] == "reject":
                    self._json(self.app.reject_candidate(candidate_id))
                else:
                    self._json(self.app.edit_candidate(candidate_id, self._read_json()))
            except KeyError:
                self._json({"error": "candidate not found"}, status=404)
            except ValueError as exc:
                self._json({"error": str(exc)}, status=400)
            return
        self._json({"error": "not found"}, status=404)

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(_jsonable(payload), ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html(self, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))


def serve_web(settings: AppSettings, host: str = "127.0.0.1", port: int = 8787) -> None:
    app = LineageWebApp(settings)

    class Handler(WebHandler):
        pass

    Handler.app = app
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"DataTraceX web listening on http://{host}:{port}")
    try:
        server.serve_forever()
    finally:
        app.close()


def _scalar(conn: psycopg.Connection, query: str) -> int:
    row = conn.execute(query).fetchone()
    if isinstance(row, dict):
        return int(next(iter(row.values())))
    return int(row[0])


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    return value


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>DataTraceX</title>
  <style>
    :root {
      --ink: #111412;
      --paper: #f5f8f6;
      --panel: #ffffff;
      --rail: #e2e8e4;
      --line: #171b18;
      --muted: #64706a;
      --read: #008c95;
      --write: #d54836;
      --derive: #6a57a8;
      --design: #8f7a26;
      --code: #26835d;
      --root: #ffd447;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      background:
        linear-gradient(90deg, rgba(17,20,18,.05) 1px, transparent 1px) 0 0/28px 28px,
        linear-gradient(rgba(17,20,18,.04) 1px, transparent 1px) 0 0/28px 28px,
        var(--paper);
      font-family: "Bahnschrift", "Segoe UI", sans-serif;
      letter-spacing: 0;
    }
    .shell {
      display: grid;
      grid-template-columns: 300px minmax(0, 1fr) 320px;
      grid-template-rows: 58px minmax(0, 1fr);
      height: 100vh;
    }
    header {
      grid-column: 1 / 4;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 16px;
      border-bottom: 2px solid var(--line);
      background: var(--root);
    }
    h1 {
      margin: 0;
      font-size: 22px;
      font-family: "Cambria", Georgia, serif;
      font-weight: 800;
    }
    .status {
      display: flex;
      gap: 14px;
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
    }
    aside, main, .right {
      min-height: 0;
      overflow: hidden;
    }
    aside {
      border-right: 2px solid var(--line);
      background: rgba(255,255,255,.92);
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
    }
    .search {
      padding: 12px;
      border-bottom: 2px solid var(--line);
    }
    input, select, button {
      font: inherit;
      border: 2px solid var(--line);
      background: var(--panel);
      color: var(--ink);
      height: 36px;
    }
    input { width: 100%; padding: 0 10px; }
    textarea {
      width: 100%;
      min-height: 58px;
      resize: vertical;
      border: 2px solid var(--line);
      background: #fbfdfb;
      color: var(--ink);
      font-family: "Cascadia Mono", Consolas, monospace;
      font-size: 11px;
      padding: 8px;
      letter-spacing: 0;
    }
    button {
      cursor: pointer;
      padding: 0 12px;
      font-weight: 800;
      box-shadow: 3px 3px 0 var(--line);
    }
    button:active { transform: translate(2px, 2px); box-shadow: 1px 1px 0 var(--line); }
    .row { display: flex; gap: 8px; margin-top: 8px; }
    .row select { width: 82px; padding-left: 6px; }
    .results, .candidates {
      overflow: auto;
      padding: 10px;
    }
    .item {
      border: 2px solid var(--line);
      background: var(--panel);
      padding: 10px;
      margin-bottom: 8px;
      cursor: pointer;
      transition: transform .12s ease, background .12s ease;
    }
    .item:hover {
      transform: translateX(3px);
      background: #eef8f3;
    }
    .item strong, .candidate strong { display: block; font-size: 13px; overflow-wrap: anywhere; }
    .meta { color: var(--muted); font-size: 12px; margin-top: 4px; overflow-wrap: anywhere; }
    main {
      display: grid;
      grid-template-rows: 82px minmax(0, 1fr) 232px;
      background: rgba(255,255,255,.38);
    }
    .flowbar {
      display: grid;
      grid-template-rows: 38px 42px;
      gap: 10px;
      border-bottom: 2px solid var(--line);
      background: rgba(255,255,255,.72);
      padding: 0 12px;
      font-size: 12px;
      font-weight: 900;
      text-transform: uppercase;
    }
    .axis-row, .toolbar-row {
      display: grid;
      align-items: center;
      gap: 12px;
      min-width: 0;
    }
    .axis-row { grid-template-columns: 1fr minmax(0, 1.4fr) 1fr; }
    .toolbar-row { grid-template-columns: auto minmax(0, 1fr) auto; }
    .axis-row span:nth-child(1) { text-align: left; color: var(--read); }
    .axis-row span:nth-child(2) { text-align: center; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .axis-row span:nth-child(3) { text-align: right; color: var(--write); }
    .segmented {
      display: inline-grid;
      grid-template-columns: 1fr 1fr;
      border: 2px solid var(--line);
      box-shadow: 3px 3px 0 var(--line);
      background: var(--panel);
      width: 210px;
    }
    .segmented button {
      border: 0;
      box-shadow: none;
      height: 30px;
      font-size: 11px;
      padding: 0 8px;
    }
    .segmented button.active { background: var(--ink); color: var(--paper); }
    .metrics {
      justify-self: center;
      max-width: 100%;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      color: var(--muted);
      font-size: 11px;
    }
    .focus-chip {
      justify-self: end;
      border: 2px solid var(--line);
      background: var(--root);
      padding: 5px 8px;
      max-width: 100%;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .canvas {
      position: relative;
      min-height: 0;
      overflow: hidden;
      background:
        linear-gradient(90deg, rgba(17,20,18,.055) 1px, transparent 1px) 0 0/32px 32px,
        linear-gradient(rgba(17,20,18,.045) 1px, transparent 1px) 0 0/32px 32px,
        rgba(250,252,250,.72);
    }
    #graph {
      width: 100%;
      height: 100%;
      display: block;
      cursor: grab;
      user-select: none;
      touch-action: none;
    }
    #graph.dragging { cursor: grabbing; }
    .legend {
      position: absolute;
      left: 12px;
      top: 12px;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      font-size: 12px;
      font-weight: 800;
      max-width: calc(100% - 24px);
      pointer-events: none;
      z-index: 2;
    }
    .pill {
      border: 2px solid var(--line);
      background: var(--panel);
      padding: 4px 8px;
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }
    .pill::before {
      content: "";
      width: 9px;
      height: 9px;
      border: 2px solid var(--line);
      background: var(--line);
    }
    .pill.read::before { background: var(--read); }
    .pill.write::before { background: var(--write); }
    .pill.derive::before { background: var(--derive); }
    .pill.context::before { background: var(--design); }
    .pill.focus::before { background: var(--root); }
    .canvas-controls {
      position: absolute;
      right: 12px;
      top: 12px;
      display: flex;
      align-items: center;
      gap: 7px;
      z-index: 3;
    }
    .canvas-controls button {
      width: 34px;
      height: 30px;
      padding: 0;
      font-size: 12px;
      box-shadow: 2px 2px 0 var(--line);
      background: var(--panel);
    }
    .canvas-controls button.wide {
      width: auto;
      padding: 0 8px;
    }
    .scale-readout {
      border: 2px solid var(--line);
      background: rgba(255,255,255,.92);
      padding: 6px 8px;
      min-width: 50px;
      text-align: center;
      font-size: 11px;
      font-weight: 900;
    }
    .detail {
      border-top: 2px solid var(--line);
      background: rgba(255,255,255,.92);
      color: var(--ink);
      overflow: hidden;
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) minmax(220px, 1.1fr);
      min-height: 0;
    }
    .trace-column {
      min-width: 0;
      overflow: auto;
      border-right: 2px solid var(--line);
      padding: 10px;
    }
    .trace-column:last-child { border-right: 0; }
    .trace-column h3 {
      margin: 0 0 8px;
      font-size: 12px;
      text-transform: uppercase;
    }
    .relation-item {
      border: 2px solid var(--line);
      background: #fff;
      margin-bottom: 8px;
      padding: 8px;
      cursor: pointer;
    }
    .relation-item.selected { background: #fff1b8; }
    .relation-item strong { display: block; font-size: 12px; overflow-wrap: anywhere; }
    .relation-item .path {
      margin-top: 4px;
      color: var(--muted);
      font-family: "Cascadia Mono", Consolas, monospace;
      font-size: 10px;
      overflow-wrap: anywhere;
    }
    .empty-note {
      border: 2px dashed var(--line);
      padding: 10px;
      color: var(--muted);
      font-size: 12px;
    }
    .selected-card {
      font-size: 12px;
      white-space: normal;
      overflow-wrap: anywhere;
    }
    .selected-title {
      font-weight: 900;
      font-size: 13px;
      margin-bottom: 8px;
    }
    .kv {
      display: grid;
      grid-template-columns: 92px minmax(0, 1fr);
      gap: 6px 8px;
      border-top: 2px solid var(--line);
      padding-top: 8px;
    }
    .kv span:nth-child(odd) {
      color: var(--muted);
      font-weight: 900;
      text-transform: uppercase;
      font-size: 10px;
    }
    .urn-line {
      margin-top: 8px;
      padding: 8px;
      border: 2px solid var(--line);
      background: #f7faf8;
      font-family: "Cascadia Mono", Consolas, monospace;
      font-size: 10px;
    }
    .right {
      border-left: 2px solid var(--line);
      background: rgba(255,255,255,.94);
      display: grid;
      grid-template-rows: 44px minmax(0, 1fr);
    }
    .right h2 {
      margin: 0;
      padding: 12px 14px;
      border-bottom: 2px solid var(--line);
      font-size: 15px;
      text-transform: uppercase;
    }
    .candidate {
      border: 2px solid var(--line);
      background: #ffffff;
      padding: 10px;
      margin-bottom: 10px;
    }
    .actions { display: flex; gap: 8px; margin-top: 8px; }
    .accept { background: #bfe3c8; }
    .reject { background: #efb7a9; }
    .inspect { background: #dceef0; }
    .edit { background: #f7df93; }
    .save { background: #d4edb4; }
    .candidate .lineage {
      margin-top: 6px;
      font-family: "Cascadia Mono", Consolas, monospace;
      color: var(--ink);
      font-size: 11px;
      overflow-wrap: anywhere;
    }
    .candidate-detail, .edit-form {
      margin-top: 10px;
      border-top: 2px solid var(--line);
      padding-top: 10px;
    }
    .snippet {
      margin: 8px 0 0;
      max-height: 220px;
      overflow: auto;
      background: #101411;
      color: #eef7ef;
      border: 2px solid var(--line);
      padding: 8px;
      font-family: "Cascadia Mono", Consolas, monospace;
      font-size: 11px;
      white-space: pre-wrap;
    }
    .edit-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      margin-top: 8px;
    }
    .edit-grid label {
      display: grid;
      gap: 4px;
      color: var(--muted);
      font-size: 11px;
      font-weight: 800;
      text-transform: uppercase;
    }
    .edit-grid label.wide { grid-column: 1 / 3; }
    .edit-grid input, .edit-grid select { height: 32px; font-size: 12px; }
    .node { cursor: pointer; }
    .node-card rect {
      stroke: var(--line);
      stroke-width: 2;
      filter: drop-shadow(3px 3px 0 rgba(17,20,18,.18));
    }
    .node-card text {
      pointer-events: none;
      letter-spacing: 0;
    }
    .node-kind {
      fill: var(--muted);
      font-size: 9px;
      font-weight: 900;
      text-transform: uppercase;
    }
    .node-name {
      fill: var(--ink);
      font-size: 12px;
      font-weight: 900;
    }
    .node-urn {
      fill: var(--muted);
      font-family: "Cascadia Mono", Consolas, monospace;
      font-size: 9px;
    }
    .link { fill: none; stroke: var(--line); stroke-width: 1.8; opacity: .72; pointer-events: none; }
    .link-hit { fill: none; stroke: transparent; stroke-width: 14; cursor: pointer; }
    .link.reads { stroke: var(--read); marker-end: url(#arrow-read); }
    .link.writes { stroke: var(--write); marker-end: url(#arrow-write); }
    .link.derives_from { stroke: var(--derive); marker-end: url(#arrow-derive); }
    .link.depends_on { stroke: var(--design); marker-end: url(#arrow-context); }
    .link.uses_code { stroke: var(--code); stroke-dasharray: 6 5; }
    .link.contains, .link.uses_connection, .link.executes_on { stroke: var(--design); stroke-dasharray: 4 5; opacity: .42; }
    .link.dimmed, .node.dimmed { opacity: .18; }
    .link.selected { stroke-width: 4; opacity: 1; }
    .dense-flow .link { stroke-width: 1.05; opacity: .34; }
    .dense-flow .link.selected { stroke-width: 4; opacity: 1; }
    .node.selected rect { stroke-width: 4; }
    .lane-band { fill: rgba(255,255,255,.58); stroke: rgba(17,20,18,.16); stroke-width: 1; }
    .lane-band.focus { fill: rgba(255,212,71,.18); }
    .lane-label { fill: rgba(17,20,18,.44); font-size: 11px; font-weight: 900; letter-spacing: 0; text-transform: uppercase; }
    .link-label {
      font-size: 10px;
      font-weight: 900;
      fill: var(--ink);
      paint-order: stroke;
      stroke: rgba(255,255,255,.82);
      stroke-width: 3px;
      stroke-linejoin: round;
    }
    @media (max-width: 1240px) {
      .shell { grid-template-columns: 300px minmax(0, 1fr); }
      .right { display: none; }
      header { grid-column: 1 / 3; }
    }
    @media (max-width: 980px) {
      .shell { grid-template-columns: 260px minmax(0, 1fr); }
      main { grid-template-rows: 120px minmax(0, 1fr) 260px; }
      .detail { grid-template-columns: 1fr 1fr; }
      .trace-column:last-child { grid-column: 1 / 3; border-top: 2px solid var(--line); }
      .toolbar-row { grid-template-columns: 1fr; gap: 4px; }
      .segmented, .focus-chip, .metrics { justify-self: start; }
    }
    @media (max-width: 760px) {
      .shell { grid-template-columns: 1fr; grid-template-rows: 58px 220px minmax(0, 1fr); }
      header { grid-column: 1; }
      aside { border-right: 0; border-bottom: 2px solid var(--line); }
      main { min-height: 0; }
      .detail { grid-template-columns: 1fr; }
      .trace-column { border-right: 0; border-bottom: 2px solid var(--line); }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <h1>DataTraceX</h1>
      <div class="status" id="stats"></div>
    </header>
    <aside>
      <div class="search">
        <input id="q" placeholder="URN / table / job / OBS path" />
        <div class="row">
          <select id="depth"><option>1</option><option selected>2</option><option>3</option><option>4</option><option>5</option></select>
          <button id="searchBtn">Search</button>
        </div>
      </div>
      <div class="results" id="results"></div>
    </aside>
    <main>
      <div class="flowbar">
        <div class="axis-row"><span>Upstream</span><span id="rootName">No root selected</span><span>Downstream</span></div>
        <div class="toolbar-row">
          <div class="segmented">
            <button class="active" data-mode="flow">Lineage Flow</button>
            <button data-mode="full">Full Graph</button>
          </div>
          <div class="metrics" id="graphMetrics"></div>
          <div class="focus-chip" id="focusChip">No focus</div>
        </div>
      </div>
      <div class="canvas">
        <div class="legend">
          <span class="pill read">READS</span><span class="pill write">WRITES</span><span class="pill derive">DERIVES</span><span class="pill context">CONTEXT</span><span class="pill focus">FOCUS</span>
        </div>
        <div class="canvas-controls">
          <button id="zoomOut" title="Zoom out">-</button>
          <span class="scale-readout" id="canvasScale">100%</span>
          <button id="zoomIn" title="Zoom in">+</button>
          <button class="wide" id="centerCanvas" title="Center the focused node">Center</button>
          <button class="wide" id="fitCanvas" title="Fit the graph">Fit</button>
        </div>
        <svg id="graph"></svg>
      </div>
      <section class="detail">
        <div class="trace-column"><h3>Inbound</h3><div id="inboundList"></div></div>
        <div class="trace-column"><h3>Outbound</h3><div id="outboundList"></div></div>
        <div class="trace-column"><h3>Selected</h3><div class="selected-card" id="selectedDetail">{}</div></div>
      </section>
    </main>
    <section class="right">
      <h2>Review Queue</h2>
      <div class="candidates" id="candidates"></div>
    </section>
  </div>
  <script>
    const $ = (id) => document.getElementById(id);
    let currentGraph = {nodes: [], links: []};
    let candidateCache = new Map();
    let viewMode = 'flow';
    let selectedNodeUrn = null;
    let selectedEdgeId = null;
    let canvasTransform = {x: 0, y: 0, k: 1};
    let canvasWorld = {width: 0, height: 0, focusX: 0, focusY: 0};
    let pendingViewport = 'center';
    let dragState = null;
    const CARD_W = 214;
    const CARD_H = 70;
    const COL_GAP = 360;
    const ROW_GAP = 96;

    async function loadStats() {
      const stats = await fetch('/api/stats').then(r => r.json());
      $('stats').innerHTML = [
        `entities ${stats.entities}`,
        `edges ${stats.edges}`,
        `evidence ${stats.evidence}`,
        `pending ${stats.candidates.pending || 0}`
      ].map(x => `<span>${x}</span>`).join('');
    }

    async function search() {
      const q = $('q').value.trim();
      const rows = await fetch(`/api/search?q=${encodeURIComponent(q)}&limit=80`).then(r => r.json());
      $('results').innerHTML = rows.map(row => `
        <div class="item" data-urn="${encodeURIComponent(row.urn)}">
          <strong>${escapeHtml(row.name || row.urn)}</strong>
          <div class="meta">${escapeHtml(row.kind)} / ${escapeHtml(row.source_system)}</div>
          <div class="meta">${escapeHtml(row.urn)}</div>
        </div>
      `).join('');
      [...document.querySelectorAll('.item')].forEach(el => {
        el.onclick = () => loadLineage(decodeURIComponent(el.dataset.urn));
      });
    }

    async function loadLineage(urn) {
      const depth = $('depth').value;
      const graph = await fetch(`/api/lineage?urn=${encodeURIComponent(urn)}&depth=${depth}`).then(r => r.json());
      currentGraph = graph;
      selectedNodeUrn = graph.root;
      selectedEdgeId = null;
      pendingViewport = 'center';
      renderGraph(graph);
    }

    async function loadCandidates() {
      const rows = await fetch('/api/candidates?status=pending').then(r => r.json());
      candidateCache = new Map(rows.map(row => [row.candidate_id, row]));
      $('candidates').innerHTML = rows.map(renderCandidateCard).join('');
      document.querySelectorAll('.inspect').forEach(btn => btn.onclick = () => inspectCandidate(btn.dataset.id));
      document.querySelectorAll('.edit').forEach(btn => btn.onclick = () => showEditForm(btn.dataset.id));
      document.querySelectorAll('.save').forEach(btn => btn.onclick = () => saveCandidateEdit(btn.dataset.id));
      document.querySelectorAll('.accept').forEach(btn => btn.onclick = () => review(btn.dataset.id, 'accept'));
      document.querySelectorAll('.reject').forEach(btn => btn.onclick = () => review(btn.dataset.id, 'reject'));
    }

    function renderCandidateCard(row) {
      return `
        <div class="candidate" id="candidate-${escapeAttr(row.candidate_id)}">
          <strong>${escapeHtml(row.proposed_kind)} ${Number(row.proposed_confidence).toFixed(2)}</strong>
          <div class="lineage">${escapeHtml(row.proposed_src_urn)} -> ${escapeHtml(row.proposed_dst_urn)}</div>
          <div class="meta">${escapeHtml(row.rationale)}</div>
          <div class="meta" title="${escapeHtml(row.node_urn || '')}">${escapeHtml(shortLabel(row.node_urn || '', 72))} ${lineRange(row)}</div>
          <div class="actions">
            <button class="inspect" data-id="${escapeAttr(row.candidate_id)}">Inspect</button>
            <button class="edit" data-id="${escapeAttr(row.candidate_id)}">Edit</button>
            <button class="accept" data-id="${escapeAttr(row.candidate_id)}">Accept</button>
            <button class="reject" data-id="${escapeAttr(row.candidate_id)}">Reject</button>
          </div>
          <div class="candidate-detail" hidden></div>
          <div class="edit-form" hidden>${renderEditForm(row)}</div>
        </div>
      `;
    }

    function renderEditForm(row) {
      const kinds = ['reads', 'writes', 'derives_from', 'uses_connection', 'executes_on', 'depends_on', 'contains'];
      const scopes = ['inferred', 'design', 'run'];
      return `
        <div class="edit-grid">
          <label class="wide">Source URN<textarea data-field="proposed_src_urn">${escapeHtml(row.proposed_src_urn)}</textarea></label>
          <label class="wide">Target URN<textarea data-field="proposed_dst_urn">${escapeHtml(row.proposed_dst_urn)}</textarea></label>
          <label>Kind<select data-field="proposed_kind">${kinds.map(kind => `<option ${kind === row.proposed_kind ? 'selected' : ''}>${kind}</option>`).join('')}</select></label>
          <label>Scope<select data-field="proposed_edge_scope">${scopes.map(scope => `<option ${scope === row.proposed_edge_scope ? 'selected' : ''}>${scope}</option>`).join('')}</select></label>
          <label>Confidence<input data-field="proposed_confidence" value="${escapeAttr(row.proposed_confidence)}" /></label>
          <label>Line Start<input data-field="line_start" value="${escapeAttr(row.line_start || '')}" /></label>
          <label>Line End<input data-field="line_end" value="${escapeAttr(row.line_end || '')}" /></label>
          <label class="wide">Rationale<textarea data-field="rationale">${escapeHtml(row.rationale)}</textarea></label>
        </div>
        <div class="actions"><button class="save" data-id="${escapeAttr(row.candidate_id)}">Save</button></div>
      `;
    }

    async function inspectCandidate(id) {
      const card = document.getElementById(`candidate-${cssSafe(id)}`);
      const panel = card?.querySelector('.candidate-detail');
      if (!panel) return;
      const detail = await fetch(`/api/candidates/${encodeURIComponent(id)}`).then(r => r.json());
      const ev = detail.evidence;
      const snippet = detail.snippet;
      panel.hidden = false;
      panel.innerHTML = `
        <div class="meta">${escapeHtml(ev?.summary || 'No source evidence linked')}</div>
        <div class="meta">${escapeHtml(ev?.source_api || '')} ${escapeHtml(ev?.payload_hash || '')}</div>
        ${snippet ? `<pre class="snippet">${escapeHtml(snippet.text)}${snippet.truncated ? '\n...' : ''}</pre>` : '<div class="meta">No script snippet available.</div>'}
      `;
    }

    function showEditForm(id) {
      const card = document.getElementById(`candidate-${cssSafe(id)}`);
      const form = card?.querySelector('.edit-form');
      if (form) form.hidden = !form.hidden;
    }

    async function saveCandidateEdit(id) {
      const card = document.getElementById(`candidate-${cssSafe(id)}`);
      if (!card) return;
      const updates = {};
      card.querySelectorAll('[data-field]').forEach(input => {
        const value = input.value;
        updates[input.dataset.field] = value === '' && input.dataset.field.startsWith('line_') ? null : value;
      });
      await fetch(`/api/candidates/${encodeURIComponent(id)}/edit`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({updates, comment: 'edited from review UI'})
      }).then(async response => {
        if (!response.ok) throw new Error((await response.json()).error || 'edit failed');
      });
      await loadCandidates();
    }

    async function review(id, action) {
      await fetch(`/api/candidates/${encodeURIComponent(id)}/${action}`, {method: 'POST'});
      await loadStats();
      await loadCandidates();
    }

    function renderGraph(graph) {
      const svg = $('graph');
      const width = svg.clientWidth || 900;
      const height = svg.clientHeight || 600;
      svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
      $('rootName').textContent = shortLabel(graph.root || 'No root selected', 72);
      const allNodes = graph.nodes.map((n) => ({...n, x: width/2, y: height/2, layer: null}));
      const allByUrn = Object.fromEntries(allNodes.map(n => [n.urn, n]));
      const allLinks = graph.links
        .filter(l => allByUrn[l.source] && allByUrn[l.target])
        .map(l => ({...l, ...flowEndpoints(l)}));
      const flowLinks = allLinks.filter(isLineageFlow);
      let viewLinks = viewMode === 'flow' ? flowLinks : allLinks;
      if (!viewLinks.length) viewLinks = allLinks;
      const focusForVisibility = selectedNodeUrn || graph.root;
      let visibleUrns = viewMode === 'flow' ? directedLineageUrns(focusForVisibility, viewLinks) : new Set();
      if (!visibleUrns.size) visibleUrns = new Set([graph.root]);
      if (viewMode !== 'flow') {
        visibleUrns.add(graph.root);
        if (selectedNodeUrn) visibleUrns.add(selectedNodeUrn);
        viewLinks.forEach(link => {
          visibleUrns.add(link.source);
          visibleUrns.add(link.target);
          visibleUrns.add(link.flowSource);
          visibleUrns.add(link.flowTarget);
        });
      }
      const nodes = allNodes
        .filter(node => visibleUrns.has(node.urn))
        .map(node => ({...node, layer: null}));
      const byUrn = Object.fromEntries(nodes.map(n => [n.urn, n]));
      const links = viewLinks.filter(l => byUrn[l.source] && byUrn[l.target]);
      if (selectedNodeUrn && !byUrn[selectedNodeUrn]) selectedNodeUrn = graph.root;
      if (selectedEdgeId && !links.some(link => edgeKey(link) === selectedEdgeId)) selectedEdgeId = null;
      const focusUrn = selectedNodeUrn || graph.root;
      const layout = layoutCanvas(nodes, links, byUrn, focusUrn, width, height);
      canvasWorld = layout.world;
      if (pendingViewport === 'fit') fitGraphToViewport(width, height);
      if (pendingViewport === 'center') centerCanvasOn(layout.world.focusX, layout.world.focusY, width, height);
      pendingViewport = null;
      $('graphMetrics').textContent = `${viewMode === 'flow' ? 'lineage flow' : 'full graph'} | ${layout.layers.length} layers | ${nodes.length}/${graph.nodes.length} nodes | ${links.length}/${allLinks.length} links`;
      $('focusChip').textContent = shortLabel(byUrn[focusUrn]?.name || focusUrn || graph.root, 46);
      const defs = `
        <defs>
          <marker id="arrow-read" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth"><path d="M0,0 L0,6 L9,3 z" fill="var(--read)"></path></marker>
          <marker id="arrow-write" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth"><path d="M0,0 L0,6 L9,3 z" fill="var(--write)"></path></marker>
          <marker id="arrow-derive" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth"><path d="M0,0 L0,6 L9,3 z" fill="var(--derive)"></path></marker>
          <marker id="arrow-context" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth"><path d="M0,0 L0,6 L9,3 z" fill="var(--design)"></path></marker>
        </defs>`;
      const laneMarkup = layout.layers.map(layer => `
        <rect class="lane-band${layer.layer === 0 ? ' focus' : ''}" x="${layer.x - CARD_W / 2 - 28}" y="28" width="${CARD_W + 56}" height="${Math.max(120, layout.world.height - 56)}"></rect>
        <text class="lane-label" x="${layer.x - CARD_W / 2 - 18}" y="52">${escapeHtml(layerTitle(layer.layer, layer.count))}</text>
      `).join('');
      const linkMarkup = links.map(l => {
        const s = byUrn[l.flowSource], t = byUrn[l.flowTarget];
        if (!s || !t) return '';
        const path = edgePath(s, t);
        const label = edgeLabelPoint(s, t);
        const key = edgeKey(l);
        const unrelatedToSelectedNode = viewMode === 'full' && selectedNodeUrn && !(l.source === selectedNodeUrn || l.target === selectedNodeUrn || l.flowSource === selectedNodeUrn || l.flowTarget === selectedNodeUrn);
        const dimmed = (selectedEdgeId && selectedEdgeId !== key) || unrelatedToSelectedNode ? ' dimmed' : '';
        const selected = selectedEdgeId === key ? ' selected' : '';
        return `<g>
          <path class="link-hit" data-edge="${escapeAttr(key)}" d="${path}"></path>
          <path class="link ${String(l.type || '').toLowerCase()}${dimmed}${selected}" data-edge="${escapeAttr(key)}" d="${path}"><title>${escapeHtml(l.type)} ${l.confidence ?? ''}</title></path>
          ${isLineageFlow(l) && links.length <= 180 ? `<text class="link-label" text-anchor="middle" x="${label.x}" y="${label.y}">${escapeHtml(edgeDisplayType(l.type))}</text>` : ''}
        </g>`;
      }).join('');
      const nodeMarkup = nodes.map(n => {
        const fill = colorFor(n.kind);
        const lines = labelLines(n.name || n.urn, 24, 2);
        const urn = shortLabel(urnTail(n.urn), 32);
        const selected = selectedNodeUrn === n.urn ? ' selected' : '';
        const connectedToSelected = selectedNodeUrn && links.some(link =>
          (link.source === selectedNodeUrn || link.target === selectedNodeUrn || link.flowSource === selectedNodeUrn || link.flowTarget === selectedNodeUrn) &&
          (link.source === n.urn || link.target === n.urn || link.flowSource === n.urn || link.flowTarget === n.urn)
        );
        const dimmed = viewMode === 'full' && selectedNodeUrn && selectedNodeUrn !== n.urn && !connectedToSelected ? ' dimmed' : '';
        return `<g class="node node-card${selected}${dimmed}" transform="translate(${n.x - CARD_W / 2},${n.y - CARD_H / 2})" data-urn="${encodeURIComponent(n.urn)}">
          <title>${escapeHtml(n.urn)}</title>
          <rect width="${CARD_W}" height="${CARD_H}" fill="${n.urn === focusUrn ? 'var(--root)' : fill}"></rect>
          <text class="node-kind" x="10" y="15">${escapeHtml(String(n.kind || 'unknown'))} / L${n.layer}</text>
          <text class="node-name" x="10" y="34">${escapeHtml(lines[0] || '')}</text>
          ${lines[1] ? `<text class="node-name" x="10" y="49">${escapeHtml(lines[1])}</text>` : ''}
          <text class="node-urn" x="10" y="64">${escapeHtml(urn)}</text>
        </g>`;
      }).join('');
      svg.innerHTML = defs + `<g id="graphViewport" class="${links.length > 120 ? 'dense-flow' : ''}">${laneMarkup}${linkMarkup}${nodeMarkup}</g>`;
      applyCanvasTransform();
      [...svg.querySelectorAll('.node')].forEach(el => {
        el.onclick = () => {
          const urn = decodeURIComponent(el.dataset.urn);
          selectedNodeUrn = urn;
          selectedEdgeId = null;
          pendingViewport = 'center';
          renderGraph(currentGraph);
        };
        el.ondblclick = () => loadLineage(decodeURIComponent(el.dataset.urn));
      });
      [...svg.querySelectorAll('.link-hit')].forEach(el => {
        el.onclick = () => {
          selectedEdgeId = el.dataset.edge;
          renderGraph(currentGraph);
        };
      });
      renderTracePanel(graph, nodes, links, byUrn);
    }

    function renderTracePanel(graph, nodes, links, byUrn) {
      const focusUrn = selectedNodeUrn || graph.root;
      const focus = byUrn[focusUrn] || graph.nodes.find(node => node.urn === focusUrn) || {urn: focusUrn, name: focusUrn, kind: 'unknown'};
      const inbound = links.filter(link => link.flowTarget === focusUrn).sort(sortByConfidence);
      const outbound = links.filter(link => link.flowSource === focusUrn).sort(sortByConfidence);
      $('inboundList').innerHTML = relationListMarkup(inbound, byUrn, 'in');
      $('outboundList').innerHTML = relationListMarkup(outbound, byUrn, 'out');

      const selectedLink = selectedEdgeId ? links.find(link => edgeKey(link) === selectedEdgeId) : null;
      if (selectedLink) {
        const attrs = parseAttrs(selectedLink.attrs_json);
        $('selectedDetail').innerHTML = `
          <div class="selected-title">${escapeHtml(String(selectedLink.type || '').toUpperCase())} ${Number(selectedLink.confidence || 0).toFixed(2)}</div>
          <div class="kv">
            <span>scope</span><span>${escapeHtml(selectedLink.edge_scope || '')}</span>
            <span>system</span><span>${escapeHtml(selectedLink.source_system || '')}</span>
            <span>model</span><span>${escapeHtml(attrs.ai_model || attrs.parser || '')}</span>
            <span>reviewer</span><span>${escapeHtml(attrs.reviewer || '')}</span>
          </div>
          <div class="urn-line">${escapeHtml(selectedLink.flowSource)}\n-> ${escapeHtml(selectedLink.flowTarget)}</div>
        `;
      } else {
        $('selectedDetail').innerHTML = `
          <div class="selected-title">${escapeHtml(focus.name || focus.urn)}</div>
          <div class="kv">
            <span>kind</span><span>${escapeHtml(focus.kind || '')}</span>
            <span>system</span><span>${escapeHtml(focus.source_system || '')}</span>
            <span>inbound</span><span>${inbound.length}</span>
            <span>outbound</span><span>${outbound.length}</span>
            <span>view</span><span>${nodes.length}/${graph.nodes.length} nodes, ${links.length}/${graph.links.length} links</span>
          </div>
          <div class="urn-line">${escapeHtml(focus.urn)}</div>
        `;
      }

      document.querySelectorAll('.relation-item').forEach(item => {
        item.onclick = () => {
          selectedEdgeId = item.dataset.edge;
          renderGraph(currentGraph);
        };
      });
    }

    function relationListMarkup(items, byUrn, direction) {
      if (!items.length) {
        return `<div class="empty-note">${direction === 'in' ? 'No inbound flow for the current focus.' : 'No outbound flow for the current focus.'}</div>`;
      }
      return items.slice(0, 40).map(link => {
        const key = edgeKey(link);
        const source = byUrn[link.flowSource] || {name: link.flowSource, kind: 'unknown'};
        const target = byUrn[link.flowTarget] || {name: link.flowTarget, kind: 'unknown'};
        const active = selectedEdgeId === key ? ' selected' : '';
        return `
          <div class="relation-item${active}" data-edge="${escapeAttr(key)}">
            <strong>${escapeHtml(shortLabel(source.name || source.urn, 30))} -> ${escapeHtml(shortLabel(target.name || target.urn, 30))}</strong>
            <div class="meta">${escapeHtml(String(link.type || '').toUpperCase())} ${Number(link.confidence || 0).toFixed(2)} / ${escapeHtml(link.source_system || '')}</div>
            <div class="path">${escapeHtml(link.flowSource)}\n${escapeHtml(link.flowTarget)}</div>
          </div>`;
      }).join('');
    }

    function flowEndpoints(link) {
      const type = String(link.type || '').toLowerCase();
      if (type === 'reads') return {flowSource: link.target, flowTarget: link.source};
      return {flowSource: link.source, flowTarget: link.target};
    }
    function isLineageFlow(link) {
      return ['reads', 'writes', 'derives_from', 'depends_on'].includes(String(link.type || '').toLowerCase());
    }
    function edgeKey(link) {
      return link.edge_id || `${link.source}|${link.target}|${link.type || ''}`;
    }
    function sortByConfidence(a, b) {
      return Number(b.confidence || 0) - Number(a.confidence || 0);
    }
    function parseAttrs(value) {
      if (!value) return {};
      try { return JSON.parse(value); } catch { return value; }
    }
    function connectedUrns(startUrn, links) {
      const adjacency = new Map();
      for (const link of links) {
        if (!adjacency.has(link.flowSource)) adjacency.set(link.flowSource, []);
        if (!adjacency.has(link.flowTarget)) adjacency.set(link.flowTarget, []);
        adjacency.get(link.flowSource).push(link.flowTarget);
        adjacency.get(link.flowTarget).push(link.flowSource);
      }
      const seen = new Set();
      const queue = [startUrn];
      while (queue.length) {
        const urn = queue.shift();
        if (!urn || seen.has(urn)) continue;
        seen.add(urn);
        for (const next of adjacency.get(urn) || []) {
          if (!seen.has(next)) queue.push(next);
        }
      }
      return seen;
    }
    function directedLineageUrns(startUrn, links) {
      const incoming = new Map();
      const outgoing = new Map();
      for (const link of links) {
        if (!incoming.has(link.flowTarget)) incoming.set(link.flowTarget, []);
        if (!outgoing.has(link.flowSource)) outgoing.set(link.flowSource, []);
        incoming.get(link.flowTarget).push(link.flowSource);
        outgoing.get(link.flowSource).push(link.flowTarget);
      }
      const seen = new Set([startUrn]);
      collectDirected(startUrn, incoming, seen);
      collectDirected(startUrn, outgoing, seen);
      if (seen.size <= 1) return connectedUrns(startUrn, links);
      return seen;
    }
    function collectDirected(startUrn, adjacency, seen) {
      const queue = [startUrn];
      while (queue.length) {
        const urn = queue.shift();
        for (const next of adjacency.get(urn) || []) {
          if (!next || seen.has(next)) continue;
          seen.add(next);
          queue.push(next);
        }
      }
    }
    function layoutCanvas(nodes, links, byUrn, focusUrn, width, height) {
      if (!nodes.length) {
        return {layers: [], world: {width, height, focusX: width / 2, focusY: height / 2}};
      }
      const focus = byUrn[focusUrn] || nodes[0];
      const incoming = new Map();
      const outgoing = new Map();
      for (const link of links.filter(isLineageFlow)) {
        if (!byUrn[link.flowSource] || !byUrn[link.flowTarget]) continue;
        if (!incoming.has(link.flowTarget)) incoming.set(link.flowTarget, []);
        if (!outgoing.has(link.flowSource)) outgoing.set(link.flowSource, []);
        incoming.get(link.flowTarget).push(link.flowSource);
        outgoing.get(link.flowSource).push(link.flowTarget);
      }
      const upstream = new Map([[focus.urn, 0]]);
      const downstream = new Map([[focus.urn, 0]]);
      walkLayerDistances(focus.urn, incoming, upstream);
      walkLayerDistances(focus.urn, outgoing, downstream);
      for (const node of nodes) {
        const up = upstream.get(node.urn);
        const down = downstream.get(node.urn);
        if (node.urn === focus.urn) node.layer = 0;
        else if (up !== undefined && (down === undefined || up <= down)) node.layer = -up;
        else if (down !== undefined) node.layer = down;
        else node.layer = inferredContextLayer(node, links, focus.urn);
      }

      const grouped = new Map();
      for (const node of nodes) {
        if (!grouped.has(node.layer)) grouped.set(node.layer, []);
        grouped.get(node.layer).push(node);
      }
      const layerKeys = [...grouped.keys()].sort((a, b) => a - b);
      const minLayer = layerKeys[0] ?? 0;
      let maxRows = 1;
      const layers = layerKeys.map(layer => {
        const group = grouped.get(layer) || [];
        maxRows = Math.max(maxRows, group.length);
        return {layer, count: group.length, x: 120 + (layer - minLayer) * COL_GAP};
      });
      const world = {
        width: Math.max(width, 240 + layerKeys.length * COL_GAP + CARD_W),
        height: Math.max(height, 120 + maxRows * ROW_GAP + CARD_H),
        focusX: 0,
        focusY: 0,
        minLayer,
        maxLayer: layerKeys[layerKeys.length - 1] ?? 0,
      };
      for (const layerInfo of layers) {
        const group = grouped.get(layerInfo.layer) || [];
        group.sort(compareNodesForCanvas);
        const groupHeight = (group.length - 1) * ROW_GAP;
        const startY = Math.max(96, (world.height - groupHeight) / 2);
        group.forEach((node, i) => {
          node.x = layerInfo.x;
          node.y = startY + i * ROW_GAP;
        });
      }
      world.focusX = focus.x || width / 2;
      world.focusY = focus.y || height / 2;
      return {layers, world};
    }
    function walkLayerDistances(start, adjacency, distances) {
      const queue = [start];
      while (queue.length) {
        const urn = queue.shift();
        const nextDistance = (distances.get(urn) || 0) + 1;
        for (const next of adjacency.get(urn) || []) {
          if (distances.has(next) && distances.get(next) <= nextDistance) continue;
          distances.set(next, nextDistance);
          queue.push(next);
        }
      }
    }
    function inferredContextLayer(node, links, focusUrn) {
      const touching = links.filter(link => link.source === node.urn || link.target === node.urn || link.flowSource === node.urn || link.flowTarget === node.urn);
      if (touching.some(link => link.flowTarget === focusUrn)) return -1;
      if (touching.some(link => link.flowSource === focusUrn)) return 1;
      if (String(node.kind || '').toLowerCase() === 'connection') return -1;
      return 1;
    }
    function compareNodesForCanvas(a, b) {
      const ak = String(a.kind || '');
      const bk = String(b.kind || '');
      if (ak !== bk) return ak.localeCompare(bk);
      return String(a.name || a.urn).localeCompare(String(b.name || b.urn));
    }
    function edgePath(source, target) {
      const leftToRight = target.x >= source.x;
      const sx = source.x + (leftToRight ? CARD_W / 2 : -CARD_W / 2);
      const tx = target.x + (leftToRight ? -CARD_W / 2 : CARD_W / 2);
      const sy = source.y;
      const ty = target.y;
      const dx = Math.max(60, Math.abs(tx - sx) * 0.42);
      const c1 = leftToRight ? sx + dx : sx - dx;
      const c2 = leftToRight ? tx - dx : tx + dx;
      return `M${sx},${sy} C${c1},${sy} ${c2},${ty} ${tx},${ty}`;
    }
    function edgeLabelPoint(source, target) {
      return {x: (source.x + target.x) / 2, y: (source.y + target.y) / 2 - 10};
    }
    function edgeDisplayType(type) {
      const value = String(type || '').toLowerCase();
      if (value === 'derives_from') return 'DERIVES';
      if (value === 'depends_on') return 'DEPENDS';
      return String(type || '').toUpperCase();
    }
    function layerTitle(layer, count) {
      if (layer < 0) return `up ${Math.abs(layer)} / ${count}`;
      if (layer > 0) return `down ${layer} / ${count}`;
      return `focus / ${count}`;
    }
    function centerCanvasOn(x, y, width, height) {
      const current = canvasTransform.k || 0.85;
      const spanLayers = Math.max(0, (canvasWorld.maxLayer || 0) - (canvasWorld.minLayer || 0));
      const fitReadableScale = spanLayers > 0
        ? Math.max(0.72, Math.min(1.05, (width - 60) / Math.max(1, spanLayers * COL_GAP + CARD_W + 80)))
        : 1;
      const nextScale = Math.min(current < 0.7 ? 0.9 : Math.max(0.7, Math.min(1.05, current)), fitReadableScale);
      let focusScreenX = width / 2;
      if ((canvasWorld.minLayer || 0) < 0 && (canvasWorld.maxLayer || 0) <= 0) {
        focusScreenX = width - (CARD_W * nextScale) / 2 - 24;
      } else if ((canvasWorld.minLayer || 0) >= 0 && (canvasWorld.maxLayer || 0) > 0) {
        focusScreenX = (CARD_W * nextScale) / 2 + 24;
      }
      canvasTransform = {k: nextScale, x: focusScreenX - x * nextScale, y: height / 2 - y * nextScale};
    }
    function fitGraphToViewport(width, height) {
      const scale = Math.max(0.08, Math.min(1.1, Math.min((width - 80) / Math.max(1, canvasWorld.width), (height - 80) / Math.max(1, canvasWorld.height))));
      canvasTransform = {k: scale, x: (width - canvasWorld.width * scale) / 2, y: (height - canvasWorld.height * scale) / 2};
    }
    function applyCanvasTransform() {
      const viewport = document.getElementById('graphViewport');
      if (viewport) viewport.setAttribute('transform', `translate(${canvasTransform.x},${canvasTransform.y}) scale(${canvasTransform.k})`);
      const scale = $('canvasScale');
      if (scale) scale.textContent = `${Math.round(canvasTransform.k * 100)}%`;
    }
    function zoomCanvas(multiplier) {
      const svg = $('graph');
      const width = svg.clientWidth || 900;
      const height = svg.clientHeight || 600;
      zoomCanvasAt(width / 2, height / 2, multiplier);
    }
    function zoomCanvasAt(screenX, screenY, multiplier) {
      const oldScale = canvasTransform.k;
      const nextScale = Math.max(0.08, Math.min(2.4, oldScale * multiplier));
      const worldX = (screenX - canvasTransform.x) / oldScale;
      const worldY = (screenY - canvasTransform.y) / oldScale;
      canvasTransform = {k: nextScale, x: screenX - worldX * nextScale, y: screenY - worldY * nextScale};
      applyCanvasTransform();
    }
    function colorFor(kind) {
      return {job:'#ffd447', node:'#98d7d9', dataset:'#c8e36e', path_prefix:'#ffac8b', connection:'#c6b7f2', code_artifact:'#80d6aa', workspace:'#ffffff'}[kind] || '#dfe4e0';
    }
    function labelLines(value, limit = 24, maxLines = 2) {
      const text = String(value || '');
      const lines = [];
      let rest = text;
      while (rest && lines.length < maxLines) {
        if (rest.length <= limit) {
          lines.push(rest);
          rest = '';
        } else {
          lines.push(rest.slice(0, limit - (lines.length === maxLines - 1 ? 3 : 0)) + (lines.length === maxLines - 1 ? '...' : ''));
          rest = rest.slice(limit);
        }
      }
      return lines.length ? lines : [''];
    }
    function urnTail(value) {
      const raw = String(value || '');
      const decoded = safeDecode(raw);
      const parts = decoded.split(/[/:]+/).filter(Boolean);
      return parts.slice(-3).join('/');
    }
    function safeDecode(value) {
      try { return decodeURIComponent(value); } catch { return value; }
    }
    function shortLabel(value, limit = 34) { return String(value).length > limit ? String(value).slice(0, limit - 3) + '...' : String(value); }
    function lineRange(row) { return row.line_start ? `line ${row.line_start}${row.line_end ? '-' + row.line_end : ''}` : ''; }
    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    }
    function escapeAttr(value) { return escapeHtml(value); }
    function cssSafe(value) { return String(value).replace(/[^a-zA-Z0-9_-]/g, '_'); }
    $('searchBtn').onclick = search;
    $('q').onkeydown = (e) => { if (e.key === 'Enter') search(); };
    document.querySelectorAll('[data-mode]').forEach(button => {
      button.onclick = () => {
        viewMode = button.dataset.mode;
        selectedEdgeId = null;
        pendingViewport = 'center';
        document.querySelectorAll('[data-mode]').forEach(item => item.classList.toggle('active', item.dataset.mode === viewMode));
        renderGraph(currentGraph);
      };
    });
    function installCanvasInteraction() {
      const svg = $('graph');
      svg.onpointerdown = (event) => {
        if (event.target.closest?.('.node, .link-hit')) return;
        dragState = {x: event.clientX, y: event.clientY, tx: canvasTransform.x, ty: canvasTransform.y};
        svg.classList.add('dragging');
        svg.setPointerCapture?.(event.pointerId);
      };
      svg.onpointermove = (event) => {
        if (!dragState) return;
        canvasTransform.x = dragState.tx + event.clientX - dragState.x;
        canvasTransform.y = dragState.ty + event.clientY - dragState.y;
        applyCanvasTransform();
      };
      svg.onpointerup = (event) => {
        dragState = null;
        svg.classList.remove('dragging');
        svg.releasePointerCapture?.(event.pointerId);
      };
      svg.onpointerleave = () => {
        dragState = null;
        svg.classList.remove('dragging');
      };
      svg.onwheel = (event) => {
        event.preventDefault();
        const rect = svg.getBoundingClientRect();
        zoomCanvasAt(event.clientX - rect.left, event.clientY - rect.top, event.deltaY < 0 ? 1.12 : 0.88);
      };
      $('zoomIn').onclick = () => zoomCanvas(1.18);
      $('zoomOut').onclick = () => zoomCanvas(0.84);
      $('centerCanvas').onclick = () => {
        const svgWidth = svg.clientWidth || 900;
        const svgHeight = svg.clientHeight || 600;
        centerCanvasOn(canvasWorld.focusX, canvasWorld.focusY, svgWidth, svgHeight);
        applyCanvasTransform();
      };
      $('fitCanvas').onclick = () => {
        const svgWidth = svg.clientWidth || 900;
        const svgHeight = svg.clientHeight || 600;
        fitGraphToViewport(svgWidth, svgHeight);
        applyCanvasTransform();
      };
    }
    function loadInitialFromPath() {
      const prefix = '/lineage/nodes/';
      if (location.pathname.startsWith(prefix)) {
        const urn = decodeURIComponent(location.pathname.slice(prefix.length));
        $('q').value = urn;
        loadLineage(urn);
      }
    }
    installCanvasInteraction();
    loadStats(); loadCandidates(); loadInitialFromPath();
  </script>
</body>
</html>"""
