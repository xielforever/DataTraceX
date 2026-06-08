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
      grid-template-columns: 340px minmax(0, 1fr) 380px;
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
      padding: 14px;
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
      grid-template-rows: 48px minmax(0, 1fr) 188px;
      background: rgba(255,255,255,.38);
    }
    .flowbar {
      display: grid;
      grid-template-columns: 1fr auto 1fr;
      align-items: center;
      gap: 10px;
      border-bottom: 2px solid var(--line);
      background: rgba(255,255,255,.72);
      padding: 0 14px;
      font-size: 12px;
      font-weight: 900;
      text-transform: uppercase;
    }
    .flowbar span:nth-child(1) { text-align: left; color: var(--read); }
    .flowbar span:nth-child(2) { text-align: center; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .flowbar span:nth-child(3) { text-align: right; color: var(--write); }
    .canvas {
      position: relative;
      min-height: 0;
      overflow: hidden;
    }
    #graph {
      width: 100%;
      height: 100%;
      display: block;
    }
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
    }
    .pill {
      border: 2px solid var(--line);
      background: var(--panel);
      padding: 4px 8px;
    }
    .detail {
      border-top: 2px solid var(--line);
      background: var(--ink);
      color: var(--paper);
      padding: 12px;
      overflow: auto;
      font-family: "Cascadia Mono", Consolas, monospace;
      font-size: 12px;
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
    .node text { font-size: 11px; pointer-events: none; paint-order: stroke; stroke: rgba(255,255,255,.88); stroke-width: 4px; stroke-linejoin: round; }
    .node circle { stroke: var(--line); stroke-width: 2; }
    .link { fill: none; stroke: var(--line); stroke-width: 1.6; opacity: .64; }
    .link.reads { stroke: var(--read); marker-end: url(#arrow-read); }
    .link.writes { stroke: var(--write); marker-end: url(#arrow-write); }
    .link.derives_from { stroke: var(--derive); marker-end: url(#arrow-derive); }
    .link.uses_code { stroke: var(--code); stroke-dasharray: 6 5; }
    .link.contains, .link.depends_on, .link.uses_connection, .link.executes_on { stroke: var(--design); stroke-dasharray: 4 5; opacity: .42; }
    .link-label {
      font-size: 10px;
      font-weight: 900;
      fill: var(--ink);
      paint-order: stroke;
      stroke: rgba(255,255,255,.82);
      stroke-width: 3px;
      stroke-linejoin: round;
    }
    @media (max-width: 1100px) {
      .shell { grid-template-columns: 280px minmax(0, 1fr); }
      .right { display: none; }
      header { grid-column: 1 / 3; }
    }
    @media (max-width: 760px) {
      .shell { grid-template-columns: 1fr; grid-template-rows: 58px 220px minmax(0, 1fr); }
      header { grid-column: 1; }
      aside { border-right: 0; border-bottom: 2px solid var(--line); }
      main { min-height: 0; }
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
      <div class="flowbar"><span>Upstream</span><span id="rootName">No root selected</span><span>Downstream</span></div>
      <div class="canvas">
        <div class="legend">
          <span class="pill">READS</span><span class="pill">WRITES</span><span class="pill">DEPENDS</span><span class="pill">REVIEW</span>
        </div>
        <svg id="graph"></svg>
      </div>
      <pre class="detail" id="detail">{}</pre>
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
      $('detail').textContent = JSON.stringify({root: graph.root, nodes: graph.nodes.length, links: graph.links.length}, null, 2);
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
      const nodes = graph.nodes.map((n) => ({...n, x: width/2, y: height/2, layer: null}));
      const byUrn = Object.fromEntries(nodes.map(n => [n.urn, n]));
      const links = graph.links
        .filter(l => byUrn[l.source] && byUrn[l.target])
        .map(l => ({...l, ...flowEndpoints(l)}));
      layoutFlow(nodes, links, byUrn, graph.root, width, height);
      const defs = `
        <defs>
          <marker id="arrow-read" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth"><path d="M0,0 L0,6 L9,3 z" fill="var(--read)"></path></marker>
          <marker id="arrow-write" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth"><path d="M0,0 L0,6 L9,3 z" fill="var(--write)"></path></marker>
          <marker id="arrow-derive" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth"><path d="M0,0 L0,6 L9,3 z" fill="var(--derive)"></path></marker>
        </defs>`;
      const linkMarkup = links.map(l => {
        const s = byUrn[l.flowSource], t = byUrn[l.flowTarget];
        const midX = (s.x + t.x) / 2;
        const midY = (s.y + t.y) / 2;
        const dx = t.x - s.x;
        const curve = Math.max(-80, Math.min(80, dx * 0.18));
        const path = `M${s.x},${s.y} C${s.x + curve},${s.y} ${t.x - curve},${t.y} ${t.x},${t.y}`;
        return `<g>
          <path class="link ${String(l.type || '').toLowerCase()}" d="${path}"><title>${escapeHtml(l.type)} ${l.confidence ?? ''}</title></path>
          ${isDataFlow(l) && links.length <= 140 ? `<text class="link-label" x="${midX}" y="${midY - 6}">${escapeHtml(String(l.type || '').toUpperCase())}</text>` : ''}
        </g>`;
      }).join('');
      const nodeMarkup = nodes.map(n => {
        const fill = colorFor(n.kind);
        const label = shortLabel(n.name || n.urn, 34);
        const showLabel = nodes.length <= 120 || n.urn === graph.root;
        return `<g class="node" transform="translate(${n.x},${n.y})" data-urn="${encodeURIComponent(n.urn)}">
          <title>${escapeHtml(n.urn)}</title>
          <circle r="${n.urn === graph.root ? 14 : (nodes.length > 240 ? 6 : 9)}" fill="${n.urn === graph.root ? 'var(--root)' : fill}"></circle>
          ${showLabel ? `<text x="13" y="4">${escapeHtml(label)}</text>` : ''}
        </g>`;
      }).join('');
      svg.innerHTML = defs + linkMarkup + nodeMarkup;
      [...svg.querySelectorAll('.node')].forEach(el => {
        el.onclick = () => {
          const urn = decodeURIComponent(el.dataset.urn);
          const node = byUrn[urn];
          const connected = links.filter(l => l.source === urn || l.target === urn || l.flowSource === urn || l.flowTarget === urn);
          $('detail').textContent = JSON.stringify({node, connected}, null, 2);
        };
        el.ondblclick = () => loadLineage(decodeURIComponent(el.dataset.urn));
      });
    }

    function flowEndpoints(link) {
      const type = String(link.type || '').toLowerCase();
      if (type === 'reads') return {flowSource: link.target, flowTarget: link.source};
      return {flowSource: link.source, flowTarget: link.target};
    }
    function isDataFlow(link) {
      return ['reads', 'writes', 'derives_from'].includes(String(link.type || '').toLowerCase());
    }
    function layoutFlow(nodes, links, byUrn, root, width, height) {
      if (!nodes.length) return;
      const rootNode = byUrn[root] || nodes[0];
      rootNode.layer = 0;
      for (let pass = 0; pass < nodes.length + 2; pass++) {
        let changed = false;
        for (const link of links.filter(isDataFlow)) {
          const s = byUrn[link.flowSource], t = byUrn[link.flowTarget];
          if (!s || !t) continue;
          if (s.layer !== null && t.layer === null) { t.layer = s.layer + 1; changed = true; }
          if (t.layer !== null && s.layer === null) { s.layer = t.layer - 1; changed = true; }
        }
        if (!changed) break;
      }
      const context = nodes.filter(n => n.layer === null);
      context.forEach((node, i) => { node.layer = i % 2 === 0 ? -1 : 1; node.context = true; });
      const minLayer = Math.min(...nodes.map(n => n.layer));
      const maxLayer = Math.max(...nodes.map(n => n.layer));
      const span = Math.max(1, maxLayer - minLayer);
      const grouped = new Map();
      for (const node of nodes) {
        if (!grouped.has(node.layer)) grouped.set(node.layer, []);
        grouped.get(node.layer).push(node);
      }
      for (const [layer, group] of grouped.entries()) {
        group.sort((a, b) => String(a.kind).localeCompare(String(b.kind)) || String(a.name || a.urn).localeCompare(String(b.name || b.urn)));
        const x = 80 + ((layer - minLayer) / span) * Math.max(1, width - 170);
        const available = Math.max(120, height - 130);
        const top = 78;
        const maxPerColumn = Math.max(8, Math.floor(available / (nodes.length > 240 ? 13 : 22)));
        const columns = Math.max(1, Math.ceil(group.length / maxPerColumn));
        const columnGap = nodes.length > 240 ? 14 : 26;
        const visibleRows = Math.ceil(group.length / columns);
        const step = available / Math.max(visibleRows, 1);
        group.forEach((node, i) => {
          const column = Math.floor(i / maxPerColumn);
          const row = i % maxPerColumn;
          node.x = x + (column - (columns - 1) / 2) * columnGap;
          node.y = top + step * (row + 0.5);
          if (node.urn === root) {
            node.x = width / 2;
            node.y = height / 2;
          }
        });
      }
    }
    function colorFor(kind) {
      return {job:'#ffd447', node:'#98d7d9', dataset:'#c8e36e', path_prefix:'#ffac8b', connection:'#c6b7f2', code_artifact:'#80d6aa', workspace:'#ffffff'}[kind] || '#dfe4e0';
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
    function loadInitialFromPath() {
      const prefix = '/lineage/nodes/';
      if (location.pathname.startsWith(prefix)) {
        const urn = decodeURIComponent(location.pathname.slice(prefix.length));
        $('q').value = urn;
        loadLineage(urn);
      }
    }
    loadStats(); loadCandidates(); loadInitialFromPath();
  </script>
</body>
</html>"""
