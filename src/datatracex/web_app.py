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

from .ai.repository import LineageCandidateRepository
from .postgres_store import PostgresFactStore
from .review.materialize import CandidateMaterializer
from .settings import AppSettings


class LineageWebApp:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.neo4j_driver = GraphDatabase.driver(
            settings.neo4j.uri,
            auth=(settings.neo4j.user, settings.neo4j.password),
        )

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
        repo = LineageCandidateRepository(self.settings.postgres.dsn)
        return [_jsonable(row) for row in repo.list_candidates(status=status, limit=limit)]

    def accept_candidate(self, candidate_id: str, reviewer: str = "web") -> dict[str, Any]:
        repo = LineageCandidateRepository(self.settings.postgres.dsn)
        materializer = CandidateMaterializer(PostgresFactStore(self.settings.postgres.dsn), repo)
        edge_id = materializer.accept_and_materialize(candidate_id, reviewer, "accepted from web")
        return {"candidate_id": candidate_id, "edge_id": edge_id}

    def reject_candidate(self, candidate_id: str, reviewer: str = "web") -> dict[str, Any]:
        repo = LineageCandidateRepository(self.settings.postgres.dsn)
        repo.update_status(candidate_id, "rejected", reviewer, "rejected from web")
        return {"candidate_id": candidate_id, "status": "rejected"}


class WebHandler(BaseHTTPRequestHandler):
    app: LineageWebApp

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
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
        self._json({"error": "not found"}, status=404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        parts = [unquote(part) for part in parsed.path.strip("/").split("/")]
        if len(parts) == 4 and parts[:2] == ["api", "candidates"] and parts[3] in {"accept", "reject"}:
            candidate_id = parts[2]
            if parts[3] == "accept":
                self._json(self.app.accept_candidate(candidate_id))
            else:
                self._json(self.app.reject_candidate(candidate_id))
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
      --ink: #12100d;
      --paper: #f4f1ea;
      --panel: #fffaf0;
      --line: #24201a;
      --muted: #6d665b;
      --amber: #c88222;
      --teal: #167d7f;
      --red: #b43d2a;
      --green: #28784f;
      --blue: #315f9c;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      background:
        linear-gradient(90deg, rgba(18,16,13,.05) 1px, transparent 1px) 0 0/24px 24px,
        linear-gradient(rgba(18,16,13,.04) 1px, transparent 1px) 0 0/24px 24px,
        var(--paper);
      font-family: "Aptos", "Segoe UI", sans-serif;
      letter-spacing: 0;
    }
    .shell {
      display: grid;
      grid-template-columns: 320px minmax(0, 1fr) 360px;
      grid-template-rows: 64px minmax(0, 1fr);
      height: 100vh;
    }
    header {
      grid-column: 1 / 4;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 18px;
      border-bottom: 2px solid var(--line);
      background: #f8d36a;
    }
    h1 {
      margin: 0;
      font-size: 24px;
      font-family: Georgia, "Times New Roman", serif;
      font-weight: 700;
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
      background: rgba(255,250,240,.92);
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
    }
    .item strong, .candidate strong { display: block; font-size: 13px; overflow-wrap: anywhere; }
    .meta { color: var(--muted); font-size: 12px; margin-top: 4px; overflow-wrap: anywhere; }
    main {
      display: grid;
      grid-template-rows: minmax(0, 1fr) 176px;
      background: rgba(255,255,255,.32);
    }
    #graph {
      width: 100%;
      height: 100%;
      display: block;
    }
    .legend {
      position: absolute;
      left: 338px;
      top: 78px;
      display: flex;
      gap: 8px;
      font-size: 12px;
      font-weight: 800;
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
      background: rgba(255,250,240,.94);
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
      background: #fff6df;
      padding: 10px;
      margin-bottom: 10px;
    }
    .actions { display: flex; gap: 8px; margin-top: 8px; }
    .accept { background: #bfe3c8; }
    .reject { background: #efb7a9; }
    .node text { font-size: 11px; pointer-events: none; }
    .node circle { stroke: var(--line); stroke-width: 2; }
    .link { stroke: var(--line); stroke-width: 1.4; opacity: .62; }
    .link.reads { stroke: var(--teal); }
    .link.writes { stroke: var(--red); }
    .link.contains { stroke: var(--amber); }
    .link.depends_on { stroke: var(--blue); }
    .link.uses_connection { stroke: var(--green); }
    @media (max-width: 1100px) {
      .shell { grid-template-columns: 280px minmax(0, 1fr); }
      .right { display: none; }
      header { grid-column: 1 / 3; }
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
      <div style="position:relative; min-height:0;">
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
      $('candidates').innerHTML = rows.map(row => `
        <div class="candidate">
          <strong>${escapeHtml(row.proposed_kind)} ${Number(row.proposed_confidence).toFixed(2)}</strong>
          <div class="meta">${escapeHtml(row.proposed_src_urn)} -> ${escapeHtml(row.proposed_dst_urn)}</div>
          <div class="meta">${escapeHtml(row.rationale)}</div>
          <div class="actions">
            <button class="accept" data-id="${row.candidate_id}">Accept</button>
            <button class="reject" data-id="${row.candidate_id}">Reject</button>
          </div>
        </div>
      `).join('');
      document.querySelectorAll('.accept').forEach(btn => btn.onclick = () => review(btn.dataset.id, 'accept'));
      document.querySelectorAll('.reject').forEach(btn => btn.onclick = () => review(btn.dataset.id, 'reject'));
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
      const nodes = graph.nodes.map((n, i) => ({...n, x: width/2 + Math.cos(i)*80, y: height/2 + Math.sin(i)*80}));
      const byUrn = Object.fromEntries(nodes.map(n => [n.urn, n]));
      const links = graph.links.filter(l => byUrn[l.source] && byUrn[l.target]);
      const radius = Math.min(width, height) * 0.38;
      nodes.forEach((n, i) => {
        const angle = (Math.PI * 2 * i) / Math.max(nodes.length, 1);
        n.x = width / 2 + Math.cos(angle) * radius * (n.urn === graph.root ? 0 : 1);
        n.y = height / 2 + Math.sin(angle) * radius * (n.urn === graph.root ? 0 : 1);
      });
      const linkMarkup = links.map(l => {
        const s = byUrn[l.source], t = byUrn[l.target];
        return `<line class="link ${String(l.type || '').toLowerCase()}" x1="${s.x}" y1="${s.y}" x2="${t.x}" y2="${t.y}"><title>${escapeHtml(l.type)} ${l.confidence ?? ''}</title></line>`;
      }).join('');
      const nodeMarkup = nodes.map(n => {
        const fill = colorFor(n.kind);
        const label = shortLabel(n.name || n.urn);
        return `<g class="node" transform="translate(${n.x},${n.y})" data-urn="${encodeURIComponent(n.urn)}">
          <circle r="${n.urn === graph.root ? 13 : 9}" fill="${fill}"></circle>
          <text x="13" y="4">${escapeHtml(label)}</text>
        </g>`;
      }).join('');
      svg.innerHTML = linkMarkup + nodeMarkup;
      [...svg.querySelectorAll('.node')].forEach(el => {
        el.onclick = () => {
          const urn = decodeURIComponent(el.dataset.urn);
          const node = byUrn[urn];
          const connected = links.filter(l => l.source === urn || l.target === urn);
          $('detail').textContent = JSON.stringify({node, connected}, null, 2);
        };
        el.ondblclick = () => loadLineage(decodeURIComponent(el.dataset.urn));
      });
    }

    function colorFor(kind) {
      return {job:'#f8d36a', node:'#9ed0d1', dataset:'#c8d79a', path_prefix:'#f0a58e', connection:'#b9aedc', workspace:'#f4f1ea'}[kind] || '#ddd1bd';
    }
    function shortLabel(value) { return String(value).length > 34 ? String(value).slice(0, 31) + '...' : String(value); }
    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    }
    $('searchBtn').onclick = search;
    $('q').onkeydown = (e) => { if (e.key === 'Enter') search(); };
    loadStats(); loadCandidates();
  </script>
</body>
</html>"""
