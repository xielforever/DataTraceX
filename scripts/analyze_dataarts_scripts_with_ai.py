from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from datatracex.ai.lineage_analyzer import analyze_script_with_ai
from datatracex.ai.provider import MockAIProvider, OpenAICompatibleProvider
from datatracex.ai.repository import LineageCandidateRepository
from datatracex.ingest.dataarts_scripts import extract_script_content, extract_script_references
from datatracex.settings import load_app_settings
from datatracex.urn import code_artifact_urn


DEFAULT_AI_NODE_TYPES = {"Python", "Shell", "MRSFlinkJob", "MRSSparkPython"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-scripts", type=int)
    parser.add_argument("--node-type", action="append", dest="node_types")
    parser.add_argument("--min-lines", type=int, default=20)
    parser.add_argument("--max-lines", type=int, default=160)
    parser.add_argument("--model")
    parser.add_argument("--mock-response")
    parser.add_argument("--retries", type=int, default=2)
    args = parser.parse_args()

    _load_env_file(Path(".env"))
    settings = load_app_settings()
    dsn = settings.postgres.dsn
    node_types = set(args.node_types or DEFAULT_AI_NODE_TYPES)
    model = args.model or os.getenv("DATATRACEX_AI_MODEL")

    if args.mock_response:
        provider = MockAIProvider(_read_mock_response(args.mock_response))
        model = model or "mock-lineage"
    else:
        if not model:
            raise RuntimeError("set --model or DATATRACEX_AI_MODEL before running real AI analysis")
        provider = OpenAICompatibleProvider(retries=args.retries)

    references = [
        ref
        for ref in _load_references(dsn)
        if ref.node_type in node_types
    ]
    payloads = _load_script_payloads(dsn)
    evidence_by_raw = _load_script_evidence(dsn)
    repo = LineageCandidateRepository(dsn)

    summary = {
        "references_seen": len(references),
        "references_attempted": 0,
        "references_analyzed": 0,
        "references_skipped": 0,
        "candidates_added": 0,
        "errors": [],
        "node_types": Counter(),
    }

    analyzed_scripts = set()
    for ref in references:
        summary["node_types"][ref.node_type] += 1
        if ref.script_source_key in analyzed_scripts:
            summary["references_skipped"] += 1
            continue
        raw = payloads.get(ref.script_source_key)
        if not raw:
            summary["references_skipped"] += 1
            continue
        raw_id, payload = raw
        content = extract_script_content(payload)
        if content is None or _line_count(content) < args.min_lines:
            summary["references_skipped"] += 1
            continue
        if args.max_scripts is not None and summary["references_attempted"] >= args.max_scripts:
            break
        analyzed_scripts.add(ref.script_source_key)
        summary["references_attempted"] += 1
        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        code_urn = code_artifact_urn(content_hash)
        try:
            candidates, responses = analyze_script_with_ai(
                provider,
                content,
                ref.node_urn,
                code_urn,
                model=model or "mock-lineage",
                max_lines=args.max_lines,
            )
        except Exception as exc:
            summary["errors"].append(f"{ref.script_name}@{ref.script_version or 'latest'}: {exc}")
            continue

        response_raw = {"responses": responses, "script_source_key": ref.script_source_key}
        prompt_hash = _hash_json([item.get("prompt_hash") for item in responses])
        response_hash = _hash_json([item.get("response_hash") for item in responses])
        for candidate in candidates:
            repo.add_candidate(
                candidate,
                source_evidence_id=evidence_by_raw.get(raw_id),
                node_urn=ref.node_urn,
                code_urn=code_urn,
                provider=responses[0]["provider"] if responses else "unknown",
                model=responses[0]["model"] if responses else model or "unknown",
                prompt_hash=prompt_hash,
                response_hash=response_hash,
                response_raw=response_raw,
            )
            summary["candidates_added"] += 1
        summary["references_analyzed"] += 1

    serializable = dict(summary)
    serializable["node_types"] = dict(summary["node_types"])
    print(json.dumps(serializable, ensure_ascii=True, indent=2))
    return 0 if not summary["errors"] else 1


def _load_references(dsn: str):
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT source_key, payload
            FROM raw_payload
            WHERE service = 'dataarts'
              AND category = 'job_detail'
            ORDER BY source_key
            """
        ).fetchall()
    refs = []
    for row in rows:
        refs.extend(extract_script_references(row["source_key"], row["payload"]))
    return refs


def _load_script_payloads(dsn: str) -> dict[str, tuple[str, dict[str, Any]]]:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT ON (source_key) source_key, raw_id, payload
            FROM raw_payload
            WHERE service = 'dataarts'
              AND category = 'script_detail'
            ORDER BY source_key, captured_at DESC
            """
        ).fetchall()
    return {
        row["source_key"]: (row["raw_id"], row["payload"])
        for row in rows
        if isinstance(row["payload"], dict)
    }


def _load_script_evidence(dsn: str) -> dict[str, str]:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT raw_id, evidence_id
            FROM evidence
            WHERE source_system = 'dataarts'
              AND source_api = 'ShowScript'
              AND raw_id IS NOT NULL
            """
        ).fetchall()
    return {row["raw_id"]: row["evidence_id"] for row in rows}


def _read_mock_response(value: str) -> str:
    if value.startswith("b64:"):
        return base64.b64decode(value[4:].encode("ascii")).decode("utf-8")
    if value.startswith("@"):
        return Path(value[1:]).read_text(encoding="utf-8")
    if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
        value = value[1:-1]
    return value.replace('\\"', '"')


def _line_count(text: str) -> int:
    return max(1, len(text.splitlines()))


def _hash_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


if __name__ == "__main__":
    raise SystemExit(main())
