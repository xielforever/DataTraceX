from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .chunker import ScriptChunk, chunk_script
from .provider import AIProvider, AIRequest
from .redaction import redact_sensitive_text


ALLOWED_KINDS = {"reads", "writes", "derives_from", "uses_connection", "executes_on", "depends_on", "contains"}


@dataclass(frozen=True, slots=True)
class LineageCandidate:
    source_urn: str
    target_urn: str
    edge_kind: str
    confidence: float
    rationale: str
    line_start: int | None = None
    line_end: int | None = None
    raw: dict[str, Any] | None = None


SYSTEM_PROMPT = """You analyze data lineage from code. Return only strict JSON.
Do not invent assets. If uncertain, lower confidence and explain assumptions."""


def build_user_prompt(node_urn: str, code_urn: str, chunk: ScriptChunk) -> str:
    return json.dumps(
        {
            "task": "Return lineage candidates from this code chunk.",
            "schema": {
                "candidates": [
                    {
                        "source_urn": "required upstream/source asset URN",
                        "target_urn": "required downstream/target asset URN",
                        "edge_kind": "reads|writes|derives_from|uses_connection|executes_on|depends_on|contains",
                        "confidence": "number 0..1",
                        "rationale": "short explanation",
                        "line_start": "optional original start line",
                        "line_end": "optional original end line",
                    }
                ]
            },
            "node_urn": node_urn,
            "code_urn": code_urn,
            "chunk": {
                "index": chunk.index,
                "start_line": chunk.start_line,
                "end_line": chunk.end_line,
                "text": chunk.text,
            },
        },
        ensure_ascii=False,
    )


def analyze_script_with_ai(
    provider: AIProvider,
    text: str,
    node_urn: str,
    code_urn: str,
    model: str,
    max_lines: int = 160,
) -> tuple[list[LineageCandidate], list[dict[str, Any]]]:
    redacted = redact_sensitive_text(text)
    candidates: list[LineageCandidate] = []
    responses: list[dict[str, Any]] = []
    for chunk in chunk_script(redacted.text, max_lines=max_lines):
        prompt = build_user_prompt(node_urn, code_urn, chunk)
        response = provider.complete(AIRequest(SYSTEM_PROMPT, prompt, model=model))
        parsed = parse_candidate_response(response.text)
        responses.append(
            {
                "provider": response.provider,
                "model": response.model,
                "chunk_index": chunk.index,
                "prompt_hash": _hash(prompt),
                "response_hash": _hash(response.text),
                "raw": response.raw,
                "redactions": redacted.replacements,
            }
        )
        candidates.extend(parsed)
    return candidates, responses


def parse_candidate_response(text: str) -> list[LineageCandidate]:
    data = json.loads(text)
    items = data.get("candidates")
    if not isinstance(items, list):
        raise ValueError("AI response must contain a candidates list")
    candidates: list[LineageCandidate] = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("candidate must be an object")
        source_urn = _required_str(item, "source_urn")
        target_urn = _required_str(item, "target_urn")
        edge_kind = _required_str(item, "edge_kind")
        if edge_kind not in ALLOWED_KINDS:
            raise ValueError(f"unsupported edge_kind: {edge_kind}")
        confidence = float(item.get("confidence"))
        if confidence < 0 or confidence > 1:
            raise ValueError("confidence must be between 0 and 1")
        rationale = _required_str(item, "rationale")
        line_start = _optional_int(item.get("line_start"))
        line_end = _optional_int(item.get("line_end"))
        if line_start and line_end and line_end < line_start:
            raise ValueError("line_end must be >= line_start")
        candidates.append(
            LineageCandidate(
                source_urn=source_urn,
                target_urn=target_urn,
                edge_kind=edge_kind,
                confidence=confidence,
                rationale=rationale,
                line_start=line_start,
                line_end=line_end,
                raw=item,
            )
        )
    return candidates


def _required_str(item: dict[str, Any], key: str) -> str:
    value = item.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} is required")
    return value.strip()


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
