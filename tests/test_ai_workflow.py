import json

import pytest

from datatracex.ai.chunker import chunk_script
from datatracex.ai.lineage_analyzer import analyze_script_with_ai, parse_candidate_response
from datatracex.ai.provider import MockAIProvider
from datatracex.ai.repository import validate_candidate_updates
from datatracex.ai.redaction import redact_sensitive_text
from datatracex.review.api import redacted_script_snippet


def test_redacts_common_secret_patterns() -> None:
    result = redact_sensitive_text("password=secret token: abc postgresql://u:p@host/db")

    assert "secret" not in result.text
    assert "abc" not in result.text
    assert "postgresql://u:***@host/db" in result.text
    assert result.replacements >= 3


def test_chunk_script_preserves_line_ranges() -> None:
    chunks = chunk_script("\n".join(f"line {i}" for i in range(1, 11)), max_lines=4, overlap_lines=1)

    assert [(chunk.start_line, chunk.end_line) for chunk in chunks] == [(1, 4), (4, 7), (7, 10)]


def test_parse_candidate_response_validates_kind_and_confidence() -> None:
    response = json.dumps(
        {
            "candidates": [
                {
                    "source_urn": "obs://bucket/input",
                    "target_urn": "dws://cluster/db/schema/table",
                    "edge_kind": "derives_from",
                    "confidence": 0.7,
                    "rationale": "explicit read/write flow",
                    "line_start": 1,
                    "line_end": 3,
                }
            ]
        }
    )

    candidates = parse_candidate_response(response)

    assert candidates[0].edge_kind == "derives_from"
    assert candidates[0].confidence == 0.7


def test_parse_candidate_response_rejects_bad_kind() -> None:
    with pytest.raises(ValueError):
        parse_candidate_response(
            json.dumps(
                {
                    "candidates": [
                        {
                            "source_urn": "a",
                            "target_urn": "b",
                            "edge_kind": "maybe",
                            "confidence": 0.5,
                            "rationale": "bad",
                        }
                    ]
                }
            )
        )


def test_analyze_script_with_mock_provider() -> None:
    response = json.dumps(
        {
            "candidates": [
                {
                    "source_urn": "obs://bucket/input",
                    "target_urn": "dws://cluster/db/schema/table",
                    "edge_kind": "reads",
                    "confidence": 0.6,
                    "rationale": "mock",
                }
            ]
        }
    )

    candidates, responses = analyze_script_with_ai(
        MockAIProvider(response),
        "password=hidden\nread('obs://bucket/input')",
        "dataarts://workspace/job/job/node/node",
        "code://sha256/demo",
        "mock",
    )

    assert len(candidates) == 1
    assert responses[0]["redactions"] == 1


def test_analyze_script_retries_invalid_json_response() -> None:
    valid_response = json.dumps(
        {
            "candidates": [
                {
                    "source_urn": "obs://bucket/input",
                    "target_urn": "dws://cluster/db/schema/table",
                    "edge_kind": "reads",
                    "confidence": 0.6,
                    "rationale": "retry produced valid JSON",
                }
            ]
        }
    )
    provider = MockAIProvider(["not-json", valid_response])

    candidates, responses = analyze_script_with_ai(
        provider,
        "read('obs://bucket/input')",
        "dataarts://workspace/job/job/node/node",
        "code://sha256/demo",
        "mock",
        response_retries=1,
    )

    assert len(candidates) == 1
    assert provider.calls == 2
    assert responses[0]["accepted"] is False
    assert responses[0]["parse_error"]
    assert responses[1]["accepted"] is True


def test_analyze_script_raises_after_json_retry_budget() -> None:
    provider = MockAIProvider(["not-json", "still-not-json"])

    with pytest.raises(ValueError, match="after 2 attempts"):
        analyze_script_with_ai(
            provider,
            "read('obs://bucket/input')",
            "dataarts://workspace/job/job/node/node",
            "code://sha256/demo",
            "mock",
            response_retries=1,
        )


def test_validate_candidate_updates_allows_review_edits() -> None:
    updates = validate_candidate_updates(
        {
            "proposed_src_urn": "obs://bucket/input",
            "proposed_dst_urn": "dws://cluster/db/schema/table",
            "proposed_kind": "derives_from",
            "proposed_edge_scope": "inferred",
            "proposed_confidence": "0.82",
            "rationale": "reviewer corrected direction",
            "line_start": "3",
            "line_end": "5",
            "ignored": "nope",
        }
    )

    assert updates["proposed_confidence"] == 0.82
    assert updates["line_start"] == 3
    assert "ignored" not in updates


def test_redacted_script_snippet_uses_line_window_without_secret_leak() -> None:
    payload = {"content": "line1\npassword=hidden\nread('obs://bucket/input')\nline4\nline5"}

    snippet = redacted_script_snippet(payload, 2, 3)

    assert snippet is not None
    assert snippet["start_line"] == 1
    assert "hidden" not in snippet["text"]
    assert "password=***" in snippet["text"]
