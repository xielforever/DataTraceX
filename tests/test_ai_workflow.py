import json

import pytest

from datatracex.ai.chunker import chunk_script
from datatracex.ai.lineage_analyzer import analyze_script_with_ai, parse_candidate_response
from datatracex.ai.provider import MockAIProvider
from datatracex.ai.redaction import redact_sensitive_text


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
