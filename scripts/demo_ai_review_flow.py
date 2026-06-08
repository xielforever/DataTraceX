from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from datatracex.ai.lineage_analyzer import analyze_script_with_ai
from datatracex.ai.provider import MockAIProvider
from datatracex.ai.repository import LineageCandidateRepository
from datatracex.postgres_store import PostgresFactStore
from datatracex.review.materialize import CandidateMaterializer
from datatracex.settings import load_app_settings


def main() -> int:
    _load_env_file(Path(".env"))
    settings = load_app_settings()
    mock_response = json.dumps(
        {
            "candidates": [
                {
                    "source_urn": "obs://ai-demo/raw/orders",
                    "target_urn": "dws://ai-demo/dw/public/fact_orders",
                    "edge_kind": "derives_from",
                    "confidence": 0.74,
                    "rationale": "The script reads raw order data and writes fact_orders.",
                    "line_start": 1,
                    "line_end": 3,
                }
            ]
        }
    )
    script = """
raw_path = "obs://ai-demo/raw/orders"
sql = "insert into public.fact_orders select * from staging_orders"
run_job(raw_path, sql)
""".strip()
    candidates, responses = analyze_script_with_ai(
        MockAIProvider(mock_response),
        script,
        node_urn="dataarts://demo/job/demo/node/demo",
        code_urn="code://sha256/demo",
        model="mock-lineage",
    )
    repo = LineageCandidateRepository(settings.postgres.dsn)
    stored_ids = []
    for candidate in candidates:
        stored = repo.add_candidate(
            candidate,
            source_evidence_id=None,
            node_urn="dataarts://demo/job/demo/node/demo",
            code_urn="code://sha256/demo",
            provider=responses[0]["provider"],
            model=responses[0]["model"],
            prompt_hash=responses[0]["prompt_hash"],
            response_hash=responses[0]["response_hash"],
            response_raw=responses[0],
        )
        stored_ids.append(stored.candidate_id)
    materializer = CandidateMaterializer(PostgresFactStore(settings.postgres.dsn), repo)
    edge_ids = [materializer.accept_and_materialize(candidate_id, "demo-reviewer", "demo acceptance") for candidate_id in stored_ids]
    print(json.dumps({"candidates": stored_ids, "edges": edge_ids}, ensure_ascii=True, indent=2))
    return 0


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
