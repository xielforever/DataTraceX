from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from datatracex.huawei.client import HuaweiClient
from datatracex.huawei.dataarts import DataArtsClient
from datatracex.ingest.dataarts_scripts import DataArtsScriptIngestor
from datatracex.postgres_store import PostgresFactStore
from datatracex.settings import load_app_settings, load_huawei_settings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--script-name")
    parser.add_argument("--version")
    parser.add_argument("--sample-size", type=int, default=3)
    parser.add_argument("--skip-list", action="store_true")
    args = parser.parse_args()

    _load_env_file(Path(".env"))
    huawei = load_huawei_settings()
    app = load_app_settings()
    client = DataArtsClient(
        HuaweiClient(
            huawei.ak,
            huawei.sk,
            endpoint=huawei.dataarts_factory_endpoint,
            workspace_id=huawei.workspace_id,
        ),
        huawei.project_id,
    )

    output: dict[str, Any] = {"list_probe": None, "detail_probes": []}
    if not args.skip_list:
        payload = client.list_scripts(limit=5, offset=0)
        output["list_probe"] = _summarize_payload(payload)

    samples = []
    if args.script_name:
        samples.append({"script_name": args.script_name, "script_version": args.version})
    else:
        ingestor = DataArtsScriptIngestor(
            client,
            PostgresFactStore(app.postgres.dsn),
            endpoint=huawei.dataarts_factory_endpoint,
            project_id=huawei.project_id,
            workspace_id=huawei.workspace_id or "",
        )
        seen = set()
        for ref in ingestor.load_references():
            key = (ref.script_name, ref.script_version)
            if key in seen:
                continue
            seen.add(key)
            samples.append({"script_name": ref.script_name, "script_version": ref.script_version})
            if len(samples) >= args.sample_size:
                break

    for sample in samples:
        payload = client.show_script(sample["script_name"], version=sample["script_version"])
        summary = _summarize_payload(payload)
        summary["script_name"] = sample["script_name"]
        summary["script_version"] = sample["script_version"]
        summary["request_path"] = client.script_detail_path(sample["script_name"])
        output["detail_probes"].append(summary)

    print(json.dumps(output, ensure_ascii=True, indent=2))
    return 0


def _summarize_payload(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        content = payload.get("content") if isinstance(payload.get("content"), str) else None
        return {
            "shape": "object",
            "keys": sorted(payload.keys()),
            "item_count": None,
            "content_length": len(content or ""),
            "name": payload.get("name"),
            "type": payload.get("type"),
            "version": payload.get("version"),
        }
    if isinstance(payload, list):
        return {
            "shape": "list",
            "keys": sorted(payload[0].keys()) if payload and isinstance(payload[0], dict) else [],
            "item_count": len(payload),
            "content_length": 0,
        }
    return {"shape": type(payload).__name__, "keys": [], "item_count": None, "content_length": 0}


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
