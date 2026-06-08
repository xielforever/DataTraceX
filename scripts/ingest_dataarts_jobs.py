from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from datatracex.huawei.client import HuaweiClient
from datatracex.huawei.dataarts import DataArtsClient
from datatracex.ingest.dataarts_jobs import DataArtsJobIngestor
from datatracex.postgres_store import PostgresFactStore
from datatracex.settings import load_huawei_settings, load_app_settings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-jobs", type=int)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--missing-only", action="store_true")
    args = parser.parse_args()

    env_file = Path(".env")
    if env_file.exists():
        _load_env_file(env_file)

    huawei = load_huawei_settings()
    app = load_app_settings()
    client = HuaweiClient(
        huawei.ak,
        huawei.sk,
        endpoint=huawei.dataarts_factory_endpoint,
        workspace_id=huawei.workspace_id,
    )
    dataarts = DataArtsClient(client, huawei.project_id)
    store = PostgresFactStore(app.postgres.dsn)
    ingestor = DataArtsJobIngestor(
        dataarts,
        store,
        endpoint=huawei.dataarts_factory_endpoint,
        project_id=huawei.project_id,
        workspace_id=huawei.workspace_id or "",
    )
    summary = ingestor.ingest(
        max_jobs=args.max_jobs,
        page_size=args.page_size,
        max_workers=args.workers,
        missing_only=args.missing_only,
        max_retries=args.max_retries,
    )
    print(json.dumps(summary.to_dict(), ensure_ascii=True, indent=2))
    print(json.dumps({"store_stats": store.stats()}, ensure_ascii=True, indent=2))
    return 0 if not summary.errors else 1


def _load_env_file(path: Path) -> None:
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


if __name__ == "__main__":
    raise SystemExit(main())
