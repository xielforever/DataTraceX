from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .collectors.dataarts import DataArtsHarvester
from .demo import build_demo_store
from .huawei.client import HuaweiClient
from .huawei.dataarts import DataArtsClient
from .http_api import serve
from .raw import RawLake
from .settings import load_huawei_settings
from .store import LineageStore


def main() -> None:
    parser = argparse.ArgumentParser(prog="datatracex")
    parser.add_argument("--env-file", help="load environment variables from a local file")
    subparsers = parser.add_subparsers(dest="command")

    serve_demo = subparsers.add_parser("serve-demo", help="serve seeded demo lineage")
    serve_demo.add_argument("--host", default="127.0.0.1")
    serve_demo.add_argument("--port", type=int, default=8765)

    serve_empty = subparsers.add_parser("serve", help="serve an empty local lineage API")
    serve_empty.add_argument("--host", default="127.0.0.1")
    serve_empty.add_argument("--port", type=int, default=8765)

    harvest = subparsers.add_parser("harvest-dataarts", help="harvest real DataArts job payloads")
    harvest.add_argument("--days", type=int, help="instance lookback days; default comes from env")
    harvest.add_argument("--max-jobs", type=int, help="limit jobs for a first real run")
    harvest.add_argument(
        "--include-instance-details",
        action="store_true",
        help="also call job instance detail for every returned instance",
    )

    args = parser.parse_args()

    if args.env_file:
        _load_env_file(Path(args.env_file))

    if args.command == "serve-demo":
        serve(build_demo_store(), host=args.host, port=args.port)
        return

    if args.command == "serve":
        serve(LineageStore(), host=args.host, port=args.port)
        return

    if args.command == "harvest-dataarts":
        settings = load_huawei_settings()
        client = HuaweiClient(
            settings.ak,
            settings.sk,
            endpoint=settings.dataarts_factory_endpoint,
            workspace_id=settings.workspace_id,
        )
        dataarts = DataArtsClient(client, settings.project_id)
        harvester = DataArtsHarvester(dataarts, RawLake(settings.raw_dir))
        summary = harvester.harvest(
            days=args.days or settings.job_instance_days,
            max_jobs=args.max_jobs,
            include_instance_details=args.include_instance_details,
        )
        print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))
        return

    parser.print_help()


def _load_env_file(path: Path) -> None:
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


if __name__ == "__main__":
    main()
