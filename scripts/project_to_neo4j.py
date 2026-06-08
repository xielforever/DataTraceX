from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from datatracex.neo4j_projector import Neo4jProjector
from datatracex.settings import load_app_settings


def main() -> int:
    env_file = Path(".env")
    if env_file.exists():
        _load_env_file(env_file)

    settings = load_app_settings()
    projector = Neo4jProjector(
        settings.postgres.dsn,
        settings.neo4j.uri,
        settings.neo4j.user,
        settings.neo4j.password,
    )
    try:
        summary = projector.project_all()
    finally:
        projector.close()
    print(json.dumps(summary.to_dict(), ensure_ascii=True, indent=2))
    return 0


def _load_env_file(path: Path) -> None:
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


if __name__ == "__main__":
    raise SystemExit(main())
