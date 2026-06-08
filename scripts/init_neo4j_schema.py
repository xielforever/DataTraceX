from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from datatracex.settings import load_app_settings


def main() -> int:
    env_file = Path(".env")
    if env_file.exists():
        _load_env_file(env_file)

    from neo4j import GraphDatabase

    settings = load_app_settings().neo4j
    statements = [
        item.strip()
        for item in Path("infra/neo4j/schema.cypher").read_text(encoding="utf-8").split(";")
        if item.strip()
    ]
    driver = GraphDatabase.driver(settings.uri, auth=(settings.user, settings.password))
    with driver:
        with driver.session() as session:
            for statement in statements:
                session.run(statement)
    print(f"Applied {len(statements)} Neo4j schema statements.")
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
