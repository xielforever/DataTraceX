from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from datatracex.dependencies import check_all
from datatracex.settings import load_app_settings


def main() -> int:
    env_file = Path(".env")
    if env_file.exists():
        _load_env_file(env_file)

    results = check_all(load_app_settings())
    print(json.dumps([item.to_dict() for item in results], ensure_ascii=True, indent=2))
    return 0 if all(item.ok for item in results) else 1


def _load_env_file(path: Path) -> None:
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


if __name__ == "__main__":
    raise SystemExit(main())
