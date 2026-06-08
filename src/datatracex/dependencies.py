from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .settings import AppSettings


@dataclass(frozen=True, slots=True)
class DependencyStatus:
    name: str
    ok: bool
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "ok": self.ok, "detail": self.detail}


def check_postgres(dsn: str) -> DependencyStatus:
    try:
        import psycopg

        with psycopg.connect(dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), current_user")
                database, user = cur.fetchone()
        return DependencyStatus("postgres", True, f"connected database={database} user={user}")
    except Exception as exc:
        return DependencyStatus("postgres", False, str(exc))


def check_neo4j(uri: str, user: str, password: str) -> DependencyStatus:
    try:
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver:
            driver.verify_connectivity()
        return DependencyStatus("neo4j", True, f"connected uri={uri} user={user}")
    except Exception as exc:
        return DependencyStatus("neo4j", False, str(exc))


def check_all(settings: AppSettings) -> list[DependencyStatus]:
    return [
        check_postgres(settings.postgres.dsn),
        check_neo4j(settings.neo4j.uri, settings.neo4j.user, settings.neo4j.password),
    ]
