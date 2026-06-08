from datatracex.settings import load_app_settings


def test_load_app_settings_defaults(monkeypatch) -> None:
    monkeypatch.delenv("DATATRACEX_POSTGRES_DSN", raising=False)
    monkeypatch.delenv("DATATRACEX_NEO4J_URI", raising=False)
    monkeypatch.delenv("DATATRACEX_NEO4J_USER", raising=False)
    monkeypatch.delenv("DATATRACEX_NEO4J_PASSWORD", raising=False)

    settings = load_app_settings()

    assert settings.postgres.dsn.startswith("postgresql://datatracex:")
    assert settings.neo4j.uri == "bolt://127.0.0.1:7687"
    assert settings.neo4j.user == "neo4j"
