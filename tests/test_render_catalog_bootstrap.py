from pathlib import Path


def test_render_catalog_bootstrap_is_fail_closed_and_champion_empty() -> None:
    source = (Path(__file__).parents[1] / "scripts" / "bootstrap_render_catalog.py").read_text(
        encoding="utf-8"
    )
    assert 'EXPECTED_DATABASE = "tagnext_challenger"' in source
    assert 'EXPECTED_PUBLIC_SOURCES = 24' in source
    assert 'EXPECTED_PUBLIC_PREDICTIONS = 374' in source
    assert 'os.environ.get("TAGNEXT_CATALOG_BOOTSTRAP", "0") != "1"' in source
    assert 'refusing to restore over a partially populated catalog' in source
    assert 'champion-table isolation check failed' in source
    assert "--single-transaction" in source
    assert "--exit-on-error" in source
