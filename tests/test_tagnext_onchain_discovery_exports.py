from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

from sqlalchemy import delete, select

from app.tagnext_comparison import create_full_brain_export, rolling_comparison_report
from app.tagnext_discovery import (
    MULTILINGUAL_QUERIES,
    SOURCE_SEEDS,
    discovery_query_plan,
    parse_search_results,
    source_seed_inventory,
)
from app.tagnext_intelligence import PRIMARY_POOL, TAG_CONTRACT, WBNB_CONTRACT
from app.tagnext_onchain import (
    DECIMALS_SELECTOR,
    TOKEN0_SELECTOR,
    TOKEN1_SELECTOR,
    TOTAL_SUPPLY_SELECTOR,
    TRANSFER_TOPIC,
    V3_SWAP_TOPIC,
    collect_bnb_chain_once,
    decode_transfer_log,
    heatmap_payload,
)
from app.tagnext_pipeline import seed_tagnext_registries
from app.terminal_database import (
    TagNextExportRunRow,
    TagNextChainCursorRow,
    TagNextHolderHistoryRow,
    TagNextOnchainEventRow,
    TagNextWhaleEntityRow,
    init_db,
    session_scope,
)


def _word(value: int) -> str:
    if value < 0:
        value = (1 << 256) + value
    return f"{value:064x}"


def _address_topic(address: str) -> str:
    return "0x" + address.removeprefix("0x").rjust(64, "0")


class FakeRpc:
    def call(self, method: str, params: list[object]):
        if method == "eth_blockNumber":
            return hex(112)
        if method == "eth_getBlockByNumber":
            return {"timestamp": hex(1_775_000_000)}
        if method == "eth_getLogs":
            query = params[0]
            assert isinstance(query, dict)
            if str(query["address"]).lower() == TAG_CONTRACT:
                return [{
                    "topics": [TRANSFER_TOPIC, _address_topic("0x" + "11" * 20), _address_topic("0x" + "22" * 20)],
                    "data": "0x" + _word(2_000_000 * 10**18),
                    "transactionHash": "0x" + "aa" * 32,
                    "logIndex": "0x1", "blockNumber": "0x64",
                }]
            assert str(query["address"]).lower() == PRIMARY_POOL
            return [{
                "topics": [V3_SWAP_TOPIC],
                "data": "0x" + _word(1_500_000 * 10**18) + _word(-2 * 10**18) + _word(0) * 5,
                "transactionHash": "0x" + "bb" * 32,
                "logIndex": "0x2", "blockNumber": "0x64",
            }]
        raise AssertionError(method)

    def eth_call(self, to: str, data: str, block: str = "latest") -> str:
        selector = data.removeprefix("0x")[:8]
        if to.lower() == PRIMARY_POOL and selector == TOKEN0_SELECTOR:
            return _address_topic(TAG_CONTRACT)
        if to.lower() == PRIMARY_POOL and selector == TOKEN1_SELECTOR:
            return _address_topic(WBNB_CONTRACT)
        if selector == DECIMALS_SELECTOR:
            return hex(18)
        if selector == TOTAL_SUPPLY_SELECTOR:
            return hex(405_380_800_000 * 10**18)
        return hex(100 * 10**18)  # balanceOf for directly observed addresses


def setup_function() -> None:
    init_db()
    with session_scope() as session:
        for model in (TagNextHolderHistoryRow, TagNextOnchainEventRow, TagNextWhaleEntityRow, TagNextChainCursorRow):
            session.execute(delete(model))


def test_transfer_decoder_and_bounded_direct_worker_persist_real_event_types() -> None:
    assert V3_SWAP_TOPIC == "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83"
    log = {
        "topics": [TRANSFER_TOPIC, _address_topic("0x" + "11" * 20), _address_topic("0x" + "22" * 20)],
        "data": "0x" + _word(5 * 10**18), "transactionHash": "0x1",
        "logIndex": "0x3", "blockNumber": "0x4",
    }
    decoded = decode_transfer_log(log)
    assert decoded["tokenQuantity"] == 5
    result = collect_bnb_chain_once(rpc=FakeRpc(), from_block=100, to_block=100)
    assert result == {
        "fromBlock": 100, "toBlock": 100, "eventsPersisted": 2,
        "transfers": 1, "largeSwaps": 1, "lpEvents": 0,
        "holderSnapshots": 2, "completeHolderCensus": False,
        "exchangeLabels": "verified_only", "paidCalls": 0,
        "confirmationDepth": 12, "confirmedHead": 100,
        "cursorState": "caught_up", "rpcEndpoint": "injected_rpc",
        "rpcEndpoints": {},
    }
    with session_scope() as session:
        assert set(session.scalars(select(TagNextOnchainEventRow.event_type))) == {"transfer", "large_swap"}
        assert all(row.verification_state == "unverified" for row in session.scalars(select(TagNextWhaleEntityRow)))


def test_postgres_constraint_migration_replaces_the_legacy_automatic_name() -> None:
    sql = (Path(__file__).parents[1] / "migrations" / "20260820_tagnext_onchain_event_constraint.sql").read_text(
        encoding="utf-8"
    ).lower()
    assert "drop constraint if exists tagnext_onchain_events_event_type_check" in sql
    assert "'lp_collect'" in sql


def test_heatmap_api_hides_illustrative_bands_and_never_implies_forecast_weight() -> None:
    payload = heatmap_payload()
    assert payload["illustrativeBandsPresented"] is False
    assert payload["providerHeatmapStatus"] == "not_configured"
    assert payload["forecastInfluence"] is False


def test_discovery_plan_covers_all_supplied_seeds_three_engines_and_twelve_languages() -> None:
    inventory = source_seed_inventory()
    assert inventory["seedCount"] == len(SOURCE_SEEDS) >= 40
    assert len(MULTILINGUAL_QUERIES) == 12
    assert set(inventory["engines"]) == {"bing_rss", "duckduckgo_html", "brave_html"}
    assert any(seed["name"] == "Coin Monkey / CoinMonkey" and "no_related" in seed["state"] for seed in SOURCE_SEEDS)
    plan = discovery_query_plan()
    assert any(TAG_CONTRACT in row["query"] for row in plan)
    assert any(row["query"].startswith("site:coincodex.com") for row in plan)


def test_search_parsers_only_return_external_candidate_urls() -> None:
    rss = "<rss><channel><item><link>https://example.test/tagger-forecast</link></item></channel></rss>"
    assert parse_search_results("bing_rss", rss) == ["https://example.test/tagger-forecast"]
    html = '<a href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.test%2Ftagger">result</a>'
    assert parse_search_results("duckduckgo_html", html) == ["https://example.test/tagger"]


def test_full_brain_export_has_manifest_internal_checksums_and_no_secret_files(tmp_path: Path) -> None:
    seed_tagnext_registries()
    target = tmp_path / "full-brain.zip"
    result = create_full_brain_export(target)
    assert result["secretMaterialIncluded"] is False
    assert result["sha256"] == hashlib.sha256(target.read_bytes()).hexdigest()
    with zipfile.ZipFile(target) as archive:
        names = set(archive.namelist())
        assert {
            "manifest.json", "SHA256SUMS.txt", "forecasts.csv", "external_forecasts.jsonl",
            "onchain_events.jsonl", "heatmaps.jsonl", "canonical_forecasts_full.jsonl",
            "canonical_forecast_grades_full.jsonl", "verified_outcomes.jsonl",
            "asset_truth_snapshots.jsonl", "portfolio_position_snapshots.jsonl",
            "independent_regrade.py", "REGRADING.md", "regrading_contract.json",
        } <= names
        manifest = json.loads(archive.read("manifest.json"))
        assert manifest["secretMaterialIncluded"] is False
        assert not any(name.lower().endswith(".env") for name in names)
        for item in manifest["files"]:
            assert hashlib.sha256(archive.read(item["path"])).hexdigest() == item["sha256"]
    assert rolling_comparison_report()["automaticPromotion"] is False
    with session_scope() as session:
        assert session.get(TagNextExportRunRow, result["exportId"]) is not None
