"""Free/direct BNB-chain evidence collection for TAGneXt.

This module uses bounded JSON-RPC reads only. Unknown addresses remain unknown;
exchange labels are accepted only from an explicitly verified label registry.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

import httpx
from eth_hash.auto import keccak
from sqlalchemy import select

from .tagnext_intelligence import PRIMARY_POOL, TAG_CONTRACT, WBNB_CONTRACT
from .terminal_database import (
    TagNextHolderHistoryRow,
    TagNextEventOutcomeRow,
    TagNextHeatmapRow,
    TagNextOnchainEventRow,
    TagNextWhaleEntityRow,
    VerifiedOutcomeRow,
    json_dumps,
    session_scope,
)


CHAIN_ID = 56
DEFAULT_RPC_URL = "https://bsc-dataseed.bnbchain.org"
TRANSFER_TOPIC = "0x" + keccak(b"Transfer(address,address,uint256)").hex()
V3_SWAP_TOPIC = "0x" + keccak(b"Swap(address,address,int256,int256,uint160,uint128,int24)").hex()
V3_MINT_TOPIC = "0x" + keccak(b"Mint(address,address,int24,int24,uint128,uint256,uint256)").hex()
V3_BURN_TOPIC = "0x" + keccak(b"Burn(address,int24,int24,uint128,uint256,uint256)").hex()
BALANCE_OF_SELECTOR = "70a08231"
TOTAL_SUPPLY_SELECTOR = "18160ddd"
DECIMALS_SELECTOR = "313ce567"
TOKEN0_SELECTOR = "0dfe1681"
TOKEN1_SELECTOR = "d21220a7"


def _hash_id(prefix: str, value: str) -> str:
    return f"{prefix}_{keccak(value.encode('utf-8')).hex()[:32]}"


def _hex_int(value: str | None, *, signed: bool = False) -> int:
    raw = str(value or "0x0").removeprefix("0x") or "0"
    number = int(raw, 16)
    bits = len(raw) * 4
    if signed and bits and number >= 1 << (bits - 1):
        number -= 1 << bits
    return number


def _word(data: str, index: int, *, signed: bool = False) -> int:
    raw = data.removeprefix("0x")
    chunk = raw[index * 64:(index + 1) * 64]
    return _hex_int(chunk, signed=signed)


def _topic_address(topic: str | None) -> str | None:
    raw = str(topic or "").removeprefix("0x")
    return "0x" + raw[-40:].lower() if len(raw) >= 40 else None


def _call_address(data: str) -> str:
    raw = str(data).removeprefix("0x")
    return "0x" + raw[-40:].lower()


def _iso_timestamp(timestamp: int) -> datetime:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def decode_transfer_log(log: Mapping[str, Any], *, decimals: int = 18) -> dict[str, Any]:
    topics = list(log.get("topics") or [])
    if not topics or str(topics[0]).lower() != TRANSFER_TOPIC.lower() or len(topics) < 3:
        raise ValueError("not an ERC-20 Transfer log")
    return {
        "eventType": "transfer",
        "txHash": str(log.get("transactionHash") or "").lower(),
        "logIndex": _hex_int(log.get("logIndex")),
        "blockNumber": _hex_int(log.get("blockNumber")),
        "from": _topic_address(topics[1]),
        "to": _topic_address(topics[2]),
        "tokenQuantity": _hex_int(log.get("data")) / (10 ** decimals),
    }


def decode_v3_pool_log(
    log: Mapping[str, Any], *, token0: str, token1: str,
    token0_decimals: int = 18, token1_decimals: int = 18,
) -> dict[str, Any]:
    topics = list(log.get("topics") or [])
    if not topics:
        raise ValueError("pool log has no event topic")
    topic = str(topics[0]).lower()
    tag_is_token0 = token0.lower() == TAG_CONTRACT
    if token1.lower() != TAG_CONTRACT and not tag_is_token0:
        raise ValueError("pool tokens do not include exact TAG contract")
    if topic == V3_SWAP_TOPIC.lower():
        amount0, amount1 = _word(str(log.get("data") or "0x"), 0, signed=True), _word(str(log.get("data") or "0x"), 1, signed=True)
        event_type = "large_swap"
    elif topic in {V3_MINT_TOPIC.lower(), V3_BURN_TOPIC.lower()}:
        # V3 Mint/Burn end with amount0 and amount1. Mint has an extra owner
        # word at the front, so select the final two ABI words.
        raw = str(log.get("data") or "0x").removeprefix("0x")
        word_count = len(raw) // 64
        amount0 = _word(raw, word_count - 2)
        amount1 = _word(raw, word_count - 1)
        event_type = "lp_mint" if topic == V3_MINT_TOPIC.lower() else "lp_burn"
    else:
        raise ValueError("unsupported PancakeSwap V3 pool event")
    tag_raw = amount0 if tag_is_token0 else amount1
    quote_raw = amount1 if tag_is_token0 else amount0
    tag_decimals = token0_decimals if tag_is_token0 else token1_decimals
    quote_decimals = token1_decimals if tag_is_token0 else token0_decimals
    return {
        "eventType": event_type,
        "txHash": str(log.get("transactionHash") or "").lower(),
        "logIndex": _hex_int(log.get("logIndex")),
        "blockNumber": _hex_int(log.get("blockNumber")),
        "tokenQuantity": abs(tag_raw) / (10 ** tag_decimals),
        "quoteQuantity": abs(quote_raw) / (10 ** quote_decimals),
        "tagDirection": "into_pool" if tag_raw > 0 else "out_of_pool",
    }


class BnbRpc:
    def __init__(self, url: str | None = None, *, timeout_seconds: int = 20) -> None:
        self.url = (url or os.getenv("BNB_RPC_URL") or DEFAULT_RPC_URL).strip()
        self.client = httpx.Client(timeout=timeout_seconds, headers={"User-Agent": "TAGneXt-BNB-readonly/1.0"})
        self._request_id = 0

    def close(self) -> None:
        self.client.close()

    def call(self, method: str, params: Sequence[Any]) -> Any:
        self._request_id += 1
        response = self.client.post(self.url, json={
            "jsonrpc": "2.0", "id": self._request_id, "method": method, "params": list(params),
        })
        response.raise_for_status()
        body = response.json()
        if body.get("error"):
            raise RuntimeError(f"BNB RPC {method} failed: {body['error'].get('code')}")
        return body.get("result")

    def eth_call(self, to: str, data: str, block: str = "latest") -> str:
        return str(self.call("eth_call", [{"to": to, "data": data}, block]))


def _verified_labels() -> dict[str, dict[str, Any]]:
    try:
        raw = json.loads(os.getenv("TAGNEXT_VERIFIED_EXCHANGE_LABELS_JSON", "{}") or "{}")
    except json.JSONDecodeError:
        return {}
    result: dict[str, dict[str, Any]] = {}
    for address, metadata in raw.items() if isinstance(raw, dict) else []:
        if not isinstance(metadata, dict) or metadata.get("verified") is not True:
            continue
        if not metadata.get("source") or not metadata.get("sourceUrl"):
            continue
        result[str(address).lower()] = dict(metadata)
    return result


def _contract_decimals(rpc: BnbRpc, contract: str) -> int:
    return _hex_int(rpc.eth_call(contract, "0x" + DECIMALS_SELECTOR))


def _balance_of(rpc: BnbRpc, contract: str, address: str, *, block: str) -> int:
    data = "0x" + BALANCE_OF_SELECTOR + address.removeprefix("0x").rjust(64, "0")
    return _hex_int(rpc.eth_call(contract, data, block))


def collect_bnb_chain_once(
    *, rpc: BnbRpc | None = None, from_block: int | None = None,
    to_block: int | None = None, max_block_span: int = 1_000,
) -> dict[str, Any]:
    """Collect transfers, holder balances, large swaps, and LP events."""
    owned_rpc = rpc is None
    client = rpc or BnbRpc()
    try:
        latest = _hex_int(client.call("eth_blockNumber", []))
        end = min(latest, to_block if to_block is not None else latest)
        start = from_block if from_block is not None else max(0, end - 299)
        if end < start or end - start + 1 > max_block_span:
            raise ValueError("BNB collection range must be positive and bounded")
        tag_decimals = _contract_decimals(client, TAG_CONTRACT)
        token0 = _call_address(client.eth_call(PRIMARY_POOL, "0x" + TOKEN0_SELECTOR))
        token1 = _call_address(client.eth_call(PRIMARY_POOL, "0x" + TOKEN1_SELECTOR))
        token0_decimals = _contract_decimals(client, token0)
        token1_decimals = _contract_decimals(client, token1)
        transfer_logs = client.call("eth_getLogs", [{
            "fromBlock": hex(start), "toBlock": hex(end), "address": TAG_CONTRACT,
            "topics": [TRANSFER_TOPIC],
        }]) or []
        pool_logs = client.call("eth_getLogs", [{
            "fromBlock": hex(start), "toBlock": hex(end), "address": PRIMARY_POOL,
            "topics": [[V3_SWAP_TOPIC, V3_MINT_TOPIC, V3_BURN_TOPIC]],
        }]) or []
        labels = _verified_labels()
        block_times: dict[int, datetime] = {}

        def observed_at(block_number: int) -> datetime:
            if block_number not in block_times:
                block = client.call("eth_getBlockByNumber", [hex(block_number), False]) or {}
                block_times[block_number] = _iso_timestamp(_hex_int(block.get("timestamp")))
            return block_times[block_number]

        persisted = transfers = large_swaps = lp_events = holders = 0
        holder_addresses: set[str] = set()
        large_threshold = float(os.getenv("TAGNEXT_LARGE_SWAP_TOKENS", "1000000"))
        with session_scope() as session:
            for raw in transfer_logs:
                event = decode_transfer_log(raw, decimals=tag_decimals)
                holder_addresses.update(value for value in (event["from"], event["to"]) if value)
                label = labels.get(event["to"] or "") or labels.get(event["from"] or "")
                payload = {**event, "source": "direct_bnb_json_rpc", "label": label}
                event_id = _hash_id("tnoe", f"56:{event['txHash']}:{event['logIndex']}")
                if session.get(TagNextOnchainEventRow, event_id) is None:
                    session.add(TagNextOnchainEventRow(
                        event_id=event_id, chain_id=CHAIN_ID, event_type="transfer",
                        tx_hash=event["txHash"], log_index=event["logIndex"],
                        block_number=event["blockNumber"], observed_at=observed_at(event["blockNumber"]),
                        address_from=event["from"], address_to=event["to"],
                        token_quantity=event["tokenQuantity"], quote_quantity=None,
                        entity_confidence=1.0 if label else 0.0,
                        label_state="verified" if label else "unverified",
                        provenance_json=json_dumps(payload),
                    ))
                    transfers += 1
                    persisted += 1
            for raw in pool_logs:
                event = decode_v3_pool_log(
                    raw, token0=token0, token1=token1,
                    token0_decimals=token0_decimals, token1_decimals=token1_decimals,
                )
                if event["eventType"] == "large_swap" and event["tokenQuantity"] < large_threshold:
                    continue
                event_id = _hash_id("tnoe", f"56:{event['txHash']}:{event['logIndex']}")
                if session.get(TagNextOnchainEventRow, event_id) is None:
                    session.add(TagNextOnchainEventRow(
                        event_id=event_id, chain_id=CHAIN_ID, event_type=event["eventType"],
                        tx_hash=event["txHash"], log_index=event["logIndex"],
                        block_number=event["blockNumber"], observed_at=observed_at(event["blockNumber"]),
                        address_from=PRIMARY_POOL if event["tagDirection"] == "out_of_pool" else None,
                        address_to=PRIMARY_POOL if event["tagDirection"] == "into_pool" else None,
                        token_quantity=event["tokenQuantity"], quote_quantity=event["quoteQuantity"],
                        entity_confidence=None, label_state="pool_event",
                        provenance_json=json_dumps({**event, "source": "direct_bnb_json_rpc", "pool": PRIMARY_POOL}),
                    ))
                    persisted += 1
                    large_swaps += int(event["eventType"] == "large_swap")
                    lp_events += int(event["eventType"] in {"lp_mint", "lp_burn"})

        # Point-in-time holder balances only for addresses directly observed in
        # this bounded range. This is not claimed as a complete holder census.
        total_supply = _hex_int(client.eth_call(TAG_CONTRACT, "0x" + TOTAL_SUPPLY_SELECTOR, hex(end)))
        for address in sorted(holder_addresses)[:100]:
            if address == "0x" + "0" * 40:
                continue
            raw_balance = _balance_of(client, TAG_CONTRACT, address, block=hex(end))
            balance = raw_balance / (10 ** tag_decimals)
            share = raw_balance / total_supply if total_supply else None
            label = labels.get(address)
            entity_payload = {"chain": "bsc", "address": address, "label": label}
            entity_id = _hash_id("tnwe", f"bsc:{address}")
            observation_id = _hash_id("tnhh", f"{entity_id}:{TAG_CONTRACT}:{end}")
            with session_scope() as session:
                if session.get(TagNextWhaleEntityRow, entity_id) is None:
                    session.add(TagNextWhaleEntityRow(
                        entity_id=entity_id, address=address, chain="bsc",
                        label=str(label.get("label")) if label else None,
                        verification_state="verified" if label else "unverified",
                        entity_confidence=1.0 if label else 0.0,
                        provenance_json=json_dumps(entity_payload),
                    ))
                if session.get(TagNextHolderHistoryRow, observation_id) is None:
                    session.add(TagNextHolderHistoryRow(
                        observation_id=observation_id, entity_id=entity_id,
                        token_contract=TAG_CONTRACT, observed_at=observed_at(end),
                        balance=balance, share_of_supply=share,
                        provenance_json=json_dumps({
                            "source": "direct_bnb_json_rpc", "blockNumber": end,
                            "completeHolderCensus": False,
                        }),
                    ))
                    holders += 1
        return {
            "fromBlock": start, "toBlock": end, "eventsPersisted": persisted,
            "transfers": transfers, "largeSwaps": large_swaps, "lpEvents": lp_events,
            "holderSnapshots": holders, "completeHolderCensus": False,
            "exchangeLabels": "verified_only", "paidCalls": 0,
        }
    finally:
        if owned_rpc:
            client.close()


def onchain_payload(*, limit: int = 100) -> dict[str, Any]:
    with session_scope() as session:
        events = list(session.scalars(select(TagNextOnchainEventRow).order_by(
            TagNextOnchainEventRow.observed_at.desc()
        ).limit(max(1, min(limit, 500)))))
        holders = list(session.scalars(select(TagNextHolderHistoryRow).order_by(
            TagNextHolderHistoryRow.observed_at.desc()
        ).limit(max(1, min(limit, 500)))))
    return {
        "events": [{
            "eventId": row.event_id, "eventType": row.event_type,
            "txHash": row.tx_hash, "blockNumber": row.block_number,
            "observedAt": row.observed_at.isoformat(), "from": row.address_from,
            "to": row.address_to, "tokenQuantity": row.token_quantity,
            "quoteQuantity": row.quote_quantity, "entityConfidence": row.entity_confidence,
            "labelState": row.label_state,
        } for row in events],
        "holderSnapshots": [{
            "observationId": row.observation_id, "entityId": row.entity_id,
            "observedAt": row.observed_at.isoformat(), "balance": row.balance,
            "shareOfSupply": row.share_of_supply,
        } for row in holders],
        "completeHolderCensus": False,
        "source": "direct_bnb_json_rpc",
    }


def grade_due_onchain_event_outcomes(
    *, now: datetime | None = None, horizons: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Attach exact-deadline verified prices to immutable on-chain events."""
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    configured = dict(horizons or {"1h": 60, "24h": 1_440})
    written = unavailable = 0
    with session_scope() as session:
        events = list(session.scalars(select(TagNextOnchainEventRow).where(
            TagNextOnchainEventRow.observed_at <= current - timedelta(minutes=min(configured.values()))
        )))
        for event in events:
            for horizon, minutes in configured.items():
                deadline = event.observed_at + timedelta(minutes=minutes)
                if deadline > current:
                    continue
                existing = session.scalar(select(TagNextEventOutcomeRow).where(
                    TagNextEventOutcomeRow.event_id == event.event_id,
                    TagNextEventOutcomeRow.horizon == horizon,
                ))
                if existing is not None:
                    continue
                outcome = session.scalar(select(VerifiedOutcomeRow).where(
                    VerifiedOutcomeRow.asset_symbol == "TAG",
                    VerifiedOutcomeRow.observed_at == deadline,
                    VerifiedOutcomeRow.verification_status == "verified",
                ).order_by(VerifiedOutcomeRow.retrieved_at.asc()).limit(1))
                reference = session.scalar(select(VerifiedOutcomeRow).where(
                    VerifiedOutcomeRow.asset_symbol == "TAG",
                    VerifiedOutcomeRow.observed_at == event.observed_at,
                    VerifiedOutcomeRow.verification_status == "verified",
                ).order_by(VerifiedOutcomeRow.retrieved_at.asc()).limit(1))
                if outcome is None:
                    price = move = evidence_id = None
                    disposition = "exact_deadline_outcome_unavailable"
                    unavailable += 1
                else:
                    price = outcome.price_usd
                    move = (
                        (price / reference.price_usd - 1.0) * 100.0
                        if reference is not None and reference.price_usd > 0 else None
                    )
                    evidence_id = outcome.outcome_id
                    disposition = "graded_exact_deadline"
                    written += 1
                outcome_id = _hash_id("tneo", f"{event.event_id}:{horizon}:{deadline.isoformat()}")
                session.add(TagNextEventOutcomeRow(
                    outcome_id=outcome_id, event_id=event.event_id, horizon=horizon,
                    deadline=deadline, outcome_price=price, move_pct=move,
                    disposition=disposition, evidence_id=evidence_id,
                ))
    return {"graded": written, "outcomeUnavailable": unavailable, "exactDeadlineOnly": True}


def heatmap_payload(*, limit: int = 50) -> dict[str, Any]:
    """Keep observed provider maps, depth maps, and estimates visibly distinct."""
    safe_limit = max(1, min(limit, 200))
    with session_scope() as session:
        rows = list(session.scalars(select(TagNextHeatmapRow).order_by(
            TagNextHeatmapRow.observed_at.desc()
        ).limit(safe_limit)))
    grouped: dict[str, list[dict[str, Any]]] = {
        "observedProviderLiquidation": [],
        "observedOrderBook": [],
        "estimatedLiquidationRisk": [],
    }
    key_by_kind = {
        "observed_provider_liquidation": "observedProviderLiquidation",
        "observed_orderbook": "observedOrderBook",
        "estimated_liquidation_risk": "estimatedLiquidationRisk",
    }
    for row in rows:
        key = key_by_kind.get(row.kind)
        if key is None:  # illustrative bands are intentionally hidden from normal presentation
            continue
        grouped[key].append({
            "heatmapId": row.heatmap_id, "observedAt": row.observed_at.isoformat(),
            "kind": row.kind, "sourceIds": json.loads(row.source_ids_json or "[]"),
            "payload": json.loads(row.payload_json or "{}"),
            "modelVersion": row.model_version,
            "influencesForecast": bool(row.influences_forecast),
        })
    return {
        **grouped,
        "providerHeatmapStatus": "available" if grouped["observedProviderLiquidation"] else "not_configured",
        "illustrativeBandsPresented": False,
        "forecastInfluence": False,
    }
