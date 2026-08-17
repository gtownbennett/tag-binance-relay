"""Observe Binance's public TAGUSDT force-order stream for a bounded window."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.terminal_addon import terminal_addon


ENDPOINTS = (
    "wss://fstream.binance.com/stream?streams=tagusdt@forceOrder",
    "wss://fstream.binance.com/market/stream?streams=tagusdt@forceOrder",
)


def parse_force_order(payload: dict) -> dict | None:
    wrapper = payload.get("data", payload)
    if not isinstance(wrapper, dict) or wrapper.get("e") != "forceOrder":
        return None
    order = wrapper.get("o")
    if not isinstance(order, dict) or str(order.get("s", "")).upper() != "TAGUSDT":
        return None
    try:
        side = str(order.get("S", "")).upper()
        price = float(order.get("ap") or order.get("p"))
        quantity = float(order.get("z") or order.get("l") or order.get("q"))
        event_time = int(order.get("T") or wrapper.get("E") or time.time() * 1000)
    except (TypeError, ValueError):
        return None
    return {
        "time": event_time,
        "timeIso": datetime.fromtimestamp(event_time / 1000, tz=timezone.utc).isoformat(),
        "liquidationSide": "LONG" if side == "SELL" else "SHORT",
        "orderSide": side,
        "price": price,
        "quantity": quantity,
        "notionalUsd": price * quantity,
        "source": "Binance public TAGUSDT forceOrder WebSocket",
        "observedProviderLiquidation": True,
    }


async def observe(duration_seconds: int) -> dict:
    started = datetime.now(timezone.utc)
    attempts: list[dict] = []
    events: list[dict] = []
    message_count = 0
    deadline = time.monotonic() + duration_seconds
    for endpoint in ENDPOINTS:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        attempt = {"endpoint": endpoint, "connected": False, "error": None}
        attempts.append(attempt)
        try:
            async with websockets.connect(endpoint, ping_interval=15, ping_timeout=15, close_timeout=5) as websocket:
                attempt["connected"] = True
                while (remaining := deadline - time.monotonic()) > 0:
                    try:
                        raw = await asyncio.wait_for(websocket.recv(), timeout=min(remaining, 10))
                    except asyncio.TimeoutError:
                        continue
                    message_count += 1
                    payload = json.loads(raw)
                    event = parse_force_order(payload if isinstance(payload, dict) else {})
                    if event:
                        terminal_addon.persist_liquidation(event)
                        events.append(event)
                break
        except Exception as exc:
            attempt["error"] = f"{type(exc).__name__}: {exc}"
    ended = datetime.now(timezone.utc)
    return {
        "provider": "Binance",
        "symbol": "TAGUSDT",
        "stream": "forceOrder",
        "startedAt": started.isoformat(),
        "endedAt": ended.isoformat(),
        "durationSeconds": (ended - started).total_seconds(),
        "attempts": attempts,
        "connected": any(row["connected"] for row in attempts),
        "messageCount": message_count,
        "actualLiquidationEventCount": len(events),
        "events": events,
        "classification": "actual_provider_liquidation_events" if events else "no_event_observed_in_bounded_window",
        "limitations": "The public force-order stream is live-only and is not a complete historical liquidation ledger.",
        "accountRequired": False,
        "paidCalls": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=45)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = asyncio.run(observe(max(5, min(args.seconds, 55))))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({key: payload[key] for key in (
        "connected", "durationSeconds", "messageCount", "actualLiquidationEventCount", "classification"
    )}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
