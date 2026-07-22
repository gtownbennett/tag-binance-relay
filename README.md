# TAG Terminal Relay 2.6.0 RC1

This is an **additive upgrade built directly on Relay 2.5.0 Durable Intelligence**.
It does not replace the existing Chad analysis, prediction ledger, calibration,
freshness controls, decision-change history, backup/export, or automatic grading.

## Preserved v2.5 endpoints

- `/v1/chad/analyze`
- `/v1/chad/ledger` and `/v1/chad/ledger/export`
- `/v1/chad/performance`
- `/v1/chad/calibration`
- `/v1/chad/changes`
- `/v1/tag/freshness`
- `/v1/tag/snapshot`, `/v1/tag/spot`, `/v1/tag/history`, `/v1/tag/liquidations`

## New additive terminal endpoints

- `/v1/tag/market` — server-normalized five-exchange market view
- `/v1/tag/client-snapshot` — accepts the phone's validated snapshot
- `/v1/tag/terminal` — one payload for Chad, forecast, pattern, heatmap and alerts
- `/v1/tag/heatmap` — stored visible order-book persistence heatmap
- `/v1/tag/forecast` and `/v1/tag/patterns`
- `/v1/tag/alerts` and `/v1/tag/share-report`
- `/v1/admin/binance-vision/backfill` — protected historical import

## Accuracy rules

- Exact Binance taker B/S uses timestamped aggregate trades from the same trailing
  60-minute window. Until the relay has a complete uninterrupted hour, the API
  labels the value `binance-5m-history` or `warming-up`; it is never called exact.
- Missing/stale/contradictory data reduces confidence and stays visibly labeled.
- The internal heatmap is visible stored order-book liquidity, not a guaranteed
  exchange liquidation map.
- Binance liquidation data contains only forced-order snapshots observed while the
  relay is connected.

## Storage

`TERMINAL_DATABASE_URL` defaults to SQLite in `/tmp` for testing. That is not durable
on a free ephemeral service. Use PostgreSQL or a persistent disk before relying on
long-term history. The original v2.5 prediction ledger still uses `LEDGER_DB_PATH`;
configure that path on persistent storage separately.

## Test

```bash
pip install -r requirements.txt
python -m compileall -q app
python -m unittest discover -s tests -v
```

See `00-START-HERE-v2.6.0-RC1.txt` and `PRESERVATION-AUDIT.md`.
