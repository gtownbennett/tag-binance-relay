# TAG Terminal Relay 2.8.3 RC6 — Bounded Neon Intelligence

This release keeps RC5's fast live market packet and restores a compact, read-only slice of the existing Neon intelligence. Market and Neon work start concurrently, the database slice has an eight-second wait limit inside a 24-second total response budget, and a slow database can never discard an otherwise valid live packet.

## Safe defaults

With missing environment flags, the relay starts with:

- `REPAIR_MODE=true`
- `LIVE_COLLECTORS_ENABLED=false`
- `BACKFILL_ENABLED=false`
- `OPENAI_AUTOMATIC_ENABLED=false`
- `PUSH_ENABLED=false`

The Render blueprint sets the same values explicitly. Provider spending limits remain the final cap and must not be raised automatically.

## Cost repairs

- `/health` uses memory only; it never queries Neon or pings an exchange.
- `GET /v1/tag/terminal?manual=true` performs one user-requested public market read, is cached for five minutes, and coalesces concurrent refreshes.
- Binance, DEX Screener, Bitget, MEXC, Gate, and BingX are requested in one concurrent window instead of two sequential phases.
- The manual packet can perform one separately governed, read-only Neon intelligence slice with at most twelve fixed `SELECT` statements.
- Neon returns only selected Chad JSON fields rather than complete stored report blobs. On the protected production branch, the newest eight report fields measured 6,627 bytes instead of 132,715 bytes for the full blobs.
- Stored results are capped at eight Chad history entries, 42 forecast records, 12 alerts, 80 alert transitions, 30 paper trades, 60 paper-equity points, 12 social callers, and 24 social calls.
- The compact intelligence payload has a 240,000-byte ceiling and never selects snapshot-table payload blobs.
- The Neon read starts concurrently with public market collection, waits no more than eight seconds, and cannot push the complete terminal route beyond its 24-second response budget.
- If Neon is unavailable or slow, the already-validated RC5 live market packet still returns.
- Manual reads never write snapshots, start a collector, call OpenAI, grade ledgers, poll social sources, or send notifications.
- Manual market and bounded-intelligence reads have separate daily/monthly circuit limits.
- `GET /v1/tag/terminal` without `manual=true` remains stored-evidence-only and cannot contact exchanges.
- Core spot/leverage data still returns if an optional stored-intelligence table is unavailable.
- GET/read routes no longer create Chad reports, forecasts, grades, alerts, paper accounts, or social-grade writes.
- Client snapshot ingestion, paper/social mutations, history collection, backfills, collectors, WebSockets, CMC polling, grading, and all OpenAI are blocked in repair mode.
- After repair mode is deliberately disabled, an OpenAI call still requires an authenticated explicit request with `allowPaidCall=true`; results are cached by stable evidence hash.
- Database reads use narrow columns, bounded windows, composite indexes, and a two-connection PostgreSQL pool.
- Daily/monthly in-process circuit breakers and usage/cache telemetry are exposed through `operatingStatus`.

## Restored read-only intelligence

- Chad history is restored as timestamped archive entries.
- Deterministic forecast records, grades, calibration counts, and unified performance are restored without running a grader.
- Alerts and alert-stage history are restored with visible `Stored` labels so old states are not presented as newly evaluated alerts.
- Paper-trading history and a bounded equity curve are restored without creating an account or changing a trade.
- Verified social callers and calls are restored without polling or grading.
- The current Chad card shows live market data quality and a deterministic specialist evidence summary, while clearly marking new analysis and Future Paths as deferred.

## Accuracy repairs

- A confirmed alert cannot fall back to candidate/observed from one noisy snapshot.
- Current heatmap bids must be below the current mark and asks above it; historical books are retained only as sample counts.
- Historical analogs are bounded and separated in time.
- Forecast reports and equivalent forecasts are deduplicated.
- Unified forecast status remains collecting/warming until every required horizon is independently calibrated.
- Multi-exchange feeds expose per-metric coverage and sanitized errors instead of claiming fully live data from a partial response.

## Validation

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
mkdir -p .testdata
TERMINAL_DATABASE_URL=sqlite:///./.testdata/relay.sqlite3 \
LEDGER_DATABASE_URL=sqlite:///./.testdata/ledger.sqlite3 \
REPAIR_MODE=true \
.venv/bin/python -m unittest discover -s tests -v
```

Expected: 30 tests pass.

Read `00-START-HERE-v2.8.3-RC6.txt` and `VALIDATION-v2.8.3-RC6.txt`. Older release files are history only.
