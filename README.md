# TAG Terminal Relay 2.8.2 RC5 — Fast Manual-Live Repair

This release retains RC3/RC4 cost containment and fixes the RC4 phone timeout. Public market sources now run concurrently, each non-Binance venue has a 12-second overall deadline, and the repair-mode manual packet returns without waiting for Neon.

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
- The repair-mode manual packet does not query Neon; stored history and intelligence are deliberately deferred until a separate bounded history path is validated.
- Manual reads never write snapshots, start a collector, call OpenAI, grade ledgers, poll social sources, or send notifications.
- Manual reads have separate daily/monthly circuit limits.
- `GET /v1/tag/terminal` without `manual=true` remains stored-evidence-only and cannot contact exchanges.
- Core spot/leverage data still returns if an optional stored-intelligence table is unavailable.
- GET/read routes no longer create Chad reports, forecasts, grades, alerts, paper accounts, or social-grade writes.
- Client snapshot ingestion, paper/social mutations, history collection, backfills, collectors, WebSockets, CMC polling, grading, and all OpenAI are blocked in repair mode.
- After repair mode is deliberately disabled, an OpenAI call still requires an authenticated explicit request with `allowPaidCall=true`; results are cached by stable evidence hash.
- Database reads use narrow columns, bounded windows, composite indexes, and a two-connection PostgreSQL pool.
- Daily/monthly in-process circuit breakers and usage/cache telemetry are exposed through `operatingStatus`.

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

Expected: 25 tests pass.

Read `00-START-HERE-v2.8.2-RC5.txt` and `VALIDATION-v2.8.2-RC5.txt`. Older release files are history only.
