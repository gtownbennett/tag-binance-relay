# TAG Terminal Relay 2.8.0 RC3 — Cost-Safe Repair Release

This release contains the RC2 paper/social system and an emergency repair mode designed to stop uncontrolled Neon, Render, and OpenAI usage.

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
- `GET /v1/tag/terminal` is read-only, bounded, cached for five minutes in repair mode, and coalesces concurrent refreshes.
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

Expected: 19 tests pass.

Read `00-START-HERE-v2.8.0-RC3.txt` and `VALIDATION-v2.8.0-RC3.txt`. Older release files are history only.
