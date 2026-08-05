# TAG Terminal Relay 2.8.7 RC6.5 — Binance-safe grading

## RC6.5 grading correction

Forecast grading no longer depends on the geo-blocked Binance Futures REST
`/fapi/v1/klines` route. Exact-due prices are resolved in this order:

1. durable Binance relay snapshots;
2. stored Binance Vision futures candles;
3. the public completed-day Binance Vision futures archive.

Every successful grade retains its exact sample timestamp, offset, tolerance,
and source. Another exchange is never silently substituted. Binance archive
timestamps are normalized across seconds, milliseconds, and microseconds.

This release preserves the fail-closed, manual-only Chad reactivation gate and
extends the structured result to the terminal's complete 25-horizon forecast.
The existing Render `OPENAI_API_KEY` is referenced only at runtime; no key is
included in this repository and no live OpenAI call is made by the test suite.

## Safe defaults

With missing environment flags, the relay starts with:

- `REPAIR_MODE=true`
- `LIVE_COLLECTORS_ENABLED=false`
- `BACKFILL_ENABLED=false`
- `OPENAI_AUTOMATIC_ENABLED=false`
- `CHAD_REACTIVATION_ENABLED=false`
- `CHAD_KILL_SWITCH=true`
- `CHAD_FORECAST_WRITES_ENABLED=false`
- `CHAD_GRADING_ENABLED=false`
- `CHAD_CACHE_WRITES_ENABLED=false`
- `OPENAI_PROJECT_BUDGET_CONFIRMED=false`
- `PUSH_ENABLED=false`
- `DB_BOOTSTRAP_ON_START=false`

The Render blueprint sets the same values explicitly. Provider spending limits remain the final cap and must not be raised automatically.

## Chad reactivation safety

- Chad remains unavailable unless all six independent reactivation/budget/write
  switches agree and the relay token plus existing server-side OpenAI key are
  configured.
- The endpoint stays authenticated, manual-only, and requires explicit paid,
  forecast-lock, and grading approval on each request.
- One request can make at most one OpenAI attempt. Network and API failures are
  never retried automatically.
- One Chad request may run at a time, requests have a five-minute cooldown, and
  the default in-process ceilings are two requests/calls per day and twenty per
  month.
- Per-request ceilings stop work before exceeding 40 database queries, 12
  database writes, 20 external reads, or one OpenAI call.
- The OpenAI output cap is 4,000 tokens and cannot be raised above 5,000 by an
  environment variable.
- Durable OpenAI evidence-hash caching is mandatory before the gate can open;
  cache hits no longer create a Neon write.
- Forecast IDs are deterministic from the evidence hash, so the same evidence
  cannot create a duplicate locked forecast after a retry or restart.
- New paid calls are blocked before OpenAI when the six-hour forecast cadence
  cannot accept a new immutable forecast.
- Regime changes require two observations separated by at least five minutes;
  an unconfirmed change cannot lock a forecast.
- Due grading is limited to four horizons per batch and accepts only a 5-minute
  Binance close within 15 minutes of the exact forecast due time.
- `/health` exposes blockers, safe feature flags, request/cache/retry telemetry,
  token counts, and budget usage without exposing credentials or querying Neon.
- PostgreSQL ledgers no longer export the entire history after each new forecast
  write, removing an avoidable Neon egress pattern.

## Cost repairs

- Repair mode no longer runs `create_all`, timestamp-column migrations, index
  creation, or legacy ledger bootstrap on every Render wake. The verified
  schema is used as-is, avoiding the 27 startup queries observed on RC6.
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
- RC6.2 keeps repair mode enabled and permits only the narrow Chad route after every reactivation gate passes; unrelated writes and automatic work remain blocked.
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

- Raw overlapping grades are preserved, but accuracy and calibration now use
  one representative record per horizon-sized UTC evaluation cohort.
- The old `104 graded 24-hour forecasts` display is no longer treated as 104
  independent samples. The live audit found only two 24-hour evaluation
  cohorts, which is far too small for a trustworthy score.
- New Chad reports cannot be stored more often than every 15 minutes.
- New deterministic forecasts are separated by a horizon-aware cadence:
  1 hour minimum and 6 hours maximum.
- A forecast is graded only when a stored market snapshot is within 15 minutes
  of its exact due time, and the closest eligible snapshot is selected.
- Future grade rows retain sample time, offset, tolerance, outcome, price
  change, and correctness metadata inside the existing JSON audit field.
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

Expected: 48 tests pass.

Read `CHANGELOG-v2.8.6-RC6.3.txt` and
`VALIDATION-v2.8.6-RC6.3.txt`. Also retain
`FORECAST-GRADE-AUDIT-v2.8.4-RC6.1.txt`. Older release files are history only.
