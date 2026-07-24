# TAG Terminal Relay 2.7.0 RC2 — Paper, Social Calls and Alert History

This is an additive release candidate built on the verified durable TAG relay. It preserves the existing five-exchange market collector, PostgreSQL history, Chad analysis, prediction grading, freshness rules, Binance Vision import and protected relay connection.

## RC2 additions

- Persistent alert timeline: first seen → candidate → confirmed → invalidated/resolved.
- Dedicated TAG paper-futures simulator with a $10,000 paper-USDT account.
- PAPER / NO REAL FUNDS enforcement; no exchange login, trading key, wallet or real order route.
- Isolated-margin Market, Limit and Trigger paper orders with transparent estimated fees, funding and liquidation risk.
- Paper positions, pending orders, realized/unrealized P/L, equity curve, grades and post-mortems.
- Persistent social-caller and social-call scorecards with exact-time provenance and no guessed timestamps or entries.
- Optional official CoinMarketCap Content API ingestion and historical quote comparison.
- Stored DEX/exchange evidence around each social call, including OI, funding, taker flow and source status.
- Consolidated forecast read model that displays both the OpenAI Chad ledger and deterministic Terminal ledger, preserves their source labels and grades due outcomes automatically.
- Server-created test alert for Android notification diagnostics.

## Main TAG Terminal endpoints

- `GET /v1/tag/terminal` — complete Android payload, including unified grading, alerts, paper and social ledgers.
- `GET /v1/tag/predictions/unified` — source-labelled consolidated forecast record.
- `GET /v1/tag/alert-timeline` — alert-stage history and active alert paths.
- `POST /v1/tag/alerts/test` — protected diagnostic alert; not a market signal.
- `GET /v1/tag/paper` — paper account, trades and equity history.
- `POST /v1/tag/paper/orders` — paper-only Market/Limit/Trigger order.
- `POST /v1/tag/paper/trades/{id}/close` and `/v1/tag/paper/orders/{id}/cancel`.
- `GET /v1/tag/social` — callers, calls, grades, returns and evidence provenance.
- Admin-only social import/research endpoints under `/v1/admin/social/`.

Existing `/v1/chad/*`, market, heatmap, liquidations, forecast, patterns, Binance Vision and history endpoints remain available.

## Social-call timestamp rules

An exact post time is accepted only from source metadata such as the official CMC `post_time`, ISO metadata, JSON-LD or an explicit `<time datetime>` value. Relative labels such as “2h ago” are never converted into an invented timestamp. An entry is aligned only when a stored DEX snapshot or optional CMC historical quote is sufficiently close to that exact post time.

Automatic CMC polling requires `CMC_PRO_API_KEY`. `CMC_TAG_ID` enables supplemental historical quote comparisons. Without those variables, the ledger stays operational for verified manual/admin imports and explicitly reports that CMC automation is not configured.

## Accuracy and safety

- Missing, stale or contradictory values stay missing and reduce confidence.
- Exact Binance taker B/S uses the same timestamped trailing 60-minute window.
- The internal heatmap is stored visible order-book liquidity, not a guaranteed liquidation map.
- Paper liquidation, funding and fees are educational estimates, never exchange guarantees.
- No automatic real orders can be created by this service.

## Storage

Set `TERMINAL_DATABASE_URL` or Render's `DATABASE_URL` to PostgreSQL. New SQLAlchemy tables are created additively; the upgrade does not drop prior history. SQLite under `/tmp` is only a temporary local/test fallback.

## Validation

```bash
pip install -r requirements.txt
python -m py_compile app/*.py
python -m unittest discover -s tests -v
python -c "from app.main import app; print(len(app.openapi()['paths']))"
```

See `START-HERE-v2.7.0-RC2.txt` and `VALIDATION-v2.7.0-RC2.txt` before deployment.
