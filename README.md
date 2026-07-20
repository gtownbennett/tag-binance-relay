# TAG Market Data Relay + Chad

This is a read-only relay for **public Binance USDⓈ-M futures market data** for
`TAGUSDT`. It contains no login, wallet, trading, account, order, withdrawal, or
API-secret functionality.

It is designed for the existing Android **TAG Terminal** app so one request can
fill the leverage screen instead of leaving most fields blank. Version **2.5.0** keeps primary-pair PancakeSwap spot confirmation and upgrades Chad into a recoverable decision-memory service. Every Chad analysis creates 6-hour, 24-hour, 3-day and 7-day forecasts, grades matured forecasts automatically in the background, records what changed between analyses, scores data freshness, and applies cautious confidence calibration after enough outcomes exist.


## Version 2.5.0 — Durable Intelligence

New in this release:

- Background forecast grading every five minutes without an OpenAI call
- Atomic JSON ledger backup after every saved prediction and graded outcome
- Startup restoration when the SQLite database is empty and a backup exists
- Protected ledger import endpoint for recovery from TAG Terminal's on-device copy
- Deterministic **Why Chad Changed His Mind** records
- Probability calibration bins, weak-horizon detection and confidence caps
- Futures/spot/depth freshness scoring with stale-data confidence controls
- Separate branch Docker image tag so v2.5.0 can be tested without replacing `latest`
- Unit tests for backup, restore, calibration, decision changes and stale-data controls

The relay remains read-only. It cannot trade, access an exchange account, move tokens or access a wallet.

## Data returned

`GET /v1/tag/spot`

- Primary TAG/WBNB PancakeSwap price and circulating market cap
- DEX liquidity, FDV and 5-minute/1-hour/6-hour/24-hour total volume
- Buy and sell transaction counts for each window
- Price change for each window
- Clear warning that buy/sell counts are transactions, not dollar buy/sell volume

`GET /v1/tag/snapshot`

- Mark price, index price, basis and current funding
- Current open interest in contracts and estimated USD value
- OI change over 5 minutes, 15 minutes, 1 hour and 4 hours
- Global account long/short ratio
- Top-trader account and position ratios when Binance returns them
- Taker buy/sell ratio and 5-minute taker volumes
- Binance futures 24-hour quote volume and price change
- Top-100-level order-book depth, imbalance and spread
- Liquidation events observed by the relay's Binance WebSocket connection
- An `errors` array instead of blanking the entire response when one field fails

`GET /v1/tag/history?period=5m&limit=100`

Returns the raw Binance history arrays for OI, ratios and taker flow.

`GET /v1/tag/liquidations`

Returns the liquidation events observed while the service has been running.

`POST /v1/chad/analyze`

Collects a fresh TAG futures snapshot, primary-pair DEX spot snapshot, selected Binance history and recent observed liquidations, then requests a structured leverage-first analysis from OpenAI.
The response includes Chad's plain-English summary, confidence, leverage
assessment, confirmation/invalidation levels, three probability scenarios and
data-quality warnings.

This endpoint requires both `OPENAI_API_KEY` and `RELAY_TOKEN` in Render. The
relay token is intentionally mandatory for Chad because each request can create
OpenAI API charges.

### Prediction ledger and intelligence endpoints

`GET /v1/chad/ledger`

Returns saved forecasts, due dates, actual outcomes, range hits, direction accuracy, error, score, post-mortems, calibration and storage status.

`GET /v1/chad/performance`

Returns overall and per-horizon accuracy, calibration and the background grader status. Chad does not treat the score as meaningful until at least eight horizons have been graded.

`GET /v1/chad/calibration`

Returns probability calibration bins, weak horizons, per-horizon confidence multipliers and the deterministic confidence cap.

`GET /v1/chad/changes`

Returns the server-side **Why Chad Changed His Mind** timeline by comparing adjacent analyses and their verified market evidence.

`POST /v1/chad/ledger/grade`

Forces a check of due forecasts without creating a new OpenAI analysis. The service also grades automatically in the background.

`GET /v1/chad/ledger/export`

Downloads the current ledger as JSON for backup.

`POST /v1/chad/ledger/import`

Restores or merges a `tag-terminal-prediction-ledger-v1` JSON export. This is designed for recovery from TAG Terminal's on-device backup after a server redeploy.

`POST /v1/chad/ledger/backup`

Forces an atomic JSON backup to `LEDGER_BACKUP_PATH`.

`GET /v1/tag/freshness`

Scores the age of the Binance market stream, depth stream, exchange event and PancakeSwap spot fetch. Chad's confidence is capped automatically when critical data is delayed, missing or stale.

Every Chad and ledger endpoint requires the same `X-Relay-Key`. Grading, import, export, backup, calibration and change-history requests do not call OpenAI and do not create an AI charge.

**Storage reality:** the default paths are under `/tmp`, so both SQLite and its JSON sidecar can be erased together after a Render restart or redeploy. Full server-side durability requires a persistent disk and paths such as `/var/data/tag_prediction_ledger.sqlite3`. Without a disk, use `GET /v1/chad/ledger/export` plus `POST /v1/chad/ledger/import`; TAG Terminal v0.4.1 already keeps an on-device copy that can become the recovery source.

## Important limitation

The liquidation stream begins collecting when the relay starts. It does not
reconstruct liquidations that occurred before startup, and Binance describes
the stream as liquidation snapshots rather than a guaranteed complete ledger.

## Deploy from GitHub to Render Frankfurt

### 1. Put this folder in GitHub

1. Create a new GitHub repository named `tag-binance-relay`.
2. Upload every file in this folder.
3. Commit the files to the default branch.

Do **not** put a Binance login, Binance password, Binance account cookie, wallet
seed phrase, or trading API secret in GitHub. None is needed.

### 2. Create the Render service

1. Sign in to Render.
2. Choose **New → Blueprint**.
3. Connect the `tag-binance-relay` GitHub repository.
4. Render reads `render.yaml` and creates a Docker web service in **Frankfurt**.
5. Deploy it.

The supplied Blueprint leaves `plan` unspecified. Render currently treats that
as the **Starter** plan, which is the better choice for keeping the liquidation
WebSocket alive. For a proof-of-concept only, add this under `region: frankfurt`
in `render.yaml` before deploying:

```yaml
    plan: free
```

A free service may sleep, which interrupts liquidation collection until it wakes
again.

The service URL will look similar to:

```text
https://tag-binance-relay-xxxx.onrender.com
```

### 3. Test it

Open:

```text
https://YOUR-SERVICE.onrender.com/health
```

You want:

```json
{
  "ok": true,
  "binanceReachable": true
}
```

Then open:

```text
https://YOUR-SERVICE.onrender.com/v1/tag/snapshot
```

You should see a large JSON response with `openInterestUsd`, `fundingRate`,
`oiChange5mPct`, `takerBuySellRatio`, order-book fields and an `errors` array.

If `/health` reports Binance HTTP 451, confirm that the Render service region is
Frankfurt. Delete and recreate the service if it was accidentally created in a
U.S. region because Render does not let a service's region be changed later.

## Optional relay key

The market data is public, but an open relay can be abused by strangers.

In Render:

1. Open the service.
2. Open **Environment**.
3. Add `RELAY_TOKEN` with a long random value.
4. Save and redeploy.

Requests must then send:

```text
X-Relay-Key: your-long-random-value
```

The Android replacement `ApiClient.kt` already sends the value saved in the
app's existing Settings key box. Until the Settings wording is renamed, that box
can hold the relay token.

## Enable and test Chad

In Render → **Environment**, add:

```text
OPENAI_API_KEY = your private replacement OpenAI project key
RELAY_TOKEN = a long random secret you create
```

Optional tuning variables:

```text
OPENAI_MODEL = gpt-5.5
OPENAI_REASONING_EFFORT = low
OPENAI_MAX_OUTPUT_TOKENS = 8000
OPENAI_TIMEOUT_SECONDS = 75
LEDGER_ENABLED = true
LEDGER_DB_PATH = /tmp/tag_prediction_ledger.sqlite3
LEDGER_BACKUP_PATH = /tmp/tag_prediction_ledger.sqlite3.backup.json
LEDGER_AUTO_BACKUP = true
LEDGER_AUTO_GRADE_SECONDS = 300
LEDGER_DEADBAND_PCT = 1.0
LEDGER_MAX_RECORDS = 5000
FRESHNESS_WARN_SECONDS = 90
FRESHNESS_STALE_SECONDS = 300
```

After the deployment becomes live, open `/docs`, expand
`POST /v1/chad/analyze`, click **Try it out**, and supply the same `RELAY_TOKEN`
in the `X-Relay-Key` header. A small test body is:

```json
{
  "question": "What is TAG doing right now?",
  "historyPeriod": "5m",
  "historyLimit": 72,
  "positionTag": 100812406,
  "averageEntryUsd": 0.00014105,
  "forceFresh": true,
  "includeRawHistory": false
}
```

The DEX Screener pair endpoint is cached by the relay and is within the official 300 requests-per-minute pair-endpoint limit. The OpenAI key is read only by the Render server. It is never returned by the
API and must never be placed in Android source code or GitHub.

## Connect the Android TAG Terminal

The `android` folder contains replacements based on the current app structure.

### 1. Replace Models.kt

Replace:

```text
app/src/main/java/com/eric/tagterminal/Models.kt
```

with:

```text
android/Models.kt
```

### 2. Replace ApiClient.kt

Replace:

```text
app/src/main/java/com/eric/tagterminal/ApiClient.kt
```

with:

```text
android/ApiClient.kt
```

Inside the replacement, change:

```kotlin
private const val RELAY_BASE_URL =
    "https://YOUR-RENDER-SERVICE.onrender.com"
```

to the exact Render URL. Do not include a trailing slash.

### 3. Replace the Leverage function

Open:

```text
app/src/main/java/com/eric/tagterminal/MainActivity.kt
```

Find the entire function beginning:

```kotlin
@Composable private fun Leverage
```

Replace that whole function with the contents of:

```text
android/LeverageScreenReplacement.txt
```

### 4. Rename two Settings labels

In `MainActivity.kt`, change:

```text
CoinGlass key is encrypted locally with Android Keystore.
```

to:

```text
Optional relay key is encrypted locally with Android Keystore.
```

Change both occurrences of `CoinGlass API key` to `Relay access key`.

The existing encrypted local key storage and ViewModel call can remain in place.

### 5. Build

In Android Studio:

```text
Build → Clean Project
Build → Rebuild Project
```

Then install the new debug APK.

## Automated tests

The `Test TAG relay` GitHub Action runs on `main` and `v2.5.0-durable-intelligence`. Locally:

```text
python -m unittest discover -s tests -v
```

## Local test without Render

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
uvicorn app.main:app --reload
```

Open:

```text
http://127.0.0.1:8000/health
```

A U.S. home connection may receive Binance HTTP 451. That is expected if the
public futures host is unavailable from that network; the point of the Frankfurt
relay is for the outbound request to originate there.

## Environment variables

See `.env.example`.

- `RELAY_TOKEN`: optional for public market-data endpoints; required for Chad
- `OPENAI_API_KEY`: private server-side OpenAI project key
- `OPENAI_MODEL`: defaults to `gpt-5.6-luna`
- `OPENAI_REASONING_EFFORT`: defaults to `low`
- `OPENAI_MAX_OUTPUT_TOKENS`: defaults to 2200
- `OPENAI_TIMEOUT_SECONDS`: defaults to 75
- `BINANCE_SYMBOL`: defaults to `TAGUSDT`
- `BINANCE_REST_BASE`: defaults to `https://fapi.binance.com`
- `BINANCE_WS_BASE`: defaults to `wss://fstream.binance.com`
- `CACHE_SECONDS`: defaults to 15 seconds

## Operational notes

- A sleeping/free host stops the liquidation WebSocket and loses in-memory
  liquidation history. For continuous liquidation tracking, use a host plan that
  stays awake.
- The REST metrics still work after a cold start.
- For durable historical analysis, add a managed database later. This first
  version focuses on eliminating blank live leverage fields.
