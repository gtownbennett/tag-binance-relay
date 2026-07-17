# TAG Binance Market Data Relay

This is a read-only relay for **public Binance USDⓈ-M futures market data** for
`TAGUSDT`. It contains no login, wallet, trading, account, order, withdrawal, or
API-secret functionality.

It is designed for the existing Android **TAG Terminal** app so one request can
fill the leverage screen instead of leaving most fields blank.

## Data returned

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

- `RELAY_TOKEN`: optional protection for relay endpoints
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
