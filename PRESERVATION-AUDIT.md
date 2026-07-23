# Relay 2.5 → 2.6 Preservation Audit

## Verified preserved

- Every route decorator present in the uploaded Relay 2.5 source is still present.
- `app/ledger.py` is byte-for-byte unchanged.
- OpenAI Chad structured analysis remains at `/v1/chad/analyze`.
- Forecast ledger, automatic grading, calibration, performance, export/import,
  backups, similar setups and decision-change history remain intact.
- Existing Binance market/depth/liquidation streams and DexScreener spot remain.

## Additive changes

- Exact same-window trailing-hour Binance taker flow with labeled warmup fallback.
- Server-side market history and tolerant 5m/15m/1h/4h/24h comparisons.
- Server-side Bitget/MEXC/Gate/BingX normalization and source status.
- Stored order-book snapshots, internal heatmap, observed liquidations and alerts.
- Deterministic terminal report used when protected OpenAI Chad is unavailable.
- Binance Vision importer and protected admin endpoint.

## Deliberately not claimed

- No live Render deployment was performed in the packaging environment.
- No third-party paid CoinGlass/Hyblock heatmap API is bundled. The app links to
  those tools and labels its internal heatmap accurately.
- No instant FCM push is enabled; Android uses approximately 15-minute polling.
- Exit AI remains safety-locked until router quotes/slippage are validated.
