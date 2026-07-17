package com.eric.tagterminal

import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.TimeUnit

private const val TAG_CIRCULATING_SUPPLY = 108_404_572_594.0

// CHANGE ONLY THIS LINE after Render gives you the service URL.
private const val RELAY_BASE_URL = "https://YOUR-RENDER-SERVICE.onrender.com"

class ApiClient {

    private val client = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(25, TimeUnit.SECONDS)
        .build()

    fun fetchSpot(): SpotSnapshot {
        val pairAddress =
            "0xf0750c373ebbb3baeef7e03d8300caad1983d67c"

        val request = Request.Builder()
            .url("https://api.dexscreener.com/latest/dex/pairs/bsc/$pairAddress")
            .get()
            .build()

        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                throw IllegalStateException(
                    "DexScreener request failed: HTTP ${response.code}"
                )
            }

            val body = response.body?.string()
                ?: throw IllegalStateException(
                    "DexScreener returned an empty response."
                )

            val root = JSONObject(body)

            val pair = root.optJSONArray("pairs")
                ?.optJSONObject(0)
                ?: root.optJSONObject("pair")
                ?: throw IllegalStateException(
                    "TAG/WBNB pair was not found."
                )

            val liquidity = pair.optJSONObject("liquidity")
            val volume = pair.optJSONObject("volume")
            val priceChange = pair.optJSONObject("priceChange")
            val transactions = pair
                .optJSONObject("txns")
                ?.optJSONObject("h1")

            val priceUsd = pair.readDouble("priceUsd")

            return SpotSnapshot(
                priceUsd = priceUsd,
                marketCap = priceUsd?.times(TAG_CIRCULATING_SUPPLY),
                fdv = pair.readDouble("fdv"),
                liquidityUsd = liquidity?.readDouble("usd"),
                volumeH1 = volume?.readDouble("h1"),
                volumeH24 = volume?.readDouble("h24"),
                buysH1 = transactions?.readInt("buys"),
                sellsH1 = transactions?.readInt("sells"),
                priceChangeH1 = priceChange?.readDouble("h1"),
                priceChangeH24 = priceChange?.readDouble("h24")
            )
        }
    }

    /**
     * The existing ViewModel already calls fetchFutures(apiKey).
     * Enter the optional RELAY_TOKEN in the app's Settings box.
     * Leave it blank when RELAY_TOKEN is blank on Render.
     */
    fun fetchFutures(apiKey: String): FuturesSnapshot {
        val requestBuilder = Request.Builder()
            .url("$RELAY_BASE_URL/v1/tag/snapshot")
            .get()

        if (apiKey.isNotBlank()) {
            requestBuilder.header("X-Relay-Key", apiKey.trim())
        }

        client.newCall(requestBuilder.build()).execute().use { response ->
            val body = response.body?.string()

            if (!response.isSuccessful) {
                throw IllegalStateException(
                    "Relay request failed: HTTP ${response.code}" +
                        if (body.isNullOrBlank()) "" else " — $body"
                )
            }

            val root = JSONObject(
                body ?: throw IllegalStateException(
                    "Relay returned an empty response."
                )
            )

            return FuturesSnapshot(
                source = root.optString("source", ""),
                markPrice = root.readDouble("markPrice"),
                indexPrice = root.readDouble("indexPrice"),
                basisBps = root.readDouble("basisBps"),

                openInterestUsd = root.readDouble("openInterestUsd"),
                openInterestContracts = root.readDouble("openInterestContracts"),
                oiChange5mPct = root.readDouble("oiChange5mPct"),
                oiChange15mPct = root.readDouble("oiChange15mPct"),
                oiChange1hPct = root.readDouble("oiChange1hPct"),
                oiChange4hPct = root.readDouble("oiChange4hPct"),

                fundingRate = root.readDouble("fundingRate"),
                longShortRatio = root.readDouble("globalLongShortRatio"),
                globalLongPct = root.readDouble("globalLongAccountPct"),
                globalShortPct = root.readDouble("globalShortAccountPct"),
                topAccountRatio = root.readDouble("topAccountRatio"),
                topPositionRatio = root.readDouble("topPositionRatio"),

                takerBuySellRatio = root.readDouble("takerBuySellRatio"),
                takerBuyVolume5m =
                    root.readDouble("takerBuyVolumeContracts5m"),
                takerSellVolume5m =
                    root.readDouble("takerSellVolumeContracts5m"),

                futuresQuoteVolume24hUsd =
                    root.readDouble("futuresQuoteVolume24hUsd"),
                futuresPriceChange24hPct =
                    root.readDouble("futuresPriceChange24hPct"),

                bidDepthUsd = root.readDouble("bidDepthUsdTop100"),
                askDepthUsd = root.readDouble("askDepthUsdTop100"),
                orderBookImbalancePct =
                    root.readDouble("orderBookImbalancePct"),
                spreadBps = root.readDouble("spreadBps"),

                longLiquidation1h =
                    root.readDouble("longLiquidation1hUsd"),
                shortLiquidation1h =
                    root.readDouble("shortLiquidation1hUsd"),
                longLiquidation24h =
                    root.readDouble("longLiquidationTracked24hUsd"),
                shortLiquidation24h =
                    root.readDouble("shortLiquidationTracked24hUsd"),
                liquidationTrackerConnected =
                    root.optBoolean("trackerConnected", false),

                updatedAt = root.optString("relayGeneratedAt", ""),
                errors = root.optJSONArray("errors").toStringList()
            )
        }
    }
}

private fun JSONObject.readDouble(key: String): Double? {
    if (!has(key) || isNull(key)) return null

    return when (val value = opt(key)) {
        is Number -> value.toDouble()
        is String -> value.toDoubleOrNull()
        else -> null
    }
}

private fun JSONObject.readInt(key: String): Int? {
    if (!has(key) || isNull(key)) return null

    return when (val value = opt(key)) {
        is Number -> value.toInt()
        is String -> value.toIntOrNull()
        else -> null
    }
}

private fun JSONArray?.toStringList(): List<String> {
    if (this == null) return emptyList()

    return buildList {
        for (index in 0 until length()) {
            optString(index, null)?.let(::add)
        }
    }
}
