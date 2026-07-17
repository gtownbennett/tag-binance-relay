package com.eric.tagterminal

data class SpotSnapshot(
    val priceUsd: Double? = null,
    val marketCap: Double? = null,
    val fdv: Double? = null,
    val liquidityUsd: Double? = null,
    val volumeH1: Double? = null,
    val volumeH24: Double? = null,
    val buysH1: Int? = null,
    val sellsH1: Int? = null,
    val priceChangeH1: Double? = null,
    val priceChangeH24: Double? = null
)

data class FuturesSnapshot(
    val source: String = "",
    val markPrice: Double? = null,
    val indexPrice: Double? = null,
    val basisBps: Double? = null,

    val openInterestUsd: Double? = null,
    val openInterestContracts: Double? = null,
    val oiChange5mPct: Double? = null,
    val oiChange15mPct: Double? = null,
    val oiChange1hPct: Double? = null,
    val oiChange4hPct: Double? = null,

    val fundingRate: Double? = null,
    val longShortRatio: Double? = null,
    val globalLongPct: Double? = null,
    val globalShortPct: Double? = null,
    val topAccountRatio: Double? = null,
    val topPositionRatio: Double? = null,

    val takerBuySellRatio: Double? = null,
    val takerBuyVolume5m: Double? = null,
    val takerSellVolume5m: Double? = null,

    val futuresQuoteVolume24hUsd: Double? = null,
    val futuresPriceChange24hPct: Double? = null,

    val bidDepthUsd: Double? = null,
    val askDepthUsd: Double? = null,
    val orderBookImbalancePct: Double? = null,
    val spreadBps: Double? = null,

    val longLiquidation1h: Double? = null,
    val shortLiquidation1h: Double? = null,
    val longLiquidation24h: Double? = null,
    val shortLiquidation24h: Double? = null,
    val liquidationTrackerConnected: Boolean = false,

    val updatedAt: String = "",
    val errors: List<String> = emptyList()
)

data class DashboardState(
    val loading: Boolean = false,
    val spot: SpotSnapshot = SpotSnapshot(),
    val futures: FuturesSnapshot = FuturesSnapshot(),
    val classification: String = "Tap Refresh to load live TAG data.",
    val updatedAt: String = "",
    val apiKeySaved: Boolean = false
)
