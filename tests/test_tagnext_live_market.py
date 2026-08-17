from __future__ import annotations

import math

from eth_hash.auto import keccak

from app.tagnext_live_market import (
    book_metrics,
    encode_quote_exact_input_single,
    normalize_levels,
)


def test_normalize_levels_handles_array_and_mapping_shapes() -> None:
    bids = normalize_levels([["0.001", "100"], ["bad", "2"], ["0.0009", "50"]], side="bid")
    asks = normalize_levels([{"p": "0.0012", "s": "10"}, {"price": "0.0011", "quantity": "20"}], side="ask")
    assert bids == [{"price": 0.001, "quantity": 100.0}, {"price": 0.0009, "quantity": 50.0}]
    assert asks[0] == {"price": 0.0011, "quantity": 20.0}


def test_book_metrics_are_one_percent_bounded() -> None:
    metrics = book_metrics(
        [{"price": 0.001, "quantity": 1000}, {"price": 0.0008, "quantity": 999999}],
        [{"price": 0.00102, "quantity": 500}, {"price": 0.0013, "quantity": 999999}],
    )
    assert math.isclose(metrics["bidDepthUsd1Pct"], 1.0)
    assert math.isclose(metrics["askDepthUsd1Pct"], 0.51)
    assert metrics["spreadBps"] > 0
    assert len(metrics["largeZones"]) == 4


def test_pancake_quote_encoding_matches_official_tuple_signature() -> None:
    token_in = "0x" + "11" * 20
    token_out = "0x" + "22" * 20
    encoded = encode_quote_exact_input_single(token_in=token_in, token_out=token_out, amount_in=123, fee=2500)
    expected_selector = "0x" + keccak(b"quoteExactInputSingle((address,address,uint256,uint24,uint160))").hex()[:8]
    assert encoded.startswith(expected_selector)
    assert len(encoded) == 2 + 8 + 5 * 64
    assert int(encoded[-3 * 64:-2 * 64], 16) == 123
    assert int(encoded[-2 * 64:-64], 16) == 2500
