from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from app import main, terminal_vision


class GradingHistoryProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_stored_relay_history_is_used_without_any_network_read(self) -> None:
        due_ts = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc).timestamp()
        with (
            patch.object(
                terminal_vision,
                "_stored_price_candidates",
                return_value=[
                    (
                        0.00123,
                        due_ts + 30,
                        "Stored Binance relay snapshot nearest forecast due time",
                    )
                ],
            ),
            patch.object(
                terminal_vision,
                "_download_csv",
                new=AsyncMock(side_effect=AssertionError("network must not be used")),
            ),
        ):
            price, actual_at, offset, source, error = (
                await terminal_vision.historical_futures_price_near(due_ts)
            )

        self.assertEqual(price, 0.00123)
        self.assertEqual(offset, 30)
        self.assertIn("Stored Binance relay", source or "")
        self.assertIsNone(error)
        self.assertEqual(datetime.fromisoformat(actual_at or "").year, 2026)

    async def test_completed_day_falls_back_to_binance_vision_archive(self) -> None:
        due = datetime.now(timezone.utc) - timedelta(days=2)
        due = due.replace(hour=12, minute=4, second=59, microsecond=999000)
        due_ts = due.timestamp()
        close_ms = int(due_ts * 1000)
        raw = [
            [
                str(close_ms - 299_999),
                "0.00120",
                "0.00125",
                "0.00119",
                "0.00124",
                "1000",
                str(close_ms),
            ]
        ]
        with (
            patch.object(terminal_vision, "_stored_price_candidates", return_value=[]),
            patch.object(
                terminal_vision,
                "_download_csv",
                new=AsyncMock(return_value=("https://data.binance.vision/test.zip", raw)),
            ) as download,
        ):
            price, actual_at, offset, source, error = (
                await terminal_vision.historical_futures_price_near(due_ts)
            )

        self.assertEqual(price, 0.00124)
        self.assertIsNotNone(offset)
        self.assertLess(offset if offset is not None else 1, 0.01)
        self.assertIn("Binance Vision daily", source or "")
        self.assertIsNone(error)
        self.assertEqual(datetime.fromisoformat(actual_at or "").year, due.year)
        download.assert_awaited_once()

    async def test_main_grader_path_never_calls_blocked_fapi_klines(self) -> None:
        due_ts = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc).timestamp()
        expected = (
            0.00124,
            datetime.fromtimestamp(due_ts, tz=timezone.utc).isoformat(),
            0.0,
            "Binance Vision daily futures close",
            None,
        )
        with (
            patch.object(
                main,
                "historical_vision_price_near",
                new=AsyncMock(return_value=expected),
            ) as provider,
            patch.object(
                main,
                "get_json",
                new=AsyncMock(side_effect=AssertionError("blocked Binance REST was called")),
            ),
        ):
            result = await main.historical_futures_price_near(due_ts)

        self.assertEqual(result, expected)
        provider.assert_awaited_once_with(
            due_ts,
            tolerance_seconds=main.FORECAST_GRADE_TOLERANCE_SECONDS,
            interval="5m",
        )

    def test_binance_microsecond_archive_timestamps_are_normalized(self) -> None:
        expected_ms = 1_785_362_800_000
        self.assertEqual(terminal_vision._timestamp_ms(expected_ms), expected_ms)
        self.assertEqual(terminal_vision._timestamp_ms(expected_ms * 1000), expected_ms)
        self.assertEqual(terminal_vision._timestamp_ms(expected_ms // 1000), expected_ms)


if __name__ == "__main__":
    unittest.main()
