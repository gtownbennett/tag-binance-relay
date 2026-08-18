import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
MAIN = (ROOT / "app/main.py").read_text(encoding="utf-8")
CONFIG = (ROOT / "app/terminal_config.py").read_text(encoding="utf-8")
TEST_WORKFLOW = (ROOT / ".github/workflows/relay-tests.yml").read_text(
    encoding="utf-8"
)
IMAGE_WORKFLOW = (ROOT / ".github/workflows/publish-image.yml").read_text(
    encoding="utf-8"
)


class FullForecastContractTests(unittest.TestCase):
    def test_version_and_workflows_cover_current_production_lineage(self):
        self.assertIn('SERVICE_VERSION = "tagnext-0.9.0-rc3"', MAIN)
        self.assertIn('TAGneXt-Relay/{SERVICE_VERSION}', MAIN)
        self.assertNotIn('TAG-Terminal-Relay/{SERVICE_VERSION}', MAIN)
        self.assertIn('APP_VERSION = "tagnext-1.0.0-rc2"', CONFIG)
        for workflow in (TEST_WORKFLOW, IMAGE_WORKFLOW):
            self.assertIn(
                "v2.8.6-rc6-3-full-forecast-contract",
                workflow,
            )
        self.assertIn("pull_request:", TEST_WORKFLOW)

    def test_all_twenty_five_requested_horizons_are_bounded(self):
        required = (
            "15m", "1h", "2h", "4h", "6h", "12h", "24h",
            "3d", "7d", "2w", "3w", "1mo", "3mo", "6mo", "1y",
            "2026", "2027", "2028", "2029", "2030",
            "2y", "3y", "4y", "5y", "6y",
        )
        contract = MAIN.split("FULL_FORECAST_HORIZONS = (", 1)[1].split(")", 1)[0]
        for horizon in required:
            self.assertIn(f'"{horizon}"', contract)
        self.assertIn('"minItems": len(FULL_FORECAST_HORIZONS)', MAIN)
        self.assertIn('"maxItems": len(FULL_FORECAST_HORIZONS)', MAIN)
        self.assertIn('"enum": list(FULL_FORECAST_HORIZONS)', MAIN)
        self.assertIn("exactly 25 machine-readable forecast horizons", MAIN)

    def test_long_range_and_cost_evidence_are_explicit(self):
        self.assertIn("probabilities more conservative", MAIN)
        self.assertIn("do not pretend they are calibrated", MAIN)
        self.assertIn('"openAIUsage": {', MAIN)
        self.assertIn('"inputTokens"', MAIN)
        self.assertIn('"outputTokens"', MAIN)


if __name__ == "__main__":
    unittest.main()
