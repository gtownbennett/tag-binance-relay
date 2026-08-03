from __future__ import annotations

import inspect
import unittest

from app import main
from app.terminal_addon import TerminalAddon
from app.terminal_intelligence import HORIZONS, LONG_TERM_HORIZONS


class SocialFirewallTests(unittest.TestCase):
    def test_required_near_term_horizon_contract_is_explicit(self) -> None:
        labels = [label for label, _ in HORIZONS]
        for required in ("1h", "4h", "12h", "24h", "3d", "7d", "30d", "90d"):
            self.assertIn(required, labels)
        self.assertNotIn("1d", labels)
        self.assertEqual(["1y", "5y"], LONG_TERM_HORIZONS)

    def test_chad_prompt_builder_has_no_social_or_community_input(self) -> None:
        source = inspect.getsource(main.request_chad_analysis).lower()
        self.assertNotIn("social", source)
        self.assertNotIn("community", source)
        self.assertNotIn("social_ledger", source)

    def test_terminal_intelligence_contract_excludes_community_calls(self) -> None:
        source = inspect.getsource(TerminalAddon.build_terminal_payload).lower()
        self.assertNotIn('"social"', source)
        self.assertNotIn("social_ledger", source)
        self.assertNotIn("community", source)


if __name__ == "__main__":
    unittest.main()
