import unittest

from utils import build_alert_header, choose_alert_label


class AlertLabelTests(unittest.TestCase):
    def test_choose_alert_label_for_strike_reports(self) -> None:
        label = choose_alert_label(
            "Israeli airstrikes hit southern Beirut after explosions were reported across Dahieh.",
            severity="high",
        )
        self.assertEqual(label, "🚨 Strike Alert")

    def test_choose_alert_label_for_casualty_reports(self) -> None:
        label = choose_alert_label(
            "Health officials say 12 people were killed and dozens injured in overnight shelling.",
            severity="high",
        )
        self.assertEqual(label, "🕯️ Casualty Alert")

    def test_build_alert_header_includes_source_when_enabled(self) -> None:
        header = build_alert_header(
            "Trump says there will be no deal with Iran except unconditional surrender.",
            severity="high",
            source_title="Wire Desk",
            include_source=True,
        )
        self.assertEqual(header, "<b>📣 Major Statement • Wire Desk</b>")

    def test_choose_alert_label_prefers_interception_over_strike(self) -> None:
        label = choose_alert_label(
            "Air defenses intercepted missiles over Bahrain after sirens sounded in Manama.",
            severity="high",
        )
        self.assertEqual(label, "🛡️ Interception Alert")


if __name__ == "__main__":
    unittest.main()
