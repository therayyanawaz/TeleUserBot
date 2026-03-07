import unittest

from ai_filter import _fallback_vital_view, _vital_view_is_too_generic


class VitalViewTests(unittest.TestCase):
    def test_radar_replacement_fallback_mentions_asset(self) -> None:
        text = (
            "The United States is moving quickly to replace a radar linked to its "
            "Terminal High Altitude Area Defense (THAAD) battery after the site was hit."
        )
        view = _fallback_vital_view(text)
        self.assertIn("Why it matters:", view)
        self.assertTrue("THAAD" in view or "radar" in view.lower())

    def test_airspace_warning_fallback_mentions_place(self) -> None:
        text = "Jordan closed its airspace after missile interceptions over Amman."
        view = _fallback_vital_view(text)
        self.assertIn("Why it matters:", view)
        self.assertTrue("Jordan" in view or "Amman" in view)

    def test_generic_boilerplate_is_flagged(self) -> None:
        self.assertTrue(
            _vital_view_is_too_generic(
                "Why it matters: This may affect regional stability; verify updates across multiple reliable sources."
            )
        )

    def test_warning_fallback_prefers_zone_not_actor(self) -> None:
        text = "The IDF warned civilians to leave Abbas Abad industrial area immediately."
        view = _fallback_vital_view(text)
        self.assertIn("Abbas Abad industrial area", view)

    def test_maritime_fallback_prefers_chokepoint_location(self) -> None:
        text = "A tanker was hit near the Strait of Hormuz and the crew were rescued."
        view = _fallback_vital_view(text)
        self.assertIn("Strait of Hormuz", view)


if __name__ == "__main__":
    unittest.main()
