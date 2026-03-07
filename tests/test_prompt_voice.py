import unittest

from ai_filter import _breaking_headline_prompt, _summary_system_prompt, _vital_rational_view_prompt
from prompts import build_digest_system_prompt, build_query_system_prompt


class PromptVoiceTests(unittest.TestCase):
    def test_summary_prompt_requires_human_newsroom_voice(self) -> None:
        prompt = _summary_system_prompt()
        self.assertIn("human newsroom", prompt.lower())
        self.assertIn("do not sound robotic", prompt.lower())

    def test_breaking_prompt_has_human_voice_and_no_hype_rules(self) -> None:
        prompt = _breaking_headline_prompt()
        self.assertIn("human live-news presenter", prompt.lower())
        self.assertIn("no hype", prompt.lower())
        self.assertIn("natural spoken cadence", prompt.lower())

    def test_vital_prompt_demands_specific_human_consequence(self) -> None:
        prompt = _vital_rational_view_prompt()
        self.assertIn("calm human analyst", prompt.lower())
        self.assertIn("specific consequence", prompt.lower())
        self.assertIn("never use generic boilerplate", prompt.lower())

    def test_digest_and_query_prompts_share_human_voice_rules(self) -> None:
        digest_prompt = build_digest_system_prompt(
            interval_minutes=60,
            json_mode=False,
            importance_scoring=True,
            include_links=False,
            output_language="English",
            include_source_tags=False,
        )
        query_prompt = build_query_system_prompt(output_language="English", detailed=False)
        self.assertIn("human, readable, and alive", digest_prompt.lower())
        self.assertIn("human, readable, and alive", query_prompt.lower())
        self.assertIn("avoid repetitive sentence shapes", digest_prompt.lower())
        self.assertIn("avoid stiff analyst jargon", query_prompt.lower())


if __name__ == "__main__":
    unittest.main()
