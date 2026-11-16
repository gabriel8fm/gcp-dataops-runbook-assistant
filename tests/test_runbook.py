from contextlib import closing
import io
import json
import unittest
from unittest.mock import patch
from urllib.error import URLError

import runbook


class RunbookTest(unittest.TestCase):
    def setUp(self):
        self.index = self.enterContext(closing(runbook.build_index(runbook.load_data("runbooks.json"))))

    def test_evaluation(self):
        result = runbook.evaluate(self.index, runbook.load_data("evaluation.json"))
        self.assertEqual(result["passed"], result["cases"], result["details"])

    def test_fts_operators_are_treated_as_text(self):
        result = runbook.ask(self.index, 'terraform" OR ) -lock*')
        self.assertEqual(result["citations"], ["RB-PIPE-01"])

    def test_portuguese_accent_matching(self):
        result = runbook.ask(self.index, "Problema de resolução")
        self.assertEqual(result["citations"], ["RB-DNS-01"])

    def test_offline_abstention_skips_model(self):
        with patch("runbook.generate") as generate:
            result = runbook.ask(self.index, "Weather forecast for Lisbon", "test-model")
        self.assertEqual(result["mode"], "abstain")
        generate.assert_not_called()

    def test_ollama_response(self):
        payload = {
            "response": json.dumps({
                "answer": "Check CoreDNS errors.",
                "source_ids": ["RB-DNS-01"],
            }),
            "prompt_eval_count": 200,
            "eval_count": 20,
        }
        with patch("runbook.urlopen", return_value=io.BytesIO(json.dumps(payload).encode())) as api:
            result = runbook.ask(self.index, "CoreDNS SERVFAIL", "test-model")
        self.assertEqual(result["mode"], "ollama")
        self.assertEqual(result["citations"], ["RB-DNS-01"])
        self.assertEqual(result["metrics"]["output_tokens"], 20)
        request = api.call_args.args[0]
        self.assertEqual(request.full_url, runbook.OLLAMA_URL)
        body = json.loads(request.data)
        self.assertFalse(body["stream"])
        self.assertIn("RB-DNS-01", body["prompt"])

    def test_invalid_citation_falls_back(self):
        payload = {"response": '{"answer":"An answer","source_ids":["RB-UNKNOWN"]}'}
        with patch("runbook.urlopen", return_value=io.BytesIO(json.dumps(payload).encode())):
            result = runbook.ask(self.index, "CoreDNS SERVFAIL", "test-model")
        self.assertEqual(result["mode"], "extractive")
        self.assertEqual(result["citations"], ["RB-DNS-01"])
        self.assertIn("ValueError", result["warning"])

    def test_ollama_unavailable_falls_back(self):
        with patch("runbook.urlopen", side_effect=URLError("offline")):
            result = runbook.ask(self.index, "Ingress 502", "test-model")
        self.assertEqual(result["mode"], "extractive")
        self.assertEqual(result["citations"], ["RB-EDGE-01"])
        self.assertIn("URLError", result["warning"])


if __name__ == "__main__":
    unittest.main()
