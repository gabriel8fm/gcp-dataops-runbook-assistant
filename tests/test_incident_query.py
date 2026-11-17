"""Assistant answers from the local validated table (offline, no GCP)."""
from contextlib import closing
from datetime import timedelta
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from dataops import events, incident_query
from dataops import local_warehouse as warehouse
import runbook

DAYS = events.date_range("2026-11-03", "2026-11-16")
AS_OF = "2026-11-17T06:00:00Z"


class IncidentQuestionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db_path = Path(cls.tmp.name) / "incidents.db"
        with closing(warehouse.connect(cls.db_path)) as connection:
            for day in DAYS:
                warehouse.load_batch(connection, day, events.generate_batch(day), AS_OF)
            assert warehouse.transform(connection, AS_OF)["status"] == "passed"

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def setUp(self):
        self.index = self.enterContext(closing(runbook.build_index(runbook.load_data("runbooks.json"))))
        self.runner = incident_query.LocalRunner(self.db_path)
        self.addCleanup(self.runner.close)

    def ask(self, question, **kwargs):
        return incident_query.answer(
            question, self.runner, lambda text: runbook.retrieve(self.index, text),
            as_of=kwargs.pop("as_of", AS_OF), **kwargs,
        )

    def expected_leader(self, severity="critical", days=7):
        start = events.format_ts(warehouse.to_utc(AS_OF) - timedelta(days=days))
        with closing(sqlite3.connect(self.db_path)) as connection:
            return connection.execute(
                """SELECT service, COUNT(*) FROM incidents_validated
                   WHERE severity = ? AND opened_at >= ? AND opened_at < ?
                   GROUP BY service ORDER BY COUNT(*) DESC, service""",
                (severity, start, AS_OF),
            ).fetchall()

    def test_demo_question_cites_data_and_runbook(self):
        result = self.ask(runbook.DEMO_QUESTION)
        leaders = self.expected_leader()
        self.assertEqual(result["mode"], "data+extractive")
        self.assertGreater(leaders[0][1], leaders[1][1], "demo seed should have a clear leader")
        self.assertTrue(result["answer"].startswith(f"{leaders[0][0]} had the most critical"))
        self.assertEqual(result["citations"], [
            "local:incidents_validated#top_services_by_severity_v1", "RB-K8S-01",
        ])
        self.assertEqual(result["evidence"]["parameters"], {
            "severity": "critical",
            "window_start": "2026-11-10T06:00:00Z",
            "window_end": AS_OF,
        })
        recommendation = result["evidence"]["recommendations"][0]
        self.assertEqual(recommendation["incident_count"], leaders[0][1])
        for incident_id in recommendation["incident_ids"]:
            self.assertIn(incident_id, result["answer"])
        self.assertEqual(result["warnings"], [])

    def test_window_and_severity_are_parsed(self):
        self.assertEqual(
            incident_query.parse_question("Which service had the most SEV2 incidents in the past 3 days?"),
            {"severity": "high", "window_days": 3, "window_assumed": False},
        )
        self.assertEqual(
            incident_query.parse_question("Top service for low incidents last week")["window_days"], 7
        )
        self.assertTrue(
            incident_query.parse_question("Which service has the most critical incidents?")["window_assumed"]
        )

    def test_unsupported_question_abstains_without_querying(self):
        with patch.object(self.runner, "fetch") as fetch:
            result = self.ask("DROP TABLE incidents_validated; which service?")
        self.assertEqual(result["mode"], "abstain")
        self.assertEqual(result["citations"], [])
        fetch.assert_not_called()

    def test_window_without_incidents_reports_insufficient_evidence(self):
        result = self.ask(runbook.DEMO_QUESTION, as_of="2026-12-31T00:00:00Z")
        self.assertEqual(result["mode"], "insufficient_evidence")
        self.assertIn("Insufficient evidence", result["answer"])
        self.assertEqual(result["citations"], ["local:incidents_validated#top_services_by_severity_v1"])
        self.assertTrue(any("stale" in warning for warning in result["warnings"]))

    def test_missing_runbook_is_insufficient_evidence(self):
        result = incident_query.answer(
            runbook.DEMO_QUESTION, self.runner, lambda text: [], as_of=AS_OF
        )
        self.assertEqual(result["mode"], "insufficient_evidence")
        self.assertIn("no runbook matched", result["answer"])
        self.assertFalse(any(citation.startswith("RB-") for citation in result["citations"]))

    def test_connection_is_read_only(self):
        with self.assertRaises(sqlite3.OperationalError):
            self.runner.connection.execute("DELETE FROM incidents_validated")

    def test_failed_quality_run_adds_warning(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "incidents.db"
            with closing(warehouse.connect(db_path)) as connection:
                for day in DAYS:
                    warehouse.load_batch(connection, day, events.generate_batch(day), AS_OF)
                warehouse.transform(connection, AS_OF)
                faulty, _ = events.inject_faults(events.generate_batch(DAYS[-1]))
                warehouse.load_batch(connection, DAYS[-1], faulty, AS_OF)
                warehouse.transform(connection, AS_OF)
            runner = incident_query.LocalRunner(db_path)
            try:
                result = incident_query.answer(
                    runbook.DEMO_QUESTION, runner, lambda text: runbook.retrieve(self.index, text),
                    as_of=AS_OF,
                )
            finally:
                runner.close()
        self.assertEqual(result["mode"], "data+extractive")
        self.assertTrue(any("Latest quality run failed" in warning for warning in result["warnings"]))

    def test_ollama_summary_must_cite_data(self):
        payload = {"response": json.dumps({"answer": "Use RB-K8S-01.", "source_ids": ["RB-K8S-01"]})}
        with patch("runbook.urlopen", return_value=io.BytesIO(json.dumps(payload).encode())):
            result = self.ask(runbook.DEMO_QUESTION, generate=runbook.generate, model="test-model")
        self.assertEqual(result["mode"], "data+extractive")
        self.assertIn("ValueError", result["warnings"][-1])

    def test_ollama_summary_with_data_citation(self):
        citation = "local:incidents_validated#top_services_by_severity_v1"
        payload = {
            "response": json.dumps({
                "answer": "checkout led critical incidents; see RB-K8S-01.",
                "source_ids": [citation, "RB-K8S-01"],
            }),
            "eval_count": 12,
        }
        with patch("runbook.urlopen", return_value=io.BytesIO(json.dumps(payload).encode())) as api:
            result = self.ask(runbook.DEMO_QUESTION, generate=runbook.generate, model="test-model")
        self.assertEqual(result["mode"], "data+ollama")
        self.assertEqual(result["citations"], [citation, "RB-K8S-01"])
        prompt = json.loads(json.loads(api.call_args.args[0].data)["prompt"])
        self.assertEqual(prompt["runbooks"][0]["id"], citation)
        self.assertNotIn("SELECT", api.call_args.args[0].data.decode())


if __name__ == "__main__":
    unittest.main()
