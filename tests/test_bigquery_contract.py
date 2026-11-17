"""Contract tests for the BigQuery path with a mocked client.

These tests build real google-cloud-bigquery job configs but never call GCP.
They prove request shape (partition decorator, WRITE_TRUNCATE, typed query
parameters, byte ceiling), not BigQuery behavior. See README "Test coverage".
"""
import unittest
from unittest.mock import MagicMock

from dataops import events, incident_query

try:
    from google.cloud import bigquery  # noqa: F401
    HAS_BIGQUERY = True
except ImportError:
    HAS_BIGQUERY = False


class TableIdTest(unittest.TestCase):
    def test_runner_rejects_injected_table_ids(self):
        for table_id in ("demo-project.ds.t`; DROP TABLE x; --", "ds.table", "p.d.t WHERE 1=1"):
            with self.assertRaises(ValueError):
                incident_query.BigQueryRunner(MagicMock(), table_id)

    def test_catalog_queries_are_select_only(self):
        for query in incident_query.QUERY_CATALOG.values():
            for dialect in ("sqlite", "bigquery"):
                self.assertTrue(query[dialect].lstrip().upper().startswith("SELECT"))


@unittest.skipUnless(HAS_BIGQUERY, "pip install -r requirements-gcp.txt to run BigQuery contract tests")
class BigQueryContractTest(unittest.TestCase):
    def setUp(self):
        from google.cloud import bigquery

        from dataops import bigquery_backend

        self.bigquery = bigquery
        self.backend = bigquery_backend
        self.client = MagicMock()

    def test_load_replaces_one_partition(self):
        job = self.client.load_table_from_json.return_value
        job.output_rows, job.job_id = 22, "job-1"
        rows = events.generate_batch("2026-11-16")

        result = self.backend.load_batch(
            self.client, "demo-project.incident_raw.incident_events", "2026-11-16", rows,
            ingested_at="2026-11-17T06:00:00Z",
        )

        payload, destination = self.client.load_table_from_json.call_args.args
        config = self.client.load_table_from_json.call_args.kwargs["job_config"]
        self.assertEqual(destination, "demo-project.incident_raw.incident_events$20261116")
        self.assertEqual(config.write_disposition, self.bigquery.WriteDisposition.WRITE_TRUNCATE)
        self.assertEqual(config.create_disposition, self.bigquery.CreateDisposition.CREATE_NEVER)
        self.assertEqual(config.source_format, self.bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
        self.assertEqual([field.name for field in config.schema],
                         [field["name"] for field in events.load_schema()])
        self.assertTrue(all(row["batch_date"] == "2026-11-16" for row in payload))
        self.assertTrue(all(row["ingested_at"] == "2026-11-17T06:00:00Z" for row in payload))
        job.result.assert_called_once()
        self.assertEqual(result["rows_loaded"], 22)

    def test_reloading_targets_the_same_partition(self):
        rows = events.generate_batch("2026-11-16")
        for _ in range(2):
            self.backend.load_batch(self.client, "p1234.d.t", "2026-11-16", rows)
        destinations = {call.args[1] for call in self.client.load_table_from_json.call_args_list}
        self.assertEqual(destinations, {"p1234.d.t$20261116"})

    def test_assistant_query_is_parameterized_with_byte_limit(self):
        self.client.query.return_value.result.return_value = []
        runner = incident_query.BigQueryRunner(
            self.client, "demo-project.incident_analytics.incidents_validated"
        )
        runner.fetch(incident_query.QUERY_ID, [
            ("severity", "STRING", "critical"),
            ("window_start", "TIMESTAMP", "2026-11-10T06:00:00Z"),
            ("window_end", "TIMESTAMP", "2026-11-17T06:00:00Z"),
        ])
        sql = self.client.query.call_args.args[0]
        config = self.client.query.call_args.kwargs["job_config"]
        self.assertIn("`demo-project.incident_analytics.incidents_validated`", sql)
        self.assertIn("severity = @severity", sql)
        self.assertNotIn("critical", sql)
        self.assertEqual(
            {(item.name, item.type_) for item in config.query_parameters},
            {("severity", "STRING"), ("window_start", "TIMESTAMP"), ("window_end", "TIMESTAMP")},
        )
        self.assertEqual(config.maximum_bytes_billed, self.backend.MAX_BYTES_BILLED)

    def test_non_select_statements_are_refused(self):
        with self.assertRaises(ValueError):
            self.backend.run_readonly_query(self.client, "DELETE FROM t WHERE TRUE", [])
        self.client.query.assert_not_called()


if __name__ == "__main__":
    unittest.main()
