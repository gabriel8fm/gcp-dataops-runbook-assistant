from pathlib import Path
import unittest

from dataops import events

ROOT = Path(__file__).resolve().parent.parent


class GeneratorTest(unittest.TestCase):
    def test_batches_are_deterministic(self):
        self.assertEqual(events.generate_batch("2026-11-10"), events.generate_batch("2026-11-10"))
        self.assertNotEqual(
            events.generate_batch("2026-11-10"), events.generate_batch("2026-11-10", seed=1)
        )

    def test_rows_match_schema_and_vocabulary(self):
        schema = [field["name"] for field in events.load_schema()]
        for day in events.date_range("2026-11-01", "2026-11-16"):
            for row in events.generate_batch(day):
                self.assertLessEqual(set(row), set(schema))
                self.assertEqual(set(schema) - set(row), {"ingested_at"})
                self.assertIn(row["severity"], events.SEVERITIES)
                self.assertIn(row["status"], events.STATUSES)
                self.assertEqual(row["event_id"], events.event_id(row["incident_id"], row["sequence"]))
                self.assertLessEqual(row["event_ts"][:10], row["batch_date"])

    def test_event_ids_are_unique_across_batches(self):
        ids = [
            row["event_id"]
            for day in events.date_range("2026-10-01", "2026-11-16")
            for row in events.generate_batch(day)
        ]
        self.assertEqual(len(ids), len(set(ids)))

    def test_late_events_exist(self):
        rows = events.generate_batch("2026-11-16")
        late = events.late_events(rows)
        self.assertTrue(late)
        self.assertTrue(all(row["event_ts"][:10] < "2026-11-16" for row in late))

    def test_committed_faulty_sample_matches_generator(self):
        expected, faults = events.inject_faults(events.generate_batch("2026-11-16"))
        sample = events.read_jsonl(ROOT / "data" / "samples" / "faulty_batch_2026-11-16.jsonl")
        self.assertEqual(sample, expected)
        self.assertEqual(len(faults), 6)

    def test_date_range_rejects_reversed_range(self):
        with self.assertRaises(ValueError):
            events.date_range("2026-11-10", "2026-11-01")


if __name__ == "__main__":
    unittest.main()
