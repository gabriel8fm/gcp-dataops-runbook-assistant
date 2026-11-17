# Operations guide: quality failures and reprocessing

This guide covers what to do when a quality check fails and how to replay one
batch or a date range. All data is synthetic.

## How a batch flows

1. `pipeline.py generate` writes one JSONL file per delivery date to
   `.warehouse/landing/batch_date=YYYY-MM-DD/events.jsonl`. The generator is
   deterministic, so regenerating a date reproduces the same file.
2. `pipeline.py load` replaces exactly one `batch_date` partition per file:
   - Local: `DELETE` plus `INSERT` for that `batch_date` in one SQLite transaction.
   - BigQuery: load job into `incident_raw.incident_events$YYYYMMDD` with
     `WRITE_TRUNCATE`.
3. Checks run against the raw table. Each check is a query that returns
   offending rows. Zero rows means the check passed.
4. `incidents_validated` is rebuilt only when every check passes. Otherwise the
   previous snapshot stays in place and the assistant warns that the latest
   quality run failed.

## Check catalog

The same eight checks exist in `dataops/local_warehouse.py` (`CHECKS`) and in
`dataform/definitions/assertions/`. A unit test keeps the two lists equal.

| Check | Category | Fails when | Typical upstream cause |
| --- | --- | --- | --- |
| `assert_event_id_not_null` | keys | A raw row has no `event_id` | Producer bug, schema drift |
| `assert_event_id_unique` | keys | One `event_id` has more than one distinct payload | Producer reused an ID or edited an event |
| `assert_incident_single_service` | keys | One `incident_id` maps to two services | ID collision across producers |
| `assert_required_fields_not_null` | required fields | `incident_id`, `sequence`, `service`, `severity`, `status` or `event_ts` is null | Partial record |
| `assert_valid_severity` | accepted values | `severity` is not `critical`, `high`, `medium` or `low` | New label not agreed in the contract |
| `assert_valid_status` | accepted values | `status` is not `open`, `acknowledged`, `mitigated` or `resolved` | New lifecycle state |
| `assert_event_ts_not_future` | timestamps | `event_ts` is more than 5 minutes after now | Clock skew, wrong time zone |
| `assert_freshness` | freshness | Newest valid event is older than 48 hours (`freshness_hours`) | Loader did not run, upstream outage |

Exact redeliveries of the same event are not failures. The staging view
collapses them, which is how at-least-once delivery is absorbed.

## Diagnose a failure

### Local mode

```bash
python3 pipeline.py transform --as-of 2026-11-17T06:00:00Z   # exit code 1 on failure
python3 pipeline.py check --as-of 2026-11-17T06:00:00Z       # checks only, with samples
python3 pipeline.py status                                   # partitions and last run
```

Each failed check lists `failing_rows` and up to five sample rows with the
`event_id`, `incident_id` and `batch_date` needed to find the source file. To
inspect more rows, open the database read-only:

```bash
sqlite3 -readonly .warehouse/incidents.db \
  "SELECT batch_date, event_id, severity FROM raw_incident_events
   WHERE severity NOT IN ('critical','high','medium','low')"
```

### BigQuery mode

The Dataform CLI prints each failed assertion. Dataform skips
`incidents_validated` because it depends on every assertion. Each assertion is
also a view in `incident_assertions`, so the failing rows can be queried:

```bash
bq query --use_legacy_sql=false \
  'SELECT * FROM `'"$GOOGLE_CLOUD_PROJECT"'.incident_assertions.assert_valid_severity`'
```

Use `batch_date` from the failing rows to find the partition to replay.

## Reprocess

Decide first whether the fix belongs upstream (bad source data) or in the
contract (a new valid value). For a contract change, update
`events.SEVERITIES` or `events.STATUSES` and the matching var in
`dataform/workflow_settings.yaml` in the same pull request.

After the upstream data is corrected, replay the affected dates:

```bash
# Local: regenerate the files, reload the partitions, run the checks again.
python3 pipeline.py reprocess --start 2026-11-16 --end 2026-11-16 --as-of 2026-11-17T06:00:00Z

# Local: replay files already in the landing zone without regenerating.
python3 pipeline.py reprocess --start 2026-11-10 --end 2026-11-16 --from-landing

# BigQuery: reload the partitions, then run Dataform.
python3 pipeline.py reprocess --backend bigquery --project "$GOOGLE_CLOUD_PROJECT" \
  --start 2026-11-16 --end 2026-11-16 --run-dataform
```

Reprocessing is safe to repeat. Each load replaces its partition, so row counts
per `batch_date` stay the same after any number of reruns. The BigQuery output
includes `partitions` with `rows` and `distinct_event_ids` per date for that
check.

## Late events

An event can arrive one or two days after `event_ts`. It is stored in the
partition of the day it arrived (`batch_date`), not the day it happened.
Staging and `incidents_validated` read all partitions and group by `event_id`
and `incident_id`, so a late `resolved` event updates the incident on the next
run. The incident's `last_batch_date` shows which delivery last changed it.
