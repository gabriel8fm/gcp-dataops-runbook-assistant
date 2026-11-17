"""BigQuery path for loading batches and running the read-only incident query.

Requires ``pip install -r requirements-gcp.txt`` and Application Default
Credentials. Nothing in this module runs during the offline demo; the contract
tests in tests/test_bigquery_contract.py exercise it with a mocked client.
"""
from dataops import events

RAW_DATASET = "incident_raw"
RAW_TABLE = "incident_events"
ANALYTICS_DATASET = "incident_analytics"
VALIDATED_TABLE = "incidents_validated"
# Hard cost ceiling for each assistant query (100 MB is far above demo size).
MAX_BYTES_BILLED = 100 * 1024 * 1024
JOB_LABELS = {"app": "runbook-assistant", "data": "synthetic"}


def bigquery_module():
    try:
        from google.cloud import bigquery
    except ImportError as error:  # pragma: no cover - depends on optional package
        raise SystemExit(
            "google-cloud-bigquery is not installed. Run: pip install -r requirements-gcp.txt"
        ) from error
    return bigquery


def make_client(project, location=None):
    return bigquery_module().Client(project=project, location=location)


def raw_table_id(project, dataset=RAW_DATASET, table=RAW_TABLE):
    return f"{project}.{dataset}.{table}"


def validated_table_id(project, dataset=ANALYTICS_DATASET, table=VALIDATED_TABLE):
    return f"{project}.{dataset}.{table}"


def schema_fields(bigquery):
    return [
        bigquery.SchemaField(
            field["name"], field["type"], mode=field["mode"], description=field["description"]
        )
        for field in events.load_schema()
    ]


def load_batch(client, table_id, batch_date, rows, ingested_at=None):
    """Replace exactly one partition with a load job (idempotent, no DML).

    The ``$YYYYMMDD`` partition decorator plus WRITE_TRUNCATE makes a reload of the
    same batch overwrite its partition instead of appending duplicates. Late
    events land in their delivery partition and reach incidents_validated via
    Dataform, which deduplicates by event_id across partitions.
    """
    from dataops.local_warehouse import to_utc

    bigquery = bigquery_module()
    batch_date = events.parse_date(batch_date)
    ingested = events.format_ts(to_utc(ingested_at))
    payload = [
        {**row, "batch_date": batch_date.isoformat(), "ingested_at": ingested} for row in rows
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema_fields(bigquery),
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
        labels=JOB_LABELS,
    )
    destination = f"{table_id}${batch_date:%Y%m%d}"
    job = client.load_table_from_json(payload, destination, job_config=job_config)
    job.result()
    return {
        "batch_date": batch_date.isoformat(),
        "rows_loaded": job.output_rows if job.output_rows is not None else len(payload),
        "destination": destination,
        "job_id": job.job_id,
    }


def partition_counts(client, table_id, start, end):
    """Row and distinct-key counts per partition, used to verify reprocessing."""
    bigquery = bigquery_module()
    sql = f"""
        SELECT batch_date, COUNT(*) AS rows, COUNT(DISTINCT event_id) AS distinct_event_ids
        FROM `{table_id}`
        WHERE batch_date BETWEEN @start AND @end
        GROUP BY batch_date ORDER BY batch_date
    """
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE", events.parse_date(start).isoformat()),
            bigquery.ScalarQueryParameter("end", "DATE", events.parse_date(end).isoformat()),
        ],
        maximum_bytes_billed=MAX_BYTES_BILLED,
        labels=JOB_LABELS,
    )
    return [
        {**dict(row.items()), "batch_date": row["batch_date"].isoformat()}
        for row in client.query(sql, job_config=config).result()
    ]


def run_readonly_query(client, sql, parameters):
    """Run a catalog query with typed parameters and a byte ceiling.

    ``sql`` must come from dataops.incident_query.QUERY_CATALOG; callers never
    pass model output here.
    """
    bigquery = bigquery_module()
    if not sql.lstrip().upper().startswith(("SELECT", "WITH")):
        raise ValueError("Only read-only SELECT statements are allowed")
    query_parameters = [
        bigquery.ScalarQueryParameter(name, kind, value) for name, kind, value in parameters
    ]
    config = bigquery.QueryJobConfig(
        query_parameters=query_parameters,
        maximum_bytes_billed=MAX_BYTES_BILLED,
        labels=JOB_LABELS,
    )
    return [dict(row.items()) for row in client.query(sql, job_config=config).result()]
