"""Answer incident questions from incidents_validated plus runbook retrieval.

The model never writes SQL. A question is matched to a fixed, reviewed query in
QUERY_CATALOG, its parameters are parsed with regular expressions, and the query
runs through a read-only connection (SQLite ``mode=ro`` locally, a viewer-only
service account and a byte ceiling on BigQuery). Unsupported questions abstain.
"""
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
import re
import sqlite3
from time import perf_counter

from dataops import events
from dataops.local_warehouse import DEFAULT_FRESHNESS_HOURS, to_utc

QUERY_ID = "top_services_by_severity_v1"
ROW_LIMIT = 500
MAX_WINDOW_DAYS = 90
DEFAULT_WINDOW_DAYS = 7
SUPPORTED = (
    "Which service had the most <critical|high|medium|low> incidents in the last N days "
    "and which runbook should I check?"
)
NUMBER_WORDS = {
    word: value for value, word in enumerate(
        "zero one two three four five six seven eight nine ten eleven twelve thirteen "
        "fourteen fifteen sixteen seventeen eighteen nineteen twenty".split()
    )
}
NUMBER_WORDS["thirty"] = 30
SEVERITY_ALIASES = {
    "critical": "critical", "sev1": "critical", "p1": "critical",
    "high": "high", "sev2": "high", "p2": "high",
    "medium": "medium", "sev3": "medium", "p3": "medium",
    "low": "low", "sev4": "low", "p4": "low",
}

QUERY_CATALOG = {
    QUERY_ID: {
        "description": "Incidents of one severity opened inside a time window.",
        "sqlite": """
            SELECT incident_id, service, summary, status, opened_at
            FROM incidents_validated
            WHERE severity = :severity
              AND opened_at >= :window_start AND opened_at < :window_end
            ORDER BY service, opened_at, incident_id
            LIMIT """ + str(ROW_LIMIT),
        "bigquery": """
            SELECT incident_id, service, summary, status, opened_at
            FROM `{table}`
            WHERE severity = @severity
              AND opened_at >= @window_start AND opened_at < @window_end
            ORDER BY service, opened_at, incident_id
            LIMIT """ + str(ROW_LIMIT),
    },
    "snapshot_v1": {
        "description": "Size and recency of the validated snapshot.",
        "sqlite": """
            SELECT COUNT(*) AS incidents, MAX(_validated_at) AS validated_at,
                   MAX(last_event_ts) AS latest_event_ts
            FROM incidents_validated""",
        "bigquery": """
            SELECT COUNT(*) AS incidents, MAX(_validated_at) AS validated_at,
                   MAX(last_event_ts) AS latest_event_ts
            FROM `{table}`""",
    },
}
TABLE_ID = re.compile(r"^[a-z][a-z0-9-]{4,61}[a-z0-9]\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")


def parse_question(question):
    """Return query parameters for a supported question, otherwise None."""
    text = question.casefold().replace("-", "")
    words = re.findall(r"[a-z0-9]+", text)
    severities = {SEVERITY_ALIASES[word] for word in words if word in SEVERITY_ALIASES}
    asks_for_top = bool({"most", "top", "highest"} & set(words))
    if "service" not in text or not asks_for_top or len(severities) != 1:
        return None
    window, assumed = DEFAULT_WINDOW_DAYS, True
    number = "|".join(NUMBER_WORDS)
    match = re.search(rf"(?:last|past)\s+(\d+|{number})\s+days?", text)
    if match:
        raw = match.group(1)
        window, assumed = int(raw) if raw.isdigit() else NUMBER_WORDS[raw], False
    elif re.search(r"(?:last|past)\s+week", text):
        window, assumed = 7, False
    if not 1 <= window <= MAX_WINDOW_DAYS:
        return None
    return {"severity": severities.pop(), "window_days": window, "window_assumed": assumed}


def _text(value):
    if isinstance(value, datetime):
        return events.format_ts(value)
    if isinstance(value, date):
        return value.isoformat()
    return value


class LocalRunner:
    """Read-only SQLite runner for the local warehouse."""

    def __init__(self, db_path):
        db_path = Path(db_path)
        if not db_path.exists():
            raise FileNotFoundError(
                f"{db_path} does not exist. Run: python3 pipeline.py demo or pipeline.py run"
            )
        self.connection = sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA query_only = ON")
        self.source = "local:incidents_validated"

    def fetch(self, query_id, parameters):
        sql = QUERY_CATALOG[query_id]["sqlite"]
        values = {name: value for name, _, value in parameters}
        return [dict(row) for row in self.connection.execute(sql, values)]

    def last_quality_run(self):
        row = self.connection.execute(
            "SELECT run_at, status, failed_checks FROM quality_runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
        return None if row is None else dict(row)

    def close(self):
        self.connection.close()


class BigQueryRunner:
    """Read-only BigQuery runner; the table name comes from configuration only."""

    def __init__(self, client, table_id):
        if not TABLE_ID.match(table_id):
            raise ValueError(f"Invalid BigQuery table ID: {table_id}")
        self.client = client
        self.table_id = table_id
        self.source = f"bigquery:{table_id}"

    def fetch(self, query_id, parameters):
        from dataops import bigquery_backend

        sql = QUERY_CATALOG[query_id]["bigquery"].format(table=self.table_id)
        typed = [
            (name, kind, to_utc(value) if kind == "TIMESTAMP" else value)
            for name, kind, value in parameters
        ]
        rows = bigquery_backend.run_readonly_query(self.client, sql, typed)
        return [{key: _text(value) for key, value in row.items()} for row in rows]

    def last_quality_run(self):
        return None

    def close(self):
        self.client.close()


def _first_sentence_after_label(body):
    sentences = [part.strip() for part in body.split(". ") if part.strip()]
    if sentences and sentences[0].lower().startswith("fictional incident"):
        sentences = sentences[1:]
    return (sentences[0].rstrip(".") + ".") if sentences else body


def answer(question, runner, retrieve, as_of=None, generate=None, model=None,
           freshness_hours=DEFAULT_FRESHNESS_HOURS):
    """Answer a catalog question from validated data and cite data and runbooks."""
    start = perf_counter()
    as_of = to_utc(as_of)
    intent = parse_question(question)
    result = {
        "mode": "abstain",
        "answer": f"Unsupported data question. Supported question: {SUPPORTED}",
        "citations": [],
        "evidence": None,
        "retrieved": [],
        "warnings": [],
        "metrics": {"hosted_api_calls": 0},
    }
    if intent is None:
        result["metrics"]["total_ms"] = round((perf_counter() - start) * 1000, 3)
        return result

    window_start = as_of - timedelta(days=intent["window_days"])
    parameters = [
        ("severity", "STRING", intent["severity"]),
        ("window_start", "TIMESTAMP", events.format_ts(window_start)),
        ("window_end", "TIMESTAMP", events.format_ts(as_of)),
    ]
    query_start = perf_counter()
    rows = runner.fetch(QUERY_ID, parameters)
    snapshot = runner.fetch("snapshot_v1", [])[0]
    result["metrics"]["query_ms"] = round((perf_counter() - query_start) * 1000, 3)

    data_citation = f"{runner.source}#{QUERY_ID}"
    counts = Counter(row["service"] for row in rows)
    evidence = {
        "source": runner.source,
        "query_id": QUERY_ID,
        "parameters": {name: value for name, _, value in parameters},
        "rows_returned": len(rows),
        "incidents_by_service": dict(sorted(counts.items(), key=lambda item: (-item[1], item[0]))),
        "snapshot": snapshot,
    }
    result["evidence"] = evidence
    result["citations"] = [data_citation]
    warnings = result["warnings"]
    if intent["window_assumed"]:
        warnings.append(f"No window in question; assumed last {DEFAULT_WINDOW_DAYS} days.")
    if len(rows) == ROW_LIMIT:
        warnings.append(f"Result truncated at {ROW_LIMIT} rows; counts may be incomplete.")
    latest = snapshot.get("latest_event_ts")
    stale_before = events.format_ts(as_of - timedelta(hours=freshness_hours))
    if latest is None or latest < stale_before:
        warnings.append(
            f"Validated data may be stale: latest event {latest}, expected after {stale_before}."
        )
    last_run = runner.last_quality_run()
    if last_run and last_run["status"] != "passed":
        warnings.append(
            f"Latest quality run failed ({last_run['failed_checks']}); "
            f"answer uses the last validated snapshot from {snapshot.get('validated_at')}."
        )

    window_text = f"{events.format_ts(window_start)} and {events.format_ts(as_of)}"
    if not rows:
        result["mode"] = "insufficient_evidence"
        result["answer"] = (
            f"Insufficient evidence: incidents_validated has no {intent['severity']} incidents "
            f"opened between {window_text}. No runbook recommendation."
        )
        result["metrics"]["total_ms"] = round((perf_counter() - start) * 1000, 3)
        return result

    top_count = max(counts.values())
    leaders = sorted(service for service, count in counts.items() if count == top_count)
    recommendations, sentences, runbooks = [], [], {}
    for service in leaders:
        service_rows = [row for row in rows if row["service"] == service]
        summary = Counter(row["summary"] for row in service_rows).most_common(1)[0][0]
        matches = retrieve(f"{service} {summary}")
        incident_ids = [row["incident_id"] for row in service_rows]
        recommendations.append({
            "service": service,
            "incident_count": top_count,
            "incident_ids": incident_ids,
            "dominant_summary": summary,
            "runbook_id": matches[0]["id"] if matches else None,
        })
        result["retrieved"].extend(
            {"id": item["id"], "title": item["title"]} for item in matches
            if item["id"] not in {seen["id"] for seen in result["retrieved"]}
        )
        if matches:
            runbook = matches[0]
            runbooks[runbook["id"]] = runbook
            if runbook["id"] not in result["citations"]:
                result["citations"].append(runbook["id"])
            sentences.append(
                f"{service}: {top_count} {intent['severity']} incident(s) "
                f"({', '.join(incident_ids)}); most common symptom \"{summary}\". "
                f"Consult {runbook['id']} ({runbook['title']}): "
                f"{_first_sentence_after_label(runbook['body'])}"
            )
        else:
            sentences.append(
                f"{service}: {top_count} {intent['severity']} incident(s) "
                f"({', '.join(incident_ids)}), but no runbook matched \"{summary}\"; "
                "insufficient evidence for a runbook recommendation."
            )
    evidence["recommendations"] = recommendations
    if len(leaders) > 1:
        lead = (
            f"Tie: {', '.join(leaders)} each had {top_count} {intent['severity']} incidents "
            f"opened between {window_text}."
        )
    else:
        lead = (
            f"{leaders[0]} had the most {intent['severity']} incidents opened between "
            f"{window_text}."
        )
    result["answer"] = " ".join([lead, *sentences, f"Data source: {data_citation}."])
    result["mode"] = "data+extractive"
    if not any(item["runbook_id"] for item in recommendations):
        result["mode"] = "insufficient_evidence"

    if model and generate and result["mode"] == "data+extractive":
        sources = [{"id": data_citation, "title": "Validated incident query result",
                    "body": result["answer"]}]
        sources += [
            {"id": item["id"], "title": item["title"], "body": item["body"]}
            for item in runbooks.values()
        ]
        generation_start = perf_counter()
        try:
            text, citations, tokens = generate(question, sources, model)
            if data_citation not in citations:
                raise ValueError("Generated answer did not cite the data source")
            result.update(mode="data+ollama", answer=text, citations=citations)
            result["metrics"].update(tokens)
        except (OSError, ValueError, TypeError, KeyError) as error:
            warnings.append(f"Ollama fallback ({type(error).__name__}).")
        result["metrics"]["generation_ms"] = round((perf_counter() - generation_start) * 1000, 3)
    result["metrics"]["total_ms"] = round((perf_counter() - start) * 1000, 3)
    return result
