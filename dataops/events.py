"""Deterministic generator for synthetic incident lifecycle events.

Every value is derived from (seed, day), so regenerating a batch yields the same
event IDs and payloads. A batch holds the events delivered on one day. Some
events arrive one or two days after they happened (late events), and incident
lifecycles can cross midnight, so a batch can contain events for earlier days.
"""
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
from pathlib import Path
import random

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema" / "incident_events.json"
DEFAULT_SEED = 20261119
SEVERITIES = ("critical", "high", "medium", "low")
STATUSES = ("open", "acknowledged", "mitigated", "resolved")
REQUIRED_FIELDS = ("event_id", "incident_id", "sequence", "service", "severity", "status", "event_ts")
PAYLOAD_FIELDS = ("incident_id", "sequence", "service", "severity", "status", "summary", "event_ts")
MAX_LATENESS_DAYS = 2
MAX_LIFECYCLE_DAYS = 1


@dataclass(frozen=True)
class Service:
    name: str
    weight: int
    critical_bias: float
    summaries: tuple


# Summaries reuse runbook keywords so the assistant can map a service to a runbook.
SERVICES = (
    Service("checkout", 5, 0.35, (
        "checkout pods restarting with OOMKilled",
        "checkout deployment in CrashLoopBackOff after release",
    )),
    Service("edge-gateway", 4, 0.20, (
        "ingress returns 502 bad upstream after rollout",
        "gateway 502 errors while endpoints are not ready",
    )),
    Service("orders-db", 3, 0.20, (
        "postgresql connection pool exhausted",
        "database pool saturation on orders",
    )),
    Service("order-queue", 3, 0.10, (
        "order queue backlog growing, oldest message delay rising",
        "sqs consumers falling behind on backlog",
    )),
    Service("ci-platform", 2, 0.05, (
        "github actions blocked by terraform state lock",
        "terraform pipeline locked on shared state",
    )),
    Service("cluster-dns", 2, 0.15, (
        "coredns servfail on service lookups",
        "intermittent dns resolution failures in cluster",
    )),
)


def load_schema():
    with SCHEMA_PATH.open(encoding="utf-8") as source:
        return json.load(source)


def format_ts(value):
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_ts(value):
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def parse_date(value):
    return value if isinstance(value, date) else date.fromisoformat(value)


def date_range(start, end):
    start, end = parse_date(start), parse_date(end)
    if end < start:
        raise ValueError(f"end date {end} is before start date {start}")
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def _rng(seed, *parts):
    digest = hashlib.sha256(":".join(map(str, (seed, *parts))).encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def event_id(incident_id, sequence):
    return "evt-" + hashlib.sha256(f"{incident_id}:{sequence}".encode()).hexdigest()[:16]


def _incident_events(seed, day, number):
    """Return (delivery_date, event) pairs for one incident opened on `day`."""
    rng = _rng(seed, day.isoformat(), number)
    service = rng.choices(SERVICES, weights=[item.weight for item in SERVICES])[0]
    if rng.random() < service.critical_bias:
        severity = "critical"
    else:
        severity = rng.choices(SEVERITIES[1:], weights=(4, 4, 2))[0]
    incident_id = f"INC-{day:%Y%m%d}-{number:03d}"
    summary = rng.choice(service.summaries)
    happened = datetime.combine(day, time(), timezone.utc) + timedelta(seconds=rng.randrange(86400))
    # Most incidents resolve; some stay mitigated or acknowledged.
    steps = rng.choices((2, 3, 4), weights=(1, 2, 7))[0]
    gaps_minutes = ((5, 30), (20, 180), (30, 600))
    pairs = []
    for sequence, status in enumerate(STATUSES[:steps], start=1):
        if sequence > 1:
            low, high = gaps_minutes[sequence - 2]
            happened += timedelta(minutes=rng.randint(low, high))
        lateness = rng.choices((0, 1, 2), weights=(85, 12, 3))[0]
        event = {
            "event_id": event_id(incident_id, sequence),
            "incident_id": incident_id,
            "sequence": sequence,
            "service": service.name,
            "severity": severity,
            "status": status,
            "summary": summary,
            "event_ts": format_ts(happened),
        }
        pairs.append((happened.date() + timedelta(days=lateness), event))
    return pairs


def incidents_opened_on(day, seed=DEFAULT_SEED):
    day = parse_date(day)
    count = _rng(seed, day.isoformat(), "count").randint(3, 7)
    return [_incident_events(seed, day, number) for number in range(1, count + 1)]


def generate_batch(batch_date, seed=DEFAULT_SEED):
    """Return every event delivered on `batch_date`, sorted by event time and ID."""
    batch_date = parse_date(batch_date)
    lookback = MAX_LATENESS_DAYS + MAX_LIFECYCLE_DAYS
    rows = []
    for offset in range(lookback, -1, -1):
        for incident in incidents_opened_on(batch_date - timedelta(days=offset), seed):
            for delivered, event in incident:
                if delivered == batch_date:
                    rows.append({**event, "batch_date": batch_date.isoformat()})
    return sorted(rows, key=lambda row: (row["event_ts"], row["event_id"]))


def late_events(rows):
    """Events whose event date is earlier than the batch delivery date."""
    return [row for row in rows if row["event_ts"][:10] < row["batch_date"]]


def inject_faults(rows):
    """Return a copy of a valid batch with one instance of each detectable defect.

    The returned mapping explains which check each fault should trigger.
    """
    if len(rows) < 6:
        raise ValueError("need at least six rows to inject every fault")
    faulty = [dict(row) for row in rows]
    other_service = {item.name for item in SERVICES}
    faults = {}

    missing_key = {**faulty[0], "event_id": None, "sequence": 99}
    faulty.append(missing_key)
    faults["assert_event_id_not_null"] = f"copy of {faulty[0]['event_id']} without event_id"

    conflict = dict(faulty[1])
    conflict["status"] = "open" if conflict["status"] != "open" else "resolved"
    faulty.append(conflict)
    faults["assert_event_id_unique"] = f"{conflict['event_id']} delivered twice with different status"

    faulty[2]["severity"] = "urgent"
    faults["assert_valid_severity"] = f"{faulty[2]['event_id']} has severity 'urgent'"

    faulty[3]["service"] = None
    faults["assert_required_fields_not_null"] = f"{faulty[3]['event_id']} has no service"

    future = parse_ts(faulty[4]["event_ts"]) + timedelta(days=30)
    faulty[4]["event_ts"] = format_ts(future)
    faults["assert_event_ts_not_future"] = f"{faulty[4]['event_id']} is dated {faulty[4]['event_ts']}"

    # Pick an untouched row whose incident has another untouched event in this batch.
    untouched = faulty[5:len(rows)]
    target = next(
        (row for row in untouched
         if sum(other["incident_id"] == row["incident_id"] for other in untouched) > 1),
        None,
    )
    if target is None:
        raise ValueError("no incident with two untouched events to corrupt")
    original = target["service"]
    target["service"] = sorted(other_service - {original})[0]
    faults["assert_incident_single_service"] = (
        f"{target['incident_id']} reported by {original} and {target['service']}"
    )
    return faulty, faults


def to_jsonl(rows):
    return "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)


def read_jsonl(path):
    with Path(path).open(encoding="utf-8") as source:
        return [json.loads(line) for line in source if line.strip()]


def batch_path(landing_dir, batch_date):
    return Path(landing_dir) / f"batch_date={parse_date(batch_date).isoformat()}" / "events.jsonl"


def write_batch(landing_dir, batch_date, rows):
    path = batch_path(landing_dir, batch_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(to_jsonl(rows), encoding="utf-8")
    return path
