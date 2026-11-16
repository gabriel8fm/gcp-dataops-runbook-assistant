# Runbook Assistant Pilot

A local, read-only SRE assistant for answering incident questions using six
fictional runbooks. It handles Kubernetes restarts, ingress 502s, PostgreSQL
pool saturation, queue backlog, Terraform state locks, and cluster DNS failures.
All scenarios are synthetic. The assistant has no tools that can access a
cluster, execute remediation, or modify infrastructure.

## Run it

Requires Python 3.11+ with SQLite FTS5 (included in standard Python builds).
The default mode runs offline and needs no packages or API credentials.
Examples, runbook content, and answers use US English (en-US).

```bash
python3 runbook.py ask "Checkout restarting with OOMKilled"
python3 runbook.py ask "Ingress responding with 502 after rollout"
python3 runbook.py evaluate
python3 -m unittest discover -s tests -v
```

The JSON output includes the answer, cited runbook IDs, retrieved candidates, and
latency. Read the source material in [data/runbooks.json](data/runbooks.json).
Questions with no matching operational keyword receive an abstention.

## Optional local generation

Start a local [Ollama](https://ollama.com/) server on port 11434 and download a
local model that supports structured output, for example:

```bash
ollama pull qwen2.5:3b
python3 runbook.py ask "What should I check for intermittent CoreDNS SERVFAIL?" --model qwen2.5:3b
```

SQLite FTS5 ranks candidates using BM25, with a keyword match required for
eligibility. The default extractive mode returns the highest-ranked entry.
With `--model`, the app sends the question and up to two matches to the local
Ollama endpoint. It checks that cited IDs were retrieved and falls back to the
extractive answer if the endpoint fails or returns invalid citations. Citation
validation checks provenance, while factual consistency still requires review.
The integration is covered by mocked tests.

## Evaluation and cost

The evaluation set has 12 answerable and 3 out-of-scope questions. It reports
top-1 accuracy, recall@2, abstention on out-of-scope inputs, and p95 retrieval
response time. The use of curated synthetic questions and overlapping search
terms makes this a smoke test. It does not measure generated answer quality or
generalization to real incidents.

The extractive baseline has no paid API calls. The optional local model reports
input and output token counts when available and its generation latency. Host
compute and power costs are outside these measurements. This project requires a
locally downloaded model for generation and uses no hosted model API.
