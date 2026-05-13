# Dynamic DAG Service — airflow_job1

Version 1.3  ·  Airflow 3.0.6 (dev) / 3.1.8 (prod)

---

## What problem does this solve

Running the same orchestration framework across dozens of workflows means maintaining dozens of near-identical DAG files. One typo in the wrong place breaks a pipeline and there is no single place to look when something goes wrong.

This service changes that. You describe what you want — a list of services to call, in what order, with what execution behaviour — and it generates the Airflow DAG file. The DAG itself contains no hardcoded endpoints, no credentials, none of that. All the runtime specifics travel separately in the trigger payload when someone actually kicks off a run.

One service to maintain instead of fifty DAG files.

---

## How the two sides fit together

There are two separate machines involved and it matters to understand which is which.

```
Your orchestrator
      │
      ├── POST /build_dag ──────────────────► FastAPI VM
      │                                        writes <dag_id>.py to DAGs folder
      │                                        tracks it in build_registry.json
      │
      └── POST Airflow trigger API ──────────► Airflow VM
                                               scheduler picks up the .py file
                                               workers run the tasks
                                               each task makes HTTP calls
                                               Kafka events fire at start and end
```

The DAG file travels from the FastAPI VM to the Airflow VM through a git-synced folder or a shared network mount. Both VMs need their own set of packages — they do not share the same Python environment.

---

## The three node fields — read this before anything else

Every node in the build payload has three fields that do completely different jobs. Getting this wrong is the most common source of confusion.

**`id`**
Internal reference only. Never shown in the Airflow UI, never used at runtime. Auto-assigns as `task1`, `task2`, etc. if you leave it out. This is what `on_success_node_ids` and `on_failure_node_ids` reference for branching wiring.

**`name`**
The label Airflow shows for this task in the graph view — what you see on screen when you open the DAG. Does not need to be globally unique. If the same service name appears in two different stages the service appends `_{order}_{seq}` automatically.

**`executor_build_id`**
The stable service identifier. This is the key you use in the run-time conf when you trigger a DAG run. Required on every node — no default, no fallback. If a node has `executor_build_id: "EDM_Location_Inbound_NAS_TO_PVC"` then the run conf must have `"EDM_Location_Inbound_NAS_TO_PVC": { "url": "...", "json": {...} }`.

---

## System requirements

### FastAPI VM

| Requirement | Minimum |
|---|---|
| Python | 3.10 |
| pip | current |
| Write access to the DAGs folder | required |
| Network access to Airflow API | required |

### Airflow VM

| Requirement | Version |
|---|---|
| Apache Airflow | **3.0.6** |
| Python | 3.10 |
| Kafka broker reachable from workers | required |
| Airflow connection `genesis_kafka_conn` | must exist in Admin → Connections |

---

## Installation

### FastAPI VM

```bash
git clone <repo-url>
cd dynamic-dag-service

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt

cp .env.example .env
# open .env and at minimum set AIRFLOW_DAGS_DIR
```

### Airflow VM

```bash
# run inside your Airflow worker container or directly on the worker machine
pip install -r requirements.txt
```

Restart the scheduler and workers after installing.

---

## Environment variables

### FastAPI VM

| Variable | Default | What it does |
|---|---|---|
| `AIRFLOW_DAGS_DIR` | `./dag_configs` | Where generated DAG `.py` files are written. Point this at the folder your Airflow scheduler watches — a git-synced path or NFS mount. |
| `BUILD_IDEMPOTENCY_REGISTRY` | `<AIRFLOW_DAGS_DIR>/build_registry.json` | JSON file that tracks which build payloads have already been processed so identical payloads do not overwrite a working DAG file. |
| `KAFKA_CONN_ID` | `genesis_kafka_conn` | Must match the connection ID you created in Airflow Admin → Connections. |
| `KAFKA_RUN_TOPIC` | `genesis.hub.run.events.v1` | Topic where run start and end events land. |
| `LOG_LEVEL` | `INFO` | Set to `DEBUG` during development if you need to trace requests. |
| `HOST` | `127.0.0.1` | Bind address. Change to `0.0.0.0` if the service needs to accept connections from other machines. |
| `PORT` | `8443` | Port uvicorn binds to. |

### Airflow VM

| Variable | Default | What it does |
|---|---|---|
| `HTTP_RETRY_ATTEMPTS` | `3` | How many times a failing HTTP call is retried before the task gives up. |
| `HTTP_RETRY_MIN_WAIT_SECONDS` | `2` | Minimum seconds to wait between retries. |
| `HTTP_RETRY_MAX_WAIT_SECONDS` | `10` | Maximum wait. Actual wait grows exponentially between min and max. |

> `RUNTIME_IDEMPOTENCY_REGISTRY` was removed in v1.2. Do not set it.

---

## Starting the service

```bash
source venv/bin/activate
python dynamic_dag_service_v1_airflow306.py
```

As a systemd service in production:

```bash
sudo systemctl start dynamic-dag-service
sudo systemctl enable dynamic-dag-service
```

Health check:

```bash
curl http://127.0.0.1:8443/health
# {"status": "UP", "version": "1.3.0-airflow306"}
```

---

## API endpoints

### GET /health

Quick liveness check. Returns service version.

```json
{"status": "UP", "version": "1.3.0-airflow306"}
```

---

### POST /build_dag

Generates the Airflow DAG file. The only endpoint you call at build time.

See `PAYLOAD_REFERENCE.md` for the complete schema and worked examples.

**Success (HTTP 201):**

```json
{
  "status": "SUCCESS",
  "dag_id": "demo_10_dag",
  "file": "demo_10_dag.py",
  "path": "/opt/dag_configs/demo_10_dag.py",
  "idempotency_key": "a3f9c2...",
  "idempotent_reused": false
}
```

When you send the same payload twice, `idempotent_reused` comes back `true` and nothing on disk is touched.

**Error codes:**

| Code | Cause |
|---|---|
| 422 | Payload validation failed. The `detail` array says exactly what is wrong — duplicate node ids, unknown branch targets, cycle detected, missing required fields. |
| 500 | Unexpected failure — disk permissions, template bug. Check service logs. |

---

## Execution modes

Every node declares `execution_mode`. Pick the one that matches how the downstream service behaves.

### sync

Makes one HTTP call, waits for the response, moves on. If the response is 2xx the task succeeds. Anything else and it fails with a full diagnostic block in the logs.

Use this for anything that completes inline in a reasonable time — typically under a few minutes.

### async_no_wait

Submits a job, extracts a tracking ID from the response body, then polls a status URL on an interval until the job reaches a terminal state. The task stays running and holds a worker slot the entire polling window.

**This mode requires a `status` block in the run-time conf.** Without it the task fails immediately with a message telling you exactly what is missing.

Polling behaviour:
- Continues while remote status is in `running_statuses`
- Succeeds when remote status is in `success_statuses`
- Fails when remote status is in `failure_statuses`
- Times out and fails if no terminal state is reached within `timeout` seconds

### fire_and_forget

Submits the job and immediately marks the task as done. No polling, no tracking, no care about the outcome. Correct for notifications, audit events, side effects.

Fire-and-forget tasks are excluded from the merge guard — downstream merge nodes do not wait for them to confirm anything beyond the initial HTTP submit going through.

---

## DAG structure

Every generated DAG looks like this regardless of what nodes you define:

```
prepare_inputs
      │
run_started_event       ← publishes run.started.v1 to Kafka
      │
  [ your nodes ]        ← wired by executor_order_id layers
      │
finalize_results        ← checks every node's XCom state (trigger_rule: ALL_DONE)
      │
run_final_event         ← publishes run.succeeded.v1 or run.failed.v1 (trigger_rule: ALL_DONE)
      │
completion              ← mirrors finalize_results outcome, sets DAG run status
```

**Why there is a `completion` node at the very end**

Without it a DAG run where business tasks failed could still show green. The Kafka operator (`run_final_event`) always succeeds — it just sends a message. So the last task succeeded, Airflow marks the run green. Wrong.

`completion` reads the `final_status` XCom that `finalize_results` wrote and raises an exception if the outcome was `FAILED`. That is what pushes the overall run to red.

**How finalize_results works**

Reads the `_task_state` XCom key from every node in the DAG. If any node wrote `failed`, or if any non-branch-skip node has no XCom at all, it raises and the run fails. Fire-and-forget nodes only need their submit to have gone through.

**How nodes connect to finalize_results**

Only the nodes in the final layer (highest `executor_order_id`) connect to `finalize_results` with a dependency arrow. This keeps the graph clean. `finalize_results` still inspects all nodes via XCom — the visible arrows and the health checking are separate things.

---

## Branching and merging

Set `branch_on_status: true` on a node to add a `BranchPythonOperator` immediately after it. The router reads the task's XCom outcome and sends execution down either `on_success_node_ids` or `on_failure_node_ids`.

Rules:
- `branch_on_status: true` requires at least one entry in one of the target lists
- A node cannot appear in both lists simultaneously
- `fire_and_forget` nodes cannot branch — they have no reliable terminal state
- Merge nodes (where both branch paths rejoin) get `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` automatically so the skipped branch path does not block them

---

## Resume and force-rerun

### Resuming a failed run

Trigger a new DAG run with `"resume": true` and `"resume_from": "<node_id>"`. Any node with `executor_order_id` less than the `resume_from` node's order gets skipped — it pushes a synthetic success XCom and returns without making any HTTP calls. Everything from `resume_from` onward runs normally.

Always use the same `correlation_id` as the original failed run so Kafka events link up correctly.

### Forcing specific nodes to re-run during resume

Put node ids in `force_rerun_nodes`. Those tasks run fully even if they would normally be skipped by the resume boundary. Everything else follows the resume rules.

Complete payload examples for both scenarios are in `PAYLOAD_REFERENCE.md`.

---

## XCom keys written by every task

| Key | Written when | Value |
|---|---|---|
| `{node_id}_task_state` | Throughout | `started` on entry → `success` or `failed` on exit |
| `{node_id}_branch` | On completion | `success` or `failure` |
| `{node_id}_response` | On success | HTTP response body as parsed JSON |
| `{node_id}_error` | On failure | Error message string |
| `{node_id}_submit_response` | async_no_wait and fire_and_forget | Response body from the initial submit call |
| `{node_id}_tracking_id` | async_no_wait only | Tracking ID extracted from the submit response |
| `{node_id}_submit_http_status` | async_no_wait and fire_and_forget | HTTP status code of the submit call |

`finalize_results` writes two additional keys:

| Key | Value |
|---|---|
| `final_status` | `SUCCESS` or `FAILED` |
| `final_summary` | Full JSON breakdown — which nodes succeeded, which failed, which were expected skips, which had no XCom |

---

## Kafka events

| Event type | Fires when | `status` value |
|---|---|---|
| `run.started.v1` | After `prepare_inputs`, before any task runs | `Running` |
| `run.succeeded.v1` | After `finalize_results` if outcome was SUCCESS | `Success` |
| `run.failed.v1` | After `finalize_results` if outcome was FAILED | `Failure` |

All events carry: `run_control_id`, `correlation_id`, `dagId`, `dagRunId`, `timestamp` (UTC ISO-8601), `status`, `trigger_payload` (the full conf as a JSON string).

`run_final_event` runs with `trigger_rule=ALL_DONE` so you always get a terminal Kafka event regardless of whether the business tasks passed or failed.

---

## Build-time idempotency

Every `/build_dag` call computes a SHA-256 hash of the canonicalised node list. If that hash already exists in `build_registry.json` the service returns the existing entry without touching the file. Same payload, same result, no extra work.

Change anything on any node — mode, order, name, build id — and the hash changes, triggering a fresh write.

The registry exists only on the FastAPI VM. There is no runtime registry on the Airflow VM.

---

## HTTP retry behaviour

All HTTP calls inside generated DAG tasks go through the retry wrapper. It uses tenacity with exponential backoff.

**Retried automatically:** `ConnectionError`, `Timeout`, `ChunkedEncodingError`, HTTP 500 / 502 / 503 / 504 / 429.

**Not retried:** HTTP 400, 401, 403, 404, 422. These are caller errors — sending the same request again will not fix them.

When retries run out the error in the Airflow task log includes the method, URL, attempt count, and last exception so you know exactly what was being called when it gave up.

---

## Error log format

Every HTTP failure from a generated task writes a structured diagnostic block to the Airflow task log:

```
  Node       : task1 (murex-script-execution)
  Build ID   : EDM_Location_Inbound_NAS_TO_PVC
  Stage      : submit
  ── Request ──────────────────────────
  Method     : POST
  URL        : http://your-service/api/execute
  Params     : None
  Body       : {"run_id": "abc123", "payload": null}
  ── Response ─────────────────────────
  HTTP Status: 422
  Body       : {"detail": [{"type":"missing","loc":["body"],"msg":"Field required"}]}
  Hint       : HTTP 422 — downstream service rejected the body.
               A required field is null or missing. Check the 'json' block
               under 'EDM_Location_Inbound_NAS_TO_PVC' in your run conf.
```

| HTTP | Usually means | Where to look |
|---|---|---|
| 401 / 403 | Auth failure | `headers.Authorization` in the run conf for this node |
| 404 | URL does not exist | The `url` field in the run conf |
| 422 | Body missing a required field | The `json` block in the run conf — something is null |
| 500–504 | Downstream server error | Will retry; check the downstream service logs if all retries fail |
| Network error | Service unreachable | Connectivity from the Airflow worker to that URL |

Passwords, secrets, tokens, and API keys in the request body are redacted before the log line is written.

---

## Known gaps

**async_no_wait holds a worker slot.** Long-running jobs keep a worker occupied for the full polling duration. Deferrable operators would fix this — on the backlog.

**No URL allowlist.** Any URL reachable from the Airflow worker can be called. `ALLOWED_URL_PREFIXES` env var guard is on the backlog.

**Kafka failures are not caught.** If the broker is down, `ProduceToTopicOperator` fails with no fallback. A try/except wrapper with log fallback is on the backlog.

**Large XCom values.** Big HTTP response bodies are stored in the Airflow metadata database as-is. Truncation logic is on the backlog.

---

## Version history

### v1.3 (current)
- `executor_build_id` is now required — no fallback to `name`
- `executor_build_id` is the primary run-time conf lookup key — `resolve_payload` checks `conf[executor_build_id]` first
- Kafka status values changed to title case: `Running`, `Success`, `Failure`
- Code comments updated throughout to clarify the three-field role split

### v1.2
- Runtime idempotency removed — portalocker and lock files gone from the Airflow VM
- `executionSteps` accepted as alias for `nodes` in the build payload
- Node `id` auto-assigned if omitted
- Duplicate node names allowed — deduplicated as `{name}_{order}_{seq}`
- `completion` terminal node added
- Only terminal layer nodes wire to `finalize_results` — cleaner graph
- HTTP error messages enriched with method, URL, body, response, and per-status hints

### v1.1
- DAG filenames no longer include a timestamp — always `{dag_id}.py`
- HTTP retry with tenacity on all HTTP calls
- Cycle detection at build time
- TaskGroup removed (was breaking XCom prefixes)
- Template f-string escaping fixed

### v1.0
- Initial release
