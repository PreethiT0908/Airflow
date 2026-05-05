# Dynamic DAG Service — airflow_job1 v1

> **Version:** 1.0.0 | **Dev:** Airflow 3.0.6 | **Prod:** Airflow 3.1.8

A FastAPI service that dynamically generates Airflow DAG Python files from a JSON payload. You describe your workflow as a list of nodes — the service builds, validates, and writes the DAG file to disk for Airflow to pick up automatically.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Two Phases: Build and Run](#2-two-phases-build-and-run)
3. [Installation and Setup](#3-installation-and-setup)
4. [Environment Variables](#4-environment-variables)
5. [Build Phase — API Reference](#5-build-phase--api-reference)
6. [Node Configuration Reference](#6-node-configuration-reference)
7. [Execution Modes](#7-execution-modes)
8. [Parallel Lanes and Layer Structure](#8-parallel-lanes-and-layer-structure)
9. [Branching](#9-branching)
10. [Merging](#10-merging)
11. [Why TaskGroup Was Removed](#11-why-taskgroup-was-removed)
12. [Run Phase — Triggering the DAG](#12-run-phase--triggering-the-dag)
13. [Async Node — How Polling Works](#13-async-node--how-polling-works)
14. [Resume a Failed DAG Run](#14-resume-a-failed-dag-run)
15. [Idempotency — Build and Runtime](#15-idempotency--build-and-runtime)
16. [Kafka Events](#16-kafka-events)
17. [Error Handling Reference](#17-error-handling-reference)
18. [Registry Files](#18-registry-files)
19. [DAG Structure — What Gets Generated](#19-dag-structure--what-gets-generated)
20. [Known Constraints and Rules](#20-known-constraints-and-rules)

---

## 1. Architecture Overview

```
  Your System
      │
      │  POST /build_dag  (once, at workflow design time)
      ▼
┌─────────────────────┐
│  Dynamic DAG        │   FastAPI + Pydantic validation
│  Service            │   Generates a .py DAG file
│  (this service)     │   Writes to AIRFLOW_DAGS_DIR
└─────────┬───────────┘
          │ writes dag_<id>_<timestamp>.py
          ▼
┌─────────────────────┐
│  Airflow DAGs       │   Airflow scheduler picks up the file
│  Folder             │   DAG appears in UI within scan interval
└─────────┬───────────┘
          │
          │  POST /api/v1/dags/<dag_id>/dagRuns  (at execution time)
          ▼
┌─────────────────────┐
│  Airflow            │   Runs the generated DAG
│  Scheduler + Worker │   Tasks call HTTP endpoints
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Kafka              │   run.started.v1 / run.succeeded.v1 / run.failed.v1
└─────────────────────┘
```

---

## 2. Two Phases: Build and Run

| Phase | Who calls it | When | What it does |
|-------|-------------|------|-------------|
| **Build** | Your system → `POST /build_dag` | Once per unique workflow | Validates nodes, generates DAG `.py` file, stores idempotency entry |
| **Run** | Your system → Airflow REST API | Every time you want to execute | Triggers the generated DAG with a `conf` payload containing per-node HTTP configs |

These are **fully decoupled**. Build once, run many times. The same build can be triggered hundreds of times with different `conf` payloads.

---

## 3. Installation and Setup

### Requirements

```
python >= 3.11
airflow >= 3.0.6
fastapi
uvicorn
pydantic >= 2.0
portalocker
requests
apache-airflow-providers-apache-kafka
apache-airflow-providers-standard
```

### Install

```bash
pip install fastapi uvicorn pydantic portalocker requests
pip install apache-airflow-providers-apache-kafka
pip install apache-airflow-providers-standard
```

### Run the service

```bash
# Dev
python dynamic_dag_service_v1_airflow306.py

# Or via uvicorn directly
uvicorn dynamic_dag_service_v1_airflow306:app --host 0.0.0.0 --port 8443

# Production
python dynamic_dag_service_v1_airflow318.py
```

### Health check

```bash
curl http://localhost:8443/health
# {"status": "UP", "version": "1.0.0-airflow306"}
```

---

## 4. Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AIRFLOW_DAGS_DIR` | `./dag_configs` | Where generated DAG files are written |
| `KAFKA_CONN_ID` | `genesis_kafka_conn` | Airflow connection ID for Kafka |
| `KAFKA_RUN_TOPIC` | `genesis.hub.run.events.v1` | Kafka topic for DAG lifecycle events |
| `BUILD_IDEMPOTENCY_REGISTRY` | `<DAGS_DIR>/build_registry.json` | Path to build idempotency registry |
| `RUNTIME_IDEMPOTENCY_REGISTRY` | `/tmp/dynamic_dag_runtime_registry.json` | Path to runtime idempotency registry (inside generated DAG) |
| `LOG_LEVEL` | `INFO` | Logging level: DEBUG, INFO, WARNING, ERROR |
| `HOST` | `127.0.0.1` | Service bind host |
| `PORT` | `8443` | Service bind port |

---

## 5. Build Phase — API Reference

### `POST /build_dag`

**Request body:**

```json
{
  "run_control_id": "DEMO_10",
  "triggerType": "O",
  "schedule": null,
  "nodes": [ ...node objects... ]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_control_id` | string | ✅ | Unique identifier for this workflow. Becomes the DAG ID prefix. |
| `triggerType` | string | No | `"O"` (one-off), `"M"` (manual), `"S"` (scheduled). Also accepts `"0"`, `"1"`, `"2"`. |
| `schedule` | string | No | Cron expression e.g. `"0 9 * * 1-5"`. `null` = no schedule. |
| `nodes` | array | ✅ | List of node objects (min 1). |

**Response (201):**

```json
{
  "status": "SUCCESS",
  "dag_id": "demo_10_dag",
  "file": "demo_10_dag_20260505_100246.py",
  "path": "/airflow/dags/demo_10_dag_20260505_100246.py",
  "idempotency_key": "sha256hex...",
  "idempotent_reused": false
}
```

If the exact same payload is sent again, `idempotent_reused: true` is returned and no new file is written.

**Validation errors return 422** with a `detail` array describing exactly what failed.

---

## 6. Node Configuration Reference

Each node in the `nodes` array:

```json
{
  "id": "task1",
  "name": "KK_File_Transfer",
  "engine": "PYTHON",
  "executor_order_id": 1,
  "executor_sequence_id": 1,
  "execution_mode": "sync",
  "branch_on_status": false,
  "on_success_node_ids": [],
  "on_failure_node_ids": []
}
```

| Field | Type | Required | Rules |
|-------|------|----------|-------|
| `id` | string | ✅ | Must be unique across all nodes. Used as the key in `conf` at run time. |
| `name` | string | ✅ | Must be unique across all nodes. Becomes the Airflow task ID exactly as written. |
| `engine` | string | ✅ | Free text. Currently informational (e.g. `"PYTHON"`, `"JAVA"`). |
| `executor_order_id` | int ≥ 1 | ✅ | Stage/layer number. Nodes with the same value run in parallel. |
| `executor_sequence_id` | int ≥ 1 | ✅ | Position within a layer. Must be unique per `executor_order_id`. |
| `execution_mode` | string | No | `"sync"` (default), `"async_no_wait"`, `"fire_and_forget"` |
| `branch_on_status` | bool | No | `false` (default). Set `true` to enable success/failure routing. |
| `on_success_node_ids` | array | No | Node IDs to route to on success. Only valid when `branch_on_status: true`. |
| `on_failure_node_ids` | array | No | Node IDs to route to on failure. Only valid when `branch_on_status: true`. |

### Critical naming rules

- `name` becomes the **Airflow task ID verbatim**. What you put in `name` is exactly what appears in the Airflow UI.
- `name` must be **unique across the entire DAG**. Duplicate names cause a 422 error at build time.
- `id` must also be unique. It is used as the key in `dag_run.conf` at run time.
- `id` and `name` can be different — `id` is your internal key, `name` is the display label.

---

## 7. Execution Modes

### `sync` (default)

Makes an HTTP call and waits for the response. Task completes when the HTTP response is received.

```
submit → wait for HTTP response → mark success/failure
```

### `async_no_wait`

Makes an HTTP call to start a job, extracts a `tracking_id` from the response, then **polls a status endpoint** in a loop until the job reaches a terminal state. The Airflow worker slot is held for the entire polling duration.

```
submit → get tracking_id → poll status every N seconds → terminal state → mark success/failure
```

> **Note on naming:** Despite the name `async_no_wait`, this mode **does wait**. It blocks the Airflow worker. The name reflects that it does not wait for the initial HTTP call to complete the job itself — it submits and then polls. See [Section 13](#13-async-node--how-polling-works) for full detail.

### `fire_and_forget`

Makes an HTTP call and immediately marks success regardless of what the service does with it. No polling. No tracking.

```
submit → mark success (whether the job runs or not)
```

> `fire_and_forget` nodes **cannot** use `branch_on_status: true`. They are also **excluded from merge guards** — downstream nodes do not wait for them to complete.

---

## 8. Parallel Lanes and Layer Structure

Nodes with the **same `executor_order_id`** run in parallel. Different `executor_sequence_id` values within the same order distinguish them.

```
executor_order_id=1  →  task1 (seq=1)
executor_order_id=2  →  task2 (seq=1), task3 (seq=2), task4 (seq=3)   ← parallel
executor_order_id=3  →  task5 (seq=1)
```

Airflow graph:

```
         task1
    ┌──────┼──────┐
  task2  task3  task4
    └──────┼──────┘
         task5
```

Parent-child wiring for parallel layers:
- If the next layer has **one node**, all parents wire to it.
- If the next layer has **multiple nodes**, each child is wired to the parent with the **same `executor_sequence_id`**.

---

## 9. Branching

Set `branch_on_status: true` on a node to make it route to different downstream nodes based on whether it succeeded or failed.

### Build payload

```json
{
  "id": "task2",
  "name": "KK_Validation",
  "executor_order_id": 2,
  "executor_sequence_id": 1,
  "execution_mode": "sync",
  "branch_on_status": true,
  "on_success_node_ids": ["task3"],
  "on_failure_node_ids": ["task4"]
}
```

### What gets generated

A `BranchPythonOperator` named `branch__KK_Validation` is inserted immediately after `KK_Validation`. It reads the `{node_id}_branch` XCom key and routes to either the success or failure target list.

```
KK_Validation
      │
branch__KK_Validation
      ├── success → task3
      └── failure → task4
```

### Rules

| Rule | Enforced at |
|------|------------|
| `branch_on_status: true` requires at least one of `on_success_node_ids` or `on_failure_node_ids` | Build (422) |
| `branch_on_status: false` must have empty branch target lists | Build (422) |
| All node IDs in branch targets must exist in the nodes list | Build (422) |
| A node ID cannot appear in both success and failure lists | Build (422) |
| `fire_and_forget` nodes cannot have `branch_on_status: true` | Build (422) |

---

## 10. Merging

When two or more branches converge back to a single node, that node is automatically detected as a **merge node** at build time.

### How merge is detected

If a node has more than one upstream task in the dependency graph, it is a merge node.

### What is applied to merge nodes

1. **Trigger rule:** `NONE_FAILED_MIN_ONE_SUCCESS` — the merge node fires as long as at least one upstream succeeded and none failed. Airflow-skipped branches (from `BranchPythonOperator`) do not block execution.

2. **Merge guard:** At runtime, before executing, the merge node calls `_check_merge_guard()` which inspects the XCom `{node_id}_task_state` of every non-fire-and-forget upstream node:

| Upstream XCom state | In branch skip whitelist? | Decision |
|--------------------|--------------------------|---------|
| `"success"` | any | ✅ Pass |
| empty/None | Yes | ✅ Pass (expected branch skip) |
| empty/None | No | ❌ Fail — unexpected skip |
| `"failed"` | any | ❌ Fail |

`fire_and_forget` upstream nodes are **excluded** from this check entirely.

### Example

```
task1 (branch_on_status: true)
  ├── success → task2
  └── failure → task3
              ↓
           task4   ← merge node
```

If task1 succeeds → task2 runs, task3 is Airflow-skipped → task4 runs (task3 skip is expected, whitelisted).
If task1 fails → task3 runs, task2 is Airflow-skipped → task4 runs (task2 skip is expected, whitelisted).

---

## 11. Why TaskGroup Was Removed

### What TaskGroup does

`TaskGroup` in Airflow is a **UI-only visual grouping**. It draws a collapsible box around tasks in the graph view. It has zero effect on execution order, dependency logic, trigger rules, or performance.

In the original code, every `executor_order_id` layer was wrapped in a `TaskGroup` named `stage_N`.

### Why it was removed

| Problem | Impact |
|---------|--------|
| Task IDs become `stage_3.KK_File_DB` instead of `KK_File_DB` | XCom lookups by `task_ids=` break unless the full prefixed ID is used |
| Branch task IDs become `stage_3.branch__KK_File_DB` | `choose_branch` and `finalize_results` must know the prefix |
| The node `name` you provide no longer matches the task ID visible in the UI | Violates the "name = task ID" contract |
| For small DAGs (≤15 nodes) it adds navigation friction, not clarity | Collapsing a 3-node group saves no scrolling |

### When you should add TaskGroup back

Only if you have **20+ nodes** and want the Airflow graph to be navigable. If you do, be sure to:
- Prefix all XCom `task_ids=` lookups with the group ID
- Update `TASK_ID_MAP` to include the group prefix
- Update `choose_branch` and `finalize_results` accordingly

---

## 12. Run Phase — Triggering the DAG

### Via Airflow REST API

```bash
curl -X POST \
  "http://<airflow-host>/api/v1/dags/demo_10_dag/dagRuns" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic <base64>" \
  -d '{
    "logical_date": "2026-05-05T10:00:00Z",
    "conf": {
      "correlation_id": "your-trace-id",
      "resume": false,
      "resume_from": null,
      "force_rerun": false,
      "force_rerun_nodes": [],
      "task1": {
        "url": "http://service/endpoint",
        "method": "POST",
        "headers": { "Authorization": "Bearer token" },
        "json": { "key": "value" },
        "timeout": 300,
        "verify_ssl": false
      },
      "task2": { ... }
    }
  }'
```

### Conf key = node id (exact match)

The key inside `conf` **must exactly match** the `id` from the build payload. If your node was built with `"id": "task3"`, the conf key must be `"task3"`. No sanitization, no aliases.

### Per-node conf fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `url` | ✅ Yes | — | HTTP endpoint |
| `method` | No | `POST` | HTTP verb |
| `headers` | No | `{}` | Request headers |
| `json` | No | `null` | JSON request body |
| `params` | No | `{}` | Query string parameters |
| `timeout` | No | `300` | Request timeout in seconds |
| `verify_ssl` | No | `false` | SSL certificate verification |

For async nodes, add a `status` block. See [Section 13](#13-async-node--how-polling-works).

---

## 13. Async Node — How Polling Works

An `async_no_wait` node submits a job and then polls a separate status endpoint until a terminal state is reached. The entire sequence happens **within a single Airflow task** — the worker slot is held throughout.

### Step-by-step

```
1. HTTP POST to node.url  →  submit job
2. Extract tracking_id from submit response
3. Loop: HTTP GET/POST to status.url every poke_interval seconds
4. Check response[response_status_key] against success/failure/running sets
5. On SUCCESS → mark task success, push XCom, continue DAG
6. On FAILURE → mark task failed, raise AirflowException
7. On TIMEOUT → raise AirflowException with last response
```

### tracking_id resolution order

The tracking_id is extracted from the submit response in this priority:

1. `status.tracking_id` (explicit override in conf)
2. `payload.tracking_id` (explicit override in conf)
3. `response_body[response_id_key]` — key named by `response_id_key` (default: `"job_id"`)
4. `response_body["run_id"]`
5. `response_body["runId"]`
6. `response_body["id"]`
7. `response_body["jobId"]`
8. `response_body["request_id"]`

If none of these resolve a value, the task fails immediately with a clear error.

### Async node conf structure

```json
"task3": {
  "url": "https://orchestrator/jobs",
  "method": "POST",
  "headers": { "Authorization": "Basic abc=" },
  "json": { "job_id": "abc", "payload": { ... } },
  "timeout": 300,
  "verify_ssl": false,
  "response_id_key": "runId",
  "status": {
    "url": "https://orchestrator/jobs/{tracking_id}",
    "method": "GET",
    "headers": { "Authorization": "Basic abc=" },
    "response_status_key": "status",
    "poke_interval": 15,
    "timeout": 3600,
    "success_statuses": ["SUCCESS", "COMPLETED"],
    "failure_statuses": ["FAILED", "ERROR", "ABORTED"],
    "running_statuses": ["RUNNING", "PENDING", "IN_PROGRESS"]
  }
}
```

### Status URL template variables

Available inside `status.url` and `status.json`:

| Variable | Value |
|----------|-------|
| `{tracking_id}` | Resolved job/run ID |
| `{job_id}` | Alias for tracking_id |
| `{run_id}` | Alias for tracking_id |
| `{node_id}` | Node's build-time id |
| `{node_name}` | Node's build-time name |
| `{dag_id}` | Airflow DAG ID |
| `{dag_run_id}` | Airflow DAG run ID |
| `{run_control_id}` | Top-level run_control_id |

### Default status sets

These apply if you don't provide custom sets:

```
SUCCESS: SUCCESS, SUCCEEDED, COMPLETED, DONE, FINISHED
FAILURE: FAILED, FAILURE, ERROR, ERRORED, CANCELLED, CANCELED, ABORTED
RUNNING: RUNNING, IN_PROGRESS, PENDING, QUEUED, SUBMITTED, PROCESSING, STARTED
```

---

## 14. Resume a Failed DAG Run

### Top-level conf fields

```json
{
  "conf": {
    "correlation_id": "same-as-original-run",
    "resume": true,
    "resume_from": "task7",
    "force_rerun": false,
    "force_rerun_nodes": []
  }
}
```

### How resume works

Resume operates at the **`executor_order_id` level** — not the individual node level.

- Nodes with `executor_order_id` **strictly less than** the `resume_from` node's order → **skipped** (XCom markers set to success, no HTTP call)
- Nodes with `executor_order_id` **equal to or greater than** `resume_from` node's order → **execute normally**
- Idempotency still applies: if an eligible node already has a success entry in the runtime registry with the same `correlation_id` and payload hash, it skips the HTTP call automatically

### force_rerun_nodes

Force specific nodes to re-execute even if they are before the resume boundary:

```json
"force_rerun_nodes": ["task2", "task4"]
```

### force_rerun

Set `"force_rerun": true` to bypass all idempotency and re-execute every node from scratch.

### resume_from validation

If `resume_from` is set to a node ID that was not in the build payload, the DAG **immediately raises an AirflowException** before any task runs:

```
AirflowException: [RESUME] resume_from='task99' is not a valid node ID.
Valid node IDs: ['task1', 'task2', ..., 'task10']
```

---

## 15. Idempotency — Build and Runtime

### Build-time idempotency

When `POST /build_dag` is called, a SHA-256 hash is computed over the canonicalized payload (sorted nodes, sorted fields, stripped strings). If that hash already exists in `build_registry.json` **and** the DAG file still exists on disk, the existing entry is returned without generating a new file.

If the DAG file has been deleted but the registry entry remains, the stale entry is removed and a fresh file is generated.

### Runtime idempotency

Before each node's HTTP call, a key is computed from:

```
SHA-256 of {
  dag_id,
  run_control_id,
  correlation_id,
  node_id,
  SHA-256 of node's conf payload
}
```

If this key has a `"state": "success"` entry in `runtime_registry.json`, the HTTP call is skipped and the previous result is replayed via XCom. This means re-triggering the same DAG with the same `correlation_id` and same conf is safe.

To bypass runtime idempotency for specific nodes: `"force_rerun_nodes": ["task3"]`
To bypass for all nodes: `"force_rerun": true`

---

## 16. Kafka Events

Two events are produced per DAG run, both to `KAFKA_RUN_TOPIC`.

### run.started.v1 — sent before any node tasks run

```json
{
  "eventType": "run.started.v1",
  "run_control_id": "DEMO_10",
  "correlation_id": "your-trace-id",
  "event_source": "AIRFLOW",
  "status": "RUNNING",
  "trigger_payload": "{ ... full conf as JSON string ... }",
  "dagId": "demo_10_dag",
  "dagRunId": "manual__2026-05-05T10:00:00+00:00",
  "timestamp": "2026-05-05T10:00:01Z"
}
```

### run.succeeded.v1 / run.failed.v1 — sent after all nodes complete

```json
{
  "eventType": "run.succeeded.v1",
  "run_control_id": "DEMO_10",
  "correlation_id": "your-trace-id",
  "event_source": "AIRFLOW",
  "status": "SUCCESS",
  "trigger_payload": "{ ... }",
  "dagId": "demo_10_dag",
  "dagRunId": "manual__2026-05-05T10:00:00+00:00",
  "timestamp": "2026-05-05T10:05:22Z"
}
```

The final event's `trigger_rule` is `ALL_DONE` — it fires even if the DAG failed.

---

## 17. Error Handling Reference

### Build phase errors

| Error | HTTP | Cause | Action |
|-------|------|-------|--------|
| Duplicate node `id` | 422 | Two nodes share the same `id` | Make IDs unique |
| Duplicate node `name` | 422 | Two nodes share the same `name` | Make names unique — names become Airflow task IDs |
| Duplicate `(executor_order_id, executor_sequence_id)` | 422 | Two nodes in same position | Fix sequencing |
| Unknown branch target | 422 | `on_success_node_ids` references a non-existent node | Fix the reference |
| `branch_on_status: true` with no targets | 422 | No branch targets configured | Add at least one target |
| `branch_on_status: false` with targets | 422 | Targets set but branching disabled | Set `branch_on_status: true` or remove targets |
| Node in both success and failure targets | 422 | Same node in both lists | Remove from one list |
| `fire_and_forget` with `branch_on_status: true` | 422 | Incompatible combination | Remove `branch_on_status` from fire_and_forget node |
| Registry write failure | 500 + warning | Disk full or permission error | DAG file IS written; idempotency entry is NOT stored |
| Generated code syntax error | 500 | Bug in code generator | File the issue |

### Runtime errors (inside generated DAG)

| Error | Behaviour | Recovery |
|-------|-----------|----------|
| Missing `url` in conf for a node | Task fails immediately | Add `url` to conf |
| HTTP 401/403 | Task fails, XCom marked `failed` | Fix Authorization header |
| HTTP non-2xx | Task fails, XCom marked `failed` | Fix endpoint or payload |
| Network timeout/exception | Task fails, XCom marked `failed` | Check connectivity; use `force_rerun_nodes` to retry |
| `async_no_wait` missing `status` block | Task fails immediately | Add `status` block to conf |
| `async_no_wait` tracking_id not found | Task fails immediately | Add correct `response_id_key` or check submit response |
| `async_no_wait` polling timeout | Task fails | Increase `status.timeout` or investigate external service |
| `resume_from` unknown node ID | First task raises exception | Fix `resume_from` to use a valid node `id` |
| Merge guard upstream failed | Merge node raises exception | Fix the failing upstream node and resume |
| Merge guard unexpected skip | Merge node raises exception | Investigate why upstream had no XCom state |
| `choose_branch` no failure targets | Branch router raises exception | Add `on_failure_node_ids` to the branching node in build payload |
| portalocker not installed | RuntimeError on any task | `pip install portalocker` |

---

## 18. Registry Files

### build_registry.json

Location: `$BUILD_IDEMPOTENCY_REGISTRY` (default: `<DAGS_DIR>/build_registry.json`)

Stores one entry per unique build payload. Key = SHA-256 hash of the canonical payload.

```json
{
  "<sha256>": {
    "dag_id": "demo_10_dag",
    "file": "demo_10_dag_20260505_100246.py",
    "path": "/airflow/dags/demo_10_dag_20260505_100246.py",
    "created_at": "2026-05-05T10:02:46.123456+00:00",
    "payload_hash": "<sha256>",
    "node_count": 10,
    "airflow_version": "3.0.6"
  }
}
```

**Locking:** `build_registry.lock` (portalocker exclusive lock). The lock is held during both read and write as a context manager — released even on exception.

### runtime_registry.json

Location: `$RUNTIME_IDEMPOTENCY_REGISTRY` (default: `/tmp/dynamic_dag_runtime_registry.json`)

Stores one entry per node execution attempt. Key = SHA-256 of `{dag_id, run_control_id, correlation_id, node_id, payload_hash}`.

```json
{
  "<sha256>": {
    "state": "success",
    "node_id": "task3",
    "node_name": "KK_File_DB",
    "execution_mode": "async_no_wait",
    "dag_id": "demo_10_dag",
    "dag_run_id": "manual__2026-05-05T10:00:00+00:00",
    "correlation_id": "your-trace-id",
    "tracking_id": "job-abc-123",
    "http_status": 202,
    "submit_response": { ... },
    "result": { ... },
    "finished_at": "2026-05-05T10:03:45Z"
  }
}
```

---

## 19. DAG Structure — What Gets Generated

Every generated DAG has this fixed skeleton:

```
prepare_inputs
      │
run_started_event  ← Kafka: run.started.v1
      │
   [your nodes, wired per executor_order_id layers]
      │
finalize_results   ← trigger_rule: ALL_DONE
      │
run_final_event    ← Kafka: run.succeeded.v1 or run.failed.v1
                     trigger_rule: ALL_DONE
```

### XCom keys written per node

| Key | Value | Description |
|-----|-------|-------------|
| `{node_id}_task_state` | `"started"` / `"success"` / `"failed"` | Primary state marker |
| `{node_id}_branch` | `"success"` / `"failure"` | Used by BranchPythonOperator |
| `{node_id}_response` | response body dict | HTTP response or poll result |
| `{node_id}_error` | error message string | Set on failure |
| `{node_id}_tracking_id` | tracking_id string | async_no_wait only |
| `{node_id}_submit_response` | submit response body | async_no_wait and fire_and_forget |
| `{node_id}_submit_http_status` | int | HTTP status of submit call |
| `{node_id}_idempotency_key` | sha256 string | For debugging idempotency |

### Constants baked into the generated DAG

These are resolved at build time and written as literals into the `.py` file:

| Constant | Description |
|----------|-------------|
| `RUN_CONTROL_ID` | The `run_control_id` from build payload |
| `FINAL_NODE_IDS` | List of all node IDs |
| `ASYNC_NODE_IDS` | IDs of async_no_wait nodes |
| `FIRE_AND_FORGET_NODE_IDS` | IDs of fire_and_forget nodes |
| `NODE_NAME_MAP` | `{node_id: node_name}` |
| `BRANCH_SKIP_WHITELIST` | Task IDs that are valid Airflow-skipped targets |
| `MERGE_NODE_IDS` | Task IDs with multiple upstreams |
| `NODE_ORDER_MAP` | `{node_id: executor_order_id}` |
| `TASK_ID_MAP` | `{node_id: airflow_task_id}` |

---

## 20. Known Constraints and Rules

| Constraint | Detail |
|-----------|--------|
| Node `name` must be unique per DAG | Enforced at build time. Name IS the Airflow task ID. |
| Node `id` must be unique per DAG | Enforced at build time. ID IS the conf key at run time. |
| `(executor_order_id, executor_sequence_id)` must be unique | Enforced at build time. |
| `fire_and_forget` cannot branch | No terminal state tracking means no reliable branch signal. |
| `async_no_wait` must have a `status` block in conf | Fails immediately with clear error if missing. |
| Merge nodes re-execute all peers in the same layer on resume | Resume is order-level granularity. Idempotency protects already-successful nodes. |
| TaskGroup removed | Task IDs are flat. No group prefix. XCom lookups use the name directly. |
| portalocker is required | Not optional. Missing it raises RuntimeError on any task. |
| The generated DAG is a standalone Python file | It contains all runtime logic as embedded functions. No external imports from this service. |
| `max_active_runs=1` | One run at a time per DAG. Prevents concurrent execution of the same workflow. |

---

## Version History

| Version | Airflow | Changes |
|---------|---------|---------|
| 1.0.0 | 3.0.6 / 3.1.8 | Initial versioned release. All fixes from original merged file applied. TaskGroup removed. Node name = task ID. Merge guard added. Resume validation added. PydanticValidationError handler added. portalocker context manager fix. Template f-string escaping fixed. |
