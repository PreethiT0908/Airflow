# Dynamic Airflow DAG Service

A FastAPI microservice (`dynamic_dag_service_baremini.py`) that generates Airflow DAG Python files on the fly from a JSON payload describing a graph of HTTP nodes. Each generated DAG is a self-contained orchestration script that calls external HTTP endpoints in the correct order, handles branching, tracks idempotency, emits Kafka lifecycle events, and reports a final summary.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Running the Service](#running-the-service)
- [Environment Variables](#environment-variables)
- [API Endpoints](#api-endpoints)
  - [GET /health](#get-health)
  - [POST /build_dag](#post-build_dag)
- [build_dag Payload Structure](#build_dag-payload-structure)
  - [Top-level Fields](#top-level-fields)
  - [Node Fields](#node-fields)
  - [Trigger Type Values](#trigger-type-values)
  - [Execution Mode Values](#execution-mode-values)
- [runPhase Payload (DAG conf at trigger time)](#runphase-payload-dag-conf-at-trigger-time)
  - [Per-node Task Payload Fields](#per-node-task-payload-fields)
  - [Async Polling (status block)](#async-polling-status-block)
  - [Top-level conf Fields](#top-level-conf-fields)
- [Resume / Partial Re-run](#resume--partial-re-run)
- [Idempotency](#idempotency)
- [Generated DAG Lifecycle](#generated-dag-lifecycle)
- [Branching](#branching)
- [Local Testing (without Airflow)](#local-testing-without-airflow)

---

## Architecture Overview

```
Caller  →  POST /build_dag  →  Service generates dag_<id>_<ts>.py  →  Airflow picks it up
Caller  →  Trigger DAG run with conf JSON  →  Generated DAG runs HTTP nodes
```

The service itself has **no runtime dependency on Airflow** — it only writes `.py` files to a folder that Airflow watches. All Airflow-specific imports live inside the generated DAG file.

---

## Prerequisites

```bash
pip install fastapi uvicorn pydantic portalocker
```

---

## Running the Service

```bash
python dynamic_dag_service_baremini.py
```

By default the service starts on `http://127.0.0.1:8443`. Override with env vars:

```bash
HOST=0.0.0.0 PORT=8080 python dynamic_dag_service_baremini.py
```

---

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `AIRFLOW_DAGS_DIR` | `./dag_configs` | Where generated DAG `.py` files are written |
| `BUILD_IDEMPOTENCY_REGISTRY` | `<DAGS_DIR>/build_registry.json` | Build-time idempotency store |
| `RUNTIME_IDEMPOTENCY_REGISTRY` | `/tmp/dynamic_dag_runtime_registry.json` | Runtime node-execution idempotency store (used inside generated DAGs) |
| `KAFKA_CONN_ID` | `genesis_kafka_conn` | Airflow connection id for Kafka |
| `KAFKA_RUN_TOPIC` | `genesis.hub.run.events.v1` | Topic for run lifecycle events |
| `HOST` | `127.0.0.1` | Bind host |
| `PORT` | `8443` | Bind port |
| `LOG_LEVEL` | `INFO` | Logging level |

---

## API Endpoints

### GET /health

Returns service status and version.

```json
{ "status": "UP", "version": "8.2.0" }
```

### POST /build_dag

Generates (or idempotently returns) a DAG file.

**Response (201 Created):**

```json
{
  "status": "SUCCESS",
  "dag_id": "my_pipeline_dag",
  "file": "my_pipeline_dag_20250101_120000.py",
  "path": "/dags/my_pipeline_dag_20250101_120000.py",
  "idempotency_key": "<sha256>",
  "idempotent_reused": false
}
```

If the exact same payload was posted before and the file still exists on disk, `idempotent_reused` will be `true` and no new file is written.

---

## build_dag Payload Structure

```json
{
  "run_control_id": "my-pipeline",
  "triggerType": "O",
  "schedule": null,
  "nodes": [
    {
      "id": "node_a",
      "name": "Fetch User Data",
      "engine": "rest",
      "executor_order_id": 1,
      "executor_sequence_id": 1,
      "execution_mode": "sync",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },
    {
      "id": "node_b",
      "name": "Process Records",
      "engine": "rest",
      "executor_order_id": 2,
      "executor_sequence_id": 1,
      "execution_mode": "async_no_wait",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    }
  ]
}
```

### Top-level Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `run_control_id` | string | ✅ | Used to derive the `dag_id`. Must be non-empty. |
| `triggerType` | string | ❌ | One of `O`, `M`, `S` (or legacy `0`, `1`, `2`). See table below. |
| `schedule` | string | ❌ | Cron expression or `null` for manually-triggered DAGs. |
| `nodes` | array | ✅ | At least one node required. |

### Node Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `id` | string | ✅ | Unique identifier for this node within the DAG. Used as the XCom key prefix and payload lookup key. |
| `name` | string | ✅ | Human-readable label. Becomes the Airflow `task_id` shown in the UI. |
| `engine` | string | ✅ | Engine/integration type (informational — stored in generated DAG, used by external consumers). |
| `executor_order_id` | int (≥1) | ✅ | Which execution layer this node belongs to. Nodes with the same `executor_order_id` run in parallel within that layer. |
| `executor_sequence_id` | int (≥1) | ✅ | Position within a layer. Used to pair parent/child nodes across layers when multiple parallel chains exist. |
| `execution_mode` | string | ❌ | `sync` (default), `async_no_wait`, or `fire_and_forget`. |
| `branch_on_status` | bool | ❌ | If `true`, a `BranchPythonOperator` is injected after this node to route to success or failure paths. Requires `on_success_node_ids` or `on_failure_node_ids` to be populated. |
| `on_success_node_ids` | string[] | ❌ | Node IDs to run when this node succeeds (only when `branch_on_status=true`). |
| `on_failure_node_ids` | string[] | ❌ | Node IDs to run when this node fails (only when `branch_on_status=true`). |

### Trigger Type Values

| Value | Alias | Meaning |
|---|---|---|
| `O` | `0` | One-time / on-demand |
| `M` | `1` | Manual |
| `S` | `2` | Scheduled |

### Execution Mode Values

| Value | Behaviour |
|---|---|
| `sync` | POST to the URL and wait for a 2xx response. Task succeeds when the response arrives. |
| `async_no_wait` | POST to the URL to submit a job, then poll a status endpoint until a terminal state is reached. |
| `fire_and_forget` | POST to the URL; do not wait for or track any result. Task marks itself successful once submission is accepted. |

---

## runPhase Payload (DAG conf at trigger time)

When the generated DAG is triggered (via Airflow UI, CLI, or REST API), you pass a `conf` JSON object. Each node reads its own sub-object from `conf` by matching `node_id` as the key. The lookup order is:

1. `conf[node_id]` — direct key match
2. `conf[task_key]` — same as node_id in current implementation
3. Any value in `conf` where `value.node_runId == node_id`

### Per-node Task Payload Fields

```json
{
  "node_a": {
    "url": "https://my-service/api/run",
    "method": "POST",
    "headers": {
      "Authorization": "Bearer <token>",
      "Content-Type": "application/json"
    },
    "json": {
      "inputParam": "value",
      "runId": "abc-123"
    },
    "params": {
      "env": "prod"
    },
    "timeout": 300,
    "verify_ssl": false
  }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `url` | string | ✅ | The HTTP endpoint to call. A missing or empty `url` causes the task to fail immediately. |
| `method` | string | ❌ | HTTP method (default: `POST`). |
| `headers` | object | ❌ | Request headers. |
| `json` | object | ❌ | JSON request body. |
| `params` | object | ❌ | URL query parameters. |
| `timeout` | int | ❌ | Request timeout in seconds (default: `300`). |
| `verify_ssl` | bool | ❌ | SSL certificate verification (default: `false`). |

### Async Polling (status block)

For nodes with `execution_mode: "async_no_wait"`, include a `status` block inside the node's conf:

```json
{
  "node_b": {
    "url": "https://my-service/api/jobs/submit",
    "method": "POST",
    "json": { "workload": "batch-1" },
    "status": {
      "url": "https://my-service/api/jobs/{tracking_id}/status",
      "method": "GET",
      "response_id_key": "job_id",
      "response_status_key": "state",
      "poke_interval": 15,
      "timeout_seconds": 600,
      "success_statuses": ["SUCCESS", "COMPLETED"],
      "failure_statuses": ["FAILED", "ERROR"],
      "running_statuses": ["RUNNING", "PENDING"]
    }
  }
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `url` | string | ✅ | Status endpoint URL. Supports `{tracking_id}` and `{job_id}` template placeholders. |
| `method` | string | `GET` | HTTP method for status poll. |
| `response_id_key` | string | `job_id` | JSON path in the submit response to extract the tracking ID. |
| `response_status_key` | string | — | JSON path in the status response to extract the status string (e.g. `state`, `status`, `result.status`). |
| `poke_interval` | int | — | Seconds between status polls. |
| `timeout_seconds` | int | — | Maximum seconds to poll before failing. |
| `success_statuses` | string[] | `["SUCCESS","SUCCEEDED","COMPLETED","DONE","FINISHED"]` | Status values that mean the job is done successfully. |
| `failure_statuses` | string[] | `["FAILED","FAILURE","ERROR","ERRORED","CANCELLED","CANCELED","ABORTED"]` | Status values that mean the job failed. |
| `running_statuses` | string[] | `["RUNNING","IN_PROGRESS","PENDING","QUEUED","SUBMITTED","PROCESSING","STARTED"]` | Status values that mean keep polling. |

### Top-level conf Fields

These are top-level keys in the `conf` object (not inside any node sub-object):

| Field | Type | Description |
|---|---|---|
| `correlation_id` | string | Correlation ID propagated to Kafka events and idempotency keys. Falls back to `run_control_id`, then `dag_run.run_id`. |
| `resume` | bool | Set to `true` to re-trigger a previously failed run while reusing already-completed nodes (see Resume section). |
| `resume_from` | string | Node ID to resume from (informational — logged but not used for skipping directly; idempotency handles reuse). |
| `force_rerun` | bool | If `true`, bypass idempotency for all nodes and re-execute everything. |
| `force_rerun_nodes` | string or string[] | Comma-separated node IDs (or a list) to force re-execute even if they succeeded in a previous run. |

**Full example conf for a two-node sync run:**

```json
{
  "correlation_id": "corr-xyz-789",
  "node_a": {
    "url": "https://service-a/run",
    "method": "POST",
    "headers": { "Authorization": "Bearer token123" },
    "json": { "param1": "value1" }
  },
  "node_b": {
    "url": "https://service-b/process",
    "method": "POST",
    "json": { "param2": "value2" }
  }
}
```

---

## Resume / Partial Re-run

The service has built-in idempotency at the node level. When a DAG run partially fails, you can re-trigger the DAG without re-executing nodes that already succeeded.

**How it works:**

Each node computes an idempotency key from:
- `dag_id`
- `run_control_id`
- `correlation_id`
- `node_id`
- SHA-256 of the node's resolved payload

On execution, the result is stored in the runtime registry (`RUNTIME_IDEMPOTENCY_REGISTRY`). On subsequent runs, if an entry with `state=success` exists for the same key, the node is skipped and its XCom values are replayed from the registry.

**Resume conf:**

```json
{
  "correlation_id": "corr-xyz-789",
  "resume": true,
  "resume_from": "node_b",
  "node_a": {
    "url": "https://service-a/run",
    "method": "POST",
    "json": { "param1": "value1" }
  },
  "node_b": {
    "url": "https://service-b/process",
    "method": "POST",
    "json": { "param2": "value2" }
  }
}
```

- `node_a` completed in the previous run → its runtime registry entry is found → **skipped, result replayed**.
- `node_b` failed → no success entry → **executes fresh**.

**Force re-run specific nodes (even if they succeeded):**

```json
{
  "correlation_id": "corr-xyz-789",
  "force_rerun_nodes": ["node_a"],
  "node_a": { "url": "...", "json": {} },
  "node_b": { "url": "...", "json": {} }
}
```

Or force everything:

```json
{
  "correlation_id": "corr-xyz-789",
  "force_rerun": true,
  "node_a": { "url": "...", "json": {} },
  "node_b": { "url": "...", "json": {} }
}
```

> **Important:** Use the **same `correlation_id`** across original run and resume — it is part of the idempotency key. If `correlation_id` changes, all nodes will re-execute.

---

## Idempotency

There are two layers of idempotency:

| Layer | Registry file | Key | Protects against |
|---|---|---|---|
| **Build-time** | `build_registry.json` | SHA-256 of the full `build_dag` payload | Re-generating the same DAG file on duplicate API calls |
| **Runtime** | `dynamic_dag_runtime_registry.json` | SHA-256 of dag_id + run_control_id + correlation_id + node_id + payload hash | Re-executing a node that already succeeded |

---

## Generated DAG Lifecycle

Every generated DAG follows this task chain:

```
prepare_inputs
    └─► run_started_event  (Kafka: run.started.v1)
            └─► [node tasks in layers]
                    └─► finalize_results  (collects XCom state from all nodes)
                            └─► run_final_event  (Kafka: run.succeeded.v1 or run.failed.v1)
```

- `finalize_results` uses `TriggerRule.ALL_DONE` so it always runs even if some tasks were skipped by a branch.
- `run_final_event` also uses `TriggerRule.ALL_DONE`.

---

## Branching

Set `branch_on_status: true` on a node and provide either `on_success_node_ids` or `on_failure_node_ids` (or both). A `BranchPythonOperator` is automatically injected after the node's task.

Rules enforced at build time:
- `branch_on_status: true` requires at least one branch target list to be non-empty.
- Providing branch targets without `branch_on_status: true` is rejected.
- `fire_and_forget` nodes cannot use `branch_on_status`.

Example branch node in `/build_dag` payload:

```json
{
  "id": "node_check",
  "name": "Validation Check",
  "engine": "rest",
  "executor_order_id": 1,
  "executor_sequence_id": 1,
  "execution_mode": "sync",
  "branch_on_status": true,
  "on_success_node_ids": ["node_process"],
  "on_failure_node_ids": ["node_notify_failure"]
}
```

---

## Local Testing (without Airflow)

See [`test_local.py`](./test_local.py) in this repository for a standalone script that exercises the service and the generated DAG logic without requiring a running Airflow instance.
