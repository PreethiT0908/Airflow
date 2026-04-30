# Dynamic Airflow DAG Service

A FastAPI microservice that dynamically generates, registers, and writes Apache Airflow DAG files from a JSON payload describing a workflow graph. Each generated DAG is production-ready with Kafka event emission, runtime idempotency, branching, async polling, and deterministic resume support.

---

## File Comparison & What Was Missing

### `dynamic_dag_service_baremini.py` (v8.2.0) — Missing features

| # | Missing Feature | Impact |
|---|----------------|--------|
| 1 | **No `executor_order_id` passed to `execute_node`** | Resume-skip logic was present only as a log line; it never actually skipped nodes — every node always re-ran regardless of `resume_from`. |
| 2 | **No `TaskGroup` per stage** | All tasks rendered as a flat list in the Airflow UI. The `_1_` version wraps each `executor_order_id` layer in its own `TaskGroup`, giving a collapsed/expandable stage view. |
| 3 | **No `doc_md` on generated tasks** | Tasks had no inline documentation in the Airflow UI; operators showed no human-readable metadata. |
| 4 | **No `NODE_ORDER_MAP` in generated code** | Without the order map, the generated DAG could not implement deterministic resume-skip even if the service passed `executor_order_id`. |
| 5 | **Merge-node trigger rule was `ALL_SUCCESS`** | Baremini used `NONE_FAILED_MIN_ONE_SUCCESS` (correct for branch-skipped merge points), but the _1_ version only used `ALL_SUCCESS` for all nodes. The merged file applies `NONE_FAILED_MIN_ONE_SUCCESS` specifically to merge nodes, fixing propagation after branch skips. |
| 6 | **No `_check_async_status` helper** | The `_1_` version provides a detailed async status checker with rich error categorisation (401/403/404/5xx/missing-field). Baremini omitted this entirely. |

### `dynamic_dag_service__1_.py` (v9.0.0) — Missing features

| # | Missing Feature | Impact |
|---|----------------|--------|
| 1 | **No `TASK_ID_TO_NODE_ID` reverse map in generated code** | The reverse map (task_id → node_id) was present in baremini. It is useful inside `choose_branch` and finalize lookups when only the Airflow task_id is known. |
| 2 | **`_indent_block` helper absent** | The _1_ file used raw string concatenation for indenting generated code blocks. The baremini `_indent_block` utility is cleaner, avoids off-by-one indentation errors, and is included in the merged version. |
| 3 | **`resume` log line dropped** | When `resume=true` but `resume_from` is not set, baremini logs the fact; the _1_ version silently skipped it. |
| 4 | **`finalize_results` error message less detailed** | Baremini's error string included `expected_skipped` count; _1_ omitted it. |

---

## Merged File: `dynamic_dag_service_merged.py` (v10.0.0)

All features from both files are unified. Key decisions made during the merge:

- **Task ID naming** — uses `make_task_id(node_id, node_name)` from `_1_`: generates `<name>__<id>` so the Airflow graph shows the human-readable node name on every task node.
- **Trigger rule for merge nodes** — uses `NONE_FAILED_MIN_ONE_SUCCESS` from baremini (correct for branch convergence).
- **TaskGroup per layer** — from `_1_`, wraps each `executor_order_id` stage.
- **Deterministic resume skip** — from `_1_`, fully wired with `executor_order_id` and `NODE_ORDER_MAP`.
- **`doc_md`** — from `_1_`, attached to every generated `PythonOperator` and `BranchPythonOperator`.
- **`TASK_ID_TO_NODE_ID`** — from baremini, embedded in generated code.
- **`_indent_block`** — from baremini, used to produce clean indented task definitions.
- **`_check_async_status`** — from `_1_`, included for detailed async status introspection.

---

## Architecture Overview

```
POST /build_dag
      │
      ▼
BuildDagPayload (Pydantic validation)
      │
      ▼
canonicalize_build_payload()  ──→  compute_sha256()
      │                                   │
      │              idempotency hit? ◄───┘
      │                   │
      │                   └──► return cached entry
      │
      ▼
build_layers()          — groups nodes by executor_order_id
build_stage_dependencies()  — wires inter-layer edges using task_id_map
generate_dag_code()     — emits a self-contained .py DAG file
      │
      ▼
validate_generated_python()  — compile() check before disk write
atomic_write_text()          — temp-file + rename (crash-safe)
store_registry_entry()       — persists build idempotency record
      │
      ▼
Airflow picks up the .py file from AIRFLOW_DAGS_DIR
```

---

## Node Execution Modes

| Mode | Behaviour |
|------|-----------|
| `sync` | HTTP call → wait for response → mark success/failure |
| `async_no_wait` | HTTP submit → poll status URL until terminal state or timeout |
| `fire_and_forget` | HTTP submit → mark success immediately, no status tracking |

---

## Airflow UI Node Labels

Every task node in the Airflow graph view displays the **node name** (not the internal node ID). This is achieved via `make_task_id(node_id, node_name)` which produces task IDs in the form `<sanitized_name>__<sanitized_id>`. The `doc_md` field on each operator also shows the name, engine, and execution mode as inline documentation in the task detail panel.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AIRFLOW_DAGS_DIR` | `./dag_configs` | Directory where generated DAG files are written |
| `KAFKA_CONN_ID` | `genesis_kafka_conn` | Airflow connection ID for Kafka |
| `KAFKA_RUN_TOPIC` | `genesis.hub.run.events.v1` | Kafka topic for run lifecycle events |
| `BUILD_IDEMPOTENCY_REGISTRY` | `<DAGS_DIR>/build_registry.json` | Path to the build-time idempotency registry |
| `RUNTIME_IDEMPOTENCY_REGISTRY` | `/tmp/dynamic_dag_runtime_registry.json` | Path to the runtime node execution registry |
| `LOG_LEVEL` | `INFO` | Service log level |
| `HOST` | `127.0.0.1` | Bind host for uvicorn |
| `PORT` | `8443` | Bind port for uvicorn |

---

## API Endpoints

### `GET /health`
Returns service status and version.

```json
{ "status": "UP", "version": "10.0.0" }
```

### `POST /build_dag`
Generates (or returns cached) an Airflow DAG file.

**Request body:**
```json
{
  "run_control_id": "my-workflow-001",
  "triggerType": "M",
  "schedule": null,
  "nodes": [
    {
      "id": "node_1",
      "name": "Fetch Customer Data",
      "engine": "http",
      "executor_order_id": 1,
      "executor_sequence_id": 1,
      "execution_mode": "sync"
    },
    {
      "id": "node_2",
      "name": "Transform Records",
      "engine": "http",
      "executor_order_id": 2,
      "executor_sequence_id": 1,
      "execution_mode": "async_no_wait"
    }
  ]
}
```

**Response (201 Created):**
```json
{
  "status": "SUCCESS",
  "dag_id": "my_workflow_001_dag",
  "file": "my_workflow_001_dag_20260430_120000.py",
  "path": "/dag_configs/my_workflow_001_dag_20260430_120000.py",
  "idempotency_key": "abc123...",
  "idempotent_reused": false
}
```

---

## DAG Run Configuration (conf)

When triggering a DAG run, pass a JSON `conf` to control behaviour:

```json
{
  "node_1": { "url": "https://api.example.com/fetch", "method": "GET" },
  "node_2": {
    "url": "https://api.example.com/transform",
    "method": "POST",
    "json": { "source": "customers" },
    "status": {
      "url": "https://api.example.com/status/{tracking_id}",
      "response_status_key": "state",
      "poke_interval": 15,
      "timeout": 900
    }
  },
  "correlation_id": "req-xyz-789",
  "resume": false,
  "resume_from": "node_2",
  "force_rerun": false,
  "force_rerun_nodes": []
}
```

### Resume / Partial Re-run

Set `resume: true` and `resume_from: "<node_id>"` to fast-forward all nodes whose `executor_order_id` is strictly less than the resume node's order. Those nodes are marked as `resume_skipped` without making any HTTP calls.

### Force Re-run

Set `force_rerun: true` to bypass the runtime idempotency cache for all nodes, or pass `force_rerun_nodes: ["node_1", "node_2"]` to selectively re-run specific nodes.

---

## Branching

Set `branch_on_status: true` on a node and provide `on_success_node_ids` / `on_failure_node_ids` to route subsequent execution based on the node's outcome. A `BranchPythonOperator` is automatically inserted after the branching node. Nodes that are potential skip targets receive `NONE_FAILED_MIN_ONE_SUCCESS` as their trigger rule so that branch-skipped upstream tasks do not block them.

---

## Build-time Idempotency

The service computes a SHA-256 fingerprint of the canonicalized payload. If an identical payload is submitted again, the existing DAG file path is returned immediately without regenerating the file.

## Runtime Idempotency

Each node execution is keyed by `(dag_id, run_control_id, correlation_id, node_id, payload_hash)`. If a node completed successfully in a prior attempt and `force_rerun` is not set, the cached result is replayed via XCom without hitting the external HTTP endpoint again.

---

## Dependencies

```
fastapi
uvicorn
pydantic>=2
portalocker
requests
apache-airflow
apache-airflow-providers-apache-kafka
apache-airflow-providers-standard
```

---

## Running the Service

```bash
pip install fastapi uvicorn pydantic portalocker requests
export AIRFLOW_DAGS_DIR=/opt/airflow/dags
python dynamic_dag_service_merged.py
```

The service starts on `http://127.0.0.1:8443` by default.
