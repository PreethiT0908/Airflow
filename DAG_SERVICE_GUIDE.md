# DAG Service — Complete Reference Guide

## Overview

`dag_service.py` is a FastAPI microservice that dynamically generates Airflow 3.x DAG Python files.

**Two-phase design:**
- **BUILD phase** — POST `/build_dag` → validates payload → writes a `.py` DAG file to the Airflow DAGs folder
- **RUN phase** — Airflow triggers the DAG → each task reads its payload from `dag_run.conf` and makes an HTTP call

---

## Build Payload

### Top-level fields

| Field | Required | Notes |
|---|---|---|
| `run_control_id` | Yes | Becomes the DAG ID (e.g. `DEMO_9`) |
| `triggerType` | No | `"0"` = on-demand, `"1"` = scheduled |
| `schedule` | No | Cron string (required if triggerType=`"1"`), else `null` |
| `nodes` | Yes* | Array of node objects |
| `executionSteps` | Yes* | Alias for `nodes` — both accepted, merged internally |

*At least one node required across `nodes` + `executionSteps`.

### Node fields

| Field | Required | Notes |
|---|---|---|
| `id` | No | Auto-assigned `task1`, `task2`... if omitted |
| `name` | Yes | Human-readable label shown in Airflow UI |
| `executor_build_id` | **Yes** | Run-time conf lookup key (see Run Payload) |
| `engine` | Yes | `"PYTHON"` |
| `executor_order_id` | Yes | Layer number — layer 1 runs first, layer 2 next, etc. |
| `executor_sequence_id` | Yes | Position within a layer (for parallel tasks in same layer) |
| `execution_mode` | No | `sync` / `async_no_wait` / `fire_and_forget` (default: `sync`) |
| `branch_on_status` | No | `true` to route on success/failure (default: `false`) |
| `on_success_node_ids` | No | Required when `branch_on_status: true` |
| `on_failure_node_ids` | No | Required when `branch_on_status: true` |

### Sample build payload

```json
{
  "run_control_id": "DEMO_9",
  "triggerType": "0",
  "schedule": null,
  "nodes": [
    {
      "id": "task1",
      "name": "KK_File_Transfer",
      "executor_build_id": "KK_File_Transfer",
      "engine": "PYTHON",
      "executor_order_id": 1,
      "executor_sequence_id": 1,
      "execution_mode": "sync",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },
    {
      "id": "task2",
      "name": "KK_File_Parsing",
      "executor_build_id": "KK_File_Parsing",
      "engine": "PYTHON",
      "executor_order_id": 2,
      "executor_sequence_id": 1,
      "execution_mode": "sync",
      "branch_on_status": true,
      "on_success_node_ids": ["task3"],
      "on_failure_node_ids": ["task4"]
    },
    {
      "id": "task3",
      "name": "KK_DB_FILE_1",
      "executor_build_id": "KK_DB_FILE_1",
      "engine": "PYTHON",
      "executor_order_id": 3,
      "executor_sequence_id": 1,
      "execution_mode": "sync",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },
    {
      "id": "task4",
      "name": "KK_FEE_DB_FILE_2",
      "executor_build_id": "KK_FEE_DB_FILE_2",
      "engine": "PYTHON",
      "executor_order_id": 3,
      "executor_sequence_id": 2,
      "execution_mode": "fire_and_forget",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    }
  ]
}
```

---

## Execution Modes

| Mode | Behavior | Fails DAG? |
|---|---|---|
| `sync` | Waits for HTTP response. Fails if non-2xx. | Yes |
| `async_no_wait` | Fires HTTP request, does not wait. Always marks success. | No |
| `fire_and_forget` | Fires in a background thread. Task completes immediately. | Never |

---

## Branching

Set `branch_on_status: true` on a node to route downstream tasks based on HTTP response:

- **Success path** → `on_success_node_ids`
- **Failure path** → `on_failure_node_ids`

### Rules
- Both `on_success_node_ids` and `on_failure_node_ids` must be non-empty when `branch_on_status: true`
- `fire_and_forget` nodes cannot use `branch_on_status`
- A node cannot appear in both success and failure lists of the same parent

### Example — branch on task2

```
task1 → task2 (branch) ─── success ──→ task3
                        └── failure ──→ task4
```

Build payload fragment:
```json
{
  "id": "task2",
  "branch_on_status": true,
  "on_success_node_ids": ["task3"],
  "on_failure_node_ids": ["task4"]
}
```

---

## Merging (Merge Guard)

When multiple upstream tasks point to the same downstream task, a **merge guard** is applied automatically.

The merge node waits for all upstream tasks using `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS`.  
At runtime it checks each upstream node's XCom state — if any upstream is `"failed"`, the merge node raises an exception.

### Example — merge after branch

```
task2 (branch) ─── success ──→ task3 ─┐
               └── failure ──→ task4 ─┴──→ task5 (merge)
```

Build payload: `task5` appears in both `task3.on_success_node_ids` and `task4.on_success_node_ids`.

---

## Orphan Detection

A node is an **orphan** if it:
- Has no downstream business children (nothing depends on it), AND
- Is not already in the final execution layer, AND
- Is not a branch leaf node

Orphan nodes are automatically wired to `finalize_results` so the DAG always completes cleanly.

### Example — orphan scenario

```
task1 → task2 (sync)
task1 → task3 (async_no_wait)   ← orphan (no children, not in final layer)
task1 → task4 (fire_and_forget) ← orphan
```

`task3` and `task4` are detected as orphans and wired to terminal flow automatically.

---

## Run Payload

Sent to Airflow's trigger API. The `conf` object maps `executor_build_id` values to HTTP call parameters.

### Conf key lookup order (resolve_payload)

1. `conf["<executor_build_id>"]` — primary (e.g. `"KK_File_Transfer"`)
2. `conf["<node_id>"]` — e.g. `"task1"` if `id` was set explicitly
3. `conf["<task_key>"]` — legacy fallback
4. `conf["taskN"]` — **positional fallback** — `"task1"` / `"Task1"` maps to global sequence 1
5. Scan entries for `node_runId` match

> `logical_date` is ignored — do not include it.

### Sample run payload

```json
{
  "conf": {
    "KK_File_Transfer": {
      "url": "http://10.5.16.153:8080/execute",
      "method": "POST",
      "timeout": 30,
      "verify_ssl": false,
      "headers": {
        "Authorization": "Bearer <token>"
      },
      "json": {
        "job_id": "kk-file-transfer",
        "run_id": "{{RunId}}"
      }
    },
    "KK_File_Parsing": {
      "url": "http://10.5.16.153:8080/execute",
      "method": "POST",
      "timeout": 30,
      "verify_ssl": false,
      "headers": {
        "Authorization": "Bearer <token>"
      },
      "json": {
        "job_id": "kk-file-parsing",
        "run_id": "{{RunId}}"
      }
    },
    "KK_DB_FILE_1": {
      "url": "http://10.5.16.153:8080/execute",
      "method": "POST",
      "timeout": 30,
      "verify_ssl": false,
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "job_id": "kk-db-file-1" }
    },
    "KK_FEE_DB_FILE_2": {
      "url": "http://10.5.16.153:8080/execute",
      "method": "POST",
      "timeout": 30,
      "verify_ssl": false,
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "job_id": "kk-fee-db-file-2" }
    }
  }
}
```

You can also use positional keys (`task1`, `task2`, ...) instead of `executor_build_id` names:

```json
{
  "conf": {
    "task1": { "url": "http://...", "method": "POST", "json": {} },
    "task2": { "url": "http://...", "method": "POST", "json": {} }
  }
}
```

### Conf entry fields

| Field | Required | Notes |
|---|---|---|
| `url` | **Yes** | Missing = task fails immediately |
| `method` | No | Default `POST` |
| `timeout` | No | Seconds to wait (sync mode) |
| `verify_ssl` | No | Set `false` for internal services |
| `headers` | No | Auth tokens, content-type, etc. |
| `json` | No | Request body sent as JSON |

---

## Rerun / Resume Payloads

### 1. Full rerun — re-run everything

Just trigger the DAG again with the normal run payload. No special fields.

### 2. Resume from a specific node

Skips all nodes with `executor_order_id` lower than `resume_from`. Resumes from that node onward.

```json
{
  "conf": {
    "resume": true,
    "resume_from": "task3",
    "task1": { "url": "..." },
    "task2": { "url": "..." },
    "task3": { "url": "..." }
  }
}
```

### 3. Resume but force specific nodes to re-run

```json
{
  "conf": {
    "resume": true,
    "resume_from": "task1",
    "force_rerun_nodes": ["task2", "task3"],
    "task1": { "url": "..." },
    "task2": { "url": "..." },
    "task3": { "url": "..." }
  }
}
```

Only `task2` and `task3` execute — all others are skipped.

### 4. Force rerun all nodes

```json
{
  "conf": {
    "force_rerun": true,
    "task1": { "url": "..." },
    "task2": { "url": "..." }
  }
}
```

### Resume/rerun field reference

| Field | Type | Purpose |
|---|---|---|
| `resume` | bool | Enable resume mode |
| `resume_from` | string | Node ID to resume from |
| `force_rerun` | bool | Force all nodes to run even in resume mode |
| `force_rerun_nodes` | list or comma-string | Force specific nodes (e.g. `["task2","task3"]`) |

---

## DAG Structure (generated)

Every generated DAG has this fixed skeleton:

```
prepare_inputs
      ↓
run_started_event  (Kafka: run.started.v1)
      ↓
  [task1] → [task2] → [task3] ...   (your business nodes)
                              ↓
                      finalize_results
                              ↓
                       run_final_event  (Kafka: run.succeeded.v1 / run.failed.v1)
                              ↓
                          completion
```

---

## Trigger Types

| Value | Code | Meaning |
|---|---|---|
| `"0"` | `O` | On-demand (triggered via Kafka event or manual trigger) |
| `"1"` | `S` | Scheduled (requires cron in `schedule` field) |
| `"2"` | `F` | TBD |
| `"3"` | `D` | TBD |
| `"4"` | `R` | TBD |

---

## Build-time Idempotency

The service maintains a SHA-256 registry of processed build payloads.  
Identical payloads do not overwrite an existing DAG file — they return a `304 Not Modified` equivalent response.  
To force a rebuild, change any field in the payload (e.g. increment a version field).

---

## Known Fixes Applied (v1.3)

| Fix | Description |
|---|---|
| `get_current_context` import | Moved to `airflow.sdk` (Airflow 3.x deprecation) |
| `{label}` template escape | Fixed `ValueError: Invalid format specifier` in generated DAG |
| Case-insensitive conf lookup | `resolve_payload` normalises all conf keys to lowercase |
| Positional `taskN` fallback | Run conf `"task1"` maps to global sequence 1 even if `executor_build_id` differs |
| Orphan detection | Mid-graph terminal nodes wired to `finalize_results` automatically |
| Expanded trigger types | `O`, `S`, `F`, `D`, `R` (previously only `O`, `M`, `S`) |
