# DAG Resume & Rerun — Production Guide

**Service:** `dag_service_latest.py` (airflow_job1 v1.3)  
**Target:** Airflow 3.0.6 / 3.1.8  

---

## Overview

The resume system lets you re-trigger a failed DAG run and skip all tasks that already succeeded, starting execution from a specific node. This avoids re-running expensive or side-effectful tasks that completed successfully.

Resume is **manually driven** — you decide where to resume by including control fields in the trigger conf. There is no automatic detection of the last failure point; you specify it explicitly.

---

## How Resume Works Internally

Every generated DAG task checks the following at the **start of execution**, before making any HTTP call:

```
conf["resume"] == true?
    └── _should_force_rerun(conf, node_id)?
            ├── YES → skip the resume check, run the task normally
            └── NO  → compare executor_order_id against resume_from node's order
                          ├── task order < resume_from order → SKIP (push fake success XCom)
                          └── task order >= resume_from order → RUN normally
```

Skipped tasks push `"success"` to their XCom state so `finalize_results` does not count them as failures.

---

## The NODE_ORDER_MAP

Every generated DAG has a `NODE_ORDER_MAP` baked in at build time:

```python
# Example for dag_generator_test_8_dag
NODE_ORDER_MAP = {
    'ICE_FILE_TRANSFER_1': 1,
    'Ice_Price_Scaffold_PVC_To_DB_Clone_3': 2,
}
```

The `resume_from` value in your conf **must match a key in this map**. If it does not, the DAG raises an `AirflowException` immediately listing all valid node IDs.

To find valid node IDs for any DAG, check the `NODE_ORDER_MAP` constant near the top of the generated `.py` file.

---

## Trigger API

All resume variants use Airflow's DAG trigger endpoint:

```
POST /api/v2/dags/{dag_id}/dagRuns
Content-Type: application/json

{
  "conf": { ... }
}
```

> Do NOT include `logical_date`. Airflow 3.x removed it from the trigger body.

---

## Resume Modes

### Mode 1 — Resume from a specific node

Skip all nodes with `executor_order_id` lower than `resume_from`. Run that node and everything after it.

**Use when:** Task N failed. Tasks 1 through N-1 succeeded and do not need to re-run.

```json
{
  "conf": {
    "resume": true,
    "resume_from": "Ice_Price_Scaffold_PVC_To_DB_Clone_3",

    "ICE_FILE_TRANSFER_1": {
      "url": "http://10.5.16.153:8080/execute",
      "method": "POST",
      "verify_ssl": false,
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "job_id": "ice-file-transfer" }
    },
    "Ice_Price_Scaffold_PVC_To_DB_Clone_3": {
      "url": "http://10.5.16.153:8080/execute",
      "method": "POST",
      "verify_ssl": false,
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "job_id": "ice-price-scaffold" }
    }
  }
}
```

**Result:**

| Node | Action |
|---|---|
| `ICE_FILE_TRANSFER_1` (order=1) | Skipped — order 1 < resume_from order 2 |
| `Ice_Price_Scaffold_PVC_To_DB_Clone_3` (order=2) | Runs |

---

### Mode 2 — Resume but force specific nodes to re-run

Skip most completed tasks, but force one or more specific nodes to re-run even though they are before `resume_from`.

**Use when:** A dependency changed (e.g. a file was updated) so one earlier task must re-run, but you don't want to restart everything from the beginning.

```json
{
  "conf": {
    "resume": true,
    "resume_from": "Ice_Price_Scaffold_PVC_To_DB_Clone_3",
    "force_rerun_nodes": ["ICE_FILE_TRANSFER_1"],

    "ICE_FILE_TRANSFER_1": {
      "url": "http://10.5.16.153:8080/execute",
      "method": "POST",
      "verify_ssl": false,
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "job_id": "ice-file-transfer" }
    },
    "Ice_Price_Scaffold_PVC_To_DB_Clone_3": {
      "url": "http://10.5.16.153:8080/execute",
      "method": "POST",
      "verify_ssl": false,
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "job_id": "ice-price-scaffold" }
    }
  }
}
```

`force_rerun_nodes` also accepts a comma-separated string:

```json
"force_rerun_nodes": "ICE_FILE_TRANSFER_1,Ice_Price_Scaffold_PVC_To_DB_Clone_3"
```

**Result:**

| Node | Action |
|---|---|
| `ICE_FILE_TRANSFER_1` | Runs (forced via `force_rerun_nodes`) |
| `Ice_Price_Scaffold_PVC_To_DB_Clone_3` | Runs (at or after `resume_from`) |

---

### Mode 3 — Force full rerun (all nodes)

Re-run every node regardless of previous state. Equivalent to a fresh trigger.

**Use when:** You want to re-execute the entire pipeline without removing the resume flag logic from your payload.

```json
{
  "conf": {
    "force_rerun": true,

    "ICE_FILE_TRANSFER_1": {
      "url": "http://10.5.16.153:8080/execute",
      "method": "POST",
      "verify_ssl": false,
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "job_id": "ice-file-transfer" }
    },
    "Ice_Price_Scaffold_PVC_To_DB_Clone_3": {
      "url": "http://10.5.16.153:8080/execute",
      "method": "POST",
      "verify_ssl": false,
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "job_id": "ice-price-scaffold" }
    }
  }
}
```

**Result:** All nodes run. `resume` and `resume_from` are ignored.

---

### Mode 4 — Normal run (no resume)

Omit all resume fields. Every node executes from the beginning.

```json
{
  "conf": {
    "ICE_FILE_TRANSFER_1": {
      "url": "http://10.5.16.153:8080/execute",
      "method": "POST",
      "verify_ssl": false,
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "job_id": "ice-file-transfer" }
    },
    "Ice_Price_Scaffold_PVC_To_DB_Clone_3": {
      "url": "http://10.5.16.153:8080/execute",
      "method": "POST",
      "verify_ssl": false,
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "job_id": "ice-price-scaffold" }
    }
  }
}
```

---

## Conf Field Reference

| Field | Type | Required | Description |
|---|---|---|---|
| `resume` | boolean | No | Set `true` to enable resume mode |
| `resume_from` | string | Required if `resume: true` | Node ID to resume from. Must be a key in `NODE_ORDER_MAP` |
| `force_rerun_nodes` | list or comma-string | No | Node IDs to force-run even when they are before `resume_from` |
| `force_rerun` | boolean | No | Set `true` to force all nodes to run (overrides `resume`) |

---

## Decision Guide — Which Mode to Use

```
DAG failed. What do I do?
│
├── Which node failed?
│       └── Check Airflow UI → task logs → find the red task
│
├── Did all tasks BEFORE the failed task succeed?
│       ├── YES → Use Mode 1: resume from the failed node
│       └── NO  → Use Mode 2: resume_from = failed node, force_rerun_nodes = the earlier task that also failed
│
├── Do I want to re-run everything from scratch?
│       └── YES → Use Mode 3 (force_rerun: true) or Mode 4 (no flags)
│
└── Did nothing fail, I just want to re-trigger?
        └── Use Mode 4: normal run, no resume flags
```

---

## Step-by-Step: Resume After a Failure

**Scenario:** `dag_generator_test_8_dag` failed at `Ice_Price_Scaffold_PVC_To_DB_Clone_3`. `ICE_FILE_TRANSFER_1` completed successfully.

**Step 1** — Confirm which node failed  
Open Airflow UI → click the DAG run → look for the red task in the graph view.

**Step 2** — Find the node ID  
Look at the task name in the graph. The node ID is the same as the Airflow task_id in the generated DAG (check `TASK_ID_MAP` at the top of the `.py` file if unsure).

**Step 3** — Trigger the resume

```bash
curl -X POST http://<airflow-host>/api/v2/dags/dag_generator_test_8_dag/dagRuns \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic <base64(user:pass)>" \
  -d '{
    "conf": {
      "resume": true,
      "resume_from": "Ice_Price_Scaffold_PVC_To_DB_Clone_3",
      "ICE_FILE_TRANSFER_1": {
        "url": "http://10.5.16.153:8080/execute",
        "method": "POST",
        "verify_ssl": false,
        "headers": { "Authorization": "Bearer <token>" },
        "json": { "job_id": "ice-file-transfer" }
      },
      "Ice_Price_Scaffold_PVC_To_DB_Clone_3": {
        "url": "http://10.5.16.153:8080/execute",
        "method": "POST",
        "verify_ssl": false,
        "headers": { "Authorization": "Bearer <token>" },
        "json": { "job_id": "ice-price-scaffold" }
      }
    }
  }'
```

**Step 4** — Verify in Airflow UI  
The new DAG run will show `ICE_FILE_TRANSFER_1` completing instantly (skipped) and `Ice_Price_Scaffold_PVC_To_DB_Clone_3` executing normally.

---

## Payload Normalization (method/headers inside json)

The service accepts `method`, `headers`, `verify_ssl`, `timeout`, and `params` either at the **top level** of a conf entry or **nested inside the `json` body**. Both are equivalent:

**Top-level (preferred):**
```json
{
  "ICE_FILE_TRANSFER_1": {
    "url": "http://...",
    "method": "POST",
    "verify_ssl": false,
    "headers": { "Authorization": "Bearer <token>" },
    "json": { "job_id": "ice-file-transfer" }
  }
}
```

**Nested inside json (also accepted):**
```json
{
  "ICE_FILE_TRANSFER_1": {
    "url": "http://...",
    "json": {
      "method": "POST",
      "verify_ssl": false,
      "headers": { "Authorization": "Bearer <token>" },
      "job_id": "ice-file-transfer"
    }
  }
}
```

The DAG automatically hoists `method`, `headers`, `verify_ssl`, `timeout`, `params` out of `json` to the top level before making the HTTP call.

---

## Conf Lookup Order (resolve_payload)

Each task searches for its payload in this order. The first match wins:

| Priority | Key searched | Example |
|---|---|---|
| 1 | `executor_build_id` | `"ICE_FILE_TRANSFER_1"` |
| 2 | `node_id` | `"task1"` (internal wiring ID) |
| 3 | `task_key` | legacy alias |
| 4 | Positional `taskN` | `"task1"` maps to global sequence 1, `"Task1"` also accepted |
| 5 | `node_runId` scan | scans all entries for a matching `node_runId` field |

---

## What Is NOT Supported (Current Version)

| Feature | Status |
|---|---|
| Automatic resume from last failure point (re-trigger same payload, no resume flags needed) | **Not implemented** |
| Per-node retry count override at run time | Not implemented |
| Resume across different `run_control_id` | Not supported — `NODE_ORDER_MAP` is baked per DAG |

---

## Limitations and Gotchas

**1. `resume_from` must be a node ID, not a task name**  
Use the value from `NODE_ORDER_MAP`, not the display name shown in the Airflow UI. For `dag_generator_test_8_dag`, valid values are `ICE_FILE_TRANSFER_1` and `Ice_Price_Scaffold_PVC_To_DB_Clone_3`.

**2. All task conf entries must still be present in the payload**  
Even skipped tasks need their conf entry. The skip decision happens at runtime inside the task function — Airflow still instantiates and queues the task operator. An empty or missing conf entry for a skipped task is fine (it returns before checking the URL), but it is best practice to include all entries.

**3. Each resume trigger creates a new DAG run**  
Airflow does not modify or continue an old run. The failed run stays as failed in the UI. The resume creates a brand new `dagRun` with a new `run_id`.

**4. Branch nodes and merge guards still apply**  
If your DAG has branching (`branch_on_status: true`), the resume skipped tasks still push `"success"` to the branch XCom key, so the branch operator sees success and routes to the success path. If that is not the behaviour you want (e.g. you want the failure path to run), use `force_rerun_nodes` on the branching task.

---

## Quick Reference

```
Resume from failed node:
  "resume": true, "resume_from": "<failed_node_id>"

Resume but re-run one earlier node too:
  "resume": true, "resume_from": "<failed_node_id>", "force_rerun_nodes": ["<earlier_node_id>"]

Re-run everything:
  "force_rerun": true   OR   omit all resume fields

Find valid node IDs:
  Check NODE_ORDER_MAP in the generated DAG .py file
  OR check the Airflow graph view task IDs
```
