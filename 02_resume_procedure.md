# Resume Procedure — Dynamic DAG Service (airflow_job1 v1)

## What is Resume?

Resume allows you to **re-trigger a failed DAG run** and pick up execution from the point of failure — without re-running nodes that already completed successfully.

It works at the **executor_order_id level**: all nodes whose order is strictly less than the `resume_from` node's order are fast-forwarded (XCom markers set to success, no HTTP call made).

---

## Step 1 — Identify the Failed DAG Run

Go to the Airflow UI and find the failed DAG run.

```
Airflow UI → DAGs → <your_dag_id> → Runs → click the failed run
```

Note:
- The **DAG ID** (e.g., `demo_10_dag`)
- The **failed task ID** (e.g., `KK_File_DB__task7`)
- The **node ID** used in the build phase (e.g., `task7`) — this is what you put in `resume_from`

> The node ID is the `id` field you sent during the `/build_dag` call, NOT the Airflow task display name.

---

## Step 2 — Check the Runtime Registry (Optional but Recommended)

The runtime registry at `/tmp/dynamic_dag_runtime_registry.json` (or your configured `RUNTIME_IDEMPOTENCY_REGISTRY` path) records the state of each node.

Look for entries with `"state": "failed"` to confirm which node failed:

```json
{
  "<idempotency_key>": {
    "state": "failed",
    "node_id": "task7",
    "node_name": "KK_File_DB",
    "dag_id": "demo_10_dag",
    "dag_run_id": "manual__2026-05-05T10:00:00+00:00",
    "correlation_id": "nifi-demo10-run-001",
    "error": "HTTP 503: Service Unavailable",
    "finished_at": "2026-05-05T10:03:45Z"
  }
}
```

---

## Step 3 — Determine the Resume Boundary

Find the `executor_order_id` of the failed node from your original build payload.

Example for DEMO_10:

| Node | executor_order_id | Execution Mode |
|------|-------------------|----------------|
| task1 | 1 | sync |
| task2 | 2 | sync |
| task3 | 3 | async_no_wait |
| task4 | 3 | sync |
| task5 | 3 | fire_and_forget |
| task6 | 4 | sync |
| task7 | 4 | async_no_wait |
| task8 | 4 | sync |
| task9 | 5 | fire_and_forget |
| task10 | 6 | sync |

If **task7 failed** → `resume_from = "task7"` → resume boundary = order 4.

**All nodes with order < 4 are skipped. All nodes with order >= 4 re-execute.**

---

## Step 4 — Build the Resume Payload

### Minimum Required Fields

```json
{
  "logical_date": "<ISO timestamp>",
  "conf": {
    "correlation_id": "<same correlation_id as original run>",
    "resume": true,
    "resume_from": "<node_id of failed node>",
    "force_rerun": false,
    "force_rerun_nodes": [],

    "<node_id_at_boundary_and_after>": { ...conf... }
  }
}
```

### Rules for Building the Conf

1. **`correlation_id`** — use the **same value** as the original run. This ensures idempotency keys match and avoids double execution.
2. **`resume_from`** — must be a valid node `id` from the build payload. If it doesn't exist in the DAG, the DAG will raise an error immediately.
3. **Include conf for ALL nodes at and after the boundary** — nodes before the boundary are skipped but it does not hurt to include them.
4. **Do NOT change the `url`, `method`, or `json` of nodes** unless the original failure was due to bad config. Idempotency is keyed on the payload hash — changing the payload creates a new idempotency entry.

---

## Step 5 — Trigger the Resume via Airflow API

### Using Airflow REST API (v1 — Airflow 3.x)

```bash
curl -X POST \
  "http://<airflow-host>/api/v1/dags/demo_10_dag/dagRuns" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic <base64-user:pass>" \
  -d '{
    "logical_date": "2026-05-05T12:00:00Z",
    "conf": {
      "correlation_id": "nifi-demo10-run-001",
      "resume": true,
      "resume_from": "task7",
      "force_rerun": false,
      "force_rerun_nodes": [],
      "task6": { ... },
      "task7": { ... },
      "task8": { ... },
      "task9": { ... },
      "task10": { ... }
    }
  }'
```

### Using Airflow UI

1. Go to `DAGs` → `demo_10_dag`
2. Click **Trigger DAG** (play button)
3. Select **"Trigger DAG w/ config"**
4. Paste the full `conf` JSON into the config box
5. Click **Trigger**

> Note: The `logical_date` in the UI trigger will be auto-set. The `conf` is the only thing you paste.

---

## Step 6 — Monitor the Resumed Run

In the Airflow UI, go to the new DAG run. You should see:

- Skipped nodes (before boundary) → shown as **light green / skipped** with XCom `_task_state = success`
- Resumed nodes (at and after boundary) → executing normally
- `finalize_results` task will collect all results at the end

---

## Step 7 — Verify the Runtime Registry Post-Resume

After the resumed run completes, check the registry:

```bash
cat /tmp/dynamic_dag_runtime_registry.json | python3 -m json.tool
```

Look for all entries with `"state": "success"` for the nodes that re-ran.

---

## Special Scenarios

### Scenario A: Resume within the Same Layer (Same executor_order_id)

> "task7 failed but task6 and task8 in the same order already succeeded. Do they re-run?"

**Yes, they re-run.** Resume works at order-level granularity. If you specify `resume_from: "task7"` and task7 is at order 4, all nodes at order 4 (including task6 and task8) will re-execute.

However, **idempotency** protects task6 and task8: if they already have a `success` entry in the runtime registry for the same correlation_id and payload, they will be skipped by the idempotency check inside `execute_node`. No actual HTTP call is made again.

This means **you get the right behaviour automatically** — only truly incomplete or failed nodes make HTTP calls.

To override this and force-rerun task6 even though it succeeded:
```json
"force_rerun_nodes": ["task6"]
```

---

### Scenario B: Resume from the First Layer

If task1 (order 1) failed:
```json
{
  "resume": true,
  "resume_from": "task1"
}
```
No nodes are before order 1, so all nodes execute. This is equivalent to a fresh run with idempotency protection.

---

### Scenario C: Full Force Rerun (Ignore All Idempotency)

```json
{
  "resume": false,
  "force_rerun": true
}
```
Every node re-executes regardless of previous success entries. Use with caution — this will re-submit all HTTP calls including fire-and-forget nodes.

---

### Scenario D: resume_from References Unknown Node

If `resume_from` is set to a node ID that was not part of the build payload (e.g., a typo), the DAG will **immediately raise an AirflowException** on the first task that checks it:

```
AirflowException: resume_from node 'task99' not found in NODE_ORDER_MAP. 
Valid node IDs: ['task1', 'task2', ..., 'task10']
```

The DAG fails fast — no tasks execute.

---

## Quick Reference Card

```
RESUME CHECKLIST
────────────────────────────────────────────
[ ] Identify failed node_id from build payload
[ ] Note its executor_order_id
[ ] Use same correlation_id as original run
[ ] Set resume: true
[ ] Set resume_from: "<failed_node_id>"
[ ] Include conf for all nodes at/after boundary
[ ] Optional: use force_rerun_nodes for exceptions
[ ] Trigger via Airflow API or UI
[ ] Monitor the new run in the UI
[ ] Verify runtime registry after completion
────────────────────────────────────────────
```
