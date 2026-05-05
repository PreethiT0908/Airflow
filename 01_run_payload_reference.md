# DAG Run Payload Reference — airflow_job1 v1

## Overview

This document covers:
1. Full Run Phase payload structure
2. Async node payload requirements
3. Resume payload structure derived from the run payload
4. Field-by-field explanation

---

## 1. Run Phase — Payload Structure

The run payload is sent as `conf` when triggering the DAG via the Airflow API or UI.

### Airflow Trigger API Shape

```json
{
  "logical_date": "2026-05-05T00:00:00Z",
  "conf": {
    "correlation_id": "your-trace-id-here",
    "resume": false,
    "resume_from": null,
    "force_rerun": false,
    "force_rerun_nodes": [],

    "<node_id>": { ...node config... },
    "<node_id>": { ...node config... }
  }
}
```

> **Critical Rule:** The top-level key inside `conf` **must exactly match** the `id` field used during the build phase. If you built with `"id": "task3"`, your conf key must be `"task3"`. No aliases, no sanitized names.

---

## 2. Node Config — By Execution Mode

### 2a. Sync Node (execution_mode: "sync")

```json
"task1": {
  "url": "http://your-service/endpoint",
  "method": "POST",
  "headers": {
    "Authorization": "Bearer <token>"
  },
  "json": {
    "any": "body"
  },
  "params": {},
  "timeout": 300,
  "verify_ssl": false
}
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `url` | ✅ Yes | — | HTTP endpoint to call |
| `method` | No | `POST` | HTTP verb |
| `headers` | No | `{}` | Request headers |
| `json` | No | `null` | JSON request body |
| `params` | No | `{}` | Query string params |
| `timeout` | No | `300` | Request timeout in seconds |
| `verify_ssl` | No | `false` | SSL certificate verification |

---

### 2b. Async Node (execution_mode: "async_no_wait")

The async node **submits** a job and then **polls** until it reaches a terminal state. The task occupies an Airflow worker slot for the entire polling duration.

```json
"task3": {
  "url": "https://orchestrator/jobs",
  "method": "POST",
  "headers": {
    "Authorization": "Basic <base64>"
  },
  "json": {
    "job_id": "cff3ca77-8f26-4cc6-8b49-1675853dba69",
    "payload": { ... }
  },
  "timeout": 300,
  "verify_ssl": false,
  "response_id_key": "runId",
  "status": {
    "url": "https://orchestrator/jobs/{tracking_id}/status",
    "method": "GET",
    "headers": {
      "Authorization": "Basic <base64>"
    },
    "response_status_key": "status",
    "poke_interval": 15,
    "timeout": 3600,
    "success_statuses": ["SUCCESS", "COMPLETED", "DONE"],
    "failure_statuses": ["FAILED", "ERROR", "ABORTED"],
    "running_statuses": ["RUNNING", "PENDING", "IN_PROGRESS", "QUEUED"]
  }
}
```

#### How tracking_id is resolved (in order of priority):
1. `status.tracking_id` in conf (explicit override)
2. `payload.tracking_id` in conf (explicit override)
3. `response_body[response_id_key]` — field named by `response_id_key` (default: `"job_id"`)
4. `response_body["run_id"]`
5. `response_body["runId"]`
6. `response_body["id"]`
7. `response_body["jobId"]`
8. `response_body["request_id"]`

#### Status URL template variables available:
```
{tracking_id}   — resolved job/run ID from submit response
{job_id}        — alias for tracking_id
{run_id}        — alias for tracking_id
{node_id}       — the node's id from build phase
{node_name}     — the node's name from build phase
{dag_id}        — Airflow DAG ID
{dag_run_id}    — Airflow DAG run ID
{run_control_id} — top-level run control ID
```

Example: `"url": "https://orchestrator/jobs/{tracking_id}"` becomes `https://orchestrator/jobs/abc-123` after substitution.

#### Async Node Fields:

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `url` | ✅ Yes | — | Submit URL |
| `response_id_key` | No | `job_id` | Field in submit response that holds the tracking ID |
| `status.url` | ✅ Yes | — | Polling URL (supports `{tracking_id}` template) |
| `status.method` | No | `GET` | Polling HTTP verb |
| `status.response_status_key` | No | `status` | Field in poll response holding the job status |
| `status.poke_interval` | No | `10` | Seconds between polls |
| `status.timeout` | No | `1800` | Max total polling time in seconds |
| `status.success_statuses` | No | see defaults | List of values meaning SUCCESS |
| `status.failure_statuses` | No | see defaults | List of values meaning FAILED |
| `status.running_statuses` | No | see defaults | List of values meaning still RUNNING |

---

### 2c. Fire and Forget Node (execution_mode: "fire_and_forget")

Same as sync — just `url`, `method`, `headers`, `json`. No `status` block needed. The task submits and immediately marks success without waiting for any outcome.

```json
"task5": {
  "url": "https://orchestrator/jobs",
  "method": "POST",
  "headers": { "Authorization": "Basic <base64>" },
  "json": { ... },
  "timeout": 300
}
```

> Fire-and-forget nodes are **excluded from merge guards**. They are never waited on.

---

## 3. Top-Level Control Fields

```json
"conf": {
  "correlation_id": "trace-abc-001",
  "resume": false,
  "resume_from": null,
  "force_rerun": false,
  "force_rerun_nodes": []
}
```

| Field | Type | Description |
|-------|------|-------------|
| `correlation_id` | string | Trace ID propagated to Kafka events and runtime registry |
| `resume` | bool | Set `true` to activate resume mode |
| `resume_from` | string | Node ID to resume from (must match build-phase `id`) |
| `force_rerun` | bool | Set `true` to bypass idempotency for ALL nodes |
| `force_rerun_nodes` | list[string] | Node IDs to force-rerun even when in resume mode |

---

## 4. Full Run Payload Example (DEMO_10 DAG)

Based on your actual `extracted_full_config.json` — trimmed for clarity:

```json
{
  "logical_date": "2026-05-05T00:00:00Z",
  "conf": {
    "correlation_id": "nifi-demo10-run-001",
    "resume": false,
    "resume_from": null,
    "force_rerun": false,
    "force_rerun_nodes": [],

    "task1": {
      "url": "http://10.5.16.153:8447/scaffold/file_to_db",
      "method": "POST",
      "headers": { "Authorization": "Bearer token" },
      "timeout": 10,
      "verify_ssl": false,
      "json": {
        "job_id": "test-file-to-db",
        "job_type": "file-to-db",
        "node_runId": "e4021416-cdb9-48aa-9907-aeaa28e35bc7",
        "run_control_id": "USER_FILETODB_TEST",
        "correlation_id": "nifi-file-to-db-test-001"
      }
    },

    "task2": {
      "url": "http://10.5.16.153:8447/scaffold/db-to-file",
      "method": "POST",
      "timeout": 10,
      "json": {
        "job_type": "db-to-file",
        "job_id": "test-db-to-file",
        "correlation_id": "73ff8c5-dc43-43ec-9a49-28e46ce1d5d3"
      }
    },

    "task3": {
      "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs",
      "method": "POST",
      "headers": { "Authorization": "Basic YWRtaW46YWRtaW4=" },
      "timeout": 300,
      "verify_ssl": false,
      "response_id_key": "runId",
      "json": {
        "job_id": "cff3ca77-8f26-4cc6-8b49-1675853dba69",
        "payload": { "runName": "edm_rc_reconciliation" }
      },
      "status": {
        "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs/{tracking_id}",
        "method": "GET",
        "headers": { "Authorization": "Basic YWRtaW46YWRtaW4=" },
        "response_status_key": "status",
        "poke_interval": 15,
        "timeout": 3600,
        "success_statuses": ["SUCCESS", "COMPLETED"],
        "failure_statuses": ["FAILED", "ERROR", "ABORTED"],
        "running_statuses": ["RUNNING", "PENDING", "IN_PROGRESS"]
      }
    },

    "task4": {
      "url": "http://10.5.16.153:8447/scaffold/api_to_db",
      "method": "POST",
      "timeout": 10,
      "json": { "job_type": "api-to-db", "job_id": "test-chatham-api-to-db" }
    },

    "task5": {
      "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs",
      "method": "POST",
      "headers": { "Authorization": "Basic YWRtaW46YWRtaW4=" },
      "timeout": 300,
      "json": { "job_type": "file-to-file", "job_id": "test-fire-forget" }
    },

    "task6": {
      "url": "http://10.5.16.153:8447/scaffold/db-to-file",
      "method": "POST",
      "timeout": 10,
      "json": { "job_type": "db-to-file", "job_id": "task6-job" }
    },

    "task7": {
      "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs",
      "method": "POST",
      "headers": { "Authorization": "Basic YWRtaW46YWRtaW4=" },
      "timeout": 300,
      "verify_ssl": false,
      "response_id_key": "runId",
      "json": {
        "job_id": "cff3ca77-8f26-4cc6-8b49-1675853dba69",
        "payload": { "runName": "edm_rc_reconciliation" }
      },
      "status": {
        "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs/{tracking_id}",
        "method": "GET",
        "headers": { "Authorization": "Basic YWRtaW46YWRtaW4=" },
        "response_status_key": "status",
        "poke_interval": 15,
        "timeout": 3600,
        "success_statuses": ["SUCCESS", "COMPLETED"],
        "failure_statuses": ["FAILED", "ERROR"],
        "running_statuses": ["RUNNING", "PENDING"]
      }
    },

    "task8": {
      "url": "http://10.5.16.153:8447/scaffold/api_to_db",
      "method": "POST",
      "timeout": 10,
      "json": { "job_type": "api-to-db", "job_id": "test-chatham-api-to-db-task8" }
    },

    "task9": {
      "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs",
      "method": "POST",
      "headers": { "Authorization": "Basic YWRtaW46YWRtaW4=" },
      "timeout": 300,
      "verify_ssl": false,
      "json": { "job_type": "file-to-file", "job_id": "test-file-to-file-task9" }
    },

    "task10": {
      "url": "http://10.5.16.153:8447/scaffold/file_to_db",
      "method": "POST",
      "headers": { "Authorization": "Bearer token" },
      "timeout": 10,
      "verify_ssl": false,
      "json": {
        "job_id": "test-file-to-db",
        "job_type": "file-to-db",
        "node_runId": "e4021416-cdb9-48aa-9907-aeaa28e35bc7",
        "run_control_id": "USER_FILETODB_TEST"
      }
    }
  }
}
```

---

## 5. Resume Payload — Design

### When to use Resume

Use resume when a previous DAG run **failed at a specific node** and you want to re-run from that node onward without re-executing already-successful nodes.

### Resume Rules

1. All nodes with `executor_order_id` **less than** `resume_from` node's order → **skipped** (XCom set to success, no HTTP call)
2. The `resume_from` node and all nodes **after** it → **execute normally**
3. `force_rerun_nodes` overrides rule 1 — those specific nodes re-execute even if before `resume_from`
4. Nodes that were **fire_and_forget** are still skipped during resume (they were already submitted)
5. If `resume_from` references an **unknown node ID** → DAG raises an error immediately (fixed in v1)

### Resume Payload Structure

```json
{
  "logical_date": "2026-05-05T00:00:00Z",
  "conf": {
    "correlation_id": "nifi-demo10-run-001",
    "resume": true,
    "resume_from": "task7",
    "force_rerun": false,
    "force_rerun_nodes": [],

    "task7": {
      "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs",
      "method": "POST",
      "headers": { "Authorization": "Basic YWRtaW46YWRtaW4=" },
      "timeout": 300,
      "response_id_key": "runId",
      "json": { ... },
      "status": { ... }
    },

    "task8": { ... },
    "task9": { ... },
    "task10": { ... }
  }
}
```

> **You only need to include the conf entries for nodes that will actually execute.** Nodes before `resume_from` are skipped — their conf is not used. However, including them does no harm.

### Resume Scenario: DEMO_10, task7 failed

Suppose order_ids are:
- task1 → order 1
- task2 → order 2
- task3, task4, task5 → order 3
- task6, task7, task8 → order 4 ← task7 failed here
- task9 → order 5
- task10 → order 6

`resume_from: "task7"` means order 4 is the resume boundary.

| Node | Order | Action |
|------|-------|--------|
| task1 | 1 | ⏭️ Skipped (order < 4) |
| task2 | 2 | ⏭️ Skipped (order < 4) |
| task3 | 3 | ⏭️ Skipped (order < 4) |
| task4 | 3 | ⏭️ Skipped (order < 4) |
| task5 | 3 | ⏭️ Skipped (order < 4) |
| task6 | 4 | ✅ Executes (order == 4) |
| task7 | 4 | ✅ Executes (same order as resume_from) |
| task8 | 4 | ✅ Executes (same order as resume_from) |
| task9 | 5 | ✅ Executes (order > 4) |
| task10 | 6 | ✅ Executes (order > 4) |

> **Note:** task6 and task8 also re-execute because they share the same `executor_order_id` (4) as task7. The resume boundary is **order-level**, not node-level.

### Resume with force_rerun_nodes

If you want to force a specific earlier node (e.g., task2) to re-run even though it's before the resume boundary:

```json
{
  "conf": {
    "resume": true,
    "resume_from": "task7",
    "force_rerun_nodes": ["task2"],

    "task2": { ... full task2 conf ... },
    "task6": { ... },
    "task7": { ... },
    "task8": { ... },
    "task9": { ... },
    "task10": { ... }
  }
}
```

### Full Resume Payload — DEMO_10 (task7 failed)

```json
{
  "logical_date": "2026-05-05T12:00:00Z",
  "conf": {
    "correlation_id": "nifi-demo10-run-001",
    "resume": true,
    "resume_from": "task7",
    "force_rerun": false,
    "force_rerun_nodes": [],

    "task6": {
      "url": "http://10.5.16.153:8447/scaffold/db-to-file",
      "method": "POST",
      "timeout": 10,
      "json": { "job_type": "db-to-file", "job_id": "task6-job" }
    },

    "task7": {
      "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs",
      "method": "POST",
      "headers": { "Authorization": "Basic YWRtaW46YWRtaW4=" },
      "timeout": 300,
      "verify_ssl": false,
      "response_id_key": "runId",
      "json": {
        "job_id": "cff3ca77-8f26-4cc6-8b49-1675853dba69",
        "payload": { "runName": "edm_rc_reconciliation" }
      },
      "status": {
        "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs/{tracking_id}",
        "method": "GET",
        "headers": { "Authorization": "Basic YWRtaW46YWRtaW4=" },
        "response_status_key": "status",
        "poke_interval": 15,
        "timeout": 3600,
        "success_statuses": ["SUCCESS", "COMPLETED"],
        "failure_statuses": ["FAILED", "ERROR"],
        "running_statuses": ["RUNNING", "PENDING"]
      }
    },

    "task8": {
      "url": "http://10.5.16.153:8447/scaffold/api_to_db",
      "method": "POST",
      "timeout": 10,
      "json": { "job_type": "api-to-db", "job_id": "test-chatham-api-to-db-task8" }
    },

    "task9": {
      "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs",
      "method": "POST",
      "headers": { "Authorization": "Basic YWRtaW46YWRtaW4=" },
      "timeout": 300,
      "json": { "job_type": "file-to-file", "job_id": "test-file-to-file-task9" }
    },

    "task10": {
      "url": "http://10.5.16.153:8447/scaffold/file_to_db",
      "method": "POST",
      "headers": { "Authorization": "Bearer token" },
      "timeout": 10,
      "json": { "job_id": "test-file-to-db", "job_type": "file-to-db" }
    }
  }
}
```
