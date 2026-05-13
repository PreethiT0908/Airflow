# Payload Reference — Dynamic DAG Service v1.3

Three payloads, three moments in time. This document covers all of them.

---

## Contents

1. [Build-time payload](#1-build-time-payload)  — sent to the FastAPI service to generate the DAG file
2. [Run-time payload](#2-run-time-payload)       — sent to Airflow when triggering a DAG run
3. [Resume payload](#3-resume-payload)           — sent when picking up a failed run mid-way
4. [Mistakes and the errors they produce](#4-mistakes-and-the-errors-they-produce)

---

## 1. Build-time payload

Sent as the POST body to `http://<fastapi-host>:8443/build_dag`.

This payload tells the service what the DAG should look like — how many tasks, in what order, and how each one executes. Nothing about actual URLs or request bodies goes here. That all comes later at run time.

### Field reference

```
{
  "run_control_id"    string  required
                      Becomes the DAG id after being lowercased and special characters
                      replaced with underscores, then "_dag" is appended.
                      "DEMO_10" becomes "demo_10_dag".
                      Should be unique per logical workflow.

  "triggerType"       string  optional, default "O"
                      How this DAG is expected to be kicked off.
                        "O" = on-demand — triggered externally via the Airflow API, no schedule
                        "S" = scheduled — needs a cron expression in the schedule field
                        "M" = manual

  "schedule"          string  optional
                      A cron expression such as "0 6 * * 1-5".
                      Set to null for on-demand DAGs.

  "nodes"             array   required  (also accepted as "executionSteps")
  or "executionSteps"
                      The list of tasks. At least one entry is required.
                      The two field names are interchangeable — use whichever your
                      system produces. If both are present they are merged.

  Each node inside the array:

    "id"              string  optional
                      Internal wiring reference only. Never appears in the Airflow UI
                      and never used as a key in the run-time conf.
                      If you leave it out the service assigns task1, task2, task3 ...
                      in order of (executor_order_id, executor_sequence_id).
                      Must be unique across all nodes if you do supply it.
                      Used in on_success_node_ids / on_failure_node_ids for branch wiring.

    "executor_build_id"  string  REQUIRED
                      The stable identifier for the service this task represents.
                      e.g. "EDM_Location_Inbound_NAS_TO_PVC"
                      This is the key you use in the run-time conf when you trigger the DAG.
                      conf["EDM_Location_Inbound_NAS_TO_PVC"] = { url, json, ... }
                      Must be provided. There is no default.

    "name"            string  required
                      The Airflow task label — what you see in the DAG graph view.
                      Does not need to be globally unique. If the same name appears in
                      multiple layers the service produces task_id as {name}_{order}_{seq}.

    "engine"          string  required
                      Currently informational. Pass "PYTHON".

    "executor_order_id"   integer  required, >= 1
                      The parallel stage this node belongs to.
                      All nodes with the same value run at the same time.
                      Nodes in stage N wait for all stage N-1 nodes to finish.

    "executor_sequence_id"  integer  required, >= 1
                      Position within a stage.
                      The pair (executor_order_id, executor_sequence_id) must be unique
                      across all nodes in the payload.

    "execution_mode"  string  required
                      "sync"           — call the URL and wait for the response
                      "async_no_wait"  — submit the job, poll for completion
                      "fire_and_forget"— submit and immediately succeed

    "branch_on_status"   boolean  required
                      Set true to add a routing step after this task.
                      Routes to on_success_node_ids or on_failure_node_ids based on outcome.
                      Requires at least one of those lists to be non-empty.

    "on_success_node_ids"  array  required (can be empty)
                      List of node ids to run when this node succeeds and branch_on_status=true.
                      Use the "id" values (task1, task2, ...) not executor_build_id.

    "on_failure_node_ids"  array  required (can be empty)
                      List of node ids to run when this node fails and branch_on_status=true.
}
```

### Minimal example — single task, on-demand

This is the smallest valid payload. One sync task, no branching, id auto-assigned.

```json
{
  "run_control_id": "remote_script_execution",
  "triggerType": "O",
  "schedule": null,
  "nodes": [
    {
      "executor_build_id": "murex-script-execution",
      "name": "murex-script-execution",
      "engine": "PYTHON",
      "executor_order_id": 1,
      "executor_sequence_id": 1,
      "execution_mode": "sync",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    }
  ]
}
```

The service assigns `id: "task1"` internally. At run time you pass the payload under `"murex-script-execution"` in conf, not under `"task1"`.

---

### Full example — multi-stage parallel pipeline (DEMO_10)

Ten tasks across six execution layers. Layers 3 and 4 have tasks running in parallel. Some service names repeat across layers — the service handles the deduplication.

```json
{
  "run_control_id": "DEMO_10",
  "triggerType": "O",
  "schedule": null,
  "nodes": [
    {
      "id": "task1",
      "executor_build_id": "KK_File_Transfer",
      "name": "KK_File_Transfer",
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
      "executor_build_id": "KK_File_Parsing",
      "name": "KK_File_Parsing",
      "engine": "PYTHON",
      "executor_order_id": 2,
      "executor_sequence_id": 1,
      "execution_mode": "sync",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },
    {
      "id": "task3",
      "executor_build_id": "KK_File_DB",
      "name": "KK_File_DB",
      "engine": "PYTHON",
      "executor_order_id": 3,
      "executor_sequence_id": 1,
      "execution_mode": "async_no_wait",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },
    {
      "id": "task4",
      "executor_build_id": "KK_DB_FILE_1",
      "name": "KK_DB_FILE_1",
      "engine": "PYTHON",
      "executor_order_id": 3,
      "executor_sequence_id": 2,
      "execution_mode": "sync",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },
    {
      "id": "task5",
      "executor_build_id": "KK_FEE_DB_FILE_2",
      "name": "KK_FEE_DB_FILE_2",
      "engine": "PYTHON",
      "executor_order_id": 3,
      "executor_sequence_id": 3,
      "execution_mode": "fire_and_forget",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },
    {
      "id": "task6",
      "executor_build_id": "KK_DB_FILE_3",
      "name": "KK_DB_FILE_3",
      "engine": "PYTHON",
      "executor_order_id": 4,
      "executor_sequence_id": 1,
      "execution_mode": "sync",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },
    {
      "id": "task7",
      "executor_build_id": "KK_File_DB_Stage4",
      "name": "KK_File_DB",
      "engine": "PYTHON",
      "executor_order_id": 4,
      "executor_sequence_id": 2,
      "execution_mode": "async_no_wait",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },
    {
      "id": "task8",
      "executor_build_id": "KK_DB_FILE_1_Stage4",
      "name": "KK_DB_FILE_1",
      "engine": "PYTHON",
      "executor_order_id": 4,
      "executor_sequence_id": 3,
      "execution_mode": "sync",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },
    {
      "id": "task9",
      "executor_build_id": "KK_FEE_DB_FILE_2_Stage5",
      "name": "KK_FEE_DB_FILE_2",
      "engine": "PYTHON",
      "executor_order_id": 5,
      "executor_sequence_id": 1,
      "execution_mode": "fire_and_forget",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },
    {
      "id": "task10",
      "executor_build_id": "KK_DB_FILE_3_Final",
      "name": "KK_DB_FILE_3",
      "engine": "PYTHON",
      "executor_order_id": 6,
      "executor_sequence_id": 1,
      "execution_mode": "sync",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    }
  ]
}
```

Note that `KK_File_DB` and `KK_DB_FILE_1` each appear in two layers. The `name` field repeats — that is fine. The `executor_build_id` values are unique across the whole payload, which matters because each one is a distinct key in the run-time conf.

---

### Branching example

task1 checks whether a file exists. If it finds one it runs task2 (process the file). If not it runs task3 (send a missing-file alert).

```json
{
  "run_control_id": "branch_demo",
  "triggerType": "O",
  "schedule": null,
  "nodes": [
    {
      "id": "task1",
      "executor_build_id": "Check_File_Exists",
      "name": "Check_File_Exists",
      "engine": "PYTHON",
      "executor_order_id": 1,
      "executor_sequence_id": 1,
      "execution_mode": "sync",
      "branch_on_status": true,
      "on_success_node_ids": ["task2"],
      "on_failure_node_ids": ["task3"]
    },
    {
      "id": "task2",
      "executor_build_id": "Process_File",
      "name": "Process_File",
      "engine": "PYTHON",
      "executor_order_id": 2,
      "executor_sequence_id": 1,
      "execution_mode": "sync",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },
    {
      "id": "task3",
      "executor_build_id": "Send_Missing_File_Alert",
      "name": "Send_Missing_File_Alert",
      "engine": "PYTHON",
      "executor_order_id": 2,
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

### Build-time validation errors

These come back as HTTP 422 before any file is written.

| What went wrong | Error message |
|---|---|
| `executor_build_id` missing from a node | `executor_build_id: Field required` |
| Two nodes with the same `id` | `Duplicate node ids: ['task3']` |
| Two nodes with the same `(executor_order_id, executor_sequence_id)` | `Duplicate executor ordering: [(order=2, seq=1)]` |
| `branch_on_status: true` but both target lists are empty | `Node task2 has branch_on_status=true but no branch targets` |
| Branch target lists populated but `branch_on_status: false` | `Node task2 has branch targets but branch_on_status=false` |
| A branch target node id that does not exist in the payload | `Node task1 references unknown node ids: ['task99']` |
| Same node id in both success and failure lists | `Node task1 has nodes in both success and failure targets: ['task2']` |
| `fire_and_forget` with `branch_on_status: true` | `Node task2 uses fire_and_forget and cannot branch on final status` |
| A circular dependency in the node graph | `Cycle detected in DAG graph: task1 -> task3 -> task1` |
| No nodes provided | `At least one node must be provided in 'nodes' or 'executionSteps'` |

---

## 2. Run-time payload

Passed as the `conf` object when you trigger a DAG run through the Airflow API.

The build payload described the wiring. This payload provides the actual data — URLs, headers, request bodies — that each task uses when it executes. Nothing here changes the DAG structure.

### Structure

```
{
  "conf": {

    "correlation_id"      string  recommended
                          A trace ID you choose. Shows up in all Kafka events so
                          downstream systems can link activity to this specific run.
                          Can be any string — use a UUID or your own run reference.

    "resume"              boolean  required
                          false for a normal first run.
                          true for a resume (see section 3).

    "resume_from"         string  required (null for normal run)
                          Node id to resume from. null for a first run.

    "force_rerun"         boolean  optional, default false
                          true re-runs every node from scratch ignoring any resume logic.

    "force_rerun_nodes"   array  optional
                          List of specific node ids to force-run during a resume,
                          even if they would normally be skipped.

    "<executor_build_id>"  object  one per task that will actually execute
                           Keyed by the executor_build_id from the build payload.
                           e.g. "EDM_Location_Inbound_NAS_TO_PVC": { ... }

      Each task entry:

        "url"             string  required
                          Full HTTP URL to call.

        "method"          string  optional, default "POST"
                          HTTP method.

        "headers"         object  optional
                          HTTP headers. Put Authorization tokens here.

        "json"            object  optional
                          The request body sent as JSON. This is your actual payload —
                          whatever the downstream service needs to do its job.

        "params"          object  optional
                          URL query parameters.

        "timeout"         integer  optional, default 300
                          Request timeout in seconds.

        "verify_ssl"      boolean  optional, default false
                          Set true to enforce TLS certificate validation.

        "status"          object  REQUIRED only for async_no_wait nodes
                          Polling configuration for async jobs. See below.

          "url"                   string  required
                                  Status check endpoint.
                                  Supports placeholders: {tracking_id}, {run_id},
                                  {node_id}, {dag_run_id}
                                  e.g. "http://service/jobs/{tracking_id}/status"

          "method"                string  optional, default "GET"

          "headers"               object  optional
                                  Can be different from the submit headers.

          "response_id_key"       string  optional, default "job_id"
                                  JSON path in the submit response where the tracking ID lives.
                                  e.g. "runId" or "data.jobId" for nested responses.

          "response_status_key"   string  optional, default "status"
                                  JSON path in the status response where the job state lives.
                                  e.g. "status" or "job.currentState" for nested responses.

          "poke_interval"         integer  optional, default 10
                                  Seconds between status polls.

          "timeout"               integer  optional, default 1800
                                  Total seconds to keep polling before giving up.

          "success_statuses"      array  optional
                                  Status values that mean the job finished successfully.
                                  Default: ["SUCCESS","SUCCEEDED","COMPLETED","DONE","FINISHED"]

          "failure_statuses"      array  optional
                                  Status values that mean the job failed.
                                  Default: ["FAILED","ERROR","CANCELLED","ABORTED"]

          "running_statuses"      array  optional
                                  Status values that mean keep polling.
                                  Default: ["RUNNING","IN_PROGRESS","PENDING","QUEUED"]
  }
}
```

---

### Example — single fire_and_forget task

Matches the `remote_script_execution` DAG from the minimal build example. The entire object you want the downstream service to receive goes inside `"json"`.

```json
{
  "conf": {
    "correlation_id": "trace-20260511-001",
    "resume": false,
    "resume_from": null,
    "force_rerun": false,
    "force_rerun_nodes": [],
    "murex-script-execution": {
      "url": "http://script-runner.internal/api/v1/execute",
      "method": "POST",
      "headers": {
        "Authorization": "Bearer eyJhbGciOiJSUzI1NiJ9...",
        "Content-Type": "application/json"
      },
      "json": {
        "target": {
          "smb": {
            "share": "GENESIS",
            "domain": "fcpd",
            "server": "vfq00010",
            "uncPath": "\\\\vfq00010\\Genesis\\GENESIS_DEV\\TCS\\airflow\\dynamic-dags",
            "password": "[[12574_D_WIN_FP:dv_genesis_linux-svc]]",
            "username": "dv_genesis_linux-svc"
          },
          "type": "smb",
          "localPath": ""
        },
        "source": {
          "type": "local",
          "localPath": "/opt/airflow/dags/"
        },
        "filePattern": "*.py",
        "deleteSource": false,
        "renamingRules": null
      },
      "timeout": 120,
      "verify_ssl": false
    }
  }
}
```

The `url`, `method`, `headers`, and `timeout` fields are wrapper instructions for the DAG task — they are not sent to the downstream service. Everything inside `"json"` is what the downstream service actually receives.

---

### Example — sync task

```json
{
  "conf": {
    "correlation_id": "trace-20260511-002",
    "resume": false,
    "resume_from": null,
    "force_rerun": false,
    "force_rerun_nodes": [],
    "KK_File_Transfer": {
      "url": "http://file-service.internal/api/transfer",
      "method": "POST",
      "headers": {
        "Authorization": "Bearer <token>"
      },
      "json": {
        "source_path": "/nas/input/batch_001/",
        "destination_path": "/pvc/staging/",
        "overwrite": true
      },
      "timeout": 300,
      "verify_ssl": false
    }
  }
}
```

---

### Example — async_no_wait task with polling

The submit call fires the job. The status block tells the task how to poll until it finishes.

```json
{
  "conf": {
    "correlation_id": "trace-20260511-003",
    "resume": false,
    "resume_from": null,
    "force_rerun": false,
    "force_rerun_nodes": [],
    "KK_File_DB": {
      "url": "http://db-loader.internal/api/v2/load",
      "method": "POST",
      "headers": {
        "Authorization": "Bearer <token>",
        "Content-Type": "application/json"
      },
      "json": {
        "table": "raw_file_data",
        "source_path": "/pvc/staging/batch_001/",
        "truncate_first": false
      },
      "timeout": 60,
      "verify_ssl": false,
      "status": {
        "url": "http://db-loader.internal/api/v2/load/{tracking_id}",
        "method": "GET",
        "headers": {
          "Authorization": "Bearer <token>"
        },
        "response_id_key": "loadJobId",
        "response_status_key": "job.currentStatus",
        "poke_interval": 15,
        "timeout": 3600,
        "success_statuses": ["COMPLETED", "DONE"],
        "failure_statuses": ["FAILED", "ERROR", "CANCELLED"],
        "running_statuses": ["RUNNING", "PENDING", "QUEUED"]
      }
    }
  }
}
```

In this example:
- Submit goes to `/api/v2/load` and the response contains `{"loadJobId": "abc-123", ...}`
- `response_id_key: "loadJobId"` tells the task to extract `abc-123` as the tracking ID
- The status URL becomes `http://db-loader.internal/api/v2/load/abc-123`
- `response_status_key: "job.currentStatus"` handles a nested response like `{"job": {"currentStatus": "RUNNING"}}`
- Polls every 15 seconds for up to 1 hour

---

### Example — full DEMO_10 multi-task conf

One entry per task keyed by executor_build_id. The fire_and_forget tasks (task5, task9) still need conf entries because they make HTTP calls — they just do not need status blocks.

```json
{
  "conf": {
    "correlation_id": "demo-10-run-001",
    "resume": false,
    "resume_from": null,
    "force_rerun": false,
    "force_rerun_nodes": [],
    "KK_File_Transfer": {
      "url": "http://file-service/transfer",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "source": "/nas/input/", "destination": "/pvc/staging/" }
    },
    "KK_File_Parsing": {
      "url": "http://parser-service/parse",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "input_path": "/pvc/staging/", "format": "CSV" }
    },
    "KK_File_DB": {
      "url": "http://db-loader/load",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "table": "raw_file_data", "source_path": "/pvc/staging/" },
      "timeout": 60,
      "status": {
        "url": "http://db-loader/load/{tracking_id}",
        "response_id_key": "loadJobId",
        "response_status_key": "status",
        "poke_interval": 10,
        "timeout": 1800,
        "success_statuses": ["DONE"],
        "failure_statuses": ["FAILED"],
        "running_statuses": ["RUNNING", "PENDING"]
      }
    },
    "KK_DB_FILE_1": {
      "url": "http://db-service/record",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "record_type": "FILE_1", "batch_id": "batch-20260511" }
    },
    "KK_FEE_DB_FILE_2": {
      "url": "http://notification-service/notify",
      "method": "POST",
      "json": { "event": "BATCH_STARTED", "batch_id": "batch-20260511" }
    },
    "KK_DB_FILE_3": {
      "url": "http://db-service/reconcile",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "batch_id": "batch-20260511", "step": "POST_LOAD" }
    },
    "KK_File_DB_Stage4": {
      "url": "http://db-loader/load",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "table": "processed_data", "source_path": "/pvc/processed/" },
      "status": {
        "url": "http://db-loader/load/{tracking_id}",
        "response_id_key": "loadJobId",
        "response_status_key": "status",
        "poke_interval": 10,
        "timeout": 1800,
        "success_statuses": ["DONE"],
        "failure_statuses": ["FAILED"],
        "running_statuses": ["RUNNING", "PENDING"]
      }
    },
    "KK_DB_FILE_1_Stage4": {
      "url": "http://db-service/record",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "record_type": "FILE_1_STAGE4", "batch_id": "batch-20260511" }
    },
    "KK_FEE_DB_FILE_2_Stage5": {
      "url": "http://notification-service/notify",
      "method": "POST",
      "json": { "event": "BATCH_STAGE5", "batch_id": "batch-20260511" }
    },
    "KK_DB_FILE_3_Final": {
      "url": "http://db-service/finalize",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "batch_id": "batch-20260511", "final_step": true }
    }
  }
}
```

---

## 3. Resume payload

When a DAG run fails mid-way, trigger a **new** DAG run (not a retry of the failed tasks) with a resume conf. The DAG skips everything that ran before the failure point and picks up from where you tell it to.

### How resume works

The skip decision is based on `executor_order_id`. If you set `resume_from: "task5"` and task5 has `executor_order_id: 3`, then every node with `executor_order_id < 3` is skipped. Skipped nodes push a synthetic `success` XCom and return immediately — no HTTP calls made.

This means resume is order-level, not individual-task-level. All nodes in the same layer as `resume_from` will run, including any that had succeeded in the original run.

### What you need to provide

- `"resume": true` — must be explicitly set
- `"resume_from": "<node_id>"` — the internal id (`task3`, not `KK_File_DB`)
- `"correlation_id"` — use the **same value** as the original failed run
- Conf entries for every executor_build_id that will actually execute — tasks being skipped do not need conf entries

### Example — resuming DEMO_10 from task3

Tasks 1 and 2 succeeded in the original run. Task 3 failed. Resume from task3.

```json
{
  "conf": {
    "correlation_id": "demo-10-run-001",
    "resume": true,
    "resume_from": "task3",
    "force_rerun": false,
    "force_rerun_nodes": [],

    "KK_File_DB": {
      "url": "http://db-loader/load",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "table": "raw_file_data", "source_path": "/pvc/staging/" },
      "status": {
        "url": "http://db-loader/load/{tracking_id}",
        "response_id_key": "loadJobId",
        "response_status_key": "status",
        "poke_interval": 10,
        "timeout": 1800,
        "success_statuses": ["DONE"],
        "failure_statuses": ["FAILED"],
        "running_statuses": ["RUNNING", "PENDING"]
      }
    },
    "KK_DB_FILE_1": {
      "url": "http://db-service/record",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "record_type": "FILE_1", "batch_id": "batch-20260511" }
    },
    "KK_FEE_DB_FILE_2": {
      "url": "http://notification-service/notify",
      "method": "POST",
      "json": { "event": "BATCH_STARTED", "batch_id": "batch-20260511" }
    },
    "KK_DB_FILE_3": {
      "url": "http://db-service/reconcile",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "batch_id": "batch-20260511", "step": "POST_LOAD" }
    },
    "KK_File_DB_Stage4": {
      "url": "http://db-loader/load",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "table": "processed_data", "source_path": "/pvc/processed/" },
      "status": {
        "url": "http://db-loader/load/{tracking_id}",
        "response_id_key": "loadJobId",
        "response_status_key": "status",
        "poke_interval": 10,
        "timeout": 1800,
        "success_statuses": ["DONE"],
        "failure_statuses": ["FAILED"],
        "running_statuses": ["RUNNING", "PENDING"]
      }
    },
    "KK_DB_FILE_1_Stage4": {
      "url": "http://db-service/record",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "record_type": "FILE_1_STAGE4", "batch_id": "batch-20260511" }
    },
    "KK_FEE_DB_FILE_2_Stage5": {
      "url": "http://notification-service/notify",
      "method": "POST",
      "json": { "event": "BATCH_STAGE5", "batch_id": "batch-20260511" }
    },
    "KK_DB_FILE_3_Final": {
      "url": "http://db-service/finalize",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "batch_id": "batch-20260511", "final_step": true }
    }
  }
}
```

`KK_File_Transfer` and `KK_File_Parsing` are omitted entirely — those tasks will skip themselves without needing conf entries.

---

### Example — resume with a specific node forced to re-run

Task 2 needs to run again even though it succeeded in the original run (maybe you fixed a data issue upstream). Add it to `force_rerun_nodes` and include its conf entry.

```json
{
  "conf": {
    "correlation_id": "demo-10-run-001",
    "resume": true,
    "resume_from": "task3",
    "force_rerun": false,
    "force_rerun_nodes": ["task2"],

    "KK_File_Parsing": {
      "url": "http://parser-service/parse",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "input_path": "/pvc/staging/", "format": "CSV" }
    },
    "KK_File_DB": {
      "url": "http://db-loader/load",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "json": { "table": "raw_file_data", "source_path": "/pvc/staging/" },
      "status": {
        "url": "http://db-loader/load/{tracking_id}",
        "response_id_key": "loadJobId",
        "response_status_key": "status",
        "poke_interval": 10,
        "timeout": 1800,
        "success_statuses": ["DONE"],
        "failure_statuses": ["FAILED"],
        "running_statuses": ["RUNNING", "PENDING"]
      }
    }
  }
}
```

task1 (`KK_File_Transfer`) still skips. task2 (`KK_File_Parsing`) runs because it is in `force_rerun_nodes`. task3 onwards run normally.

---

### Full force-rerun from scratch

Ignore all resume logic and run everything again. No `resume_from`, no skipping.

```json
{
  "conf": {
    "correlation_id": "demo-10-run-002",
    "resume": false,
    "resume_from": null,
    "force_rerun": true,
    "force_rerun_nodes": [],
    "KK_File_Transfer": { "url": "...", "json": { ... } },
    "KK_File_Parsing": { "url": "...", "json": { ... } }
  }
}
```

---

## 4. Mistakes and the errors they produce

### Passing the request body at the top level of conf

This is the most common mistake. The task looks for its payload under `conf[executor_build_id]` and finds nothing.

```
What you sent:
{
  "conf": {
    "correlation_id": "trace-001",
    "target": { ... },         <-- wrong — these are at the top level
    "source": { ... },
    "filePattern": "*.py"
  }
}

Error in Airflow task log:
  Node task1 (murex-script-execution) — executor_build_id: murex-script-execution
  Missing or empty 'url' in conf.
  Expected conf key: 'murex-script-execution'

Fix:
{
  "conf": {
    "correlation_id": "trace-001",
    "murex-script-execution": {        <-- wrap it under executor_build_id
      "url": "http://...",
      "method": "POST",
      "json": {
        "target": { ... },             <-- your payload goes inside "json"
        "source": { ... },
        "filePattern": "*.py"
      }
    }
  }
}
```

---

### Using the wrong key (task1 instead of executor_build_id)

```
What you sent:
{
  "conf": {
    "task1": { "url": "http://...", "json": { ... } }   <-- task1 is internal, not the conf key
  }
}

The task falls back to conf["task1"] and may find something, but this is not reliable.
The correct key is the executor_build_id, e.g. "EDM_Location_Inbound_NAS_TO_PVC".

Use:
{
  "conf": {
    "EDM_Location_Inbound_NAS_TO_PVC": { "url": "http://...", "json": { ... } }
  }
}
```

---

### async_no_wait node missing the status block

```
Error: Node task3 (KK_File_DB) is async_no_wait but has no 'status' block in conf.

Fix: add a "status" object inside the task's conf entry:
{
  "conf": {
    "KK_File_DB": {
      "url": "http://db-loader/load",
      "json": { ... },
      "status": {                             <-- required for async_no_wait
        "url": "http://db-loader/load/{tracking_id}",
        "response_status_key": "status",
        "poke_interval": 10,
        "timeout": 1800,
        "success_statuses": ["DONE"],
        "failure_statuses": ["FAILED"],
        "running_statuses": ["RUNNING"]
      }
    }
  }
}
```

---

### Status URL without a tracking_id placeholder

```
"status": {
  "url": "http://db-loader/load/status",     <-- no {tracking_id} in the URL
  ...
}
```

The task will extract the tracking ID from the submit response but then throw it away because there is nowhere to put it in the URL. Every status poll will hit the same fixed URL. The job may never reach a terminal state or you may be polling the wrong job entirely.

Fix:

```
"url": "http://db-loader/load/{tracking_id}/status"
```

---

### Resuming to a node id that does not exist

```
Error: [RESUME] resume_from='KK_File_DB' is not a valid node ID.
       Valid: ['task1', 'task2', 'task3', ...]

resume_from takes a node id (task3), not an executor_build_id (KK_File_DB).
Use the id values from the build payload.
```

---

### Using a different correlation_id on a resume

This does not cause an error but breaks event traceability. The Kafka events from the resume run will have a different `correlation_id` than the original run's events. Anything listening downstream will see two separate runs instead of one with a retry. Always carry the same `correlation_id` from the original run into the resume conf.

---

### executor_build_id missing from a node in the build payload

```
HTTP 422 from /build_dag:
{
  "detail": [
    {
      "type": "missing",
      "loc": ["body", "nodes", 0, "executor_build_id"],
      "msg": "Field required"
    }
  ]
}
```

Every node must have `executor_build_id`. It cannot be omitted or null.
