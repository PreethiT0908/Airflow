# Run Payload Analysis & Test Guide — DEMO_10 DAG

## DAG Overview (from Build Payload)

The DEMO_10 DAG has 10 nodes across 6 layers:

```
Layer 1 (order=1):   task1  ── sync
Layer 2 (order=2):   task2  ── sync
Layer 3 (order=3):   task3  ── async_no_wait   (parallel)
                     task4  ── sync             (parallel)
                     task5  ── fire_and_forget  (parallel)
Layer 4 (order=4):   task6  ── sync             (parallel)
                     task7  ── async_no_wait    (parallel)
                     task8  ── sync             (parallel)
Layer 5 (order=5):   task9  ── fire_and_forget
Layer 6 (order=6):   task10 ── sync
```

---

## Section 1 — Payload Analysis: What Works and What Doesn't

### Quick Status Table

| Task | Name | Mode | URL Present | Status Block | Will It Work? |
|------|------|------|------------|--------------|---------------|
| task1 | KK_File_Transfer | sync | ✅ | Not needed | ✅ **YES** |
| task2 | KK_File_Parsing | sync | ✅ | Not needed | ✅ **YES** |
| task3 | KK_File_DB | async_no_wait | ✅ | ❌ MISSING | ❌ **WILL FAIL** |
| task4 | KK_DB_FILE_1 | sync | ✅ | Not needed | ✅ **YES** |
| task5 | KK_FEE_DB_FILE_2 | fire_and_forget | ✅ | Not needed | ✅ **YES** |
| task6 | KK_DB_FILE_3 | sync | ✅ | Not needed | ✅ **YES** |
| task7 | KK_File_DB | async_no_wait | ✅ | ❌ MISSING | ❌ **WILL FAIL** |
| task8 | KK_DB_FILE_1 | sync | ✅ | Not needed | ✅ **YES** |
| task9 | KK_FEE_DB_FILE_2 | fire_and_forget | ✅ | Not needed | ✅ **YES** |
| task10 | KK_DB_FILE_3 | sync | ✅ | Not needed | ✅ **YES** |

**Result: 8 of 10 tasks will work. task3 and task7 will fail.**

---

## Section 2 — What Is Wrong With task3 and task7

Both task3 and task7 are `async_no_wait` nodes. The DAG service requires a top-level `"status"` block inside the task conf to know how to poll for job completion.

### What your current payload has (WRONG)

**task3 — no status block at all:**
```json
"task3": {
  "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs",
  "method": "post",
  "headers": { "Authorization": "Basic YWRtaW46YWRtaW4=" },
  "json": {
    "job_id": "cff3ca77-...",
    "payload": { ... },
    "metadata": { ... }
  }
}
```
The `status` block is completely absent. The task will fail with:
```
AirflowException: Node task3 (KK_File_DB) is async_no_wait but has no 'status' block in conf.
A 'status' block with at minimum 'url' and 'response_status_key' is required.
```

**task7 — status put in wrong place:**
```json
"task7": {
  "url": "...",
  "json": {
    "job_id": "...",
    "async_status": {          ← WRONG: this is inside json body, sent to the API
      "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs"
    }
  }
}
```
`async_status` is buried inside `json` — this gets passed as part of the HTTP request body to the orchestrator, not read by the DAG service. The DAG service looks for `"status"` at the **top level** of the task conf, not inside `json`.

---

## Section 3 — How to Fix task3 and task7

The `"status"` block must be at the **top level** of the task conf, alongside `url`, `method`, and `headers`. It is NOT part of the JSON body sent to your service.

### Fixed task3

```json
"task3": {
  "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs",
  "method": "POST",
  "headers": {
    "Authorization": "Basic YWRtaW46YWRtaW4="
  },
  "timeout": 300,
  "verify_ssl": false,
  "json": {
    "job_id": "cff3ca77-8f26-4cc6-8b49-1675853dba69",
    "payload": {
      "runName": "edm_rc_reconciliation",
      "putDatasets": [
        {
          "name": "edm_rc_new_obj",
          "filePath": "/files/sourcesystem/edm/rc/Kratos/{{CorrelationId}}/edm_rc_new_obj.csv",
          "correlation_id": "{{CorrelationId}}"
        },
        {
          "name": "edm_rc_old_obj",
          "filePath": "/files/sourcesystem/edm/rc/Kratos/{{CorrelationId}}/edm_rc_old_obj.csv",
          "correlation_id": "{{CorrelationId}}"
        }
      ],
      "run_control_id": "{{RunControlId}}",
      "executionContext": {
        "parameters": {
          "outputFormat": "CSV"
        }
      },
      "job_type": "genesis-kratos-exec-svc"
    },
    "metadata": {
      "test": "api-to-db-temporal",
      "user": "admin"
    }
  },
  "response_id_key": "runId",
  "status": {
    "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs/{tracking_id}",
    "method": "GET",
    "headers": {
      "Authorization": "Basic YWRtaW46YWRtaW4="
    },
    "response_status_key": "status",
    "poke_interval": 15,
    "timeout": 3600,
    "success_statuses": ["SUCCESS", "COMPLETED", "SUCCEEDED"],
    "failure_statuses": ["FAILED", "ERROR", "ABORTED"],
    "running_statuses": ["RUNNING", "PENDING", "IN_PROGRESS", "QUEUED"]
  }
}
```

### Fixed task7

```json
"task7": {
  "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs",
  "method": "POST",
  "headers": {
    "Authorization": "Basic YWRtaW46YWRtaW4="
  },
  "timeout": 300,
  "verify_ssl": false,
  "json": {
    "job_id": "cff3ca77-8f26-4cc6-8b49-1675853dba69",
    "payload": {
      "runName": "edm_rc_reconciliation",
      "putDatasets": [
        {
          "name": "edm_rc_new_obj",
          "filePath": "/files/sourcesystem/edm/rc/Kratos/{{CorrelationId}}/edm_rc_new_obj.csv",
          "correlation_id": "{{CorrelationId}}"
        },
        {
          "name": "edm_rc_old_obj",
          "filePath": "/files/sourcesystem/edm/rc/Kratos/{{CorrelationId}}/edm_rc_old_obj.csv",
          "correlation_id": "{{CorrelationId}}"
        }
      ],
      "run_control_id": "{{RunControlId}}",
      "executionContext": {
        "parameters": {
          "outputFormat": "CSV"
        }
      },
      "job_type": "genesis-kratos-exec-svc"
    },
    "metadata": {
      "test": "api-to-db-temporal",
      "user": "admin"
    }
  },
  "response_id_key": "runId",
  "status": {
    "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs/{tracking_id}",
    "method": "GET",
    "headers": {
      "Authorization": "Basic YWRtaW46YWRtaW4="
    },
    "response_status_key": "status",
    "poke_interval": 15,
    "timeout": 3600,
    "success_statuses": ["SUCCESS", "COMPLETED", "SUCCEEDED"],
    "failure_statuses": ["FAILED", "ERROR", "ABORTED"],
    "running_statuses": ["RUNNING", "PENDING", "IN_PROGRESS", "QUEUED"]
  }
}
```

> **Important:** The `response_id_key` tells the DAG which field in the submit response contains the job ID to track. If your orchestrator returns `{"runId": "abc-123"}`, use `"response_id_key": "runId"`. If it returns `{"id": "abc-123"}`, use `"response_id_key": "id"`. Check your orchestrator's actual response schema.

---

## Section 4 — Complete Fixed Run Payload

This is your full corrected payload with task3 and task7 fixed and all tasks included:

```json
{
  "logical_date": "2026-05-05T10:00:00Z",
  "conf": {
    "correlation_id": "demo10-test-run-001",
    "resume": false,
    "resume_from": null,
    "force_rerun": false,
    "force_rerun_nodes": [],

    "task1": {
      "verify_ssl": false,
      "headers": { "Authorization": "Bearer token" },
      "method": "POST",
      "timeout": 10,
      "url": "http://10.5.16.153:8447/scaffold/file_to_db",
      "json": {
        "job_id": "test-file-to-db",
        "job_type": "file-to-db",
        "node_runId": "e4021416-cdb9-48aa-9907-aeaa28e35bc7",
        "run_control_id": "USER_FILETODB_TEST",
        "correlation_id": "nifi-file-to-db-test-001",
        "run_id": "RUN-2026-04-03-001",
        "user_id": "test_user",
        "cob_date": "2026-04-03",
        "payload": {
          "db": {
            "dbType": "postgresql",
            "host": "d12508vdrc-1855",
            "port": "7432",
            "database": "genesis",
            "username": "dev_genesis_admin",
            "password": "PmgVtrH9whkuf!BS8FD*xavW",
            "table_name": "exchange_rate_test_test",
            "schema_name": "centrl"
          },
          "file": {
            "file_type": "csv",
            "file_path": "/opt/nifi/nifi-current/genesis_dev/TCS/nifi/Inbound/ExchangeRateReport_202602.csv",
            "delimiter": ",",
            "has_header": true,
            "encoding": "utf-8"
          }
        }
      }
    },

    "task2": {
      "method": "POST",
      "timeout": 10,
      "url": "http://10.5.16.153:8447/scaffold/db-to-file",
      "json": {
        "job_type": "db-to-file",
        "job_id": "test-db-to-file",
        "payload": {
          "file": {
            "type": "delimited",
            "output_path": "/opt/nifi/nifi-current/genesis_dev/TCS/nifi/Outbound/",
            "filename_prefix": "exchange_rate_export",
            "delimiter": ","
          },
          "db": {
            "sqlalchemy_url": "postgresql://dev_genesis_admin:PmgVtrH9whkuf!BS8FD*xavW@d12508vdrc-1855:7432/genesis",
            "schema_name": "centrl",
            "table_name": "exchange_rate_test_test"
          }
        }
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
        "payload": {
          "runName": "edm_rc_reconciliation",
          "run_control_id": "DEMO10_RC",
          "job_type": "genesis-kratos-exec-svc",
          "executionContext": { "parameters": { "outputFormat": "CSV" } },
          "putDatasets": [
            { "name": "edm_rc_new_obj", "filePath": "/files/sourcesystem/edm/rc/edm_rc_new_obj.csv" },
            { "name": "edm_rc_old_obj", "filePath": "/files/sourcesystem/edm/rc/edm_rc_old_obj.csv" }
          ]
        },
        "metadata": { "test": "api-to-db-temporal", "user": "admin" }
      },
      "status": {
        "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs/{tracking_id}",
        "method": "GET",
        "headers": { "Authorization": "Basic YWRtaW46YWRtaW4=" },
        "response_status_key": "status",
        "poke_interval": 15,
        "timeout": 3600,
        "success_statuses": ["SUCCESS", "COMPLETED", "SUCCEEDED"],
        "failure_statuses": ["FAILED", "ERROR", "ABORTED"],
        "running_statuses": ["RUNNING", "PENDING", "IN_PROGRESS", "QUEUED"]
      }
    },

    "task4": {
      "method": "POST",
      "timeout": 10,
      "url": "http://10.5.16.153:8447/scaffold/api_to_db",
      "json": {
        "job_type": "api-to-db",
        "job_id": "test-chatham-api-to-db",
        "payload": {
          "db": {
            "sqlalchemy_url": "postgresql://dev_genesis_admin:PmgVtrH9whkuf!BS8FD*xavW@d12508vdrc-1855:7432/genesis",
            "schema_name": "genesis_demo",
            "table_name": "chatham_alltransactiondetails",
            "write_mode": "append",
            "create_table": true
          },
          "api": {
            "url": "https://api.chathamdirect.com/reporting/reports/transactions/all_transaction_details",
            "method": "GET",
            "bearer_token": "f57d3623-cf1e-4696-9ec9-9c26d3391d82",
            "verify_ssl": false
          }
        },
        "metadata": { "user": "admin", "test": "api-to-db-chatham" }
      }
    },

    "task5": {
      "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs",
      "verify_ssl": false,
      "method": "POST",
      "timeout": 300,
      "headers": {
        "Content-Type": "application/json",
        "Authorization": "Basic YWRtaW46YWRtaW4="
      },
      "json": {
        "job_type": "file-to-file",
        "job_id": "test-file-to-file",
        "user_id": "GEN_MAESTRO",
        "cob_date": "2026-06-03",
        "correlation_id": "73ff8c5-dc43-43ec-9a49-28e46ce1d5d3",
        "run_control_id": "USER_FILETOFILE_TEST"
      }
    },

    "task6": {
      "method": "POST",
      "timeout": 10,
      "url": "http://10.5.16.153:8447/scaffold/db-to-file",
      "json": {
        "job_type": "db-to-file",
        "job_id": "test-db-to-file-task6",
        "payload": {
          "file": {
            "type": "delimited",
            "output_path": "/opt/nifi/nifi-current/genesis_dev/TCS/nifi/Outbound/",
            "filename_prefix": "exchange_rate_export_task6",
            "delimiter": ","
          },
          "db": {
            "sqlalchemy_url": "postgresql://dev_genesis_admin:PmgVtrH9whkuf!BS8FD*xavW@d12508vdrc-1855:7432/genesis",
            "schema_name": "centrl",
            "table_name": "exchange_rate_test_test"
          }
        }
      }
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
        "payload": {
          "runName": "edm_rc_reconciliation",
          "run_control_id": "DEMO10_RC",
          "job_type": "genesis-kratos-exec-svc",
          "executionContext": { "parameters": { "outputFormat": "CSV" } },
          "putDatasets": [
            { "name": "edm_rc_new_obj", "filePath": "/files/sourcesystem/edm/rc/edm_rc_new_obj.csv" },
            { "name": "edm_rc_old_obj", "filePath": "/files/sourcesystem/edm/rc/edm_rc_old_obj.csv" }
          ]
        },
        "metadata": { "test": "api-to-db-temporal", "user": "admin" }
      },
      "status": {
        "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs/{tracking_id}",
        "method": "GET",
        "headers": { "Authorization": "Basic YWRtaW46YWRtaW4=" },
        "response_status_key": "status",
        "poke_interval": 15,
        "timeout": 3600,
        "success_statuses": ["SUCCESS", "COMPLETED", "SUCCEEDED"],
        "failure_statuses": ["FAILED", "ERROR", "ABORTED"],
        "running_statuses": ["RUNNING", "PENDING", "IN_PROGRESS", "QUEUED"]
      }
    },

    "task8": {
      "method": "POST",
      "timeout": 10,
      "url": "http://10.5.16.153:8447/scaffold/api_to_db",
      "json": {
        "job_type": "api-to-db",
        "job_id": "test-chatham-api-to-db-task8",
        "payload": {
          "db": {
            "sqlalchemy_url": "postgresql://dev_genesis_admin:PmgVtrH9whkuf!BS8FD*xavW@d12508vdrc-1855:7432/genesis",
            "schema_name": "genesis_demo",
            "table_name": "chatham_alltransactiondetails",
            "write_mode": "append",
            "create_table": true
          },
          "api": {
            "url": "https://api.chathamdirect.com/reporting/reports/transactions/all_transaction_details",
            "method": "GET",
            "bearer_token": "f57d3623-cf1e-4696-9ec9-9c26d3391d82",
            "verify_ssl": false
          }
        },
        "metadata": { "user": "admin", "test": "api-to-db-chatham-task8" }
      }
    },

    "task9": {
      "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs",
      "verify_ssl": false,
      "method": "POST",
      "timeout": 300,
      "headers": {
        "Content-Type": "application/json",
        "Authorization": "Basic YWRtaW46YWRtaW4="
      },
      "json": {
        "job_type": "file-to-file",
        "job_id": "test-file-to-file-task9",
        "user_id": "GEN_MAESTRO",
        "cob_date": "2026-06-03",
        "correlation_id": "73ff8c5-dc43-43ec-9a49-28e46ce1d5d3",
        "run_control_id": "USER_FILETOFILE_TEST"
      }
    },

    "task10": {
      "verify_ssl": false,
      "headers": { "Authorization": "Bearer token" },
      "method": "POST",
      "timeout": 10,
      "url": "http://10.5.16.153:8447/scaffold/file_to_db",
      "json": {
        "job_id": "test-file-to-db-task10",
        "job_type": "file-to-db",
        "node_runId": "e4021416-cdb9-48aa-9907-aeaa28e35bc7",
        "run_control_id": "USER_FILETODB_TEST",
        "correlation_id": "nifi-file-to-db-test-001",
        "run_id": "RUN-2026-04-03-001",
        "user_id": "test_user",
        "cob_date": "2026-04-03",
        "payload": {
          "db": {
            "dbType": "postgresql",
            "host": "d12508vdrc-1855",
            "port": "7432",
            "database": "genesis",
            "username": "dev_genesis_admin",
            "password": "PmgVtrH9whkuf!BS8FD*xavW",
            "table_name": "exchange_rate_test_test",
            "schema_name": "centrl"
          },
          "file": {
            "file_type": "csv",
            "file_path": "/opt/nifi/nifi-current/genesis_dev/TCS/nifi/Inbound/ExchangeRateReport_202602.csv",
            "delimiter": ",",
            "has_header": true,
            "encoding": "utf-8"
          }
        }
      }
    }
  }
}
```

---

## Section 5 — How to Test Resume

### What DAG structure supports resume testing

Resume works at the `executor_order_id` level. For DEMO_10:

| Node | Order | Mode |
|------|-------|------|
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

### Test Scenario 1 — Resume from task7 (most common scenario)

**Simulate:** task7 (async_no_wait, order=4) failed during a previous run.

**What resume does:**
- task1, task2, task3, task4, task5 → **skipped** (order < 4)
- task6, task7, task8 → **re-executed** (order = 4, same layer as resume_from)
- task9, task10 → **re-executed** (order > 4)
- Idempotency protects task6 and task8 if they previously succeeded with the same conf

**Resume payload:**

```json
{
  "logical_date": "2026-05-05T11:00:00Z",
  "conf": {
    "correlation_id": "demo10-test-run-001",
    "resume": true,
    "resume_from": "task7",
    "force_rerun": false,
    "force_rerun_nodes": [],

    "task6": {
      "method": "POST",
      "timeout": 10,
      "url": "http://10.5.16.153:8447/scaffold/db-to-file",
      "json": {
        "job_type": "db-to-file",
        "job_id": "test-db-to-file-task6",
        "payload": {
          "file": { "type": "delimited", "output_path": "/opt/nifi/nifi-current/genesis_dev/TCS/nifi/Outbound/", "filename_prefix": "exchange_rate_export_task6", "delimiter": "," },
          "db": { "sqlalchemy_url": "postgresql://dev_genesis_admin:PmgVtrH9whkuf!BS8FD*xavW@d12508vdrc-1855:7432/genesis", "schema_name": "centrl", "table_name": "exchange_rate_test_test" }
        }
      }
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
        "payload": {
          "runName": "edm_rc_reconciliation",
          "run_control_id": "DEMO10_RC",
          "job_type": "genesis-kratos-exec-svc",
          "executionContext": { "parameters": { "outputFormat": "CSV" } },
          "putDatasets": [
            { "name": "edm_rc_new_obj", "filePath": "/files/sourcesystem/edm/rc/edm_rc_new_obj.csv" },
            { "name": "edm_rc_old_obj", "filePath": "/files/sourcesystem/edm/rc/edm_rc_old_obj.csv" }
          ]
        },
        "metadata": { "test": "api-to-db-temporal", "user": "admin" }
      },
      "status": {
        "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs/{tracking_id}",
        "method": "GET",
        "headers": { "Authorization": "Basic YWRtaW46YWRtaW4=" },
        "response_status_key": "status",
        "poke_interval": 15,
        "timeout": 3600,
        "success_statuses": ["SUCCESS", "COMPLETED", "SUCCEEDED"],
        "failure_statuses": ["FAILED", "ERROR", "ABORTED"],
        "running_statuses": ["RUNNING", "PENDING", "IN_PROGRESS", "QUEUED"]
      }
    },

    "task8": {
      "method": "POST",
      "timeout": 10,
      "url": "http://10.5.16.153:8447/scaffold/api_to_db",
      "json": {
        "job_type": "api-to-db",
        "job_id": "test-chatham-api-to-db-task8",
        "payload": {
          "db": { "sqlalchemy_url": "postgresql://dev_genesis_admin:PmgVtrH9whkuf!BS8FD*xavW@d12508vdrc-1855:7432/genesis", "schema_name": "genesis_demo", "table_name": "chatham_alltransactiondetails", "write_mode": "append", "create_table": true },
          "api": { "url": "https://api.chathamdirect.com/reporting/reports/transactions/all_transaction_details", "method": "GET", "bearer_token": "f57d3623-cf1e-4696-9ec9-9c26d3391d82", "verify_ssl": false }
        }
      }
    },

    "task9": {
      "url": "https://genesis-orchestrator.apps.dv-p.ocp.fcbint.net/jobs",
      "verify_ssl": false,
      "method": "POST",
      "timeout": 300,
      "headers": { "Content-Type": "application/json", "Authorization": "Basic YWRtaW46YWRtaW4=" },
      "json": { "job_type": "file-to-file", "job_id": "test-file-to-file-task9", "user_id": "GEN_MAESTRO", "cob_date": "2026-06-03" }
    },

    "task10": {
      "verify_ssl": false,
      "headers": { "Authorization": "Bearer token" },
      "method": "POST",
      "timeout": 10,
      "url": "http://10.5.16.153:8447/scaffold/file_to_db",
      "json": {
        "job_id": "test-file-to-db-task10",
        "job_type": "file-to-db",
        "run_control_id": "USER_FILETODB_TEST",
        "correlation_id": "nifi-file-to-db-test-001",
        "payload": {
          "db": { "dbType": "postgresql", "host": "d12508vdrc-1855", "port": "7432", "database": "genesis", "username": "dev_genesis_admin", "password": "PmgVtrH9whkuf!BS8FD*xavW", "table_name": "exchange_rate_test_test", "schema_name": "centrl" },
          "file": { "file_type": "csv", "file_path": "/opt/nifi/nifi-current/genesis_dev/TCS/nifi/Inbound/ExchangeRateReport_202602.csv", "delimiter": ",", "has_header": true, "encoding": "utf-8" }
        }
      }
    }
  }
}
```

---

### Test Scenario 2 — Resume from task3 (mid-parallel layer)

**Simulate:** task3 (async_no_wait, order=3) failed.

**What resume does:**
- task1, task2 → skipped (order < 3)
- task3, task4, task5 → re-executed (order = 3)
- task6, task7, task8, task9, task10 → re-executed (order > 3)

```json
{
  "logical_date": "2026-05-05T11:30:00Z",
  "conf": {
    "correlation_id": "demo10-test-run-001",
    "resume": true,
    "resume_from": "task3",
    "force_rerun": false,
    "force_rerun_nodes": [],

    "task3": { "...same as fixed task3 above..." },
    "task4": { "...same as task4 above..." },
    "task5": { "...same as task5 above..." },
    "task6": { "...same as task6 above..." },
    "task7": { "...same as fixed task7 above..." },
    "task8": { "...same as task8 above..." },
    "task9": { "...same as task9 above..." },
    "task10": { "...same as task10 above..." }
  }
}
```

---

### Test Scenario 3 — Force rerun a single node (task4 only)

**Use case:** task4 succeeded originally but you need to re-run it again (e.g. data changed). You want everything else to resume normally from where it left off.

```json
{
  "logical_date": "2026-05-05T12:00:00Z",
  "conf": {
    "correlation_id": "demo10-test-run-001",
    "resume": true,
    "resume_from": "task4",
    "force_rerun": false,
    "force_rerun_nodes": ["task4"],

    "task4": {
      "method": "POST",
      "timeout": 10,
      "url": "http://10.5.16.153:8447/scaffold/api_to_db",
      "json": { "job_type": "api-to-db", "job_id": "test-chatham-api-to-db" }
    },
    "task5": { "...task5 conf..." },
    "task6": { "...task6 conf..." },
    "task7": { "...fixed task7 conf..." },
    "task8": { "...task8 conf..." },
    "task9": { "...task9 conf..." },
    "task10": { "...task10 conf..." }
  }
}
```

---

### Test Scenario 4 — Full force rerun (ignore all idempotency)

Re-run every single node from scratch, ignoring all previous success entries:

```json
{
  "logical_date": "2026-05-05T13:00:00Z",
  "conf": {
    "correlation_id": "demo10-test-run-002",
    "resume": false,
    "force_rerun": true,
    "force_rerun_nodes": [],

    "task1": { "...task1 conf..." },
    "task2": { "...task2 conf..." },
    "task3": { "...fixed task3 conf with status block..." },
    "task4": { "...task4 conf..." },
    "task5": { "...task5 conf..." },
    "task6": { "...task6 conf..." },
    "task7": { "...fixed task7 conf with status block..." },
    "task8": { "...task8 conf..." },
    "task9": { "...task9 conf..." },
    "task10": { "...task10 conf..." }
  }
}
```

> Note the new `correlation_id` (`demo10-test-run-002`). This generates entirely new idempotency keys, so no previous entries interfere.

---

## Section 6 — How to Verify Resume Is Working

### Step 1 — Check XCom in Airflow UI

After triggering the resume, go to Airflow UI → your DAG run → click any task that should have been **skipped** → XCom tab.

You should see:
```
Key: task1_task_state   Value: success
Key: task1_branch       Value: success
```

These are set by the resume skip logic — no HTTP call was made.

### Step 2 — Check task logs

Open a skipped task log. You will see:

```
[RESUME] Skipping node task1 (order=1) — resume_from=task7 (order=4)
```

Open an executing task log. You will see normal execution logs starting with the HTTP call.

### Step 3 — Check the runtime registry

```bash
cat /tmp/dynamic_dag_runtime_registry.json | python3 -m json.tool | grep -A5 '"state"'
```

Entries for skipped nodes will not be present (no registry write for resume skips). Entries for re-executed nodes will show `"state": "success"`.

### Step 4 — Check finalize_results summary

In the Airflow UI, click `finalize_results` → XCom → `final_summary`. You will see:

```json
{
  "final_status": "SUCCESS",
  "successful_tasks": [
    { "task_id": "KK_File_Transfer", "node_id": "task1", "state": "success", "status_source": "xcom_task_state_marker" },
    ...
  ],
  "failed_tasks": [],
  "expected_skipped_tasks": [],
  "unexpected_skipped_tasks": [],
  "running_tasks": [],
  "unknown_tasks": []
}
```

---

## Section 7 — Common Mistakes and Fixes

| Mistake | Symptom | Fix |
|---------|---------|-----|
| `status` block inside `json` body (task7 original) | Task completes instantly but never polls; external job may never be checked | Move `status` to top level of task conf |
| `status` block missing for async_no_wait (task3 original) | `AirflowException: has no 'status' block in conf` | Add `status` block with `url`, `response_status_key` |
| Wrong `response_id_key` | `AirflowException: could not extract tracking_id` | Check actual submit response body and set correct key |
| `correlation_id` changed between original run and resume | Idempotency keys don't match; all nodes re-execute | Use the same `correlation_id` for resume |
| `resume_from` typo | `AirflowException: resume_from='taskX' is not a valid node ID` | Use exact `id` from build payload |
| `method` lowercase (`"post"` instead of `"POST"`) | No issue — DAG uppercases it automatically | Minor; safe to fix for clarity |
| Missing `url` for any task | `AirflowException: Missing or empty 'url' in conf` | Add `url` to that task's conf entry |
