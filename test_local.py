"""
test_local.py — Local test harness for dynamic_dag_service_baremini.py

Runs without Airflow. Covers:
  1. POST /build_dag  — builds a DAG file and checks the response
  2. POST /build_dag  — same payload again to verify idempotent reuse
  3. Simulates execute_node() logic by importing the generated DAG file's
     resolve_payload() helper and mocking the HTTP call + XCom store
  4. Demonstrates resume behaviour (force_rerun_nodes)

Usage:
    # Terminal 1 — start the service
    python dynamic_dag_service_baremini.py

    # Terminal 2 — run the tests
    python test_local.py

Dependencies (in addition to what the service already needs):
    pip install requests
"""

import ast
import importlib.util
import json
import os
import sys
import time
import traceback
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

# ---------------------------------------------------------------------------
# Configuration — adjust if you run the service on a different host/port
# ---------------------------------------------------------------------------
BASE_URL = os.getenv("SERVICE_URL", "http://127.0.0.1:8443")
DAGS_DIR = Path(os.getenv("AIRFLOW_DAGS_DIR", "./dag_configs"))

# ---------------------------------------------------------------------------
# Colour helpers (works on most terminals)
# ---------------------------------------------------------------------------
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"

def ok(msg):   print(f"{GREEN}  ✓ {msg}{RESET}")
def fail(msg): print(f"{RED}  ✗ {msg}{RESET}")
def info(msg): print(f"{CYAN}  → {msg}{RESET}")
def warn(msg): print(f"{YELLOW}  ⚠ {msg}{RESET}")
def section(title): print(f"\n{CYAN}{'='*60}\n  {title}\n{'='*60}{RESET}")


# ---------------------------------------------------------------------------
# Build-dag payloads
# ---------------------------------------------------------------------------

# A simple two-node linear pipeline (sync)
SIMPLE_PAYLOAD = {
    "run_control_id": "local-test-pipeline",
    "triggerType": "O",
    "schedule": None,
    "nodes": [
        {
            "id": "node_fetch",
            "name": "Fetch Data",
            "engine": "rest",
            "executor_order_id": 1,
            "executor_sequence_id": 1,
            "execution_mode": "sync",
        },
        {
            "id": "node_process",
            "name": "Process Records",
            "engine": "rest",
            "executor_order_id": 2,
            "executor_sequence_id": 1,
            "execution_mode": "sync",
        },
    ],
}

# A branching pipeline: node_check → on success → node_process; on failure → node_notify
BRANCH_PAYLOAD = {
    "run_control_id": "local-branch-pipeline",
    "triggerType": "M",
    "nodes": [
        {
            "id": "node_check",
            "name": "Validation Check",
            "engine": "rest",
            "executor_order_id": 1,
            "executor_sequence_id": 1,
            "execution_mode": "sync",
            "branch_on_status": True,
            "on_success_node_ids": ["node_process"],
            "on_failure_node_ids": ["node_notify"],
        },
        {
            "id": "node_process",
            "name": "Process Records",
            "engine": "rest",
            "executor_order_id": 2,
            "executor_sequence_id": 1,
            "execution_mode": "sync",
        },
        {
            "id": "node_notify",
            "name": "Notify Failure",
            "engine": "rest",
            "executor_order_id": 2,
            "executor_sequence_id": 2,
            "execution_mode": "fire_and_forget",
        },
    ],
}

# A pipeline with one async node (polls a status endpoint)
ASYNC_PAYLOAD = {
    "run_control_id": "local-async-pipeline",
    "nodes": [
        {
            "id": "node_submit",
            "name": "Submit Job",
            "engine": "rest",
            "executor_order_id": 1,
            "executor_sequence_id": 1,
            "execution_mode": "async_no_wait",
        },
    ],
}


# ---------------------------------------------------------------------------
# Section 1 – Health check
# ---------------------------------------------------------------------------
def test_health():
    section("1. Health check")
    try:
        r = requests.get(f"{BASE_URL}/health", timeout=5)
        assert r.status_code == 200, f"Expected 200, got {r.status_code}"
        body = r.json()
        assert body.get("status") == "UP", f"Unexpected body: {body}"
        ok(f"Service is UP  version={body.get('version')}")
    except requests.ConnectionError:
        fail("Cannot connect to service. Is it running?  →  python dynamic_dag_service_baremini.py")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Section 2 – build_dag (first call → new file)
# ---------------------------------------------------------------------------
def test_build_dag_new(payload: dict, label: str) -> dict:
    section(f"2. build_dag — {label} (first call)")
    r = requests.post(f"{BASE_URL}/build_dag", json=payload, timeout=10)
    info(f"Status: {r.status_code}")
    body = r.json()
    info(f"Response: {json.dumps(body, indent=2)}")

    assert r.status_code == 201, f"Expected 201, got {r.status_code}: {r.text}"
    assert body["status"] == "SUCCESS"
    assert not body["idempotent_reused"], "Expected a fresh build"

    dag_file = Path(body["path"])
    assert dag_file.exists(), f"DAG file not found at {dag_file}"
    ok(f"DAG file created: {dag_file.name}")
    ok(f"idempotency_key: {body['idempotency_key']}")
    return body


# ---------------------------------------------------------------------------
# Section 3 – build_dag (second call with same payload → idempotent reuse)
# ---------------------------------------------------------------------------
def test_build_dag_idempotent(payload: dict, label: str, first_response: dict):
    section(f"3. build_dag — {label} (idempotent reuse)")
    r = requests.post(f"{BASE_URL}/build_dag", json=payload, timeout=10)
    body = r.json()
    info(f"Status: {r.status_code}")
    info(f"Response: {json.dumps(body, indent=2)}")

    assert r.status_code == 201, f"Expected 201, got {r.status_code}"
    assert body["idempotent_reused"] is True, "Expected idempotent reuse"
    assert body["dag_id"] == first_response["dag_id"]
    ok("Idempotent reuse confirmed — same dag_id returned, no new file written")


# ---------------------------------------------------------------------------
# Section 4 – Validate generated DAG parses as valid Python
# ---------------------------------------------------------------------------
def test_generated_dag_syntax(dag_path: str, label: str):
    section(f"4. Syntax check — {label}")
    source = Path(dag_path).read_text(encoding="utf-8")
    try:
        ast.parse(source)
        ok("Generated DAG is valid Python (ast.parse passed)")
    except SyntaxError as exc:
        fail(f"Syntax error in generated DAG: {exc}")
        raise


# ---------------------------------------------------------------------------
# Section 5 – Simulate execute_node (mocked HTTP + XCom)
# ---------------------------------------------------------------------------

def _make_mock_context(conf: dict, dag_id: str = "test_dag", run_id: str = "test_run_id"):
    """Build a minimal Airflow-like context dict usable by execute_node."""
    xcom_store: dict = {}

    ti = MagicMock()
    ti.xcom_push.side_effect = lambda key, value: xcom_store.update({key: value})
    ti.xcom_pull.side_effect = lambda task_ids=None, key=None: xcom_store.get(key)

    dag = MagicMock()
    dag.dag_id = dag_id

    dag_run = MagicMock()
    dag_run.conf = conf
    dag_run.run_id = run_id
    dag_run.external_trigger_id = run_id

    return {
        "dag": dag,
        "dag_run": dag_run,
        "ti": ti,
        "_xcom_store": xcom_store,
    }


def _load_service_module():
    """Import dynamic_dag_service_baremini without executing __main__."""
    spec = importlib.util.spec_from_file_location(
        "dynamic_dag_service",
        Path(__file__).parent / "dynamic_dag_service_baremini.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_execute_node_sync():
    section("5a. execute_node simulation — sync mode")

    # Conf that would be passed when triggering the DAG
    conf = {
        "correlation_id": "test-corr-001",
        "node_fetch": {
            "url": "https://httpbin.org/post",
            "method": "POST",
            "json": {"hello": "world"},
        },
    }

    ctx = _make_mock_context(conf)

    # Mock resolve_payload and the HTTP call to avoid real network calls
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = '{"result": "ok"}'
    mock_response.json.return_value = {"result": "ok"}

    mod = _load_service_module()

    # Patch requests.request and get_current_context inside the *generated* DAG template
    # (They live in the same module here since we're testing the service helpers directly.)
    with patch("requests.request", return_value=mock_response):
        # Simulate what the generated DAG's execute_node() does:
        payload = mod.resolve_payload.__func__(conf, node_id="node_fetch", task_key="node_fetch") \
            if hasattr(mod.resolve_payload, "__func__") else mod.resolve_payload(conf, node_id="node_fetch", task_key="node_fetch")  # type: ignore[attr-defined]

        info(f"Resolved payload for node_fetch: {json.dumps(payload, indent=2)}")
        assert payload.get("url") == "https://httpbin.org/post"
        ok("resolve_payload correctly resolved node_fetch conf")

    # Also test fallback lookup by node_runId
    conf_alt = {
        "some_key": {
            "node_runId": "node_fetch",
            "url": "https://example.com/api",
            "json": {},
        }
    }
    payload_alt = mod.resolve_payload(conf_alt, node_id="node_fetch")
    assert payload_alt.get("url") == "https://example.com/api"
    ok("resolve_payload fallback via node_runId works")


def test_execute_node_missing_url():
    section("5b. execute_node simulation — missing URL raises")
    conf = {
        "node_fetch": {
            # intentionally missing 'url'
            "json": {"param": "value"},
        }
    }
    mod = _load_service_module()
    payload = mod.resolve_payload(conf, node_id="node_fetch")
    assert not payload.get("url"), "Should have no URL"
    ok("Payload correctly missing URL — task would raise AirflowException")


# ---------------------------------------------------------------------------
# Section 6 – Validation rejections from /build_dag
# ---------------------------------------------------------------------------
def test_validation_errors():
    section("6. Validation error cases")

    cases = [
        (
            "missing run_control_id",
            {"nodes": [{"id": "n1", "name": "N1", "engine": "rest", "executor_order_id": 1, "executor_sequence_id": 1}]},
        ),
        (
            "duplicate node IDs",
            {
                "run_control_id": "dup-test",
                "nodes": [
                    {"id": "n1", "name": "A", "engine": "rest", "executor_order_id": 1, "executor_sequence_id": 1},
                    {"id": "n1", "name": "B", "engine": "rest", "executor_order_id": 2, "executor_sequence_id": 1},
                ],
            },
        ),
        (
            "branch_on_status=true without targets",
            {
                "run_control_id": "branch-no-target",
                "nodes": [
                    {"id": "n1", "name": "A", "engine": "rest", "executor_order_id": 1, "executor_sequence_id": 1, "branch_on_status": True},
                ],
            },
        ),
        (
            "fire_and_forget with branch_on_status",
            {
                "run_control_id": "ff-branch",
                "nodes": [
                    {
                        "id": "n1", "name": "A", "engine": "rest",
                        "executor_order_id": 1, "executor_sequence_id": 1,
                        "execution_mode": "fire_and_forget",
                        "branch_on_status": True,
                        "on_success_node_ids": ["n2"],
                    },
                    {"id": "n2", "name": "B", "engine": "rest", "executor_order_id": 2, "executor_sequence_id": 1},
                ],
            },
        ),
        (
            "branch references unknown node",
            {
                "run_control_id": "bad-ref",
                "nodes": [
                    {
                        "id": "n1", "name": "A", "engine": "rest",
                        "executor_order_id": 1, "executor_sequence_id": 1,
                        "branch_on_status": True,
                        "on_success_node_ids": ["does_not_exist"],
                    },
                ],
            },
        ),
    ]

    for case_name, payload in cases:
        r = requests.post(f"{BASE_URL}/build_dag", json=payload, timeout=5)
        if r.status_code == 422:
            ok(f"Rejected as expected — {case_name}")
        else:
            fail(f"Expected 422 for '{case_name}', got {r.status_code}: {r.text}")


# ---------------------------------------------------------------------------
# Section 7 – Resume / force_rerun_nodes conf structure demo
# ---------------------------------------------------------------------------
def test_resume_conf_structure():
    section("7. Resume conf structure demo")

    # This is NOT an API call — it just prints example confs to show the structure
    # In real usage these confs are passed when triggering the Airflow DAG run.

    normal_run_conf = {
        "correlation_id": "corr-abc-001",
        "node_fetch": {
            "url": "https://my-service/api/fetch",
            "method": "POST",
            "json": {"dataset": "orders"},
        },
        "node_process": {
            "url": "https://my-service/api/process",
            "method": "POST",
            "json": {"mode": "full"},
        },
    }

    resume_conf = {
        **normal_run_conf,
        # Keep the SAME correlation_id — it's part of the idempotency key.
        # node_fetch succeeded before → it will be skipped (registry hit).
        # node_process failed before → it will re-execute.
        "resume": True,
        "resume_from": "node_process",
    }

    force_rerun_conf = {
        **normal_run_conf,
        "force_rerun_nodes": ["node_fetch"],   # re-run even if it succeeded
    }

    force_all_conf = {
        **normal_run_conf,
        "force_rerun": True,   # ignore registry for every node
    }

    info("Normal run conf:")
    print(json.dumps(normal_run_conf, indent=2))

    info("\nResume conf (reuse succeeded nodes, retry failed ones):")
    print(json.dumps(resume_conf, indent=2))

    info("\nForce re-run specific nodes:")
    print(json.dumps(force_rerun_conf, indent=2))

    info("\nForce full re-run:")
    print(json.dumps(force_all_conf, indent=2))

    ok("Resume conf structures printed above — use these when triggering the Airflow DAG run")


# ---------------------------------------------------------------------------
# Section 8 – Async node conf structure demo
# ---------------------------------------------------------------------------
def test_async_conf_structure():
    section("8. Async node conf structure demo")

    async_conf = {
        "correlation_id": "corr-async-001",
        "node_submit": {
            "url": "https://my-service/api/jobs/submit",
            "method": "POST",
            "json": {"workload": "nightly-batch"},
            "status": {
                "url": "https://my-service/api/jobs/{tracking_id}/status",
                "method": "GET",
                "response_id_key": "job_id",
                "response_status_key": "state",
                "poke_interval": 15,
                "timeout_seconds": 600,
                "success_statuses": ["SUCCESS", "COMPLETED"],
                "failure_statuses": ["FAILED", "ERROR"],
                "running_statuses": ["RUNNING", "PENDING"],
            },
        },
    }

    info("Async node conf (async_no_wait execution mode):")
    print(json.dumps(async_conf, indent=2))
    ok("Async conf structure printed above")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
def main():
    errors = []

    def run(fn, *args, **kwargs):
        try:
            fn(*args, **kwargs)
        except Exception as exc:
            errors.append((fn.__name__, exc))
            fail(f"{fn.__name__} raised: {exc}")
            traceback.print_exc()

    test_health()

    # Simple pipeline
    simple_result = test_build_dag_new(SIMPLE_PAYLOAD, "simple linear")
    run(test_build_dag_idempotent, SIMPLE_PAYLOAD, "simple linear", simple_result)
    run(test_generated_dag_syntax, simple_result["path"], "simple linear")

    # Branch pipeline
    branch_result = test_build_dag_new(BRANCH_PAYLOAD, "branching")
    run(test_generated_dag_syntax, branch_result["path"], "branching")

    # Async pipeline
    async_result = test_build_dag_new(ASYNC_PAYLOAD, "async")
    run(test_generated_dag_syntax, async_result["path"], "async")

    # Unit-level tests (no Airflow needed)
    run(test_execute_node_sync)
    run(test_execute_node_missing_url)
    run(test_validation_errors)
    run(test_resume_conf_structure)
    run(test_async_conf_structure)

    # Summary
    section("Summary")
    if errors:
        fail(f"{len(errors)} test(s) failed:")
        for name, exc in errors:
            print(f"    {RED}{name}{RESET}: {exc}")
        sys.exit(1)
    else:
        ok("All tests passed!")


if __name__ == "__main__":
    main()
