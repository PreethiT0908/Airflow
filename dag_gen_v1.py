"""
Dynamic Airflow DAG Service — airflow_job1 v1
Target: Airflow 3.0.6 (dev environment)

Fixes applied vs original:
  1. Task ID = exact node name (validated unique at build time)
  2. resume_from unknown node raises immediate error
  3. choose_branch null XCom falls to failure with clear error
  4. finalize_results XCom missing → logs warning, defaults to FAILED
  5. store_registry_entry / get_existing_registry_entry wrapped in try/except
  6. portalocker missing at runtime raises RuntimeError (no silent skip)
  7. Runtime lock uses contextmanager (no lock-leak on exception)
  8. Merge guard: checks all sync+async upstreams have success XCom
     before allowing merge node to proceed; fire_and_forget excluded
  9. Async node renamed internally to async_poll; status block required
 10. Two separate execution versions: 3.0.6 (this file) and 3.1.8
 11. Node name uniqueness enforced at build time (exception on collision)
 12. Branch with no failure targets raises clean error instead of crash
"""

import hashlib
import json
import logging
import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional, Set, Tuple

import portalocker
import uvicorn
from fastapi import FastAPI, HTTPException, Request, status
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    RetryError,
)
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError as PydanticValidationError, field_validator, model_validator


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("dynamic-dag-service")

# ---------------------------------------------------------------------------
# App & constants
# ---------------------------------------------------------------------------

app = FastAPI(title="Dynamic Airflow DAG Service", version="1.0.0-airflow306")

DAGS_FOLDER = Path(os.getenv("AIRFLOW_DAGS_DIR", "./dag_configs"))
DAGS_FOLDER.mkdir(parents=True, exist_ok=True)

KAFKA_CONN_ID = os.getenv("KAFKA_CONN_ID", "genesis_kafka_conn")
KAFKA_RUN_TOPIC = os.getenv("KAFKA_RUN_TOPIC", "genesis.hub.run.events.v1")

SUPPORTED_EXECUTION_MODES = {"sync", "async_no_wait", "fire_and_forget"}
SUPPORTED_TRIGGER_TYPES = {"O", "M", "S"}
TRIGGER_TYPE_MAPPING = {"0": "O", "1": "M", "2": "S"}

EVENTS = {
    "run_started": "run.started.v1",
    "run_succeeded": "run.succeeded.v1",
    "run_failed": "run.failed.v1",
}

REGISTRY_FILE = Path(
    os.getenv("BUILD_IDEMPOTENCY_REGISTRY", str(DAGS_FOLDER / "build_registry.json"))
)
REGISTRY_FILE.parent.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class Node(BaseModel):
    id: str = Field(min_length=1)
    engine: str = Field(min_length=1)
    name: str = Field(min_length=1)
    executor_order_id: int = Field(ge=1)
    executor_sequence_id: int = Field(ge=1)
    execution_mode: str = Field(default="sync")
    branch_on_status: bool = False
    on_success_node_ids: List[str] = Field(default_factory=list)
    on_failure_node_ids: List[str] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    @field_validator("id", "engine", "name", mode="before")
    @classmethod
    def normalize_string_fields(cls, value: Any) -> str:
        if value is None:
            raise ValueError("Field cannot be null")
        clean_value = str(value).strip()
        if not clean_value:
            raise ValueError("Field cannot be empty or whitespace")
        return clean_value

    @field_validator("execution_mode", mode="before")
    @classmethod
    def validate_execution_mode(cls, value: Any) -> str:
        clean_value = str(value or "sync").strip().lower()
        if clean_value not in SUPPORTED_EXECUTION_MODES:
            raise ValueError(f"Unsupported execution_mode: {clean_value}")
        return clean_value

    @field_validator("on_success_node_ids", "on_failure_node_ids", mode="before")
    @classmethod
    def normalize_node_id_lists(cls, value: Any) -> List[str]:
        if value in (None, ""):
            return []
        if not isinstance(value, list):
            raise ValueError("Must be a list")
        cleaned: List[str] = []
        seen: Set[str] = set()
        for item in value:
            item_str = str(item).strip()
            if not item_str:
                continue
            if item_str not in seen:
                cleaned.append(item_str)
                seen.add(item_str)
        return cleaned


class BuildDagPayload(BaseModel):
    run_control_id: str = Field(min_length=1)
    trigger_type: Optional[str] = Field(default=None, alias="triggerType")
    schedule: Optional[str] = None
    nodes: List[Node] = Field(min_length=1)

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    @field_validator("run_control_id", mode="before")
    @classmethod
    def normalize_run_control_id(cls, value: Any) -> str:
        if value is None:
            raise ValueError("run_control_id cannot be null")
        clean_value = str(value).strip()
        if not clean_value:
            raise ValueError("run_control_id cannot be empty or whitespace")
        return clean_value

    @field_validator("trigger_type", mode="before")
    @classmethod
    def normalize_trigger_type(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        clean_value = str(value).strip().upper()
        if not clean_value:
            return None
        clean_value = TRIGGER_TYPE_MAPPING.get(clean_value, clean_value)
        if clean_value not in SUPPORTED_TRIGGER_TYPES:
            raise ValueError(f"Unsupported triggerType: {clean_value}")
        return clean_value

    @field_validator("schedule", mode="before")
    @classmethod
    def normalize_schedule(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        clean_value = str(value).strip()
        return clean_value or None

    @field_validator("nodes")
    @classmethod
    def validate_unique_ids_names_and_positions(cls, nodes: List[Node]) -> List[Node]:
        seen_ids: Set[str] = set()
        duplicate_ids: Set[str] = set()
        # FIX #1: node name must be unique — task ID = node name directly
        seen_names: Set[str] = set()
        duplicate_names: Set[str] = set()
        seen_sequence_pairs: Set[Tuple[int, int]] = set()
        duplicate_sequence_pairs: Set[Tuple[int, int]] = set()

        for node in nodes:
            if node.id in seen_ids:
                duplicate_ids.add(node.id)
            seen_ids.add(node.id)

            # Names must be unique because they become Airflow task_ids
            if node.name in seen_names:
                duplicate_names.add(node.name)
            seen_names.add(node.name)

            seq_pair = (node.executor_order_id, node.executor_sequence_id)
            if seq_pair in seen_sequence_pairs:
                duplicate_sequence_pairs.add(seq_pair)
            seen_sequence_pairs.add(seq_pair)

        errors = []
        if duplicate_ids:
            errors.append(f"Duplicate node ids: {sorted(duplicate_ids)}")
        if duplicate_names:
            errors.append(
                f"Duplicate node names found: {sorted(duplicate_names)}. "
                f"Each node name must be unique because it becomes the Airflow task_id."
            )
        if duplicate_sequence_pairs:
            pairs = [f"(order={o}, seq={s})" for o, s in sorted(duplicate_sequence_pairs)]
            errors.append(f"Duplicate executor ordering: {pairs}")

        if errors:
            raise ValueError("; ".join(errors))

        return nodes

    @model_validator(mode="after")
    def validate_branch_references(self) -> "BuildDagPayload":
        node_ids = {node.id for node in self.nodes}
        errors: List[str] = []

        for node in self.nodes:
            referenced_ids = node.on_success_node_ids + node.on_failure_node_ids
            missing_ids = sorted({ref for ref in referenced_ids if ref not in node_ids})
            if missing_ids:
                errors.append(f"Node {node.id} references unknown node ids: {missing_ids}")

            if node.branch_on_status and not referenced_ids:
                errors.append(f"Node {node.id} has branch_on_status=true but no branch targets")

            if not node.branch_on_status and referenced_ids:
                errors.append(f"Node {node.id} has branch targets but branch_on_status=false")

            if node.execution_mode == "fire_and_forget" and node.branch_on_status:
                errors.append(
                    f"Node {node.id} uses fire_and_forget and cannot branch on final status"
                )

            # FIX #12: node cannot appear in both success and failure lists
            overlap = set(node.on_success_node_ids) & set(node.on_failure_node_ids)
            if overlap:
                errors.append(
                    f"Node {node.id} has nodes in both success and failure targets: {sorted(overlap)}"
                )

        if errors:
            raise ValueError("; ".join(errors))

        return self


# ---------------------------------------------------------------------------
# FastAPI exception handlers
# ---------------------------------------------------------------------------


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handles FastAPI request-level validation errors (missing fields, wrong types)."""
    try:
        errors = exc.errors()
        # Pydantic errors may contain non-serializable `url` objects — normalize them
        safe_errors = json.loads(json.dumps(errors, default=str))
    except Exception:
        safe_errors = [{"msg": str(exc)}]
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": safe_errors},
    )


@app.exception_handler(PydanticValidationError)
async def pydantic_validation_exception_handler(request: Request, exc: PydanticValidationError):
    """
    Handles Pydantic ValidationError raised inside validators (e.g. duplicate node names).
    These bubble up as a raw PydanticValidationError when Pydantic wraps our ValueError,
    causing 'Object of type ValueError is not JSON serializable' if handled generically.
    """
    try:
        raw_errors = exc.errors()
        # Each error dict has a 'ctx' key that may contain the original ValueError object
        safe_errors = []
        for err in raw_errors:
            safe_err = {
                "type": err.get("type", "validation_error"),
                "loc": list(err.get("loc", [])),
                "msg": err.get("msg", str(err)),
            }
            # Extract the human-readable message from ctx.error if present
            ctx = err.get("ctx", {})
            if isinstance(ctx, dict) and "error" in ctx:
                ctx_error = ctx["error"]
                safe_err["msg"] = str(ctx_error)
            safe_errors.append(safe_err)
    except Exception:
        safe_errors = [{"msg": str(exc)}]

    logger.warning("Payload validation failed at %s: %s", request.url.path, safe_errors)
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": safe_errors},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Global exception caught at: %s", request.url.path)
    try:
        error_message = str(exc)
    except Exception:
        error_message = repr(exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "Internal server error",
            "error_type": type(exc).__name__,
            "error_message": error_message,
        },
    )


# ---------------------------------------------------------------------------
# Identifier helpers
# ---------------------------------------------------------------------------


def sanitize_identifier(raw: str) -> str:
    """Normalize identifiers to lowercase ASCII-safe tokens."""
    value = (raw or "").strip().lower()
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "dag"


def python_var_safe(raw: str) -> str:
    """Convert an arbitrary identifier into a Python-safe variable name."""
    base = sanitize_identifier(raw)
    if not base:
        base = "task"
    if base[0].isdigit():
        base = f"n_{base}"
    return base


# ---------------------------------------------------------------------------
# File I/O helpers
# ---------------------------------------------------------------------------


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
        tmp.write(content)
        tmp_name = tmp.name
    os.chmod(tmp_name, 0o644)
    Path(tmp_name).replace(path)


def load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        logger.warning("Failed to load JSON file %s; using default", path)
        return default


@contextmanager
def file_lock(lock_path: Path):
    """FIX #14: contextmanager ensures lock is always released even on exception."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        portalocker.lock(lock_file, portalocker.LOCK_EX)
        try:
            yield
        finally:
            portalocker.unlock(lock_file)


def write_json_file(path: Path, data: Any) -> None:
    atomic_write_text(path, json.dumps(data, indent=2, sort_keys=True, default=str))


# ---------------------------------------------------------------------------
# Build-time idempotency registry
# FIX #5: all registry operations wrapped in try/except with clear errors
# ---------------------------------------------------------------------------


def canonicalize_build_payload(payload: BuildDagPayload) -> Dict[str, Any]:
    ordered_nodes = sorted(
        payload.nodes,
        key=lambda node: (node.executor_order_id, node.executor_sequence_id, node.id),
    )
    normalized_nodes = [
        {
            "id": node.id,
            "name": node.name,
            "engine": node.engine,
            "executor_order_id": node.executor_order_id,
            "executor_sequence_id": node.executor_sequence_id,
            "execution_mode": node.execution_mode,
            "branch_on_status": node.branch_on_status,
            "on_success_node_ids": sorted(node.on_success_node_ids),
            "on_failure_node_ids": sorted(node.on_failure_node_ids),
        }
        for node in ordered_nodes
    ]
    return {
        "run_control_id": payload.run_control_id,
        "trigger_type": payload.trigger_type,
        "schedule": payload.schedule,
        "nodes": normalized_nodes,
    }


def canonicalize_payload_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(k): canonicalize_payload_value(v)
            for k, v in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (tuple, list)):
        return [canonicalize_payload_value(item) for item in value]
    if isinstance(value, str):
        return value.strip()
    return value


def compute_sha256(payload: Any) -> str:
    canonical = canonicalize_payload_value(payload)
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def get_existing_registry_entry(idempotency_key: str) -> Optional[Dict[str, Any]]:
    """FIX #5: wrapped in try/except so disk errors don't crash the service."""
    try:
        with file_lock(REGISTRY_FILE.with_suffix(".lock")):
            registry = load_json_file(REGISTRY_FILE, default={})
            entry = registry.get(idempotency_key)
            if not entry:
                return None
            path_str = entry.get("path")
            if not path_str:
                return None
            dag_file_path = Path(path_str)
            if not dag_file_path.exists():
                logger.warning(
                    "Registry entry exists but DAG file missing for key %s; removing stale entry",
                    idempotency_key,
                )
                registry.pop(idempotency_key, None)
                write_json_file(REGISTRY_FILE, registry)
                return None
            return entry
    except Exception as exc:
        logger.error(
            "Failed to read build registry (key=%s): %s. Proceeding as cache miss.",
            idempotency_key, exc,
        )
        return None


def store_registry_entry(idempotency_key: str, entry: Dict[str, Any]) -> None:
    """FIX #5: wrapped in try/except so disk errors don't silently corrupt state."""
    try:
        with file_lock(REGISTRY_FILE.with_suffix(".lock")):
            registry = load_json_file(REGISTRY_FILE, default={})
            registry[idempotency_key] = entry
            write_json_file(REGISTRY_FILE, registry)
    except Exception as exc:
        logger.error(
            "Failed to write build registry (key=%s): %s. "
            "The DAG file was written but idempotency entry was NOT stored. "
            "A duplicate build may occur on the next call.",
            idempotency_key, exc,
        )
        raise RuntimeError(
            f"Registry write failed for key {idempotency_key}: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Layer / dependency builders
# ---------------------------------------------------------------------------


def detect_cycles(nodes: List[Node]) -> None:
    """
    FIX C: Detects cycles in the DAG graph using DFS (depth-first search).

    Checks two types of edges:
      1. Layer-level edges: nodes in layer N depend on all nodes in layer N-1
         (implicit sequential dependency via executor_order_id)
      2. Branch edges: on_success_node_ids and on_failure_node_ids
         (explicit conditional dependencies)

    Raises ValueError with a human-readable cycle path if a cycle is found.

    Example cycle that would be caught:
      task1 -> on_success -> task2 -> on_success -> task1  (direct cycle)
      task1 order=1, task2 order=2, task3 order=1 with on_success=[task2] (back-edge)
    """
    # Build adjacency list of ALL outbound edges per node
    adjacency: Dict[str, List[str]] = {node.id: [] for node in nodes}
    order_to_ids: Dict[int, List[str]] = {}
    id_to_order: Dict[str, int] = {}

    for node in nodes:
        order_to_ids.setdefault(node.executor_order_id, []).append(node.id)
        id_to_order[node.id] = node.executor_order_id

    # Layer-level implicit edges: every node in order N points to every node in order N+1
    sorted_orders = sorted(order_to_ids.keys())
    for i in range(len(sorted_orders) - 1):
        current_order = sorted_orders[i]
        next_order = sorted_orders[i + 1]
        for src_id in order_to_ids[current_order]:
            for dst_id in order_to_ids[next_order]:
                if dst_id not in adjacency[src_id]:
                    adjacency[src_id].append(dst_id)

    # Branch edges: explicit on_success / on_failure connections
    for node in nodes:
        for target_id in node.on_success_node_ids + node.on_failure_node_ids:
            if target_id in adjacency and target_id not in adjacency[node.id]:
                adjacency[node.id].append(target_id)

    # DFS cycle detection
    # State: 0 = unvisited, 1 = in current path (grey), 2 = fully visited (black)
    state: Dict[str, int] = {node_id: 0 for node_id in adjacency}
    path: List[str] = []

    def dfs(node_id: str) -> bool:
        state[node_id] = 1  # mark as in-path
        path.append(node_id)
        for neighbour in adjacency.get(node_id, []):
            if state[neighbour] == 1:
                # Found a back-edge — cycle detected
                cycle_start = path.index(neighbour)
                cycle_path = path[cycle_start:] + [neighbour]
                raise ValueError(
                    f"Cycle detected in DAG graph: {' -> '.join(cycle_path)}. "
                    f"A DAG cannot have circular dependencies. "
                    f"Check executor_order_id assignments and branch target node IDs."
                )
            if state[neighbour] == 0:
                dfs(neighbour)
        path.pop()
        state[node_id] = 2  # mark as fully visited
        return False

    for node_id in list(adjacency.keys()):
        if state[node_id] == 0:
            dfs(node_id)


def build_layers(nodes: List[Node]) -> List[Tuple[int, List[Dict[str, Any]]]]:
    layers: Dict[int, List[Dict[str, Any]]] = {}
    for node in nodes:
        layers.setdefault(node.executor_order_id, []).append(node.model_dump())
    return [
        (order_id, sorted(items, key=lambda item: item["executor_sequence_id"]))
        for order_id, items in sorted(layers.items(), key=lambda item: item[0])
    ]


def build_stage_dependencies(
    sorted_layers: List[Tuple[int, List[Dict[str, Any]]]],
    task_id_map: Dict[str, str],
) -> List[Tuple[str, str]]:
    """
    Return (parent_task_id, child_task_id) pairs for sequential stage wiring.
    Branch-source nodes are excluded — their outbound edges are handled
    separately via the BranchPythonOperator.
    """
    deps: List[Tuple[str, str]] = []
    branch_source_ids = {
        node["id"]
        for _, nodes in sorted_layers
        for node in nodes
        if node.get("branch_on_status")
    }

    for index, (_, current_nodes) in enumerate(sorted_layers):
        if index == 0:
            for child in current_nodes:
                deps.append(("run_started_event", task_id_map[child["id"]]))
            continue

        _, prev_nodes = sorted_layers[index - 1]

        if len(current_nodes) == 1:
            child = current_nodes[0]
            for parent in prev_nodes:
                if parent["id"] not in branch_source_ids:
                    deps.append((task_id_map[parent["id"]], task_id_map[child["id"]]))
        else:
            for child in current_nodes:
                same_seq_parents = [
                    parent
                    for parent in prev_nodes
                    if parent["executor_sequence_id"] == child["executor_sequence_id"]
                    and parent["id"] not in branch_source_ids
                ]
                for parent in same_seq_parents:
                    deps.append((task_id_map[parent["id"]], task_id_map[child["id"]]))

    return deps


def validate_generated_python(code: str) -> None:
    compile(code, "<generated_dag>", "exec")


# ---------------------------------------------------------------------------
# Indentation helper
# ---------------------------------------------------------------------------

_TASK_INDENT = "    "


def _indent_block(code: str, indent: str) -> str:
    """Re-indent every non-empty line of *code* with *indent*."""
    lines = []
    for line in code.splitlines():
        if line.strip():
            lines.append(indent + line)
        else:
            lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# DAG code generator
# ---------------------------------------------------------------------------


def generate_dag_code(
    dag_id: str,
    run_control_id: str,
    sorted_layers: List[Tuple[int, List[Dict[str, Any]]]],
    schedule: Optional[str],
) -> str:
    all_nodes = [node for _, layer_nodes in sorted_layers for node in layer_nodes]

    # FIX #1: Task ID = exact node name (no sanitization suffix)
    # Names are guaranteed unique by build-time validation.
    task_id_map: Dict[str, str] = {node["id"]: node["name"] for node in all_nodes}

    node_ids = [node["id"] for node in all_nodes]
    async_node_ids = [node["id"] for node in all_nodes if node["execution_mode"] == "async_no_wait"]
    fire_and_forget_node_ids = [
        node["id"] for node in all_nodes if node["execution_mode"] == "fire_and_forget"
    ]
    node_name_map = {node["id"]: node["name"] for node in all_nodes}
    task_id_to_node_id = {tid: nid for nid, tid in task_id_map.items()}

    # Python variable names (safe, unique)
    var_map: Dict[str, str] = {}
    used_vars: Set[str] = set()
    for nid in node_ids:
        base = python_var_safe(task_id_map[nid])
        candidate = base
        i = 2
        while candidate in used_vars:
            candidate = f"{base}_{i}"
            i += 1
        used_vars.add(candidate)
        var_map[nid] = candidate

    # Branch task IDs
    branch_task_ids: Dict[str, str] = {
        node["id"]: f"branch__{node['name']}"
        for node in all_nodes
        if node.get("branch_on_status")
    }

    branch_var_map: Dict[str, str] = {}
    for nid, branch_tid in branch_task_ids.items():
        base = f"branch_{var_map[nid]}"
        candidate = base
        i = 2
        while candidate in used_vars:
            candidate = f"{base}_{i}"
            i += 1
        used_vars.add(candidate)
        branch_var_map[branch_tid] = candidate

    deps = build_stage_dependencies(sorted_layers, task_id_map)

    for node in all_nodes:
        if node.get("branch_on_status"):
            deps.append((task_id_map[node["id"]], branch_task_ids[node["id"]]))
            for target in node.get("on_success_node_ids", []):
                deps.append((branch_task_ids[node["id"]], task_id_map[target]))
            for target in node.get("on_failure_node_ids", []):
                deps.append((branch_task_ids[node["id"]], task_id_map[target]))

    # De-duplicate
    deduped_deps: List[Tuple[str, str]] = []
    seen_deps: Set[Tuple[str, str]] = set()
    for dep in deps:
        if dep not in seen_deps:
            deduped_deps.append(dep)
            seen_deps.add(dep)

    # Upstream map — detect merge nodes
    upstream_map: Dict[str, Set[str]] = {task_id_map[nid]: set() for nid in node_ids}
    for parent, child in deduped_deps:
        if child in upstream_map and parent != "run_started_event":
            upstream_map[child].add(parent)

    branch_skip_whitelist: Set[str] = set()
    for node in all_nodes:
        if node.get("branch_on_status"):
            branch_skip_whitelist.update(task_id_map[t] for t in node.get("on_success_node_ids", []))
            branch_skip_whitelist.update(task_id_map[t] for t in node.get("on_failure_node_ids", []))

    merge_task_ids = {tid for tid, parents in upstream_map.items() if len(parents) > 1}

    # node_order_map: node_id -> executor_order_id
    node_order_map: Dict[str, int] = {node["id"]: node["executor_order_id"] for node in all_nodes}

    # upstream_node_ids_map: node_id -> list of upstream node_ids (for merge guard)
    # Only includes sync/async nodes (not fire_and_forget)
    upstream_node_ids_map: Dict[str, List[str]] = {}
    for node in all_nodes:
        nid = node["id"]
        tid = task_id_map[nid]
        if tid in merge_task_ids:
            upstream_nids = []
            for parent_tid in upstream_map[tid]:
                parent_nid = task_id_to_node_id.get(parent_tid)
                if parent_nid and parent_nid not in fire_and_forget_node_ids:
                    upstream_nids.append(parent_nid)
            upstream_node_ids_map[nid] = upstream_nids


    task_def_blocks: List[str] = []
    for node in all_nodes:
        node_id = node["id"]
        tid = task_id_map[node_id]
        py_var = var_map[node_id]
        order_id = node["executor_order_id"]

        # FIX #8: merge nodes use NONE_FAILED_MIN_ONE_SUCCESS
        # This allows skipped branch nodes to not block the merge
        if tid in merge_task_ids:
            trigger_rule = "TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS"
        else:
            trigger_rule = "TriggerRule.ALL_SUCCESS"

        success_tids = [task_id_map[t] for t in node.get("on_success_node_ids", [])]
        failure_tids = [task_id_map[t] for t in node.get("on_failure_node_ids", [])]

        node_name_val = node["name"]
        node_engine_val = node["engine"]
        node_mode_val = node["execution_mode"]
        doc_md_str = (
            f"**{node_name_val}**  \\n"
            f"engine: `{node_engine_val}`  \\n"
            f"mode: `{node_mode_val}`  \\n"
            f"order: `{order_id}`"
        )

        # upstream guard list for this merge node
        upstream_guard = upstream_node_ids_map.get(node_id, [])

        block = (
                        f"{py_var} = PythonOperator(\n"
            f"        task_id={tid!r},\n"
            f"        python_callable=execute_node,\n"
            f"        op_kwargs={{\n"
            f"            \"task_key\": {node_id!r},\n"
            f"            \"node_id\": {node_id!r},\n"
            f"            \"node_name\": {node['name']!r},\n"
            f"            \"engine\": {node['engine']!r},\n"
            f"            \"execution_mode\": {node['execution_mode']!r},\n"
            f"            \"executor_order_id\": {node['executor_order_id']!r},\n"
            f"            \"upstream_guard_node_ids\": {upstream_guard!r},\n"
            f"        }},\n"
            f"        trigger_rule={trigger_rule},\n"
            f"        doc_md={doc_md_str!r},\n"
            f"    )"
        )
        task_def_blocks.append(block)

        if node.get("branch_on_status"):
            branch_tid = branch_task_ids[node_id]
            branch_var = branch_var_map[branch_tid]
            branch_doc_md = f"Branch router for **{node_name_val}**"
            b_block = (
                                f"{branch_var} = BranchPythonOperator(\n"
                f"        task_id={branch_tid!r},\n"
                f"        python_callable=choose_branch,\n"
                f"        op_kwargs={{\n"
                f"            \"node_task_id\": {tid!r},\n"
                f"            \"node_id\": {node_id!r},\n"
                f"            \"success_task_ids\": {success_tids!r},\n"
                f"            \"failure_task_ids\": {failure_tids!r},\n"
                f"        }},\n"
                f"        trigger_rule=TriggerRule.ALL_DONE,\n"
                f"        doc_md={branch_doc_md!r},\n"
                f"    )"
            )
            task_def_blocks.append(b_block)

    # Wire all nodes -> finalize_results -> run_final_event
    for nid in node_ids:
        deduped_deps.append((task_id_map[nid], "finalize_results"))
    deduped_deps.append(("finalize_results", "run_final_event"))

    special_task_vars = {
        "prepare_inputs": "prepare_inputs_task",
        "run_started_event": "run_started_event",
        "finalize_results": "finalize_results_task",
        "run_final_event": "run_final_event",
    }

    def to_var(task_ref: str) -> str:
        if task_ref in special_task_vars:
            return special_task_vars[task_ref]
        for nid, tid in task_id_map.items():
            if tid == task_ref:
                return var_map[nid]
        if task_ref in branch_var_map:
            return branch_var_map[task_ref]
        return python_var_safe(task_ref)

    dep_lines = [f"{to_var(parent)} >> {to_var(child)}" for parent, child in deduped_deps]

    indented_task_defs = "\n\n".join(
        _indent_block(block, _TASK_INDENT) for block in task_def_blocks
    )
    indented_deps = ("\n" + _TASK_INDENT).join(dep_lines)

    schedule_str = repr(schedule) if schedule else "None"
    node_order_map_repr = repr(node_order_map)
    task_id_map_repr = repr(task_id_map)
    task_id_to_node_id_repr = repr(task_id_to_node_id)

    template = f"""# Generated by dynamic-dag-service — airflow_job1 v1 — Airflow 3.0.6
# run_control_id: {repr(run_control_id)}
# dag_id: {repr(dag_id)}

from datetime import datetime, timezone
from pathlib import Path
from contextlib import contextmanager
import hashlib
import json
import logging
import os
import time
import requests

try:
    from tenacity import (
        retry,
        retry_if_exception_type,
        stop_after_attempt,
        wait_exponential,
        RetryError,
    )
    _TENACITY_AVAILABLE = True
except ImportError:
    _TENACITY_AVAILABLE = False

try:
    import portalocker
except ImportError:
    portalocker = None

from airflow.sdk import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator,
    get_current_context,
)
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger("airflow.task")

# ---------------------------------------------------------------------------
# DAG-level constants (baked in at build time)
# ---------------------------------------------------------------------------

KAFKA_CONN_ID = {repr(KAFKA_CONN_ID)}
KAFKA_RUN_TOPIC = {repr(KAFKA_RUN_TOPIC)}
RUN_CONTROL_ID = {repr(run_control_id)}
FINAL_NODE_IDS = {repr(node_ids)}
ASYNC_NODE_IDS = {repr(async_node_ids)}
FIRE_AND_FORGET_NODE_IDS = {repr(fire_and_forget_node_ids)}
NODE_NAME_MAP = {repr(node_name_map)}
BRANCH_SKIP_WHITELIST = {repr(sorted(branch_skip_whitelist))}
MERGE_NODE_IDS = {repr(sorted(merge_task_ids))}
NODE_ORDER_MAP = {node_order_map_repr}
TASK_ID_MAP = {task_id_map_repr}
TASK_ID_TO_NODE_ID = {task_id_to_node_id_repr}
RUNTIME_REGISTRY_FILE = Path(
    os.getenv("RUNTIME_IDEMPOTENCY_REGISTRY", "/tmp/dynamic_dag_runtime_registry.json")
)
RUNTIME_REGISTRY_FILE.parent.mkdir(parents=True, exist_ok=True)

HTTP_SUCCESS_CODES = (200, 201, 202, 204)

# Retry configuration — controls how HTTP submit calls are retried on transient failures.
# 401/403/404/422 are NOT retried (they are caller errors, retrying won't help).
# 5xx, connection errors, and timeouts ARE retried with exponential backoff.
HTTP_RETRY_ATTEMPTS = int(os.getenv("HTTP_RETRY_ATTEMPTS", "3"))
HTTP_RETRY_MIN_WAIT = float(os.getenv("HTTP_RETRY_MIN_WAIT_SECONDS", "2"))
HTTP_RETRY_MAX_WAIT = float(os.getenv("HTTP_RETRY_MAX_WAIT_SECONDS", "10"))
HTTP_NO_RETRY_CODES = (400, 401, 403, 404, 405, 422)  # caller errors — retrying won't fix these

DEFAULT_ASYNC_SUCCESS_STATUSES = {{"SUCCESS", "SUCCEEDED", "COMPLETED", "DONE", "FINISHED"}}
DEFAULT_ASYNC_FAILURE_STATUSES = {{"FAILED", "FAILURE", "ERROR", "ERRORED", "CANCELLED", "CANCELED", "ABORTED"}}
DEFAULT_ASYNC_RUNNING_STATUSES = {{"RUNNING", "IN_PROGRESS", "PENDING", "QUEUED", "SUBMITTED", "PROCESSING", "STARTED"}}


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------


def _utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_json(value):
    return json.dumps(value, default=str)


def _canonicalize_value(value):
    if isinstance(value, dict):
        return {{str(k): _canonicalize_value(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}}
    if isinstance(value, (list, tuple)):
        return [_canonicalize_value(v) for v in value]
    if isinstance(value, str):
        return value.strip()
    return value


def _sha256(value):
    raw = json.dumps(_canonicalize_value(value), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Runtime registry — FIX #6: portalocker missing raises error, not silent skip
# FIX #7: contextmanager ensures lock release even on exception
# ---------------------------------------------------------------------------


@contextmanager
def _runtime_registry_lock():
    \"\"\"FIX #7: context manager so lock is always released.\"\"\"
    if portalocker is None:
        raise RuntimeError(
            "portalocker is required for runtime registry locking but is not installed. "
            "Install it with: pip install portalocker"
        )
    lock_path = RUNTIME_REGISTRY_FILE.with_suffix(".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        portalocker.lock(lock_file, portalocker.LOCK_EX)
        try:
            yield
        finally:
            portalocker.unlock(lock_file)


def _load_runtime_registry():
    if not RUNTIME_REGISTRY_FILE.exists():
        return {{}}
    try:
        with RUNTIME_REGISTRY_FILE.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
            return data if isinstance(data, dict) else {{}}
    except Exception:
        log.warning("Failed to load runtime registry %s; using empty registry", RUNTIME_REGISTRY_FILE)
        return {{}}


def _write_runtime_registry(registry):
    tmp_path = RUNTIME_REGISTRY_FILE.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(registry, handle, indent=2, sort_keys=True, default=str)
    tmp_path.replace(RUNTIME_REGISTRY_FILE)


def _get_runtime_entry(node_key):
    with _runtime_registry_lock():
        registry = _load_runtime_registry()
        entry = registry.get(node_key)
        return entry if isinstance(entry, dict) else None


def _store_runtime_entry(node_key, entry):
    with _runtime_registry_lock():
        registry = _load_runtime_registry()
        registry[node_key] = entry
        _write_runtime_registry(registry)


def _mark_node_from_registry(ti, node_id, task_id, entry):
    ti.xcom_push(key=f"{{node_id}}_task_state", value="success")
    ti.xcom_push(key=f"{{node_id}}_branch", value="success")
    if "tracking_id" in entry:
        ti.xcom_push(key=f"{{node_id}}_tracking_id", value=entry.get("tracking_id"))
    if "submit_response" in entry:
        ti.xcom_push(key=f"{{node_id}}_submit_response", value=entry.get("submit_response"))
    if "result" in entry:
        ti.xcom_push(key=f"{{node_id}}_response", value=entry.get("result"))
    return {{
        "status": "idempotent_reused",
        "node_id": node_id,
        "previous_state": entry.get("state"),
        "result": entry.get("result"),
    }}


def _make_node_idempotency_key(ctx, node_id, payload):
    base = {{
        "dag_id": ctx["dag"].dag_id,
        "run_control_id": RUN_CONTROL_ID,
        "correlation_id": _get_correlation_id(ctx),
        "node_id": node_id,
        "payload_hash": _sha256(payload),
    }}
    return _sha256(base)


def _should_force_rerun(conf, node_id):
    force_nodes = conf.get("force_rerun_nodes") or []
    if isinstance(force_nodes, str):
        force_nodes = [item.strip() for item in force_nodes.split(",") if item.strip()]
    return bool(conf.get("force_rerun")) or node_id in set(force_nodes)


def _get_conf(ctx):
    return ctx["dag_run"].conf or {{}}


def _get_correlation_id(ctx):
    conf = _get_conf(ctx)
    return (
        conf.get("correlation_id")
        or conf.get("run_control_id")
        or ctx["dag_run"].run_id
        or ctx["dag_run"].external_trigger_id
    )


def _to_upper_set(values, fallback):
    if not values:
        return set(fallback)
    return {{str(v).strip().upper() for v in values if str(v).strip()}}


def _extract_by_path(data, path, default=None):
    if not path:
        return data if data is not None else default
    current = data
    for part in str(path).split("."):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except Exception:
                return default
        else:
            return default
        if current is None:
            return default
    return current


def _render_value(value, replacements):
    if isinstance(value, str):
        try:
            return value.format(**replacements)
        except Exception:
            return value
    if isinstance(value, dict):
        return {{k: _render_value(v, replacements) for k, v in value.items()}}
    if isinstance(value, list):
        return [_render_value(v, replacements) for v in value]
    return value


def _parse_response_body(response):
    if not response.text:
        return {{}}
    try:
        return response.json()
    except Exception:
        return {{"raw_response": response.text}}


def _normalize_terminal_status(status_value, success_statuses, failure_statuses, running_statuses):
    status_text = str(status_value or "UNKNOWN").strip()
    status_upper = status_text.upper()
    if status_upper in success_statuses:
        return "SUCCESS", status_text
    if status_upper in failure_statuses:
        return "FAILED", status_text
    if status_upper in running_statuses:
        return "RUNNING", status_text
    return "UNKNOWN", status_text


# ---------------------------------------------------------------------------
# Async polling — FIX #5 (async_no_wait is actually a blocking poll)
# The node submits a job, extracts tracking_id from the response,
# then polls a status endpoint until SUCCESS/FAILED/TIMEOUT.
# The task occupies an Airflow worker slot for the full duration.
# ---------------------------------------------------------------------------


def _http_request_with_retry(
    method, url, headers=None, params=None, json_body=None,
    timeout=300, verify=False, node_id="unknown", call_label="submit"
):
    \"\"\"
    FIX B: Wraps requests.request() with retry logic using tenacity.

    Retries on:
      - requests.exceptions.ConnectionError  (network blip)
      - requests.exceptions.Timeout          (upstream too slow)
      - requests.exceptions.ChunkedEncodingError
      - HTTP 500/502/503/504/429 responses (transient server errors)

    Does NOT retry on:
      - HTTP 400/401/403/404/422 (caller errors - retrying these will not help)
      - requests.exceptions.SSLError (cert problems won't self-heal)
      - requests.exceptions.TooManyRedirects
    \"\"\"
    TRANSIENT_HTTP_ERRORS = {{500, 502, 503, 504, 429}}

    def _do_request():
        resp = requests.request(
            method=str(method).upper(),
            url=url,
            headers=headers or {{}},
            params=params,
            json=json_body,
            timeout=timeout,
            verify=verify,
        )
        # Raise a retryable error for transient HTTP codes so tenacity catches it
        if resp.status_code in TRANSIENT_HTTP_ERRORS:
            raise requests.exceptions.ConnectionError(
                f"Node {{node_id}} [{{call_label}}]: HTTP {{resp.status_code}} (transient) - will retry"
            )
        return resp

    if not _TENACITY_AVAILABLE:
        log.warning(
            "tenacity not installed — retries disabled for node %s [%s]. "
            "Install with: pip install tenacity",
            node_id, call_label,
        )
        return _do_request()

    retry_decorator = retry(
        retry=retry_if_exception_type((
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        )),
        stop=stop_after_attempt(HTTP_RETRY_ATTEMPTS),
        wait=wait_exponential(min=HTTP_RETRY_MIN_WAIT, max=HTTP_RETRY_MAX_WAIT),
        reraise=True,
    )

    try:
        return retry_decorator(_do_request)()
    except RetryError as exc:
        raise AirflowException(
            f"Node {{node_id}} [{{call_label}}]: all {{HTTP_RETRY_ATTEMPTS}} retry attempts exhausted. "
            f"Last error: {{exc.last_attempt.exception()}}"
        ) from exc


def _poll_status_until_terminal(node_id, node_name, payload, start_response_body, tracking_id):
    ctx = get_current_context()
    status_cfg = payload.get("status") or {{}}
    if not isinstance(status_cfg, dict) or not status_cfg:
        raise AirflowException(
            f"Node {{node_id}} ({{node_name}}) is async_no_wait but has no 'status' block in conf. "
            f"A 'status' block with at minimum 'url' and 'response_status_key' is required."
        )

    response_id_key = status_cfg.get("response_id_key") or payload.get("response_id_key") or "job_id"

    # FIX: tracking_id resolution priority (explicit > response body fields)
    if not tracking_id:
        tracking_id = (
            _extract_by_path(start_response_body, response_id_key)
            or _extract_by_path(start_response_body, "run_id")
            or _extract_by_path(start_response_body, "runId")
            or _extract_by_path(start_response_body, "id")
            or _extract_by_path(start_response_body, "jobId")
            or _extract_by_path(start_response_body, "request_id")
        )

    if not tracking_id:
        raise AirflowException(
            f"Node {{node_id}}: could not extract tracking_id from submit response. "
            f"Tried key '{{response_id_key}}' and fallback keys (run_id, runId, id, jobId, request_id). "
            f"Submit response was: {{start_response_body}}"
        )

    replacements = {{
        "tracking_id": tracking_id,
        "job_id": tracking_id,
        "run_id": tracking_id,
        "runId": tracking_id,
        "node_runId": tracking_id,
        "node_run_id": tracking_id,
        "node_id": node_id,
        "node_name": node_name,
        "dag_id": ctx["dag"].dag_id,
        "dag_run_id": ctx["dag_run"].run_id,
        "run_control_id": RUN_CONTROL_ID,
    }}

    status_url = _render_value(status_cfg.get("url"), replacements)
    if not status_url:
        raise AirflowException(
            f"Node {{node_id}}: 'status.url' is required for async_no_wait nodes."
        )

    method = str(status_cfg.get("method") or "GET").upper()
    headers = _render_value(status_cfg.get("headers") or payload.get("headers") or {{}}, replacements)
    params = _render_value(status_cfg.get("params") or {{}}, replacements)
    json_body = _render_value(status_cfg.get("json") or None, replacements)
    verify_ssl = status_cfg.get("verify_ssl", payload.get("verify_ssl", False))
    request_timeout = int(status_cfg.get("request_timeout", payload.get("timeout", 300)))
    poke_interval = float(status_cfg.get("poke_interval", status_cfg.get("poke_interval_seconds", 10)))
    timeout_seconds = float(status_cfg.get("timeout", 1800))

    response_status_key = status_cfg.get("response_status_key") or "status"
    success_statuses = _to_upper_set(status_cfg.get("success_statuses"), DEFAULT_ASYNC_SUCCESS_STATUSES)
    failure_statuses = _to_upper_set(status_cfg.get("failure_statuses"), DEFAULT_ASYNC_FAILURE_STATUSES)
    running_statuses = _to_upper_set(status_cfg.get("running_statuses"), DEFAULT_ASYNC_RUNNING_STATUSES)

    deadline = time.time() + timeout_seconds
    last_status_response = {{}}
    poll_count = 0

    log.info(
        "[ASYNC] Node %s: starting poll. tracking_id=%s url=%s poke_interval=%ss timeout=%ss",
        node_id, tracking_id, status_url, poke_interval, timeout_seconds,
    )

    while time.time() <= deadline:
        poll_count += 1
        try:
            response = _http_request_with_retry(
                method=method,
                url=status_url,
                headers=headers,
                params=params,
                json_body=json_body,
                timeout=request_timeout,
                verify=verify_ssl,
                node_id=node_id,
                call_label=f"poll_{{poll_count}}",
            )
            status_body = _parse_response_body(response)
            last_status_response = status_body
        except AirflowException:
            raise
        except Exception as exc:
            raise AirflowException(
                f"Node {{node_id}}: status poll #{{poll_count}} failed after retries: {{exc}}"
            ) from exc

        if response.status_code in (401, 403):
            raise AirflowException(
                f"Node {{node_id}}: status endpoint returned HTTP {{response.status_code}}. "
                f"Check Authorization headers in 'status' block."
            )
        if response.status_code == 404:
            raise AirflowException(
                f"Node {{node_id}}: status endpoint returned 404. "
                f"tracking_id={{tracking_id}} may be invalid or expired."
            )
        if response.status_code not in HTTP_SUCCESS_CODES:
            raise AirflowException(
                f"Node {{node_id}}: status endpoint HTTP {{response.status_code}}: {{response.text}}"
            )

        raw_status = _extract_by_path(status_body, response_status_key)
        normalized_status, status_text = _normalize_terminal_status(
            raw_status, success_statuses, failure_statuses, running_statuses
        )

        log.info(
            "[ASYNC] Node %s poll #%d: status_field=%s raw=%s normalized=%s",
            node_id, poll_count, response_status_key, status_text, normalized_status,
        )

        if normalized_status == "SUCCESS":
            return {{
                "status": "completed",
                "node_id": node_id,
                "tracking_id": tracking_id,
                "poll_count": poll_count,
                "external_status": status_text,
                "start_response": start_response_body,
                "status_response": status_body,
            }}
        if normalized_status == "FAILED":
            raise AirflowException(
                f"Node {{node_id}}: external job failed. "
                f"status_field={{response_status_key}}, value={{status_text}}, "
                f"tracking_id={{tracking_id}}"
            )

        time.sleep(poke_interval)

    raise AirflowException(
        f"Node {{node_id}}: status polling timed out after {{timeout_seconds}}s ({{poll_count}} polls). "
        f"tracking_id={{tracking_id}}, last_response={{last_status_response}}",
    )


# ---------------------------------------------------------------------------
# Payload resolution
# ---------------------------------------------------------------------------


def resolve_payload(conf, node_id, task_key=None):
    \"\"\"Resolve node conf from dag_run.conf. Key = node_id (exact match).\"\"\"
    direct = conf.get(node_id)
    if isinstance(direct, dict):
        return direct
    if task_key and task_key != node_id:
        keyed = conf.get(task_key)
        if isinstance(keyed, dict):
            return keyed
    # Fallback: search by node_runId field
    for _, value in conf.items():
        if isinstance(value, dict) and str(value.get("node_runId", "")).strip() == node_id:
            return value
    return {{}}


# ---------------------------------------------------------------------------
# Merge guard — FIX #2/#8
# Runs at the start of any merge node to verify all sync/async upstreams
# have succeeded (or were legitimately skipped via branching).
# fire_and_forget upstreams are excluded from this check.
# ---------------------------------------------------------------------------


def _check_merge_guard(ti, node_id, upstream_guard_node_ids):
    \"\"\"
    For each upstream sync/async node, verify XCom task_state = 'success'
    OR the node is in BRANCH_SKIP_WHITELIST (expected branch skip).
    Raises AirflowException if any upstream is in an unexpected state.
    \"\"\"
    if not upstream_guard_node_ids:
        return  # Not a merge node or no guarded upstreams

    problems = []
    for upstream_nid in upstream_guard_node_ids:
        upstream_tid = TASK_ID_MAP.get(upstream_nid, upstream_nid)
        marker = ti.xcom_pull(task_ids=upstream_tid, key=f"{{upstream_nid}}_task_state")
        marker = str(marker).lower().strip() if marker is not None else ""

        if marker == "success":
            continue  # All good

        if not marker:
            # Check if this is an expected branch skip
            if upstream_tid in BRANCH_SKIP_WHITELIST:
                log.info(
                    "[MERGE_GUARD] Node %s: upstream %s has no XCom but is in branch skip whitelist — OK",
                    node_id, upstream_nid,
                )
                continue
            else:
                problems.append(
                    f"upstream {{upstream_nid}} (task={{upstream_tid}}) has no task_state XCom "
                    f"and is not in branch skip whitelist — unexpected state"
                )
        elif marker == "failed":
            problems.append(f"upstream {{upstream_nid}} (task={{upstream_tid}}) state=failed")
        else:
            problems.append(
                f"upstream {{upstream_nid}} (task={{upstream_tid}}) has unexpected state={{marker}}"
            )

    if problems:
        raise AirflowException(
            f"Merge guard FAILED for node {{node_id}}. "
            f"The following upstream nodes did not complete successfully: "
            + "; ".join(problems)
        )

    log.info("[MERGE_GUARD] Node %s: all %d upstream(s) verified OK", node_id, len(upstream_guard_node_ids))


# ---------------------------------------------------------------------------
# execute_node — main task callable
# ---------------------------------------------------------------------------


def execute_node(
    task_key,
    node_id,
    node_name,
    engine,
    execution_mode,
    executor_order_id,
    upstream_guard_node_ids=None,
    **kwargs,
):
    ctx = get_current_context()
    conf = _get_conf(ctx)
    ti = ctx["ti"]
    task_id = TASK_ID_MAP.get(node_id, node_id)

    # FIX #8: run merge guard before any other logic
    if upstream_guard_node_ids:
        _check_merge_guard(ti, node_id, upstream_guard_node_ids)

    payload = resolve_payload(conf, node_id=node_id, task_key=task_key)

    # FIX #4: Resume — validate resume_from before acting on it
    if conf.get("resume") and not _should_force_rerun(conf, node_id):
        resume_from = conf.get("resume_from")
        if resume_from:
            if resume_from not in NODE_ORDER_MAP:
                raise AirflowException(
                    f"[RESUME] resume_from='{{resume_from}}' is not a valid node ID. "
                    f"Valid node IDs: {{sorted(NODE_ORDER_MAP.keys())}}"
                )
            resume_order = NODE_ORDER_MAP[resume_from]
            if executor_order_id < resume_order:
                log.info(
                    "[RESUME] Skipping node %s (order=%s) — resume_from=%s (order=%s)",
                    node_id, executor_order_id, resume_from, resume_order,
                )
                ti.xcom_push(key=f"{{node_id}}_task_state", value="success")
                ti.xcom_push(key=f"{{node_id}}_branch", value="success")
                return {{"status": "resume_skipped", "node_id": node_id}}

    if not payload or not payload.get("url"):
        ti.xcom_push(key=f"{{node_id}}_task_state", value="failed")
        ti.xcom_push(key=f"{{node_id}}_branch", value="failure")
        raise AirflowException(
            f"Node {{node_id}} ({{node_name}}): missing or empty 'url' in conf. "
            f"Expected conf key '{{node_id}}' with a 'url' field."
        )

    node_key = _make_node_idempotency_key(ctx, node_id, payload)
    existing_entry = _get_runtime_entry(node_key)
    if existing_entry and existing_entry.get("state") == "success" and not _should_force_rerun(conf, node_id):
        log.info(
            "[IDEMPOTENCY] Reusing completed node %s for correlation_id=%s",
            node_id, _get_correlation_id(ctx),
        )
        return _mark_node_from_registry(ti, node_id, task_id, existing_entry)

    ti.xcom_push(key=f"{{node_id}}_task_state", value="started")
    ti.xcom_push(key=f"{{node_id}}_idempotency_key", value=node_key)
    _store_runtime_entry(
        node_key,
        {{
            "state": "started",
            "node_id": node_id,
            "node_name": node_name,
            "execution_mode": execution_mode,
            "dag_id": ctx["dag"].dag_id,
            "dag_run_id": ctx["dag_run"].run_id,
            "correlation_id": _get_correlation_id(ctx),
            "started_at": _utc_now(),
        }},
    )

    request_timeout = int(payload.get("timeout", 300))
    verify_ssl = payload.get("verify_ssl", False)

    try:
        response = _http_request_with_retry(
            method=str(payload.get("method", "POST")).upper(),
            url=payload["url"],
            json_body=payload.get("json"),
            params=payload.get("params"),
            headers=payload.get("headers", {{}}),
            timeout=request_timeout,
            verify=verify_ssl,
            node_id=node_id,
            call_label="submit",
        )
        response_body = _parse_response_body(response)
    except AirflowException:
        # Already formatted by _http_request_with_retry — just record and re-raise
        ti.xcom_push(key=f"{{node_id}}_task_state", value="failed")
        ti.xcom_push(key=f"{{node_id}}_branch", value="failure")
        _store_runtime_entry(
            node_key,
            {{
                "state": "failed",
                "node_id": node_id,
                "node_name": node_name,
                "execution_mode": execution_mode,
                "dag_id": ctx["dag"].dag_id,
                "dag_run_id": ctx["dag_run"].run_id,
                "correlation_id": _get_correlation_id(ctx),
                "error": "submit failed after retries — see task log",
                "finished_at": _utc_now(),
            }},
        )
        raise
    except Exception as exc:
        ti.xcom_push(key=f"{{node_id}}_task_state", value="failed")
        ti.xcom_push(key=f"{{node_id}}_branch", value="failure")
        ti.xcom_push(key=f"{{node_id}}_error", value=str(exc))
        _store_runtime_entry(
            node_key,
            {{
                "state": "failed",
                "node_id": node_id,
                "node_name": node_name,
                "execution_mode": execution_mode,
                "dag_id": ctx["dag"].dag_id,
                "dag_run_id": ctx["dag_run"].run_id,
                "correlation_id": _get_correlation_id(ctx),
                "error": str(exc),
                "finished_at": _utc_now(),
            }},
        )
        raise AirflowException(f"Node {{node_id}}: submit failed: {{exc}}") from exc

    if response.status_code in (401, 403):
        error_message = (
            f"Node {{node_id}}: HTTP {{response.status_code}}. "
            f"Check Authorization headers."
        )
        ti.xcom_push(key=f"{{node_id}}_task_state", value="failed")
        ti.xcom_push(key=f"{{node_id}}_branch", value="failure")
        ti.xcom_push(key=f"{{node_id}}_error", value=error_message)
        _store_runtime_entry(
            node_key,
            {{
                "state": "failed",
                "node_id": node_id,
                "node_name": node_name,
                "execution_mode": execution_mode,
                "dag_id": ctx["dag"].dag_id,
                "dag_run_id": ctx["dag_run"].run_id,
                "correlation_id": _get_correlation_id(ctx),
                "http_status": response.status_code,
                "error": error_message,
                "finished_at": _utc_now(),
            }},
        )
        raise AirflowException(error_message)

    if response.status_code not in HTTP_SUCCESS_CODES:
        error_message = f"Node {{node_id}}: HTTP {{response.status_code}}: {{response.text[:500]}}"
        ti.xcom_push(key=f"{{node_id}}_task_state", value="failed")
        ti.xcom_push(key=f"{{node_id}}_branch", value="failure")
        ti.xcom_push(key=f"{{node_id}}_error", value=error_message)
        _store_runtime_entry(
            node_key,
            {{
                "state": "failed",
                "node_id": node_id,
                "node_name": node_name,
                "execution_mode": execution_mode,
                "dag_id": ctx["dag"].dag_id,
                "dag_run_id": ctx["dag_run"].run_id,
                "correlation_id": _get_correlation_id(ctx),
                "http_status": response.status_code,
                "error": error_message,
                "response": response_body,
                "finished_at": _utc_now(),
            }},
        )
        raise AirflowException(error_message)

    # --- fire_and_forget ---
    if execution_mode == "fire_and_forget":
        result = {{"status": "submitted_no_track", "node_id": node_id, "submit_response": response_body}}
        ti.xcom_push(key=f"{{node_id}}_submit_response", value=response_body)
        ti.xcom_push(key=f"{{node_id}}_submit_http_status", value=response.status_code)
        ti.xcom_push(key=f"{{node_id}}_response", value=result)
        ti.xcom_push(key=f"{{node_id}}_task_state", value="success")
        ti.xcom_push(key=f"{{node_id}}_branch", value="success")
        _store_runtime_entry(
            node_key,
            {{
                "state": "success",
                "node_id": node_id,
                "node_name": node_name,
                "execution_mode": execution_mode,
                "dag_id": ctx["dag"].dag_id,
                "dag_run_id": ctx["dag_run"].run_id,
                "correlation_id": _get_correlation_id(ctx),
                "http_status": response.status_code,
                "submit_response": response_body,
                "result": result,
                "finished_at": _utc_now(),
            }},
        )
        return result

    # --- async_no_wait (blocking poll) ---
    if execution_mode == "async_no_wait":
        status_cfg = payload.get("status") or {{}}
        response_id_key = (
            status_cfg.get("response_id_key")
            or payload.get("response_id_key")
            or "job_id"
        )
        # tracking_id: explicit override in conf takes priority, then response body
        tracking_id = (
            status_cfg.get("tracking_id")
            or payload.get("tracking_id")
            or _extract_by_path(response_body, response_id_key)
            or _extract_by_path(response_body, "run_id")
            or _extract_by_path(response_body, "runId")
            or _extract_by_path(response_body, "id")
            or _extract_by_path(response_body, "jobId")
            or _extract_by_path(response_body, "request_id")
        )
        ti.xcom_push(key=f"{{node_id}}_submit_response", value=response_body)
        ti.xcom_push(key=f"{{node_id}}_tracking_id", value=tracking_id)
        ti.xcom_push(key=f"{{node_id}}_submit_http_status", value=response.status_code)

        log.info(
            "[ASYNC] Node %s: submitted. tracking_id=%s. Now polling for terminal state.",
            node_id, tracking_id,
        )

        try:
            result = _poll_status_until_terminal(node_id, node_name, payload, response_body, tracking_id)
        except Exception as exc:
            ti.xcom_push(key=f"{{node_id}}_task_state", value="failed")
            ti.xcom_push(key=f"{{node_id}}_branch", value="failure")
            ti.xcom_push(key=f"{{node_id}}_error", value=str(exc))
            _store_runtime_entry(
                node_key,
                {{
                    "state": "failed",
                    "node_id": node_id,
                    "node_name": node_name,
                    "execution_mode": execution_mode,
                    "dag_id": ctx["dag"].dag_id,
                    "dag_run_id": ctx["dag_run"].run_id,
                    "correlation_id": _get_correlation_id(ctx),
                    "tracking_id": tracking_id,
                    "submit_response": response_body,
                    "error": str(exc),
                    "finished_at": _utc_now(),
                }},
            )
            raise

        ti.xcom_push(key=f"{{node_id}}_response", value=result)
        ti.xcom_push(key=f"{{node_id}}_task_state", value="success")
        ti.xcom_push(key=f"{{node_id}}_branch", value="success")
        _store_runtime_entry(
            node_key,
            {{
                "state": "success",
                "node_id": node_id,
                "node_name": node_name,
                "execution_mode": execution_mode,
                "dag_id": ctx["dag"].dag_id,
                "dag_run_id": ctx["dag_run"].run_id,
                "correlation_id": _get_correlation_id(ctx),
                "tracking_id": tracking_id,
                "http_status": response.status_code,
                "submit_response": response_body,
                "result": result,
                "finished_at": _utc_now(),
            }},
        )
        return result

    # --- sync ---
    result = response_body
    ti.xcom_push(key=f"{{node_id}}_response", value=result)
    ti.xcom_push(key=f"{{node_id}}_task_state", value="success")
    ti.xcom_push(key=f"{{node_id}}_branch", value="success")
    _store_runtime_entry(
        node_key,
        {{
            "state": "success",
            "node_id": node_id,
            "node_name": node_name,
            "execution_mode": execution_mode,
            "dag_id": ctx["dag"].dag_id,
            "dag_run_id": ctx["dag_run"].run_id,
            "correlation_id": _get_correlation_id(ctx),
            "http_status": response.status_code,
            "result": result,
            "finished_at": _utc_now(),
        }},
    )
    return result


# ---------------------------------------------------------------------------
# choose_branch — FIX #3: null XCom and missing targets handled cleanly
# ---------------------------------------------------------------------------


def choose_branch(node_task_id, node_id, success_task_ids, failure_task_ids, **kwargs):
    ctx = get_current_context()
    ti = ctx["ti"]
    marker = ti.xcom_pull(task_ids=node_task_id, key=f"{{node_id}}_branch")
    marker = str(marker).lower().strip() if marker is not None else None

    log.info(
        "[BRANCH] Node %s (task=%s): branch marker=%s success_targets=%s failure_targets=%s",
        node_id, node_task_id, marker, success_task_ids, failure_task_ids,
    )

    if marker == "success":
        if not success_task_ids:
            raise AirflowException(
                f"[BRANCH] Node {{node_id}}: branch marker=success but no on_success_node_ids configured. "
                f"This is a build payload error."
            )
        return success_task_ids

    # marker is None (XCom not set), empty, or "failure"
    if marker is None or marker == "":
        log.warning(
            "[BRANCH] Node %s (task=%s): branch marker is None/empty — treating as failure. "
            "This may indicate the node task did not complete normally.",
            node_id, node_task_id,
        )

    if not failure_task_ids:
        raise AirflowException(
            f"[BRANCH] Node {{node_id}}: branch marker={{marker}} (failure/unknown) "
            f"but no on_failure_node_ids configured. "
            f"Add on_failure_node_ids to this node in the build payload, "
            f"or check why the node did not set its branch XCom."
        )
    return failure_task_ids


# ---------------------------------------------------------------------------
# finalize_results — FIX #3: XCom missing defaults to FAILED with warning
# ---------------------------------------------------------------------------


def finalize_results(node_ids, **kwargs):
    ctx = get_current_context()
    conf = _get_conf(ctx)
    ti = ctx["ti"]
    ti.xcom_push(key="dag_terminal_state_seen", value=True)

    failed = []
    success = []
    expected_skipped = []
    unexpected_skipped = []
    running = []
    unknown = []

    for node_id in node_ids:
        node_label = NODE_NAME_MAP.get(node_id, node_id)
        airflow_task_id = TASK_ID_MAP.get(node_id, node_id)

        marker = ti.xcom_pull(task_ids=airflow_task_id, key=f"{{node_id}}_task_state")
        marker = str(marker).lower().strip() if marker is not None else ""

        if node_id in FIRE_AND_FORGET_NODE_IDS:
            entry = {{
                "task_id": airflow_task_id,
                "node_id": node_id,
                "name": node_label,
                "state": "submitted_no_track" if marker == "success" else (marker or "unknown"),
                "status_source": "airflow_submit_only",
            }}
            if marker == "success":
                success.append(entry)
            elif marker == "failed":
                failed.append(entry)
            elif not marker and node_id in BRANCH_SKIP_WHITELIST and node_id not in MERGE_NODE_IDS:
                expected_skipped.append({{**entry, "skip_type": "expected_branch_skip"}})
            else:
                unknown.append(entry)
            continue

        entry = {{
            "task_id": airflow_task_id,
            "node_id": node_id,
            "name": node_label,
            "state": marker or "unknown",
            "status_source": "xcom_task_state_marker",
        }}

        if marker == "success":
            success.append(entry)
        elif marker == "failed":
            failed.append(entry)
        elif not marker:
            if node_id in BRANCH_SKIP_WHITELIST and node_id not in MERGE_NODE_IDS:
                expected_skipped.append({{**entry, "skip_type": "expected_branch_skip"}})
            else:
                # FIX #3: log warning instead of silently treating as unknown
                log.warning(
                    "[FINALIZE] Node %s (task=%s) has no task_state XCom and is not in branch skip whitelist. "
                    "This may indicate the task was skipped unexpectedly or did not run.",
                    node_id, airflow_task_id,
                )
                unexpected_skipped.append({{**entry, "skip_type": "unexpected_no_xcom"}})
        else:
            unknown.append(entry)

    has_any_problem = bool(failed or unexpected_skipped or running or unknown)
    final_status = "FAILED" if has_any_problem else "SUCCESS"

    summary = {{
        "final_status": final_status,
        "successful_tasks": success,
        "failed_tasks": failed,
        "expected_skipped_tasks": expected_skipped,
        "unexpected_skipped_tasks": unexpected_skipped,
        "running_tasks": running,
        "unknown_tasks": unknown,
    }}

    # FIX #3: always push final_status so run_final_event can read it reliably
    ti.xcom_push(key="final_status", value=final_status)
    ti.xcom_push(key="final_summary", value=summary)

    if has_any_problem:
        raise AirflowException(
            f"Run completed with problems. "
            f"failed={{len(failed)}}, unexpected_skipped={{len(unexpected_skipped)}}, "
            f"running={{len(running)}}, unknown={{len(unknown)}}"
        )

    return summary


# ---------------------------------------------------------------------------
# Kafka event producers
# ---------------------------------------------------------------------------


def build_run_started_event_messages():
    ctx = get_current_context()
    dag_run = ctx["dag_run"]
    payload = {{
        "eventType": {repr(EVENTS["run_started"])},
        "run_control_id": RUN_CONTROL_ID,
        "correlation_id": _get_correlation_id(ctx),
        "event_source": "AIRFLOW",
        "status": "RUNNING",
        "trigger_payload": _safe_json(_get_conf(ctx)),
        "dagId": ctx["dag"].dag_id,
        "dagRunId": dag_run.run_id,
        "timestamp": _utc_now(),
    }}
    yield (dag_run.run_id, _safe_json(payload).encode())


def build_run_final_event_messages():
    ctx = get_current_context()
    dag_run = ctx["dag_run"]
    ti = ctx["ti"]
    # FIX #3: if finalize_results XCom is missing, default to FAILED with warning
    status_value = ti.xcom_pull(task_ids="finalize_results", key="final_status")
    if status_value is None:
        log.warning(
            "[KAFKA] final_status XCom not found from finalize_results task. "
            "Defaulting to FAILED. This may mean finalize_results itself failed."
        )
        status_value = "FAILED"
    payload = {{
        "eventType": {repr(EVENTS["run_succeeded"])} if status_value == "SUCCESS" else {repr(EVENTS["run_failed"])},
        "run_control_id": RUN_CONTROL_ID,
        "correlation_id": _get_correlation_id(ctx),
        "event_source": "AIRFLOW",
        "status": status_value,
        "trigger_payload": _safe_json(_get_conf(ctx)),
        "dagId": ctx["dag"].dag_id,
        "dagRunId": dag_run.run_id,
        "timestamp": _utc_now(),
    }}
    yield (dag_run.run_id, _safe_json(payload).encode())


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------


with DAG(
    dag_id={repr(dag_id)},
    schedule={schedule_str},
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["dynamic", "generated", "v1", "airflow-3.0.6"],
    doc_md=f"Dynamic DAG — run_control_id: {repr(run_control_id)} | nodes: {repr(len(all_nodes))}",  # baked at build time
) as dag:

    prepare_inputs_task = PythonOperator(
        task_id="prepare_inputs",
        python_callable=lambda: True,
        doc_md="Entry sentinel task — ensures DAG context is initialized before Kafka event.",
    )

    run_started_event = ProduceToTopicOperator(
        task_id="run_started_event",
        kafka_config_id=KAFKA_CONN_ID,
        topic=KAFKA_RUN_TOPIC,
        producer_function=build_run_started_event_messages,
    )

{indented_task_defs}

    finalize_results_task = PythonOperator(
        task_id="finalize_results",
        python_callable=finalize_results,
        op_kwargs={{"node_ids": FINAL_NODE_IDS}},
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
        doc_md="Aggregates XCom state from all nodes. Fails DAG if any unexpected issues found.",
    )

    run_final_event = ProduceToTopicOperator(
        task_id="run_final_event",
        kafka_config_id=KAFKA_CONN_ID,
        topic=KAFKA_RUN_TOPIC,
        producer_function=build_run_final_event_messages,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    prepare_inputs_task >> run_started_event
    {indented_deps}
"""

    return template


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "UP", "version": app.version}


@app.post("/build_dag", status_code=status.HTTP_201_CREATED)
def build_dag(payload: BuildDagPayload) -> Dict[str, Any]:
    dag_id = f"{sanitize_identifier(payload.run_control_id)}_dag"

    canonical_build_payload = canonicalize_build_payload(payload)
    idempotency_key = compute_sha256(canonical_build_payload)

    existing_entry = get_existing_registry_entry(idempotency_key)
    if existing_entry:
        logger.info("Idempotent build hit for dag_id=%s", existing_entry.get("dag_id"))
        return {
            "status": "SUCCESS",
            "dag_id": existing_entry["dag_id"],
            "file": existing_entry["file"],
            "path": existing_entry["path"],
            "idempotency_key": idempotency_key,
            "idempotent_reused": True,
        }

    # FIX A: No timestamp in filename — one file per dag_id, always overwritten.
    # Prevents DAG explosion: previously every build created a new timestamped file
    # causing Airflow scheduler to parse N files for the same dag_id on every scan cycle.
    # Stale timestamped files from old versions are cleaned up automatically.
    dag_file_name = f"{dag_id}.py"
    dag_file_path = DAGS_FOLDER / dag_file_name

    # Remove any stale timestamped files for this dag_id left from previous service versions
    for stale in DAGS_FOLDER.glob(f"{dag_id}_2*.py"):
        try:
            stale.unlink()
            logger.info("Removed stale timestamped DAG file: %s", stale.name)
        except Exception as exc:
            logger.warning("Could not remove stale DAG file %s: %s", stale.name, exc)

    # FIX C: cycle detection — runs after all structural validations pass
    try:
        detect_cycles(payload.nodes)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        )

    layers = build_layers(payload.nodes)
    code = generate_dag_code(
        dag_id=dag_id,
        run_control_id=payload.run_control_id,
        sorted_layers=layers,
        schedule=payload.schedule,
    )

    validate_generated_python(code)
    atomic_write_text(dag_file_path, code)

    entry = {
        "dag_id": dag_id,
        "file": dag_file_name,
        "path": str(dag_file_path.resolve()),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "payload_hash": idempotency_key,
        "node_count": len(payload.nodes),
        "airflow_version": "3.0.6",
    }

    try:
        store_registry_entry(idempotency_key, entry)
    except RuntimeError as exc:
        logger.error("DAG file written but registry update failed: %s", exc)
        return {
            "status": "SUCCESS",
            "dag_id": dag_id,
            "file": dag_file_name,
            "path": str(dag_file_path.resolve()),
            "idempotency_key": idempotency_key,
            "idempotent_reused": False,
            "warning": "Registry write failed — idempotency may not work on next call",
        }

    logger.info("DAG generated: %s (%d nodes)", dag_id, len(payload.nodes))

    return {
        "status": "SUCCESS",
        "dag_id": dag_id,
        "file": dag_file_name,
        "path": str(dag_file_path.resolve()),
        "idempotency_key": idempotency_key,
        "idempotent_reused": False,
    }


if __name__ == "__main__":
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8443"))
    uvicorn.run(app, host=host, port=port, log_level=LOG_LEVEL.lower())
