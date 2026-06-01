"""
Dynamic Airflow DAG Service  —  airflow_job1 v1.3
Target: Airflow 3.0.6  (dev)  /  3.1.8  (production)

What this service does
----------------------
This is a FastAPI application that lives on a separate VM from Airflow.
You send it a POST with a list of tasks (nodes), it figures out the wiring,
writes a ready-to-run Python DAG file to disk, and Airflow picks it up.

Nothing about actual endpoints or credentials goes in here at build time.
All of that travels with the run-time conf when someone triggers the DAG.

Two-phase flow
--------------
  BUILD  ->  POST /build_dag   ->  validates nodes, generates .py file, writes to AIRFLOW_DAGS_DIR
  RUN    ->  Airflow trigger    ->  DAG reads conf, executes tasks as HTTP calls, sends Kafka events

Key design decisions captured in this version
----------------------------------------------
  - executor_build_id  is the stable service key AND the run-time conf lookup key.
    When a task runs it calls  conf["EDM_Location_Inbound_NAS_TO_PVC"]  not  conf["task1"].

  - name  is what Airflow shows as the task label in the graph view.

  - id  (task1, task2 ...)  is purely internal for wiring.  It auto-assigns if you omit it.

  - No runtime idempotency registry on the Airflow VM.  Portalocker and the .lock file
    are gone from the generated DAG code.  Each task always runs fresh.

  - The completion node at the end of every DAG makes the overall run status reflect
    whether your business tasks actually succeeded, not whether Kafka fired OK.

  - Kafka status values: "Running", "Success", "Failure"  (title-cased, agreed convention).
"""

import hashlib
import json
import logging
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional, Set, Tuple

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
# FastAPI app + runtime constants
# ---------------------------------------------------------------------------

app = FastAPI(title="Dynamic Airflow DAG Service", version="1.3.0-airflow306")

# Where the generated .py files land.  Airflow watches this folder.
# On the FastAPI VM this should point at the git-synced folder or shared mount
# that the Airflow scheduler reads from.
DAGS_FOLDER = Path(os.getenv("AIRFLOW_DAGS_DIR", "./dag_configs"))
DAGS_FOLDER.mkdir(parents=True, exist_ok=True)

# These two values get baked into every generated DAG file at build time.
# They must match the Airflow connection you created in Admin -> Connections.
KAFKA_CONN_ID = os.getenv("KAFKA_CONN_ID", "genesis_kafka_conn")
KAFKA_RUN_TOPIC = os.getenv("KAFKA_RUN_TOPIC", "genesis.hub.run.events.v1")

# The three execution behaviours a node can have.
# sync           -> call the URL and wait for the response before moving on.
# async_no_wait  -> submit the job, then poll a status URL until it finishes.
# fire_and_forget-> submit the job and immediately mark the task as done regardless of outcome.
SUPPORTED_EXECUTION_MODES = {"sync", "async_no_wait", "fire_and_forget"}

# O = on-demand (get the payload from UI and job triggered through Kafka event)
# S = scheduled (needs a cron expression in the schedule field)
# F = ?
# D = ?
# R = ?
SUPPORTED_TRIGGER_TYPES = {"O", "S", "F", "D", "R"}
TRIGGER_TYPE_MAPPING = {"0": "O", "1": "S", "2": "F", "3": "D", "4": "R"}

# Kafka event type strings baked into every event the DAG emits.
EVENTS = {
    "run_started": "run.started.v1",
    "run_succeeded": "run.succeeded.v1",
    "run_failed": "run.failed.v1",
}

# The build-time idempotency registry lives only on the FastAPI VM.
# It is a plain JSON file that records which build payloads have already been processed
# so identical payloads don't overwrite a perfectly good DAG file on every call.
REGISTRY_FILE = Path(
    os.getenv("BUILD_IDEMPOTENCY_REGISTRY", str(DAGS_FOLDER / "build_registry.json"))
)
REGISTRY_FILE.parent.mkdir(parents=True, exist_ok=True)

# In-process lock that serialises concurrent /build_dag calls hitting the registry.
# A threading.Lock() is enough here because this is a single uvicorn process.
# There is no .lock file on disk — that was removed along with portalocker.
_REGISTRY_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class Node(BaseModel):
    # --- SCHEMA (v1.3) ---
    #
    # Three fields, three different jobs:
    #
    #   id               — internal reference used for DAG wiring only.
    #                      Optional in the build payload. If you leave it out,
    #                      the service assigns task1, task2, task3 ... in order.
    #                      This is what on_success_node_ids / on_failure_node_ids
    #                      reference. Never appears in the Airflow UI.
    #
    #   name             — the Airflow task_id. This is what you see as the node
    #                      label in the Airflow graph view. Does not need to be
    #                      unique — if the same name appears in multiple layers
    #                      the service appends _{order}_{seq} automatically.
    #
    #   executor_build_id — the stable service key. This is the key the task uses
    #                       to look up its payload in dag_run.conf at run time.
    #                       e.g. "EDM_Location_Inbound_NAS_TO_PVC".
    #                       Required — must be provided in every node.
    id: str = Field(default="", min_length=0)
    executor_build_id: str = Field(min_length=1)
    engine: str = Field(min_length=1)
    name: str = Field(min_length=1)
    executor_order_id: int = Field(ge=1)
    executor_sequence_id: int = Field(ge=1)
    execution_mode: str = Field(default="sync")
    branch_on_status: bool = False
    on_success_node_ids: List[str] = Field(default_factory=list)
    on_failure_node_ids: List[str] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    @field_validator("executor_build_id", "engine", "name", mode="before")
    @classmethod
    def normalize_string_fields(cls, value: Any) -> str:
        if value is None:
            return ""
        clean_value = str(value).strip()
        return clean_value

    @field_validator("id", mode="before")
    @classmethod
    def normalize_id(cls, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

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
    # Accept both `nodes` and `executionSteps` as field names (v1.2)
    nodes: List[Node] = Field(default_factory=list, alias="nodes")
    execution_steps: List[Node] = Field(default_factory=list, alias="executionSteps")

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

    @model_validator(mode="after")
    def merge_and_assign_ids(self) -> "BuildDagPayload":
        """
        v1.3: Merge executionSteps + nodes into a single list, then
        auto-assign `id` as task1, task2, … in sorted order
        (executor_order_id ASC, executor_sequence_id ASC).
        Preserves any explicit `id` supplied by the caller if it was non-empty.
        """
        combined = list(self.nodes) + list(self.execution_steps)
        if not combined:
            raise ValueError("At least one node must be provided in 'nodes' or 'executionSteps'")

        # Sort by position so auto-generated IDs are deterministic
        combined.sort(key=lambda n: (n.executor_order_id, n.executor_sequence_id))

        # Auto-assign ids where missing
        for idx, node in enumerate(combined, start=1):
            if not node.id:
                node.id = f"task{idx}"

        # Validate: IDs must be unique after assignment
        seen_ids: Set[str] = set()
        duplicate_ids: Set[str] = set()
        seen_sequence_pairs: Set[Tuple[int, int]] = set()
        duplicate_sequence_pairs: Set[Tuple[int, int]] = set()

        for node in combined:
            if node.id in seen_ids:
                duplicate_ids.add(node.id)
            seen_ids.add(node.id)

            seq_pair = (node.executor_order_id, node.executor_sequence_id)
            if seq_pair in seen_sequence_pairs:
                duplicate_sequence_pairs.add(seq_pair)
            seen_sequence_pairs.add(seq_pair)

        errors = []
        if duplicate_ids:
            errors.append(f"Duplicate node ids: {sorted(duplicate_ids)}")
        if duplicate_sequence_pairs:
            pairs = [f"(order={o}, seq={s})" for o, s in sorted(duplicate_sequence_pairs)]
            errors.append(f"Duplicate executor ordering: {pairs}")

        if errors:
            raise ValueError("; ".join(errors))

        # Write merged+sorted list back
        object.__setattr__(self, "nodes", combined)
        object.__setattr__(self, "execution_steps", [])
        return self

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

            # node cannot appear in both success and failure lists
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
    # Write to a temp file first, then rename.  A rename is atomic on most
    # filesystems so you never end up with a half-written DAG file that
    # Airflow tries to parse mid-write.
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
            "executor_build_id": node.executor_build_id,
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
    # Uses the in-process threading lock — no .lock file on disk.
    try:
        with _REGISTRY_LOCK:
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
    # Uses the in-process threading lock — no .lock file on disk.
    try:
        with _REGISTRY_LOCK:
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
    # Group nodes by executor_order_id so we know which tasks run in parallel
    # and which ones wait for the previous group to finish.
    # Within each layer the nodes are sorted by executor_sequence_id — this
    # only affects the order Python variables are declared in the generated file,
    # not the actual Airflow execution order (parallel tasks start together).
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
    # Compile-check the generated Python before writing it to disk.
    # This catches f-string escaping mistakes or template bugs before Airflow
    # ever sees the file.  SyntaxError here means something went wrong in the
    # template generator — it should never happen in normal operation.
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

    # v1.3: task_id = node name, but deduplicate by appending _<order>_<seq> when names collide.
    # This allows the same service (e.g. KK_File_DB) to appear in multiple layers.
    name_counts: Dict[str, int] = {}
    for node in all_nodes:
        name_counts[node["name"]] = name_counts.get(node["name"], 0) + 1

    task_id_map: Dict[str, str] = {}
    name_seen: Dict[str, int] = {}
    for node in all_nodes:
        base_name = node["name"]
        if name_counts[base_name] > 1:
            # append order/seq so each occurrence gets a unique task_id
            tid = f"{base_name}_{node['executor_order_id']}_{node['executor_sequence_id']}"
        else:
            tid = base_name
        task_id_map[node["id"]] = tid

    node_ids = [node["id"] for node in all_nodes]
    async_node_ids = [node["id"] for node in all_nodes if node["execution_mode"] == "async_no_wait"]
    fire_and_forget_node_ids = [
        node["id"] for node in all_nodes if node["execution_mode"] == "fire_and_forget"
    ]
    node_name_map = {node["id"]: node["name"] for node in all_nodes}
    executor_build_id_map = {node["id"]: node["executor_build_id"] for node in all_nodes}
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
        node["id"]: f"branch__{task_id_map[node['id']]}"
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
    for global_seq, node in enumerate(all_nodes, 1):
        node_id = node["id"]
        tid = task_id_map[node_id]
        py_var = var_map[node_id]
        order_id = node["executor_order_id"]
        build_id = node["executor_build_id"]

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
            f"build_id: `{build_id}`  \\n"
            f"engine: `{node_engine_val}`  \\n"
            f"mode: `{node_mode_val}`  \\n"
            f"order: `{order_id}`"
        )

        upstream_guard = upstream_node_ids_map.get(node_id, [])

        block = (
                        f"{py_var} = PythonOperator(\n"
            f"        task_id={tid!r},\n"
            f"        python_callable=execute_node,\n"
            f"        op_kwargs={{\n"
            f"            \"task_key\": {node_id!r},\n"
            f"            \"node_id\": {node_id!r},\n"
            f"            \"node_name\": {node['name']!r},\n"
            f"            \"executor_build_id\": {build_id!r},\n"
            f"            \"engine\": {node['engine']!r},\n"
            f"            \"execution_mode\": {node['execution_mode']!r},\n"
            f"            \"executor_order_id\": {node['executor_order_id']!r},\n"
            f"            \"global_seq\": {global_seq!r},\n"
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

    # Wire ONLY terminal nodes -> finalize_results -> run_final_event -> completion
    #
    # "Terminal" = node in the highest executor_order_id layer, plus any branch-leaf
    # nodes in earlier layers that have no downstream business children.
    #
    # This gives the clean "last layer → finalize_results" look instead of every
    # node shooting a separate arrow across the graph.

    max_order = max(node["executor_order_id"] for node in all_nodes)
    final_layer_node_ids = [
        node["id"] for node in all_nodes if node["executor_order_id"] == max_order
    ]

    # Business nodes that act as a parent of at least one other business node
    nodes_that_are_parents_of_business: Set[str] = {
        parent for parent, child in deduped_deps
        if child in {task_id_map[nid] for nid in node_ids}
    }

    # Branch-target task ids
    branch_target_tids: Set[str] = {
        task_id_map[t]
        for node in all_nodes if node.get("branch_on_status")
        for t in node.get("on_success_node_ids", []) + node.get("on_failure_node_ids", [])
    }

    # Branch-leaf nodes: are branch targets, live in an earlier layer,
    # and are NOT themselves parents of any downstream business node
    branch_leaf_node_ids = [
        nid for nid in node_ids
        if task_id_map[nid] in branch_target_tids
        and node_order_map[nid] < max_order
        and task_id_map[nid] not in nodes_that_are_parents_of_business
    ]

    # Orphan detection: business nodes that have no downstream business children and
    # are not already captured by final_layer or branch_leaf logic.
    nodes_with_children: Set[str] = {
        parent for parent, _ in deduped_deps
        if parent in task_id_map.values()
    }
    orphan_node_ids = [
        nid for nid in node_ids
        if task_id_map[nid] not in nodes_with_children
    ]

    terminal_node_ids = list(dict.fromkeys(final_layer_node_ids + branch_leaf_node_ids + orphan_node_ids))

    for nid in terminal_node_ids:
        deduped_deps.append((task_id_map[nid], "finalize_results"))
    deduped_deps.append(("finalize_results", "run_final_event"))
    deduped_deps.append(("run_final_event", "completion"))

    special_task_vars = {
        "prepare_inputs": "prepare_inputs_task",
        "run_started_event": "run_started_event",
        "finalize_results": "finalize_results_task",
        "run_final_event": "run_final_event",
        "completion": "completion_task",
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
    executor_build_id_map_repr = repr(executor_build_id_map)

    template = f"""# Generated by dynamic-dag-service — airflow_job1 v1.3 — Airflow 3.0.6
# run_control_id: {repr(run_control_id)}
# dag_id: {repr(dag_id)}

from datetime import datetime, timezone
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

from airflow.sdk import DAG, get_current_context
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator,
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
EXECUTOR_BUILD_ID_MAP = {executor_build_id_map_repr}
BRANCH_SKIP_WHITELIST = {repr(sorted(branch_skip_whitelist))}
MERGE_NODE_IDS = {repr(sorted(merge_task_ids))}
NODE_ORDER_MAP = {node_order_map_repr}
TASK_ID_MAP = {task_id_map_repr}
TASK_ID_TO_NODE_ID = {task_id_to_node_id_repr}

HTTP_SUCCESS_CODES = (200, 201, 202, 204)

# Retry config (override with env vars on Airflow VM)
HTTP_RETRY_ATTEMPTS = int(os.getenv("HTTP_RETRY_ATTEMPTS", "3"))
HTTP_RETRY_MIN_WAIT = float(os.getenv("HTTP_RETRY_MIN_WAIT_SECONDS", "2"))
HTTP_RETRY_MAX_WAIT = float(os.getenv("HTTP_RETRY_MAX_WAIT_SECONDS", "10"))

DEFAULT_ASYNC_SUCCESS_STATUSES = {{"SUCCESS", "SUCCEEDED", "COMPLETED", "DONE", "FINISHED"}}
DEFAULT_ASYNC_FAILURE_STATUSES = {{"FAILED", "FAILURE", "ERROR", "ERRORED", "CANCELLED", "CANCELED", "ABORTED"}}
DEFAULT_ASYNC_RUNNING_STATUSES = {{"RUNNING", "IN_PROGRESS", "PENDING", "QUEUED", "SUBMITTED", "PROCESSING", "STARTED"}}


# ---------------------------------------------------------------------------
# Utility helpers
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


def _should_force_rerun(conf, node_id):
    force_nodes = conf.get("force_rerun_nodes") or []
    if isinstance(force_nodes, str):
        force_nodes = [item.strip() for item in force_nodes.split(",") if item.strip()]
    return bool(conf.get("force_rerun")) or node_id in set(force_nodes)


# ---------------------------------------------------------------------------
# HTTP retry
# ---------------------------------------------------------------------------


def _http_request_with_retry(
    method, url, headers=None, params=None, json_body=None,
    timeout=300, verify=False, node_id="unknown", call_label="submit"
):
    \"\"\"Wraps requests.request() with tenacity retry logic.\"\"\"
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
        if resp.status_code in TRANSIENT_HTTP_ERRORS:
            raise requests.exceptions.ConnectionError(
                f"Node {{node_id}} [{{call_label}}]: HTTP {{resp.status_code}} (transient) - will retry"
            )
        return resp

    if not _TENACITY_AVAILABLE:
        log.warning("tenacity not installed — retries disabled for node %s [%s].", node_id, call_label)
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
            f"\\n"
            f"  Node       : {{node_id}}\\n"
            f"  Stage      : {{call_label}}\\n"
            f"  ── Request ──────────────────────────\\n"
            f"  Method     : {{str(method).upper()}}\\n"
            f"  URL        : {{url}}\\n"
            f"  ── Error ────────────────────────────\\n"
            f"  All {{HTTP_RETRY_ATTEMPTS}} retry attempts exhausted.\\n"
            f"  Last error : {{exc.last_attempt.exception()}}\\n"
            f"  Hint       : Check the URL is reachable, the service is running, "
            f"and the network path is open from the Airflow worker."
        ) from exc


# ---------------------------------------------------------------------------
# Async polling
# ---------------------------------------------------------------------------


def _poll_status_until_terminal(node_id, node_name, payload, start_response_body, tracking_id):
    ctx = get_current_context()
    status_cfg = payload.get("status") or {{}}
    if not isinstance(status_cfg, dict) or not status_cfg:
        raise AirflowException(
            f"Node {{node_id}} ({{node_name}}) is async_no_wait but has no 'status' block in conf."
        )

    response_id_key = status_cfg.get("response_id_key") or payload.get("response_id_key") or "job_id"

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
            f"Submit response was: {{start_response_body}}"
        )

    replacements = {{
        "tracking_id": tracking_id, "job_id": tracking_id, "run_id": tracking_id,
        "runId": tracking_id, "node_runId": tracking_id, "node_run_id": tracking_id,
        "node_id": node_id, "node_name": node_name,
        "dag_id": ctx["dag"].dag_id, "dag_run_id": ctx["dag_run"].run_id,
        "run_control_id": RUN_CONTROL_ID,
    }}

    status_url = _render_value(status_cfg.get("url"), replacements)
    if not status_url:
        raise AirflowException(f"Node {{node_id}}: 'status.url' is required for async_no_wait nodes.")

    method = str(status_cfg.get("method") or "GET").upper()
    headers = _render_value(status_cfg.get("headers") or payload.get("headers") or {{}}, replacements)
    params = _render_value(status_cfg.get("params") or {{}}, replacements)
    json_body = _render_value(status_cfg.get("json") or None, replacements)
    verify_ssl = status_cfg.get("verify_ssl", payload.get("verify_ssl", False))
    request_timeout = int(status_cfg.get("request_timeout", payload.get("timeout", 300)))
    poke_interval = float(status_cfg.get("poke_interval", 10))
    timeout_seconds = float(status_cfg.get("timeout", 1800))

    response_status_key = status_cfg.get("response_status_key") or "status"
    success_statuses = _to_upper_set(status_cfg.get("success_statuses"), DEFAULT_ASYNC_SUCCESS_STATUSES)
    failure_statuses = _to_upper_set(status_cfg.get("failure_statuses"), DEFAULT_ASYNC_FAILURE_STATUSES)
    running_statuses = _to_upper_set(status_cfg.get("running_statuses"), DEFAULT_ASYNC_RUNNING_STATUSES)

    deadline = time.time() + timeout_seconds
    last_status_response = {{}}
    poll_count = 0

    log.info("[ASYNC] Node %s: polling. tracking_id=%s url=%s", node_id, tracking_id, status_url)

    while time.time() <= deadline:
        poll_count += 1
        try:
            response = _http_request_with_retry(
                method=method, url=status_url, headers=headers, params=params,
                json_body=json_body, timeout=request_timeout, verify=verify_ssl,
                node_id=node_id, call_label=f"poll_{{poll_count}}",
            )
            status_body = _parse_response_body(response)
            last_status_response = status_body
        except AirflowException:
            raise
        except Exception as exc:
            raise AirflowException(f"Node {{node_id}}: poll #{{poll_count}} failed: {{exc}}") from exc

        if response.status_code in (401, 403):
            raise AirflowException(
                f"\\n"
                f"  Node       : {{node_id}} ({{node_name}})\\n"
                f"  Stage      : poll #{{poll_count}}\\n"
                f"  ── Request ──────────────────────────\\n"
                f"  Method     : {{method}}\\n"
                f"  URL        : {{status_url}}\\n"
                f"  ── Response ─────────────────────────\\n"
                f"  HTTP Status: {{response.status_code}}\\n"
                f"  Body       : {{response.text[:400]}}\\n"
                f"  Hint       : Check Authorization headers in the 'status' block of the run conf."
            )
        if response.status_code == 404:
            raise AirflowException(
                f"\\n"
                f"  Node       : {{node_id}} ({{node_name}})\\n"
                f"  Stage      : poll #{{poll_count}}\\n"
                f"  ── Request ──────────────────────────\\n"
                f"  URL        : {{status_url}}\\n"
                f"  Tracking ID: {{tracking_id}}\\n"
                f"  ── Response ─────────────────────────\\n"
                f"  HTTP Status: 404\\n"
                f"  Hint       : tracking_id may be invalid or the job already expired."
            )
        if response.status_code not in HTTP_SUCCESS_CODES:
            raise AirflowException(
                f"\\n"
                f"  Node       : {{node_id}} ({{node_name}})\\n"
                f"  Stage      : poll #{{poll_count}}\\n"
                f"  ── Request ──────────────────────────\\n"
                f"  Method     : {{method}}\\n"
                f"  URL        : {{status_url}}\\n"
                f"  Tracking ID: {{tracking_id}}\\n"
                f"  ── Response ─────────────────────────\\n"
                f"  HTTP Status: {{response.status_code}}\\n"
                f"  Body       : {{response.text[:600]}}"
            )

        raw_status = _extract_by_path(status_body, response_status_key)
        normalized_status, status_text = _normalize_terminal_status(
            raw_status, success_statuses, failure_statuses, running_statuses
        )
        log.info("[ASYNC] Node %s poll #%d: %s → %s", node_id, poll_count, status_text, normalized_status)

        if normalized_status == "SUCCESS":
            return {{
                "status": "completed", "node_id": node_id, "tracking_id": tracking_id,
                "poll_count": poll_count, "external_status": status_text,
                "start_response": start_response_body, "status_response": status_body,
            }}
        if normalized_status == "FAILED":
            raise AirflowException(
                f"Node {{node_id}}: external job failed. status={{status_text}}, tracking_id={{tracking_id}}"
            )
        time.sleep(poke_interval)

    raise AirflowException(
        f"Node {{node_id}}: polling timed out after {{timeout_seconds}}s ({{poll_count}} polls)."
    )


# ---------------------------------------------------------------------------
# Payload resolution
# ---------------------------------------------------------------------------


def resolve_payload(conf, node_id, executor_build_id, task_key=None, global_seq=None):
    \"\"\"
    Look up this node's payload from dag_run.conf.

    Lookup order:
      1. conf[executor_build_id]  -- primary key, e.g. "ICE_FILE_TRANSFER_1"
      2. conf[node_id]            -- same as executor_build_id in most cases
      3. conf[task_key]           -- legacy fallback
      4. conf["taskN"]            -- positional fallback: "task1"/"Task1" maps to global_seq=1
      5. scan for node_runId match
    \"\"\"
    conf_normalized = {{str(k).strip().lower(): v for k, v in conf.items()}}
    node_id_l = str(node_id).lower()
    exec_id_l = str(executor_build_id).lower() if executor_build_id else None
    task_id_l = str(task_key).lower() if task_key else None

    # Primary: executor_build_id is the intended run-time key
    if exec_id_l:
        direct = conf_normalized.get(exec_id_l)
        if isinstance(direct, dict):
            return direct

    # Fallback 1: node_id
    direct = conf_normalized.get(node_id_l)
    if isinstance(direct, dict):
        return direct

    # Fallback 2: legacy task_key
    if task_id_l and task_id_l not in (node_id_l, exec_id_l):
        keyed = conf_normalized.get(task_id_l)
        if isinstance(keyed, dict):
            return keyed

    # Fallback 3: positional "taskN" key — allows run conf to use "task1", "Task1", etc.
    # global_seq is baked in at DAG build time as the 1-based position of this node
    # in execution order (sorted by executor_order_id, executor_sequence_id).
    if global_seq is not None:
        seq_key = f"task{{global_seq}}"
        keyed = conf_normalized.get(seq_key)
        if isinstance(keyed, dict):
            return keyed

    # Fallback 4: scan for node_runId match at top level of each conf entry
    for _, value in conf.items():
        if isinstance(value, dict) and str(value.get("node_runId", "")).strip() == node_id:
            return value

    return {{}}


# ---------------------------------------------------------------------------
# Merge guard
# ---------------------------------------------------------------------------


def _check_merge_guard(ti, node_id, upstream_guard_node_ids):
    if not upstream_guard_node_ids:
        return
    problems = []
    for upstream_nid in upstream_guard_node_ids:
        upstream_tid = TASK_ID_MAP.get(upstream_nid, upstream_nid)
        marker = ti.xcom_pull(task_ids=upstream_tid, key=f"{{upstream_nid}}_task_state")
        marker = str(marker).lower().strip() if marker is not None else ""
        if marker == "success":
            continue
        if not marker:
            if upstream_tid in BRANCH_SKIP_WHITELIST:
                continue
            problems.append(
                f"upstream {{upstream_nid}} (task={{upstream_tid}}) has no task_state XCom "
                f"and is not in branch skip whitelist"
            )
        elif marker == "failed":
            problems.append(f"upstream {{upstream_nid}} (task={{upstream_tid}}) state=failed")
        else:
            problems.append(f"upstream {{upstream_nid}} unexpected state={{marker}}")
    if problems:
        raise AirflowException(
            f"Merge guard FAILED for node {{node_id}}: " + "; ".join(problems)
        )
    log.info("[MERGE_GUARD] Node %s: %d upstream(s) OK", node_id, len(upstream_guard_node_ids))


# ---------------------------------------------------------------------------
# execute_node — v1.3: no runtime idempotency registry
# ---------------------------------------------------------------------------


def execute_node(
    task_key,
    node_id,
    node_name,
    executor_build_id,
    engine,
    execution_mode,
    executor_order_id,
    global_seq=None,
    upstream_guard_node_ids=None,
    **kwargs,
):
    ctx = get_current_context()
    conf = _get_conf(ctx)
    ti = ctx["ti"]

    if upstream_guard_node_ids:
        _check_merge_guard(ti, node_id, upstream_guard_node_ids)

    payload = resolve_payload(conf, node_id=node_id, executor_build_id=executor_build_id, task_key=task_key, global_seq=global_seq)

    # Resume
    if conf.get("resume") and not _should_force_rerun(conf, node_id):
        resume_from = conf.get("resume_from")
        if resume_from:
            if resume_from not in NODE_ORDER_MAP:
                raise AirflowException(
                    f"[RESUME] resume_from='{{resume_from}}' is not a valid node ID. "
                    f"Valid: {{sorted(NODE_ORDER_MAP.keys())}}"
                )
            if executor_order_id < NODE_ORDER_MAP[resume_from]:
                log.info("[RESUME] Skipping node %s (order=%s)", node_id, executor_order_id)
                ti.xcom_push(key=f"{{node_id}}_task_state", value="success")
                ti.xcom_push(key=f"{{node_id}}_branch", value="success")
                return {{"status": "resume_skipped", "node_id": node_id}}

    if not payload or not payload.get("url"):
        ti.xcom_push(key=f"{{node_id}}_task_state", value="failed")
        ti.xcom_push(key=f"{{node_id}}_branch", value="failure")
        raise AirflowException(
            f"Node {{node_id}} ({{node_name}}) — executor_build_id: {{executor_build_id}}\\n"
            f"  Missing or empty 'url' in conf.\\n"
            f"  Expected conf key: '{{executor_build_id}}'\\n"
            f"  The run-time conf must have an entry keyed by executor_build_id, e.g.:\\n"
            f"    {{ '{{executor_build_id}}': {{ 'url': 'http://...', 'json': {{...}} }} }}"
        )

    ti.xcom_push(key=f"{{node_id}}_task_state", value="started")

    request_method = str(payload.get("method", "POST")).upper()
    request_url    = payload["url"]
    request_body   = payload.get("json")
    request_params = payload.get("params")
    request_timeout = int(payload.get("timeout", 300))
    verify_ssl = payload.get("verify_ssl", False)

    # Sanitize request body for logging — remove auth/token fields
    def _safe_request_body(body):
        if not isinstance(body, dict):
            return body
        _AUTH_KEYS = {{"password", "secret", "token", "authorization", "api_key", "apikey", "access_key"}}
        return {{
            k: ("***REDACTED***" if str(k).lower() in _AUTH_KEYS else v)
            for k, v in body.items()
        }}

    def _fmt_error(label, http_status, response_text, hint=""):
        sanitized_body = _safe_request_body(request_body)
        parts = [
            f"",
            f"  Node       : {{node_id}} ({{node_name}})",
            f"  Build ID   : {{executor_build_id}}",
            f"  Stage      : {{label}}",
            f"  ── Request ──────────────────────────",
            f"  Method     : {{request_method}}",
            f"  URL        : {{request_url}}",
            f"  Params     : {{request_params}}",
            f"  Body       : {{json.dumps(sanitized_body, default=str)[:600]}}",
            f"  ── Response ─────────────────────────",
            f"  HTTP Status: {{http_status}}",
            f"  Body       : {{response_text[:800]}}",
        ]
        if hint:
            parts.append(f"  Hint       : {{hint}}")
        return "\\n".join(parts)

    try:
        response = _http_request_with_retry(
            method=request_method,
            url=request_url,
            json_body=request_body,
            params=request_params,
            headers=payload.get("headers", {{}}),
            timeout=request_timeout,
            verify=verify_ssl,
            node_id=node_id,
            call_label="submit",
        )
        response_body = _parse_response_body(response)
    except AirflowException:
        ti.xcom_push(key=f"{{node_id}}_task_state", value="failed")
        ti.xcom_push(key=f"{{node_id}}_branch", value="failure")
        raise
    except Exception as exc:
        msg = _fmt_error("submit (network/timeout)", "N/A", str(exc),
                         hint="Check the URL is reachable and the service is running.")
        ti.xcom_push(key=f"{{node_id}}_task_state", value="failed")
        ti.xcom_push(key=f"{{node_id}}_branch", value="failure")
        ti.xcom_push(key=f"{{node_id}}_error", value=str(exc))
        raise AirflowException(msg) from exc

    if response.status_code in (401, 403):
        msg = _fmt_error("submit", response.status_code, response.text,
                         hint="Check the Authorization header / Bearer token in the run conf for this node.")
        ti.xcom_push(key=f"{{node_id}}_task_state", value="failed")
        ti.xcom_push(key=f"{{node_id}}_branch", value="failure")
        ti.xcom_push(key=f"{{node_id}}_error", value=msg)
        raise AirflowException(msg)

    if response.status_code == 422:
        msg = _fmt_error("submit", response.status_code, response.text,
                         hint=(
                             "HTTP 422 = the downstream service rejected the request body. "
                             "Check 'Body' above — a required field is likely null or missing. "
                             "Verify the 'json' block in the run conf for node '{{node_id}}'."
                         ))
        ti.xcom_push(key=f"{{node_id}}_task_state", value="failed")
        ti.xcom_push(key=f"{{node_id}}_branch", value="failure")
        ti.xcom_push(key=f"{{node_id}}_error", value=msg)
        raise AirflowException(msg)

    if response.status_code == 404:
        msg = _fmt_error("submit", response.status_code, response.text,
                         hint="HTTP 404 = URL not found. Check the 'url' field in the run conf for this node.")
        ti.xcom_push(key=f"{{node_id}}_task_state", value="failed")
        ti.xcom_push(key=f"{{node_id}}_branch", value="failure")
        ti.xcom_push(key=f"{{node_id}}_error", value=msg)
        raise AirflowException(msg)

    if response.status_code not in HTTP_SUCCESS_CODES:
        msg = _fmt_error("submit", response.status_code, response.text)
        ti.xcom_push(key=f"{{node_id}}_task_state", value="failed")
        ti.xcom_push(key=f"{{node_id}}_branch", value="failure")
        ti.xcom_push(key=f"{{node_id}}_error", value=msg)
        raise AirflowException(msg)

    # fire_and_forget
    if execution_mode == "fire_and_forget":
        result = {{"status": "submitted_no_track", "node_id": node_id, "submit_response": response_body}}
        ti.xcom_push(key=f"{{node_id}}_submit_response", value=response_body)
        ti.xcom_push(key=f"{{node_id}}_submit_http_status", value=response.status_code)
        ti.xcom_push(key=f"{{node_id}}_response", value=result)
        ti.xcom_push(key=f"{{node_id}}_task_state", value="success")
        ti.xcom_push(key=f"{{node_id}}_branch", value="success")
        return result

    # async_no_wait
    if execution_mode == "async_no_wait":
        status_cfg = payload.get("status") or {{}}
        response_id_key = status_cfg.get("response_id_key") or payload.get("response_id_key") or "job_id"
        tracking_id = (
            status_cfg.get("tracking_id") or payload.get("tracking_id")
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

        try:
            result = _poll_status_until_terminal(node_id, node_name, payload, response_body, tracking_id)
        except Exception as exc:
            ti.xcom_push(key=f"{{node_id}}_task_state", value="failed")
            ti.xcom_push(key=f"{{node_id}}_branch", value="failure")
            ti.xcom_push(key=f"{{node_id}}_error", value=str(exc))
            raise

        ti.xcom_push(key=f"{{node_id}}_response", value=result)
        ti.xcom_push(key=f"{{node_id}}_task_state", value="success")
        ti.xcom_push(key=f"{{node_id}}_branch", value="success")
        return result

    # sync
    result = response_body
    ti.xcom_push(key=f"{{node_id}}_response", value=result)
    ti.xcom_push(key=f"{{node_id}}_task_state", value="success")
    ti.xcom_push(key=f"{{node_id}}_branch", value="success")
    return result


# ---------------------------------------------------------------------------
# choose_branch
# ---------------------------------------------------------------------------


def choose_branch(node_task_id, node_id, success_task_ids, failure_task_ids, **kwargs):
    ctx = get_current_context()
    ti = ctx["ti"]
    marker = ti.xcom_pull(task_ids=node_task_id, key=f"{{node_id}}_branch")
    marker = str(marker).lower().strip() if marker is not None else None

    log.info("[BRANCH] Node %s: marker=%s success=%s failure=%s", node_id, marker, success_task_ids, failure_task_ids)

    if marker == "success":
        if not success_task_ids:
            raise AirflowException(f"[BRANCH] Node {{node_id}}: no on_success_node_ids configured.")
        return success_task_ids

    if marker is None or marker == "":
        log.warning("[BRANCH] Node %s: marker is None/empty — treating as failure.", node_id)

    if not failure_task_ids:
        raise AirflowException(
            f"[BRANCH] Node {{node_id}}: marker={{marker}} but no on_failure_node_ids configured."
        )
    return failure_task_ids


# ---------------------------------------------------------------------------
# finalize_results
# ---------------------------------------------------------------------------


def finalize_results(node_ids, **kwargs):
    ctx = get_current_context()
    ti = ctx["ti"]
    ti.xcom_push(key="dag_terminal_state_seen", value=True)

    failed, success, expected_skipped, unexpected_skipped, running, unknown = [], [], [], [], [], []

    for node_id in node_ids:
        node_label = NODE_NAME_MAP.get(node_id, node_id)
        build_id = EXECUTOR_BUILD_ID_MAP.get(node_id, node_id)
        airflow_task_id = TASK_ID_MAP.get(node_id, node_id)

        marker = ti.xcom_pull(task_ids=airflow_task_id, key=f"{{node_id}}_task_state")
        marker = str(marker).lower().strip() if marker is not None else ""

        entry = {{
            "task_id": airflow_task_id,
            "node_id": node_id,
            "name": node_label,
            "executor_build_id": build_id,
            "state": marker or "unknown",
            "status_source": "xcom_task_state_marker",
        }}

        if node_id in FIRE_AND_FORGET_NODE_IDS:
            entry["status_source"] = "airflow_submit_only"
            entry["state"] = "submitted_no_track" if marker == "success" else (marker or "unknown")

        if marker == "success":
            success.append(entry)
        elif marker == "failed":
            failed.append(entry)
        elif not marker:
            if node_id in BRANCH_SKIP_WHITELIST and node_id not in MERGE_NODE_IDS:
                expected_skipped.append({{**entry, "skip_type": "expected_branch_skip"}})
            else:
                log.warning("[FINALIZE] Node %s (task=%s) has no task_state XCom.", node_id, airflow_task_id)
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
# completion — terminal sentinel (v1.2)
# Mirrors finalize_results outcome so the DAG state reflects business reality.
# run_final_event (Kafka) always runs via ALL_DONE and does NOT set DAG state.
# completion runs after Kafka and raises if the run failed.
# ---------------------------------------------------------------------------


def run_completion(**kwargs):
    ctx = get_current_context()
    ti = ctx["ti"]
    final_status = ti.xcom_pull(task_ids="finalize_results", key="final_status")

    if final_status is None:
        log.warning(
            "[COMPLETION] final_status XCom missing from finalize_results — defaulting to FAILED."
        )
        final_status = "FAILED"

    log.info("[COMPLETION] DAG terminal state: %s", final_status)

    if final_status != "SUCCESS":
        raise AirflowException(
            f"[COMPLETION] DAG run finished with status: {{final_status}}. "
            f"Check finalize_results and task logs for details."
        )

    return {{"completion_status": "SUCCESS"}}


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
        "status": "Running",
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
    status_value = ti.xcom_pull(task_ids="finalize_results", key="final_status")
    if status_value is None:
        log.warning("[KAFKA] final_status XCom missing — defaulting to FAILED.")
        status_value = "FAILED"
    # Map internal uppercase states to the agreed Kafka event status values
    kafka_status = "Success" if status_value == "SUCCESS" else "Failure"
    payload = {{
        "eventType": {repr(EVENTS["run_succeeded"])} if status_value == "SUCCESS" else {repr(EVENTS["run_failed"])},
        "run_control_id": RUN_CONTROL_ID,
        "correlation_id": _get_correlation_id(ctx),
        "event_source": "AIRFLOW",
        "status": kafka_status,
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
    tags=["dynamic", "generated", "v1.3", "airflow-3.0.6"],
    doc_md=f"Dynamic DAG — run_control_id: {repr(run_control_id)} | nodes: {repr(len(all_nodes))}",
) as dag:

    prepare_inputs_task = PythonOperator(
        task_id="prepare_inputs",
        python_callable=lambda: True,
        doc_md="Entry sentinel — initializes DAG context before Kafka event.",
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
        doc_md="Aggregates XCom state from all nodes. Raises if any failed or went missing.",
    )

    run_final_event = ProduceToTopicOperator(
        task_id="run_final_event",
        kafka_config_id=KAFKA_CONN_ID,
        topic=KAFKA_RUN_TOPIC,
        producer_function=build_run_final_event_messages,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Publishes run.succeeded.v1 or run.failed.v1 Kafka event. Always runs.",
    )

    completion_task = PythonOperator(
        task_id="completion",
        python_callable=run_completion,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
        doc_md=(
            "Terminal sentinel node (v1.2). "
            "Reads finalize_results XCom and mirrors its SUCCESS/FAILED state. "
            "This is the node that drives the overall DAG run status."
        ),
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

    # One file per dag_id, no timestamp in the name.
    # Previously each build wrote a timestamped file (dag_id_20260511_143012.py).
    # That caused the Airflow scheduler to pick up and parse every old version on
    # every scan cycle — slow and confusing.  Now we always write dag_id.py and
    # overwrite it cleanly.  Old timestamped files are removed automatically below.
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
