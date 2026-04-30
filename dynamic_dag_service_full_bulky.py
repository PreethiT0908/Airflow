import hashlib
import json
import logging
import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from textwrap import dedent
from typing import Any, Dict, List, Optional, Set, Tuple

import portalocker
import uvicorn
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("dynamic-dag-service")

app = FastAPI(title="Dynamic Airflow DAG Service", version="9.0.0")

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
    def validate_unique_ids_and_positions(cls, nodes: List[Node]) -> List[Node]:
        seen_ids: Set[str] = set()
        duplicate_ids: Set[str] = set()
        seen_sequence_pairs: Set[Tuple[int, int]] = set()
        duplicate_sequence_pairs: Set[Tuple[int, int]] = set()

        for node in nodes:
            if node.id in seen_ids:
                duplicate_ids.add(node.id)
            seen_ids.add(node.id)

            seq_pair = (node.executor_order_id, node.executor_sequence_id)
            if seq_pair in seen_sequence_pairs:
                duplicate_sequence_pairs.add(seq_pair)
            seen_sequence_pairs.add(seq_pair)

        if duplicate_ids:
            raise ValueError(f"Duplicate node ids found: {sorted(duplicate_ids)}")

        if duplicate_sequence_pairs:
            pairs = [f"(order={o}, seq={s})" for o, s in sorted(duplicate_sequence_pairs)]
            raise ValueError(f"Duplicate executor ordering found: {pairs}")

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

        if errors:
            raise ValueError("; ".join(errors))

        return self


# ---------------------------------------------------------------------------
# FastAPI exception handlers
# ---------------------------------------------------------------------------


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": exc.errors()},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Global exception caught at: %s", request.url.path)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "Internal server error",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        },
    )


# ---------------------------------------------------------------------------
# Identifier helpers
# ---------------------------------------------------------------------------


def sanitize_identifier(raw: str) -> str:
    value = (raw or "").strip().lower()
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "dag"


def python_var_safe(raw: str) -> str:
    base = sanitize_identifier(raw)
    if not base:
        base = "task"
    if base[0].isdigit():
        base = f"n_{base}"
    return base


# ---------------------------------------------------------------------------
# FIX #5 / #6 — task_id derives from node name so Airflow graph nodes show
# human-readable labels.  The node id is appended after a double-underscore
# to keep uniqueness and allow xcom key lookups.
# ---------------------------------------------------------------------------

def make_task_id(node_id: str, node_name: str) -> str:
    label = sanitize_identifier(node_name)
    suffix = sanitize_identifier(node_id)
    if label == suffix or not label:
        return suffix
    return f"{label}__{suffix}"


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
                "Registry entry exists but DAG file is missing for key %s", idempotency_key
            )
            registry.pop(idempotency_key, None)
            write_json_file(REGISTRY_FILE, registry)
            return None
        return entry


def store_registry_entry(idempotency_key: str, entry: Dict[str, Any]) -> None:
    with file_lock(REGISTRY_FILE.with_suffix(".lock")):
        registry = load_json_file(REGISTRY_FILE, default={})
        registry[idempotency_key] = entry
        write_json_file(REGISTRY_FILE, registry)


# ---------------------------------------------------------------------------
# Layer / dependency builders
# ---------------------------------------------------------------------------


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
    task_id_map: node_id -> airflow task_id
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
# DAG code generator
# ---------------------------------------------------------------------------


def generate_dag_code(
    dag_id: str,
    run_control_id: str,
    sorted_layers: List[Tuple[int, List[Dict[str, Any]]]],
    schedule: Optional[str],
) -> str:
    all_nodes = [node for _, layer_nodes in sorted_layers for node in layer_nodes]

    # FIX #5 / #6: task IDs now derive from node name so Airflow graph nodes
    # show the human-readable label, not a raw internal identifier.
    task_id_map: Dict[str, str] = {}
    used_task_ids: Set[str] = set()
    for node in all_nodes:
        base_tid = make_task_id(node["id"], node["name"])
        candidate = base_tid
        i = 2
        while candidate in used_task_ids:
            candidate = f"{base_tid}_{i}"
            i += 1
        used_task_ids.add(candidate)
        task_id_map[node["id"]] = candidate

    node_ids = [node["id"] for node in all_nodes]

    async_node_ids = [node["id"] for node in all_nodes if node["execution_mode"] == "async_no_wait"]
    fire_and_forget_node_ids = [
        node["id"] for node in all_nodes if node["execution_mode"] == "fire_and_forget"
    ]
    node_name_map = {node["id"]: node["name"] for node in all_nodes}

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

    # Branch task IDs — named after the node task_id for clarity
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

    # Upstream map — used to detect merge nodes and set trigger rules
    upstream_map: Dict[str, Set[str]] = {task_id_map[nid]: set() for nid in node_ids}
    for parent, child in deduped_deps:
        if child in upstream_map and parent != "run_started_event":
            upstream_map[child].add(parent)

    branch_skip_whitelist: Set[str] = set()
    for node in all_nodes:
        if node.get("branch_on_status"):
            branch_skip_whitelist.update(task_id_map[t] for t in node.get("on_success_node_ids", []))
            branch_skip_whitelist.update(task_id_map[t] for t in node.get("on_failure_node_ids", []))

    # FIX #1 — merge nodes use ALL_SUCCESS so that a failed parent propagates
    # failure to the merge point instead of being silently absorbed.
    merge_task_ids = {tid for tid, parents in upstream_map.items() if len(parents) > 1}

    # Build node_order_map: node_id -> executor_order_id, needed for resume skip logic
    node_order_map: Dict[str, int] = {node["id"]: node["executor_order_id"] for node in all_nodes}

    # Task definitions
    task_defs: List[str] = []
    for node in all_nodes:
        node_id = node["id"]
        tid = task_id_map[node_id]
        py_var = var_map[node_id]

        # FIX #1 — merge nodes require ALL parents to succeed
        if tid in merge_task_ids:
            trigger_rule = "TriggerRule.ALL_SUCCESS"
        else:
            trigger_rule = "TriggerRule.ALL_SUCCESS"

        task_defs.append(
            dedent(
                f"""
                {py_var} = PythonOperator(
                    task_id={tid!r},
                    python_callable=execute_node,
                    op_kwargs={{
                        "task_key": {node_id!r},
                        "node_id": {node_id!r},
                        "node_name": {node['name']!r},
                        "engine": {node['engine']!r},
                        "execution_mode": {node['execution_mode']!r},
                        "executor_order_id": {node['executor_order_id']!r},
                    }},
                    trigger_rule={trigger_rule},
                    doc_md={f"**{node['name']}**  \\nengine: `{node['engine']}`  \\nmode: `{node['execution_mode']}`"!r},
                )
                """.strip()
            )
        )

        if node.get("branch_on_status"):
            branch_tid = branch_task_ids[node_id]
            branch_var = branch_var_map[branch_tid]
            success_tids = [task_id_map[t] for t in node.get("on_success_node_ids", [])]
            failure_tids = [task_id_map[t] for t in node.get("on_failure_node_ids", [])]
            task_defs.append(
                dedent(
                    f"""
                    {branch_var} = BranchPythonOperator(
                        task_id={branch_tid!r},
                        python_callable=choose_branch,
                        op_kwargs={{
                            "node_task_id": {tid!r},
                            "node_id": {node_id!r},
                            "success_task_ids": {success_tids!r},
                            "failure_task_ids": {failure_tids!r},
                        }},
                        trigger_rule=TriggerRule.ALL_DONE,
                        doc_md={f"Branch router for **{node['name']}**"!r},
                    )
                    """.strip()
                )
            )

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
        # reverse-lookup: task_id -> node_id -> py_var
        for nid, tid in task_id_map.items():
            if tid == task_ref:
                return var_map[nid]
        if task_ref in branch_var_map:
            return branch_var_map[task_ref]
        return python_var_safe(task_ref)

    dep_lines = [f"{to_var(parent)} >> {to_var(child)}" for parent, child in deduped_deps]

    # FIX #5 — use TaskGroup per executor_order_id layer to tidy the graph view
    # Each layer gets its own group so collapsed stages show as single boxes.
    layer_groups: Dict[int, List[str]] = {}
    for node in all_nodes:
        layer_groups.setdefault(node["executor_order_id"], []).append(var_map[node["id"]])

    group_defs: List[str] = []
    for order_id in sorted(layer_groups.keys()):
        group_defs.append(
            f"    stage_{order_id}_group = TaskGroup(group_id='stage_{order_id}')"
        )

    # Rebuild task_defs with group membership
    indented_task_defs_with_groups: List[str] = []
    for node in all_nodes:
        node_id = node["id"]
        tid = task_id_map[node_id]
        py_var = var_map[node_id]
        order_id = node["executor_order_id"]

        if tid in merge_task_ids:
            trigger_rule = "TriggerRule.ALL_SUCCESS"
        else:
            trigger_rule = "TriggerRule.ALL_SUCCESS"

        success_tids = [task_id_map[t] for t in node.get("on_success_node_ids", [])]
        failure_tids = [task_id_map[t] for t in node.get("on_failure_node_ids", [])]

        indented_task_defs_with_groups.append(
            dedent(
                f"""
                with stage_{order_id}_group:
                    {py_var} = PythonOperator(
                        task_id={tid!r},
                        python_callable=execute_node,
                        op_kwargs={{
                            "task_key": {node_id!r},
                            "node_id": {node_id!r},
                            "node_name": {node['name']!r},
                            "engine": {node['engine']!r},
                            "execution_mode": {node['execution_mode']!r},
                            "executor_order_id": {node['executor_order_id']!r},
                        }},
                        trigger_rule={trigger_rule},
                        doc_md={f"**{node['name']}**  \\nengine: `{node['engine']}`  \\nmode: `{node['execution_mode']}`"!r},
                    )
                """.strip()
            )
        )

        if node.get("branch_on_status"):
            branch_tid = branch_task_ids[node_id]
            branch_var = branch_var_map[branch_tid]
            indented_task_defs_with_groups.append(
                dedent(
                    f"""
                    with stage_{order_id}_group:
                        {branch_var} = BranchPythonOperator(
                            task_id={branch_tid!r},
                            python_callable=choose_branch,
                            op_kwargs={{
                                "node_task_id": {tid!r},
                                "node_id": {node_id!r},
                                "success_task_ids": {success_tids!r},
                                "failure_task_ids": {failure_tids!r},
                            }},
                            trigger_rule=TriggerRule.ALL_DONE,
                            doc_md={f"Branch router for **{node['name']}**"!r},
                        )
                    """.strip()
                )
            )

    joined_group_defs = "\n    ".join(group_defs)
    joined_task_defs = "\n\n    ".join(indented_task_defs_with_groups)
    joined_deps = "\n    ".join(dep_lines)
    schedule_str = repr(schedule) if schedule else "None"

    # node_order_map embedded in generated code for resume skip logic
    node_order_map_repr = repr(node_order_map)
    # task_id_map embedded so execute_node can look up task_id from node_id
    task_id_map_repr = repr(task_id_map)

    template = """from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import logging
import os
import time
import requests

try:
    import portalocker
except Exception:
    portalocker = None

from airflow.sdk import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator, get_current_context
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger("airflow.task")

KAFKA_CONN_ID = __KAFKA_CONN_ID__
KAFKA_RUN_TOPIC = __KAFKA_RUN_TOPIC__
RUN_CONTROL_ID = __RUN_CONTROL_ID__
FINAL_NODE_IDS = __FINAL_NODE_IDS__
ASYNC_NODE_IDS = __ASYNC_NODE_IDS__
FIRE_AND_FORGET_NODE_IDS = __FIRE_AND_FORGET_NODE_IDS__
NODE_NAME_MAP = __NODE_NAME_MAP__
BRANCH_SKIP_WHITELIST = __BRANCH_SKIP_WHITELIST__
MERGE_NODE_IDS = __MERGE_NODE_IDS__
NODE_ORDER_MAP = __NODE_ORDER_MAP__
TASK_ID_MAP = __TASK_ID_MAP__
RUNTIME_REGISTRY_FILE = Path(os.getenv("RUNTIME_IDEMPOTENCY_REGISTRY", "/tmp/dynamic_dag_runtime_registry.json"))
RUNTIME_REGISTRY_FILE.parent.mkdir(parents=True, exist_ok=True)

HTTP_SUCCESS_CODES = (200, 201, 202, 204)
DEFAULT_ASYNC_SUCCESS_STATUSES = {"SUCCESS", "SUCCEEDED", "COMPLETED", "DONE", "FINISHED"}
DEFAULT_ASYNC_FAILURE_STATUSES = {"FAILED", "FAILURE", "ERROR", "ERRORED", "CANCELLED", "CANCELED", "ABORTED"}
DEFAULT_ASYNC_RUNNING_STATUSES = {"RUNNING", "IN_PROGRESS", "PENDING", "QUEUED", "SUBMITTED", "PROCESSING", "STARTED"}


def _utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_json(value):
    return json.dumps(value, default=str)


def _canonicalize_value(value):
    if isinstance(value, dict):
        return {str(k): _canonicalize_value(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_canonicalize_value(v) for v in value]
    if isinstance(value, str):
        return value.strip()
    return value


def _sha256(value):
    raw = json.dumps(_canonicalize_value(value), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _lock_runtime_registry():
    lock_path = RUNTIME_REGISTRY_FILE.with_suffix(".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = open(lock_path, "a+", encoding="utf-8")
    if portalocker:
        portalocker.lock(lock_file, portalocker.LOCK_EX)
    return lock_file


def _unlock_runtime_registry(lock_file):
    try:
        if portalocker:
            portalocker.unlock(lock_file)
    finally:
        lock_file.close()


def _load_runtime_registry():
    if not RUNTIME_REGISTRY_FILE.exists():
        return {}
    try:
        with RUNTIME_REGISTRY_FILE.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
            return data if isinstance(data, dict) else {}
    except Exception:
        log.warning("Failed to load runtime registry %s; using empty", RUNTIME_REGISTRY_FILE)
        return {}


def _write_runtime_registry(registry):
    tmp_path = RUNTIME_REGISTRY_FILE.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(registry, handle, indent=2, sort_keys=True, default=str)
    tmp_path.replace(RUNTIME_REGISTRY_FILE)


def _make_node_idempotency_key(ctx, node_id, payload):
    base = {
        "dag_id": ctx["dag"].dag_id,
        "run_control_id": RUN_CONTROL_ID,
        "correlation_id": _get_correlation_id(ctx),
        "node_id": node_id,
        "payload_hash": _sha256(payload),
    }
    return _sha256(base)


def _get_runtime_entry(node_key):
    lock_file = _lock_runtime_registry()
    try:
        registry = _load_runtime_registry()
        entry = registry.get(node_key)
        return entry if isinstance(entry, dict) else None
    finally:
        _unlock_runtime_registry(lock_file)


def _store_runtime_entry(node_key, entry):
    lock_file = _lock_runtime_registry()
    try:
        registry = _load_runtime_registry()
        registry[node_key] = entry
        _write_runtime_registry(registry)
    finally:
        _unlock_runtime_registry(lock_file)


def _mark_node_from_registry(ti, node_id, task_id, entry):
    ti.xcom_push(key=f"{node_id}_task_state", value="success")
    ti.xcom_push(key=f"{node_id}_branch", value="success")
    if "tracking_id" in entry:
        ti.xcom_push(key=f"{node_id}_tracking_id", value=entry.get("tracking_id"))
    if "submit_response" in entry:
        ti.xcom_push(key=f"{node_id}_submit_response", value=entry.get("submit_response"))
    if "result" in entry:
        ti.xcom_push(key=f"{node_id}_response", value=entry.get("result"))
    return {
        "status": "idempotent_reused",
        "node_id": node_id,
        "previous_state": entry.get("state"),
        "result": entry.get("result"),
    }


def _should_force_rerun(conf, node_id):
    force_nodes = conf.get("force_rerun_nodes") or []
    if isinstance(force_nodes, str):
        force_nodes = [item.strip() for item in force_nodes.split(",") if item.strip()]
    return bool(conf.get("force_rerun")) or node_id in set(force_nodes)


def _get_conf(ctx):
    return ctx["dag_run"].conf or {}


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
    return {str(v).strip().upper() for v in values if str(v).strip()}


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
        return {k: _render_value(v, replacements) for k, v in value.items()}
    if isinstance(value, list):
        return [_render_value(v, replacements) for v in value]
    return value


def _parse_response_body(response):
    if not response.text:
        return {}
    try:
        return response.json()
    except Exception:
        return {"raw_response": response.text}


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


def _poll_status_until_terminal(node_id, node_name, payload, start_response_body, tracking_id):
    ctx = get_current_context()
    status_cfg = payload.get("status") or {}
    if not isinstance(status_cfg, dict) or not status_cfg:
        raise AirflowException(f"Missing status block for async node {node_id}")

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

    replacements = {
        "tracking_id": tracking_id or "",
        "job_id": tracking_id or "",
        "run_id": tracking_id or "",
        "runId": tracking_id or "",
        "node_runId": tracking_id or "",
        "node_run_id": tracking_id or "",
        "node_id": node_id,
        "node_name": node_name,
        "dag_id": ctx["dag"].dag_id,
        "dag_run_id": ctx["dag_run"].run_id,
        "run_control_id": RUN_CONTROL_ID,
    }

    status_url = _render_value(status_cfg.get("url"), replacements)
    if not status_url:
        raise AirflowException(f"Missing status.url for async node {node_id}")

    method = str(status_cfg.get("method") or "GET").upper()
    headers = _render_value(status_cfg.get("headers") or payload.get("headers") or {}, replacements)
    params = _render_value(status_cfg.get("params") or {}, replacements)
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
    last_status_response = {}

    while time.time() <= deadline:
        try:
            response = requests.request(
                method=method,
                url=status_url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=request_timeout,
                verify=verify_ssl,
            )
            status_body = _parse_response_body(response)
            last_status_response = status_body
        except Exception as exc:
            raise AirflowException(f"{node_id} status request failed: {exc}") from exc

        if response.status_code in (401, 403):
            raise AirflowException(
                f"{node_id} status endpoint authorization failed: HTTP {response.status_code}"
            )
        if response.status_code == 404:
            raise AirflowException(f"{node_id} status endpoint returned 404")
        if response.status_code not in HTTP_SUCCESS_CODES:
            raise AirflowException(
                f"{node_id} status endpoint returned HTTP {response.status_code}: {response.text}"
            )

        raw_status = _extract_by_path(status_body, response_status_key)
        normalized_status, status_text = _normalize_terminal_status(
            raw_status, success_statuses, failure_statuses, running_statuses
        )
        if normalized_status == "SUCCESS":
            return {
                "status": "completed",
                "node_id": node_id,
                "tracking_id": tracking_id,
                "external_status": status_text,
                "start_response": start_response_body,
                "status_response": status_body,
            }
        if normalized_status == "FAILED":
            raise AirflowException(
                f"{node_id} external status failed. "
                f"status_field={response_status_key}, value={status_text}"
            )
        time.sleep(poke_interval)

    raise AirflowException(
        f"{node_id} status polling timed out after {timeout_seconds}s. "
        f"Last response: {last_status_response}"
    )


def resolve_payload(conf, node_id, task_key=None):
    direct = conf.get(node_id)
    if isinstance(direct, dict):
        return direct
    if task_key:
        keyed = conf.get(task_key)
        if isinstance(keyed, dict):
            return keyed
    for _, value in conf.items():
        if isinstance(value, dict) and str(value.get("node_runId", "")).strip() == node_id:
            return value
    return {}


def execute_node(task_key, node_id, node_name, engine, execution_mode, executor_order_id, **kwargs):
    ctx = get_current_context()
    conf = _get_conf(ctx)
    ti = ctx["ti"]
    task_id = TASK_ID_MAP.get(node_id, node_id)

    payload = resolve_payload(conf, node_id=node_id, task_key=task_key)

    # FIX #4 — deterministic resume skip.
    # When resume=true and resume_from is set, any node whose executor_order_id
    # is strictly less than the resume_from node's order is fast-forwarded
    # without hitting the network.  The idempotency registry still provides the
    # secondary path when resume_from is not specified.
    if conf.get("resume") and not _should_force_rerun(conf, node_id):
        resume_from = conf.get("resume_from")
        if resume_from:
            resume_order = NODE_ORDER_MAP.get(resume_from)
            if resume_order is not None and executor_order_id < resume_order:
                log.info(
                    "[RESUME] Skipping node %s (order=%s) — resume_from=%s (order=%s)",
                    node_id, executor_order_id, resume_from, resume_order,
                )
                ti.xcom_push(key=f"{node_id}_task_state", value="success")
                ti.xcom_push(key=f"{node_id}_branch", value="success")
                return {"status": "resume_skipped", "node_id": node_id}

    if not payload or not payload.get("url"):
        ti.xcom_push(key=f"{node_id}_task_state", value="failed")
        ti.xcom_push(key=f"{node_id}_branch", value="failure")
        raise AirflowException(f"Missing URL for node {node_id} ({node_name})")

    node_key = _make_node_idempotency_key(ctx, node_id, payload)
    existing_entry = _get_runtime_entry(node_key)
    if existing_entry and existing_entry.get("state") == "success" and not _should_force_rerun(conf, node_id):
        log.info(
            "[IDEMPOTENCY] Reusing completed node %s for correlation_id=%s",
            node_id, _get_correlation_id(ctx),
        )
        return _mark_node_from_registry(ti, node_id, task_id, existing_entry)

    ti.xcom_push(key=f"{node_id}_task_state", value="started")
    ti.xcom_push(key=f"{node_id}_idempotency_key", value=node_key)
    _store_runtime_entry(
        node_key,
        {
            "state": "started",
            "node_id": node_id,
            "node_name": node_name,
            "execution_mode": execution_mode,
            "dag_id": ctx["dag"].dag_id,
            "dag_run_id": ctx["dag_run"].run_id,
            "correlation_id": _get_correlation_id(ctx),
            "started_at": _utc_now(),
        },
    )

    request_timeout = int(payload.get("timeout", 300))
    verify_ssl = payload.get("verify_ssl", False)

    try:
        response = requests.request(
            method=payload.get("method", "POST"),
            url=payload["url"],
            json=payload.get("json"),
            params=payload.get("params"),
            headers=payload.get("headers", {}),
            timeout=request_timeout,
            verify=verify_ssl,
        )
        response_body = _parse_response_body(response)
    except Exception as exc:
        ti.xcom_push(key=f"{node_id}_task_state", value="failed")
        ti.xcom_push(key=f"{node_id}_branch", value="failure")
        ti.xcom_push(key=f"{node_id}_error", value=str(exc))
        _store_runtime_entry(
            node_key,
            {
                "state": "failed",
                "node_id": node_id,
                "node_name": node_name,
                "execution_mode": execution_mode,
                "dag_id": ctx["dag"].dag_id,
                "dag_run_id": ctx["dag_run"].run_id,
                "correlation_id": _get_correlation_id(ctx),
                "error": str(exc),
                "finished_at": _utc_now(),
            },
        )
        raise AirflowException(str(exc))

    if response.status_code in (401, 403):
        error_message = f"HTTP {response.status_code} for {node_id}. Check credentials."
        ti.xcom_push(key=f"{node_id}_task_state", value="failed")
        ti.xcom_push(key=f"{node_id}_branch", value="failure")
        ti.xcom_push(key=f"{node_id}_error", value=error_message)
        _store_runtime_entry(
            node_key,
            {
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
            },
        )
        raise AirflowException(error_message)

    if response.status_code not in HTTP_SUCCESS_CODES:
        error_message = f"HTTP {response.status_code}: {response.text}"
        ti.xcom_push(key=f"{node_id}_task_state", value="failed")
        ti.xcom_push(key=f"{node_id}_branch", value="failure")
        ti.xcom_push(key=f"{node_id}_error", value=error_message)
        _store_runtime_entry(
            node_key,
            {
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
            },
        )
        raise AirflowException(error_message)

    if execution_mode == "fire_and_forget":
        result = {"status": "submitted_no_track", "node_id": node_id, "submit_response": response_body}
        ti.xcom_push(key=f"{node_id}_submit_response", value=response_body)
        ti.xcom_push(key=f"{node_id}_submit_http_status", value=response.status_code)
        ti.xcom_push(key=f"{node_id}_response", value=result)
        ti.xcom_push(key=f"{node_id}_task_state", value="success")
        ti.xcom_push(key=f"{node_id}_branch", value="success")
        _store_runtime_entry(
            node_key,
            {
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
            },
        )
        return result

    if execution_mode == "async_no_wait":
        status_cfg = payload.get("status") or {}
        response_id_key = (
            status_cfg.get("response_id_key")
            or payload.get("response_id_key")
            or "job_id"
        )
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
        ti.xcom_push(key=f"{node_id}_submit_response", value=response_body)
        ti.xcom_push(key=f"{node_id}_tracking_id", value=tracking_id)
        ti.xcom_push(key=f"{node_id}_submit_http_status", value=response.status_code)
        try:
            result = _poll_status_until_terminal(node_id, node_name, payload, response_body, tracking_id)
        except Exception as exc:
            ti.xcom_push(key=f"{node_id}_task_state", value="failed")
            ti.xcom_push(key=f"{node_id}_branch", value="failure")
            ti.xcom_push(key=f"{node_id}_error", value=str(exc))
            _store_runtime_entry(
                node_key,
                {
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
                },
            )
            raise
        ti.xcom_push(key=f"{node_id}_response", value=result)
        ti.xcom_push(key=f"{node_id}_task_state", value="success")
        ti.xcom_push(key=f"{node_id}_branch", value="success")
        _store_runtime_entry(
            node_key,
            {
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
            },
        )
        return result

    # sync execution
    result = response_body
    ti.xcom_push(key=f"{node_id}_response", value=result)
    ti.xcom_push(key=f"{node_id}_task_state", value="success")
    ti.xcom_push(key=f"{node_id}_branch", value="success")
    _store_runtime_entry(
        node_key,
        {
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
        },
    )
    return result


def choose_branch(node_task_id, node_id, success_task_ids, failure_task_ids, **kwargs):
    ctx = get_current_context()
    ti = ctx["ti"]
    marker = ti.xcom_pull(task_ids=node_task_id, key=f"{node_id}_branch")

    if marker == "success":
        if not success_task_ids:
            raise AirflowException(f"No success_task_ids configured for {node_task_id}")
        return success_task_ids

    if not failure_task_ids:
        raise AirflowException(f"No failure_task_ids configured for {node_task_id}")
    return failure_task_ids


def _check_async_status(node_id, node_label, payload):
    ctx = get_current_context()
    ti = ctx["ti"]

    async_cfg = payload.get("async_status", {})
    verify_ssl = async_cfg.get("verify_ssl", payload.get("verify_ssl", False))
    request_timeout = int(async_cfg.get("timeout", payload.get("timeout", 300)))
    tracking_id = (
        ti.xcom_pull(task_ids=TASK_ID_MAP.get(node_id, node_id), key=f"{node_id}_tracking_id")
        or async_cfg.get("tracking_id")
        or payload.get("tracking_id")
    )
    submit_response = (
        ti.xcom_pull(task_ids=TASK_ID_MAP.get(node_id, node_id), key=f"{node_id}_submit_response") or {}
    )

    replacements = {
        "tracking_id": tracking_id or "",
        "job_id": tracking_id or "",
        "node_id": node_id,
        "dag_id": ctx["dag"].dag_id,
        "dag_run_id": ctx["dag_run"].run_id,
        "run_control_id": RUN_CONTROL_ID,
    }

    status_url = _render_value(async_cfg.get("url") or payload.get("status_url"), replacements)
    method = str(async_cfg.get("method") or payload.get("status_method") or "GET").upper()
    headers = _render_value(
        async_cfg.get("headers") or payload.get("status_headers") or payload.get("headers") or {},
        replacements,
    )
    params = _render_value(async_cfg.get("params") or payload.get("status_params") or {}, replacements)
    json_body = _render_value(async_cfg.get("json") or payload.get("status_json") or None, replacements)

    response_status_key = async_cfg.get("response_status_key") or payload.get("response_status_key") or "status"
    response_id_key = async_cfg.get("response_id_key") or payload.get("response_id_key") or "job_id"

    success_statuses = _to_upper_set(
        async_cfg.get("success_statuses") or payload.get("success_statuses"),
        DEFAULT_ASYNC_SUCCESS_STATUSES,
    )
    failure_statuses = _to_upper_set(
        async_cfg.get("failure_statuses") or payload.get("failure_statuses"),
        DEFAULT_ASYNC_FAILURE_STATUSES,
    )
    running_statuses = _to_upper_set(
        async_cfg.get("running_statuses") or payload.get("running_statuses"),
        DEFAULT_ASYNC_RUNNING_STATUSES,
    )

    if not status_url:
        return {
            "task_id": node_id,
            "name": node_label,
            "state": "unknown",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "reason": "Missing status URL for async node",
        }, "UNKNOWN"

    if not tracking_id:
        tracking_id = _extract_by_path(submit_response, response_id_key)

    try:
        response = requests.request(
            method=method,
            url=status_url,
            headers=headers,
            params=params,
            json=json_body,
            timeout=request_timeout,
            verify=verify_ssl,
        )
        status_body = _parse_response_body(response)
    except Exception as exc:
        return {
            "task_id": node_id,
            "name": node_label,
            "state": "unknown",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "reason": f"Status API call failed: {exc}",
        }, "UNKNOWN"

    if response.status_code in (401, 403):
        return {
            "task_id": node_id,
            "name": node_label,
            "state": "failed",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "http_status": response.status_code,
            "reason": f"Status endpoint returned {response.status_code}",
            "status_response": status_body,
        }, "FAILED"

    if response.status_code == 404:
        return {
            "task_id": node_id,
            "name": node_label,
            "state": "failed",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "http_status": response.status_code,
            "reason": "Status endpoint returned 404",
            "status_response": status_body,
        }, "FAILED"

    if response.status_code >= 500:
        return {
            "task_id": node_id,
            "name": node_label,
            "state": "unknown",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "http_status": response.status_code,
            "reason": "Status endpoint returned 5xx",
            "status_response": status_body,
        }, "UNKNOWN"

    if response.status_code not in HTTP_SUCCESS_CODES:
        return {
            "task_id": node_id,
            "name": node_label,
            "state": "unknown",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "http_status": response.status_code,
            "reason": f"Unexpected HTTP code {response.status_code}",
            "status_response": status_body,
        }, "UNKNOWN"

    if not tracking_id:
        tracking_id = _extract_by_path(status_body, response_id_key)

    raw_status = _extract_by_path(status_body, response_status_key)

    if raw_status is None:
        return {
            "task_id": node_id,
            "name": node_label,
            "state": "unknown",
            "status_source": "external_async_status",
            "tracking_id": tracking_id,
            "http_status": response.status_code,
            "reason": f"Missing status field: {response_status_key}",
            "status_response": status_body,
        }, "UNKNOWN"

    normalized_status, status_text = _normalize_terminal_status(
        raw_status, success_statuses, failure_statuses, running_statuses
    )

    return {
        "task_id": node_id,
        "name": node_label,
        "state": status_text.lower() if status_text else "unknown",
        "normalized_status": normalized_status,
        "status_source": "external_async_status",
        "tracking_id": tracking_id,
        "http_status": response.status_code,
        "status_response": status_body,
    }, normalized_status


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
        task_id = TASK_ID_MAP.get(node_id, node_id)
        payload = resolve_payload(conf, node_id=node_id, task_key=node_id)

        marker = ti.xcom_pull(task_ids=task_id, key=f"{node_id}_task_state")
        marker = str(marker).lower().strip() if marker is not None else ""

        if node_id in FIRE_AND_FORGET_NODE_IDS:
            entry = {
                "task_id": task_id,
                "name": node_label,
                "state": "submitted_no_track" if marker == "success" else (marker or "unknown"),
                "status_source": "airflow_submit_only",
            }
            if marker == "success":
                success.append(entry)
            elif marker == "failed":
                failed.append(entry)
            elif not marker and node_id in BRANCH_SKIP_WHITELIST and node_id not in MERGE_NODE_IDS:
                expected_skipped.append({**entry, "skip_type": "expected_branch_skip"})
            else:
                unknown.append(entry)
            continue

        entry = {
            "task_id": task_id,
            "name": node_label,
            "state": marker or "unknown",
            "status_source": "xcom_task_state_marker",
        }

        if marker == "success":
            success.append(entry)
        elif marker == "failed":
            failed.append(entry)
        elif not marker:
            if node_id in BRANCH_SKIP_WHITELIST and node_id not in MERGE_NODE_IDS:
                expected_skipped.append({**entry, "skip_type": "expected_branch_skip"})
            else:
                unknown.append(entry)
        else:
            unknown.append(entry)

    has_any_problem = bool(failed or unexpected_skipped or running or unknown)
    final_status = "FAILED" if has_any_problem else "SUCCESS"

    summary = {
        "final_status": final_status,
        "successful_tasks": success,
        "failed_tasks": failed,
        "expected_skipped_tasks": expected_skipped,
        "unexpected_skipped_tasks": unexpected_skipped,
        "running_tasks": running,
        "unknown_tasks": unknown,
    }

    ti.xcom_push(key="final_status", value=final_status)
    ti.xcom_push(key="final_summary", value=summary)

    if has_any_problem:
        raise AirflowException(
            f"Run completed with problems. "
            f"failed={len(failed)}, unexpected_skipped={len(unexpected_skipped)}, "
            f"running={len(running)}, unknown={len(unknown)}"
        )

    return summary


def build_run_started_event_messages():
    ctx = get_current_context()
    dag_run = ctx["dag_run"]
    payload = {
        "eventType": __EVENT_RUN_STARTED__,
        "run_control_id": RUN_CONTROL_ID,
        "correlation_id": _get_correlation_id(ctx),
        "event_source": "AIRFLOW",
        "status": "RUNNING",
        "trigger_payload": _safe_json(_get_conf(ctx)),
        "dagId": ctx["dag"].dag_id,
        "dagRunId": dag_run.run_id,
        "timestamp": _utc_now(),
    }
    yield (dag_run.run_id, _safe_json(payload).encode())


def build_run_final_event_messages():
    ctx = get_current_context()
    dag_run = ctx["dag_run"]
    ti = ctx["ti"]
    status_value = ti.xcom_pull(task_ids="finalize_results", key="final_status") or "FAILED"
    payload = {
        "eventType": __EVENT_RUN_SUCCEEDED__ if status_value == "SUCCESS" else __EVENT_RUN_FAILED__,
        "run_control_id": RUN_CONTROL_ID,
        "correlation_id": _get_correlation_id(ctx),
        "event_source": "AIRFLOW",
        "status": status_value,
        "trigger_payload": _safe_json(_get_conf(ctx)),
        "dagId": ctx["dag"].dag_id,
        "dagRunId": dag_run.run_id,
        "timestamp": _utc_now(),
    }
    yield (dag_run.run_id, _safe_json(payload).encode())


with DAG(
    dag_id=__DAG_ID__,
    schedule=__SCHEDULE__,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["dynamic", "generated"],
) as dag:
    prepare_inputs_task = PythonOperator(
        task_id="prepare_inputs",
        python_callable=lambda: True,
    )

    run_started_event = ProduceToTopicOperator(
        task_id="run_started_event",
        kafka_config_id=KAFKA_CONN_ID,
        topic=KAFKA_RUN_TOPIC,
        producer_function=build_run_started_event_messages,
    )

    __JOINED_GROUP_DEFS__

    __JOINED_TASK_DEFS__

    finalize_results_task = PythonOperator(
        task_id="finalize_results",
        python_callable=finalize_results,
        op_kwargs={"node_ids": FINAL_NODE_IDS},
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )

    run_final_event = ProduceToTopicOperator(
        task_id="run_final_event",
        kafka_config_id=KAFKA_CONN_ID,
        topic=KAFKA_RUN_TOPIC,
        producer_function=build_run_final_event_messages,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    prepare_inputs_task >> run_started_event
    __JOINED_DEPS__
"""

    replacements = {
        "__KAFKA_CONN_ID__": repr(KAFKA_CONN_ID),
        "__KAFKA_RUN_TOPIC__": repr(KAFKA_RUN_TOPIC),
        "__RUN_CONTROL_ID__": repr(run_control_id),
        "__FINAL_NODE_IDS__": repr(node_ids),
        "__ASYNC_NODE_IDS__": repr(async_node_ids),
        "__FIRE_AND_FORGET_NODE_IDS__": repr(fire_and_forget_node_ids),
        "__NODE_NAME_MAP__": repr(node_name_map),
        "__BRANCH_SKIP_WHITELIST__": repr(sorted(branch_skip_whitelist)),
        "__MERGE_NODE_IDS__": repr(sorted(merge_task_ids)),
        "__NODE_ORDER_MAP__": repr(node_order_map),
        "__TASK_ID_MAP__": repr(task_id_map),
        "__EVENT_RUN_STARTED__": repr(EVENTS["run_started"]),
        "__EVENT_RUN_SUCCEEDED__": repr(EVENTS["run_succeeded"]),
        "__EVENT_RUN_FAILED__": repr(EVENTS["run_failed"]),
        "__DAG_ID__": repr(dag_id),
        "__SCHEDULE__": schedule_str,
        "__JOINED_GROUP_DEFS__": joined_group_defs,
        "__JOINED_TASK_DEFS__": joined_task_defs,
        "__JOINED_DEPS__": joined_deps,
    }

    result = template
    for key, val in replacements.items():
        result = result.replace(key, val)

    return result


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

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    dag_file_name = f"{dag_id}_{timestamp}.py"
    dag_file_path = DAGS_FOLDER / dag_file_name

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
        "payload_hash": idempotency_key,
    }
    store_registry_entry(idempotency_key, entry)

    logger.info("DAG generated: %s", dag_id)

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
