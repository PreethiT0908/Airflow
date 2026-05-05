# Branching & Merging — Visual Reference Guide (airflow_job1 v1)

## Overview

There are **two completely different concepts** in this DAG system that people sometimes confuse:

| Concept | What it is | When to use |
|---------|-----------|-------------|
| **Parallel Lanes** | Multiple nodes with the same `executor_order_id`, different `executor_sequence_id` | Run independent jobs simultaneously |
| **Status Branching** | One node routes to different next-nodes based on its success or failure | Conditional workflow paths |

---

## Part 1 — Parallel Lanes (No Branching)

This is the simplest multi-node pattern. Nodes in the **same layer** run concurrently.

### Build Payload Shape

```json
{
  "nodes": [
    { "id": "task1", "executor_order_id": 1, "executor_sequence_id": 1, "branch_on_status": false },
    { "id": "task2", "executor_order_id": 2, "executor_sequence_id": 1, "branch_on_status": false },
    { "id": "task3", "executor_order_id": 2, "executor_sequence_id": 2, "branch_on_status": false },
    { "id": "task4", "executor_order_id": 2, "executor_sequence_id": 3, "branch_on_status": false },
    { "id": "task5", "executor_order_id": 3, "executor_sequence_id": 1, "branch_on_status": false }
  ]
}
```

### Flow Diagram

```
                    ┌─────────────────┐
                    │   run_started   │
                    │     (Kafka)     │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │     task1       │  order=1
                    │     (sync)      │
                    └────────┬────────┘
                             │
           ┌─────────────────┼─────────────────┐
           │                 │                 │
  ┌────────▼───────┐ ┌───────▼────────┐ ┌─────▼──────────┐
  │    task2       │ │    task3       │ │    task4       │
  │ order=2, seq=1 │ │ order=2, seq=2 │ │ order=2, seq=3 │
  │    (sync)      │ │  (async_poll)  │ │ (fire_forget)  │
  └────────┬───────┘ └───────┬────────┘ └─────┬──────────┘
           │                 │                 │
           └─────────────────┼─────────────────┘
                             │
                    ┌────────▼────────┐
                    │     task5       │  order=3
                    │     (sync)      │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │ finalize_results │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  run_final_event │
                    │     (Kafka)     │
                    └─────────────────┘
```

### Merge Behaviour (task5 waiting for task2, task3, task4)

task5 has **3 upstream tasks**. This makes it a **merge node**.

| Upstream Task | Mode | Is it waited on? |
|--------------|------|-----------------|
| task2 | sync | ✅ Yes — must succeed |
| task3 | async_no_wait | ✅ Yes — polls until terminal |
| task4 | fire_and_forget | ❌ No — excluded from merge gate |

**The merge guard checks that all sync + async upstream nodes have XCom state = `success` before allowing task5 to proceed.**

If task4 is fire_and_forget and did not succeed (HTTP error at submit), task5 still proceeds because fire_and_forget is never waited on.

If task2 or task3 fail → task5 is **blocked** and raises an exception.

---

## Part 2 — Status Branching (branch_on_status: true)

This is for **conditional routing** — the workflow takes a different path depending on whether a node succeeded or failed.

### Build Payload Shape

```json
{
  "nodes": [
    {
      "id": "task1",
      "executor_order_id": 1,
      "executor_sequence_id": 1,
      "branch_on_status": true,
      "on_success_node_ids": ["task2"],
      "on_failure_node_ids": ["task3"]
    },
    { "id": "task2", "executor_order_id": 2, "executor_sequence_id": 1, "branch_on_status": false },
    { "id": "task3", "executor_order_id": 2, "executor_sequence_id": 1, "branch_on_status": false },
    { "id": "task4", "executor_order_id": 3, "executor_sequence_id": 1, "branch_on_status": false }
  ]
}
```

### Flow Diagram — Success Path

```
              ┌──────────────────┐
              │     task1        │  branch_on_status: true
              │  (sync/async)    │
              └────────┬─────────┘
                       │
              ┌────────▼──────────┐
              │  branch__task1    │  BranchPythonOperator
              │ (reads XCom:      │  checks {task1_id}_branch
              │  success/failure) │
              └──┬─────────────┬──┘
                 │             │
         SUCCESS │             │ FAILURE
                 │             │
       ┌─────────▼──┐    ┌─────▼──────────┐
       │   task2    │    │    task3        │  ← SKIPPED on success path
       │ (executes) │    │   (SKIPPED)     │
       └─────────┬──┘    └─────────────────┘
                 │
       ┌─────────▼──────────┐
       │       task4         │  merge node: NONE_FAILED_MIN_ONE_SUCCESS
       │   (always runs)     │  ← guaranteed to execute
       └─────────────────────┘
```

### Flow Diagram — Failure Path

```
              ┌──────────────────┐
              │     task1        │  branch_on_status: true
              │    (FAILED)      │
              └────────┬─────────┘
                       │
              ┌────────▼──────────┐
              │  branch__task1    │
              └──┬─────────────┬──┘
                 │             │
         SUCCESS │             │ FAILURE
                 │             │
       ┌─────────▼──┐    ┌─────▼──────────┐
       │   task2    │    │    task3        │  ← executes on failure path
       │  (SKIPPED) │    │   (executes)   │
       └────────────┘    └─────┬──────────┘
                               │
                     ┌─────────▼──────────┐
                     │       task4         │  still executes (merge guard
                     │   (always runs)     │  sees task3 succeeded,
                     └─────────────────────┘  task2 skipped = expected skip)
```

---

## Part 3 — The Merge Problem & How It Is Fixed

### The Question You Asked

> "If task1 branches to task2 (success) and task3 (failure), and task4 comes after both — does task4 get skipped if task3 was skipped?"

**Answer: No — task4 must NOT be skipped, and this is the fix applied in v1.**

### How Airflow Handles This By Default (The Problem)

Without special handling, when task3 is Airflow-skipped (because the success branch was taken), the default `ALL_SUCCESS` trigger rule on task4 would cause task4 to also be **skipped** because one of its upstreams (task3) is in a "skipped" state.

This is the classic Airflow branching propagation problem.

### The Fix Applied

Merge nodes — nodes with **more than one upstream task** — receive `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS`.

```
NONE_FAILED_MIN_ONE_SUCCESS means:
  - At least one upstream task succeeded
  - None of the upstream tasks failed (failed, not skipped)
  - Skipped upstreams are ALLOWED and do not block execution
```

This ensures task4 runs as long as **one branch executed and succeeded**.

### Merge Guard (Additional Safety)

Beyond the trigger rule, a **merge guard** runs inside `execute_node` at the start of any merge node. It checks:

- For each upstream sync/async node → XCom `{node_id}_task_state` must be `"success"` OR the node must be in `BRANCH_SKIP_WHITELIST` (meaning it was an expected skip due to branching)
- Fire-and-forget upstream nodes are **excluded** from this check
- If an upstream sync/async node has no XCom marker at all (neither success nor expected-skip) → the merge guard raises an exception

```
Merge Guard Decision Table
──────────────────────────────────────────────────────
Upstream XCom State     | In Skip Whitelist? | Decision
──────────────────────────────────────────────────────
"success"               | any               | ✅ PASS
(empty / None)          | YES               | ✅ PASS (expected branch skip)
(empty / None)          | NO                | ❌ FAIL — unexpected skip
"failed"                | any               | ❌ FAIL
──────────────────────────────────────────────────────
```

---

## Part 4 — All Patterns at a Glance

### Pattern A: Simple Linear (no branching, no parallel)

```
task1 → task2 → task3
```
All nodes have unique executor_order_id (1, 2, 3). No branching. No merge.

---

### Pattern B: Parallel Fan-Out, then Merge

```
            task1
         ┌────┴────┐
       task2     task3
         └────┬────┘
            task4        ← merge node
```
task2 and task3: same order_id, different sequence_id.
task4: merge node with `NONE_FAILED_MIN_ONE_SUCCESS` + merge guard.

---

### Pattern C: Success/Failure Branch, then Merge

```
          task1  (branch_on_status: true)
            │
         [branch router]
         ┌──┴──┐
       task2  task3
         └──┬──┘
          task4          ← merge node (must execute regardless)
```

---

### Pattern D: Branch with No Merge (Terminal Paths)

```
          task1  (branch_on_status: true)
            │
         [branch router]
         ┌──┴──┐
       task2  task3
         │      │
        (end)  (end)     ← finalize_results collects both
```

Both task2 and task3 are terminal nodes. `finalize_results` uses `ALL_DONE` trigger rule and collects XCom from both. The skipped one is recorded as `expected_skipped` (not a failure).

---

### Pattern E: Nested Branch (Branch within a Branch)

```
           task1  (branch_on_status: true)
             │
          [branch1]
          ┌──┴──┐
        task2  task3  (task3: branch_on_status: true)
                │
           [branch3]
           ┌────┴────┐
         task4     task5
```

task3 itself branches. task4 and task5 are terminal. task2, task4, task5 all feed `finalize_results`.

---

## Part 5 — Validation Rules (Build Phase)

| Rule | Error if violated |
|------|------------------|
| `branch_on_status: true` must have at least one of `on_success_node_ids` or `on_failure_node_ids` | `ValueError` |
| `branch_on_status: false` must have empty `on_success_node_ids` and `on_failure_node_ids` | `ValueError` |
| All IDs in `on_success_node_ids` / `on_failure_node_ids` must exist in the nodes list | `ValueError` |
| `fire_and_forget` nodes cannot have `branch_on_status: true` | `ValueError` |
| A node cannot be in both `on_success_node_ids` and `on_failure_node_ids` of the same parent | `ValueError` |

---

## Part 6 — When to Use What

```
DECISION GUIDE
──────────────────────────────────────────────────────────────────────
Q: Do you want multiple tasks to run at the same time?
→ Use parallel lanes (same executor_order_id, different sequence_id)

Q: Do you want the next step to depend on whether this task succeeded?
→ Use branch_on_status: true with on_success_node_ids / on_failure_node_ids

Q: Do you want all parallel paths to converge back to one task?
→ The converging task becomes a merge node automatically (detected by
  having multiple upstreams). Use NONE_FAILED_MIN_ONE_SUCCESS + merge guard.

Q: Can a fire_and_forget node be a branching node?
→ NO. fire_and_forget does not track terminal status, so branching is undefined.

Q: Can a merge node itself branch?
→ YES. A merge node can have branch_on_status: true and route based on its own result.
──────────────────────────────────────────────────────────────────────
```
