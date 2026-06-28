# tickflow

A small Petri-style workflow control framework for building short, auditable,
reversible flows. You describe the graph in a mermaid-like syntax; the engine
runs it as synchronous Petri-net *steps* over a boolean slot marking, with
all runtime state centralized in `RunState` — making snapshots, pause, rewind,
and replay cheap.

> Is this a state machine? No — it's a **Petri net** (specifically a marked
> graph with AND/OR joins). A finite state machine is the degenerate case
> where exactly one token is live in the whole net at any time. `tickflow`
> supports multiple concurrent starts, AND-joins (wait for all upstream),
> OR-joins (fire on any upstream), and cycles — none of which plain FSMs
> express natively. See [Design notes](#design-notes).

## Quick start

```bash
python -m tickflow run examples/counter_loop.txt -b examples/counter_loop_beh.py
```

The graph file declares **structure only**; a Python "behaviours" file
registers the actual body/guard callables on `tickflow.registry`.

### Graph syntax

```
[A]-->B                  # A is a start node; plain edge A→B (always True)
B--|g1|-->C              # guarded edge: slot is True iff guard g1(view) is True
B--|g1|-->A              # cycle / loop-back edge
C.inputs: A, B[2]        # C reads A (latest_before) and B's 2nd fire (1-based)
C.body: compute_c        # C's body is the registered callable compute_c
C.join: OR               # override join (AND is the default)
```

- `[A]` marks a **start node**. Multiple starts are allowed and fire
  concurrently at tick 0.
- `-->` is a plain data-flow edge: it always writes `True` into the
  downstream slot when its source fires.
- `--|name|-->` is a guarded edge: it writes `guard(view)` (so a failing
  guard writes **`False`**, an explicit clobber — a stale `True` from a
  previous iteration can never leak into a loop).
- `node.inputs:` and `node.body:` lines bind behaviour. Both are optional
  (default body = identity/echo; default inputs = every producer, each with
  `latest_before`). `inputs` may reference non-producer nodes (e.g. `A[k]`
  to pin A's k-th fire) as long as the referenced node is **upstream** in
  the graph (has a directed path to the consumer).
- `#` starts a comment.

### Behaviours file

```python
from tickflow import registry
from tickflow.views import Missing

@registry.body("incr")
def incr(v):
    return v.A.value + 1          # v.A is the resolved value of producer A

@registry.guard("cont_lt3")
def cont(v):
    return v.B.value < 3          # guard on edge out of B sees B's output
```

A body or guard receives a `DictView`: `v.A` (or `v["A"]`) resolves the
declared producer per the node's input policy. `v.A.value` is the bare value;
`v.A.k` is the fire index used (or `None` for `latest_before`). A producer
with no qualifying fire yet yields the `Missing` sentinel (falsy).

## Architecture

### RunState — single source of truth

All runtime state lives in `RunState`, organized in three internal layers
with distinct responsibilities:

```
RunState
├── _edges   dict[node, list[(tick, output)]]    always maintained, for resolve()
├── _state   dict[node, dict[str, Any]]          always maintained, O(1) mutable state
└── _records list[NodeState]                     only when keep_records=True, full audit trail
```

- **`_edges`** — fast-lookup index for `resolve()` (input resolution during
  tick execution). Replaces the old `History` class.
- **`_state`** — current mutable state per node (what bodies write via
  `view.state`). Always maintained, O(1) access. Replaces the old
  `Marking.node_state`.
- **`_records`** — full `NodeState` records (inputs, output, edges_fired,
  status, error, mutable_state). Controlled by `keep_records`.

Derived artefacts are extracted from these three layers:

| Artefact | Source | Controlled by |
|----------|--------|---------------|
| `resolve()` — input resolution | `_edges` | always available |
| `mutable_state()` — node state | `_state` | always available |
| `firings_of()` — output history | `_edges` | always available |
| `audit()` — full audit log | `_records` | `keep_records` |
| `to_snapshot_data()` — snapshot | all three | `records` key only when `keep_records=True` |

### NodeState — one firing, all data

```python
@dataclass
class NodeState:
    tick: int                          # which tick
    node: str                          # which node
    inputs: dict[str, Any]             # resolved input values
    output: Any                        # body return value
    edges_fired: list[tuple[...]]      # downstream edge results (filled in Phase B)
    status: "ok" | "failed" | "aborted"
    error: str | None
    mutable_state: dict[str, Any]      # node state after body ran
```

This is the **single source of truth** for everything that happened to a node
at a given tick. It replaces the old `Firing` class (same fields, unified
ownership).

### Memory-saving mode

```python
rn = Runner(graph, registry, keep_records=False)
```

When `keep_records=False`, `_records` is not populated — saving memory — but
`_edges` and `_state` are still maintained. Input resolution and node mutable
state work correctly regardless of this switch. The snapshot omits the
`"records"` key. Backend persistence (firings, snapshots) is unaffected.

## Semantics

### The model

The engine is a **pure function**:

```
tick: (marking_t, run_state, t) -> (marking_{t+1}, firings_t)
```

Only two mutable state containers exist:
- **marking** — `dict[(dst, src), bool]`, one boolean *slot* per incoming
  edge, plus a set of *armed* start nodes (one-shot).
- **run_state** — `RunState` instance (all history, state, audit).

There is no hidden in-flight state. That's what makes snapshots cheap.

### A tick

1. Compute the set of *fireable* nodes: a node fires iff
   - it is an armed start (fires once, then disarmed), **or**
   - its join predicate holds over its input slots: `all` for AND, `any`
     for OR.
2. Each fireable node fires concurrently: its body reads inputs
   (`latest_before(t)` — strictly prior ticks, so peers can't see each
   other's same-tick writes), its output is recorded in `RunState`, and its
   input slots are consumed (reset to `False`).
3. After all fires, each fired node's out-edges write into downstream slots:
   plain edges write `True`, guarded edges write `guard(view)`.

### Input policies

- **`latest`** (default): the producer's most recent fire with `tick < t`.
  This is the marking-consistent read — a node cannot see a peer's same-tick
  write. In a loop, this means "the previous iteration's output".
- **`A[k]`** (index): the producer's `k`-th fire overall (1-based),
  independent of tick. For cross-iteration pinning and audit replays.

A producer with no qualifying fire yields `Missing` (falsy); bodies are
expected to handle it.

Inputs may reference nodes that are not direct producers (no edge into the
consumer), e.g. `C.inputs: A[1]` where A is upstream of C via a longer path.
This is valid as long as A has a directed path to C (so A fires before C).

### Joins

- **AND-join** (default): fire iff *all* input slots are `True`. Use for
  "wait for all upstream".
- **OR-join**: fire iff *≥1* input slot is `True`. Declare with
  `node.join: OR`. Use when a node has multiple producers but should fire on
  any one (e.g. a node that's both a loop member and seeded by a one-shot
  start).

### Deadlock detection

If an AND-join `M` has ≥2 producers lying on **mutually-exclusive branches**
of an XOR-splitter `B` (a node with ≥2 guarded out-edges), `M` would
deadlock: each fire of `B` sets at most one branch's slots `True`, so `M`
waits forever for the other half. The checker flags this and (in the CLI)
prompts to promote `M` to OR-join. OR-joins don't deadlock here because the
synchronous step semantics make the "≥1 slot" predicate decidable — no
"will more tokens arrive?" question (the open Petri-net OR-join problem).

```python
from tickflow import parse, check, promote
g = parse(text, registry=r)
for s in check(g):       # list[DeadlockSuggestion]
    promote(s, g)        # flips s.node.join to "OR"
```

Constructing a `Runner` with unresolved suggestions raises `DeadlockError`
(no silent deadlocks).

### Static warnings (parse time)

The parser emits warnings for common pitfalls:

| Warning | Condition |
|---------|-----------|
| Consumer reads from bodyless producer | `C.inputs` references a node with no body — C will receive `None` |
| Non-producer input | `C.inputs` references a node not connected by an edge — resolves via history, not token flow |
| Unguarded cycle | A cycle has no guarded edge — will loop forever |

At runtime, the engine warns when a node returning `Failure` has guarded
out-edges (guards are never evaluated for failed nodes — all out-edges
write `False`).

## Snapshot, pause, rewind

```python
rn = Runner(graph, registry)
rn.run_until_idle(max_ticks=100, pause_at={5})   # stop before tick 5
snap = rn.snapshot()                              # JSON-able dict
rn.run_until_idle(max_ticks=100)                  # finish
rn.restore(snap)                                  # rewind to tick 5
rn.run_until_idle(max_ticks=100)                  # replay (identical if bodies pure)
```

- **`snapshot()`** returns `{"tick", "marking", "run_state", "status",
  "cancel_reason", "fireable"}` — pure JSON. `run_state` contains `edges`
  (output index), `state` (mutable state per node), and `records` (full audit,
  only when `keep_records=True`).
- **`restore(snap)`** rewinds: sets tick/marking/run_state/status from `snap`.
  RunState records with `tick >= snap["tick"]` are dropped. A restored
  terminal status (ABORTED/CANCELLED/FAILED) is reset to IDLE so the run can
  resume.
- **`pause_at={n}`** stops at the tick boundary before tick `n` — no
  half-fired state to save.
- **Branching / what-if**: `copy.deepcopy(snap)` and `restore` into separate
  Runners. The library doesn't maintain a timeline forest.
- **`to_json()` / `from_json()`** serialize the full state as a single JSON
  object. The graph and registry are *not* stored — supply them on reload.

**Body purity**: bodies should be pure functions of their input view (state
writes aside). If a body is non-pure, restore-then-replay may diverge from the
original run (the audit log still records what originally happened).

## Failure, status, and control

### Failure (body error signalling)

A body may return `Failure(error, type=...)` instead of a normal value:

```python
from tickflow import Failure

@registry.body("call_llm")
def call_llm(v):
    try:
        return llm_client.chat(...)
    except NetworkError as e:
        return Failure(str(e), type="infrastructure")  # halts the run
    except ParseError as e:
        return Failure(str(e), type="llm")              # downstream skipped
```

- **`type="llm"`** (default): a logical/recoverable failure. The node's
  out-edges write `False`, so downstream AND-joins don't fire (= "upstream
  failed, skip downstream"). The run continues.
- **`type="infrastructure"`**: an unrecoverable failure. Out-edges write
  `False` **and** the runner enters `ABORTED`, halting all further ticks.

A `Failure` is still recorded in `RunState` and the audit log with
`NodeState.status` (`"failed"` / `"aborted"`) and `NodeState.error`.

> **Important**: a failed node writes `False` to **all** out-edges —
> guarded edges are not evaluated. To implement controllable routing
> (e.g. retry on failure), have the body return a result dict and let the
> guard inspect the output value, rather than returning `Failure`.

### RunStatus

`Runner.status` is a `RunStatus` enum:

| Status | Meaning |
|--------|---------|
| `IDLE` | quiescent (nothing fired last tick, or never started) |
| `RUNNING` | a tick fired (transient; becomes IDLE/terminal next) |
| `ABORTED` | an infrastructure `Failure` occurred; halted |
| `CANCELLED` | `cancel()` was called; halted |
| `FAILED` | (reserved) all nodes failed and nothing fireable |

```python
rn.cancel("user requested")        # -> CANCELLED; ticks become no-ops
rn.is_idle()                       # status == IDLE
rn.is_terminal()                   # ABORTED/CANCELLED/FAILED, or IDLE with nothing pending
rn.reset()                         # clear a non-RUNNING status back to IDLE
```

### Node state (`view.state`)

Each node has a mutable state dict managed by `RunState` (so it participates
in snapshots/restore) and is visible to guards. Bodies read/write their own
state; guards receive a read-only view:

```python
@registry.body("retryable")
def retryable(v):
    v.state["attempts"] = v.state.get("attempts", 0) + 1
    return {"ok": v.state["attempts"] >= 3, "attempt": v.state["attempts"]}

@registry.guard("should_retry")
def should_retry(v):
    out = v.B.value
    return isinstance(out, dict) and not out.get("ok") and v.state.get("attempts", 0) < 3
```

This is how a retry self-loop is expressed: the body tracks `attempts` in
state, returns a result dict, and the guard checks both the result and the
state to decide whether to loop.

## Hooks (the observer seam)

`on_fire` / `on_tick_end` / `on_tick_start` are the single seam between
tickflow and the outside world (events, record stores, progress). They fire
on every node fire / tick end / tick start. Hook exceptions are logged and
swallowed so a misbehaving observer can't corrupt the run.

```python
rn.on_fire(lambda ns: event_bus.emit(ns.node, ns.output))
rn.on_tick_start(lambda tick, fireable: ui.highlight(fireable))
rn.on_tick_end(lambda tick, firings: db.save(tick, rn.snapshot()))
```

The `AsyncRunner` accepts async hooks (`async def`).

## Persistence backend

A `Runner` constructed with `backend=...` and `session_id=...` persists,
at the end of every tick: each `NodeState` (the process record) and a full
snapshot at the new tick index.

```python
from tickflow import JsonBackend

be = JsonBackend("tickflow/sessions")
rn = Runner(graph, registry, backend=be, session_id="sess-1")
rn.run_until_idle(max_ticks=100)

# Later / elsewhere: resume from the latest persisted tick.
snap = be.load_snapshot("sess-1", be.latest_tick("sess-1"))
rn2 = Runner(graph, registry)
rn2.restore(snap)
```

- **`Backend`** (Protocol): `save_snapshot` / `load_snapshot` / `latest_tick`
  / `list_snapshots` / `save_firing` / `list_firings` / `save_checkpoint` /
  `list_checkpoints` / `load_checkpoint`.
- **`JsonBackend(storage_dir)`**: one dir per session, `tick_<N>.json` +
  `firings.jsonl` + `checkpoints.json`. Default; human-inspectable.
- **`SqliteBackend(db_path)`**: single SQLite file with `snapshots` /
  `firings` / `checkpoints` tables. Better for high tick-throughput.
- **`NullBackend`**: in-memory, for tests.

### Named checkpoints

Layered on the backend:

```python
rn.checkpoint("after_prep")        # save current state under a label
rn.list_checkpoints()              # [(label, tick), ...]
rn.rollback_to("after_prep")       # restore to that checkpoint
```

### Graph remap (hot-swap graph structure)

After a rollback, you can replace the graph structure and/or registry before
resuming:

```python
rn.rollback_to("safe_point")
rn.remap_graph(new_graph, registry=new_registry)
rn.run_until_idle(max_ticks=100)
```

Slots that exist in both graphs keep their current value; new slots start
`False`; removed slots are discarded. `RunState` is filtered to keep only
nodes present in the new graph.

## AsyncRunner

For graphs whose bodies do IO (LLM calls, HTTP, DB), use `AsyncRunner`.
Bodies and guards may be `async def`; fireable nodes fire **concurrently**
within a tick via `asyncio.gather`. Semantics are identical to the sync
`Runner`.

```python
from tickflow.async_runner import AsyncRunner

@registry.body("harness")
async def harness(v):
    return await llm_client.chat(prompt=v.seed.value)

rn = AsyncRunner(graph, registry, backend=be, session_id="sess-1")
await rn.run_until_idle(max_ticks=100)
```

Sync and async bodies/guards may be mixed in the same graph. `Runner` and
`AsyncRunner` share all state-management logic via `_BaseRunner`.

## Visualization & front-end integration

### Static graph export

```python
graph.to_dict()      # JSON-able {nodes, edges, starts} for front-end rendering
graph.to_mermaid()   # mermaid "graph TD" text (READMEs / debuggers)
```

`to_dict()` includes derived `producers` per node so the front-end doesn't
recompute adjacency. `to_mermaid()` renders start nodes with stadium shape
`([name])`, plain edges `A --> B`, guarded edges `A -->|g| B`.

### Fireable preview & node state

```python
rn.fireable()         # [node names] that would fire on the next tick
rn.node_states()      # {node: {state dict}} — read-only copy (e.g. attempts)
```

`fireable()` is computed identically to the engine's internal check, so it
exactly predicts the next `tick()`. `node_states()` returns copies from
`RunState.all_mutable_states()`.

### Tick lifecycle hooks

```
on_tick_start(tick, fireable[])   # before any node fires this tick
  → engine runs (body/guard execute)
  → on_fire(ns) × N               # after each node fires (receives NodeState)
on_tick_end(tick, firings[])      # after all fires committed
```

### Audit log

```python
for ns in rn.audit_log():          # list[NodeState]
    print(f"t{ns.tick} {ns.node}: {ns.inputs} → {ns.output} [{ns.status}]")

rn.audit_json()                    # JSON string
```

Each `NodeState` record includes `inputs` (what the body read), `output`
(what it returned), `edges_fired` (how tokens propagated), `status`, `error`,
and `mutable_state` (node state snapshot after the body ran).

## CLI

```
python -m tickflow run      graph.txt -b beh.py [--max-ticks N] [--pause-at T ...]
python -m tickflow step     graph.txt -b beh.py [--from-snapshot snap.json] --ticks N
python -m tickflow snapshot graph.txt -b beh.py --out snap.json [--max-ticks N]
python -m tickflow audit    run.json
```

Deadlock suggestions: use `--auto-promote` to accept all, or `--no-promote`
to reject (and error). In non-interactive environments (CI, scripts), the
CLI defaults to erroring out with guidance — no hanging on stdin.

## Examples

| Example | Description |
|---------|-------------|
| `counter_loop` | Counter that loops while < 3, then stops. Cycles + terminating guard + OR-join. |
| `xor_merge` | XOR branch feeding a Merge. Checker flags AND-join deadlock → promote to OR. |
| `retry_loop` | Retry with `view.state` counter. Guard reads both output and state. |
| `fan_out` | Parallel workers + AND-join merge. Three branches fire concurrently. |
| `pipeline` | Three-stage pipe with `A[k]` index policy pinning A's first fire. |
| `state_machine` | Approval workflow: submit → review → approve/reject → done. XOR-splitter. |
| `checkpoint_restore` | Full workflow: checkpoint → rollback → remap → resume. Python script. |
| `keep_records_false` | Memory-saving mode demo: no audit, but state persists. Python script. |

## Design notes

**Why Petri net, not FSM?** The original design — a boolean slot per
incoming edge, AND over slots to fire, consume-on-fire, produce-into-
downstream — is exactly a marked graph (a Petri net subclass). An FSM is the
special case with exactly one live token; `tickflow` allows multiple
concurrent starts and AND/OR joins, which FSMs don't express natively.
Cycles (loops) are natural in Petri nets and forbidden in DAG schedulers.

**Why synchronous steps?** It makes the OR-join decidable (no "could more
tokens arrive?" question) and makes snapshots a trivial JSON dict — there's
no in-flight partial firing to save, so pause lands cleanly on tick
boundaries.

**Why three-layer RunState?** The old design had `History`, `audit` list, and
`Marking.node_state` as three separate, partially redundant structures. The
three-layer `RunState` (`_edges` + `_state` + `_records`) unifies them under
one owner with clear responsibilities. `_edges` and `_state` are always
maintained (engine needs them); `_records` (detailed audit) is gated on
`keep_records`.

**What's deliberately out of scope:** inclusive/XOR-join syntax beyond OR;
distributed/multi-worker scheduling; a built-in timeline forest (use
`deepcopy`); LLM token streaming (that's a harness/EventBus concern — tickflow
records at node granularity).

## Project layout

```
tickflow/
  ir.py           Node, Edge, Graph, InputPolicy, Failure
  parser.py       mermaid-like text → Graph (with static warnings)
  checker.py      deadlock detection, OR-join promotion, unguarded cycle detection
  state.py        NodeState, RunState — single source of truth for all runtime data
  engine.py       Marking, tick (pure function), join logic
  runner.py       Runner, _BaseRunner (shared sync/async logic)
  async_runner.py AsyncRunner (async bodies, concurrent firing)
  registry.py     body/guard registration
  views.py        DictView, Resolved, Missing
  persistence.py  Backend protocol, JsonBackend, SqliteBackend, NullBackend
  cli.py          python -m tickflow ...
tests/            17 files, 163 tests
examples/         8 examples (6 graph + 2 Python scripts)
```

## Running the tests

```bash
python -m pytest tests/ -q
```
