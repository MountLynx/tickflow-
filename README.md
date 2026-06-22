# tickflow

A small Petri-style workflow control framework for building short, auditable,
reversible flows. You describe the graph in a mermaid-like syntax; the engine
runs it as synchronous Petri-net *steps* over a boolean slot marking, with an
append-only history that makes snapshots, pause, rewind, and replay cheap.

> Is this a state machine? No — it's a **Petri net** (specifically a marked
> graph with AND/OR joins). A finite state machine is the degenerate case
> where exactly one token is live in the whole net at any time. `flow`
> supports multiple concurrent starts, AND-joins (wait for all upstream),
> OR-joins (fire on any upstream), and cycles — none of which plain FSMs
> express natively. See [Design notes](#design-notes).



## Quick start

```bash
python -m flow run examples/counter_loop.txt -b examples/counter_loop_beh.py
```

The graph file declares **structure only**; a Python "behaviours" file
registers the actual body/guard callables on `flow.registry`.

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
  `latest_before`).
- `#` starts a comment.

### Behaviours file

```python
from flow import registry
from flow.views import Missing

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

## Semantics

### The model

The engine is a **pure function**:

```
tick: (marking_t, history_{<t}) -> (marking_{t+1}, firings_t)
```

Three quantities determine everything:

- **marking** — `dict[(dst, src), bool]`, one boolean *slot* per incoming
  edge, plus a set of *armed* start nodes (one-shot).
- **history** — `dict[node, list[(tick, value)]]`, append-only.
- **tick** — the current tick index.

There is no hidden in-flight state. That's what makes snapshots cheap.

### A tick

1. Compute the set of *fireable* nodes: a node fires iff
   - it is an armed start (fires once, then disarmed), **or**
   - its join predicate holds over its input slots: `all` for AND, `any`
     for OR.
2. Each fireable node fires concurrently: its body reads inputs
   (`latest_before(t)` — strictly prior ticks, so peers can't see each
   other's same-tick writes), its output is appended to history, and its
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
from flow import parse, check, promote
g = parse(text, registry=r)
for s in check(g):       # list[DeadlockSuggestion]
    promote(s, g)        # flips s.node.join to "OR"
```

Constructing a `Runner` with unresolved suggestions raises `DeadlockError`
(no silent deadlocks).

## Snapshot, pause, rewind

```python
rn = Runner(graph, registry)
rn.run_until_idle(max_ticks=100, pause_at={5})   # stop before tick 5
snap = rn.snapshot()                              # JSON-able dict
rn.run_until_idle(max_ticks=100)                  # finish
rn.restore(snap)                                  # rewind to tick 5
rn.run_until_idle(max_ticks=100)                  # replay (identical if bodies pure)
```

- **`snapshot()`** returns `{"tick", "marking", "history", "status", "cancel_reason"}` — pure
  JSON. `tick` is the *next* tick to fire. `marking` now includes `node_state`.
- **`restore(snap)`** rewinds: sets tick/marking/history/status from `snap` and
  truncates the audit log to ticks `< snap["tick"]`. A restored terminal status
  (ABORTED/CANCELLED/FAILED) is reset to IDLE so the run can resume.
- **`pause_at={n}`** stops at the tick boundary before tick `n` — no
  half-fired state to save.
- **Branching / what-if**: `copy.deepcopy(snap)` and `restore` into separate
  Runners. The library doesn't maintain a timeline forest.
- **`to_json()` / `from_json()`** serialize the full state (snapshot + audit
  log). The graph and registry are *not* stored — supply them on reload.

**Body purity**: bodies should be pure functions of their input view (state
writes aside). If a body is non-pure, restore-then-replay may diverge from the
original run (the audit log still records what originally happened).

## Failure, status, and control

### Failure (body error signalling)

A body may return `Failure(error, type=...)` instead of a normal value:

```python
from flow import Failure

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

A `Failure` is still written to history and recorded in the audit log with
`Firing.status` (`"failed"` / `"aborted"`) and `Firing.error`.

### RunStatus

`Runner.status` is a `RunStatus` enum replacing the old `_idle` flag:

| Status | Meaning |
|---|---|
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

Each node has a mutable state dict that lives in the marking (so it
participates in snapshots/restore) and is visible to guards. Bodies read/write
their own state; guards receive a read-only view:

```python
@registry.body("retryable")
def retryable(v):
    v.state["attempts"] = v.state.get("attempts", 0) + 1
    return do_work(v.A.value)

@registry.guard("under_max")
def under_max(v):
    return v.state.get("attempts", 0) < 3
```

This is how a compliance-retry self-loop is expressed: the body increments
`attempts`, the guard stops looping once the cap is reached.

## Hooks (the observer seam)

`on_fire` / `on_tick_end` are the single seam between flow and the outside
world (events, record stores, progress). They fire on every node fire / tick
end. Hook exceptions are logged and swallowed so a misbehaving observer can't
corrupt the run.

```python
rn.on_fire(lambda firing: event_bus.emit_task_completed(...))
rn.on_tick_end(lambda tick, firings: snapshot_store.save(tick, rn.snapshot()))
```

The `AsyncRunner` accepts async hooks (`async def`).

## Persistence backend

A `Runner` constructed with `backend=...` and `session_id=...` persists, at
the end of every tick: each `Firing` (the process record) and a full snapshot
at the new tick index. This realizes the 重构.md design: "快照粒度一个 tick"
+ "整个进程进行过程记录".

```python
from flow import JsonBackend

be = JsonBackend(".flow/sessions")
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
- **`NullBackend`**: in-memory, for tests.
- `SqliteBackend` is planned (not in v1) for high tick-throughput.

### Named checkpoints

Layered on the backend:

```python
rn.checkpoint("after_prep")        # save current state under a label
rn.list_checkpoints()              # [(label, tick), ...]
rn.rollback_to("after_prep")       # restore to that checkpoint
```

This replaces the old `SnapshotManager`'s label/list/rollback_to.

## AsyncRunner

For graphs whose bodies do IO (LLM calls, HTTP, DB), use `AsyncRunner`.
Bodies and guards may be `async def`; fireable nodes fire **concurrently**
within a tick via `asyncio.gather`. Semantics are identical to the sync
`Runner` (marking-step concurrency, Failure propagation, node_state, hooks,
persistence, checkpoints).

```python
from flow.async_runner import AsyncRunner

@registry.body("harness")
async def harness(v):
    return await llm_client.chat(prompt=v.seed.value)

rn = AsyncRunner(graph, registry, backend=be, session_id="sess-1")
await rn.run_until_idle(max_ticks=100)
```

Sync and async bodies/guards may be mixed in the same graph.

## Visualization & front-end integration

tickflow exposes everything a front-end needs to render and drive a live
process graph: static structure, per-tick "what's about to fire", per-node
state, and a hook timeline.

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
exactly predicts the next `tick()`. `node_states()` returns a deep copy so
mutating it doesn't affect the run.

### Tick lifecycle hooks

```
on_tick_start(tick, fireable[])   # before any node fires this tick
  → engine runs (body/guard execute)
  → on_fire(firing) × N           # after each node fires
on_tick_end(tick, firings[])      # after all fires committed
```

```python
rn.on_tick_start(lambda tick, fireable: ws.broadcast({"tick": tick, "fireable": fireable}))
rn.on_fire(lambda firing: ws.broadcast({"fired": firing.node, "status": firing.status}))
rn.on_tick_end(lambda tick, firings: ws.broadcast({"tick_done": tick}))
```

AsyncRunner accepts sync or `async def` hooks. Hook exceptions are logged and
swallowed — a misbehaving observer can't corrupt the run.

### Snapshot carries fireable

```python
snap = rn.snapshot()
# snap["fireable"] == rn.fireable()  — front-end reads one dict and gets
#                                       both state and "what's next"
```

`fireable` is derived from the marking but persisted in the snapshot so a
front-end reading a saved snapshot (e.g. via backend) gets the preview without
recomputing. On `restore()`, `fireable` is not read back — the restored
marking is authoritative, so `rn.fireable()` is correct automatically.

### Disabling in-memory audit

For embedded / front-end-push scenarios where you don't need the in-memory
audit log (you're streaming via hooks instead), turn it off to save memory:

```python
rn = Runner(graph, registry, enable_audit=False)
```

- `self.audit` stays empty; `tick()` doesn't accumulate.
- **firings.jsonl backend persistence is unaffected** — `Backend.save_firing`
  still runs every tick, so the process record survives for crash recovery.
- `to_json()` emits `"audit": []`.

### Where streaming LLM tokens go

tickflow deliberately does **not** handle LLM token streaming — that's a
call-level concern belonging to the harness/EventBus layer, not the flow
engine. tickflow's hooks give the front-end coarse anchors ("node X is
firing", "node X done with status ok"), while token chunks flow through a
separate channel (e.g. ModularHarness's EventBus `llm_token` events). This
keeps tickflow's audit/snapshot small (node-level, not token-level).

## CLI

```
python -m flow run     graph.txt -b beh.py [--max-ticks N] [--pause-at T ...]
python -m flow step    graph.txt -b beh.py [--from-snapshot snap.json] --ticks N
python -m flow snapshot graph.txt -b beh.py --out snap.json [--max-ticks N]
python -m flow audit   run.json
```

Deadlock suggestions are presented interactively on `run`/`step`. Use
`--auto-promote` to accept all, or `--no-promote` to reject (and error).

## Examples

- `examples/counter_loop.txt` — a counter that loops while < 3, then stops.
  Shows cycles, a terminating guard, and an explicit OR-join on the loop
  member.
- `examples/xor_merge.txt` — an XOR branch (B picks A or D) feeding a Merge.
  The checker flags the AND-join deadlock and prompts to promote Merge to
  OR-join.

## Design notes

**Why Petri net, not FSM?** The original design — a boolean slot per
incoming edge, AND over slots to fire, consume-on-fire, produce-into-
downstream — is exactly a marked graph (a Petri net subclass). An FSM is the
special case with exactly one live token; `flow` allows multiple concurrent
starts and AND/OR joins, which FSMs don't express natively. Cycles (loops)
are natural in Petri nets and forbidden in DAG schedulers (Airflow etc.).

**Why synchronous steps?** It makes the OR-join decidable (no "could more
tokens arrive?" question) and makes snapshots a trivial JSON triple —
there's no in-flight partial firing to save, so pause lands cleanly on tick
boundaries.

**What's deliberately out of scope (v1):** inclusive/XOR-join syntax beyond
OR; per-node persistent state (a `node_state` field is reserved);
distributed/multi-worker scheduling; graph→mermaid reverse rendering; a
built-in timeline forest (use `deepcopy`).

## Project layout

```
flow/
  ir.py        Node, Edge, Graph, InputPolicy dataclasses
  parser.py    mermaid-like text -> Graph
  checker.py   static deadlock detection + OR-join promotion
  engine.py    Marking, History, tick (pure), Firing
  runner.py    Runner: tick/run/snapshot/restore/audit/pause
  registry.py  body/guard registration
  views.py     DictView + Resolved + Missing
  cli.py       python -m flow ...
tests/         42 tests: parser, checker, engine, loop, snapshot, audit
examples/      counter_loop, xor_merge (+ behaviours)
```

## Running the tests

```bash
python -m pytest tests/ -q
```
