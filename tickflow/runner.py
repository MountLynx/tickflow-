"""High-level runner: tick loop + snapshot/restore/pause/audit + hooks.

The :class:`Runner` is the only stateful object. All state lives in four
fields -- :attr:`marking`, :attr:`history`, :attr:`tick_count`, :attr:`status`
-- which are pure data (JSON-able). That makes :meth:`snapshot` a copy of those
and :meth:`restore` an assignment + history truncation: there is no in-flight
partial firing to save, because :func:`tickflow.engine.tick` is synchronous and
fires whole ticks.

RunStatus
---------
Replaces the old ``_idle: bool``. A run transitions:
    IDLE --tick--> RUNNING --(no fireable / max_ticks)--> IDLE
                --(infra Failure)--> ABORTED
                --cancel()--> CANCELLED
                --(all failed, no fireable)--> FAILED
``is_idle()`` is ``status == IDLE``. ABORTED/CANCELLED/FAILED ticks return
empty and do not advance.

Hooks
-----
:meth:`on_fire` / :meth:`on_tick_end` are the single seam between flow and the
outside world (events, record stores, progress updates). They fire on the
sync Runner; the :class:`tickflow.async_runner.AsyncRunner` accepts async hooks.
Hooks must not raise (errors are logged and swallowed) so a misbehaving
observer can't corrupt the run.

What-if / branching
-------------------
The library does not maintain a "forest" of timelines. To branch, the caller
``copy.deepcopy(runner.snapshot())`` and constructs a second Runner per
branch. Snapshots are plain dicts so this is cheap and explicit.

Body purity
-----------
Bodies are *expected* to be pure functions of their input view (state writes
aside). If a body is non-pure (reads external state, mutates), restore-then-
replay may produce different results than the original run -- the audit log
will still record the original, but the replay diverges.
"""

from __future__ import annotations

import enum
import json
import logging
from typing import Any, Callable, Iterable

from .ir import Graph
from .registry import Registry, registry as _default_registry
from .engine import Marking, History, tick, bootstrap, Firing, _join_satisfied
from .checker import check, DeadlockSuggestion, DeadlockError

log = logging.getLogger(__name__)


def _validate_registry_for_graph(graph: Graph, registry: Registry) -> None:
    """Raise ValueError if ``registry`` is missing any body or guard name
    referenced by ``graph``."""
    missing: list[str] = []
    for node in graph.nodes.values():
        if node.body is not None and not registry.has_body(node.body):
            missing.append(f"body {node.body!r} (required by node {node.name!r})")
    for edge in graph.edges:
        if edge.guard is not None and not registry.has_guard(edge.guard):
            missing.append(f"guard {edge.guard!r} (required by edge {edge.src}-->{edge.dst})")
    if missing:
        raise ValueError(
            "registry missing required entries:\n  " + "\n  ".join(missing)
        )


def _warn_graph_changes(old: Graph, new: Graph, history: History) -> None:
    """Validate/warn on structural changes between graphs.

    - A node with history that becomes a start is an *error*: armed_starts are
      one-shot, so such a node would silently never re-fire, breaking the
      one-shot semantics the user expects from a start. The caller must
      explicitly roll back to tick 0 (or construct a fresh marking) to re-arm.
    - Removed nodes with history, and changed start sets, are warnings (the
      user made a deliberate structural edit; we surface the consequence).
    """
    old_nodes = set(old.nodes)
    new_nodes = set(new.nodes)

    # Nodes removed that had history.
    for n in sorted(old_nodes - new_nodes):
        if n in history.data:
            log.warning("Node %r removed but has history entries — data orphaned", n)

    # Nodes that became starts but already have history: ERROR (one-shot break).
    for n in sorted(old_nodes & new_nodes):
        was_start = n in old.starts
        is_start = n in new.starts
        if not was_start and is_start and n in history.data:
            raise ValueError(
                f"Node {n!r} became a start but already has history — armed_starts "
                f"are one-shot, so it would never re-fire. Roll back to tick 0 or "
                f"construct a fresh marking to re-arm it."
            )

    if set(old.starts) != set(new.starts):
        log.warning(
            "Start node set changed: old=%s, new=%s",
            sorted(old.starts), sorted(new.starts),
        )


class RunStatus(str, enum.Enum):
    """Lifecycle of a Runner. IDLE means "quiescent, may have work pending but
    nothing fired last tick" -- historically the only state. The terminal
    states (ABORTED/CANCELLED/FAILED) stop further ticking."""

    IDLE = "idle"           # quiescent (nothing fired, or never started)
    RUNNING = "running"     # a tick is in progress (transient, not persisted)
    ABORTED = "aborted"     # an infrastructure Failure occurred; halted
    CANCELLED = "cancelled"  # cancel() was called
    FAILED = "failed"       # all nodes failed and nothing is fireable


# Hook type aliases.
FireHook = Callable[[Firing], None]
TickEndHook = Callable[[int, list[Firing]], None]
TickStartHook = Callable[[int, list[str]], None]   # (tick, fireable_node_names)


def _jsonable(v: Any) -> Any:
    """Coerce ``v`` to something json.dumps can serialise. Falls back to repr
    for arbitrary objects -- this keeps snapshots/logs lossy-but-stable rather
    than crashing on a body that returns, say, a file handle."""
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    try:
        json.dumps(v)
        return v
    except TypeError:
        return repr(v)


# Terminal statuses: ticking is a no-op and returns [].
_TERMINAL = {RunStatus.ABORTED, RunStatus.CANCELLED, RunStatus.FAILED}


class Runner:
    def __init__(
        self,
        graph: Graph,
        registry: Registry | None = None,
        *,
        strict_deadlock: bool = True,
        backend: Any = None,
        session_id: str | None = None,
        enable_audit: bool = True,
    ) -> None:
        self.graph = graph
        self.registry = registry if registry is not None else _default_registry
        self.marking: Marking = bootstrap(graph)
        self.history: History = History()
        self.tick_count: int = 0
        self.audit: list[Firing] = []
        self.enable_audit = enable_audit
        self.status: RunStatus = RunStatus.IDLE
        self.cancel_reason: str | None = None
        # Persistence (optional). When set, every tick's snapshot + each firing
        # are persisted under session_id.
        self._backend = backend
        self._session_id = session_id
        # Hooks.
        self._fire_hooks: list[FireHook] = []
        self._tick_end_hooks: list[TickEndHook] = []
        self._tick_start_hooks: list[TickStartHook] = []
        if strict_deadlock:
            pending = check(graph)
            if pending:
                raise DeadlockError(pending)

    # --- hooks ------------------------------------------------------------

    def on_fire(self, callback: FireHook) -> None:
        """Register a callback invoked after each node fires, with the
        :class:`Firing` record. This is the single seam for external observers
        (events, record stores, progress). Callback errors are logged and
        swallowed."""
        self._fire_hooks.append(callback)

    def on_tick_end(self, callback: TickEndHook) -> None:
        """Register a callback invoked at the end of each tick with
        ``(tick_index, firings)``. Use for layer-complete callbacks and
        tick-level snapshot persistence."""
        self._tick_end_hooks.append(callback)

    def on_tick_start(self, callback: TickStartHook) -> None:
        """Register a callback invoked at the start of each tick, *before* any
        node fires, with ``(tick_index, fireable_node_names)``. Use this to
        tell a front-end "these nodes are about to light up". The fireable
        list is computed from the current marking, identical to what the
        engine will actually fire this tick."""
        self._tick_start_hooks.append(callback)

    def _run_fire_hooks(self, firing: Firing) -> None:
        for cb in self._fire_hooks:
            try:
                cb(firing)
            except Exception:
                log.exception("on_fire hook raised; swallowed")

    def _run_tick_end_hooks(self, tick: int, firings: list[Firing]) -> None:
        for cb in self._tick_end_hooks:
            try:
                cb(tick, firings)
            except Exception:
                log.exception("on_tick_end hook raised; swallowed")

    def _run_tick_start_hooks(self, tick: int, fireable: list[str]) -> None:
        for cb in self._tick_start_hooks:
            try:
                cb(tick, fireable)
            except Exception:
                log.exception("on_tick_start hook raised; swallowed")

    # --- fireable / node state (read-only derived views) ------------------

    def fireable(self) -> list[str]:
        """Nodes that would fire on the next tick given the current marking
        and join rules. Computed identically to the engine's internal check,
        so the result equals the set of nodes the next ``tick()`` will fire.
        Read-only; does not advance the run."""
        return [n for n in self.graph.nodes if _join_satisfied(self.graph, n, self.marking)]

    def node_states(self) -> dict[str, dict[str, Any]]:
        """Read-only copy of every node's mutable state (e.g. retry counters).
        Mutating the returned dicts does not affect the run."""
        return {n: dict(s) for n, s in self.marking.node_state.items()}

    # --- core -------------------------------------------------------------

    def tick(self) -> list[Firing]:
        """Advance exactly one tick. Returns the firings that occurred.
        No-op (returns ``[]``) when status is terminal (ABORTED/CANCELLED/
        FAILED). Sets status to IDLE if nothing fired (fixed point), ABORTED
        if an infrastructure Failure occurred."""
        if self.status in _TERMINAL:
            return []
        # tick-start hooks fire before the engine runs, with the fireable set
        # computed from the current (pre-tick) marking.
        fireable = self.fireable()
        self._run_tick_start_hooks(self.tick_count, fireable)
        next_marking, firings, aborted = tick(
            self.graph, self.marking, self.history, self.tick_count, self.registry
        )
        self.marking = next_marking
        if self.enable_audit:
            self.audit.extend(firings)
        # Fire hooks fire *before* tick_count increments, with the tick index
        # at which the fire logically occurred.
        for f in firings:
            self._run_fire_hooks(f)
        self.tick_count += 1
        if aborted:
            self.status = RunStatus.ABORTED
        elif not firings:
            self.status = RunStatus.IDLE
        else:
            self.status = RunStatus.RUNNING
        # Tick-end hooks + persistence fire after the tick is committed.
        self._run_tick_end_hooks(self.tick_count - 1, firings)
        self._persist_tick(firings)
        return firings

    def _persist_tick(self, firings: list[Firing]) -> None:
        """Persist this tick's snapshot + firings to the backend, if any.
        Firings are written in a single batch call (one transaction) when the
        backend supports it, to avoid per-firing fsync overhead."""
        if self._backend is None or self._session_id is None:
            return
        try:
            if firings:
                self._backend.save_firings(self._session_id, firings)
            self._backend.save_snapshot(self._session_id, self.tick_count, self.snapshot())
        except Exception:
            log.exception("backend persistence failed; swallowed")

    def run_until_idle(
        self,
        max_ticks: int = 1000,
        pause_at: Iterable[int] | None = None,
    ) -> list[Firing]:
        """Tick until idle/terminal or ``max_ticks`` reached. If ``pause_at``
        contains the *next* tick index (i.e. ``self.tick_count``), stop before
        firing it and return control -- the tick is not consumed. Returns all
        firings observed during this call."""
        pauses = set(pause_at or ())
        seen: list[Firing] = []
        while self.tick_count < max_ticks:
            if self.tick_count in pauses:
                break
            if self.status in _TERMINAL:
                break
            if self.status == RunStatus.IDLE and self.tick_count > 0 and not self._has_pending():
                break
            firings = self.tick()
            if not firings:
                break
            seen.extend(firings)
            if self.status in _TERMINAL:
                break
        return seen

    def _has_pending(self) -> bool:
        """True if any node could still fire (a start is armed or any slot is
        True). Used to distinguish 'idle because done' from 'idle mid-run'."""
        if self.marking.armed_starts:
            return True
        return any(self.marking.slots.values())

    def is_idle(self) -> bool:
        return self.status == RunStatus.IDLE

    def is_terminal(self) -> bool:
        """True if the run has halted and won't make progress: ABORTED,
        CANCELLED, FAILED, or IDLE with nothing pending."""
        if self.status in _TERMINAL:
            return True
        return self.status == RunStatus.IDLE and not self._has_pending()

    def cancel(self, reason: str = "cancelled") -> None:
        """Mark the run cancelled. Subsequent ticks are no-ops."""
        if self.status not in _TERMINAL:
            self.status = RunStatus.CANCELLED
            self.cancel_reason = reason

    def reset(self) -> None:
        """Clear a non-IDLE status back to IDLE so ticking can resume (e.g.
        after externally restoring a non-quiescent snapshot). Does not undo
        ABORTED semantics for the current marking -- the caller usually calls
        this right after :meth:`restore`."""
        if self.status != RunStatus.RUNNING:
            self.status = RunStatus.IDLE
            self.cancel_reason = None

    # --- snapshot / restore ----------------------------------------------

    def snapshot(self) -> dict:
        """JSON-able snapshot of (marking, history, tick, status, fireable).
        Does not include the graph or registry -- those are structural/code
        and live outside. ``fireable`` is the set of nodes that would fire on
        the next tick from this marking (derived, but persisted so a front-end
        reading the snapshot once gets the "what's next" without recomputing)."""
        return {
            "tick": self.tick_count,
            "marking": self.marking.to_json(),
            "history": {
                n: [[t, _jsonable(v)] for (t, v) in lst]
                for n, lst in self.history.data.items()
            },
            "status": self.status.value,
            "cancel_reason": self.cancel_reason,
            "fireable": self.fireable(),
        }

    def restore(self, snap: dict) -> None:
        """Rewind to ``snap``. ``snap["tick"]`` is the *next* tick to fire
        (i.e. ticks ``0..snap["tick"]-1`` have already fired). History entries
        with ``tick >= snap["tick"]`` are dropped, and the audit log is
        truncated to entries with ``tick < snap["tick"]`` (only if audit is
        enabled). Status is restored and terminal states reset to IDLE so
        ticking can resume if the restored marking has pending work.

        To swap the body/guard implementation or restructure the graph after a
        rollback, call :meth:`set_registry` / :meth:`remap_graph` separately --
        those validate against the (possibly new) graph. ``restore`` only
        rewinds marking/history/tick/status.

        ``fireable`` is NOT read back -- it is derived from the marking, which
        is the authoritative source, so ``self.fireable()`` after restore is
        correct without it."""
        self.tick_count = int(snap["tick"])
        self.marking = Marking.from_json(snap["marking"])
        h = History()
        for n, lst in snap["history"].items():
            h.data[n] = [(int(t), v) for (t, v) in lst if int(t) < self.tick_count]
        self.history = h
        if self.enable_audit:
            self.audit = [f for f in self.audit if f.tick < self.tick_count]
        else:
            self.audit = []
        status_val = snap.get("status", RunStatus.IDLE.value)
        try:
            self.status = RunStatus(status_val)
        except ValueError:
            self.status = RunStatus.IDLE
        self.cancel_reason = snap.get("cancel_reason")
        # After restore, allow resuming unless we were truly terminal.
        if self.status in _TERMINAL:
            self.status = RunStatus.IDLE
            self.cancel_reason = None

    def to_json(self) -> str:
        """Full state as a JSON string (snapshot + audit log). Suitable for
        writing to a file and reloading via :meth:`from_json`."""
        return json.dumps({
            "snapshot": self.snapshot(),
            "audit": [f.to_json() for f in self.audit],
        }, indent=2, default=_jsonable)

    @classmethod
    def from_json(cls, s: str, graph: Graph, registry: Registry | None = None) -> "Runner":
        """Reconstruct a Runner from a prior :meth:`to_json` dump. The graph
        and registry must be supplied (they are not stored in the dump).
        Deadlock check is skipped (the graph was already validated when the
        dump was made -- or the caller knows what they're doing)."""
        d = json.loads(s)
        r = cls(graph, registry, strict_deadlock=False)
        r.restore(d["snapshot"])
        r.audit = [Firing.from_json(f) for f in d["audit"]]
        return r

    # --- registry swap ----------------------------------------------------

    def _validate_registry(self, registry: Registry) -> None:
        """Raise ValueError if ``registry`` is missing any body or guard name
        referenced by :attr:`graph`."""
        _validate_registry_for_graph(self.graph, registry)

    def set_registry(self, registry: Registry) -> None:
        """Replace :attr:`registry` with a new :class:`Registry` instance.

        Validates that the new registry provides every body and guard name
        referenced by the graph. Call this between ticks or after a rollback
        to hot-swap body/guard implementations without restarting the run.

        The graph structure, marking, history, and tick are untouched — only
        the lookup table for body/guard functions changes.
        """
        _validate_registry_for_graph(self.graph, registry)
        self.registry = registry

    # --- graph remap ------------------------------------------------------

    def remap_graph(
        self,
        new_graph: Graph,
        registry: Registry | None = None,
        *,
        strict_deadlock: bool = True,
    ) -> None:
        """Replace :attr:`graph` with *new_graph*, porting the current marking.

        Slots that exist in both graphs keep their current value (True tokens
        are preserved). Slots in the new graph that didn't exist before start
        at ``False``. Slots from the old graph that don't exist in the new
        graph are discarded.

        *registry*, if given, replaces :attr:`registry` at the same time
        (validated against *new_graph*).

        Typical use: rollback to a checkpoint, then call ``remap_graph`` with
        a structurally modified graph before resuming.
        """
        reg = registry if registry is not None else self.registry
        _validate_registry_for_graph(new_graph, reg)

        if strict_deadlock:
            pending = check(new_graph)
            if pending:
                raise DeadlockError(pending)

        _warn_graph_changes(self.graph, new_graph, self.history)

        # Port slots: keep old value for edges that exist in both.
        old_slots = self.marking.slots
        new_slots: dict[tuple[str, str], bool] = {}
        for e in new_graph.edges:
            key = (e.dst, e.src)
            new_slots[key] = old_slots.get(key, False)

        # armed_starts: only keep those still marked as starts in the new graph.
        new_armed = self.marking.armed_starts & set(new_graph.starts)

        # node_state: carry over for nodes that exist in both graphs.
        new_node_state = {
            n: dict(s)
            for n, s in self.marking.node_state.items()
            if n in new_graph.nodes
        }

        self.marking = Marking(
            slots=new_slots,
            armed_starts=new_armed,
            node_state=new_node_state,
        )
        self.graph = new_graph
        self.registry = reg
        # A remap may introduce new fireable work (new edges, new starts) even
        # if the run was previously IDLE/terminal. Reset so run_until_idle
        # re-evaluates -- mirrors restore()'s status reset.
        self.reset()

    # --- checkpoints (named snapshots via backend) ------------------------

    def checkpoint(self, label: str) -> None:
        """Save the current state as a named checkpoint in the backend.
        Requires a backend + session_id."""
        if self._backend is None or self._session_id is None:
            raise RuntimeError("checkpoint requires a backend and session_id")
        self._backend.save_checkpoint(self._session_id, label, self.snapshot())

    def list_checkpoints(self) -> list[tuple[str, int]]:
        if self._backend is None or self._session_id is None:
            return []
        return self._backend.list_checkpoints(self._session_id)

    def rollback_to(self, label: str) -> None:
        """Restore to a named checkpoint. Requires a backend + session_id.

        Only rewinds marking/history/tick/status. To also swap the body/guard
        implementation or restructure the graph, follow with
        :meth:`set_registry` / :meth:`remap_graph`."""
        if self._backend is None or self._session_id is None:
            raise RuntimeError("rollback_to requires a backend and session_id")
        snap = self._backend.load_checkpoint(self._session_id, label)
        if snap is None:
            raise KeyError(f"checkpoint {label!r} not found")
        self.restore(snap)

    # --- audit ------------------------------------------------------------

    def audit_log(self) -> list[Firing]:
        return list(self.audit)

    def audit_json(self) -> str:
        return json.dumps([f.to_json() for f in self.audit], indent=2, default=_jsonable)

    # --- history inspection ----------------------------------------------

    def last_output(self, node: str) -> Any:
        entries = self.history.data.get(node, [])
        return entries[-1][1] if entries else None

    def firings_of(self, node: str) -> list[tuple[int, Any]]:
        return self.history.firings_of(node)
