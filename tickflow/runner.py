"""High-level runner: tick loop + snapshot/restore/pause/audit + hooks.

The :class:`Runner` is the only stateful object. All state lives in three
fields -- :attr:`marking`, :attr:`run_state`, :attr:`tick_count`, :attr:`status`
-- which are pure data (JSON-able). That makes :meth:`snapshot` a copy of those
and :meth:`restore` an assignment + run_state truncation: there is no in-flight
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
from .engine import Marking, tick, bootstrap, _join_satisfied
from .state import NodeState, RunState, _jsonable
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


def _warn_graph_changes(old: Graph, new: Graph, run_state: RunState) -> None:
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

    for n in sorted(old_nodes - new_nodes):
        if run_state.last_output(n) is not None:
            log.warning("Node %r removed but has history entries — data orphaned", n)

    for n in sorted(old_nodes & new_nodes):
        was_start = n in old.starts
        is_start = n in new.starts
        if not was_start and is_start and run_state.last_output(n) is not None:
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
FireHook = Callable[[NodeState], None]
TickEndHook = Callable[[int, list[NodeState]], None]
TickStartHook = Callable[[int, list[str]], None]   # (tick, fireable_node_names)


# Terminal statuses: ticking is a no-op and returns [].
_TERMINAL = {RunStatus.ABORTED, RunStatus.CANCELLED, RunStatus.FAILED}


class _BaseRunner:
    """Shared logic for :class:`Runner` and :class:`AsyncRunner`.

    Contains all state management that doesn't depend on sync vs async:
    snapshot/restore, registry swap, graph remap, checkpoints, audit, and
    derived queries (fireable, node_states, is_idle, etc.).

    Subclasses provide ``tick()``, hook registration/invocation, and
    ``run_until_idle()``.
    """

    def __init__(
        self,
        graph: Graph,
        registry: Registry | None = None,
        *,
        strict_deadlock: bool = True,
        backend: Any = None,
        session_id: str | None = None,
        keep_records: bool = True,
    ) -> None:
        self.graph = graph
        self.registry = registry if registry is not None else _default_registry
        self.marking: Marking = bootstrap(graph)
        self.run_state: RunState = RunState(keep_records=keep_records)
        self.tick_count: int = 0
        self.status: RunStatus = RunStatus.IDLE
        self.cancel_reason: str | None = None
        self._backend = backend
        self._session_id = session_id
        if strict_deadlock:
            pending = check(graph)
            if pending:
                raise DeadlockError(pending)

    # ------------------------------------------------------------------
    # Derived queries
    # ------------------------------------------------------------------

    def fireable(self) -> list[str]:
        """Nodes that would fire on the next tick given the current marking."""
        return [n for n in self.graph.nodes if _join_satisfied(self.graph, n, self.marking)]

    def node_states(self) -> dict[str, dict[str, Any]]:
        """Read-only copy of every node's mutable state."""
        return self.run_state.all_mutable_states()

    def _has_pending(self) -> bool:
        """True if any node could still fire."""
        if self.marking.armed_starts:
            return True
        return any(self.marking.slots.values())

    def is_idle(self) -> bool:
        return self.status == RunStatus.IDLE

    def is_terminal(self) -> bool:
        """True if the run has halted and won't make progress."""
        if self.status in _TERMINAL:
            return True
        return self.status == RunStatus.IDLE and not self._has_pending()

    def cancel(self, reason: str = "cancelled") -> None:
        """Mark the run cancelled. Subsequent ticks are no-ops."""
        if self.status not in _TERMINAL:
            self.status = RunStatus.CANCELLED
            self.cancel_reason = reason

    def reset(self) -> None:
        """Clear a non-IDLE status back to IDLE."""
        if self.status != RunStatus.RUNNING:
            self.status = RunStatus.IDLE
            self.cancel_reason = None

    # ------------------------------------------------------------------
    # Persistence helper
    # ------------------------------------------------------------------

    def _persist_tick(self, firings: list[NodeState]) -> None:
        """Persist this tick's snapshot + firings to the backend, if any."""
        if self._backend is None or self._session_id is None:
            return
        try:
            if firings:
                self._backend.save_firings(self._session_id, firings)
            self._backend.save_snapshot(self._session_id, self.tick_count, self.snapshot())
        except Exception:
            log.exception("backend persistence failed; swallowed")

    # ------------------------------------------------------------------
    # Snapshot / restore
    # ------------------------------------------------------------------

    def snapshot(self) -> dict:
        """JSON-able snapshot of (marking, run_state, tick, status, fireable)."""
        run_data = self.run_state.to_snapshot_data()
        return {
            "tick": self.tick_count,
            "marking": self.marking.to_json(),
            "run_state": run_data,
            "status": self.status.value,
            "cancel_reason": self.cancel_reason,
            "fireable": self.fireable(),
        }

    def restore(self, snap: dict) -> None:
        """Rewind to ``snap``."""
        self.tick_count = int(snap["tick"])
        self.marking = Marking.from_json(snap["marking"])
        run_snap = snap.get("run_state", {})
        if run_snap:
            self.run_state = RunState.from_snapshot_data(run_snap)
        else:
            # Legacy snapshot without run_state key.
            h_data = snap.get("history", {})
            rs = RunState(keep_records=False)
            for n, lst in h_data.items():
                for t, v in lst:
                    if int(t) < self.tick_count:
                        rs._edges.setdefault(n, []).append((int(t), v))
            self.run_state = rs
        self.run_state.truncate_after(self.tick_count - 1)
        status_val = snap.get("status", RunStatus.IDLE.value)
        try:
            self.status = RunStatus(status_val)
        except ValueError:
            self.status = RunStatus.IDLE
        self.cancel_reason = snap.get("cancel_reason")
        if self.status in _TERMINAL:
            self.status = RunStatus.IDLE
            self.cancel_reason = None

    def to_json(self) -> str:
        """Full state as a single JSON string.  Audit trail lives under
        ``snapshot.run_state.records``."""
        return json.dumps({
            "snapshot": self.snapshot(),
        }, indent=2, default=_jsonable)

    @classmethod
    def from_json(cls, s: str, graph: Graph, registry: Registry | None = None) -> "Runner":
        """Reconstruct a Runner from a prior :meth:`to_json` dump."""
        d = json.loads(s)
        r = cls(graph, registry, strict_deadlock=False)
        r.restore(d["snapshot"])
        return r

    # ------------------------------------------------------------------
    # Registry swap
    # ------------------------------------------------------------------

    def _validate_registry(self, registry: Registry) -> None:
        _validate_registry_for_graph(self.graph, registry)

    def set_registry(self, registry: Registry) -> None:
        """Replace :attr:`registry` with a new Registry instance."""
        _validate_registry_for_graph(self.graph, registry)
        self.registry = registry

    # ------------------------------------------------------------------
    # Graph remap
    # ------------------------------------------------------------------

    def remap_graph(
        self,
        new_graph: Graph,
        registry: Registry | None = None,
        *,
        strict_deadlock: bool = True,
    ) -> None:
        """Replace :attr:`graph` with *new_graph*, porting the current marking."""
        reg = registry if registry is not None else self.registry
        _validate_registry_for_graph(new_graph, reg)

        if strict_deadlock:
            pending = check(new_graph)
            if pending:
                raise DeadlockError(pending)

        _warn_graph_changes(self.graph, new_graph, self.run_state)

        old_slots = self.marking.slots
        new_slots: dict[tuple[str, str], bool] = {}
        for e in new_graph.edges:
            key = (e.dst, e.src)
            new_slots[key] = old_slots.get(key, False)

        new_armed = self.marking.armed_starts & set(new_graph.starts)

        self.marking = Marking(slots=new_slots, armed_starts=new_armed)
        self.run_state.keep_nodes(set(new_graph.nodes))
        self.graph = new_graph
        self.registry = reg
        self.reset()

    # ------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------

    def checkpoint(self, label: str) -> None:
        """Save the current state as a named checkpoint in the backend."""
        if self._backend is None or self._session_id is None:
            raise RuntimeError("checkpoint requires a backend and session_id")
        self._backend.save_checkpoint(self._session_id, label, self.snapshot())

    def list_checkpoints(self) -> list[tuple[str, int]]:
        if self._backend is None or self._session_id is None:
            return []
        return self._backend.list_checkpoints(self._session_id)

    def rollback_to(self, label: str) -> None:
        """Restore to a named checkpoint."""
        if self._backend is None or self._session_id is None:
            raise RuntimeError("rollback_to requires a backend and session_id")
        snap = self._backend.load_checkpoint(self._session_id, label)
        if snap is None:
            raise KeyError(f"checkpoint {label!r} not found")
        self.restore(snap)

    # ------------------------------------------------------------------
    # Audit / history inspection
    # ------------------------------------------------------------------

    def audit_log(self) -> list[NodeState]:
        return self.run_state.audit()

    def audit_json(self) -> str:
        return json.dumps(
            [ns.to_json() for ns in self.run_state.audit()],
            indent=2, default=_jsonable,
        )

    def last_output(self, node: str) -> Any:
        return self.run_state.last_output(node)

    def firings_of(self, node: str) -> list[tuple[int, Any]]:
        return self.run_state.firings_of(node)


class Runner(_BaseRunner):
    """Synchronous runner. See :class:`_BaseRunner` for shared API."""

    def __init__(
        self,
        graph: Graph,
        registry: Registry | None = None,
        *,
        strict_deadlock: bool = True,
        backend: Any = None,
        session_id: str | None = None,
        keep_records: bool = True,
    ) -> None:
        super().__init__(
            graph, registry,
            strict_deadlock=strict_deadlock,
            backend=backend, session_id=session_id,
            keep_records=keep_records,
        )
        self._fire_hooks: list[FireHook] = []
        self._tick_end_hooks: list[TickEndHook] = []
        self._tick_start_hooks: list[TickStartHook] = []

    # --- hooks ------------------------------------------------------------

    def on_fire(self, callback: FireHook) -> None:
        self._fire_hooks.append(callback)

    def on_tick_end(self, callback: TickEndHook) -> None:
        self._tick_end_hooks.append(callback)

    def on_tick_start(self, callback: TickStartHook) -> None:
        self._tick_start_hooks.append(callback)

    def _run_fire_hooks(self, firing: NodeState) -> None:
        for cb in self._fire_hooks:
            try:
                cb(firing)
            except Exception:
                log.exception("on_fire hook raised; swallowed")

    def _run_tick_end_hooks(self, tick: int, firings: list[NodeState]) -> None:
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

    # --- core -------------------------------------------------------------

    def tick(self) -> list[NodeState]:
        """Advance exactly one tick."""
        if self.status in _TERMINAL:
            return []
        fireable = self.fireable()
        self._run_tick_start_hooks(self.tick_count, fireable)
        next_marking, firings, aborted = tick(
            self.graph, self.marking, self.run_state, self.tick_count, self.registry
        )
        self.marking = next_marking
        for f in firings:
            self._run_fire_hooks(f)
        self.tick_count += 1
        if aborted:
            self.status = RunStatus.ABORTED
        elif not firings:
            self.status = RunStatus.IDLE
        else:
            self.status = RunStatus.RUNNING
        self._run_tick_end_hooks(self.tick_count - 1, firings)
        self._persist_tick(firings)
        return firings

    def run_until_idle(
        self,
        max_ticks: int = 1000,
        pause_at: Iterable[int] | None = None,
    ) -> list[NodeState]:
        """Tick until idle/terminal or ``max_ticks`` reached."""
        pauses = set(pause_at or ())
        seen: list[NodeState] = []
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
