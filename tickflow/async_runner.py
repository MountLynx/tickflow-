"""Asynchronous runner: async bodies + concurrent firing.

Mirrors :class:`tickflow.runner.Runner` but allows node bodies and edge guards to
be ``async def``. Within a tick, all fireable nodes fire **concurrently** via
``asyncio.gather`` (matching the Petri step semantics -- fireable transitions
fire together, each seeing only prior-tick state).

When to use
-----------
Use AsyncRunner when a body does IO (LLM calls, HTTP, DB) -- i.e. the
ModuleHarness case where each node runs a Harness (LLM + tools). The sync
Runner is fine for pure-computation graphs and tests.

A body or guard may be sync *or* async; the runner detects via
``inspect.iscoroutinefunction`` and awaits accordingly. Mixing is fine.

Semantics are identical to the sync engine: marking-step concurrency (peers
can't see same-tick writes), Failure propagation (infra -> ABORTED), node_state,
hooks (async hooks supported), backend persistence, checkpoints.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Awaitable, Callable, Iterable

from .ir import Graph, Failure
from .registry import Registry, registry as _default_registry
from .engine import (
    Marking, History, bootstrap, Firing,
    _join_satisfied, _resolve_inputs, _guard_view, _NodeStateView,
)
from .views import DictView
from .runner import RunStatus, _TERMINAL, _jsonable, FireHook, TickEndHook

log = logging.getLogger(__name__)

# Async hook type aliases.
AsyncFireHook = Callable[[Firing], Awaitable[None]]
AsyncTickEndHook = Callable[[int, list[Firing]], Awaitable[None]]


async def _maybe_await(fn: Any, *args: Any, **kw: Any) -> Any:
    """Call ``fn`` and await if it returns a coroutine (covers both async-def
    functions and sync functions that happen to return a coroutine)."""
    result = fn(*args, **kw)
    if inspect.isawaitable(result):
        result = await result
    return result


async def async_tick(
    graph: Graph,
    marking: Marking,
    history: History,
    t: int,
    registry: Registry,
) -> tuple[Marking, list[Firing], bool]:
    """Async counterpart of :func:`tickflow.engine.tick`. Bodies and guards may be
    async; fireable nodes fire concurrently via ``asyncio.gather``. Returns
    ``(next_marking, firings, aborted)``."""
    from typing import Literal

    fireable = [n for n in graph.nodes if _join_satisfied(graph, n, marking)]
    if not fireable:
        return marking.copy(), [], False

    m_next = marking.copy()

    async def _fire(node: str) -> Firing:
        resolved = _resolve_inputs(graph, node, history, t, registry)
        state_view = _NodeStateView(m_next.node_state.setdefault(node, {}))
        view = DictView(resolved, state_view, node)
        body = registry.get_body(graph.nodes[node].body)
        output = await _maybe_await(body, view)
        history.append(node, t, output)
        is_fail = isinstance(output, Failure)
        status: Literal["ok", "failed", "aborted"] = "ok"
        error: str | None = None
        if is_fail:
            error = output.error
            status = "aborted" if output.type == "infrastructure" else "failed"
        return Firing(
            tick=t,
            node=node,
            inputs={k: v.value for k, v in resolved.items()},
            output=output,
            edges_fired=[],
            status=status,
            error=error,
            node_state=dict(m_next.node_state.get(node, {})),
        )

    firings = list(await asyncio.gather(*[_fire(n) for n in fireable]))
    aborted = any(f.status == "aborted" for f in firings)

    # Consume input slots + disarm starts (committed to m_next).
    for f in firings:
        for p in graph.producers(f.node):
            m_next.slots[(f.node, p)] = False
        m_next.armed_starts.discard(f.node)

    # Phase B: produce downstream slots. Failed nodes write False on all
    # out-edges (no token propagation). Guards are awaited concurrently.
    async def _produce(f: Firing) -> None:
        failed = f.status in ("failed", "aborted")
        for e in graph.out_edges(f.node):
            if failed:
                v = False
            elif e.guard is None:
                v = True
            else:
                gview = _guard_view(
                    graph, e.src, f.output, history, t, registry,
                    m_next.node_state.get(e.src, {}),
                )
                v = bool(await _maybe_await(registry.get_guard(e.guard), gview))
            m_next.slots[(e.dst, e.src)] = v
            f.edges_fired.append((e.dst, e.guard, v))

    await asyncio.gather(*[_produce(f) for f in firings])
    return m_next, firings, aborted


class AsyncRunner:
    """Async counterpart of :class:`tickflow.runner.Runner`. See module docstring."""

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
        from .checker import check, DeadlockError
        self.graph = graph
        self.registry = registry if registry is not None else _default_registry
        self.marking: Marking = bootstrap(graph)
        self.history: History = History()
        self.tick_count: int = 0
        self.audit: list[Firing] = []
        self.enable_audit = enable_audit
        self.status: RunStatus = RunStatus.IDLE
        self.cancel_reason: str | None = None
        self._backend = backend
        self._session_id = session_id
        self._fire_hooks: list = []          # sync FireHook or AsyncFireHook
        self._tick_end_hooks: list = []      # sync TickEndHook or AsyncTickEndHook
        self._tick_start_hooks: list = []    # sync TickStartHook or async variant
        if strict_deadlock:
            pending = check(graph)
            if pending:
                raise DeadlockError(pending)

    # --- hooks ------------------------------------------------------------

    def on_fire(self, callback) -> None:
        self._fire_hooks.append(callback)

    def on_tick_end(self, callback) -> None:
        self._tick_end_hooks.append(callback)

    def on_tick_start(self, callback) -> None:
        """Register a callback invoked at the start of each tick, *before* any
        node fires, with ``(tick_index, fireable_node_names)``. Accepts sync
        or ``async def`` callbacks."""
        self._tick_start_hooks.append(callback)

    async def _run_fire_hooks(self, firing: Firing) -> None:
        for cb in self._fire_hooks:
            try:
                await _maybe_await(cb, firing)
            except Exception:
                log.exception("on_fire hook raised; swallowed")

    async def _run_tick_end_hooks(self, tick: int, firings: list[Firing]) -> None:
        for cb in self._tick_end_hooks:
            try:
                await _maybe_await(cb, tick, firings)
            except Exception:
                log.exception("on_tick_end hook raised; swallowed")

    async def _run_tick_start_hooks(self, tick: int, fireable: list[str]) -> None:
        for cb in self._tick_start_hooks:
            try:
                await _maybe_await(cb, tick, fireable)
            except Exception:
                log.exception("on_tick_start hook raised; swallowed")

    # --- fireable / node state (read-only derived views) ------------------

    def fireable(self) -> list[str]:
        """Nodes that would fire on the next tick given the current marking.
        Computed identically to the engine's internal check. Read-only."""
        return [n for n in self.graph.nodes if _join_satisfied(self.graph, n, self.marking)]

    def node_states(self) -> dict[str, dict[str, Any]]:
        """Read-only copy of every node's mutable state."""
        return {n: dict(s) for n, s in self.marking.node_state.items()}

    # --- core -------------------------------------------------------------

    async def tick(self) -> list[Firing]:
        if self.status in _TERMINAL:
            return []
        # tick-start hooks fire before the engine runs, with the fireable set
        # computed from the current (pre-tick) marking.
        fireable = self.fireable()
        await self._run_tick_start_hooks(self.tick_count, fireable)
        next_marking, firings, aborted = await async_tick(
            self.graph, self.marking, self.history, self.tick_count, self.registry
        )
        self.marking = next_marking
        if self.enable_audit:
            self.audit.extend(firings)
        for f in firings:
            await self._run_fire_hooks(f)
        self.tick_count += 1
        if aborted:
            self.status = RunStatus.ABORTED
        elif not firings:
            self.status = RunStatus.IDLE
        else:
            self.status = RunStatus.RUNNING
        await self._run_tick_end_hooks(self.tick_count - 1, firings)
        self._persist_tick(firings)
        return firings

    def _persist_tick(self, firings: list[Firing]) -> None:
        if self._backend is None or self._session_id is None:
            return
        try:
            for f in firings:
                self._backend.save_firing(self._session_id, f)
            self._backend.save_snapshot(self._session_id, self.tick_count, self.snapshot())
        except Exception:
            log.exception("backend persistence failed; swallowed")

    async def run_until_idle(
        self,
        max_ticks: int = 1000,
        pause_at: Iterable[int] | None = None,
    ) -> list[Firing]:
        pauses = set(pause_at or ())
        seen: list[Firing] = []
        while self.tick_count < max_ticks:
            if self.tick_count in pauses:
                break
            if self.status in _TERMINAL:
                break
            if self.status == RunStatus.IDLE and self.tick_count > 0 and not self._has_pending():
                break
            firings = await self.tick()
            if not firings:
                break
            seen.extend(firings)
            if self.status in _TERMINAL:
                break
        return seen

    def _has_pending(self) -> bool:
        if self.marking.armed_starts:
            return True
        return any(self.marking.slots.values())

    def is_idle(self) -> bool:
        return self.status == RunStatus.IDLE

    def is_terminal(self) -> bool:
        if self.status in _TERMINAL:
            return True
        return self.status == RunStatus.IDLE and not self._has_pending()

    def cancel(self, reason: str = "cancelled") -> None:
        if self.status not in _TERMINAL:
            self.status = RunStatus.CANCELLED
            self.cancel_reason = reason

    def reset(self) -> None:
        if self.status != RunStatus.RUNNING:
            self.status = RunStatus.IDLE
            self.cancel_reason = None

    # --- snapshot / restore (same shape as Runner) -----------------------

    def snapshot(self) -> dict:
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
        try:
            self.status = RunStatus(snap.get("status", RunStatus.IDLE.value))
        except ValueError:
            self.status = RunStatus.IDLE
        self.cancel_reason = snap.get("cancel_reason")
        if self.status in _TERMINAL:
            self.status = RunStatus.IDLE
            self.cancel_reason = None

    def to_json(self) -> str:
        """Full state as a JSON string (snapshot + audit log). Counterpart of
        :meth:`tickflow.runner.Runner.to_json`."""
        import json
        return json.dumps({
            "snapshot": self.snapshot(),
            "audit": [f.to_json() for f in self.audit],
        }, indent=2, default=_jsonable)

    @classmethod
    def from_json(cls, s: str, graph: Graph, registry: Registry | None = None) -> "AsyncRunner":
        """Reconstruct an AsyncRunner from a prior :meth:`to_json` dump. The
        graph and registry must be supplied (not stored in the dump)."""
        import json
        d = json.loads(s)
        r = cls(graph, registry, strict_deadlock=False)
        r.restore(d["snapshot"])
        r.audit = [Firing.from_json(f) for f in d["audit"]]
        return r

    # --- checkpoints ------------------------------------------------------

    def checkpoint(self, label: str) -> None:
        if self._backend is None or self._session_id is None:
            raise RuntimeError("checkpoint requires a backend and session_id")
        self._backend.save_checkpoint(self._session_id, label, self.snapshot())

    def list_checkpoints(self) -> list[tuple[str, int]]:
        if self._backend is None or self._session_id is None:
            return []
        return self._backend.list_checkpoints(self._session_id)

    def rollback_to(self, label: str) -> None:
        if self._backend is None or self._session_id is None:
            raise RuntimeError("rollback_to requires a backend and session_id")
        snap = self._backend.load_checkpoint(self._session_id, label)
        if snap is None:
            raise KeyError(f"checkpoint {label!r} not found")
        self.restore(snap)

    # --- audit / history --------------------------------------------------

    def audit_log(self) -> list[Firing]:
        return list(self.audit)

    def audit_json(self) -> str:
        import json
        return json.dumps([f.to_json() for f in self.audit], indent=2, default=_jsonable)

    def last_output(self, node: str) -> Any:
        entries = self.history.data.get(node, [])
        return entries[-1][1] if entries else None

    def firings_of(self, node: str) -> list[tuple[int, Any]]:
        return self.history.firings_of(node)
