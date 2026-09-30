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

Since D6 the default backend is a temp SQLite file (auto-cleaned), so an
AsyncRunner created without an explicit ``backend=`` performs a small
synchronous SQLite write at the end of every tick — fine when LLM latency
dominates, but pass ``NullBackend()`` for zero-I/O async runs.

A body or guard may be sync *or* async; the runner detects via
``inspect.iscoroutinefunction`` and awaits accordingly. Mixing is fine.

Semantics are identical to the sync engine: marking-step concurrency (peers
can't see same-tick writes), Failure propagation (infra -> ABORTED), node_state,
hooks (async hooks supported), backend persistence, checkpoints.

All shared logic (snapshot/restore, remap, checkpoints, audit, etc.) lives in
:class:`tickflow.runner._BaseRunner`.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Awaitable, Callable, Iterable

from .ir import Graph, Failure
from .registry import Registry, registry as _default_registry
from .engine import (
    Marking, bootstrap, _join_satisfied, _prepare_fire,
    prepare_body_call, prepare_guard_call, _NodeStateView,
)
from .state import NodeState, RunState, _jsonable
from .runner import (
    _BaseRunner, RunStatus, _TERMINAL, FireHook, TickEndHook,
    _validate_registry_for_graph, _warn_graph_changes,
)

log = logging.getLogger(__name__)

# Async hook type aliases.
AsyncFireHook = Callable[[NodeState], Awaitable[None]]
AsyncTickEndHook = Callable[[int, list[NodeState]], Awaitable[None]]


async def _maybe_await(fn: Any, *args: Any, **kw: Any) -> Any:
    """Call ``fn`` and await if it returns a coroutine."""
    result = fn(*args, **kw)
    if inspect.isawaitable(result):
        result = await result
    return result


async def async_tick(
    graph: Graph,
    marking: Marking,
    run_state: RunState,
    t: int,
    registry: Registry,
    fireable: list[str] | None = None,
) -> tuple[Marking, list[NodeState], bool]:
    """Async counterpart of :func:`tickflow.engine.tick`.

    ``fireable``: precomputed fireable set for ``marking`` (see
    :func:`tickflow.engine.tick`); default None computes internally. Caller
    responsibility: the list must match ``marking`` exactly and must not be
    mutated afterwards -- an inconsistent set silently changes firing
    behaviour.
    """
    from typing import Literal

    if fireable is None:
        fireable = [n for n in graph.nodes if _join_satisfied(graph, n, marking)]
    if not fireable:
        return marking.copy(), [], False

    m_next = marking.copy()

    async def _fire(node: str) -> NodeState:
        legacy, resolved, entries, values = _prepare_fire(graph, node, run_state, t, registry)
        initial_state = run_state.mutable_state(node)
        state_view = _NodeStateView(initial_state)
        body_name = graph.nodes[node].body
        if body_name is None:
            output = values[0] if values else None
        else:
            fn, args, kwargs = prepare_body_call(
                registry, body_name, entries, values, state_view, legacy, node,
            )
            output = await _maybe_await(fn, *args, **kwargs)
        is_fail = isinstance(output, Failure)
        status: Literal["ok", "failed", "aborted"] = "ok"
        error: str | None = None
        if is_fail:
            error = output.error
            status = "aborted" if output.type == "infrastructure" else "failed"
        return NodeState(
            tick=t, node=node,
            inputs={k: v.value for k, v in resolved.items()},
            output=output, edges_fired=[],
            status=status, error=error,
            mutable_state=initial_state,
        )

    firings = list(await asyncio.gather(*[_fire(n) for n in fireable]))
    aborted = any(f.status == "aborted" for f in firings)

    for f in firings:
        run_state.record(f)

    for f in firings:
        for p in graph.producers(f.node):
            m_next.slots[(f.node, p)] = False
        m_next.armed_starts.discard(f.node)

    async def _produce(f: NodeState) -> None:
        failed = f.status in ("failed", "aborted")
        for e in graph.out_edges(f.node):
            if failed:
                v = False
            elif e.guard is None:
                v = True
            else:
                gfn, gargs, gkwargs = prepare_guard_call(
                    registry, e.guard, graph, e.src, f.output, run_state, t,
                )
                v = bool(await _maybe_await(gfn, *gargs, **gkwargs))
            m_next.slots[(e.dst, e.src)] = v
            f.edges_fired.append((e.dst, e.guard, v))

    await asyncio.gather(*[_produce(f) for f in firings])
    return m_next, firings, aborted


class AsyncRunner(_BaseRunner):
    """Async counterpart of :class:`tickflow.runner.Runner`.

    All shared logic (snapshot, restore, remap, checkpoints, audit, etc.) is
    inherited from :class:`_BaseRunner`.
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
        from .checker import check, DeadlockError
        super().__init__(
            graph, registry,
            strict_deadlock=strict_deadlock,
            backend=backend, session_id=session_id,
            keep_records=keep_records,
        )
        self._fire_hooks: list = []
        self._tick_end_hooks: list = []
        self._tick_start_hooks: list = []

    # --- hooks ------------------------------------------------------------

    def on_fire(self, callback) -> None:
        self._fire_hooks.append(callback)

    def on_tick_end(self, callback) -> None:
        self._tick_end_hooks.append(callback)

    def on_tick_start(self, callback) -> None:
        self._tick_start_hooks.append(callback)

    async def _run_fire_hooks(self, firing: NodeState) -> None:
        for cb in self._fire_hooks:
            try:
                await _maybe_await(cb, firing)
            except Exception:
                log.exception("on_fire hook raised; swallowed")

    async def _run_tick_end_hooks(self, tick: int, firings: list[NodeState]) -> None:
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

    # --- core -------------------------------------------------------------

    async def tick(self) -> list[NodeState]:
        """Advance exactly one tick."""
        if self.status in _TERMINAL:
            return []
        fireable = self.fireable()
        await self._run_tick_start_hooks(self.tick_count, fireable)
        next_marking, firings, aborted = await async_tick(
            self.graph, self.marking, self.run_state, self.tick_count, self.registry,
            fireable=fireable,
        )
        self.marking = next_marking
        for f in firings:
            await self._run_fire_hooks(f)
        self.tick_count += 1
        if aborted:
            self.status = RunStatus.ABORTED
        elif not firings:
            # Mirror of Runner.tick: empty-tick fixpoint — pending work with
            # nothing fireable is the starved FAILED terminal.
            self.status = (
                RunStatus.FAILED if self._has_pending() else RunStatus.IDLE
            )
        else:
            self.status = RunStatus.RUNNING
        await self._run_tick_end_hooks(self.tick_count - 1, firings)
        self._persist_tick([f.node for f in firings])
        return firings

    async def run_until_idle(
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
            firings = await self.tick()
            if not firings:
                break
            seen.extend(firings)
            if self.status in _TERMINAL:
                break
        return seen
