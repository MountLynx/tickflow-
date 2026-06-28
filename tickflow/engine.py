"""The synchronous tick engine.

Pure function
-------------
``tick`` is a pure function of ``(graph, marking_t, run_state, t, registry)``
returning ``(marking_{t+1}, firings_t)``. The :class:`Runner` owns the only
mutable state (marking + run_state + tick) and reconstructs the inputs each
tick; there is no in-flight partial-firing state to snapshot. This is what
makes ``Runner.snapshot`` a cheap JSON triple and ``restore`` a rewind.

Marking
-------
``Marking.slots`` is keyed by ``(dst, src)`` -- one slot per *incoming edge*.
A plain edge (``-->``) writes True when its source fires; a guarded edge
(``--|g|-->``) writes ``g(view)`` (so a failing guard writes False, an
explicit clobber -- a stale True from a previous iteration cannot leak into
a loop downstream's join). When a node fires, **all** its input slots reset to
False (consumed). OR-join does not change slot bit-width; only the join
predicate differs.

RunState
--------
:class:`tickflow.state.RunState` is the single source of truth for all
state recording. It replaces the old scattered ``History``, ``audit`` list,
and ``Marking.node_state``. Each tick, ``NodeState`` records are created
and recorded into the ``RunState``.

Bootstrap
---------
``bootstrap`` initialises the marking so every start node's input slots are
True (starts have no producers, so they have no slots; instead, their
*downstream* slots need to be primed for tick 0). Concretely: at tick 0 we
need each start node to be fireable. A start with no producers is fireable
trivially (empty AND over an empty set is True). So bootstrap is a no-op for
starts themselves; it only needs to set ``tick=0`` and an empty marking.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

import logging

from .ir import Graph, Failure
from .registry import Registry
from .state import NodeState, RunState, _jsonable
from .views import DictView, Resolved, Missing

log = logging.getLogger(__name__)


@dataclass
class Marking:
    # (dst, src) -> bool. One slot per incoming edge.
    slots: dict[tuple[str, str], bool] = field(default_factory=dict)
    # Start nodes that have not yet fired. A start fires exactly once: when
    # it fires we remove it from this set, so it won't be re-armed (no
    # self-loop producer to re-True its slot). Lets ``all([])==True`` be
    # gated on "hasn't fired yet" rather than firing every tick forever.
    armed_starts: set[str] = field(default_factory=set)

    def copy(self) -> "Marking":
        return Marking(
            slots=dict(self.slots),
            armed_starts=set(self.armed_starts),
        )

    def to_json(self) -> dict:
        return {
            "slots": {f"{dst}|{src}": v for (dst, src), v in self.slots.items()},
            "armed_starts": sorted(self.armed_starts),
        }

    @classmethod
    def from_json(cls, d: dict) -> "Marking":
        slots: dict[tuple[str, str], bool] = {}
        for k, v in d.get("slots", {}).items():
            dst, src = k.split("|", 1)
            slots[(dst, src)] = bool(v)
        return cls(
            slots=slots,
            armed_starts=set(d.get("armed_starts", [])),
        )


def bootstrap(graph: Graph) -> Marking:
    """Initial marking: empty. Start nodes fire at tick 0 by the empty-AND
    rule (a node with no producers has an empty slot set; ``all([]) == True``).
    """
    m = Marking()
    # Pre-create slots for every incoming edge so the marking is total; all
    # start out False (consumed/unfed).
    for e in graph.edges:
        m.slots.setdefault((e.dst, e.src), False)
    # Arm every start node so it fires exactly once at the first tick it can.
    m.armed_starts = set(graph.starts)
    return m


def _join_satisfied(graph: Graph, node: str, marking: Marking) -> bool:
    # An armed start fires once unconditionally -- this is how we inject the
    # initial token even when the start is also a loop member (has producers
    # from back-edges). After firing it's disarmed and join logic takes over.
    if node in marking.armed_starts:
        return True
    prods = graph.producers(node)
    if not prods:
        # No producers and not armed: nothing to fire on. (A non-start node
        # with no producers would fire forever, so we never arm it.)
        return False
    vals = [marking.slots.get((node, p), False) for p in prods]
    join = graph.nodes[node].join
    if join == "AND":
        return all(vals)
    if join == "OR":
        return any(vals)
    raise ValueError(f"unknown join {join!r} on {node!r}")


def _resolve_inputs(
    graph: Graph, node: str, run_state: RunState, t: int, registry: Registry
) -> dict[str, Resolved]:
    out: dict[str, Resolved] = {}
    for prod, policy in graph.nodes[node].inputs.items():
        v = run_state.resolve(prod, policy.kind, policy.k, t)
        out[prod] = Resolved(value=v, k=policy.k)
    return out


def tick(
    graph: Graph,
    marking: Marking,
    run_state: RunState,
    t: int,
    registry: Registry,
) -> tuple[Marking, list[NodeState], bool]:
    """One synchronous tick. Returns ``(next_marking, firings, aborted)``.

    ``aborted`` is True iff some node returned an ``infrastructure`` Failure
    this tick -- callers (Runner) should then stop further ticks.

    Body/guard callables here are *synchronous*. For async bodies use
    :mod:`tickflow.async_runner` which mirrors this logic with ``await`` +
    ``asyncio.gather`` over the fireable set.
    """
    fireable = [n for n in graph.nodes if _join_satisfied(graph, n, marking)]
    if not fireable:
        return marking.copy(), [], False

    m_next = marking.copy()
    firings: list[NodeState] = []
    aborted = False

    # Phase A: fire each fireable node. Writes go to run_state and are visible
    # to *resolutions and guards* in Phase B only via the SAME-tick entries
    # we have just recorded -- but reads use ``latest_before(t)`` (tick < t),
    # so a peer cannot see this tick's write. A node's own body is resolved
    # before its write, so it too sees only prior ticks. This is the marking
    # step semantics.
    for node in fireable:
        resolved = _resolve_inputs(graph, node, run_state, t, registry)
        # Initial mutable state: copy of the node's latest state from prior ticks.
        initial_state = run_state.mutable_state(node)
        state_view = _NodeStateView(initial_state)
        view = DictView(resolved, state_view, node)
        body = registry.get_body(graph.nodes[node].body)
        output = body(view)
        is_fail = isinstance(output, Failure)
        status: Literal["ok", "failed", "aborted"] = "ok"
        error: str | None = None
        if is_fail:
            error = output.error
            if output.type == "infrastructure":
                status = "aborted"
                aborted = True
            else:
                status = "failed"
        ns = NodeState(
            tick=t,
            node=node,
            inputs={k: v.value for k, v in resolved.items()},
            output=output,
            edges_fired=[],  # filled in Phase B
            status=status,
            error=error,
            mutable_state=initial_state,
        )
        firings.append(ns)
        # Record into run_state: this commits the output for resolution and
        # the mutable_state for downstream guard views.
        run_state.record(ns)
        # Consume this node's input slots.
        for p in graph.producers(node):
            m_next.slots[(node, p)] = False
        # Disarm if it was an armed start (one-shot).
        m_next.armed_starts.discard(node)

    # Phase B: produce downstream slots. A guard on edge ``src--|g|-->dst``
    # decides routing based on the *firing node's own output this tick* (the
    # thing it just produced), so we resolve it with ``src``'s current-tick
    # output visible -- not latest_before. A *failed* node writes False into
    # all its out-edges (no token propagates) -- downstream AND-joins thus
    # don't fire, equivalent to "upstream failed, skip downstream". Guards
    # are not even consulted for failed nodes.
    for f in firings:
        failed = f.status in ("failed", "aborted")
        if failed:
            guarded = [e.guard for e in graph.out_edges(f.node) if e.guard is not None]
            if guarded:
                log.warning(
                    "Node %r returned Failure (status=%s): guards %s on out-edges "
                    "will NOT be evaluated — all out-edges write False. "
                    "Consider using a guard on the output value instead of Failure "
                    "for controllable routing.",
                    f.node, f.status, guarded,
                )
        for e in graph.out_edges(f.node):
            if failed:
                v = False
            elif e.guard is None:
                v = True
            else:
                v = bool(registry.get_guard(e.guard)(
                    _guard_view(
                        graph, e.src, f.output, run_state, t, registry,
                    )
                ))
            m_next.slots[(e.dst, e.src)] = v
            f.edges_fired.append((e.dst, e.guard, v))

    return m_next, firings, aborted


def _guard_view(
    graph: Graph,
    src: str,
    src_output: Any,
    run_state: RunState,
    t: int,
    registry: Registry,
) -> DictView:
    """Build a view for guard evaluation where the firing node ``src``'s
    *current-tick* output is visible under its own name, and any other
    producer the guard may reference resolves to latest_before(t).

    The firing node's own mutable state (just recorded in run_state) is
    exposed via ``view.state`` so a guard like "retry under max" can read
    ``view.state["attempts"]``.
    """
    resolved: dict[str, Resolved] = {}
    # The firing node itself, with its just-produced output.
    resolved[src] = Resolved(value=src_output, k=None)
    for name in graph.nodes:
        if name == src:
            continue
        v = run_state.resolve(name, "latest", None, t)
        resolved[name] = Resolved(value=v, k=None)
    src_state = run_state.mutable_state(src)
    state_view = _NodeStateView(src_state)
    return DictView(resolved, state_view, src)


class _NodeStateView:
    """A thin read/write proxy over a node's mutable state dict. Bodies write
    through ``view.state["k"] = v``; the underlying dict is the per-firing
    state that will be captured in the NodeState record. Guards receive a
    read-only view (writes would be ignored/discarded)."""

    __slots__ = ("_d",)

    def __init__(self, d: dict[str, Any]) -> None:
        object.__setattr__(self, "_d", d)

    def __getitem__(self, k: str) -> Any:
        return self._d[k]

    def __setitem__(self, k: str, v: Any) -> None:
        self._d[k] = v

    def __contains__(self, k: str) -> bool:
        return k in self._d

    def get(self, k: str, default: Any = None) -> Any:
        return self._d.get(k, default)

    def keys(self):
        return self._d.keys()

    def items(self):
        return self._d.items()

    def values(self):
        return self._d.values()

    def __repr__(self) -> str:
        return f"NodeState({self._d!r})"
