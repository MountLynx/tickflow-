"""The synchronous tick engine.

Pure function
-------------
``tick`` is a pure function of ``(graph, marking_t, history, t, registry)``
returning ``(marking_{t+1}, firings_t)``. The :class:`Runner` owns the only
mutable state (marking + history + tick) and reconstructs the inputs each
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

History
-------
``History.data`` is ``dict[node, list[(tick, value)]]``, append-only. Reads
default to ``latest_before(t)``: the most recent fire of the producer with
``tick < t`` -- the marking-consistent read (you cannot see a same-tick write
by a peer). ``A[k]`` pins the producer's ``k``-th fire overall (1-based),
independent of tick, for cross-iteration audit/replay.

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

from .ir import Graph, Failure
from .registry import Registry
from .views import DictView, Resolved, Missing


@dataclass
class Marking:
    # (dst, src) -> bool. One slot per incoming edge.
    slots: dict[tuple[str, str], bool] = field(default_factory=dict)
    # Start nodes that have not yet fired. A start fires exactly once: when
    # it fires we remove it from this set, so it won't be re-armed (no
    # self-loop producer to re-True its slot). Lets ``all([])==True`` be
    # gated on "hasn't fired yet" rather than firing every tick forever.
    armed_starts: set[str] = field(default_factory=set)
    # Per-node mutable state (e.g. retry counters). Lives in the marking so it
    # participates in snapshots/restore and is visible to guards. A node's body
    # reads/writes its own slot via ``view.state``; writes take effect for the
    # *next* marking (committed in Phase A along with slot consumption).
    node_state: dict[str, dict[str, Any]] = field(default_factory=dict)

    def copy(self) -> "Marking":
        return Marking(
            slots=dict(self.slots),
            armed_starts=set(self.armed_starts),
            node_state={n: dict(s) for n, s in self.node_state.items()},
        )

    def to_json(self) -> dict:
        return {
            "slots": {f"{dst}|{src}": v for (dst, src), v in self.slots.items()},
            "armed_starts": sorted(self.armed_starts),
            "node_state": {n: dict(s) for n, s in self.node_state.items()},
        }

    @classmethod
    def from_json(cls, d: dict) -> "Marking":
        slots: dict[tuple[str, str], bool] = {}
        for k, v in d["slots"].items():
            dst, src = k.split("|", 1)
            slots[(dst, src)] = bool(v)
        return cls(
            slots=slots,
            armed_starts=set(d.get("armed_starts", [])),
            node_state={n: dict(s) for n, s in d.get("node_state", {}).items()},
        )


class History:
    """Append-only ``(tick, value)`` log per node, with policy-based reads."""

    def __init__(self) -> None:
        self.data: dict[str, list[tuple[int, Any]]] = {}

    def append(self, node: str, tick: int, value: Any) -> None:
        self.data.setdefault(node, []).append((tick, value))

    def read(self, node: str, kind: str, k: int | None, t: int) -> Any:
        entries = self.data.get(node, [])
        if kind == "index":
            if k is None or k < 1 or k > len(entries):
                return Missing
            return entries[k - 1][1]
        # latest_before(t): most recent fire with tick < t.
        last: tuple[int, Any] | None = None
        for tk, v in entries:
            if tk < t:
                last = (tk, v)
            else:
                break  # entries are appended in tick order; safe to stop
        return last[1] if last is not None else Missing

    def firings_of(self, node: str) -> list[tuple[int, Any]]:
        return list(self.data.get(node, []))

    def to_json(self) -> dict:
        # values may be arbitrary; Runner.snapshot coerces non-JSON-able ones
        # to repr() (see runner._jsonable). Here we just store the raw lists.
        return {n: [[t, v] for (t, v) in lst] for n, lst in self.data.items()}

    @classmethod
    def from_json(cls, d: dict) -> "History":
        h = cls()
        for n, lst in d.items():
            h.data[n] = [(int(t), v) for (t, v) in lst]
        return h

    def truncate_after(self, tick: int) -> None:
        """Rewind: drop all fires with ``tick > tick``. Used by Runner.restore."""
        for n in list(self.data):
            self.data[n] = [(t, v) for (t, v) in self.data[n] if t <= tick]
            if not self.data[n]:
                del self.data[n]


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


def _resolve_inputs(graph: Graph, node: str, history: History, t: int, registry: Registry) -> dict[str, Resolved]:
    out: dict[str, Resolved] = {}
    for prod, policy in graph.nodes[node].inputs.items():
        v = history.read(prod, policy.kind, policy.k, t)
        out[prod] = Resolved(value=v, k=policy.k)
    return out


@dataclass
class Firing:
    """Record of one node firing in one tick. Stored in Runner.audit and
    serialised for snapshots/logs via :meth:`to_json`."""
    tick: int
    node: str
    inputs: dict[str, Any]      # producer -> resolved value
    output: Any                # what the body returned (may be a Failure)
    edges_fired: list[tuple[str, str | None, bool]]
    # edges_fired: (dst, guard_name_or_None, slot_value_written)
    status: Literal["ok", "failed", "aborted"] = "ok"
    error: str | None = None
    # Snapshot of this node's state *after* the body ran (for audit/restore).
    node_state: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict:
        from .runner import _jsonable  # late import: runner imports engine
        return {
            "tick": self.tick,
            "node": self.node,
            "inputs": {k: _jsonable(v) for k, v in self.inputs.items()},
            "output": _jsonable(self.output),
            "edges_fired": [[dst, g, v] for (dst, g, v) in self.edges_fired],
            "status": self.status,
            "error": self.error,
            "node_state": _jsonable(self.node_state),
        }

    @classmethod
    def from_json(cls, d: dict) -> "Firing":
        return cls(
            tick=d["tick"],
            node=d["node"],
            inputs=d["inputs"],
            output=d["output"],
            edges_fired=[(dst, g, v) for (dst, g, v) in d["edges_fired"]],
            status=d.get("status", "ok"),
            error=d.get("error"),
            node_state=d.get("node_state", {}),
        )


def tick(
    graph: Graph,
    marking: Marking,
    history: History,
    t: int,
    registry: Registry,
) -> tuple[Marking, list[Firing], bool]:
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
    firings: list[Firing] = []
    aborted = False

    # Phase A: fire each fireable node. Writes go to history and are visible
    # to *resolutions and guards* in Phase B only via the SAME-tick entries
    # we have just appended -- but reads use ``latest_before(t)`` (tick < t),
    # so a peer cannot see this tick's write. A node's own body is resolved
    # before its write, so it too sees only prior ticks. This is the marking
    # step semantics.
    for node in fireable:
        resolved = _resolve_inputs(graph, node, history, t, registry)
        state_view = _NodeStateView(m_next.node_state.setdefault(node, {}))
        view = DictView(resolved, state_view, node)
        body = registry.get_body(graph.nodes[node].body)
        output = body(view)
        history.append(node, t, output)
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
        firings.append(
            Firing(
                tick=t,
                node=node,
                inputs={k: v.value for k, v in resolved.items()},
                output=output,
                edges_fired=[],  # filled in Phase B
                status=status,
                error=error,
                node_state=dict(m_next.node_state.get(node, {})),
            )
        )
        # consume this node's input slots
        for p in graph.producers(node):
            m_next.slots[(node, p)] = False
        # disarm if it was an armed start (one-shot)
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
        for e in graph.out_edges(f.node):
            if failed:
                v = False
            elif e.guard is None:
                v = True
            else:
                v = bool(registry.get_guard(e.guard)(
                    _guard_view(graph, e.src, f.output, history, t, registry, m_next.node_state.get(e.src, {}))
                ))
            m_next.slots[(e.dst, e.src)] = v
            f.edges_fired.append((e.dst, e.guard, v))

    return m_next, firings, aborted


def _guard_view(
    graph: Graph, src: str, src_output: Any, history: History, t: int, registry: Registry,
    src_state: dict[str, Any] | None = None,
) -> DictView:
    """Build a view for guard evaluation where the firing node ``src``'s
    *current-tick* output is visible under its own name, and any other
    producer the guard may reference resolves to latest_before(t).

    ``src_state`` is the firing node's own state dict (read-only for guards),
    exposed via ``view.state`` so a guard like "retry under max" can read
    ``view.state["attempts"]``.
    """
    resolved: dict[str, Resolved] = {}
    # The firing node itself, with its just-produced output.
    resolved[src] = Resolved(value=src_output, k=None)
    for name in graph.nodes:
        if name == src:
            continue
        v = history.read(name, "latest", None, t)
        resolved[name] = Resolved(value=v, k=None)
    state_view = _NodeStateView(src_state or {})
    return DictView(resolved, state_view, src)


class _NodeStateView:
    """A thin read/write proxy over a node's state dict. Bodies write through
    ``view.state["k"] = v``; the underlying dict is the marking's slot, so
    writes land in the next marking (committed in Phase A). Guards receive a
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
