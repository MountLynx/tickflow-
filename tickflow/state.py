"""Unified state recording: ``NodeState`` + ``RunState``.

``NodeState`` is the single source of truth for everything that happened to
a node at a given tick — inputs, output, edge propagation, status, and the
node's mutable state after the body ran.

``RunState`` manages all ``NodeState`` records and maintains three internal
layers with distinct responsibilities::

    _edges   — output index, always maintained, for ``resolve()``
    _state   — current mutable state per node, always maintained, O(1)
    _records — full audit trail, only when ``keep_records=True``

Derived artefacts (audit, snapshot, persistence) are extracted from these
three layers via the public API.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Literal

from .views import Missing


def _jsonable(v: Any) -> Any:
    """Coerce ``v`` to something json.dumps can serialise."""
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


@dataclass
class NodeState:
    """Complete runtime record for one node firing at one tick.

    ``edges_fired`` starts empty and is filled in during Phase B.
    """

    tick: int
    node: str
    inputs: dict[str, Any] = field(default_factory=dict)
    output: Any = None
    edges_fired: list[tuple[str, str | None, bool]] = field(default_factory=list)
    # edges_fired: (dst, guard_name_or_None, slot_value_written)
    status: Literal["ok", "failed", "aborted"] = "ok"
    error: str | None = None
    mutable_state: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict:
        return {
            "tick": self.tick,
            "node": self.node,
            "inputs": {k: _jsonable(v) for k, v in self.inputs.items()},
            "output": _jsonable(self.output),
            "edges_fired": [[dst, g, v] for (dst, g, v) in self.edges_fired],
            "status": self.status,
            "error": self.error,
            "mutable_state": _jsonable(self.mutable_state),
        }

    @classmethod
    def from_json(cls, d: dict) -> "NodeState":
        return cls(
            tick=d["tick"],
            node=d["node"],
            inputs=d.get("inputs", {}),
            output=d.get("output"),
            edges_fired=[(dst, g, v) for (dst, g, v) in d.get("edges_fired", [])],
            status=d.get("status", "ok"),
            error=d.get("error"),
            mutable_state=d.get("mutable_state", {}),
        )


class RunState:
    """Central state manager — owns all :class:`NodeState` records.

    Three internal layers with distinct responsibilities::

        _edges   — ``dict[node, list[(tick, value)]]``
                   Output index for fast ``resolve()``.  Always maintained.

        _state   — ``dict[node, dict[str, Any]]``
                   Current mutable state per node (latest ``mutable_state``
                   from the most recent firing).  Always maintained, O(1).

        _records — ``list[NodeState]``
                   Full audit trail.  Only maintained when ``keep_records=True``.

    Derived artefacts
    -----------------
    - **audit** (:meth:`audit`) → built from ``_records``.
    - **snapshot** (:meth:`to_snapshot_data`) → ``edges`` + ``state`` always,
      ``records`` only when audit enabled.
    - **persistence** — backends call ``to_snapshot_data()`` for snapshots and
      ``save_firing()`` for individual :class:`NodeState` records.

    Parameters
    ----------
    keep_records : bool
        When False, ``_records`` is not populated (saving memory), but
        ``_edges`` and ``_state`` are still maintained — so input resolution
        and node mutable state work correctly regardless of this switch.
    """

    def __init__(self, keep_records: bool = True) -> None:
        # Layer 1: output index for resolve() — always maintained.
        self._edges: dict[str, list[tuple[int, Any]]] = {}
        # Layer 2: current mutable state per node — always maintained.
        self._state: dict[str, dict[str, Any]] = {}
        # Layer 3: full audit records — only when keep_records=True.
        self._records: list[NodeState] = []
        self._keep_records = keep_records

    # ------------------------------------------------------------------
    # Core: record a firing
    # ------------------------------------------------------------------

    def record(self, ns: NodeState) -> None:
        """Record a node firing into all active layers."""
        self._edges.setdefault(ns.node, []).append((ns.tick, ns.output))
        self._state[ns.node] = dict(ns.mutable_state)  # defensive copy
        if self._keep_records:
            self._records.append(ns)

    # ------------------------------------------------------------------
    # Input resolution (replaces History.read)
    # ------------------------------------------------------------------

    def resolve(self, node: str, kind: str, k: int | None, t: int) -> Any:
        """Resolve a producer's output for a consumer firing at tick *t*.

        ``kind`` is ``"latest"`` (most recent fire with tick < t)
        or ``"index"`` (the k-th fire overall, 1-based).
        """
        entries = self._edges.get(node, [])
        if kind == "index":
            if k is None or k < 1 or k > len(entries):
                return Missing
            return entries[k - 1][1]
        # latest_before(t)
        last: tuple[int, Any] | None = None
        for tk, v in entries:
            if tk < t:
                last = (tk, v)
            else:
                break
        return last[1] if last is not None else Missing

    # ------------------------------------------------------------------
    # Firing history
    # ------------------------------------------------------------------

    def firings_of(self, node: str) -> list[tuple[int, Any]]:
        """Return ``[(tick, output), ...]`` for *node*, in tick order."""
        return list(self._edges.get(node, []))

    def last_output(self, node: str) -> Any:
        """The most recent output of *node*, or None if never fired."""
        entries = self._edges.get(node, [])
        return entries[-1][1] if entries else None

    # ------------------------------------------------------------------
    # Audit log (from _records)
    # ------------------------------------------------------------------

    def audit(self) -> list[NodeState]:
        """Full audit log. Empty when ``keep_records=False``."""
        return list(self._records)

    def tick_firings(self, tick: int) -> list[NodeState]:
        """All :class:`NodeState` records for a given *tick*."""
        return [ns for ns in self._records if ns.tick == tick]

    @property
    def keep_records(self) -> bool:
        return self._keep_records

    # ------------------------------------------------------------------
    # Mutable state (from _state — always available, O(1))
    # ------------------------------------------------------------------

    def mutable_state(self, node: str) -> dict[str, Any]:
        """Current mutable state for *node*.  Always returns a copy."""
        return dict(self._state.get(node, {}))

    def all_mutable_states(self) -> dict[str, dict[str, Any]]:
        """Current mutable state for every node that has ever fired.
        Returns copies; mutating them does not affect the run."""
        return {n: dict(s) for n, s in self._state.items()}

    # ------------------------------------------------------------------
    # Read-only views
    # ------------------------------------------------------------------

    @property
    def edges(self) -> dict[str, list[tuple[int, Any]]]:
        """Read-only view of the output index: ``node -> [(tick, value), ...]``."""
        return {n: list(lst) for n, lst in self._edges.items()}

    # ------------------------------------------------------------------
    # Snapshot data
    # ------------------------------------------------------------------

    def to_snapshot_data(self) -> dict:
        """JSON-able dict for ``Runner.snapshot()``.

        ``edges`` + ``state`` are always included; ``records`` only when
        ``keep_records=True``.
        """
        data: dict[str, Any] = {
            "edges": {
                n: [[t, _jsonable(v)] for (t, v) in lst]
                for n, lst in self._edges.items()
            },
            "state": {n: dict(s) for n, s in self._state.items()},
            "keep_records": self._keep_records,
        }
        if self._keep_records:
            data["records"] = [ns.to_json() for ns in self._records]
        return data

    @classmethod
    def from_snapshot_data(cls, d: dict) -> "RunState":
        """Reconstruct from ``to_snapshot_data()`` output."""
        keep_records = d.get("keep_records", True)
        rs = cls(keep_records=keep_records)
        for n, lst in d.get("edges", d.get("outputs", {})).items():
            rs._edges[n] = [(int(t), v) for (t, v) in lst]
        for n, s in d.get("state", {}).items():
            rs._state[n] = dict(s)
        for rec in d.get("records", []):
            rs._records.append(NodeState.from_json(rec))
        return rs

    # ------------------------------------------------------------------
    # Rewind
    # ------------------------------------------------------------------

    def truncate_after(self, tick: int) -> None:
        """Drop all records with ``tick > tick``."""
        for n in list(self._edges):
            kept = [(t, v) for (t, v) in self._edges[n] if t <= tick]
            if kept:
                self._edges[n] = kept
            else:
                del self._edges[n]
                self._state.pop(n, None)
        # Prune _records (if maintained) and rebuild _state from remaining records.
        if self._keep_records:
            self._records = [ns for ns in self._records if ns.tick <= tick]
            self._state.clear()
            for ns in reversed(self._records):
                if ns.node not in self._state:
                    self._state[ns.node] = ns.mutable_state
        else:
            self._records = []

    # ------------------------------------------------------------------
    # Filter nodes (for remap_graph)
    # ------------------------------------------------------------------

    def keep_nodes(self, node_names: set[str]) -> None:
        """Drop state for nodes not in *node_names*."""
        for n in list(self._edges):
            if n not in node_names:
                del self._edges[n]
                self._state.pop(n, None)
        self._records = [ns for ns in self._records if ns.node in node_names]

    # ------------------------------------------------------------------
    # Full serialisation
    # ------------------------------------------------------------------

    def to_json(self) -> dict:
        return self.to_snapshot_data()

    @classmethod
    def from_json(cls, d: dict) -> "RunState":
        return cls.from_snapshot_data(d)
