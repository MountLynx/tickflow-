"""Core IR: the structural dataclasses the rest of the library operates on.

Design notes
------------
- ``Edge.guard`` is ``None`` for a plain ``-->`` (always-True data-flow edge) or
  the registered guard function's name for ``--|name|-->``. The slot value it
  produces is recorded per tick and read by the downstream node's join.
- ``Node.inputs`` maps a *producer name* to an :class:`InputPolicy` describing
  which historical fire the body should consume. A producer absent from the
  dict still contributes a slot to the marking (control flow is independent of
  data binding) but is not surfaced to the body.
- ``Node.join`` is ``"AND"`` by default; the checker may flip it to ``"OR"``
  after confirming a deadlock pattern with the caller. Slot bit-width is
  unchanged; only the join predicate differs.
- ``Node.body`` is a registry key string, not a callable -- snapshots store
  structure only, never code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class Failure:
    """A node body may return ``Failure(...)`` instead of a normal value to
    signal that the node did not produce a usable output.

    - ``type="llm"`` (default): a recoverable/logical failure (bad LLM output,
      compliance violation, parse error). The node's out-edges write ``False``
      so downstream AND-joins naturally don't fire (= "upstream failed, skip
      downstream"). The run continues.
    - ``type="infrastructure"``: an unrecoverable failure (network down, OOM,
      auth error). Out-edges write ``False`` **and** the runner enters the
      ``ABORTED`` state, halting all further ticks.

    A ``Failure`` is still written to history (so audits/replays see what
    happened) and still consumes its input slots.
    """

    error: str
    type: Literal["llm", "infrastructure"] = "llm"


@dataclass
class InputPolicy:
    """How a node reads a producer's output from history at tick ``t``.

    - ``kind="latest"``  -> most recent fire of the producer with ``tick < t``
                             (the Petri marking-consistent read; the default,
                             so loops don't read their own same-tick output).
    - ``kind="index"``   -> the producer's ``k``-th fire, regardless of tick
                             (``A[2]`` syntax). ``k`` is 1-based; used for
                             cross-iteration pinning / audit replays.
    """

    kind: Literal["latest", "index"] = "latest"
    k: int | None = None

    @classmethod
    def latest(cls) -> "InputPolicy":
        return cls(kind="latest", k=None)

    @classmethod
    def index(cls, k: int) -> "InputPolicy":
        if k < 1:
            raise ValueError("input index is 1-based; k>=1 required")
        return cls(kind="index", k=k)


@dataclass
class Edge:
    src: str
    dst: str
    guard: str | None  # None => plain "-->" (always True); else guard fn name


@dataclass
class Node:
    name: str
    is_start: bool = False
    join: Literal["AND", "OR"] = "AND"
    body: str | None = None  # registry key; None => identity (echo inputs)
    # producer name -> how this node reads that producer's history
    inputs: dict[str, InputPolicy] = field(default_factory=dict)


@dataclass
class Graph:
    nodes: dict[str, Node] = field(default_factory=dict)
    edges: list[Edge] = field(default_factory=list)
    # Lazy adjacency index (C4): edges are append-only in this codebase
    # (parser.py:183), so a length change invalidates; excluded from
    # repr/eq (preserves legacy Graph semantics).
    _adj: tuple[
        dict[str, list[Edge]], dict[str, set[str]], dict[str, set[str]]
    ] | None = field(default=None, repr=False, compare=False)
    _adj_len: int = field(default=-1, repr=False, compare=False)

    @property
    def starts(self) -> list[str]:
        return [n for n, node in self.nodes.items() if node.is_start]

    # --- adjacency helpers (used by engine + checker) ---------------------

    def _ensure_adj(self) -> None:
        """Rebuild the adjacency index (on first call or when the edge list
        length changes)."""
        if self._adj is not None and len(self.edges) == self._adj_len:
            return
        out: dict[str, list[Edge]] = {}
        prod: dict[str, set[str]] = {}
        cons: dict[str, set[str]] = {}
        for e in self.edges:
            out.setdefault(e.src, []).append(e)
            prod.setdefault(e.dst, set()).add(e.src)
            cons.setdefault(e.src, set()).add(e.dst)
        self._adj = (out, prod, cons)
        self._adj_len = len(self.edges)

    def producers(self, node: str) -> list[str]:
        """Distinct producer nodes with at least one edge into ``node``."""
        self._ensure_adj()
        return sorted(self._adj[1].get(node, ()))

    def out_edges(self, node: str) -> list[Edge]:
        """Out-edges of ``node`` (returns a new list; safe for the caller
        to hold)."""
        self._ensure_adj()
        return list(self._adj[0].get(node, ()))

    def consumers(self, node: str) -> list[str]:
        self._ensure_adj()
        return sorted(self._adj[2].get(node, ()))

    def is_xor_splitter(self, node: str) -> bool:
        """A node is an XOR-splitter if it has >=2 guarded out-edges."""
        guarded = sum(1 for e in self.out_edges(node) if e.guard is not None)
        return guarded >= 2

    def copy(self) -> "Graph":
        """Deep-ish copy: new nodes dict (with copied InputPolicy maps) +
        new edge list. Used by the checker when it mutates join types so the
        original graph is preserved for the caller to diff against."""
        g = Graph()
        for name, node in self.nodes.items():
            g.nodes[name] = Node(
                name=node.name,
                is_start=node.is_start,
                join=node.join,
                body=node.body,
                inputs={k: InputPolicy(kind=v.kind, k=v.k) for k, v in node.inputs.items()},
            )
        g.edges = [Edge(src=e.src, dst=e.dst, guard=e.guard) for e in self.edges]
        return g

    # --- export (for visualisation / front-end) ---------------------------

    def to_dict(self) -> dict:
        """Structured JSON-able view of the graph for front-end rendering.

        Includes derived ``producers`` per node so the front-end doesn't have
        to recompute adjacency. Output is plain data (JSON-serialisable).
        """
        return {
            "nodes": {
                name: {
                    "is_start": n.is_start,
                    "join": n.join,
                    "body": n.body,
                    "inputs": {
                        k: {"kind": v.kind, "k": v.k}
                        for k, v in n.inputs.items()
                    },
                    "producers": self.producers(name),
                }
                for name, n in self.nodes.items()
            },
            "edges": [
                {"src": e.src, "dst": e.dst, "guard": e.guard} for e in self.edges
            ],
            "starts": self.starts,
        }

    def to_mermaid(self) -> str:
        """Render the graph as mermaid ``graph TD`` text (for READMEs /
        debuggers / any mermaid renderer).

        - Start nodes use the stadium shape ``([name])``.
        - Plain edges: ``A --> B``.
        - Guarded edges: ``A -->|guard| B``.
        - Nodes are emitted in sorted order for stable output.
        """
        lines = ["graph TD"]
        # Declare nodes first (sorted) so shape/label is stable regardless of
        # edge order. Non-start nodes need no explicit declaration (mermaid
        # infers them from edges), but declaring starts gives the stadium shape.
        for name in sorted(self.nodes):
            node = self.nodes[name]
            if node.is_start:
                lines.append(f'    {name}(["{name}"])')
        # Edges.
        for e in self.edges:
            if e.guard is None:
                lines.append(f"    {e.src} --> {e.dst}")
            else:
                lines.append(f"    {e.src} -->|{e.guard}| {e.dst}")
        return "\n".join(lines)
