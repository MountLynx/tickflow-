"""Mermaid-like graph parser.

Grammar (informal)
------------------
A graph is a sequence of lines. Blank lines and ``#`` comments are ignored.

Edge lines (the only structural statements)::

    A-->B              plain edge: A produces True into B's slot, always
    [A]-->B            A is a start node; same as above
    B--|g1|-->C        guarded edge: slot is True iff guard ``g1`` returns True
    [A]--|g1|-->B      start + guard combined

The square brackets mark a start node (``[A]``). Multiple starts are allowed.
``[`` ``]`` may only appear on the *source* side of an edge; a node becomes a
start the first time it's seen in brackets. To declare a start with no
outgoing edge, write ``[A]`` on its own line.

Declaration lines (optional; defaults below)::

    C.inputs: A, B[2]   C reads A (latest_before) and B's 2nd fire (1-based)
    C.body: compute_c   C's body is the registered callable ``compute_c``
    C.join: OR          override join (AND default); usually set via checker

Node names are ``[A-Za-z0-9_]+``. Guards are names too. Whitespace around
tokens is ignored.

Defaults
--------
- ``body``: ``None`` -> identity (echo first declared input).
- ``inputs``: every producer of the node, each with ``latest_before`` policy.
  Declaring ``C.inputs:`` *replaces* (not merges) this default for C.
- ``join``: ``AND`` (checker may flip to ``OR``).

Validation
----------
After parsing, :func:`parse` checks:
- no edge references an undeclared node (a node is declared by any edge or
  declaration mentioning it),
- at least one start node exists,
- every guarded edge's guard name and every declared body name exist in the
  given :class:`Registry` (pass ``registry=None`` to skip the lookup check).

Errors raise :class:`ParseError` with the line number and a message.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Iterable

from .ir import Graph, Node, Edge, InputPolicy
from .registry import Registry, registry as _default_registry
from .checker import check_unguarded_cycles

log = logging.getLogger(__name__)


_NAME = r"[A-Za-z_][A-Za-z0-9_]*"

# Matches: optional [src] , then src name, then -->(dst) or --|guard|-->dst
# We match the source part and the edge part separately for clarity.
_SRC_RE = re.compile(rf"^\[(?P<src>{_NAME})\](?P<rest>.*)$")
_EDGE_RE = re.compile(
    rf"^(?P<src>{_NAME})\s*(?P<edge>-->\s*(?P<dst>{_NAME})|--\|(?P<guard>{_NAME})\|-->\s*(?P<gdst>{_NAME}))\s*$"
)
_INPUTS_RE = re.compile(rf"^(?P<node>{_NAME})\.inputs\s*:\s*(?P<spec>.+)$")
_BODY_RE = re.compile(rf"^(?P<node>{_NAME})\.body\s*:\s*(?P<body>{_NAME})$")
_JOIN_RE = re.compile(rf"^(?P<node>{_NAME})\.join\s*:\s*(?P<join>AND|OR)$")
# Bare start declaration: "[A]" alone on a line.
_BARE_START_RE = re.compile(rf"^\[(?P<node>{_NAME})\]\s*$")
# A single input term: "A" or "A[2]"
_INPUT_TERM_RE = re.compile(rf"^(?P<name>{_NAME})(?:\[(?P<k>\d+)\])?$")


class ParseError(ValueError):
    def __init__(self, msg: str, lineno: int | None = None) -> None:
        self.lineno = lineno
        super().__init__(f"line {lineno}: {msg}" if lineno is not None else msg)


@dataclass
class _ParsedEdge:
    src: str
    dst: str
    guard: str | None
    src_is_start: bool


def _strip_comment(line: str) -> str:
    # ``#`` starts a comment unless we ever support quoting; we don't.
    i = line.find("#")
    return line[:i] if i >= 0 else line


def _parse_inputs_spec(spec: str, lineno: int) -> dict[str, InputPolicy]:
    out: dict[str, InputPolicy] = {}
    for term in spec.split(","):
        term = term.strip()
        if not term:
            continue
        m = _INPUT_TERM_RE.match(term)
        if not m:
            raise ParseError(f"bad input term {term!r}", lineno)
        name = m.group("name")
        k = m.group("k")
        out[name] = InputPolicy.latest() if k is None else InputPolicy.index(int(k))
    return out


def parse(text: str, registry: Registry | None = None) -> Graph:
    """Parse graph text into a :class:`Graph`.

    ``registry`` defaults to the module-level :data:`tickflow.registry`. When
    provided, guard and body names are validated to exist; pass a fresh
    ``Registry()`` (empty) only if you intend to register later and want to
    skip the check -- but you'd then lose validation, so prefer registering
    first.
    """
    reg = registry if registry is not None else _default_registry
    g = Graph()
    parsed_edges: list[_ParsedEdge] = []
    # Track explicit inputs declarations so we know not to auto-fill.
    explicit_inputs: set[str] = set()

    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = _strip_comment(raw).strip()
        if not line:
            continue

        # Bare start: "[A]"
        if (m := _BARE_START_RE.match(line)):
            _ensure_node(g, m.group("node"), is_start=True)
            continue

        # declarations: inputs / body / join
        if (m := _INPUTS_RE.match(line)):
            node = m.group("node")
            _ensure_node(g, node)
            g.nodes[node].inputs = _parse_inputs_spec(m.group("spec"), lineno)
            explicit_inputs.add(node)
            continue
        if (m := _BODY_RE.match(line)):
            node = m.group("node")
            _ensure_node(g, node)
            g.nodes[node].body = m.group("body")
            continue
        if (m := _JOIN_RE.match(line)):
            node = m.group("node")
            _ensure_node(g, node)
            g.nodes[node].join = m.group("join")  # type: ignore[assignment]
            continue

        # edge with bracketed start source: "[A]-->B" or "[A]--|g|-->B"
        src_is_start = False
        if (m := _SRC_RE.match(line)):
            src_name = m.group("src")
            rest = m.group("rest").strip()
            src_is_start = True
            # Re-form a plain edge line for the edge regex.
            em = _EDGE_RE.match(f"{src_name}{rest}")
            if not em:
                raise ParseError(f"bad edge after start marker: {line!r}", lineno)
        else:
            em = _EDGE_RE.match(line)
            if not em:
                raise ParseError(f"unrecognized line: {line!r}", lineno)

        src = em.group("src")
        if em.group("dst"):
            dst, guard = em.group("dst"), None
        else:
            dst, guard = em.group("gdst"), em.group("guard")

        _ensure_node(g, src, is_start=src_is_start)
        _ensure_node(g, dst)
        parsed_edges.append(_ParsedEdge(src=src, dst=dst, guard=guard, src_is_start=src_is_start))

    # Commit edges
    for pe in parsed_edges:
        g.edges.append(Edge(src=pe.src, dst=pe.dst, guard=pe.guard))

    # Auto-fill inputs: every producer -> latest_before, unless declared.
    for name, node in g.nodes.items():
        if name in explicit_inputs:
            continue
        for prod in g.producers(name):
            node.inputs.setdefault(prod, InputPolicy.latest())

    _validate(g, reg, len(text.splitlines()))

    # Warn about cycles with no guarded edge (potential infinite loops).
    for w in check_unguarded_cycles(g):
        log.warning(w.msg)

    return g


def _ensure_node(g: Graph, name: str, is_start: bool = False) -> None:
    if name not in g.nodes:
        g.nodes[name] = Node(name=name, is_start=is_start)
    elif is_start:
        g.nodes[name].is_start = True


def _validate(g: Graph, reg: Registry, n_lines: int) -> None:
    if not g.starts:
        raise ParseError("no start node declared (use [A] to mark one)", n_lines)
    # guard / body name lookups
    for e in g.edges:
        if e.guard is not None and not reg.has_guard(e.guard):
            raise ParseError(f"guard '{e.guard}' not registered", n_lines)
    for name, node in g.nodes.items():
        if node.body is not None and not reg.has_body(node.body):
            raise ParseError(f"body '{node.body}' for node {name!r} not registered", n_lines)
    # declared-input producers must actually be producers (warn-level: raise
    # to catch typos early -- a body reading a non-producer would always get
    # Missing).
    for name, node in g.nodes.items():
        producers = set(g.producers(name))
        for src in node.inputs:
            if src not in producers:
                raise ParseError(
                    f"node {name!r} declares input from {src!r} which is not a producer "
                    f"(producers: {sorted(producers) or 'none'})",
                    n_lines,
                )
