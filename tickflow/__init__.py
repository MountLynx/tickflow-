"""flow — a small Petri-style workflow control framework.

Public surface:

    from tickflow import Graph, parse, check, Runner, Registry

Quick sketch::

    [start]-->B
    B.body: decide
    B--|go_a|-->A
    B--|go_d|-->D
    A-->Merge
    D-->Merge
    Merge.body: merge

Each node carries a boolean "slot" per incoming edge. A node fires when its
join predicate (AND: all slots True; OR: >=1 slot True) holds over the current
marking. Firing consumes its input slots (resets to False) and, after every
node in the tick has fired, produces a True/False into each downstream slot
via the edge (plain edges always True, guarded edges evaluate the guard
function against a read-only view of history). Outputs land in an append-only
history keyed by (node, tick); reads default to ``latest_before(t)`` and may
pin a specific fire with ``A[k]`` syntax.

Because the engine is a pure function ``f(marking_t, history_{<t}) ->
(marking_{t+1}, firings_t)`` with no hidden in-flight state, snapshots are a
cheap JSON triple (marking + history + tick) and ``restore`` is a trivial
rewind. See ``runner.Runner``.
"""

from .ir import Graph, Node, Edge, InputPolicy, Failure
from .parser import parse, ParseError
from .checker import (
    check,
    promote,
    resolve_or_raise,
    DeadlockSuggestion,
    DeadlockError,
    check_unguarded_cycles,
    UnguardedCycleWarning,
)
from .registry import Registry, registry, Body, Guard
from .engine import Marking, tick, bootstrap
from .state import NodeState, RunState
from .runner import Runner, RunStatus
from .persistence import Backend, JsonBackend, SqliteBackend

__all__ = [
    "Graph",
    "Node",
    "Edge",
    "InputPolicy",
    "Failure",
    "parse",
    "ParseError",
    "check",
    "promote",
    "resolve_or_raise",
    "DeadlockSuggestion",
    "DeadlockError",
    "check_unguarded_cycles",
    "UnguardedCycleWarning",
    "Registry",
    "registry",
    "Body",
    "Guard",
    "Marking",
    "NodeState",
    "RunState",
    "tick",
    "bootstrap",
    "Runner",
    "RunStatus",
    "Backend",
    "JsonBackend",
    "SqliteBackend",
]
