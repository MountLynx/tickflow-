from __future__ import annotations

"""Static deadlock detection and OR-join promotion.

Deadlock pattern
----------------
A node ``M`` is an AND-join with >=2 producers ``P1..Pn``. There exists an
*XOR-splitter* ``B`` (a node with >=2 guarded out-edges ``--|g|-->``) such
that >=2 of ``M``'s producers lie on distinct, mutually-exclusive branches
reaching from ``B``. Under AND-join, ``M`` waits for *all* producers' slots
to be True, but each fire of ``B`` sets at most one branch's slots True (the
other branch's guard returns False that tick). The "other half" never arrives
within one ``B`` cycle, so ``M`` never fires -> deadlock.

Solution
--------
Promote ``M`` to OR-join: ``M`` fires when >=1 input slot is True. With the
synchronous step semantics here, this is *decidable* -- we never need to
answer "could more tokens still arrive?" (the open Petri-net OR-join
problem). A failing branch simply leaves its slot False; OR-join ignores
it. Each tick ``M`` fires at most once (after firing it consumes all its
slots -> False), so no double-firing.

Workflow
--------
``check(graph)`` returns a list of :class:`DeadlockSuggestion` -- it does
**not** mutate. The caller (CLI) prompts the user; on confirm it calls
``promote(suggestion)`` which flips ``M.join`` to "OR". Re-running ``check``
yields no further suggestion for ``M`` (OR-joins don't match the pattern).
Suggestions left un-promoted cause :class:`DeadlockError` when the
:class:`Runner` is constructed -- we never silently run a graph that will
hang.

Branch analysis
---------------
We compute, for each splitter ``B``, the set of nodes reachable from each
guarded out-edge of ``B`` *before re-merging*: two branches are
"mutually exclusive" if their downstream reachable sets are disjoint.
Concretely: branch_i = reachable from (B's i-th guarded edge) *without*
passing back through any of B's guarded edges' dsts' shared join points.
We use a simpler, conservative approximation that is sound (no false
negatives, only over-approximation of reach):

    branch_i = the set of nodes reachable from e_i.dst by walking
               out-edges, *stopping* when we reach any node that has >=2
               producers (a potential merge) -- because past a merge the
               branch's exclusivity is no longer locally provable.

Two producers P_a, P_b of M are "on distinct branches of B" if there exist
guarded out-edges e_a, e_b of B with e_a != e_b and P_a in branch(e_a) and
P_b in branch(e_b). This is sufficient to flag the deadlock; the user
confirms intent.
"""

import logging
from dataclasses import dataclass
from typing import Iterable

from .ir import Graph

log = logging.getLogger(__name__)


@dataclass
class DeadlockSuggestion:
    node: str           # the AND-join M that would deadlock
    producers: list[str]# the >=2 producers of M lying on distinct branches
    splitter: str       # the XOR-splitter B responsible
    branches: dict[str, list[str]]
    # branches: guard name -> list of downstream node names on that branch

    @property
    def msg(self) -> str:
        return (
            f"AND-join {self.node!r} has producers {self.producers} on mutually-"
            f"exclusive branches of XOR-splitter {self.splitter!r} "
            f"(branches={ {g: v for g, v in self.branches.items()} }). "
            f"It would deadlock under AND-join; promote to OR-join?"
        )


class DeadlockError(RuntimeError):
    """Raised when a graph still has unresolved AND-join deadlocks at run
    time (i.e. ``check`` returned suggestions the caller did not promote)."""

    def __init__(self, suggestions: list[DeadlockSuggestion]) -> None:
        self.suggestions = suggestions
        super().__init__(
            "unresolved AND-join deadlock(s):\n" + "\n".join(s.msg for s in suggestions)
        )


def _reachable_until_merge(graph: Graph, start: str) -> set[str]:
    """BFS from ``start`` over out-edges, stopping at any node that has >=2
    producers (a candidate merge) -- beyond which exclusivity is not locally
    provable. ``start`` itself is included."""
    seen: set[str] = {start}
    frontier: list[str] = [start]
    while frontier:
        n = frontier.pop()
        # Stop expanding this node if it's a candidate merge (>=2 producers).
        if n != start and len(graph.producers(n)) >= 2:
            continue
        for e in graph.out_edges(n):
            if e.dst not in seen:
                seen.add(e.dst)
                frontier.append(e.dst)
    return seen


def _branches_of(graph: Graph, splitter: str) -> dict[str, list[str]]:
    """For an XOR-splitter B, map each guarded-out-edge's guard name to the
    reachable-until-merge set of its destination."""
    branches: dict[str, list[str]] = {}
    for e in graph.out_edges(splitter):
        if e.guard is None:
            continue
        reach = _reachable_until_merge(graph, e.dst)
        # record the branch; remove the splitter itself from the branch
        reach.discard(splitter)
        branches.setdefault(e.guard, sorted(reach))
    return branches


def _producers_on_distinct_branches(
    graph: Graph, m: str, branches: dict[str, list[str]]
) -> tuple[list[str], str] | None:
    """Given branches (guard -> node list) for one splitter, return a pair of
    M's producers that lie on two distinct branches, plus the splitter name --
    or None if no such pair exists. We pick the first qualifying pair."""
    prods = graph.producers(m)
    # Map each producer to the set of guards whose branch contains it.
    prod_to_guards: dict[str, set[str]] = {}
    for p in prods:
        gs = {g for g, lst in branches.items() if p in lst}
        if gs:
            prod_to_guards[p] = gs
    # Find two producers with disjoint guard sets (different branches).
    pairs = list(prod_to_guards.items())
    for i in range(len(pairs)):
        for j in range(i + 1, len(pairs)):
            if not (pairs[i][1] & pairs[j][1]):  # disjoint -> distinct branches
                return [pairs[i][0], pairs[j][0]], ""
    return None


def check(graph: Graph) -> list[DeadlockSuggestion]:
    """Return all deadlock suggestions for ``graph``. Does not mutate."""
    out: list[DeadlockSuggestion] = []
    for m, node in graph.nodes.items():
        if node.join != "AND":
            continue
        prods = graph.producers(m)
        if len(prods) < 2:
            continue
        # Find any XOR-splitter B such that >=2 of M's producers lie on
        # distinct branches of B.
        for b in graph.nodes:
            if not graph.is_xor_splitter(b):
                continue
            branches = _branches_of(graph, b)
            res = _producers_on_distinct_branches(graph, m, branches)
            if res is not None:
                pair = res[0]
                out.append(
                    DeadlockSuggestion(
                        node=m,
                        producers=pair,
                        splitter=b,
                        branches=branches,
                    )
                )
    # Dedupe by (node, splitter) -- multiple producer pairs on same splitter
    # collapse to one suggestion.
    seen: set[tuple[str, str]] = set()
    deduped: list[DeadlockSuggestion] = []
    for s in out:
        key = (s.node, s.splitter)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(s)
    return deduped


def promote(suggestion: DeadlockSuggestion, graph: Graph) -> None:
    """Flip ``suggestion.node``'s join to OR in ``graph``. Idempotent."""
    graph.nodes[suggestion.node].join = "OR"


# --- unguarded cycle detection -----------------------------------------------


@dataclass
class UnguardedCycleWarning:
    """Warning: a cycle with no guarded edge will loop forever."""

    nodes: list[str]  # node names in the strongly connected component

    @property
    def msg(self) -> str:
        names = ", ".join(self.nodes)
        return (
            f"Cycle [{names}] has no guarded edge — it will loop forever "
            f"without an exit condition. Add a guard (--|guard|-->) to at "
            f"least one edge in the cycle."
        )


def _find_sccs(graph: Graph) -> list[set[str]]:
    """Tarjan's algorithm: return all strongly connected components."""
    index_counter = 0
    indices: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    sccs: list[set[str]] = []

    def _strongconnect(v: str) -> None:
        nonlocal index_counter
        indices[v] = index_counter
        lowlink[v] = index_counter
        index_counter += 1
        stack.append(v)
        on_stack.add(v)

        for e in graph.out_edges(v):
            w = e.dst
            if w not in indices:
                _strongconnect(w)
                lowlink[v] = min(lowlink[v], lowlink[w])
            elif w in on_stack:
                lowlink[v] = min(lowlink[v], indices[w])

        if lowlink[v] == indices[v]:
            scc: set[str] = set()
            while True:
                w = stack.pop()
                on_stack.discard(w)
                scc.add(w)
                if w == v:
                    break
            sccs.append(scc)

    for node in graph.nodes:
        if node not in indices:
            _strongconnect(node)

    return sccs


def check_unguarded_cycles(graph: Graph) -> list[UnguardedCycleWarning]:
    """Find cycles that have no guarded edge and will therefore loop forever.

    Returns a list of warnings; does **not** mutate the graph.
    """
    sccs = _find_sccs(graph)
    warnings: list[UnguardedCycleWarning] = []

    for scc in sccs:
        # Single-node SCC: only warn if it has a self-loop.
        if len(scc) == 1:
            node = next(iter(scc))
            has_self_loop = any(
                e.src == node and e.dst == node for e in graph.edges
            )
            if not has_self_loop:
                continue

        # Edges where both src and dst are inside this SCC.
        internal = [e for e in graph.edges if e.src in scc and e.dst in scc]
        if not internal:
            continue

        # If NO internal edge is guarded → guaranteed infinite loop.
        if not any(e.guard is not None for e in internal):
            warnings.append(UnguardedCycleWarning(nodes=sorted(scc)))

    return warnings


def resolve_or_raise(graph: Graph, suggestions: Iterable[DeadlockSuggestion]) -> None:
    """Convenience: promote every suggestion in ``suggestions``. If after
    promotion ``check`` still yields suggestions (impossible in v1 unless the
    caller promoted some but not all), raise."""
    for s in suggestions:
        promote(s, graph)
    remaining = check(graph)
    if remaining:
        raise DeadlockError(remaining)
