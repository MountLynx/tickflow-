"""Tests for the static deadlock checker + OR-join promotion."""
from __future__ import annotations

import pytest

from tickflow import (
    parse, check, promote, DeadlockSuggestion, DeadlockError, Runner, Registry,
    check_unguarded_cycles, UnguardedCycleWarning,
)


def _reg() -> Registry:
    r = Registry()
    r.body("decide", lambda v: "go_a")
    r.body("merge", lambda v: None)
    r.guard("go_a", lambda v: True)
    r.guard("go_d", lambda v: False)
    return r


def _xor_merge_text() -> str:
    return """
[start]-->B
B.body: decide
B--|go_a|-->A
B--|go_d|-->D
A-->Merge
D-->Merge
Merge.body: merge
"""


def test_xor_after_and_triggers_suggestion():
    g = parse(_xor_merge_text(), registry=_reg())
    sugs = check(g)
    assert len(sugs) == 1
    s = sugs[0]
    assert s.node == "Merge"
    assert s.splitter == "B"
    assert set(s.producers) == {"A", "D"}


def test_promote_flips_to_or_and_recheck_clean():
    g = parse(_xor_merge_text(), registry=_reg())
    for s in check(g):
        promote(s, g)
    assert g.nodes["Merge"].join == "OR"
    assert check(g) == []


def test_unresolved_suggestion_raises_at_runner_construction():
    g = parse(_xor_merge_text(), registry=_reg())
    # Don't promote; Runner(strict_deadlock=True default) should raise.
    with pytest.raises(DeadlockError):
        Runner(g, _reg())


def test_after_promote_runner_constructs_and_runs():
    g = parse(_xor_merge_text(), registry=_reg())
    for s in check(g):
        promote(s, g)
    r = Runner(g, _reg())
    r.run_until_idle(max_ticks=20)
    # B fires once, picks go_a; A fires; Merge fires (OR). D never fires.
    nodes_fired = {f.node for f in r.audit_log()}
    assert "B" in nodes_fired and "A" in nodes_fired and "Merge" in nodes_fired
    assert "D" not in nodes_fired


def test_no_splitter_no_suggestion():
    # Plain linear: no XOR splitter -> no suggestion.
    r = Registry()
    r.body("b", lambda v: None)
    g = parse("[A]-->B\n[A]-->C\nB-->D\nC-->D\nD.body: b", registry=r)
    # B and C are both producers of D (AND-join) but A is not a splitter
    # (no guarded edges), so no deadlock pattern.
    assert check(g) == []


def test_no_loop_no_suggestion():
    # The earlier loop test: A is OR-join but no XOR splitter -> no suggestion.
    r = Registry()
    r.body("b", lambda v: None)
    r.guard("c", lambda v: True)
    g = parse("[A]-->B\nB.body: b\nB--|c|-->A\nA.join: OR", registry=r)
    assert check(g) == []


def test_single_guarded_edge_not_splitter():
    # B has only one guarded edge -> not a splitter -> no suggestion.
    r = Registry()
    r.body("b", lambda v: None)
    r.guard("c", lambda v: True)
    g = parse("[A]-->B\nB.body: b\nB--|c|-->C\nC.body: b", registry=r)
    assert check(g) == []


# --- unguarded cycle detection -------------------------------------------


def _bare_reg() -> Registry:
    r = Registry()
    r.body("passthru", lambda v: None)
    return r


def test_two_node_cycle_no_guard_warns():
    """A-->B-->A with no guards → warning."""
    r = _bare_reg()
    g = parse("[A]-->B\nB-->A\nA.join: OR", registry=r)
    warnings = check_unguarded_cycles(g)
    assert len(warnings) == 1
    assert set(warnings[0].nodes) == {"A", "B"}


def test_self_loop_no_guard_warns():
    """[A]-->A with no guard → warning."""
    r = _bare_reg()
    g = parse("[A]-->A\nA.join: OR", registry=r)
    warnings = check_unguarded_cycles(g)
    assert len(warnings) == 1
    assert warnings[0].nodes == ["A"]


def test_cycle_with_guard_no_warning():
    """A--|g|-->B-->A has a guarded edge → no warning."""
    r = _bare_reg()
    r.guard("g", lambda v: True)
    g = parse("[A]--|g|-->B\nB-->A\nA.join: OR", registry=r)
    warnings = check_unguarded_cycles(g)
    assert warnings == []


def test_self_loop_with_guard_no_warning():
    """[A]--|g|-->A has a guarded edge → no warning."""
    r = _bare_reg()
    r.guard("g", lambda v: True)
    g = parse("[A]--|g|-->A\nA.join: OR", registry=r)
    warnings = check_unguarded_cycles(g)
    assert warnings == []


def test_linear_graph_no_warning():
    """[A]-->B-->C has no cycle → no warning."""
    r = _bare_reg()
    g = parse("[A]-->B\nB-->C", registry=r)
    warnings = check_unguarded_cycles(g)
    assert warnings == []


def test_diamond_no_cycle_no_warning():
    """A-->B, A-->C, B-->D, C-->D (DAG) → no warning."""
    r = _bare_reg()
    g = parse("[A]-->B\n[A]-->C\nB-->D\nC-->D\nD.join: OR", registry=r)
    warnings = check_unguarded_cycles(g)
    assert warnings == []


def test_three_node_cycle_no_guard_warns():
    """A-->B-->C-->A with no guards → warning."""
    r = _bare_reg()
    g = parse("[A]-->B\nB-->C\nC-->A\nA.join: OR", registry=r)
    warnings = check_unguarded_cycles(g)
    assert len(warnings) == 1
    assert set(warnings[0].nodes) == {"A", "B", "C"}


def test_cycle_partial_guard_still_ok():
    """A-->B--|g|-->C-->A: B→C is guarded, so the cycle can terminate → no warning."""
    r = _bare_reg()
    r.guard("g", lambda v: True)
    g = parse("[A]-->B\nB--|g|-->C\nC-->A\nA.join: OR", registry=r)
    warnings = check_unguarded_cycles(g)
    assert warnings == []
