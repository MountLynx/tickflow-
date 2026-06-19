"""Tests for the static deadlock checker + OR-join promotion."""
from __future__ import annotations

import pytest

from tickflow import parse, check, promote, DeadlockSuggestion, DeadlockError, Runner, Registry


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
    nodes_fired = {f.node for f in r.audit}
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
