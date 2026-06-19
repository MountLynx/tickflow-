"""Tests for loops: latest_before semantics, A[k] index, termination."""
from __future__ import annotations

import pytest

from tickflow import parse, Runner, Registry, check
from tickflow.views import Missing


def _reg():
    r = Registry()

    @r.body("seed_zero")
    def _s(v):
        return 0

    @r.body("passthru")
    def _p(v):
        for name, val in v.items():
            if val is not Missing:
                return val
        return None

    @r.body("incr")
    def _incr(v):
        return v.A.value + 1

    @r.body("kth")
    def _kth(v):
        # Reads A's value (resolved by A's policy). If policy is index, the
        # k-th fire is delivered; if latest, the previous-iteration value.
        return v.A.value

    @r.guard("cont_lt3")
    def _c(v):
        return v.B.value < 3

    return r


def test_loop_terminates_when_guard_false():
    r = _reg()
    g = parse(
        "[seed]-->A\nseed.body: seed_zero\nA.body: passthru\nA.join: OR\nA-->B\nB.body: incr\nB--|cont_lt3|-->A",
        registry=r,
    )
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=50)
    b_outputs = [f.output for f in rn.audit if f.node == "B"]
    assert b_outputs == [1, 2, 3]
    assert rn.is_idle()


def test_loop_latest_before_no_same_iteration_crosstalk():
    # In the loop, A reads B's *previous* fire (latest_before(t)), not the
    # current-tick one. So A's value lags B by one tick.
    r = _reg()
    g = parse(
        "[seed]-->A\nseed.body: seed_zero\nA.body: passthru\nA.join: OR\nA-->B\nB.body: incr\nB--|cont_lt3|-->A",
        registry=r,
    )
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=50)
    a_outputs = [f.output for f in rn.audit if f.node == "A"]
    # A: seed=0 at tick1, then B's prev: 1, 2 (B=3 doesn't loop back, guard false)
    assert a_outputs == [0, 1, 2]


def test_index_policy_pins_specific_fire():
    # A[k] reads A's k-th fire regardless of tick.
    r = _reg()
    # A fires multiple times via a self-feeding loop; C reads A[1] (the first).
    @r.body("track_first")
    def _track(v):
        return v.A.value  # resolved to A's 1st fire by policy

    g = parse(
        """
        [seed]-->A
        seed.body: seed_zero
        A.body: passthru
        A.join: OR
        A-->B
        B.body: incr
        B--|cont_lt3|-->A
        A-->C
        C.inputs: A[1]
        C.body: track_first
""",
        registry=r,
    )
    # C has producers A; OR-join default? It's AND with one producer -> fine.
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=50)
    # C fires each time A fires, always seeing A's 1st fire (value 0).
    c_outputs = [f.output for f in rn.audit if f.node == "C"]
    assert all(o == 0 for o in c_outputs)
    # A fired 3 times (values 0, 1, 2).
    a_outputs = [f.output for f in rn.audit if f.node == "A"]
    assert a_outputs == [0, 1, 2]


def test_index_out_of_range_yields_missing():
    r = _reg()
    g = parse(
        "[seed]-->A\nseed.body: seed_zero\nA-->C\nC.inputs: A[5]\nC.body: passthru",
        registry=r,
    )
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20)
    # A fired once (value 0); A[5] doesn't exist -> Missing -> passthru None.
    c_fires = [f for f in rn.audit if f.node == "C"]
    assert len(c_fires) == 1
    assert c_fires[0].output is None  # passthru found no non-Missing input


def test_no_suggestions_for_loop_graph():
    # The loop graph has no XOR-splitter, so checker should be clean.
    r = _reg()
    g = parse(
        "[seed]-->A\nseed.body: seed_zero\nA.body: passthru\nA.join: OR\nA-->B\nB.body: incr\nB--|cont_lt3|-->A",
        registry=r,
    )
    assert check(g) == []
