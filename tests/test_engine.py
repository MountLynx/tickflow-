"""Tests for the engine: AND/OR joins, slot reset, step semantics."""
from __future__ import annotations

import pytest

from tickflow import parse, Runner, Registry, check, promote
from tickflow.views import Missing


def _reg():
    r = Registry()

    @r.body("seed_zero")
    def _seed(v):
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

    @r.body("add")
    def _add(v):
        return v.A.value + v.B.value

    @r.guard("always_true")
    def _t(v):
        return True

    @r.guard("always_false")
    def _f(v):
        return False

    @r.guard("cont_lt3")
    def _c(v):
        return v.B.value < 3

    return r


def test_and_join_waits_for_all_upstream():
    # A-->C and B-->C; C is AND-join. C must not fire until both A and B fired.
    r = _reg()
    g = parse("[A]-->C\n[B]-->C\nC.body: passthru", registry=r)
    rn = Runner(g, r)
    # tick 0: A and B both fire (both armed starts, no producers).
    fs = rn.tick()
    assert {f.node for f in fs} == {"A", "B"}
    # tick 1: C fires (both its slots True from tick 0).
    fs = rn.tick()
    assert {f.node for f in fs} == {"C"}
    # tick 2: idle.
    fs = rn.tick()
    assert fs == []
    assert rn.is_idle()


def test_and_join_does_not_fire_with_one_upstream():
    # If only one upstream fires, AND-join must not fire.
    r = _reg()
    # B is gated behind A so it won't fire same tick as A.
    g = parse("[A]-->B\nB.body: passthru\nA-->C\nB-->C\nC.body: passthru", registry=r)
    rn = Runner(g, r)
    # tick 0: A fires (start).
    fs = rn.tick()
    assert {f.node for f in fs} == {"A"}
    # tick 1: B fires (A->B slot True); C has (C,A)=True but (C,B)=False -> no.
    fs = rn.tick()
    assert {f.node for f in fs} == {"B"}
    # tick 2: C fires now that (C,B) True too.
    fs = rn.tick()
    assert {f.node for f in fs} == {"C"}
    fs = rn.tick()
    assert fs == []
    assert rn.is_idle()


def test_slot_reset_after_fire():
    # After C fires, its input slots must reset to False so it doesn't loop.
    r = _reg()
    g = parse("[A]-->C\nC.body: passthru", registry=r)
    rn = Runner(g, r)
    rn.tick()  # A
    rn.tick()  # C
    # After C fired, (C,A) must be False.
    assert rn.marking.slots[("C", "A")] is False
    rn.tick()  # idle
    assert rn.is_idle()


def test_or_join_fires_with_one_upstream():
    r = _reg()
    g = parse("[A]-->C\nA-->B\nB.body: passthru\nB--|always_false|-->C\nC.body: passthru\nC.join: OR", registry=r)
    # C has producers A, B; join OR. B->C guarded always_false so won't fire.
    rn = Runner(g, r)
    rn.tick()  # A
    fs = rn.tick()
    # B fires (A->B True). C: (C,A)=True, (C,B)=False. OR -> fire.
    assert "C" in {f.node for f in fs}
    rn.tick()  # idle
    assert rn.is_idle()


def test_guard_failure_writes_false_not_stale_true():
    # A loop where guard eventually fails: stale True must not leak.
    r = _reg()
    g = parse(
        "[seed]-->A\nseed.body: seed_zero\nA.body: passthru\nA.join: OR\nA-->B\nB.body: incr\nB--|cont_lt3|-->A",
        registry=r,
    )
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20)
    # B should reach value 3 then stop (guard fails -> writes False -> A
    # doesn't re-fire -> loop terminates).
    b_outputs = [f.output for f in rn.audit_log() if f.node == "B"]
    assert b_outputs == [1, 2, 3]
    assert rn.is_idle()


def test_step_semantics_no_same_tick_visibility():
    # Two peers A, B fire same tick; both feed C. C cannot see their same-tick
    # outputs (latest_before(t) = tick < t). C fires next tick.
    r = _reg()
    g = parse("[A]-->C\n[B]-->C\nC.body: passthru", registry=r)
    rn = Runner(g, r)
    fs0 = rn.tick()  # A, B
    assert {f.node for f in fs0} == {"A", "B"}
    # C not yet fireable (its reads would be Missing -> but join is about slots
    # which ARE True at tick 1, so C fires at tick 1 with latest_before(1)=tick0
    # outputs of A,B).
    fs1 = rn.tick()
    assert {f.node for f in fs1} == {"C"}
    # C's inputs should be A and B's tick-0 outputs.
    cf = fs1[0]
    assert "A" in cf.inputs and "B" in cf.inputs


def test_start_fires_exactly_once():
    r = _reg()
    g = parse("[A]-->B\nB.body: passthru", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20)
    a_fires = [f for f in rn.audit_log() if f.node == "A"]
    assert len(a_fires) == 1


def test_max_ticks_terminates():
    r = _reg()
    # Infinite loop via always_true guard with no termination.
    g = parse("[A]-->B\nB.body: passthru\nB--|always_true|-->A\nA.join: OR", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=5)
    # Stopped by max_ticks, not idle.
    assert not rn.is_idle()
    assert rn.tick_count == 5
