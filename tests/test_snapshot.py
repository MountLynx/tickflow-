"""Tests for snapshot/restore, pause, and audit log."""
from __future__ import annotations

import json

import pytest

from tickflow import parse, Runner, Registry, check
from tickflow.views import Missing


def _loop_reg() -> Registry:
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

    @r.guard("cont_lt3")
    def _c(v):
        return v.B.value < 3

    return r


def _loop_graph(r: Registry):
    return parse(
        "[seed]-->A\nseed.body: seed_zero\nA.body: passthru\nA.join: OR\nA-->B\nB.body: incr\nB--|cont_lt3|-->A",
        registry=r,
    )


def test_snapshot_is_json_serializable():
    r = _loop_reg()
    rn = Runner(_loop_graph(r), r)
    rn.run_until_idle(max_ticks=20, pause_at={3})
    snap = rn.snapshot()
    # Must round-trip through json without error.
    s = json.dumps(snap)
    snap2 = json.loads(s)
    assert snap2["tick"] == snap["tick"]


def test_restore_replays_identically():
    r = _loop_reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20, pause_at={3})
    snap = rn.snapshot()
    # Finish the run.
    rn.run_until_idle(max_ticks=20)
    final = [(f.tick, f.node, f.output) for f in rn.audit]
    # Restore to the snapshot and replay.
    rn.restore(snap)
    assert rn.tick_count == snap["tick"]
    # Audit truncated to ticks < snap["tick"].
    assert all(f.tick < snap["tick"] for f in rn.audit)
    rn.run_until_idle(max_ticks=20)
    replayed = [(f.tick, f.node, f.output) for f in rn.audit]
    assert replayed == final


def test_restore_truncates_history():
    r = _loop_reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20, pause_at={3})
    snap = rn.snapshot()
    rn.run_until_idle(max_ticks=20)
    # After full run, history has entries beyond snap tick.
    assert any(t >= snap["tick"] for lst in rn.history.data.values() for (t, _) in lst)
    rn.restore(snap)
    # After restore, no history entry at or beyond snap tick.
    for lst in rn.history.data.values():
        for (t, _) in lst:
            assert t < snap["tick"]


def test_pause_at_stops_before_firing():
    r = _loop_reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20, pause_at={4})
    # Stopped at tick boundary 4: ticks 0..3 fired, tick 4 not yet.
    assert rn.tick_count == 4
    assert all(f.tick < 4 for f in rn.audit)
    # Not idle (more work pending).
    assert not rn.is_idle()


def test_pause_at_resume_continues():
    r = _loop_reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20, pause_at={4})
    rn.run_until_idle(max_ticks=20)
    # Now complete.
    assert rn.is_idle()
    b_outputs = [f.output for f in rn.audit if f.node == "B"]
    assert b_outputs == [1, 2, 3]


def test_to_json_from_json_roundtrip():
    r = _loop_reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20, pause_at={3})
    rn.run_until_idle(max_ticks=20)
    s = rn.to_json()
    rn2 = Runner.from_json(s, g, r)
    assert [(f.tick, f.node, f.output) for f in rn2.audit] == [
        (f.tick, f.node, f.output) for f in rn.audit
    ]
    assert rn2.tick_count == rn.tick_count
    assert rn2.is_idle() == rn.is_idle()


def test_audit_log_contains_edges_fired():
    r = _loop_reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    rn.tick()  # seed fires
    seed_firing = [f for f in rn.audit if f.node == "seed"][0]
    # seed's only out-edge is seed-->A (plain), so edges_fired has (A, None, True).
    assert ("A", None, True) in seed_firing.edges_fired


def test_audit_log_serializable():
    r = _loop_reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20)
    s = rn.audit_json()
    parsed = json.loads(s)
    assert isinstance(parsed, list)
    assert all("tick" in e and "node" in e and "output" in e for e in parsed)


def test_deepcopy_snapshot_for_branching():
    import copy

    r = _loop_reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20, pause_at={3})
    snap_a = rn.snapshot()
    snap_b = copy.deepcopy(snap_a)
    # Continue one branch to completion.
    rn.run_until_idle(max_ticks=20)
    # The other snapshot is untouched (still at tick 3).
    assert snap_b["tick"] == 3
    rn2 = Runner(g, r)
    rn2.restore(snap_b)
    assert rn2.tick_count == 3


def test_snapshot_contains_fireable():
    r = _loop_reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20, pause_at={3})
    snap = rn.snapshot()
    assert "fireable" in snap
    assert set(snap["fireable"]) == set(rn.fireable())


def test_snapshot_fireable_after_restore():
    r = _loop_reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20, pause_at={3})
    snap = rn.snapshot()
    rn.run_until_idle(max_ticks=20)
    rn.restore(snap)
    # After restore, fireable() is recomputed from the restored marking and
    # matches what the snapshot recorded.
    assert set(rn.fireable()) == set(snap["fireable"])


def test_snapshot_fireable_empty_at_terminal():
    r = _loop_reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=50)
    snap = rn.snapshot()
    assert snap["fireable"] == []
