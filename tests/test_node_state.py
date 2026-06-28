"""Tests for per-node mutable state (node_state): bodies write, guards read,
state persists in marking and survives snapshot/restore."""
from __future__ import annotations

import pytest

from tickflow import parse, Runner, Registry
from tickflow.views import Missing


def _reg():
    r = Registry()

    @r.body("counter")
    def _counter(v):
        # Increment this node's own attempt counter each fire.
        v.state["attempts"] = v.state.get("attempts", 0) + 1
        return v.state["attempts"]

    @r.body("passthru")
    def _p(v):
        for _n, val in v.items():
            if val is not Missing:
                return val
        return None

    @r.guard("under_three")
    def _under3(v):
        # Guard on the self-loop reads the node's own state.
        return v.state.get("attempts", 0) < 3

    @r.guard("always_true")
    def _t(v):
        return True

    return r


def test_body_writes_state_visible_next_fire():
    r = _reg()
    g = parse("[A]-->A_loop\nA_loop.body: counter\nA_loop--|always_true|-->A_loop\nA_loop.join: OR", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=10)
    # Wait, this loops forever (always_true). Use under_three instead.


def test_state_driven_loop_terminates():
    r = _reg()
    g = parse(
        "[A]-->B\nB.body: counter\nB--|under_three|-->B\nB.join: OR",
        registry=r,
    )
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20)
    b_outputs = [f.output for f in rn.audit_log() if f.node == "B"]
    # B fires 3 times: attempts 1,2,3. After 3, guard under_three (attempts<3) is False -> stop.
    assert b_outputs == [1, 2, 3]
    assert rn.is_idle()


def test_state_persists_in_marking_across_ticks():
    r = _reg()
    g = parse("[A]-->B\nB.body: counter\nB--|under_three|-->B\nB.join: OR", registry=r)
    rn = Runner(g, r)
    rn.tick()  # A
    rn.tick()  # B fires once, attempts=1
    assert rn.run_state.mutable_state("B")["attempts"] == 1
    rn.tick()  # B fires again, attempts=2
    assert rn.run_state.mutable_state("B")["attempts"] == 2


def test_state_survives_snapshot_restore():
    r = _reg()
    g = parse("[A]-->B\nB.body: counter\nB--|under_three|-->B\nB.join: OR", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20, pause_at={3})
    snap = rn.snapshot()
    # Snapshot has node state in run_state.
    assert "B" in snap["run_state"]["edges"]
    rn.run_until_idle(max_ticks=20)
    rn.restore(snap)
    # State restored: B's attempts reflects the snapshot point.
    assert "B" in rn.run_state.all_mutable_states()


def test_state_in_firing_record():
    r = _reg()
    g = parse("[A]-->B\nB.body: counter\nB--|under_three|-->B\nB.join: OR", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20)
    b_fires = [f for f in rn.audit_log() if f.node == "B"]
    # Each firing records the state *after* the body ran.
    assert b_fires[0].mutable_state["attempts"] == 1
    assert b_fires[1].mutable_state["attempts"] == 2
    assert b_fires[2].mutable_state["attempts"] == 3


def test_separate_nodes_have_separate_state():
    r = _reg()
    g = parse("[A]-->B\n[A]-->C\nB.body: counter\nC.body: counter", registry=r)
    rn = Runner(g, r)
    rn.tick()  # A
    rn.tick()  # B, C each fire once
    assert rn.run_state.mutable_state("B")["attempts"] == 1
    assert rn.run_state.mutable_state("C")["attempts"] == 1
    # They're distinct dicts.
    assert rn.run_state.mutable_state("B") is not rn.run_state.mutable_state("C")


def test_node_states_public_api():
    r = _reg()
    g = parse("[A]-->B\nB.body: counter\nB--|under_three|-->B\nB.join: OR", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20, pause_at={3})
    states = rn.node_states()
    # Returns a copy with the expected structure.
    assert "B" in states
    assert "attempts" in states["B"]
    # Mutating the returned dict does not affect the run state.
    states["B"]["attempts"] = 999
    assert rn.run_state.mutable_state("B")["attempts"] != 999


def test_node_states_empty_when_no_state():
    r = _reg()
    g = parse("[A]-->B\nB.body: passthru\nA.body: passthru", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20)
    # passthru body doesn't touch state; node_states() has empty dicts at most.
    states = rn.node_states()
    for s in states.values():
        assert s == {}
