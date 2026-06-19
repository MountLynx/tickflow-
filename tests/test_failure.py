"""Tests for Failure semantics: llm vs infrastructure propagation."""
from __future__ import annotations

import pytest

from tickflow import parse, Runner, Registry, Failure
from tickflow.views import Missing


def _reg():
    r = Registry()

    @r.body("ok")
    def _ok(v):
        return "ok"

    @r.body("fail_llm")
    def _fllm(v):
        return Failure("bad output", type="llm")

    @r.body("fail_infra")
    def _finfra(v):
        return Failure("network down", type="infrastructure")

    @r.body("passthru")
    def _p(v):
        for _n, val in v.items():
            if val is not Missing:
                return val
        return None

    return r


def test_llm_failure_writes_false_downstream():
    # A fails (llm) -> B's slot (B,A) must be False -> B must NOT fire.
    r = _reg()
    g = parse("[A]-->B\nA.body: fail_llm\nB.body: passthru", registry=r)
    rn = Runner(g, r)
    rn.tick()  # A fires, fails (llm)
    assert rn.audit[-1].node == "A"
    assert rn.audit[-1].status == "failed"
    assert rn.audit[-1].error == "bad output"
    # B's slot (B,A) is False; B never fires; next tick is idle.
    rn.tick()
    assert rn.is_idle()
    assert not any(f.node == "B" for f in rn.audit)


def test_llm_failure_does_not_abort_run():
    r = _reg()
    # A fails llm, but C is independent (separate start) and should still run.
    g = parse("[A]-->B\nA.body: fail_llm\nB.body: passthru\n[C]-->D\nD.body: ok\nC.body: ok", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20)
    nodes = {f.node for f in rn.audit}
    # A failed, B never ran; C and D ran fine.
    assert "A" in nodes and "C" in nodes and "D" in nodes
    assert "B" not in nodes
    assert rn.status.value != "aborted"


def test_infrastructure_failure_aborts_run():
    r = _reg()
    g = parse("[A]-->B\nA.body: fail_infra\nB.body: ok\n[C]-->D\nD.body: ok\nC.body: ok", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20)
    # A aborted; further ticks no-op.
    assert rn.status.value == "aborted"
    a_fire = [f for f in rn.audit if f.node == "A"][0]
    assert a_fire.status == "aborted"
    assert a_fire.error == "network down"


def test_aborted_runner_stops_ticking():
    r = _reg()
    g = parse("[A]-->B\nA.body: fail_infra\nB.body: ok\nA.body: fail_infra", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20)
    assert rn.status.value == "aborted"
    # Subsequent ticks are no-ops.
    before = len(rn.audit)
    rn.tick()
    assert len(rn.audit) == before


def test_failure_still_consumes_slots_and_writes_history():
    r = _reg()
    g = parse("[A]-->B\nA.body: fail_llm\nB.body: ok\nA.body: fail_llm", registry=r)
    rn = Runner(g, r)
    rn.tick()
    # A's input slots consumed (it was a start, disarmed).
    assert "A" not in rn.marking.armed_starts
    # A's output is in history (the Failure object, serialized).
    assert rn.history.data.get("A")


def test_failure_in_firing_to_json():
    r = _reg()
    g = parse("[A]-->B\nA.body: fail_llm\nB.body: ok\nA.body: fail_llm", registry=r)
    rn = Runner(g, r)
    rn.tick()
    j = rn.audit[-1].to_json()
    assert j["status"] == "failed"
    assert j["error"] == "bad output"
    # Round-trip.
    from tickflow.engine import Firing
    f2 = Firing.from_json(j)
    assert f2.status == "failed"
    assert f2.error == "bad output"
