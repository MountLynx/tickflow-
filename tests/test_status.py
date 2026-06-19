"""Tests for RunStatus: cancel, abort, terminal, is_terminal."""
from __future__ import annotations

import pytest

from tickflow import parse, Runner, Registry, RunStatus, Failure


def _reg():
    r = Registry()
    r.body("ok", lambda v: "ok")
    r.body("fail_infra", lambda v: Failure("x", type="infrastructure"))
    r.guard("loop_true", lambda v: True)
    return r


def test_initial_status_is_idle():
    r = _reg()
    g = parse("[A]-->B\nB.body: ok\nA.body: ok", registry=r)
    rn = Runner(g, r)
    assert rn.status == RunStatus.IDLE


def test_cancel_marks_cancelled_and_stops():
    r = _reg()
    g = parse("[A]-->B\nB.body: ok\nA.body: ok", registry=r)
    rn = Runner(g, r)
    rn.cancel("user")
    assert rn.status == RunStatus.CANCELLED
    assert rn.cancel_reason == "user"
    # tick is no-op.
    assert rn.tick() == []


def test_cancel_mid_run_then_tick_noop():
    r = _reg()
    g = parse("[A]-->B\nB.body: ok\nA.body: ok", registry=r)
    rn = Runner(g, r)
    rn.tick()  # A fires -> RUNNING
    rn.cancel("mid")
    assert rn.status == RunStatus.CANCELLED
    before = len(rn.audit)
    rn.tick()
    assert len(rn.audit) == before  # B never fires


def test_infra_failure_sets_aborted():
    r = _reg()
    g = parse("[A]-->B\nA.body: fail_infra\nB.body: ok\nA.body: fail_infra", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=10)
    assert rn.status == RunStatus.ABORTED
    assert rn.is_terminal()


def test_is_terminal_after_completion():
    r = _reg()
    g = parse("[A]-->B\nB.body: ok\nA.body: ok", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=10)
    assert rn.is_terminal()
    assert rn.is_idle()


def test_is_terminal_false_mid_loop():
    r = _reg()
    g = parse("[A]-->B\nB.body: ok\nB--|loop_true|-->A\nA.join: OR\nA.body: ok", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=20, pause_at={2})
    assert not rn.is_terminal()
    assert not rn.is_idle()


def test_status_in_snapshot_and_restore():
    r = _reg()
    g = parse("[A]-->B\nA.body: fail_infra\nB.body: ok\nA.body: fail_infra", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=10)
    assert rn.status == RunStatus.ABORTED
    snap = rn.snapshot()
    assert snap["status"] == "aborted"
    # Restore: terminal status is reset to IDLE so resume is possible.
    rn.restore(snap)
    assert rn.status == RunStatus.IDLE


def test_reset_clears_terminal_status():
    r = _reg()
    g = parse("[A]-->B\nA.body: fail_infra\nB.body: ok\nA.body: fail_infra", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=10)
    rn.reset()
    assert rn.status == RunStatus.IDLE
