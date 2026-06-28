"""Tests for Runner.fireable() and AsyncRunner.fireable(): the fireable set
matches what the next tick actually fires."""
from __future__ import annotations

import asyncio

import pytest

from tickflow import parse, Runner, Registry
from tickflow.async_runner import AsyncRunner
from tickflow.views import Missing


def _reg():
    r = Registry()
    r.body("seed_zero", lambda v: 0)

    @r.body("passthru")
    def _p(v):
        for _n, val in v.items():
            if val is not Missing:
                return val
        return None

    @r.body("incr")
    def _incr(v):
        return v.A.value + 1

    r.guard("cont_lt3", lambda v: v.B.value < 3)
    return r


def _loop_graph(r):
    return parse(
        "[seed]-->A\nseed.body: seed_zero\nA.body: passthru\nA.join: OR\nA-->B\nB.body: incr\nB--|cont_lt3|-->A",
        registry=r,
    )


def test_fireable_matches_tick_initial():
    r = _reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    # Before any tick: seed is armed start -> fireable.
    assert rn.fireable() == ["seed"]
    fs = rn.tick()
    assert {f.node for f in fs} == set(rn.fireable()) or {f.node for f in fs} == {"seed"}


def test_fireable_matches_next_tick():
    r = _reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    rn.tick()  # seed
    # Now fireable should predict tick 1's fires.
    predicted = set(rn.fireable())
    fs = rn.tick()
    actual = {f.node for f in fs}
    assert predicted == actual


def test_fireable_empty_at_terminal():
    r = _reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=50)
    assert rn.is_terminal()
    assert rn.fireable() == []


def test_fireable_with_loop_mid_run():
    r = _reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=50, pause_at={3})
    # Mid-run: fireable should be non-empty (loop still has work).
    assert len(rn.fireable()) > 0
    # And it should match what the next tick fires.
    predicted = set(rn.fireable())
    fs = rn.tick()
    assert {f.node for f in fs} == predicted


def test_fireable_read_only():
    r = _reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    f1 = rn.fireable()
    f2 = rn.fireable()
    # Calling fireable doesn't advance state.
    assert f1 == f2
    assert rn.tick_count == 0


def test_fireable_async_runner_matches_sync():
    r1 = _reg()
    rn_sync = Runner(_loop_graph(r1), r1)
    rn_sync.run_until_idle(max_ticks=50, pause_at={3})
    r2 = _reg()
    rn_async = AsyncRunner(_loop_graph(r2), r2)
    asyncio.run(rn_async.run_until_idle(max_ticks=50, pause_at={3}))
    assert set(rn_sync.fireable()) == set(rn_async.fireable())


def test_fireable_after_cancel_reflects_marking():
    # fireable() reflects the marking, not the run status. After cancel, the
    # marking still has pending slots, so fireable() is non-empty -- but
    # tick() is a no-op because status is terminal.
    r = _reg()
    g = _loop_graph(r)
    rn = Runner(g, r)
    rn.tick()  # seed fires, A's slot becomes True
    rn.cancel("test")
    assert rn.status.value == "cancelled"
    # fireable() is a pure marking query; cancel doesn't clear slots.
    assert "A" in rn.fireable()
    # But tick() is a no-op (terminal), so the run is halted.
    before = len(rn.audit_log())
    rn.tick()
    assert len(rn.audit_log()) == before
