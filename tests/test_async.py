"""Tests for AsyncRunner: async bodies, concurrent firing, hooks, persistence,
failure propagation, node_state -- mirroring the sync Runner."""
from __future__ import annotations

import asyncio

import pytest

from tickflow import parse, Registry, Failure, JsonBackend, RunStatus
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


def _run(coro):
    return asyncio.run(coro)


def test_async_loop_terminates():
    r = _reg()
    rn = AsyncRunner(_loop_graph(r), r)
    _run(rn.run_until_idle(max_ticks=50))
    b_outputs = [f.output for f in rn.audit if f.node == "B"]
    assert b_outputs == [1, 2, 3]
    assert rn.is_idle()


def test_async_body_can_be_async_def():
    r = Registry()

    @r.body("async_val")
    async def _av(v):
        await asyncio.sleep(0)
        return 42

    g = parse("[A]-->B\nB.body: async_val\nA.body: async_val", registry=r)
    rn = AsyncRunner(g, r)
    _run(rn.run_until_idle(max_ticks=20))
    assert [f.output for f in rn.audit if f.node == "B"] == [42]


def test_async_guard_can_be_async_def():
    r = Registry()

    @r.body("counter")
    def _c(v):
        v.state["attempts"] = v.state.get("attempts", 0) + 1
        return v.state["attempts"]

    @r.guard("under_three")
    async def _u3(v):
        await asyncio.sleep(0)
        return v.state.get("attempts", 0) < 3

    g = parse("[A]-->B\nB.body: counter\nB--|under_three|-->B\nB.join: OR", registry=r)
    rn = AsyncRunner(g, r)
    _run(rn.run_until_idle(max_ticks=20))
    assert [f.output for f in rn.audit if f.node == "B"] == [1, 2, 3]


def test_async_concurrent_firing():
    # Two independent async bodies that sleep; they should run concurrently
    # (total time ~ max, not sum). We assert both fire in the same tick.
    r = Registry()
    order = []

    @r.body("slow")
    async def _slow(v):
        await asyncio.sleep(0.05)
        order.append(v.node)
        return "done"

    g = parse("[A]-->C\n[B]-->C\nA.body: slow\nB.body: slow\nC.body: slow", registry=r)
    rn = AsyncRunner(g, r)
    _run(rn.run_until_idle(max_ticks=20))
    # A and B both fire at tick 0 concurrently.
    tick0 = [f.node for f in rn.audit if f.tick == 0]
    assert set(tick0) == {"A", "B"}


def test_async_failure_infra_aborts():
    r = Registry()
    r.body("ok", lambda v: "ok")

    @r.body("fail_infra")
    async def _fi(v):
        return Failure("net", type="infrastructure")

    g = parse("[A]-->B\nA.body: fail_infra\nB.body: ok\nA.body: fail_infra", registry=r)
    rn = AsyncRunner(g, r)
    _run(rn.run_until_idle(max_ticks=20))
    assert rn.status == RunStatus.ABORTED
    assert not any(f.node == "B" for f in rn.audit)


def test_async_hooks_called():
    r = _reg()
    rn = AsyncRunner(_loop_graph(r), r)
    fired = []
    ticks = []

    async def _on_fire(f):
        fired.append(f.node)

    async def _on_tick_end(t, fs):
        ticks.append(t)

    rn.on_fire(_on_fire)
    rn.on_tick_end(_on_tick_end)
    _run(rn.run_until_idle(max_ticks=20))
    assert "B" in fired
    assert len(ticks) > 0


def test_async_persistence(tmp_path):
    r = _reg()
    be = JsonBackend(tmp_path)
    rn = AsyncRunner(_loop_graph(r), r, backend=be, session_id="s1")
    _run(rn.run_until_idle(max_ticks=20))
    assert be.latest_tick("s1") == rn.tick_count
    assert len(be.list_firings("s1")) == len(rn.audit)


def test_async_checkpoint_rollback():
    r = _reg()
    from tickflow.persistence import NullBackend
    be = NullBackend()
    rn = AsyncRunner(_loop_graph(r), r, backend=be, session_id="s1")
    _run(rn.run_until_idle(max_ticks=20, pause_at={3}))
    rn.checkpoint("cp3")
    _run(rn.run_until_idle(max_ticks=20))
    final = [(f.tick, f.node, f.output) for f in rn.audit]
    rn.rollback_to("cp3")
    assert rn.tick_count == 3
    _run(rn.run_until_idle(max_ticks=20))
    assert [(f.tick, f.node, f.output) for f in rn.audit] == final


def test_async_snapshot_restore_matches_sync():
    # AsyncRunner should produce identical results to Runner for a pure graph.
    from tickflow import Runner
    r1 = _reg()
    rn_sync = Runner(_loop_graph(r1), r1)
    rn_sync.run_until_idle(max_ticks=50)
    r2 = _reg()
    rn_async = AsyncRunner(_loop_graph(r2), r2)
    _run(rn_async.run_until_idle(max_ticks=50))
    assert [f.output for f in rn_sync.audit] == [f.output for f in rn_async.audit]
