"""Tests for on_fire / on_tick_end hooks."""
from __future__ import annotations

import pytest

from tickflow import parse, Runner, Registry
from tickflow.views import Missing


def _reg():
    r = Registry()
    r.body("ok", lambda v: "ok")
    r.body("passthru", lambda v: next((val for _n, val in v.items() if val is not Missing), None))
    r.guard("loop_true", lambda v: True)
    return r


def test_on_fire_called_per_firing():
    r = _reg()
    g = parse("[A]-->B\n[A]-->C\nB.body: ok\nC.body: ok\nA.body: ok", registry=r)
    rn = Runner(g, r)
    seen = []
    rn.on_fire(lambda f: seen.append((f.tick, f.node)))
    rn.run_until_idle(max_ticks=20)
    # A fires at tick 0; B and C at tick 1.
    assert ("A" in [n for _, n in seen])
    assert ("B" in [n for _, n in seen])
    assert ("C" in [n for _, n in seen])


def test_on_fire_hook_receives_full_firing():
    r = _reg()
    g = parse("[A]-->B\nB.body: ok\nA.body: ok", registry=r)
    rn = Runner(g, r)
    captured = []
    rn.on_fire(lambda f: captured.append(f))
    rn.tick()
    assert captured[0].node == "A"
    assert captured[0].output == "ok"
    assert captured[0].status == "ok"


def test_on_tick_end_called_with_tick_and_firings():
    r = _reg()
    g = parse("[A]-->B\nB.body: ok\nA.body: ok", registry=r)
    rn = Runner(g, r)
    ticks = []
    rn.on_tick_end(lambda t, fs: ticks.append((t, len(fs))))
    rn.run_until_idle(max_ticks=20)
    # tick 0: A (1 firing); tick 1: B (1 firing); tick 2: idle (0).
    assert (0, 1) in ticks
    assert (1, 1) in ticks
    assert (2, 0) in ticks


def test_hook_exception_swallowed():
    r = _reg()
    g = parse("[A]-->B\nB.body: ok\nA.body: ok", registry=r)
    rn = Runner(g, r)

    def bad(f):
        raise ValueError("boom")
    rn.on_fire(bad)
    # Should not raise.
    rn.run_until_idle(max_ticks=20)
    # Run still completed.
    assert rn.is_terminal()


def test_multiple_hooks_all_called():
    r = _reg()
    g = parse("[A]-->B\nB.body: ok\nA.body: ok", registry=r)
    rn = Runner(g, r)
    a, b = [], []
    rn.on_fire(lambda f: a.append(f.node))
    rn.on_fire(lambda f: b.append(f.node))
    rn.run_until_idle(max_ticks=20)
    assert a == ["A", "B"]
    assert b == ["A", "B"]


def test_on_tick_start_called_before_fire():
    r = _reg()
    g = parse("[A]-->B\nB.body: ok\nA.body: ok", registry=r)
    rn = Runner(g, r)
    order = []

    def on_start(tick, fireable):
        order.append(("start", tick, list(fireable)))

    def on_fire(f):
        order.append(("fire", f.tick, f.node))

    def on_end(tick, firings):
        order.append(("end", tick, len(firings)))

    rn.on_tick_start(on_start)
    rn.on_fire(on_fire)
    rn.on_tick_end(on_end)
    rn.run_until_idle(max_ticks=20)
    # tick 0: start(0, [A]) -> fire A -> end(0, 1)
    assert order[0] == ("start", 0, ["A"])
    assert order[1] == ("fire", 0, "A")
    assert order[2] == ("end", 0, 1)


def test_on_tick_start_fireable_matches_firings():
    r = _reg()
    g = parse("[A]-->C\n[B]-->C\nC.body: ok\nA.body: ok\nB.body: ok", registry=r)
    rn = Runner(g, r)
    captured = []
    rn.on_tick_start(lambda tick, fireable: captured.append((tick, set(fireable))))
    rn.run_until_idle(max_ticks=20)
    # tick 0: A and B both fireable
    assert captured[0] == (0, {"A", "B"})
    # tick 1: C fireable
    assert captured[1] == (1, {"C"})


def test_on_tick_start_exception_swallowed():
    r = _reg()
    g = parse("[A]-->B\nB.body: ok\nA.body: ok", registry=r)
    rn = Runner(g, r)

    def bad(tick, fireable):
        raise ValueError("boom")
    rn.on_tick_start(bad)
    rn.run_until_idle(max_ticks=20)
    assert rn.is_terminal()


def test_on_tick_start_async():
    import asyncio
    from tickflow.async_runner import AsyncRunner

    r = Registry()
    r.body("ok", lambda v: "ok")
    g = parse("[A]-->B\nB.body: ok\nA.body: ok", registry=r)
    rn = AsyncRunner(g, r)
    starts = []

    async def on_start(tick, fireable):
        starts.append((tick, list(fireable)))

    rn.on_tick_start(on_start)
    asyncio.run(rn.run_until_idle(max_ticks=20))
    assert starts[0] == (0, ["A"])
