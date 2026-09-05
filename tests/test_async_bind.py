"""Async mirror of the bind dispatch: value/view modes, guards, Missing."""
import asyncio

from tickflow import Registry, parse
from tickflow.async_runner import AsyncRunner
from tickflow.views import Missing


def _run(coro):
    return asyncio.run(coro)


def test_async_value_mode_positional_async_body():
    r = Registry()
    r.body("seed", lambda: "s")

    async def incr(a):
        return a + "!"

    r.body("incr", incr)
    g = parse("[S]-->B\nS.body: seed\nB.body: incr", registry=r)
    rn = AsyncRunner(g, r)
    _run(rn.run_until_idle(max_ticks=5))
    assert rn.run_state.last_output("B") == "s!"


def test_async_value_mode_named_and_state():
    r = Registry()
    r.body("s1", lambda: 1)

    async def add1(a, *, state):
        state["n"] = state.get("n", 0) + 1
        return a + state["n"]

    r.body("add1", add1)
    g = parse("[S]-->A\nS.body: s1\nA.bind: {a: S}\nA.body: add1", registry=r)
    rn = AsyncRunner(g, r)
    _run(rn.run_until_idle(max_ticks=5))
    assert rn.run_state.last_output("A") == 2


def test_async_view_mode_and_missing():
    r = Registry()
    seen = {}

    async def loop_body(v):
        seen["args"] = v.args
        return "out"

    async def sink_body(x):
        return x

    # stop-guard on the back edge: A must fire EXACTLY ONCE — on that first
    # fire sink has not fired yet, so prev is Missing (the plan's unguarded
    # cycle re-fired A and overwrote args with sink's real output).
    r.body("seed_body", lambda: "S")
    r.body("loop_body", loop_body)
    r.body("sink_body", sink_body)
    r.guard("stop", lambda output: False)
    g = parse(
        "[S]-->A\nA-->sink\nsink--|stop|-->A\nA.join: OR\nS.body: seed_body\n"
        "A.bind: {prev: sink, seed: S}\nA.body: loop_body\nsink.body: sink_body",
        registry=r,
    )
    rn = AsyncRunner(g, r)
    _run(rn.run_until_idle(max_ticks=6))
    assert seen["args"] == (Missing, "S")


def test_async_guard_both_modes():
    r = Registry()
    # arity 1: B/D's auto-bind has one entry each; zero-arity would fail E1.
    r.body("b1", lambda s: 5)
    value_seen, view_seen = [], []

    def watch_value(output):
        value_seen.append(output)
        return False

    async def watch_view(v):
        view_seen.append(v.output)
        return False

    r.guard("watch_value", watch_value)
    r.guard("watch_view", watch_view)
    g = parse(
        "[S]-->B\nB--|watch_value|-->C\nB.body: b1\n"
        "[T]-->D\nD--|watch_view|-->E\nD.body: b1",
        registry=r,
    )
    rn = AsyncRunner(g, r)
    _run(rn.run_until_idle(max_ticks=5))
    assert value_seen == [5]
    assert view_seen == [5]


def test_async_identity_body_bodyless_node():
    # New-to-async in Task 7: bodyless nodes used to KeyError under AsyncRunner.
    r = Registry()
    r.body("seed", lambda: "s")
    g = parse("[S]-->B\nS.body: seed", registry=r)  # B has no body -> identity
    rn = AsyncRunner(g, r)
    _run(rn.run_until_idle(max_ticks=5))
    assert rn.run_state.last_output("B") == "s"


def test_async_named_bind_missing_via_kwargs():
    # Missing fidelity through the value-mode NAMED (kwargs) path, async side.
    r = Registry()
    seen = {}

    async def loop_body(a, b):
        seen["pair"] = (a, b)
        return "out"

    async def sink_body(x):
        return x

    r.body("seed_body", lambda: "S")
    r.body("loop_body", loop_body)
    r.body("sink_body", sink_body)
    r.guard("stop", lambda v: False)
    g = parse(
        "[S]-->A\nA-->sink\nsink--|stop|-->A\nA.join: OR\n"
        "A.bind: {a: sink, b: S}\nS.body: seed_body\nA.body: loop_body\n"
        "sink.body: sink_body",
        registry=r,
    )
    rn = AsyncRunner(g, r)
    _run(rn.run_until_idle(max_ticks=6))
    # A's first fire: back-edge producer `sink` has not fired -> kwargs carry Missing.
    assert seen["pair"] == (Missing, "S")
