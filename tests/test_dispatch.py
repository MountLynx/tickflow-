"""Engine dispatch: value-mode / view-mode bodies, bind resolution, Missing."""
import pytest

from tickflow import Registry, Runner, parse
from tickflow.views import Missing


def test_value_mode_positional():
    r = Registry()
    r.body("seed", lambda: "s")
    r.body("incr", lambda a: a + "!")
    g = parse("[S]-->B\nS.body: seed\nB.body: incr", registry=r)
    run = Runner(g, r)
    run.run_until_idle(max_ticks=5)
    assert run.run_state.last_output("B") == "s!"


def test_value_mode_named_bind_kwargs():
    r = Registry()
    r.body("a", lambda: 1)
    r.body("b", lambda: 2)

    def combine(a, b):
        return a + b

    r.body("sum", combine)
    g = parse(
        "[A]-->C\n[B]-->C\nA.body: a\nB.body: b\n"
        "C.bind: {a: A, b: B}\nC.body: sum", registry=r
    )
    run = Runner(g, r)
    run.run_until_idle(max_ticks=5)
    assert run.run_state.last_output("C") == 3


def test_value_mode_state_tail_param():
    # `state` must be keyword-only: positional params are bind values, so a
    # body wanting state + values writes `def f(value, *, state)`.
    r = Registry()

    def count(_prev, *, state):
        n = state.get("n", 0) + 1
        state["n"] = n
        return n

    r.body("count", count)
    # A.bind: [S] pins one entry (self-loop A is a producer too, but the body
    # does not consume it); without the explicit bind E1 would demand arity 2.
    g = parse("[S]-->A\nA.body: count\nA-->A\nA.join: OR\nA.bind: [S]", registry=r)
    run = Runner(g, r)
    for _ in range(4):  # t0 seed; t1..t3 count fires 3 times
        run.tick()
    assert run.run_state.last_output("A") == 3


def test_view_mode_auto_detected_and_legacy_access_warns():
    r = Registry()
    r.body("seed", lambda: "s")

    def sink(v):
        with pytest.warns(DeprecationWarning):
            val = v["S"].value
        return val

    r.body("sink", sink)
    g = parse("[S]-->B\nS.body: seed\nB.body: sink", registry=r)
    Runner(g, r).run_until_idle(max_ticks=5)


def test_identity_body_echoes_first_bind_value():
    r = Registry()
    r.body("seed", lambda: "s")
    g = parse("[S]-->B\nS.body: seed", registry=r)  # B has no body -> identity
    run = Runner(g, r)
    run.run_until_idle(max_ticks=5)
    assert run.run_state.last_output("B") == "s"


def test_index_policy_flows_through_bind():
    r = Registry()
    r.body("sseed", lambda: "x")       # S: start, no producers -> arity 0 ok
    r.body("aseed", lambda s: "x")     # A: one producer -> arity must be 1
    seen = {}

    def two_b(a, b):
        seen["pair"] = (a, b)
        return "done"

    r.body("two_b", two_b)
    g = parse(
        "[S]-->A\nA-->C\nS-->C\nS.body: sseed\nA.body: aseed\n"
        "C.inputs: A, S[1]\nC.bind: [A, S]\nC.body: two_b",
        registry=r,
    )
    run = Runner(g, r)
    run.run_until_idle(max_ticks=5)
    # S[1] pins S's first fire; index policies flow through positional binds.
    assert seen["pair"] == ("x", "x")


def test_missing_survives_bind_resolution_end_to_end():
    r = Registry()
    seen = {}

    def seed_body():
        return "S"

    def loop_body(v):
        seen.setdefault("args", v.args)
        seen.setdefault("named", dict(v.named))
        seen.setdefault("input", v.input())
        return "A-out"

    def sink_body(x):
        return x

    r.body("seed_body", seed_body)
    r.body("loop_body", loop_body)
    r.body("sink_body", sink_body)
    g = parse(
        "[S]-->A\nA-->sink\nsink-->A\nA.join: OR\nS.body: seed_body\n"
        "A.bind: {prev: sink, seed: S}\nA.body: loop_body\nsink.body: sink_body",
        registry=r,
    )
    Runner(g, r).run_until_idle(max_ticks=6)
    # At A's first fire, back-edge producer `sink` has not fired yet: the
    # bound element MUST be Missing (not None) so hosts can fall back.
    assert seen["named"]["prev"] is Missing
    assert seen["named"]["seed"] == "S"
    assert seen["args"] == (Missing, "S")
    assert seen["input"] == (Missing, "S")


def test_field_overlays_stay_out_of_audit_and_view_collision_reads_pure():
    r = Registry()
    r.body("a", lambda: "A-out")
    r.body("b", lambda: "B-out")

    def cap(**kw):
        return "ok"

    r.body("cap", cap)
    g = parse(
        "[A]-->C\n[B]-->C\nA.body: a\nB.body: b\n"
        "C.bind: {B: A, a: B}\nC.body: cap",
        registry=r,
    )
    run = Runner(g, r)
    run.run_until_idle(max_ticks=5)
    rec = [f for f in run.audit_log() if f.node == "C"][-1]
    # Audit records real producers only — no synthetic field keys, no clobber.
    assert rec.inputs == {"A": "A-out", "B": "B-out"}


def test_named_state_collision_is_loud_at_dispatch():
    from tickflow.engine import prepare_body_call

    r = Registry()

    def f(*, state): ...

    r.body("f", f)
    with pytest.raises(TypeError, match="collides"):
        prepare_body_call(r, "f", (("state", "S"),), (1,), {}, {}, "A")
