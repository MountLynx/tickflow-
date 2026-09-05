"""Guard adjudication semantics: GuardView contents and value-mode guards."""
import pytest

from tickflow import Registry, Runner, parse


def test_guard_output_is_current_tick_output():
    r = Registry()
    seen = []

    def bump(v):
        n = v.state.get("n", 0) + 1
        v.state["n"] = n
        return n

    r.body("bump", bump)
    r.body("echo", lambda x: x)

    def watch(output):
        seen.append(output)
        return output < 2

    r.guard("watch", watch)
    g = parse(
        "[S]-->B\nB--|watch|-->C\nC-->B\nB.join: OR\nB.body: bump\nC.body: echo",
        registry=r,
    )
    Runner(g, r).run_until_idle(max_ticks=10)
    assert seen == [1, 2]  # adjudicated output, this tick — not latest_before


def test_guard_view_sees_src_declared_inputs():
    r = Registry()
    seen = {}
    # arity 1: M's auto-bind has one entry (S); zero-arity would fail E1.
    r.body("mbody", lambda s: "M-out")

    def rbody(d):
        return {"got": d}

    r.body("rbody", rbody)

    def watch(v):
        seen["output"] = v.output
        seen["named"] = dict(v.named)
        with pytest.warns(DeprecationWarning):
            seen["legacy_src"] = v["R"].value
            seen["legacy_prod"] = v["M"].value
        with pytest.raises(KeyError):
            v["NotDeclared"]  # P4: undeclared nodes are unreachable
        with pytest.raises(KeyError):
            v["S"]  # P4: real graph nodes outside the bind declaration too
        return False

    r.guard("watch", watch)
    g = parse(
        "[S]-->M\nM-->R\nR--|watch|-->X\nR.bind: {d: M}\nR.body: rbody\nM.body: mbody",
        registry=r,
    )
    run = Runner(g, r)
    run.run_until_idle(max_ticks=5)
    assert seen["output"] == {"got": "M-out"}
    assert seen["named"] == {"d": "M-out"}
    assert seen["legacy_src"] == {"got": "M-out"}
    assert seen["legacy_prod"] == "M-out"


def test_guard_view_mode_gets_guard_view_state_readonly():
    r = Registry()
    r.body("b1", lambda v: "x")

    def watch(v):
        assert v.state.get("n", 0) == 0
        try:
            v.state["n"] = 1
            raised = False
        except TypeError:
            raised = True
        assert raised, "guard state must be read-only"
        return False

    r.guard("watch", watch)
    g = parse("[S]-->B\nB--|watch|-->C\nB.body: b1", registry=r)
    Runner(g, r).run_until_idle(max_ticks=3)


def test_guard_value_mode_receives_output_only():
    r = Registry()
    # arity 1: B's auto-bind has one entry (S); a zero-arity body would fail E1.
    r.body("b1", lambda s: 5)
    seen = []

    def watch(output):
        seen.append(output)
        return False

    r.guard("watch", watch)
    g = parse("[S]-->B\nB--|watch|-->C\nB.body: b1", registry=r)
    Runner(g, r).run_until_idle(max_ticks=3)
    assert seen == [5]
