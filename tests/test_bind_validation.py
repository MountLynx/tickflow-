"""Build-time validation: bind <-> signature consistency (E1-E3) + W1 warning."""
import warnings

import pytest

from tickflow import Registry, Runner, parse


def _r():
    r = Registry()
    r.body("one", lambda a: a)
    r.body("two", lambda a, b: a + b)
    return r


def test_e1_arity_mismatch_raises_at_construction():
    r = _r()
    g = parse("[A]-->C\n[B]-->C\nC.bind: [A, B]\nC.body: one", registry=r)
    with pytest.raises(ValueError, match="expects 1 parameter"):
        Runner(g, r)


def test_e1_ok_when_arity_matches():
    r = _r()
    g = parse("[A]-->C\n[B]-->C\nC.bind: [A, B]\nC.body: two", registry=r)
    Runner(g, r)  # no raise


def test_e2_named_fields_must_match_params():
    r = Registry()
    r.body("named", lambda x, y: x)
    g = parse("[A]-->C\n[B]-->C\nC.bind: {a: A, b: B}\nC.body: named", registry=r)
    with pytest.raises(ValueError, match="do not match named bind fields"):
        Runner(g, r)


def test_e2_ok_when_names_match():
    r = Registry()
    r.body("named", lambda a, b: a + b)
    g = parse("[A]-->C\n[B]-->C\nC.bind: {a: A, b: B}\nC.body: named", registry=r)
    Runner(g, r)


def test_e3_guard_arity_must_be_one():
    r = Registry()
    r.body("b1", lambda v: None)
    r.guard("g2", lambda a, b: True)
    g = parse("[A]-->B\nB--|g2|-->C\nB.body: b1\nA.body: b1", registry=r)
    with pytest.raises(ValueError, match="exactly 1 parameter"):
        Runner(g, r)


def test_w1_ambiguous_auto_bind_warns():
    r = Registry()
    r.body("pos", lambda a, b: a + b)
    g = parse("[A]-->C\n[B]-->C\nC.body: pos", registry=r)
    with pytest.warns(UserWarning, match="auto-bind"):
        Runner(g, r)


def test_explicit_bind_no_w1():
    r = Registry()
    r.body("pos", lambda a, b: a + b)
    g = parse("[A]-->C\n[B]-->C\nC.bind: [B, A]\nC.body: pos", registry=r)
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        Runner(g, r)
    assert not any("auto-bind" in str(x.message) for x in rec)


def test_view_mode_body_no_w1_and_no_arity_check():
    r = Registry()
    r.body("viewer", lambda v: "x")
    g = parse("[A]-->C\n[B]-->C\nC.body: viewer", registry=r)
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        Runner(g, r)
    assert not any("auto-bind" in str(x.message) for x in rec)


def test_e1_skips_arity_unknown_var_args():
    # classify() records arity=None for *args bodies and non-introspectable
    # callables; E1 must skip them (the engine calls them with *values).
    r = Registry()
    r.body("var", lambda *a: len(a))
    g = parse("[A]-->C\n[B]-->C\nC.body: var", registry=r)
    Runner(g, r)  # no raise (a W1 auto-bind warning may fire; that's fine)


def test_e3_skips_arity_unknown_var_args_guard():
    r = Registry()
    r.body("b1", lambda v: None)
    r.guard("gvar", lambda *a: True)
    g = parse("[A]-->B\nB--|gvar|-->C\nB.body: b1\nA.body: b1", registry=r)
    Runner(g, r)  # arity unknown -> skip, like E1


def test_e2_state_kwonly_param_excluded_from_match():
    r = Registry()
    r.body("with_state", lambda a, *, state: a)
    g = parse("[A]-->C\nC.bind: {a: A}\nC.body: with_state", registry=r)
    Runner(g, r)  # kwonly `state` is not part of param_names -> E2 passes


def test_w1_fires_on_async_runner_too():
    from tickflow.async_runner import AsyncRunner

    r = Registry()
    r.body("pos", lambda a, b: a + b)
    g = parse("[A]-->C\n[B]-->C\nC.body: pos", registry=r)
    with pytest.warns(UserWarning, match="auto-bind"):
        AsyncRunner(g, r)


def test_w1_stacklevel_points_at_caller():
    # W1 must attribute to the user's Runner(...) line, not tickflow internals.
    r = Registry()
    r.body("pos", lambda a, b: a + b)
    g = parse("[A]-->C\n[B]-->C\nC.body: pos", registry=r)
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        Runner(g, r)
    w1 = [x for x in rec if "auto-bind" in str(x.message)]
    assert w1, "W1 should fire"
    assert w1[0].filename == __file__


def test_e2_kwonly_body_message_is_actionable():
    r = Registry()
    r.body("kwo", lambda *, a, b: a)
    g = parse("[A]-->C\n[B]-->C\nC.bind: {a: A, b: B}\nC.body: kwo", registry=r)
    with pytest.raises(ValueError, match="keyword-only"):
        Runner(g, r)
