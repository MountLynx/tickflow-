"""Registration-time signature classification (value mode vs view mode)."""
import pytest

from tickflow.registry import Registry, classify
from tickflow.views import DictView


def test_view_by_name_v():
    def f(v): ...
    assert classify(f).mode == "view"


def test_view_by_name_view():
    def f(view): ...
    assert classify(f).mode == "view"


def test_view_by_string_annotation():
    def f(v: "DictView"): ...
    assert classify(f).mode == "view"


# TODO(Task 4): switch to NodeView annotation (NodeView lands with Task 4).
def test_view_by_class_annotation():
    def f(v: DictView): ...
    assert classify(f).mode == "view"


def test_value_plain_names():
    def f(a, b): ...
    s = classify(f)
    assert s.mode == "value"
    assert s.arity == 2
    assert s.param_names == ("a", "b")
    assert s.wants_state is False


def test_value_zero_arity():
    def f(): ...
    s = classify(f)
    assert s.mode == "value"
    assert s.arity == 0


def test_value_wants_state():
    def f(a, *, state): ...
    s = classify(f)
    assert s.mode == "value"
    assert s.arity == 1
    assert s.wants_state is True


def test_async_detection():
    async def f(v): ...
    assert classify(f).is_async is True

    async def g(a): ...
    s = classify(g)
    assert s.mode == "value" and s.is_async is True


def test_registry_records_and_serves_sigs():
    r = Registry()
    r.body("b", lambda a: a)
    r.guard("g", lambda v: True)
    assert r.body_sig("b").arity == 1
    assert r.guard_sig("g").mode == "view"
    with pytest.raises(KeyError):
        r.body_sig("nope")
    with pytest.raises(KeyError):
        r.guard_sig("nope")


def test_value_wants_state_async():
    async def f(a, *, state): ...
    s = classify(f)
    assert s.wants_state is True and s.is_async is True


def test_non_introspectable_callable_falls_back():
    import functools

    s = classify(functools.reduce)
    assert s.mode == "value"
    assert s.arity is None  # unknown -> build-time arity checks skip


def test_var_positional_arity_unknown():
    def f(*values): ...
    s = classify(f)
    assert s.mode == "value"
    assert s.arity is None


def test_future_annotations_module_shapes():
    """PEP 563 modules store annotations as strings — the exact SpecModule shape."""
    import textwrap

    ns: dict = {}
    exec(
        textwrap.dedent(
            """
            from __future__ import annotations
            from tickflow.views import DictView

            def bare(view: DictView): ...
            def quoted(view: "DictView"): ...
            def dotted(v: "tickflow.views.DictView"): ...
            """
        ),
        ns,
    )
    assert classify(ns["bare"]).mode == "view"
    assert classify(ns["quoted"]).mode == "view"
    assert classify(ns["dotted"]).mode == "view"
