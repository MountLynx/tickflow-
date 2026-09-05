"""NodeView / GuardView consumption API + legacy DictView shim."""
import pytest

from tickflow.views import (
    DictView, GuardView, Missing, NodeView, Resolved, _ReadOnlyStateView,
)


def _node_view(fields=None, values=(), resolved=None, node="n"):
    return NodeView(node=node, fields=fields, values=values,
                    state={"s": 1}, resolved=resolved)


def test_input_none_when_no_inputs():
    v = _node_view(fields=(), values=())
    assert v.input() is None
    assert v.args == ()
    assert v.named == {}


def test_input_single_value():
    v = _node_view(fields=((None, "A"),), values=(7,))
    assert v.input() == 7
    assert v.args == (7,)


def test_input_multi_returns_tuple():
    v = _node_view(fields=((None, "A"), (None, "B")), values=(1, 2))
    assert v.input() == (1, 2)
    assert v.args == (1, 2)


def test_missing_survives_in_args_named_and_input():
    # Correctness contract (design 4.6): Missing is never collapsed to None.
    v = _node_view(fields=(("prev", "P"), ("seed", "S")),
                   values=(Missing, "S"))
    assert v.args == (Missing, "S")
    assert v.named == {"prev": Missing, "seed": "S"}
    assert v.input() == (Missing, "S")
    assert v.field("prev") is Missing


def test_named_empty_for_positional_bind():
    v = _node_view(fields=((None, "A"),), values=(1,))
    assert v.named == {}


def test_field_on_positional_bind_raises_type_error():
    v = _node_view(fields=((None, "A"),), values=(1,))
    with pytest.raises(TypeError):
        v.field("A")


def test_field_unknown_name_raises_key_error():
    v = _node_view(fields=(("x", "A"),), values=(1,))
    with pytest.raises(KeyError):
        v.field("y")


def test_state_and_node():
    v = _node_view(node="me")
    assert v.state == {"s": 1}
    assert v.node == "me"


def test_legacy_name_access_warns_and_resolves():
    v = _node_view(fields=None, values=(5,),
                   resolved={"A": Resolved(value=5, k=None)})
    with pytest.warns(DeprecationWarning):
        assert v.A.value == 5
    with pytest.warns(DeprecationWarning):
        assert v["A"].value == 5
    with pytest.warns(DeprecationWarning):
        assert v.inputs() == {"A": 5}
    with pytest.warns(DeprecationWarning):
        assert list(v.items()) == [("A", 5)]
    with pytest.warns(DeprecationWarning):
        assert "A" in v


def test_legacy_missing_name_raises_without_warning():
    import warnings as w
    v = _node_view(resolved={})
    with w.catch_warnings(record=True) as rec:
        w.simplefilter("always")
        with pytest.raises(AttributeError):
            v.Nope
        with pytest.raises(KeyError):
            v["Nope"]
        assert not any(issubclass(x.category, DeprecationWarning) for x in rec)


def test_dictview_shim_warns_and_behaves():
    with pytest.warns(DeprecationWarning):
        v = DictView({"A": Resolved(value=5, k=None)}, node="legacy")
    assert v.node == "legacy"
    assert v.input() == 5
    with pytest.warns(DeprecationWarning):
        assert v["A"].value == 5


# --- GuardView ---

def _guard_view(**kw):
    defaults = dict(
        src="R", output="out", fields=(("d", "M"),), values=("m",),
        state=_ReadOnlyStateView({"n": 1}),
        resolved={"M": Resolved("m", None), "d": Resolved("m", None),
                  "R": Resolved("out", None)},
    )
    defaults.update(kw)
    return GuardView(**defaults)


def test_guard_view_output_args_named_field():
    v = _guard_view()
    assert v.output == "out"
    assert v.args == ("m",)
    assert v.named == {"d": "m"}
    assert v.field("d") == "m"
    assert v.node == "R" and v.src == "R"


def test_guard_view_has_no_input():
    v = _guard_view()
    with pytest.raises(AttributeError):
        v.input()


def test_guard_view_state_read_only():
    st = _ReadOnlyStateView({"n": 1})
    assert st.get("n") == 1
    with pytest.raises(TypeError):
        st["x"] = 2
    with pytest.raises(TypeError):
        st.x = 2


def test_guard_view_legacy_access_and_precedence():
    v = _guard_view()
    with pytest.warns(DeprecationWarning):
        assert v["R"].value == "out"      # src name -> adjudicated output
    with pytest.warns(DeprecationWarning):
        assert v["M"].value == "m"        # producer name -> declared input
    with pytest.raises(KeyError):
        v["Nope"]                          # undeclared node -> refused (P4)
