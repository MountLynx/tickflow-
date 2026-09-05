"""Bind declaration IR: construction, validation, Node/Graph integration."""
import json

import pytest

from tickflow import parse, ParseError
from tickflow.ir import Bind


def test_bind_positional():
    b = Bind.positional(["A", "C"])
    assert b.entries == ((None, "A"), (None, "C"))
    assert b.producers == ("A", "C")
    assert b.fields == ()
    assert b.is_named is False


def test_bind_named():
    b = Bind.named({"outline": "A", "draft": "C"})
    assert b.fields == ("outline", "draft")
    assert b.producers == ("A", "C")
    assert b.is_named is True


def test_bind_named_accepts_pair_list():
    b = Bind.named([("x", "A"), ("y", "B")])
    assert b.fields == ("x", "y")


def test_bind_named_rejects_bare_string():
    with pytest.raises(TypeError, match="bare string"):
        Bind.named("xy")


def test_bind_rejects_mixed():
    with pytest.raises(ValueError, match="mix"):
        Bind(entries=(("x", "A"), (None, "C")))


def test_bind_rejects_duplicate_field():
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        Bind(entries=(("x", "A"), ("x", "C")))


def test_bind_rejects_duplicate_producer():
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        Bind.positional(["A", "A"])


def test_bind_rejects_empty():
    with pytest.raises(ValueError, match="at least one"):
        Bind(entries=())


def test_node_bind_default_none_copy_and_to_dict():
    g = parse("[A]-->B", registry=None)
    assert g.nodes["A"].bind is None
    assert g.nodes["B"].bind is None
    g.nodes["B"].bind = Bind.named({"x": "A"})
    g2 = g.copy()
    assert g2.nodes["B"].bind == g.nodes["B"].bind
    d = g.to_dict()
    assert d["nodes"]["B"]["bind"] == {"fields": ["x"], "producers": ["A"]}
    assert d["nodes"]["A"]["bind"] is None
    json.dumps(d)  # must stay JSON-serialisable


# --- parser syntax (Task 2) ---

def test_bind_positional_rejects_bare_string():
    with pytest.raises(TypeError, match="bare"):
        Bind.positional("AB")


def test_bind_normalizes_list_entries():
    b = Bind(entries=[["x", "A"]])
    assert b.entries == (("x", "A"),)
    hash(b)  # normalized payload must be hashable


def test_bind_rejects_bare_string_entry():
    with pytest.raises(ValueError, match="pairs"):
        Bind(entries=["AB"])


def test_parser_bind_positional():
    g = parse("[A]-->C\n[B]-->C\nC.bind: [A, B]", registry=None)
    assert g.nodes["C"].bind.entries == ((None, "A"), (None, "B"))


def test_parser_bind_named():
    g = parse("[A]-->C\nC.bind: {x: A}", registry=None)
    assert g.nodes["C"].bind.entries == (("x", "A"),)


def test_parser_bind_single_sugar():
    g = parse("[A]-->C\nC.bind: A", registry=None)
    assert g.nodes["C"].bind.entries == ((None, "A"),)


def test_parser_bind_bad_term():
    with pytest.raises(ParseError):
        parse("[A]-->C\nC.bind: [A, 2B]", registry=None)


def test_parser_bind_unterminated():
    with pytest.raises(ParseError):
        parse("[A]-->C\nC.bind: [A", registry=None)


def test_parser_bind_named_bad_term():
    with pytest.raises(ParseError):
        parse("[A]-->C\nC.bind: {x A}", registry=None)


def test_parser_bind_unknown_producer():
    with pytest.raises(ParseError, match="not a node"):
        parse("[A]-->C\nC.bind: [Z]", registry=None)


def test_parser_bind_unreachable_producer():
    with pytest.raises(ParseError, match="no directed path"):
        parse("[A]-->B\nC-->D\nB.bind: [C]", registry=None)


def test_parser_bind_upstream_nonproducer_adds_policy():
    g = parse("[A]-->B\nB-->C\nC.bind: [A]", registry=None)
    assert g.nodes["C"].inputs["A"].kind == "latest"


def test_parser_bind_preserves_declared_policies():
    g = parse("[A]-->C\n[B]-->C\nC.inputs: A, B[2]\nC.bind: [B, A]", registry=None)
    assert g.nodes["C"].inputs["B"].kind == "index"
    assert g.nodes["C"].inputs["A"].kind == "latest"


def test_parser_bind_error_has_line_number():
    with pytest.raises(ParseError, match="line 2"):
        parse("[A]-->C\nC.bind: [Z]", registry=None)


def test_parser_bind_text_level_duplicates_rejected():
    with pytest.raises(ParseError, match="[Dd]uplicate"):
        parse("[A]-->C\n[B]-->C\nC.bind: [A, A]", registry=None)
    with pytest.raises(ParseError, match="[Dd]uplicate"):
        parse("[A]-->C\nC.bind: {x: A, x: A}", registry=None)


def test_parser_bind_empty_containers_rejected():
    with pytest.raises(ParseError):
        parse("[A]-->C\nC.bind: {}", registry=None)
    with pytest.raises(ParseError):
        parse("[A]-->C\nC.bind: []", registry=None)


def test_parser_bind_undeclared_node_no_path():
    with pytest.raises(ParseError, match="no directed path"):
        parse("[A]-->B\nZ.bind: [A]", registry=None)
