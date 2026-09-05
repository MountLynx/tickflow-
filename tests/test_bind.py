"""Bind declaration IR: construction, validation, Node/Graph integration."""
import json

import pytest

from tickflow import parse
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
