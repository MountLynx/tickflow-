"""Tests for the mermaid-like parser."""
from __future__ import annotations

import pytest

from tickflow import parse, ParseError, Registry


def _reg() -> Registry:
    r = Registry()
    r.body("b1", lambda v: None)
    r.body("compute_c", lambda v: None)
    r.guard("g1", lambda v: True)
    return r


def test_plain_edge_and_start():
    g = parse("[A]-->B", registry=_reg())
    assert g.starts == ["A"]
    assert len(g.edges) == 1
    assert g.edges[0].src == "A" and g.edges[0].dst == "B"
    assert g.edges[0].guard is None


def test_multiple_starts():
    g = parse("[A]-->C\n[B]-->C", registry=_reg())
    assert set(g.starts) == {"A", "B"}
    assert g.producers("C") == ["A", "B"]


def test_guarded_edge():
    r = _reg()
    # A guarded edge without a start raises (no start declared).
    with pytest.raises(ParseError):
        parse("A--|g1|-->B", registry=r)
    g = parse("[A]--|g1|-->B", registry=r)
    assert g.edges[0].guard == "g1"
    assert g.edges[0].src == "A" and g.edges[0].dst == "B"


def test_bare_start_declaration():
    g = parse("[A]\nA.body: b1", registry=_reg())
    assert g.starts == ["A"]
    assert g.nodes["A"].body == "b1"


def test_inputs_default_latest():
    g = parse("[A]-->C\n[A]-->B\nB-->C", registry=_reg())
    # C has producers A, B; both should default to latest_before.
    assert set(g.nodes["C"].inputs) == {"A", "B"}
    for p in g.nodes["C"].inputs.values():
        assert p.kind == "latest" and p.k is None


def test_inputs_index_syntax():
    r = _reg()
    g = parse("[A]-->B\nB-->C\nA-->C\nC.inputs: A, B[2]\nC.body: b1", registry=r)
    assert g.nodes["C"].inputs["A"].kind == "latest"
    assert g.nodes["C"].inputs["B"].kind == "index"
    assert g.nodes["C"].inputs["B"].k == 2


def test_inputs_replaces_default():
    r = _reg()
    g = parse("[A]-->C\nB-->C\n[A]-->B\nB.body: b1\nC.inputs: A\nC.body: b1", registry=r)
    # Explicit inputs replaces default: only A, not B.
    assert set(g.nodes["C"].inputs) == {"A"}


def test_body_and_join_declarations():
    r = _reg()
    g = parse("[A]-->C\nC.body: b1\nC.join: OR", registry=r)
    assert g.nodes["C"].body == "b1"
    assert g.nodes["C"].join == "OR"


def test_comments_and_blank_lines():
    g = parse("# a comment\n\n[A]-->B  # trailing\n", registry=_reg())
    assert g.starts == ["A"]
    assert len(g.edges) == 1


def test_no_start_raises():
    with pytest.raises(ParseError):
        parse("A-->B", registry=_reg())


def test_unregistered_guard_raises():
    r = Registry()
    with pytest.raises(ParseError):
        parse("[A]--|missing|-->B", registry=r)


def test_unregistered_body_raises():
    r = Registry()
    r.guard("g1", lambda v: True)
    with pytest.raises(ParseError):
        parse("[A]-->B\nB.body: nope", registry=r)


def test_input_from_non_producer_warns_not_errors():
    r = _reg()
    # Non-producer input must be upstream (A → B → C: A is upstream of C).
    g = parse("[A]-->B\nB.body: b1\nB-->C\nC.inputs: A\nC.body: b1", registry=r)
    assert "A" in g.nodes["C"].inputs


def test_input_from_non_existent_node_raises():
    r = _reg()
    with pytest.raises(ParseError, match="not a node"):
        parse("[A]-->C\nC.inputs: Z\nC.body: b1", registry=r)


def test_input_from_downstream_node_raises():
    r = _reg()
    # B reads X, but X is downstream of B (B→D→X). X fires after B.
    with pytest.raises(ParseError, match="no directed path"):
        parse("[A]-->B\nB.body: b1\nB-->D\nD.body: b1\nD-->X\nX.body: b1\nB.inputs: X", registry=r)
