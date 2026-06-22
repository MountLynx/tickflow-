"""Tests for graph export: to_dict() and to_mermaid() for front-end rendering."""
from __future__ import annotations

import json

import pytest

from tickflow import parse, Registry


def _reg() -> Registry:
    r = Registry()
    r.body("b1", lambda v: None)
    r.guard("g1", lambda v: True)
    return r


def test_to_dict_structure():
    r = _reg()
    g = parse("[A]-->B\nB--|g1|-->C\nC.body: b1\nB.body: b1", registry=r)
    d = g.to_dict()
    assert set(d.keys()) == {"nodes", "edges", "starts"}
    # starts
    assert d["starts"] == ["A"]
    # nodes
    assert "A" in d["nodes"] and "B" in d["nodes"] and "C" in d["nodes"]
    assert d["nodes"]["A"]["is_start"] is True
    assert d["nodes"]["B"]["is_start"] is False
    assert d["nodes"]["C"]["join"] == "AND"
    assert d["nodes"]["C"]["body"] == "b1"
    # producers derived
    assert d["nodes"]["C"]["producers"] == ["B"]
    assert d["nodes"]["B"]["producers"] == ["A"]
    # edges
    assert len(d["edges"]) == 2
    edge_bc = [e for e in d["edges"] if e["src"] == "B" and e["dst"] == "C"][0]
    assert edge_bc["guard"] == "g1"
    edge_ab = [e for e in d["edges"] if e["src"] == "A" and e["dst"] == "B"][0]
    assert edge_ab["guard"] is None


def test_to_dict_json_serializable():
    r = _reg()
    g = parse("[A]-->B\nB.body: b1\nA.body: b1", registry=r)
    d = g.to_dict()
    s = json.dumps(d)  # must not raise
    assert json.loads(s) == d


def test_to_dict_inputs_policy():
    r = _reg()
    g = parse("[A]-->B\nB-->C\nA-->C\nC.inputs: A, B[2]\nC.body: b1", registry=r)
    d = g.to_dict()
    inputs_c = d["nodes"]["C"]["inputs"]
    assert inputs_c["A"] == {"kind": "latest", "k": None}
    assert inputs_c["B"] == {"kind": "index", "k": 2}


def test_to_mermaid_basic():
    r = _reg()
    g = parse("[A]-->B\nB.body: b1\nA.body: b1", registry=r)
    m = g.to_mermaid()
    assert m.startswith("graph TD")
    # start node uses stadium shape
    assert 'A(["A"])' in m
    # plain edge
    assert "A --> B" in m


def test_to_mermaid_guarded_edge():
    r = _reg()
    g = parse("[A]-->B\nB--|g1|-->C\nB.body: b1\nA.body: b1\nC.body: b1", registry=r)
    m = g.to_mermaid()
    assert "B -->|g1| C" in m
    assert "A --> B" in m


def test_to_mermaid_stable_order():
    r = _reg()
    g = parse("[A]-->B\nB.body: b1\nA.body: b1", registry=r)
    assert g.to_mermaid() == g.to_mermaid()


def test_to_mermaid_multiple_starts():
    r = _reg()
    g = parse("[A]-->C\n[B]-->C\nC.body: b1\nA.body: b1\nB.body: b1", registry=r)
    m = g.to_mermaid()
    assert 'A(["A"])' in m
    assert 'B(["B"])' in m


def test_to_mermaid_self_loop():
    r = _reg()
    g = parse("[A]-->A\nA.body: b1\nA.join: OR", registry=r)
    m = g.to_mermaid()
    assert "A --> A" in m


def test_to_dict_empty_graph():
    from tickflow.ir import Graph
    g = Graph()
    d = g.to_dict()
    assert d == {"nodes": {}, "edges": [], "starts": []}
