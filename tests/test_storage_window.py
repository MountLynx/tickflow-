"""Dual-layer storage: memory window + SQLite full history (spec 2026-08-05)."""
from __future__ import annotations

import gc
from pathlib import Path

from tickflow import parse, Runner, Registry, SqliteBackend, JsonBackend
from tickflow.persistence import NullBackend
from tickflow.views import Missing


# --------------------------------------------------------------------------
# A1: Backend cold-query protocol
# --------------------------------------------------------------------------

def test_sqlite_firing_at_and_firings_of(tmp_path):
    be = SqliteBackend(tmp_path / "f.db")
    be.save_firings("s1", [
        {"tick": 1, "node": "A", "output": "a1"},
        {"tick": 2, "node": "B", "output": "b1"},
        {"tick": 3, "node": "A", "output": "a2"},
        {"tick": 4, "node": "A", "output": "a3"},
    ])
    assert be.firing_at("s1", "A", 1) == "a1"
    assert be.firing_at("s1", "A", 3) == "a3"
    assert be.firing_at("s1", "A", 4) is None      # only 3 fires
    assert be.firing_at("s1", "B", 1) == "b1"
    assert be.firing_at("s1", "nope", 1) is None
    assert be.firings_of("s1", "A") == [(1, "a1"), (3, "a2"), (4, "a3")]
    assert be.firings_of("s1", "nope") == []


def test_null_backend_cold_queries_degrade():
    be = NullBackend()
    be.save_firings("s1", [{"tick": 1, "node": "A", "output": "a1"}])
    assert be.firing_at("s1", "A", 1) is None      # D7: no cold history
    assert be.firings_of("s1", "A") == []


def test_json_backend_firing_at_and_firings_of(tmp_path):
    be = JsonBackend(tmp_path)
    be.save_firings("s1", [
        {"tick": 1, "node": "A", "output": "a1"},
        {"tick": 3, "node": "A", "output": "a2"},
        {"tick": 2, "node": "B", "output": "b1"},
    ])
    assert be.firing_at("s1", "A", 1) == "a1"
    assert be.firing_at("s1", "A", 2) == "a2"
    assert be.firing_at("s1", "A", 3) is None
    assert be.firings_of("s1", "A") == [(1, "a1"), (3, "a2")]
