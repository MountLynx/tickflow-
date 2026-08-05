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


def test_sqlite_legacy_db_migrates_node_column(tmp_path):
    """Old databases (no `node` column) migrate on open, backfill from data,
    and re-initialising is idempotent."""
    import sqlite3
    import json as _json

    db = tmp_path / "legacy.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE firings ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "session_id TEXT NOT NULL, tick INTEGER NOT NULL, data TEXT NOT NULL)"
    )
    conn.execute(
        "INSERT INTO firings (session_id, tick, data) VALUES (?, ?, ?)",
        ("s1", 1, _json.dumps({"tick": 1, "node": "A", "output": "a1"})),
    )
    conn.commit()
    conn.close()

    be = SqliteBackend(db)          # opens the legacy DB -> migration runs
    assert be.firing_at("s1", "A", 1) == "a1"
    assert be.firings_of("s1", "A") == [(1, "a1")]
    be2 = SqliteBackend(db)         # second open: migration must be a no-op
    assert be2.firing_at("s1", "A", 1) == "a1"
    be.close()
    be2.close()
