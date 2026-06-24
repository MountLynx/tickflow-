"""Tests for persistence: JsonBackend + NullBackend, Runner auto-persist."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tickflow import parse, Runner, Registry, JsonBackend, SqliteBackend
from tickflow.persistence import NullBackend
from tickflow.views import Missing


def _reg():
    r = Registry()
    r.body("seed_zero", lambda v: 0)

    @r.body("passthru")
    def _p(v):
        for _n, val in v.items():
            if val is not Missing:
                return val
        return None

    @r.body("incr")
    def _incr(v):
        return v.A.value + 1

    r.guard("cont_lt3", lambda v: v.B.value < 3)
    return r


def _loop_graph(r):
    return parse(
        "[seed]-->A\nseed.body: seed_zero\nA.body: passthru\nA.join: OR\nA-->B\nB.body: incr\nB--|cont_lt3|-->A",
        registry=r,
    )


def test_null_backend_records_snapshots_and_firings():
    r = _reg()
    g = _loop_graph(r)
    be = NullBackend()
    rn = Runner(g, r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=20)
    # Snapshots saved at each tick (1..final).
    snaps = be.list_snapshots("s1")
    assert snaps == list(range(1, rn.tick_count + 1))
    # Firings recorded.
    fs = be.list_firings("s1")
    assert len(fs) == len(rn.audit)


def test_null_backend_latest_tick():
    r = _reg()
    g = _loop_graph(r)
    be = NullBackend()
    rn = Runner(g, r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=20)
    assert be.latest_tick("s1") == rn.tick_count


def test_json_backend_writes_files(tmp_path):
    r = _reg()
    g = _loop_graph(r)
    be = JsonBackend(tmp_path)
    rn = Runner(g, r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=20)
    sess_dir = tmp_path / "s1"
    assert sess_dir.exists()
    # tick_*.json files exist.
    tick_files = sorted(sess_dir.glob("tick_*.json"))
    assert len(tick_files) == rn.tick_count
    # firings.jsonl exists with one line per firing.
    fpath = sess_dir / "firings.jsonl"
    assert fpath.exists()
    lines = [l for l in fpath.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(lines) == len(rn.audit)


def test_json_backend_load_snapshot(tmp_path):
    r = _reg()
    g = _loop_graph(r)
    be = JsonBackend(tmp_path)
    rn = Runner(g, r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=20, pause_at={3})
    snap3 = be.load_snapshot("s1", 3)
    assert snap3 is not None
    assert snap3["tick"] == 3
    # latest_tick reflects the highest saved.
    rn.run_until_idle(max_ticks=20)
    assert be.latest_tick("s1") == rn.tick_count


def test_json_backend_list_firings_since(tmp_path):
    r = _reg()
    g = _loop_graph(r)
    be = JsonBackend(tmp_path)
    rn = Runner(g, r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=20)
    # Firings from tick 3 onward.
    since3 = be.list_firings("s1", since_tick=3)
    assert all(f["tick"] >= 3 for f in since3)
    assert len(since3) < len(rn.audit)


def test_runner_without_backend_no_persistence(tmp_path):
    r = _reg()
    g = _loop_graph(r)
    rn = Runner(g, r)  # no backend
    rn.run_until_idle(max_ticks=20)
    # No files written.
    assert not any(tmp_path.iterdir())


def test_persisted_snapshot_restores_into_new_runner(tmp_path):
    r = _reg()
    g = _loop_graph(r)
    be = JsonBackend(tmp_path)
    rn = Runner(g, r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=20, pause_at={3})
    snap = be.load_snapshot("s1", 3)
    # Build a fresh runner from the persisted snapshot and finish the run.
    rn2 = Runner(g, r)
    rn2.restore(snap)
    assert rn2.tick_count == 3
    rn2.run_until_idle(max_ticks=20)
    # Compare against an in-memory runner that also paused at 3 then finished.
    r3 = _reg()
    rn3 = Runner(_loop_graph(r3), r3)
    rn3.run_until_idle(max_ticks=20, pause_at={3})
    rn3.run_until_idle(max_ticks=20)
    # Compare firings from tick 3 onward (rn2's audit starts empty at restore;
    # rn3's audit has ticks 0..2 from its first run). Both resumed identically.
    rn2_from3 = [(f.tick, f.node, f.output) for f in rn2.audit if f.tick >= 3]
    rn3_from3 = [(f.tick, f.node, f.output) for f in rn3.audit if f.tick >= 3]
    assert rn2_from3 == rn3_from3


# ---- SqliteBackend tests ---------------------------------------------------


def test_sqlite_backend_save_load_snapshot(tmp_path):
    be = SqliteBackend(tmp_path / "test.db")
    snap = {"tick": 3, "marking": {"slots": {}}, "history": {}}
    be.save_snapshot("s1", 3, snap)
    loaded = be.load_snapshot("s1", 3)
    assert loaded == snap
    assert be.load_snapshot("s1", 99) is None


def test_sqlite_backend_latest_tick(tmp_path):
    be = SqliteBackend(tmp_path / "test.db")
    assert be.latest_tick("s1") is None
    be.save_snapshot("s1", 5, {"tick": 5})
    be.save_snapshot("s1", 1, {"tick": 1})
    be.save_snapshot("s1", 3, {"tick": 3})
    assert be.latest_tick("s1") == 5


def test_sqlite_backend_list_snapshots_ascending(tmp_path):
    be = SqliteBackend(tmp_path / "test.db")
    be.save_snapshot("s1", 3, {"tick": 3})
    be.save_snapshot("s1", 1, {"tick": 1})
    be.save_snapshot("s1", 2, {"tick": 2})
    assert be.list_snapshots("s1") == [1, 2, 3]
    assert be.list_snapshots("nosuch") == []


def test_sqlite_backend_save_list_firings(tmp_path):
    be = SqliteBackend(tmp_path / "test.db")
    be.save_firing("s1", {"tick": 1, "node": "A"})
    be.save_firing("s1", {"tick": 2, "node": "B"})
    be.save_firing("s1", {"tick": 3, "node": "A"})
    be.save_firing("s1", {"tick": 4, "node": "C"})
    all_fs = be.list_firings("s1")
    assert len(all_fs) == 4
    since3 = be.list_firings("s1", since_tick=3)
    assert len(since3) == 2
    assert all(f["tick"] >= 3 for f in since3)


def test_sqlite_backend_save_firings_batch(tmp_path):
    """Batch save_firings inserts all rows in one transaction."""
    be = SqliteBackend(tmp_path / "test.db")
    be.save_firings("s1", [
        {"tick": 1, "node": "A"},
        {"tick": 1, "node": "B"},
        {"tick": 2, "node": "C"},
    ])
    all_fs = be.list_firings("s1")
    assert len(all_fs) == 3
    assert [f["node"] for f in all_fs] == ["A", "B", "C"]


def test_sqlite_backend_save_firings_empty_is_noop(tmp_path):
    be = SqliteBackend(tmp_path / "test.db")
    be.save_firings("s1", [])
    assert be.list_firings("s1") == []


def test_json_backend_save_firings_batch(tmp_path):
    """JsonBackend batch writes all firings with one file open."""
    from tickflow import JsonBackend
    be = JsonBackend(tmp_path)
    be.save_firings("s1", [
        {"tick": 0, "node": "A"},
        {"tick": 0, "node": "B"},
    ])
    fs = be.list_firings("s1")
    assert len(fs) == 2
    assert [f["node"] for f in fs] == ["A", "B"]


def test_sqlite_backend_checkpoints(tmp_path):
    be = SqliteBackend(tmp_path / "test.db")
    be.save_checkpoint("s1", "cp1", {"tick": 3, "data": "hello"})
    be.save_checkpoint("s1", "cp2", {"tick": 5, "data": "world"})
    cps = be.list_checkpoints("s1")
    assert ("cp1", 3) in cps
    assert ("cp2", 5) in cps
    loaded = be.load_checkpoint("s1", "cp1")
    assert loaded == {"tick": 3, "data": "hello"}
    # Overwrite same label.
    be.save_checkpoint("s1", "cp1", {"tick": 7, "data": "updated"})
    cps = be.list_checkpoints("s1")
    assert ("cp1", 7) in cps
    loaded = be.load_checkpoint("s1", "cp1")
    assert loaded == {"tick": 7, "data": "updated"}


def test_sqlite_backend_multiple_sessions(tmp_path):
    be = SqliteBackend(tmp_path / "test.db")
    be.save_snapshot("s1", 1, {"tick": 1, "val": "a"})
    be.save_snapshot("s2", 1, {"tick": 1, "val": "b"})
    be.save_snapshot("s1", 2, {"tick": 2, "val": "c"})
    assert be.list_snapshots("s1") == [1, 2]
    assert be.list_snapshots("s2") == [1]
    assert be.latest_tick("s1") == 2
    assert be.latest_tick("s2") == 1
    assert be.load_snapshot("s1", 1) == {"tick": 1, "val": "a"}
    assert be.load_snapshot("s2", 1) == {"tick": 1, "val": "b"}


def test_sqlite_backend_records_snapshots_and_firings(tmp_path):
    r = _reg()
    g = _loop_graph(r)
    be = SqliteBackend(tmp_path / "test.db")
    rn = Runner(g, r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=20)
    snaps = be.list_snapshots("s1")
    assert snaps == list(range(1, rn.tick_count + 1))
    fs = be.list_firings("s1")
    assert len(fs) == len(rn.audit)


def test_sqlite_backend_restore_runner(tmp_path):
    r = _reg()
    g = _loop_graph(r)
    be = SqliteBackend(tmp_path / "test.db")
    rn = Runner(g, r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=20, pause_at={3})
    snap = be.load_snapshot("s1", 3)
    rn2 = Runner(g, r)
    rn2.restore(snap)
    assert rn2.tick_count == 3
    rn2.run_until_idle(max_ticks=20)
    r3 = _reg()
    rn3 = Runner(_loop_graph(r3), r3)
    rn3.run_until_idle(max_ticks=20, pause_at={3})
    rn3.run_until_idle(max_ticks=20)
    rn2_from3 = [(f.tick, f.node, f.output) for f in rn2.audit if f.tick >= 3]
    rn3_from3 = [(f.tick, f.node, f.output) for f in rn3.audit if f.tick >= 3]
    assert rn2_from3 == rn3_from3
