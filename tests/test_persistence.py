"""Tests for persistence: JsonBackend + NullBackend, Runner auto-persist."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tickflow import parse, Runner, Registry, JsonBackend
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
