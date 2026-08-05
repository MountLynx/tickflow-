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


# --------------------------------------------------------------------------
# Shared helpers
# --------------------------------------------------------------------------

def _reg(limit: int = 3) -> Registry:
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

    r.guard("cont_ltN", lambda v: v.B.value < limit)
    return r


def _loop_graph(r: Registry, limit: int = 3):
    return parse(
        "[seed]-->A\nseed.body: seed_zero\nA.body: passthru\nA.join: OR\n"
        "A-->B\nB.body: incr\nB--|cont_ltN|-->A",
        registry=r,
    )


# --------------------------------------------------------------------------
# A2: memory window
# --------------------------------------------------------------------------

def test_loop_window_bounded():
    r = _reg(limit=100)
    rn = Runner(_loop_graph(r, 100), r)
    rn.run_until_idle(max_ticks=500)
    assert len(rn.run_state._edges["A"]) == 2
    assert len(rn.run_state._edges["B"]) == 2
    # 窗口保留最近两次触发：A 输出 0..99，最后一条为 99
    assert rn.run_state._edges["A"][-1][1] == 99
    assert len(rn.audit_log()) >= 100      # full trail still available


def test_linear_flow_window_bounded():
    r = _reg()
    g = parse(
        "[seed]-->A\nseed.body: seed_zero\nA.body: passthru\n"
        "A-->B\nB.body: passthru\nB-->C\nC.body: passthru",
        registry=r,
    )
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=50)
    for lst in rn.run_state._edges.values():
        assert len(lst) <= 2


def test_big_output_not_retained_in_memory(tmp_path):
    # 直接驱动 RunState + 持久 backend：大 output 只留窗口两条，
    # 内存 _records 不累积，全量在库（D3/D4 语义）
    from tickflow.state import RunState, NodeState

    be = SqliteBackend(tmp_path / "big.db")
    rs = RunState(backend=be, session_id="s1", persistent=True)
    payload = {"payload": "x" * 100_000}
    for tick in range(1, 21):
        rs.record(NodeState(tick=tick, node="A", output=payload))
    assert len(rs._edges["A"]) <= 2
    assert rs._edges["A"][-1][1] is payload   # 窗口保留最近一次触发
    assert rs._records == []                  # 内存不累积（D4）
    rs.flush_firings()                        # 尾批落盘
    fs = be.firings_of("s1", "A")
    assert len(fs) == 20                      # 全量在库（D3）


def test_pending_queue_batches_per_tick_and_flushes():
    from tickflow.state import RunState, NodeState

    be = NullBackend()
    rs = RunState(backend=be, session_id="s1", persistent=True)
    rs.record(NodeState(tick=1, node="A", output="a1"))
    rs.record(NodeState(tick=1, node="B", output="b1"))
    assert be.list_firings("s1") == []        # 未跨 tick：不 flush
    rs.record(NodeState(tick=2, node="A", output="a2"))   # tick 推进 → 第一批落盘
    assert [f["node"] for f in be.list_firings("s1")] == ["A", "B"]
    assert len(rs._pending) == 1              # 队列只剩第二批
    rs.flush_firings()                        # 尾批显式 flush
    assert [f["node"] for f in be.list_firings("s1")] == ["A", "B", "A"]
