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


# --------------------------------------------------------------------------
# A3: resolve() dispatch
# --------------------------------------------------------------------------

def _index_graph(r: Registry, k: str = "A[3]"):
    @r.body("track_k")
    def _track(v):
        return v.A.value

    return parse(
        "[seed]-->A\nseed.body: seed_zero\nA.body: passthru\nA.join: OR\n"
        "A-->B\nB.body: incr\nB--|cont_ltN|-->A\n"
        "A-->C\nC.inputs: %s\nC.body: track_k" % k,
        registry=r,
    )


def test_index_resolves_from_backend():
    r = _reg(limit=5)              # A fires 5 times (values 0..4)
    # A6 前 Runner 未接线 backend（persistent=False）：窗口外 A[3] 降级 Missing，
    # A6 接通 backend 后经 firing_at 冷查询解析为 2
    rn = Runner(_index_graph(r), r)
    rn.run_until_idle(max_ticks=500)
    a_outputs = [f.output for f in rn.audit_log() if f.node == "A"]
    assert len(a_outputs) == 5
    c_outputs = [f.output for f in rn.audit_log() if f.node == "C"]
    assert c_outputs[-1] == a_outputs[2]   # A[3] = 3rd fire = 2


def test_index_outside_window_missing_with_null_backend():
    r = _reg(limit=5)
    rn = Runner(_index_graph(r), r, backend=NullBackend())
    rn.run_until_idle(max_ticks=500)
    c_outputs = [f.output for f in rn.audit_log() if f.node == "C"]
    assert c_outputs[-1] is Missing   # A[3] outside the 2-entry window → Missing


def test_index_dispatch_window_backend_null(tmp_path):
    # 直接驱动 RunState：窗口命中 / 窗口外 backend 冷查询 / NullBackend 降级
    from tickflow.state import RunState, NodeState

    be = SqliteBackend(tmp_path / "idx.db")
    rs = RunState(backend=be, session_id="s1", persistent=True)
    for tick, v in [(1, "v1"), (3, "v2"), (5, "v3"), (7, "v4"), (9, "v5")]:
        rs.record(NodeState(tick=tick, node="A", output=v))
    rs.flush_firings()
    assert rs.resolve("A", "index", 4, 99) == "v4"      # 窗口内
    assert rs.resolve("A", "index", 5, 99) == "v5"
    assert rs.resolve("A", "index", 3, 99) == "v3"      # 窗口外 → backend
    assert rs.resolve("A", "index", 1, 99) == "v1"
    assert rs.resolve("A", "index", 6, 99) is Missing   # 只有 5 次
    assert rs.resolve("A", "index", 0, 99) is Missing   # k<1 守卫
    # NullBackend：窗口内命中、窗口外降级
    rs2 = RunState(backend=NullBackend(), session_id="s1", persistent=False)
    for tick, v in [(1, "v1"), (3, "v2"), (5, "v3"), (7, "v4"), (9, "v5")]:
        rs2.record(NodeState(tick=tick, node="A", output=v))
    assert rs2.resolve("A", "index", 5, 99) == "v5"
    assert rs2.resolve("A", "index", 3, 99) is Missing  # 窗口外降级（D7）


def test_and_or_join_no_same_tick_crosstalk():
    r = _reg()
    g = parse(
        "[seed]-->A\nseed.body: seed_zero\nA.body: passthru\n"
        "A-->C\nA-->D\nC-->D\n"
        "C.body: passthru\nD.join: OR\nD.inputs: C\nD.body: passthru",
        registry=r,
    )
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=50)
    d_outputs = [f.output for f in rn.audit_log() if f.node == "D"]
    # D fires at tick 2 alongside C (OR: A slot) — must NOT see C's same-tick
    # write (Missing → passthru None); at tick 3 it sees C's tick-2 output.
    assert d_outputs[0] is None
    assert d_outputs[1] == 0


# --------------------------------------------------------------------------
# A4: audit / firings_of dispatch
# --------------------------------------------------------------------------

def test_audit_from_backend_full_and_dedup(tmp_path):
    from tickflow.state import RunState, NodeState

    be = SqliteBackend(tmp_path / "audit.db")
    rs = RunState(backend=be, session_id="s1", persistent=True)
    rs.record(NodeState(tick=1, node="A", output="a1"))
    rs.record(NodeState(tick=1, node="B", output="b1"))
    rs.record(NodeState(tick=2, node="A", output="a2"))
    # audit() 自动 flush 尾批（无需显式 flush）
    audit = rs.audit()
    assert [(ns.tick, ns.node, ns.output) for ns in audit] == [
        (1, "A", "a1"), (1, "B", "b1"), (2, "A", "a2"),
    ]
    assert rs._records == []                 # D4: 持久路径内存不累积
    # 重放去重：同一 (tick, node) 再次落盘 → audit 只保留第一条
    be.save_firings("s1", [
        {"tick": 1, "node": "A", "output": "a1-replayed"},
    ])
    audit2 = rs.audit()
    assert [(ns.tick, ns.node, ns.output) for ns in audit2] == [
        (1, "A", "a1"), (1, "B", "b1"), (2, "A", "a2"),
    ]
    # ceiling：超 ceiling 的行（如同 session 的上一轮运行数据）被排除
    be.save_firings("s1", [
        {"tick": 99, "node": "A", "output": "stale"},
    ])
    audit3 = rs.audit()
    assert [(ns.tick, ns.node, ns.output) for ns in audit3] == [
        (1, "A", "a1"), (1, "B", "b1"), (2, "A", "a2"),
    ]


def test_audit_empty_when_keep_records_false(tmp_path):
    from tickflow.state import RunState, NodeState

    be = SqliteBackend(tmp_path / "audit2.db")
    rs = RunState(backend=be, session_id="s1", persistent=True, keep_records=False)
    rs.record(NodeState(tick=1, node="A", output="a1"))
    assert rs.audit() == []                  # keep_records 门控不变
    rs.flush_firings()                       # 尾批落盘（runner 每 tick 驱动）
    assert len(be.list_firings("s1")) == 1   # 但落盘与 keep_records 正交（D11）


def test_firings_of_dispatch_direct():
    from tickflow.state import RunState, NodeState

    be = SqliteBackend(":memory:")
    rs = RunState(backend=be, session_id="s1", persistent=True)
    for tick, v in [(1, "v1"), (3, "v2"), (5, "v3")]:
        rs.record(NodeState(tick=tick, node="A", output=v))
    rs.flush_firings()
    assert rs.firings_of("A") == [(1, "v1"), (3, "v2"), (5, "v3")]  # 全量
    rs2 = RunState(backend=NullBackend(), session_id="s1", persistent=False)
    for tick, v in [(1, "v1"), (3, "v2"), (5, "v3")]:
        rs2.record(NodeState(tick=tick, node="A", output=v))
    assert rs2.firings_of("A") == [(3, "v2"), (5, "v3")]            # 窗口 2 条
    assert len(rs2.firings_of("A")) == 2


# --------------------------------------------------------------------------
# A5: snapshot / restore / truncate
# --------------------------------------------------------------------------

def test_restore_then_index_resolves_from_backend(tmp_path):
    r = _reg(limit=5)
    be = SqliteBackend(tmp_path / "restore.db")
    rn = Runner(_index_graph(r), r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=500, pause_at={5})
    snap = rn.snapshot()
    rn.run_until_idle(max_ticks=500)
    rn.restore(snap)
    # D5：restore 后 A[k] 仍可解析（快照窗口 + firings 全量在库）
    assert rn.run_state.resolve("A", "index", 3, 999) == 2
    rn.run_until_idle(max_ticks=500)
    # 重放后窗口反映最新一次触发（C 最近一次读到 A[3] = 2）
    assert rn.run_state.edges["C"][-1][1] == 2


def test_state_rebuilt_from_backend_after_restore(tmp_path):
    r = Registry()

    @r.body("counter")
    def _counter(v):
        v.state["attempts"] = v.state.get("attempts", 0) + 1
        return v.state["attempts"]

    @r.guard("under_three")
    def _under3(v):
        return v.state.get("attempts", 0) < 3

    # 与 test_node_state.py::test_state_driven_loop_terminates 同构（已知可靠模式）
    g = parse(
        "[A]-->B\nB.body: counter\nB--|under_three|-->B\nB.join: OR",
        registry=r,
    )
    be = SqliteBackend(tmp_path / "state.db")
    rn = Runner(g, r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=50, pause_at={3})
    snap = rn.snapshot()
    rn.run_until_idle(max_ticks=50)
    rn.restore(snap)
    assert "B" in rn.run_state.all_mutable_states()   # D5: state rebuilt
    assert rn.run_state.mutable_state("B")["attempts"] == 2   # 快照点的状态
    # 鉴别 DB 重建 vs 沿用快照 state：截断到 tick 1 后应为第 1 次触发的状态
    rn.run_state.truncate_after(1)
    assert rn.run_state.mutable_state("B")["attempts"] == 1


def test_cold_queries_dedup_replayed_rows(tmp_path):
    """重放产生的重复 (tick, node) 行不破坏第 k 次触发契约（keep-first）。"""
    be = SqliteBackend(tmp_path / "dup.db")
    be.save_firings("s1", [
        {"tick": 1, "node": "A", "output": "a1"},
        {"tick": 3, "node": "A", "output": "a2"},
        {"tick": 5, "node": "A", "output": "a3"},
        {"tick": 1, "node": "A", "output": "a1-replayed"},
        {"tick": 3, "node": "A", "output": "a2-replayed"},
    ])
    assert be.firing_at("s1", "A", 1) == "a1"     # 保留首见行
    assert be.firing_at("s1", "A", 3) == "a3"
    assert be.firing_at("s1", "A", 4) is None
    assert be.firings_of("s1", "A") == [(1, "a1"), (3, "a2"), (5, "a3")]
    jb = JsonBackend(tmp_path / "j")
    jb.save_firings("s1", [
        {"tick": 1, "node": "A", "output": "a1"},
        {"tick": 1, "node": "A", "output": "a1-replayed"},
    ])
    assert jb.firings_of("s1", "A") == [(1, "a1")]


def test_truncate_persistent_branch_direct(tmp_path):
    """直接驱动：持久路径 truncate 重建窗口/序号/ceiling/_state/audit，且幂等。

    不预 flush：truncate 内部的 flush 负责未落盘尾批入库（覆盖尾批场景）。
    """
    from tickflow.state import RunState, NodeState

    be = SqliteBackend(tmp_path / "trunc.db")
    rs = RunState(backend=be, session_id="s1", persistent=True)
    for tick, node, v in [
        (1, "A", "a1"), (2, "B", "b1"), (3, "A", "a2"),
        (4, "B", "b2"), (5, "A", "a3"), (6, "B", "b3"), (7, "A", "a4"),
    ]:
        rs.record(NodeState(tick=tick, node=node, output=v,
                            mutable_state={node: tick}))
    rs.truncate_after(5)          # 内部 flush 尾批（tick 6/7）后重建
    assert rs._edges["A"] == [(3, "a2"), (5, "a3")]   # 窗口：最近两条 ≤ 5
    assert rs._edges["B"] == [(2, "b1"), (4, "b2")]
    assert rs._fire_counts == {"A": 3, "B": 2}
    assert rs._audit_ceiling == 5
    assert rs.mutable_state("A") == {"A": 5}          # 从库重建：最后一次 ≤ 5
    assert rs.mutable_state("B") == {"B": 4}
    assert [ns.tick for ns in rs.audit()] == [1, 2, 3, 4, 5]
    # D5：落盘记录不因 truncate 丢失（库中全量保留）
    assert len(be.firings_of("s1", "A")) == 4
    assert len(be.firings_of("s1", "B")) == 3
    rs.truncate_after(5)                              # 幂等
    assert rs._audit_ceiling == 5
    assert rs._edges["A"] == [(3, "a2"), (5, "a3")]
