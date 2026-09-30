"""Tests for the FAILED terminal state: a starved run (pending work, nothing
fireable) must end FAILED, not IDLE. Spec: SpecModule
2026-09-30-tickflow-failed-terminal-design.md."""
from __future__ import annotations

import asyncio

import pytest

from tickflow import parse, Runner, Registry, RunStatus, Failure
from tickflow.async_runner import AsyncRunner


def _reg():
    r = Registry()

    @r.body("ok")
    def _ok(v):
        return "ok"

    @r.body("fail_llm")
    def _fllm(v):
        return Failure("bad output", type="llm")

    @r.body("bump")
    def _bump(v):
        v.state["n"] = v.state.get("n", 0) + 1
        return v.state["n"]

    r.guard("lt3", lambda out: out < 3)
    return r


def _starved_graph(r):
    # A ok -> slot (C,A)=True; B llm-fail -> slot (C,B)=False.
    # AND-join C waits forever for (C,B): pending but nothing fireable.
    return parse(
        "[A]-->C\n[B]-->C\nC.join: AND\nA.body: ok\nB.body: fail_llm",
        registry=r,
    )


def test_starved_sync_ends_failed():
    r = _reg()
    rn = Runner(_starved_graph(r), r)
    rn.run_until_idle(max_ticks=10)
    assert rn.status == RunStatus.FAILED
    assert rn.is_terminal()
    assert not rn.is_idle()
    # FAILED 已在 _TERMINAL：后续 tick 空操作。
    before = len(rn.audit_log())
    rn.tick()
    assert len(rn.audit_log()) == before


def test_starved_status_set_inside_tick_before_break():
    # 状态判定发生在 tick 内、run_until_idle 空 tick break 读取之前：
    # 首个空 tick 返回 [] 时 status 已是 FAILED。
    r = _reg()
    rn = Runner(_starved_graph(r), r)
    rn.tick()                   # tick 0: A ok + B fail_llm（双双点火）
    assert rn.status == RunStatus.RUNNING
    firings = rn.tick()         # tick 1: 空 tick
    assert firings == []
    assert rn.status == RunStatus.FAILED


def test_starved_status_set_inside_tick_before_break_async():
    # sync 版的 async 对偶：空 tick 返回 [] 时 status 已在 tick 内定为 FAILED。
    r = _reg()
    rn = AsyncRunner(_starved_graph(r), r)

    async def _drive():
        await rn.tick()             # tick 0: A ok + B fail_llm
        assert rn.status == RunStatus.RUNNING
        firings = await rn.tick()   # tick 1: 空 tick
        assert firings == []
        assert rn.status == RunStatus.FAILED

    asyncio.run(_drive())


def test_starved_async_ends_failed():
    r = _reg()
    rn = AsyncRunner(_starved_graph(r), r)
    asyncio.run(rn.run_until_idle(max_ticks=10))
    assert rn.status == RunStatus.FAILED
    assert rn.is_terminal()


# --- 不误伤面：语义矩阵其余各行不受状态判定改动影响 -----------------------


def test_full_completion_still_idle():
    # 全部工作完成（无 pending）→ IDLE（Module 层照旧映射 done）。
    r = _reg()
    g = parse("[A]-->B\nA.body: ok\nB.body: ok", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=10)
    assert rn.status == RunStatus.IDLE
    assert not rn._has_pending()
    assert rn.is_terminal()          # IDLE 且无事可做仍是终局


def test_or_join_loop_exit_stays_idle():
    # OR-join repair 环（producer 条件到场，波间衔接无空 tick）：守卫关闭
    # 循环边后所有槽位 False → 空 tick → IDLE 而非 FAILED。
    r = _reg()
    g = parse(
        "[P]-->W\nP.body: ok\nW.body: bump\nW--|lt3|-->W\nW.join: OR",
        registry=r,
    )
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=50)
    assert rn.status == RunStatus.IDLE
    w_outputs = [f.output for f in rn.audit_log() if f.node == "W"]
    assert w_outputs == [1, 2, 3]


def test_or_join_loop_exit_stays_idle_async():
    r = _reg()
    g = parse(
        "[P]-->W\nP.body: ok\nW.body: bump\nW--|lt3|-->W\nW.join: OR",
        registry=r,
    )
    rn = AsyncRunner(g, r)
    asyncio.run(rn.run_until_idle(max_ticks=50))
    assert rn.status == RunStatus.IDLE


def test_failed_upstream_false_slot_still_idle():
    # 失败上游写 False ≠ pending：下游被跳过，照旧 IDLE
    #（与 tests/test_failure.py::test_llm_failure_writes_false_downstream 互为锚）。
    r = _reg()
    g = parse("[A]-->B\nA.body: fail_llm\nB.body: ok", registry=r)
    rn = Runner(g, r)
    rn.run_until_idle(max_ticks=10)
    assert rn.status == RunStatus.IDLE


def test_max_ticks_cutoff_stays_running():
    # max_ticks 耗尽停在 RUNNING（Module 层映射 truncated，可 resume），
    # 不经过空 tick 判定——不得变成 FAILED/IDLE。
    r = _reg()
    rn = Runner(_starved_graph(r), r)
    rn.run_until_idle(max_ticks=1)     # tick 0 点火 A、B 后即达上限
    assert rn.status == RunStatus.RUNNING


def test_pause_breaks_before_status_judgement():
    # pause 在 tick 前 break，不进状态判定。
    r = _reg()
    rn = Runner(_starved_graph(r), r)
    rn.run_until_idle(max_ticks=10, pause_at={1})
    assert rn.status == RunStatus.RUNNING


def test_cancel_before_run_and_after_failed():
    r = _reg()
    rn = Runner(_starved_graph(r), r)
    rn.cancel("user")
    assert rn.status == RunStatus.CANCELLED
    assert rn.cancel_reason == "user"
    rn2 = Runner(_starved_graph(r), r)
    rn2.run_until_idle(max_ticks=10)
    assert rn2.status == RunStatus.FAILED
    rn2.cancel("late")
    assert rn2.status == RunStatus.FAILED   # 终态 cancel 是 no-op


def test_reset_from_failed_back_to_idle():
    r = _reg()
    rn = Runner(_starved_graph(r), r)
    rn.run_until_idle(max_ticks=10)
    assert rn.status == RunStatus.FAILED
    rn.reset()
    assert rn.status == RunStatus.IDLE


def test_restore_failed_snapshot_resumes_as_idle():
    # restore() 把终态快照重置回 IDLE（既有 resume 语义），快照本身带 failed。
    r = _reg()
    rn = Runner(_starved_graph(r), r)
    rn.run_until_idle(max_ticks=10)
    snap = rn.snapshot()
    assert snap["status"] == "failed"
    rn.restore(snap)
    assert rn.status == RunStatus.IDLE
