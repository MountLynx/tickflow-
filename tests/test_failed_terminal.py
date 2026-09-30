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


def test_starved_async_ends_failed():
    r = _reg()
    rn = AsyncRunner(_starved_graph(r), r)
    asyncio.run(rn.run_until_idle(max_ticks=10))
    assert rn.status == RunStatus.FAILED
    assert rn.is_terminal()
