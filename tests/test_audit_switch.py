"""Tests for the keep_records switch: turning off in-memory audit while
keeping firings.jsonl backend persistence."""
from __future__ import annotations

import asyncio

import pytest

from tickflow import parse, Runner, Registry, JsonBackend
from tickflow.async_runner import AsyncRunner
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


def test_keep_records_true_default():
    r = _reg()
    rn = Runner(_loop_graph(r), r)
    assert rn.run_state.keep_records is True
    rn.run_until_idle(max_ticks=50)
    assert len(rn.audit_log()) > 0


def test_keep_records_false_keeps_audit_empty():
    r = _reg()
    rn = Runner(_loop_graph(r), r, keep_records=False)
    rn.run_until_idle(max_ticks=50)
    assert rn.audit_log() == []


def test_keep_records_false_still_persists_firings(tmp_path):
    r = _reg()
    be = JsonBackend(tmp_path)
    rn = Runner(_loop_graph(r), r, backend=be, session_id="s1", keep_records=False)
    rn.run_until_idle(max_ticks=50)
    # In-memory audit empty.
    assert rn.audit_log() == []
    # But backend has all firings.
    persisted = be.list_firings("s1")
    assert len(persisted) > 0
    # And snapshots were saved.
    assert be.latest_tick("s1") == rn.tick_count


def test_keep_records_false_to_json_has_empty_audit():
    r = _reg()
    rn = Runner(_loop_graph(r), r, keep_records=False)
    rn.run_until_idle(max_ticks=10)
    import json
    d = json.loads(rn.to_json())
    # Records key absent when audit disabled.
    assert "records" not in d["snapshot"]["run_state"]
    # snapshot still present and has fireable.
    assert "fireable" in d["snapshot"]


def test_keep_records_false_restore_keeps_empty():
    r = _reg()
    be = NullBackend()
    rn = Runner(_loop_graph(r), r, backend=be, session_id="s1", keep_records=False)
    rn.run_until_idle(max_ticks=50, pause_at={3})
    snap = rn.snapshot()
    rn.run_until_idle(max_ticks=50)
    rn.restore(snap)
    assert rn.audit_log() == []


def test_async_runner_keep_records_false():
    r = _reg()
    be = NullBackend()
    rn = AsyncRunner(_loop_graph(r), r, backend=be, session_id="s1", keep_records=False)
    asyncio.run(rn.run_until_idle(max_ticks=50))
    assert rn.audit_log() == []
    # Backend still has firings.
    assert len(be.list_firings("s1")) > 0


def test_async_runner_to_json_roundtrip():
    r = _reg()
    rn = AsyncRunner(_loop_graph(r), r)
    asyncio.run(rn.run_until_idle(max_ticks=50, pause_at={3}))
    s = rn.to_json()
    rn2 = AsyncRunner.from_json(s, _loop_graph(_reg()), _reg())
    assert rn2.tick_count == rn.tick_count
    assert [f.tick for f in rn2.audit_log()] == [f.tick for f in rn.audit_log()]


def test_async_runner_audit_json():
    r = _reg()
    rn = AsyncRunner(_loop_graph(r), r)
    asyncio.run(rn.run_until_idle(max_ticks=10))
    import json
    d = json.loads(rn.audit_json())
    assert isinstance(d, list)
    assert len(d) == len(rn.audit_log())
