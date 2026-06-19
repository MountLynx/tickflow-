"""Tests for named checkpoints: save/list/rollback via backend."""
from __future__ import annotations

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


def test_checkpoint_requires_backend():
    r = _reg()
    rn = Runner(_loop_graph(r), r)
    with pytest.raises(RuntimeError):
        rn.checkpoint("after_prep")


def test_checkpoint_save_and_list():
    r = _reg()
    be = NullBackend()
    rn = Runner(_loop_graph(r), r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=20, pause_at={3})
    rn.checkpoint("after_3")
    rn.run_until_idle(max_ticks=20, pause_at={5})
    rn.checkpoint("after_5")
    cps = rn.list_checkpoints()
    assert ("after_3", 3) in cps
    assert ("after_5", 5) in cps


def test_rollback_to_checkpoint_restores_state():
    r = _reg()
    be = NullBackend()
    rn = Runner(_loop_graph(r), r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=20, pause_at={3})
    rn.checkpoint("cp3")
    rn.run_until_idle(max_ticks=20)  # finish
    final_audit = [(f.tick, f.node, f.output) for f in rn.audit]
    # Rollback to cp3 and replay.
    rn.rollback_to("cp3")
    assert rn.tick_count == 3
    rn.run_until_idle(max_ticks=20)
    replayed = [(f.tick, f.node, f.output) for f in rn.audit]
    assert replayed == final_audit


def test_rollback_to_missing_label_raises():
    r = _reg()
    be = NullBackend()
    rn = Runner(_loop_graph(r), r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=5, pause_at={3})
    with pytest.raises(KeyError):
        rn.rollback_to("nope")


def test_json_backend_checkpoints(tmp_path):
    r = _reg()
    be = JsonBackend(tmp_path)
    rn = Runner(_loop_graph(r), r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=20, pause_at={3})
    rn.checkpoint("cp3")
    # checkpoints.json written.
    cp_file = tmp_path / "s1" / "checkpoints.json"
    assert cp_file.exists()
    import json
    data = json.loads(cp_file.read_text(encoding="utf-8"))
    assert "cp3" in data
    assert data["cp3"]["tick"] == 3
    # list_checkpoints via backend.
    assert ("cp3", 3) in be.list_checkpoints("s1")
    # load_checkpoint.
    snap = be.load_checkpoint("s1", "cp3")
    assert snap["tick"] == 3


def test_checkpoint_overwrites_same_label():
    r = _reg()
    be = NullBackend()
    rn = Runner(_loop_graph(r), r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=20, pause_at={2})
    rn.checkpoint("cp")
    rn.run_until_idle(max_ticks=20, pause_at={5})
    rn.checkpoint("cp")  # overwrite
    cps = dict(rn.list_checkpoints())
    assert cps["cp"] == 5
