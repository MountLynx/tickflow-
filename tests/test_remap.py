"""Tests for graph remap (remap_graph): slot porting, validation, end-to-end."""
from __future__ import annotations

import pytest

from tickflow import parse, Runner, Registry
from tickflow.ir import Graph, Node, Edge, InputPolicy
from tickflow.views import Missing


# -- helpers ---------------------------------------------------------------


def _chain_reg():
    r = Registry()
    r.body("seed_zero", lambda v: 0)

    @r.body("passthru")
    def _p(v):
        for _n, val in v.items():
            if val is not Missing:
                return val
        return None

    return r


def _chain_graph(r: Registry):
    """[seed]-->A-->B  (linear chain, 3 nodes, 2 edges)."""
    return parse(
        "[seed]-->A\nseed.body: seed_zero\nA.body: passthru\nA-->B\nB.body: passthru",
        registry=r,
    )


def _loop_reg():
    """Registry for a self-loop counter graph."""
    r = Registry()
    r.body("seed_zero", lambda v: 0)

    @r.body("incr")
    def _incr(v):
        # A may not have fired yet — handle Missing.
        cur = v.A.value if v.A.value is not Missing else 0
        return cur + 1

    r.guard("lt5", lambda v: v.A.value < 5)

    @r.body("passthru")
    def _p(v):
        for _n, val in v.items():
            if val is not Missing:
                return val
        return None

    return r


def _loop_graph(r: Registry):
    """[seed]-->A  with A self-looping via guard lt5 (A fires repeatedly)."""
    return parse(
        "[seed]-->A\nseed.body: seed_zero\nA.body: incr\nA.join: OR\nA--|lt5|-->A",
        registry=r,
    )


# -- construct graphs via IR (bypass parser validation) --------------------


def _e(src: str, dst: str, guard: str | None = None) -> Edge:
    return Edge(src=src, dst=dst, guard=guard)


def _g(nodes: list[Node], edges: list[Edge]) -> Graph:
    return Graph(nodes={n.name: n for n in nodes}, edges=edges)


# -- basic slot porting ----------------------------------------------------


def test_remap_add_edge_slot_starts_false():
    """New edge added during remap gets a slot initialised to False."""
    r = _chain_reg()
    rn = Runner(_chain_graph(r), r)
    rn.run_until_idle(max_ticks=20, pause_at={2})  # paused after A, before B

    g_new = _g(
        [
            Node("seed", is_start=True, body="seed_zero"),
            Node("A", body="passthru", inputs={"seed": InputPolicy.latest()}),
            Node("B", body="passthru"),
            Node("C", body="passthru"),
        ],
        [_e("seed", "A"), _e("A", "B"), _e("A", "C")],
    )
    rn.remap_graph(g_new)
    # Existing slots preserved.
    assert rn.marking.slots[("B", "A")] is True    # A produced, not consumed
    # New slot starts False (old A firing didn't retroactively set it).
    assert rn.marking.slots[("C", "A")] is False


def test_remap_preserve_true_token():
    """A True slot on an unchanged edge stays True after remap."""
    r = _chain_reg()
    rn = Runner(_chain_graph(r), r)
    rn.run_until_idle(max_ticks=20, pause_at={2})  # seed + A fired
    assert rn.marking.slots[("B", "A")] is True

    g_new = _g(
        [
            Node("seed", is_start=True, body="seed_zero"),
            Node("A", body="passthru", inputs={"seed": InputPolicy.latest()}),
            Node("B", body="passthru"),
            Node("X", body="passthru"),
        ],
        [_e("seed", "A"), _e("A", "B"), _e("A", "X")],
    )
    rn.remap_graph(g_new)
    assert rn.marking.slots[("B", "A")] is True      # unchanged edge
    assert rn.marking.slots[("X", "A")] is False     # new edge


def test_remap_remove_edge():
    """Removed edge's slot is discarded; other slots unaffected."""
    r = _chain_reg()
    rn = Runner(_chain_graph(r), r)
    rn.run_until_idle(max_ticks=20)
    assert rn.is_terminal()

    g_new = _g(
        [
            Node("seed", is_start=True, body="seed_zero"),
            Node("A", body="passthru", inputs={"seed": InputPolicy.latest()}),
            Node("B", body="passthru"),
        ],
        [_e("seed", "A")],  # A-->B removed
    )
    rn.remap_graph(g_new)
    assert ("A", "seed") in rn.marking.slots
    assert ("B", "A") not in rn.marking.slots


def test_remap_swaps_armed_starts():
    """armed_starts becomes intersection of old armed and new start set."""
    r = _chain_reg()
    g = _g([Node("seed", is_start=True, body="seed_zero")], [])
    rn = Runner(g, r)
    assert "seed" in rn.marking.armed_starts

    g_new = _g([Node("seed", is_start=False, body="seed_zero")], [])
    rn.remap_graph(g_new)
    assert "seed" not in rn.marking.armed_starts


def test_remap_history_node_becomes_start_raises():
    """A node that already has history cannot become a start: armed_starts are
    one-shot, so it would silently never re-fire. remap_graph must raise."""
    r = _chain_reg()
    rn = Runner(_chain_graph(r), r)
    rn.run_until_idle(max_ticks=20)  # seed, A, B all have history now
    assert "seed" in rn.run_state.edges  # has history

    # Make 'seed' (which has history) ... it's already a start, so use A:
    # A has history (fired), and is not a start. Promote it to start -> error.
    g_new = _g(
        [
            Node("seed", is_start=True, body="seed_zero"),
            Node("A", is_start=True, body="passthru", inputs={"seed": InputPolicy.latest()}),
            Node("B", body="passthru"),
        ],
        [_e("seed", "A"), _e("A", "B")],
    )
    with pytest.raises(ValueError, match="became a start"):
        rn.remap_graph(g_new)


# -- registry interaction -------------------------------------------------


def test_remap_with_registry():
    """remap_graph(new_graph, registry=new_reg) swaps both in one call."""
    r = _chain_reg()
    rn = Runner(_chain_graph(r), r)

    r_new = _chain_reg()
    r_new.body("seed_zero", lambda v: 42)

    g_new = _g(
        [
            Node("seed", is_start=True, body="seed_zero"),
            Node("A", body="passthru", inputs={"seed": InputPolicy.latest()}),
            Node("B", body="passthru"),
        ],
        [_e("seed", "A"), _e("A", "B")],
    )
    rn.remap_graph(g_new, registry=r_new)
    assert rn.registry is r_new
    rn.run_until_idle(max_ticks=20)
    assert rn.last_output("seed") == 42


def test_remap_missing_body_raises():
    """remap_graph raises ValueError for an unregistered body in new graph."""
    r = _chain_reg()
    rn = Runner(_chain_graph(r), r)

    g_new = _g(
        [Node("seed", is_start=True, body="seed_zero"), Node("A", body="nonexistent")],
        [_e("seed", "A")],
    )
    with pytest.raises(ValueError, match="nonexistent"):
        rn.remap_graph(g_new)


def test_remap_missing_guard_raises():
    """remap_graph raises ValueError for an unregistered guard in new graph."""
    r = _chain_reg()
    r.guard("ok", lambda v: True)
    rn = Runner(_chain_graph(r), r)

    g_new = _g(
        [
            Node("seed", is_start=True, body="seed_zero"),
            Node("A", body="passthru", inputs={"seed": InputPolicy.latest()}),
            Node("B", body="passthru"),
        ],
        [_e("seed", "A"), Edge(src="A", dst="B", guard="bad_guard")],
    )
    with pytest.raises(ValueError, match="bad_guard"):
        rn.remap_graph(g_new)


# -- end-to-end: add edge mid-run, source fires again, new node fires -----


def test_remap_add_edge_source_refires():
    """When the source node will fire again, the new edge gets a True token
    and the downstream fires."""
    r = _loop_reg()
    rn = Runner(_loop_graph(r), r)
    # Tick 0: seed fires. Tick 1: A fires (output=1). Pause at tick 2.
    # A's self-loop guard lt5(1) → True, so (A,A)=True; A fires again.
    rn.run_until_idle(max_ticks=20, pause_at={2})
    assert rn.tick_count == 2
    assert rn.marking.slots[("A", "A")] is True

    # Add edge A-->B (B is new). (B,A) starts False.
    g_new = _g(
        [
            Node("seed", is_start=True, body="seed_zero"),
            Node("A", body="incr", join="OR", inputs={"A": InputPolicy.latest()}),
            Node("B", body="passthru"),
        ],
        [_e("seed", "A"), Edge(src="A", dst="A", guard="lt5"), _e("A", "B")],
    )
    rn.remap_graph(g_new)
    assert rn.marking.slots[("B", "A")] is False

    # A fires again → Phase B sets (B,A)=True → B fires.
    rn.run_until_idle(max_ticks=20)
    b_fired = any(f.node == "B" for f in rn.audit_log())
    assert b_fired


def test_rollback_remap_resume():
    """Full workflow: run→checkpoint→rollback→remap→resume with new edge."""
    from tickflow.persistence import NullBackend

    r = _loop_reg()
    be = NullBackend()
    rn = Runner(_loop_graph(r), r, backend=be, session_id="s1")
    rn.run_until_idle(max_ticks=20, pause_at={2})
    rn.checkpoint("safe")

    g_new = _g(
        [
            Node("seed", is_start=True, body="seed_zero"),
            Node("A", body="incr", join="OR", inputs={"A": InputPolicy.latest()}),
            Node("B", body="passthru"),
        ],
        [_e("seed", "A"), Edge(src="A", dst="A", guard="lt5"), _e("A", "B")],
    )
    rn.rollback_to("safe")
    rn.remap_graph(g_new)

    assert rn.tick_count == 2
    assert "B" in rn.graph.nodes
    assert rn.marking.slots[("B", "A")] is False

    rn.run_until_idle(max_ticks=20)
    b_fired = any(f.node == "B" for f in rn.audit_log())
    assert b_fired


# -- update body via registry on same structure ---------------------------


def test_remap_same_structure_new_body():
    """Replace only the registry, keeping graph structure identical."""
    r = _chain_reg()
    rn = Runner(_chain_graph(r), r)

    r_new = _chain_reg()
    r_new.body("seed_zero", lambda v: 99)

    g_new = _g(
        [
            Node("seed", is_start=True, body="seed_zero"),
            Node("A", body="passthru", inputs={"seed": InputPolicy.latest()}),
            Node("B", body="passthru"),
        ],
        [_e("seed", "A"), _e("A", "B")],
    )
    rn.remap_graph(g_new, registry=r_new)
    rn.run_until_idle(max_ticks=20)
    assert rn.last_output("seed") == 99
    assert rn.last_output("A") == 99


# -- AsyncRunner remap -----------------------------------------------------


def test_async_remap_graph():
    """AsyncRunner.remap_graph mirrors sync Runner."""
    import asyncio
    from tickflow.async_runner import AsyncRunner

    async def _run():
        r = _chain_reg()
        rn = AsyncRunner(_chain_graph(r), r)
        await rn.run_until_idle(max_ticks=20, pause_at={2})

        g_new = _g(
            [
                Node("seed", is_start=True, body="seed_zero"),
                Node("A", body="passthru", inputs={"seed": InputPolicy.latest()}),
                Node("B", body="passthru"),
                Node("C", body="passthru"),
            ],
            [_e("seed", "A"), _e("A", "B"), _e("A", "C")],
        )
        rn.remap_graph(g_new)
        assert ("C", "A") in rn.marking.slots
        assert rn.marking.slots[("B", "A")] is True
        assert rn.marking.slots[("C", "A")] is False

    asyncio.run(_run())
