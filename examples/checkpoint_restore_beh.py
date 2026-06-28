"""Checkpoint/restore/remap demo — full workflow test.

Run:
  python examples/checkpoint_restore_beh.py
"""
from tickflow import (
    parse, Runner, Registry, JsonBackend,
    Graph, Node, Edge, InputPolicy,
)
from tickflow.views import Missing


def main():
    r = Registry()

    @r.body("seed_zero")
    def _seed(v):
        return 0

    @r.body("passthru")
    def _p(v):
        for _n, val in v.items():
            if val is not Missing:
                return val
        return None

    @r.body("incr")
    def _incr(v):
        return v.A.value + 1

    r.guard("cont_lt5", lambda v: v.B.value < 5)

    graph_text = """
[seed]-->A
seed.body: seed_zero
A.body: passthru
A.join: OR
A-->B
B.body: incr
B--|cont_lt5|-->A
"""
    g = parse(graph_text, registry=r)

    import tempfile, os
    tmp = tempfile.mkdtemp()
    try:
        be = JsonBackend(tmp)
        rn = Runner(g, r, backend=be, session_id="demo")

        # 1. Run 3 ticks then checkpoint.
        rn.run_until_idle(max_ticks=20, pause_at={3})
        rn.checkpoint("tick3")
        print(f"1. Checkpoint at tick {rn.tick_count}")

        # 2. Run to completion.
        rn.run_until_idle(max_ticks=20)
        final_outputs = [f.output for f in rn.audit_log() if f.node == "B"]
        print(f"2. Full run B outputs: {final_outputs}")
        assert final_outputs == [1, 2, 3, 4, 5]

        # 3. Rollback to checkpoint.
        rn.rollback_to("tick3")
        print(f"3. Rolled back to tick {rn.tick_count}")

        # 4. Remap: change guard from cont_lt5 to cont_lt3 (shorter loop).
        new_r = Registry()
        new_r.body("seed_zero", lambda v: 0)
        new_r.body("passthru", _p)
        new_r.body("incr", _incr)
        new_r.guard("cont_lt3", lambda v: v.B.value < 3)

        new_g = parse(graph_text.replace("cont_lt5", "cont_lt3"), registry=new_r)
        rn.remap_graph(new_g, registry=new_r)
        print(f"4. Remapped graph (guard now cont_lt3)")

        # 5. Resume.
        rn.run_until_idle(max_ticks=20)
        b_after = [f.output for f in rn.audit_log() if f.node == "B" and f.tick >= 3]
        print(f"5. After remap B outputs from tick 3: {b_after}")
        # After rollback to tick 3, B's history has outputs at ticks 1.
        # With cont_lt3, B fires 2 more times: tick 4 (output 2), tick 6 (output 3).
        assert b_after == [2, 3], f"Expected [2, 3] (short loop), got {b_after}"
        assert rn.is_idle()

        print()
        print("=== checkpoint_restore PASSED ===")
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
