"""Demonstrates keep_records=False: no detailed audit, but mutable_state persists.

Run:
  python examples/keep_records_false_beh.py
"""
from tickflow import Graph, Node, Edge, InputPolicy, Registry, Runner


def main():
    r = Registry()

    @r.body("accumulator")
    def _acc(v):
        total = v.state.get("total", 0)
        # Self-loop: A fires every tick, always increments by 1.
        total += 1
        v.state["total"] = total
        return total

    g = Graph()
    g.nodes["start"] = Node(name="start", is_start=True)
    g.nodes["A"] = Node(
        name="A", body="accumulator", join="OR",
        inputs={"start": InputPolicy.latest(), "A": InputPolicy.latest()},
    )
    g.edges = [
        Edge(src="start", dst="A", guard=None),
        Edge(src="A", dst="A", guard=None),  # self-loop, fires every tick
    ]

    # -- Mode 1: keep_records=True (default) --
    r1 = Runner(g, r, strict_deadlock=False, keep_records=True)
    r1.run_until_idle(max_ticks=5)
    print(f"keep_records=True:  audit={len(r1.audit_log())} records, "
          f"A.state={r1.run_state.mutable_state('A')}")

    # -- Mode 2: keep_records=False (memory saving) --
    r2 = Runner(g, r, strict_deadlock=False, keep_records=False)
    r2.run_until_idle(max_ticks=5)
    print(f"keep_records=False: audit={len(r2.audit_log())} records, "
          f"A.state={r2.run_state.mutable_state('A')}")

    # Mutable state persists even with keep_records=False.
    assert r2.run_state.mutable_state("A")["total"] == r2.last_output("A")
    # Audit is empty.
    assert r2.audit_log() == []
    # input resolution still works.
    assert r2.last_output("A") is not None

    # Snapshot differences: records only present when keep_records=True.
    snap1 = r1.snapshot()
    snap2 = r2.snapshot()
    assert "records" in snap1["run_state"]   # keep_records=True → records present
    assert "records" not in snap2["run_state"]  # keep_records=False → records absent
    # Both always have edges + state.
    for snap in (snap1, snap2):
        assert "edges" in snap["run_state"]
        assert "state" in snap["run_state"]

    print(f"  keep_records=True:  snapshot has {len(snap1['run_state']['records'])} records")
    print(f"  keep_records=False: snapshot has no records key")

    print()
    print("=== keep_records_false PASSED ===")


if __name__ == "__main__":
    main()
