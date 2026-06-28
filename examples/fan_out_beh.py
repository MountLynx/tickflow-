"""Behaviours for fan_out.txt — parallel workers + AND-join merge."""
from tickflow import registry


@registry.body("seed_num")
def _seed(v):
    return 5


@registry.body("do_work")
def _work(v):
    seed = v.source.value if "source" in v else 1
    return {"worker": v.node, "value": seed * 10}


@registry.body("combine")
def _combine(v):
    return {
        "a": v.worker_a.value,
        "b": v.worker_b.value,
        "c": v.worker_c.value,
    }
