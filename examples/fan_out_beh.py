"""Behaviours for fan_out.txt — parallel workers + AND-join merge."""
from tickflow import registry


@registry.body("seed_num")
def _seed():
    return 5


@registry.body("do_work")
def _work(source):
    return {"value": source * 10}


@registry.body("combine")
def _combine(worker_a, worker_b, worker_c):
    return {"a": worker_a, "b": worker_b, "c": worker_c}
