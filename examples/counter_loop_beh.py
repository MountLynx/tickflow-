"""Behaviours for counter_loop.txt."""
from tickflow import registry
from tickflow.views import Missing


@registry.body("seed_zero")
def _seed(v):
    return 0


@registry.body("passthru")
def _passthru(v):
    # Echo whichever declared producer has actually fired.
    for _name, val in v.items():
        if val is not Missing:
            return val
    return None


@registry.body("incr")
def _incr(v):
    return v.A.value + 1


@registry.guard("cont_lt3")
def _cont(v):
    # Guard on edge out of B: sees B's current-tick output.
    return v.B.value < 3
