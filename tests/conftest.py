"""Shared test helpers: a fresh Registry per test with common bodies/guards."""
from __future__ import annotations

from tickflow import Registry
from tickflow.views import Missing


def make_registry() -> Registry:
    r = Registry()

    @r.body("seed_zero")
    def _seed(v):
        return 0

    @r.body("passthru")
    def _passthru(v):
        # Echo whichever declared producer has actually fired (not Missing).
        for name, val in v.items():
            if val is not Missing:
                return val
        return None

    @r.body("incr")
    def _incr(v):
        return v.A.value + 1

    @r.body("echo_first")
    def _echo(v):
        for name, val in v.items():
            return val
        return None

    @r.body("decide_a")
    def _decide_a(v):
        return "go_a"

    @r.body("merge_pick")
    def _merge(v):
        a = v.A.value if "A" in v else Missing
        d = v.D.value if "D" in v else Missing
        if a is not Missing:
            return ("A", a)
        return ("D", d)

    @r.guard("always_true")
    def _true(v):
        return True

    @r.guard("always_false")
    def _false(v):
        return False

    @r.guard("cont_lt3")
    def _cont(v):
        # Guard on edge out of B: see B's current-tick output.
        return v.B.value < 3

    @r.guard("go_a")
    def _go_a(v):
        return True

    @r.guard("go_d")
    def _go_d(v):
        return False

    return r
