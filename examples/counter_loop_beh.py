"""Behaviours for counter_loop.txt — value mode.

A is an OR-join over seed (bootstrap) and B (loop-back); ``passthru`` echoes
whichever has fired: seed on the first pass, B once the loop is running.
"""
from tickflow import registry
from tickflow.views import Missing


@registry.body("seed_zero")
def _seed():
    return 0


@registry.body("passthru")
def _passthru(B, seed):
    return B if B is not Missing else seed


@registry.body("incr")
def _incr(A):
    return A + 1


@registry.guard("cont_lt3")
def _cont(out):
    # Guard on edge out of B: sees B's current-tick output.
    return out < 3
