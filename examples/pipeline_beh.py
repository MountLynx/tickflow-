"""Behaviours for pipeline.txt — three-stage pipe with A[k] index policy.

C reads A[1] (A's first fire) regardless of B's current output; B is C's
producer (token flow) but is not declared as a read input, so value-mode C
only receives A[1].
"""
from tickflow import registry


@registry.body("seed_value")
def _seed():
    return 7


@registry.body("transform")
def _transform(A):
    return A * 10 + 5


@registry.body("reference_first")
def _ref(A):
    # C reads A[1] (A's first fire) regardless of B's current output.
    return {"a_first": A}
