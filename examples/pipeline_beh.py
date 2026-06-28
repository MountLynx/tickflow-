"""Behaviours for pipeline.txt — three-stage pipe with A[k] index policy."""
from tickflow import registry
from tickflow.views import Missing


@registry.body("seed_value")
def _seed(v):
    return 7


@registry.body("transform")
def _transform(v):
    a_val = v.A.value if "A" in v else 0
    return a_val * 10 + 5



@registry.body("reference_first")
def _ref(v):
    # C reads A[1] (A's first fire) regardless of B's current output.
    a_first = v.A.value if "A" in v else Missing
    b_val = v.B.value if "B" in v else Missing
    return {"a_first": a_first, "b_current": b_val}
