"""Behaviours for xor_merge.txt. Importing this module registers bodies and
guards on the default tickflow.registry."""
from tickflow import registry
from tickflow.views import Missing


@registry.body("decide")
def _decide(v):
    # In a real graph this would inspect inputs; here it always routes to A.
    return "go_a"


@registry.body("merge")
def _merge(v):
    a = v.A.value if "A" in v else Missing
    d = v.D.value if "D" in v else Missing
    if a is not Missing:
        return ("A", a)
    return ("D", d)


@registry.guard("go_a")
def _go_a(v):
    return True


@registry.guard("go_d")
def _go_d(v):
    return False
