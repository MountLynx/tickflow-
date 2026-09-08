"""Behaviours for xor_merge.txt. Importing this module registers bodies and
guards on the default tickflow.registry."""
from tickflow import registry
from tickflow.views import Missing


@registry.body("decide")
def _decide(start):
    # In a real graph this would inspect inputs; here it always routes to A.
    return "go_a"


@registry.body("merge")
def _merge(A, D):
    return ("A", A) if A is not Missing else ("D", D)


@registry.guard("go_a")
def _go_a(out):
    return True


@registry.guard("go_d")
def _go_d(out):
    return False
