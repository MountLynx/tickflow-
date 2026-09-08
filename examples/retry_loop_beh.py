"""Behaviours for retry_loop.txt — retry with a mutable-state counter.

B always returns a result dict; the guards inspect the result to decide
whether to loop or exit. Value mode: the guards receive B's output directly.
"""
from tickflow import registry


@registry.body("retry_task")
def _retry(B, start, *, state):
    attempt = state.get("attempts", 0) + 1
    state["attempts"] = attempt
    ok = attempt >= 3
    return {"attempt": attempt, "ok": ok}


@registry.body("finalize")
def _finalize(B):
    return {"result": B, "total_attempts": 3}


@registry.guard("should_retry")
def _retry_guard(out):
    # Loop while the result is not ok and attempts < 3.
    return not out["ok"] and out["attempt"] < 3


@registry.guard("give_up")
def _give_up(out):
    return out["ok"]
