"""Behaviours for retry_loop.txt — retry with mutable_state counter.

Design: B always returns a result dict. The guard inspects the result to
decide whether to loop or exit. Mutable state tracks the attempt count.
(A failed node writes False to ALL out-edges, so Failure can't be used
for controllable retry loops — the guard is never consulted.)
"""
from tickflow import registry


@registry.body("retry_task")
def _retry(v):
    attempt = v.state.get("attempts", 0) + 1
    v.state["attempts"] = attempt
    ok = attempt >= 3
    return {"attempt": attempt, "ok": ok}


@registry.body("finalize")
def _finalize(v):
    return {"result": v.B.value, "total_attempts": 3}


@registry.guard("should_retry")
def _retry_guard(v):
    # Loop while the result is not ok and attempts < 3.
    out = v.B.value
    ok = isinstance(out, dict) and out.get("ok", False)
    return not ok and v.state.get("attempts", 0) < 3


@registry.guard("give_up")
def _give_up(v):
    out = v.B.value
    return isinstance(out, dict) and out.get("ok", False)
