"""Behaviours for state_machine.txt — approval workflow state machine."""
from tickflow import registry
from tickflow.views import Missing


@registry.body("submit_form")
def _submit(start):
    return {"form_id": 1, "data": "request"}


@registry.body("review_form")
def _review(submit, *, state):
    # Reviewer decision based on form data.
    approved = submit.get("data") == "request"
    state["decision"] = "approved" if approved else "rejected"
    return state["decision"]


@registry.body("approve_action")
def _approve(review):
    return {"action": "approved", "by": "reviewer"}


@registry.body("reject_action")
def _reject(review):
    return {"action": "rejected", "reason": "invalid"}


@registry.body("finalize")
def _finalize(approve, reject):
    # OR-join: only one branch fires, so one input will be Missing.
    result = approve if approve is not Missing else reject
    return {"final_result": result}


@registry.guard("approved")
def _guard_approved(out):
    # Guard reads the firing node's (review's) own output.
    return out == "approved"


@registry.guard("rejected")
def _guard_rejected(out):
    return out == "rejected"
