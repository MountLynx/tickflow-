"""Behaviours for state_machine.txt — approval workflow state machine."""
from tickflow import registry


@registry.body("submit_form")
def _submit(v):
    return {"form_id": 1, "data": "request"}


@registry.body("review_form")
def _review(v):
    # Reviewer decision based on form data.
    form = v.submit.value
    approved = form.get("data") == "request"
    v.state["decision"] = "approved" if approved else "rejected"
    return v.state["decision"]


@registry.body("approve_action")
def _approve(v):
    return {"action": "approved", "by": "reviewer"}


@registry.body("reject_action")
def _reject(v):
    return {"action": "rejected", "reason": "invalid"}


@registry.body("finalize")
def _finalize(v):
    # OR-join: only one branch fires, so one input will be Missing.
    from tickflow.views import Missing
    a = v.approve.value if "approve" in v else Missing
    r = v.reject.value if "reject" in v else Missing
    result = a if a is not Missing else r
    return {"final_result": result}


@registry.guard("approved")
def _guard_approved(v):
    # Guard reads the firing node's (review's) own output.
    return v.review.value == "approved"


@registry.guard("rejected")
def _guard_rejected(v):
    return v.review.value == "rejected"
