import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import azure.functions as func
import azure.durable_functions as df


# Durable Functions application using the Python v2 programming model.
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


VALID_CATEGORIES = {
    "travel",
    "meals",
    "supplies",
    "equipment",
    "software",
    "other",
}

REQUIRED_FIELDS = {
    "employeeName",
    "employeeEmail",
    "amount",
    "category",
    "description",
    "managerEmail",
}

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def json_response(
    body: dict[str, Any],
    status_code: int = 200,
) -> func.HttpResponse:
    """Create a JSON HTTP response."""

    return func.HttpResponse(
        body=json.dumps(body),
        status_code=status_code,
        mimetype="application/json",
    )


def get_timeout_seconds() -> int:
    """Read and validate the local approval timeout setting."""

    raw_value = os.getenv("APPROVAL_TIMEOUT_SECONDS", "30")

    try:
        timeout = int(raw_value)
    except ValueError:
        logging.warning(
            "Invalid APPROVAL_TIMEOUT_SECONDS value. Using 30 seconds."
        )
        timeout = 30

    return max(timeout, 1)


# ---------------------------------------------------------------------------
# HTTP CLIENT FUNCTION
# ---------------------------------------------------------------------------

@app.route(route="expenses", methods=["POST"])
@app.durable_client_input(client_name="client")
async def submit_expense(
    req: func.HttpRequest,
    client: df.DurableOrchestrationClient,
) -> func.HttpResponse:
    """
    Start a new expense approval orchestration.

    The request is not validated here because validation must be performed
    by an activity function as part of the orchestrated workflow.
    """

    try:
        expense = req.get_json()
    except ValueError:
        return json_response(
            {
                "error": "Request body must contain valid JSON."
            },
            status_code=400,
        )

    if not isinstance(expense, dict):
        return json_response(
            {
                "error": "Request body must be a JSON object."
            },
            status_code=400,
        )

    workflow_input = {
        "expense": expense,
        # The value becomes part of the stored orchestration input, making
        # the timeout value consistent during orchestration replay.
        "timeoutSeconds": get_timeout_seconds(),
    }

    instance_id = await client.start_new(
        "expense_orchestrator",
        client_input=workflow_input,
    )

    logging.info(
        "Started expense orchestration with instance ID %s.",
        instance_id,
    )

    return client.create_check_status_response(req, instance_id)


# ---------------------------------------------------------------------------
# MANAGER DECISION HTTP ENDPOINT
# ---------------------------------------------------------------------------

@app.route(
    route="expenses/{instanceId}/decision",
    methods=["POST"],
)
@app.durable_client_input(client_name="client")
async def manager_decision(
    req: func.HttpRequest,
    client: df.DurableOrchestrationClient,
) -> func.HttpResponse:
    """
    Simulate a manager approving or rejecting an expense.

    Expected JSON:
    {
        "decision": "approve",
        "decidedBy": "Manager Name",
        "comments": "Optional comments"
    }
    """

    instance_id = req.route_params.get("instanceId")

    if not instance_id:
        return json_response(
            {"error": "The orchestration instance ID is required."},
            status_code=400,
        )

    try:
        request_body = req.get_json()
    except ValueError:
        return json_response(
            {"error": "Request body must contain valid JSON."},
            status_code=400,
        )

    if not isinstance(request_body, dict):
        return json_response(
            {"error": "Request body must be a JSON object."},
            status_code=400,
        )

    decision = str(request_body.get("decision", "")).strip().lower()

    if decision not in {"approve", "reject"}:
        return json_response(
            {
                "error": (
                    "Decision must be either 'approve' or 'reject'."
                )
            },
            status_code=400,
        )

    orchestration_status = await client.get_status(instance_id)

    if orchestration_status is None:
        return json_response(
            {
                "error": (
                    f"No orchestration was found with ID '{instance_id}'."
                )
            },
            status_code=404,
        )

    event_data = {
        "decision": decision,
        "decidedBy": str(
            request_body.get("decidedBy", "Manager")
        ).strip(),
        "comments": str(
            request_body.get("comments", "")
        ).strip(),
    }

    await client.raise_event(
        instance_id,
        "ManagerDecision",
        event_data,
    )

    logging.info(
        "Manager decision '%s' sent to orchestration %s.",
        decision,
        instance_id,
    )

    return json_response(
        {
            "message": "Manager decision submitted.",
            "instanceId": instance_id,
            "decision": decision,
        },
        status_code=202,
    )


# ---------------------------------------------------------------------------
# ORCHESTRATOR FUNCTION
# ---------------------------------------------------------------------------

@app.orchestration_trigger(context_name="context")
def expense_orchestrator(
    context: df.DurableOrchestrationContext,
):
    """
    Coordinate validation, approval, timeout, processing, and notification.

    Orchestrator functions must remain deterministic. External work such as
    sending email and logging business actions is delegated to activities.
    """

    workflow_input = context.get_input() or {}
    expense = workflow_input.get("expense", {})
    timeout_seconds = int(
        workflow_input.get("timeoutSeconds", 30)
    )

    context.set_custom_status(
        {
            "stage": "validating",
            "message": "Validating expense request.",
        }
    )

    validation_result = yield context.call_activity(
        "validate_expense",
        expense,
    )

    if not validation_result["valid"]:
        context.set_custom_status(
            {
                "stage": "validation_failed",
                "errors": validation_result["errors"],
            }
        )

        return {
            "instanceId": context.instance_id,
            "finalOutcome": "validation_error",
            "approved": False,
            "escalated": False,
            "errors": validation_result["errors"],
        }

    normalized_expense = validation_result["expense"]

    # Expenses below $100 do not require manager approval.
    if normalized_expense["amount"] < 100:
        context.set_custom_status(
            {
                "stage": "processing",
                "message": "Expense automatically approved.",
            }
        )

        decision_result = {
            "finalOutcome": "approved",
            "approved": True,
            "escalated": False,
            "decisionSource": "automatic",
            "managerDecision": None,
        }

    else:
        context.set_custom_status(
            {
                "stage": "requesting_manager_approval",
                "message": "Creating manager approval request.",
            }
        )

        yield context.call_activity(
            "request_manager_approval",
            {
                "instanceId": context.instance_id,
                "expense": normalized_expense,
                "timeoutSeconds": timeout_seconds,
            },
        )

        deadline = (
            context.current_utc_datetime
            + timedelta(seconds=timeout_seconds)
        )

        manager_event_task = context.wait_for_external_event(
            "ManagerDecision"
        )
        timeout_task = context.create_timer(deadline)

        context.set_custom_status(
            {
                "stage": "waiting_for_manager",
                "instanceId": context.instance_id,
                "timeoutSeconds": timeout_seconds,
                "managerDecisionEndpoint": (
                    f"/api/expenses/{context.instance_id}/decision"
                ),
            }
        )

        winning_task = yield context.task_any(
            [manager_event_task, timeout_task]
        )

        if winning_task == manager_event_task:
            # The manager answered before the timer expired.
            timeout_task.cancel()

            raw_manager_response = manager_event_task.result

            # Depending on serialization, the external-event payload can arrive
            # either as a dictionary or as a JSON string.
            if isinstance(raw_manager_response, dict):
                manager_response = raw_manager_response

            elif isinstance(raw_manager_response, str):
                try:
                    parsed_response = json.loads(raw_manager_response)

                    if isinstance(parsed_response, dict):
                        manager_response = parsed_response
                    else:
                        # Handles a simple JSON string such as "approve".
                        manager_response = {
                            "decision": str(parsed_response)
                        }

                except json.JSONDecodeError:
                    # Handles an unquoted plain string such as approve.
                    manager_response = {
                        "decision": raw_manager_response
                    }

            else:
                manager_response = {}

            manager_choice = str(
                manager_response.get("decision", "")
            ).strip().lower()

            if manager_choice not in {"approve", "reject"}:
                raise ValueError(
                    f"Invalid manager decision received: "
                    f"{raw_manager_response!r}"
                )

            if manager_choice == "approve":
                decision_result = {
                    "finalOutcome": "approved",
                    "approved": True,
                    "escalated": False,
                    "decisionSource": "manager",
                    "managerDecision": manager_response,
                }
            else:
                decision_result = {
                    "finalOutcome": "rejected",
                    "approved": False,
                    "escalated": False,
                    "decisionSource": "manager",
                    "managerDecision": manager_response,
                }
        else:
            # Assignment rule: timeout means automatically approved and
            # flagged as escalated.
            decision_result = {
                "finalOutcome": "escalated",
                "approved": True,
                "escalated": True,
                "decisionSource": "timeout_auto_approval",
                "managerDecision": None,
            }

    context.set_custom_status(
        {
            "stage": "processing_outcome",
            "outcome": decision_result["finalOutcome"],
        }
    )

    processed_result = yield context.call_activity(
        "process_outcome",
        {
            "instanceId": context.instance_id,
            "expense": normalized_expense,
            "decision": decision_result,
        },
    )

    context.set_custom_status(
        {
            "stage": "notifying_employee",
            "outcome": processed_result["finalOutcome"],
        }
    )

    notification_result = yield context.call_activity(
        "notify_employee",
        processed_result,
    )

    processed_result["notification"] = notification_result

    context.set_custom_status(
        {
            "stage": "completed",
            "outcome": processed_result["finalOutcome"],
        }
    )

    return processed_result


# ---------------------------------------------------------------------------
# VALIDATION ACTIVITY
# ---------------------------------------------------------------------------

@app.activity_trigger(input_name="expense")
def validate_expense(
    expense: dict[str, Any],
) -> dict[str, Any]:
    """Validate and normalize an expense request."""

    logging.info("Validating expense request.")

    if not isinstance(expense, dict):
        return {
            "valid": False,
            "errors": ["Expense request must be a JSON object."],
        }

    errors: list[str] = []

    for field in REQUIRED_FIELDS:
        if field not in expense:
            errors.append(f"Missing required field: {field}")

    string_fields = [
        "employeeName",
        "employeeEmail",
        "category",
        "description",
        "managerEmail",
    ]

    for field in string_fields:
        if field in expense:
            value = expense[field]

            if not isinstance(value, str) or not value.strip():
                errors.append(
                    f"Field '{field}' must be a non-empty string."
                )

    normalized_amount: float | None = None

    if "amount" in expense:
        raw_amount = expense["amount"]

        if isinstance(raw_amount, bool):
            errors.append("Field 'amount' must be numeric.")
        else:
            try:
                normalized_amount = float(raw_amount)

                if normalized_amount <= 0:
                    errors.append(
                        "Field 'amount' must be greater than zero."
                    )

            except (TypeError, ValueError):
                errors.append("Field 'amount' must be numeric.")

    category = str(expense.get("category", "")).strip().lower()

    if category and category not in VALID_CATEGORIES:
        errors.append(
            "Invalid category. Valid categories are: "
            + ", ".join(sorted(VALID_CATEGORIES))
            + "."
        )

    for email_field in ["employeeEmail", "managerEmail"]:
        email_value = expense.get(email_field)

        if (
            isinstance(email_value, str)
            and email_value.strip()
            and not EMAIL_PATTERN.match(email_value.strip())
        ):
            errors.append(
                f"Field '{email_field}' must contain a valid email."
            )

    if errors:
        logging.warning(
            "Expense validation failed: %s",
            errors,
        )

        return {
            "valid": False,
            "errors": errors,
        }

    normalized_expense = {
        "employeeName": expense["employeeName"].strip(),
        "employeeEmail": expense["employeeEmail"].strip(),
        "amount": round(normalized_amount or 0, 2),
        "category": category,
        "description": expense["description"].strip(),
        "managerEmail": expense["managerEmail"].strip(),
    }

    return {
        "valid": True,
        "errors": [],
        "expense": normalized_expense,
    }


# ---------------------------------------------------------------------------
# MANAGER APPROVAL REQUEST ACTIVITY
# ---------------------------------------------------------------------------

@app.activity_trigger(input_name="approval_request")
def request_manager_approval(
    approval_request: dict[str, Any],
) -> dict[str, Any]:
    """
    Simulate creating a manager approval request.

    The manager responds through the manager_decision HTTP endpoint.
    """

    expense = approval_request["expense"]
    instance_id = approval_request["instanceId"]

    logging.info(
        (
            "Manager approval requested. Instance=%s, Manager=%s, "
            "Employee=%s, Amount=$%.2f"
        ),
        instance_id,
        expense["managerEmail"],
        expense["employeeName"],
        expense["amount"],
    )

    logging.info(
        "Decision endpoint: /api/expenses/%s/decision",
        instance_id,
    )

    return {
        "requested": True,
        "instanceId": instance_id,
        "managerEmail": expense["managerEmail"],
        "timeoutSeconds": approval_request["timeoutSeconds"],
    }


# ---------------------------------------------------------------------------
# PROCESS OUTCOME ACTIVITY
# ---------------------------------------------------------------------------

@app.activity_trigger(input_name="processing_input")
def process_outcome(
    processing_input: dict[str, Any],
) -> dict[str, Any]:
    """Create the final expense result."""

    expense = processing_input["expense"]
    decision = processing_input["decision"]
    final_outcome = decision["finalOutcome"]

    if final_outcome == "approved":
        outcome_message = (
            "Your expense request has been approved."
        )

    elif final_outcome == "rejected":
        outcome_message = (
            "Your expense request has been rejected by your manager."
        )

    else:
        outcome_message = (
            "No manager decision was received before the deadline. "
            "The expense was automatically approved and flagged as "
            "escalated."
        )

    result = {
        "instanceId": processing_input["instanceId"],
        "employeeName": expense["employeeName"],
        "employeeEmail": expense["employeeEmail"],
        "managerEmail": expense["managerEmail"],
        "amount": expense["amount"],
        "category": expense["category"],
        "description": expense["description"],
        "finalOutcome": final_outcome,
        "approved": decision["approved"],
        "escalated": decision["escalated"],
        "decisionSource": decision["decisionSource"],
        "managerDecision": decision["managerDecision"],
        "message": outcome_message,
        "processedAtUtc": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    logging.info(
        "Expense %s processed with outcome '%s'.",
        result["instanceId"],
        final_outcome,
    )

    return result


# ---------------------------------------------------------------------------
# EMPLOYEE NOTIFICATION ACTIVITY
# ---------------------------------------------------------------------------

@app.activity_trigger(input_name="result")
def notify_employee(
    result: dict[str, Any],
) -> dict[str, Any]:
    """
    Notify the employee.

    In log mode, the email is simulated locally.
    In acs mode, Azure Communication Services Email sends the message.
    """

    subject = (
        f"Expense request result: "
        f"{result['finalOutcome'].upper()}"
    )

    plain_text = (
        f"Hello {result['employeeName']},\n\n"
        f"{result['message']}\n\n"
        f"Amount: ${result['amount']:.2f}\n"
        f"Category: {result['category']}\n"
        f"Description: {result['description']}\n"
        f"Reference: {result['instanceId']}\n"
    )

    notification_mode = os.getenv(
        "NOTIFICATION_MODE",
        "log",
    ).strip().lower()

    if notification_mode != "acs":
        logging.info(
            "SIMULATED EMAIL\nTo: %s\nSubject: %s\n\n%s",
            result["employeeEmail"],
            subject,
            plain_text,
        )

        return {
            "mode": "log",
            "status": "simulated",
            "recipient": result["employeeEmail"],
            "subject": subject,
        }

    connection_string = os.getenv(
        "COMMUNICATION_SERVICES_CONNECTION_STRING"
    )
    sender_address = os.getenv("EMAIL_SENDER_ADDRESS")

    if not connection_string or not sender_address:
        raise ValueError(
            "ACS email configuration is missing. Set "
            "COMMUNICATION_SERVICES_CONNECTION_STRING and "
            "EMAIL_SENDER_ADDRESS."
        )

    # Imported inside the activity so the orchestrator remains free of
    # network operations.
    from azure.communication.email import EmailClient

    email_client = EmailClient.from_connection_string(
        connection_string
    )

    email_message = {
        "senderAddress": sender_address,
        "recipients": {
            "to": [
                {
                    "address": result["employeeEmail"],
                    "displayName": result["employeeName"],
                }
            ]
        },
        "content": {
            "subject": subject,
            "plainText": plain_text,
        },
    }

    poller = email_client.begin_send(email_message)
    email_result = poller.result()

    return {
        "mode": "acs",
        "status": email_result.get("status"),
        "messageId": email_result.get("id"),
        "recipient": result["employeeEmail"],
        "subject": subject,
    }