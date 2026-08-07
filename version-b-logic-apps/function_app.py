import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any

import azure.functions as func
from azure.servicebus import ServiceBusClient, ServiceBusMessage


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


VALID_CATEGORIES = (
    "travel",
    "meals",
    "supplies",
    "equipment",
    "software",
    "other",
)

REQUIRED_FIELDS = (
    "employeeName",
    "employeeEmail",
    "amount",
    "category",
    "description",
    "managerEmail",
)

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def create_json_response(
    body: dict[str, Any],
    status_code: int = 200,
) -> func.HttpResponse:
    """Return a consistent JSON HTTP response."""

    return func.HttpResponse(
        body=json.dumps(body),
        status_code=status_code,
        mimetype="application/json",
    )


def validate_expense_payload(
    expense: Any,
) -> dict[str, Any]:
    """Validate and normalize an expense request."""

    if not isinstance(expense, dict):
        return {
            "valid": False,
            "errors": ["Expense request must be a JSON object."],
        }

    errors: list[str] = []

    for field in REQUIRED_FIELDS:
        if field not in expense:
            errors.append(f"Missing required field: {field}")

    string_fields = (
        "employeeName",
        "employeeEmail",
        "category",
        "description",
        "managerEmail",
    )

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

    category = str(
        expense.get("category", "")
    ).strip().lower()

    if category and category not in VALID_CATEGORIES:
        errors.append(
            "Invalid category. Valid categories are: "
            + ", ".join(VALID_CATEGORIES)
            + "."
        )

    for email_field in ("employeeEmail", "managerEmail"):
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


@app.route(
    route="expenses",
    methods=["POST"],
)
def submit_expense(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """
    Accept an expense request and place it in the Service Bus queue.

    Full business validation happens later through validate_expense,
    which is called by the Logic App.
    """

    try:
        expense = req.get_json()
    except ValueError:
        return create_json_response(
            {
                "error": "Request body must contain valid JSON."
            },
            status_code=400,
        )

    if not isinstance(expense, dict):
        return create_json_response(
            {
                "error": "Request body must be a JSON object."
            },
            status_code=400,
        )

    connection_string = os.getenv(
        "SERVICE_BUS_CONNECTION_STRING"
    )
    queue_name = os.getenv(
        "EXPENSE_QUEUE_NAME",
        "expense-requests",
    )

    if not connection_string:
        logging.error(
            "SERVICE_BUS_CONNECTION_STRING is not configured."
        )

        return create_json_response(
            {
                "error": "Service Bus configuration is missing."
            },
            status_code=500,
        )

    expense_id = str(uuid.uuid4())

    message_body = {
        "expenseId": expense_id,
        "submittedAtUtc": datetime.now(
            timezone.utc
        ).isoformat(),
        "expense": expense,
    }

    try:
        with ServiceBusClient.from_connection_string(
            connection_string
        ) as service_bus_client:

            with service_bus_client.get_queue_sender(
                queue_name=queue_name
            ) as sender:

                message = ServiceBusMessage(
                    json.dumps(message_body),
                    content_type="application/json",
                    message_id=expense_id,
                    application_properties={
                        "messageType": "expense-request"
                    },
                )

                sender.send_messages(message)

    except Exception:
        logging.exception(
            "Failed to send expense %s to Service Bus.",
            expense_id,
        )

        return create_json_response(
            {
                "error": "The expense could not be queued.",
                "expenseId": expense_id,
            },
            status_code=500,
        )

    logging.info(
        "Expense %s sent to queue %s.",
        expense_id,
        queue_name,
    )

    return create_json_response(
        {
            "message": "Expense request accepted.",
            "expenseId": expense_id,
            "queue": queue_name,
            "status": "queued",
        },
        status_code=202,
    )


@app.route(
    route="validate-expense",
    methods=["POST"],
)
def validate_expense(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """
    Validate an expense request.

    This endpoint accepts either:
    1. A direct expense object, or
    2. The Service Bus envelope containing expenseId and expense.
    """

    try:
        raw_body = req.get_body()

        logging.info(
            "Raw validation request body: %r",
            raw_body[:1000]
        )

        # Decode the actual HTTP request body ourselves.
        request_body = json.loads(
            raw_body.decode("utf-8-sig")
        )

        # Handles a JSON body that was accidentally serialized twice.
        if isinstance(request_body, str):
            request_body = json.loads(request_body)

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ) as exc:

        logging.error(
            "Could not parse validation JSON body: %s",
            exc,
        )

        return create_json_response(
            {
                "valid": False,
                "errors": [
                    "Request body must contain valid JSON."
                ],
            },
            status_code=400,
        )

    if not isinstance(request_body, dict):
        return create_json_response(
            {
                "valid": False,
                "errors": [
                    "Request body must be a JSON object."
                ],
            },
            status_code=400,
        )

    # The Logic App will normally send the complete queue envelope.
    if "expense" in request_body:
        expense = request_body.get("expense")
        expense_id = request_body.get("expenseId")
    else:
        expense = request_body
        expense_id = request_body.get("expenseId")

    validation_result = validate_expense_payload(expense)

    response_body = {
        "expenseId": expense_id,
        **validation_result,
    }

    logging.info(
        "Validation completed for expense %s. Valid=%s",
        expense_id,
        validation_result["valid"],
    )

    # Validation failures use HTTP 200 so the Logic App can evaluate
    # the "valid" property through a normal Condition action.
    return create_json_response(response_body)