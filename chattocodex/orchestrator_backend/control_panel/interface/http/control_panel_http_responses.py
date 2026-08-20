from fastapi import Response
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ValidationError

from chattocodex.orchestrator_backend.control_panel.domain.control_panel_exceptions import (
    ControlPanelError,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
)
from chattocodex.shared.contracts.control_panel.control_panel_schemas import (
    ControlPanelErrorResponse,
)


def json_response(model: BaseModel) -> Response:
    """Serialize a typed Pydantic model at the explicit HTTP response seam. Args: model. Returns: Response."""
    return Response(
        content=model.model_dump_json(),
        media_type="application/json",
        headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
    )


def validation_error_response(error: ValidationError) -> Response:
    """Validation error response. Args: error. Returns: Response."""
    return error_response(
        error_code="validation_error",
        message=validation_message(error=error),
        recoverable=True,
        status_code=422,
    )


def request_validation_error_response(error: RequestValidationError) -> Response:
    """Request validation error response. Args: error. Returns: Response."""
    return error_response(
        error_code="validation_error",
        message=_request_validation_message(error=error),
        recoverable=True,
        status_code=422,
    )


def control_panel_error_response(error: ControlPanelError) -> Response:
    """Control panel error response. Args: error. Returns: Response."""
    return error_response(
        error_code=error.error_code,
        message=error.message,
        recoverable=error.recoverable,
        status_code=409 if error.recoverable else 400,
    )


def orchestration_error_response(error: OrchestrationError) -> Response:
    """Orchestration error response. Args: error. Returns: Response."""
    return error_response(
        error_code=error.error_code,
        message=error.message,
        recoverable=error.recoverable,
        status_code=409 if error.recoverable else 400,
    )


def project_binding_error_response(error: OrchestrationError) -> Response:
    """Project binding error response. Args: error. Returns: Response."""
    status_code = (
        404
        if error.error_code.value == "not_found"
        else (409 if error.recoverable else 400)
    )
    return error_response(
        error_code=error.error_code,
        message=error.message,
        recoverable=error.recoverable,
        status_code=status_code,
    )


def provisioning_error_response(error: OrchestrationError) -> Response:
    """Provisioning error response. Args: error. Returns: Response."""
    return error_response(
        error_code=error.error_code,
        message=error.message,
        recoverable=error.recoverable,
        status_code=409 if error.recoverable else 400,
    )


def error_response(
    error_code: str,
    message: str,
    recoverable: bool,
    status_code: int,
) -> Response:
    """Error response. Args: error_code, message, recoverable, status_code. Returns: Response."""
    payload = ControlPanelErrorResponse(
        error_code=error_code,
        message=message,
        recoverable=recoverable,
    )
    return Response(
        content=payload.model_dump_json(),
        status_code=status_code,
        media_type="application/json",
        headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
    )


def validation_message(error: ValidationError) -> str:
    """Validation message. Args: error. Returns: str."""
    details = error.errors(include_url=False)
    if not details:
        return "Invalid request."
    message = details[0].get("msg")
    return str(message) if message else "Invalid request."


def _request_validation_message(error: RequestValidationError) -> str:
    """Request validation message. Args: error. Returns: str."""
    details = error.errors()
    if not details:
        return "Invalid request."
    message = details[0].get("msg")
    return str(message) if message else "Invalid request."
