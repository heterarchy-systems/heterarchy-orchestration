from collections.abc import Callable
from typing import Annotated

from asyncer import asyncify
from fastapi import APIRouter, Depends
from starlette.responses import Response

from chattocodex.orchestrator_backend.control_panel.interface.http.control_panel_http_responses import (
    json_response as _json_response,
    provisioning_error_response as _provisioning_error_response,
)
from chattocodex.orchestrator_backend.control_panel.interface.http.fastapi_json_boundary import (
    build_strict_json_parser,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_dto import (
    ProvisioningRequestStatus,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_mapper import (
    request_view_from_state,
)
from chattocodex.orchestrator_backend.provisioning.worker_provisioning_service import (
    WorkerProvisioningService,
)
from chattocodex.shared.contracts.worker.worker_provisioning_schemas import (
    ProvisioningRequestId,
    WorkerProvisioningFailureRequest,
    WorkerProvisioningFailureResponse,
    WorkerProvisioningNextResponse,
    WorkerProvisioningProgressRequest,
    WorkerProvisioningRegistrationRequest,
    WorkerProvisioningRequestResponse,
    WorkerProvisioningRetryRequest,
    WorkerProvisioningStatusResponse,
)

WorkerProvisioningServiceResolver = Callable[[], WorkerProvisioningService]


_parse_registered = build_strict_json_parser(WorkerProvisioningRegistrationRequest)
_parse_failed = build_strict_json_parser(WorkerProvisioningFailureRequest)
_parse_progress = build_strict_json_parser(WorkerProvisioningProgressRequest)
_parse_retry = build_strict_json_parser(WorkerProvisioningRetryRequest)


def build_worker_provisioning_routes(
    resolve_service: WorkerProvisioningServiceResolver,
) -> APIRouter:
    """Build worker provisioning routes. Args: resolve_service. Returns: APIRouter."""

    async def worker_provisioning_next() -> Response:
        """Worker provisioning next. Args: None. Returns: Response."""
        try:
            provisioning = resolve_service()
            request_state = await asyncify(provisioning.admit_next)()
            return _json_response(
                model=WorkerProvisioningNextResponse(
                    request=(
                        request_view_from_state(request_state)
                        if request_state is not None
                        else None
                    )
                )
            )
        except OrchestrationError as error:
            return _provisioning_error_response(error=error)

    async def worker_provisioning_status() -> Response:
        """Worker provisioning status. Args: None. Returns: Response."""
        try:
            provisioning = resolve_service()
            demands, requests = await asyncify(provisioning.list_status)()
            return _json_response(
                model=WorkerProvisioningStatusResponse(
                    demands=demands,
                    requests=requests,
                )
            )
        except OrchestrationError as error:
            return _provisioning_error_response(error=error)

    async def worker_provisioning_registered(
        request_id: ProvisioningRequestId,
        payload: Annotated[
            WorkerProvisioningRegistrationRequest, Depends(_parse_registered)
        ],
    ) -> Response:
        """Worker provisioning registered. Args: request_id, payload. Returns: Response."""
        try:
            provisioning = resolve_service()
            state = await asyncify(provisioning.register_callback)(
                request_id=request_id,
                conversation_id=payload.conversation_id,
                wake_target_id=payload.wake_target_id,
                admission_revision=payload.admission_revision,
            )
            return _json_response(
                model=WorkerProvisioningRequestResponse(
                    request=request_view_from_state(state)
                )
            )
        except OrchestrationError as error:
            return _provisioning_error_response(error=error)

    async def worker_provisioning_failed(
        request_id: ProvisioningRequestId,
        payload: Annotated[WorkerProvisioningFailureRequest, Depends(_parse_failed)],
    ) -> Response:
        """Worker provisioning failed. Args: request_id, payload. Returns: Response."""
        try:
            provisioning = resolve_service()
            _state, retry_after_ms = await asyncify(provisioning.fail_callback)(
                request_id=request_id,
                admission_revision=payload.admission_revision,
                reason=payload.reason,
                rate_limited=payload.rate_limited,
            )
            return _json_response(
                model=WorkerProvisioningFailureResponse(retry_after_ms=retry_after_ms)
            )
        except OrchestrationError as error:
            return _provisioning_error_response(error=error)

    async def worker_provisioning_progress(
        request_id: ProvisioningRequestId,
        payload: Annotated[WorkerProvisioningProgressRequest, Depends(_parse_progress)],
    ) -> Response:
        """Worker provisioning progress. Args: request_id, payload. Returns: Response."""
        try:
            provisioning = resolve_service()
            state = await asyncify(provisioning.progress_request)(
                request_id=request_id,
                status=ProvisioningRequestStatus(payload.status),
                admission_revision=payload.admission_revision,
            )
            return _json_response(
                model=WorkerProvisioningRequestResponse(
                    request=request_view_from_state(state)
                )
            )
        except OrchestrationError as error:
            return _provisioning_error_response(error=error)

    async def worker_provisioning_retry(
        request_id: ProvisioningRequestId,
        payload: Annotated[WorkerProvisioningRetryRequest, Depends(_parse_retry)],
    ) -> Response:
        """Worker provisioning retry. Args: request_id, payload. Returns: Response."""
        try:
            provisioning = resolve_service()
            state = await asyncify(provisioning.retry_operator_blocked_request)(
                request_id=request_id,
                expected_revision=payload.expected_revision,
            )
            return _json_response(
                model=WorkerProvisioningRequestResponse(
                    request=request_view_from_state(state)
                )
            )
        except OrchestrationError as error:
            return _provisioning_error_response(error=error)

    router = APIRouter()
    router.add_api_route(
        "/api/worker-provisioning/next", worker_provisioning_next, methods=["POST"]
    )
    router.add_api_route(
        "/api/worker-provisioning/status", worker_provisioning_status, methods=["GET"]
    )
    router.add_api_route(
        "/api/worker-provisioning/{request_id}/registered",
        worker_provisioning_registered,
        methods=["POST"],
    )
    router.add_api_route(
        "/api/worker-provisioning/{request_id}/failed",
        worker_provisioning_failed,
        methods=["POST"],
    )
    router.add_api_route(
        "/api/worker-provisioning/{request_id}/progress",
        worker_provisioning_progress,
        methods=["POST"],
    )
    router.add_api_route(
        "/api/worker-provisioning/{request_id}/retry",
        worker_provisioning_retry,
        methods=["POST"],
    )
    return router
