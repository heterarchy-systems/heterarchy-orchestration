import asyncio

from pydantic import ValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from chattocodex.main_runtime.main_review_project_catalog import (
    MainReviewProjectCatalog,
)
from chattocodex.main_runtime.main_review_target_coordinator import (
    MainReviewTargetCoordinator,
)
from chattocodex.main_runtime.main_review_view_mapper import (
    build_main_review_target_view,
)
from chattocodex.orchestration.orchestration_exceptions import OrchestrationError
from chattocodex.project_runtime.project_binding_service import ProjectBindingService
from chattocodex.schemas.control_panel_schemas import ControlPanelErrorResponse
from chattocodex.schemas.main_runtime_schemas import (
    MainReviewTargetPollRequest,
    MainReviewTargetRegisterRequest,
    MainReviewTargetResponse,
)


class MainReviewRouteAdapter:
    def __init__(
        self,
        *,
        target_coordinator: MainReviewTargetCoordinator,
        project_catalog: MainReviewProjectCatalog,
        project_binding_service: ProjectBindingService,
    ) -> None:
        self._target_coordinator = target_coordinator
        self._project_catalog = project_catalog
        self._project_binding_service = project_binding_service

    async def register_target(self, request: Request) -> Response:
        try:
            payload = MainReviewTargetRegisterRequest.model_validate_json(
                await request.body()
            )
            project_id = await asyncio.to_thread(
                self._project_catalog.require_project_id,
                project_id=payload.project_id,
            )
            await asyncio.to_thread(
                self._project_binding_service.require_connector_binding,
                project_id=project_id,
            )
            target = await asyncio.to_thread(
                self._target_coordinator.register_target,
                main_target_id=payload.main_target_id,
                project_id=project_id,
            )
            return _json_response(
                model=MainReviewTargetResponse(
                    target=build_main_review_target_view(state=target)
                )
            )
        except ValidationError as error:
            return _error_response(
                error_code="validation_error",
                message=_validation_message(error=error),
                recoverable=True,
                status_code=422,
            )
        except OrchestrationError as error:
            return _orchestration_error_response(error=error)

    async def poll_target(self, request: Request) -> Response:
        try:
            payload = MainReviewTargetPollRequest(
                main_target_id=str(request.path_params["main_target_id"])
            )
            target = await asyncio.to_thread(
                self._target_coordinator.poll_target,
                main_target_id=payload.main_target_id,
            )
            return _json_response(
                model=MainReviewTargetResponse(
                    target=build_main_review_target_view(state=target)
                )
            )
        except ValidationError as error:
            return _error_response(
                error_code="validation_error",
                message=_validation_message(error=error),
                recoverable=True,
                status_code=422,
            )
        except OrchestrationError as error:
            return _orchestration_error_response(error=error)

    async def heartbeat_chat(self, request: Request) -> Response:
        try:
            payload = MainReviewTargetPollRequest(
                main_target_id=str(request.path_params["main_target_id"])
            )
            target = await asyncio.to_thread(
                self._target_coordinator.heartbeat_chat,
                main_target_id=payload.main_target_id,
            )
            return _json_response(
                model=MainReviewTargetResponse(
                    target=build_main_review_target_view(state=target)
                )
            )
        except ValidationError as error:
            return _error_response(
                error_code="validation_error",
                message=_validation_message(error=error),
                recoverable=True,
                status_code=422,
            )
        except OrchestrationError as error:
            return _orchestration_error_response(error=error)


def _json_response(*, model) -> JSONResponse:
    return JSONResponse(
        model.model_dump(mode="json"),
        headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
    )


def _orchestration_error_response(*, error: OrchestrationError) -> JSONResponse:
    return _error_response(
        error_code=error.error_code,
        message=error.message,
        recoverable=error.recoverable,
        status_code=409 if error.recoverable else 400,
    )


def _error_response(
    *,
    error_code: str,
    message: str,
    recoverable: bool,
    status_code: int,
) -> JSONResponse:
    payload = ControlPanelErrorResponse(
        error_code=error_code,
        message=message,
        recoverable=recoverable,
    )
    return JSONResponse(
        payload.model_dump(mode="json"),
        status_code=status_code,
        headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
    )


def _validation_message(*, error: ValidationError) -> str:
    first = error.errors()[0]
    location = ".".join(str(part) for part in first.get("loc", ()))
    message = str(first.get("msg", "Validation failed."))
    return f"{location}: {message}" if location else message
