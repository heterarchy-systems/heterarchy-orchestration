from collections.abc import Callable
from typing import Annotated

from asyncer import asyncify
from fastapi import APIRouter, Depends
from starlette.responses import Response

from chattocodex.orchestrator_backend.control_panel.interface.http.control_panel_http_responses import (
    json_response,
    orchestration_error_response,
)
from chattocodex.orchestrator_backend.control_panel.interface.http.fastapi_json_boundary import (
    build_strict_json_parser,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
)
from chattocodex.orchestrator_backend.wake.wake_control_service import (
    WakeControlService,
)
from chattocodex.orchestrator_backend.wake.wake_dto import WakeTargetRetirement
from chattocodex.orchestrator_backend.wake.wake_view_mapper import (
    build_wake_target_view,
)
from chattocodex.shared.contracts.wake.wake_schemas import (
    WakeProjectsResponse,
    WakeProjectView,
    WakeTargetId,
    WakeTargetRegisterRequest,
    WakeTargetResponse,
    WakeTargetRetireBatchRequest,
    WakeTargetRetireBatchResponse,
    WakeTargetRetireRequest,
    WakeTargetRetireResponse,
)

WakeServiceResolver = Callable[[], WakeControlService]
_parse_register = build_strict_json_parser(WakeTargetRegisterRequest)
_parse_retire = build_strict_json_parser(WakeTargetRetireRequest)
_parse_retire_batch = build_strict_json_parser(WakeTargetRetireBatchRequest)


def build_wake_routes(resolve_service: WakeServiceResolver) -> APIRouter:
    """Build wake routes. Args: resolve_service. Returns: APIRouter."""

    async def wake_projects() -> Response:
        """Wake projects. Args: None. Returns: Response."""
        try:
            projects = await asyncify(resolve_service().list_projects)()
            return json_response(
                model=WakeProjectsResponse(
                    projects=tuple(
                        WakeProjectView(
                            project_id=project.project_id, name=project.name
                        )
                        for project in projects
                    )
                )
            )
        except OrchestrationError as error:
            return orchestration_error_response(error=error)

    async def wake_register(
        payload: Annotated[WakeTargetRegisterRequest, Depends(_parse_register)],
    ) -> Response:
        """Wake register. Args: payload. Returns: Response."""
        try:
            target = await asyncify(resolve_service().register_target)(
                wake_target_id=payload.wake_target_id,
                project_id=payload.project_id,
            )
            return json_response(
                model=WakeTargetResponse(target=build_wake_target_view(state=target))
            )
        except OrchestrationError as error:
            return orchestration_error_response(error=error)

    async def wake_target(wake_target_id: WakeTargetId) -> Response:
        """Wake target. Args: wake_target_id. Returns: Response."""
        try:
            target = await asyncify(resolve_service().get_target)(
                wake_target_id=wake_target_id,
            )
            return json_response(
                model=WakeTargetResponse(target=build_wake_target_view(state=target))
            )
        except OrchestrationError as error:
            return orchestration_error_response(error=error)

    async def wake_runtime_heartbeat(wake_target_id: WakeTargetId) -> Response:
        """Wake runtime heartbeat. Args: wake_target_id. Returns: Response."""
        try:
            target = await asyncify(resolve_service().runtime_heartbeat_target)(
                wake_target_id=wake_target_id,
            )
            return json_response(
                model=WakeTargetResponse(target=build_wake_target_view(state=target))
            )
        except OrchestrationError as error:
            return orchestration_error_response(error=error)

    async def wake_retire(
        wake_target_id: WakeTargetId,
        payload: Annotated[WakeTargetRetireRequest, Depends(_parse_retire)],
    ) -> Response:
        """Wake retire. Args: wake_target_id, payload. Returns: Response."""
        try:
            retired = await asyncify(resolve_service().retire_unclaimed_target)(
                wake_target_id=wake_target_id,
                expected_revision=payload.expected_revision,
            )
            return json_response(
                model=WakeTargetRetireResponse(
                    wake_target_id=retired.wake_target_id,
                    retired_revision=retired.revision,
                )
            )
        except OrchestrationError as error:
            return orchestration_error_response(error=error)

    async def wake_retire_batch(
        payload: Annotated[
            WakeTargetRetireBatchRequest,
            Depends(_parse_retire_batch),
        ],
    ) -> Response:
        """Wake retire batch. Args: payload. Returns: Response."""
        try:
            retired_ids = await asyncify(
                resolve_service().retire_unclaimed_targets_batch
            )(
                project_id=payload.project_id,
                targets=tuple(
                    WakeTargetRetirement(
                        wake_target_id=item.wake_target_id,
                        expected_revision=item.expected_revision,
                    )
                    for item in payload.targets
                ),
            )
            return json_response(
                model=WakeTargetRetireBatchResponse(
                    project_id=payload.project_id,
                    retired_wake_target_ids=retired_ids,
                )
            )
        except OrchestrationError as error:
            return orchestration_error_response(error=error)

    router = APIRouter()
    router.add_api_route("/api/wake/projects", wake_projects, methods=["GET"])
    router.add_api_route("/api/wake/targets/register", wake_register, methods=["POST"])
    router.add_api_route(
        "/api/wake/targets/retire-batch", wake_retire_batch, methods=["POST"]
    )
    router.add_api_route(
        "/api/wake/targets/{wake_target_id}", wake_target, methods=["GET"]
    )
    router.add_api_route(
        "/api/wake/targets/{wake_target_id}/runtime-heartbeat",
        wake_runtime_heartbeat,
        methods=["POST"],
    )
    router.add_api_route(
        "/api/wake/targets/{wake_target_id}/retire",
        wake_retire,
        methods=["POST"],
    )
    return router
