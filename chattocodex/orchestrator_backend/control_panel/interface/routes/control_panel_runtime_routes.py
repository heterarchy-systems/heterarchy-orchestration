import asyncio
from collections.abc import Callable
from typing import Annotated

from asyncer import asyncify
from fastapi import APIRouter, Depends
from starlette.responses import Response

from chattocodex.orchestrator_backend.broker_client.broker_operations_gateway import (
    BrokerOperationsGateway,
)
from chattocodex.orchestrator_backend.control_panel.application.control_panel_mapper import (
    build_control_panel_status,
)
from chattocodex.orchestrator_backend.control_panel.application.control_panel_operations_service import (
    ControlPanelOperationsService,
)
from chattocodex.orchestrator_backend.control_panel.application.control_panel_service import (
    ControlPanelService,
)
from chattocodex.orchestrator_backend.control_panel.composition.runtime_boundary_provider import (
    RuntimeBoundaryProvider,
)
from chattocodex.orchestrator_backend.control_panel.domain.control_panel_exceptions import (
    ControlPanelError,
    RuntimeLifecycleError,
)
from chattocodex.orchestrator_backend.control_panel.infrastructure.macos_folder_picker import (
    choose_workspace_folder,
)
from chattocodex.orchestrator_backend.control_panel.interface.http.control_panel_http_responses import (
    control_panel_error_response,
    json_response,
    orchestration_error_response,
)
from chattocodex.orchestrator_backend.control_panel.interface.http.fastapi_json_boundary import (
    build_strict_json_parser,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_service import (
    ProjectBindingService,
)
from chattocodex.orchestrator_backend.wake.wake_control_service import (
    WakeControlService,
)
from chattocodex.shared.contracts.control_panel.control_panel_operations_schemas import (
    ControlPanelOperationsResponse,
)
from chattocodex.shared.contracts.control_panel.control_panel_schemas import (
    ControlPanelPreferencesRequest,
    FolderPickerResponse,
    OwnerTokenResponse,
)

WakeServiceResolver = Callable[[], WakeControlService]
ProjectBindingServiceResolver = Callable[[], ProjectBindingService]
_parse_preferences = build_strict_json_parser(ControlPanelPreferencesRequest)


def build_runtime_routes(
    service: ControlPanelService,
    runtime_boundary_provider: RuntimeBoundaryProvider | None,
    broker_operations: BrokerOperationsGateway | None,
    resolve_wake_service: WakeServiceResolver,
    resolve_project_binding_service: ProjectBindingServiceResolver,
) -> APIRouter:
    """Build runtime routes. Args: service, runtime_boundary_provider, broker_operations, resolve_wake_service, resolve_project_binding_service. Returns: APIRouter."""

    async def status() -> Response:
        """Status. Args: None. Returns: Response."""
        return json_response(
            model=build_control_panel_status(snapshot=await service.snapshot())
        )

    async def save_preferences(
        payload: Annotated[
            ControlPanelPreferencesRequest,
            Depends(_parse_preferences),
        ],
    ) -> Response:
        """Save preferences. Args: payload. Returns: Response."""
        try:
            snapshot = await service.save_preferences(request=payload)
            return json_response(model=build_control_panel_status(snapshot=snapshot))
        except ControlPanelError as error:
            return control_panel_error_response(error=error)

    async def start_runtime() -> Response:
        """Start runtime. Args: None. Returns: Response."""
        try:
            snapshot = await service.start()
            return json_response(model=build_control_panel_status(snapshot=snapshot))
        except ControlPanelError as error:
            return control_panel_error_response(error=error)

    async def stop_runtime() -> Response:
        """Stop runtime. Args: None. Returns: Response."""
        try:
            snapshot = await service.stop()
            return json_response(model=build_control_panel_status(snapshot=snapshot))
        except ControlPanelError as error:
            return control_panel_error_response(error=error)

    async def rotate_owner_token() -> Response:
        """Rotate owner token. Args: None. Returns: Response."""
        try:
            token = await service.rotate_owner_token()
            return json_response(model=OwnerTokenResponse(owner_token=token))
        except ControlPanelError as error:
            return control_panel_error_response(error=error)

    async def folder_picker() -> Response:
        """Folder picker. Args: None. Returns: Response."""
        try:
            selected = await choose_workspace_folder()
            return json_response(
                model=FolderPickerResponse(
                    selected_path=str(selected) if selected is not None else None
                )
            )
        except ControlPanelError as error:
            return control_panel_error_response(error=error)

    async def operations() -> Response:
        """Operations. Args: None. Returns: Response."""
        try:
            return json_response(model=await operations_snapshot())
        except OrchestrationError as error:
            return orchestration_error_response(error=error)

    async def worker_browser_setup() -> Response:
        """Worker browser setup. Args: None. Returns: Response."""
        try:
            await service.start_worker_browser_setup()
            return json_response(model=await operations_snapshot())
        except ControlPanelError as error:
            return control_panel_error_response(error=error)
        except OrchestrationError as error:
            return orchestration_error_response(error=error)

    async def worker_browser_start() -> Response:
        """Worker browser start. Args: None. Returns: Response."""
        try:
            await service.start_worker_browser_headless()
            return json_response(model=await operations_snapshot())
        except ControlPanelError as error:
            return control_panel_error_response(error=error)
        except OrchestrationError as error:
            return orchestration_error_response(error=error)

    async def worker_browser_stop() -> Response:
        """Worker browser stop. Args: None. Returns: Response."""
        try:
            await service.stop_worker_browser()
            return json_response(model=await operations_snapshot())
        except ControlPanelError as error:
            return control_panel_error_response(error=error)
        except OrchestrationError as error:
            return orchestration_error_response(error=error)

    async def operations_snapshot() -> ControlPanelOperationsResponse:
        """Operations snapshot. Args: None. Returns: ControlPanelOperationsResponse."""
        if runtime_boundary_provider is None:
            raise RuntimeLifecycleError(
                message="Operations routes require the workspace runtime boundary provider."
            )
        boundaries = runtime_boundary_provider.current()
        projects, bindings = await asyncio.gather(
            asyncify(resolve_wake_service().list_projects)(),
            asyncify(resolve_project_binding_service().list_bindings)(),
        )
        operations_service = ControlPanelOperationsService(
            execution_root=boundaries.execution_root,
            broker_operations=broker_operations,
        )
        source_snapshot = await asyncify(operations_service.snapshot)(
            projects=projects,
            bindings=bindings,
            browser_running=service.worker_browser_running(),
            browser_mode=service.worker_browser_mode(),
            browser_profile_initialized=service.worker_browser_profile_initialized(),
        )
        return await service.project_operations_read_model(snapshot=source_snapshot)

    router = APIRouter()
    router.add_api_route("/api/status", status, methods=["GET"])
    router.add_api_route("/api/preferences", save_preferences, methods=["POST"])
    router.add_api_route("/api/start", start_runtime, methods=["POST"])
    router.add_api_route("/api/stop", stop_runtime, methods=["POST"])
    router.add_api_route(
        "/api/owner-token/rotate", rotate_owner_token, methods=["POST"]
    )
    router.add_api_route("/api/folder-picker", folder_picker, methods=["POST"])
    router.add_api_route("/api/operations", operations, methods=["GET"])
    router.add_api_route(
        "/api/worker-browser/setup", worker_browser_setup, methods=["POST"]
    )
    router.add_api_route(
        "/api/worker-browser/start", worker_browser_start, methods=["POST"]
    )
    router.add_api_route(
        "/api/worker-browser/stop", worker_browser_stop, methods=["POST"]
    )
    return router
