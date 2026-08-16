import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from pydantic import BaseModel, TypeAdapter, ValidationError
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from chattocodex.control_panel.control_panel_exceptions import (
    ControlPanelError,
    RuntimeLifecycleError,
)
from chattocodex.control_panel.control_panel_mapper import build_control_panel_status
from chattocodex.control_panel.control_panel_operations_service import (
    ControlPanelOperationsService,
)
from chattocodex.control_panel.control_panel_service import ControlPanelService
from chattocodex.control_panel.loopback_request_guard import LoopbackRequestGuard
from chattocodex.control_panel.macos_folder_picker import choose_workspace_folder
from chattocodex.control_panel.runtime_boundary_provider import (
    RuntimeBoundaryProvider,
)
from chattocodex.main_runtime.main_review_route_adapter import MainReviewRouteAdapter
from chattocodex.orchestration.orchestration_exceptions import OrchestrationError
from chattocodex.project_runtime.project_binding_exceptions import (
    ProjectBindingConfigurationError,
)
from chattocodex.project_runtime.project_binding_mapper import view_from_state
from chattocodex.project_runtime.project_binding_service import ProjectBindingService
from chattocodex.provisioning.provisioning_dto import ProvisioningRequestStatus
from chattocodex.provisioning.provisioning_mapper import (
    request_view_from_state,
)
from chattocodex.provisioning.worker_provisioning_service import (
    WorkerProvisioningService,
)
from chattocodex.schemas.control_panel_schemas import (
    ControlPanelErrorResponse,
    ControlPanelPreferencesRequest,
    FolderPickerResponse,
    OwnerTokenResponse,
)
from chattocodex.schemas.project_binding_schemas import (
    ConnectorAppIdentity,
    ProjectBindingResponse,
    ProjectBindingsResponse,
    ProjectBindingUpsertRequest,
    ProjectConnectorResponse,
    ProjectConnectorView,
)
from chattocodex.schemas.project_schemas import ProjectId
from chattocodex.schemas.wake_schemas import (
    WakeProjectsResponse,
    WakeProjectView,
    WakeTargetPollRequest,
    WakeTargetRegisterRequest,
    WakeTargetResponse,
    WakeTargetRetireBatchRequest,
    WakeTargetRetireBatchResponse,
    WakeTargetRetireRequest,
    WakeTargetRetireResponse,
)
from chattocodex.schemas.worker_provisioning_schemas import (
    WorkerProvisioningFailureRequest,
    WorkerProvisioningFailureResponse,
    WorkerProvisioningNextResponse,
    WorkerProvisioningProgressRequest,
    WorkerProvisioningRegistrationRequest,
    WorkerProvisioningRequestResponse,
    WorkerProvisioningRetryRequest,
    WorkerProvisioningStatusResponse,
)
from chattocodex.wake.wake_control_service import WakeControlService
from chattocodex.wake.wake_dto import WakeTargetRetirement
from chattocodex.wake.wake_view_mapper import build_wake_target_view

_STATIC_ROOT = Path(__file__).resolve().parent / "static"


def create_control_panel_app(
    *,
    service: ControlPanelService,
    wake_service: WakeControlService | None = None,
    main_review_adapter: MainReviewRouteAdapter | None = None,
    project_binding_service: ProjectBindingService | None = None,
    worker_provisioning_service: WorkerProvisioningService | None = None,
    runtime_boundary_provider: RuntimeBoundaryProvider | None = None,
) -> Starlette:
    """Create the loopback Control Panel application and local runtime routes.

    Args:
        service: Control Panel lifecycle and preference service.
        wake_service: Optional static Worker wake service for embedding/tests.
        main_review_adapter: Optional static Main review adapter for embedding/tests.
        project_binding_service: Optional canonical project binding service. It is
            optional for backwards-compatible embedding; production wiring supplies it.
        runtime_boundary_provider: Optional dynamic production provider. When supplied,
            each runtime request resolves services for the current workspace config.

    Returns:
        A Starlette application bound by the caller to a loopback interface.
    """

    @asynccontextmanager
    async def lifespan(_app: Starlette):
        try:
            yield
        finally:
            await service.shutdown()

    async def index(_request: Request) -> Response:
        return FileResponse(
            _STATIC_ROOT / "index.html",
            headers={"Cache-Control": "no-store"},
        )

    async def status(_request: Request) -> Response:
        return _json_response(
            model=build_control_panel_status(snapshot=await service.snapshot())
        )

    async def save_preferences(request: Request) -> Response:
        try:
            payload = ControlPanelPreferencesRequest.model_validate_json(
                await request.body()
            )
            snapshot = await service.save_preferences(request=payload)
            return _json_response(model=build_control_panel_status(snapshot=snapshot))
        except ValidationError as error:
            return _error_response(
                error_code="validation_error",
                message=_validation_message(error=error),
                recoverable=True,
                status_code=422,
            )
        except ControlPanelError as error:
            return _control_panel_error_response(error=error)

    async def start_runtime(_request: Request) -> Response:
        try:
            snapshot = await service.start()
            return _json_response(model=build_control_panel_status(snapshot=snapshot))
        except ControlPanelError as error:
            return _control_panel_error_response(error=error)

    async def stop_runtime(_request: Request) -> Response:
        try:
            snapshot = await service.stop()
            return _json_response(model=build_control_panel_status(snapshot=snapshot))
        except ControlPanelError as error:
            return _control_panel_error_response(error=error)

    async def rotate_owner_token(_request: Request) -> Response:
        try:
            token = await service.rotate_owner_token()
            return _json_response(model=OwnerTokenResponse(owner_token=token))
        except ControlPanelError as error:
            return _control_panel_error_response(error=error)

    async def folder_picker(_request: Request) -> Response:
        try:
            selected = await choose_workspace_folder()
            return _json_response(
                model=FolderPickerResponse(
                    selected_path=str(selected) if selected is not None else None
                )
            )
        except ControlPanelError as error:
            return _control_panel_error_response(error=error)

    async def operations(_request: Request) -> Response:
        try:
            return _json_response(model=await _operations_snapshot())
        except OrchestrationError as error:
            return _orchestration_error_response(error=error)

    async def worker_browser_setup(_request: Request) -> Response:
        try:
            await service.start_worker_browser_setup()
            return _json_response(model=await _operations_snapshot())
        except ControlPanelError as error:
            return _control_panel_error_response(error=error)
        except OrchestrationError as error:
            return _orchestration_error_response(error=error)

    async def worker_browser_start(_request: Request) -> Response:
        try:
            await service.start_worker_browser_headless()
            return _json_response(model=await _operations_snapshot())
        except ControlPanelError as error:
            return _control_panel_error_response(error=error)
        except OrchestrationError as error:
            return _orchestration_error_response(error=error)

    async def worker_browser_stop(_request: Request) -> Response:
        try:
            await service.stop_worker_browser()
            return _json_response(model=await _operations_snapshot())
        except ControlPanelError as error:
            return _control_panel_error_response(error=error)
        except OrchestrationError as error:
            return _orchestration_error_response(error=error)

    async def _operations_snapshot():
        if runtime_boundary_provider is None:
            raise RuntimeLifecycleError(
                message="Operations routes require the workspace runtime boundary provider."
            )
        boundaries = runtime_boundary_provider.current()
        projects = await asyncio.to_thread(_current_wake_service().list_projects)
        bindings = await asyncio.to_thread(
            _current_project_binding_service().list_bindings
        )
        operations_service = ControlPanelOperationsService(
            execution_root=boundaries.execution_root
        )
        source_snapshot = await asyncio.to_thread(
            operations_service.snapshot,
            projects=projects,
            bindings=bindings,
            browser_running=service.worker_browser_running(),
            browser_mode=service.worker_browser_mode(),
            browser_profile_initialized=service.worker_browser_profile_initialized(),
        )
        return await service.project_operations_read_model(snapshot=source_snapshot)

    async def wake_projects(_request: Request) -> Response:
        try:
            current_wake_service = _current_wake_service()
            projects = await asyncio.to_thread(current_wake_service.list_projects)
            return _json_response(
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
            return _orchestration_error_response(error=error)

    async def wake_register(request: Request) -> Response:
        try:
            payload = WakeTargetRegisterRequest.model_validate_json(
                await request.body()
            )
            current_wake_service = _current_wake_service()
            target = await asyncio.to_thread(
                current_wake_service.register_target,
                wake_target_id=payload.wake_target_id,
                project_id=payload.project_id,
            )
            return _json_response(
                model=WakeTargetResponse(target=build_wake_target_view(state=target))
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

    async def wake_target(request: Request) -> Response:
        try:
            payload = WakeTargetPollRequest(
                wake_target_id=str(request.path_params["wake_target_id"])
            )
            current_wake_service = _current_wake_service()
            target = await asyncio.to_thread(
                current_wake_service.get_target,
                wake_target_id=payload.wake_target_id,
            )
            return _json_response(
                model=WakeTargetResponse(target=build_wake_target_view(state=target))
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

    async def wake_runtime_heartbeat(request: Request) -> Response:
        try:
            payload = WakeTargetPollRequest(
                wake_target_id=str(request.path_params["wake_target_id"])
            )
            current_wake_service = _current_wake_service()
            target = await asyncio.to_thread(
                current_wake_service.runtime_heartbeat_target,
                wake_target_id=payload.wake_target_id,
            )
            return _json_response(
                model=WakeTargetResponse(target=build_wake_target_view(state=target))
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

    async def wake_retire(request: Request) -> Response:
        try:
            target_id = WakeTargetPollRequest(
                wake_target_id=str(request.path_params["wake_target_id"])
            ).wake_target_id
            payload = WakeTargetRetireRequest.model_validate_json(await request.body())
            retired = await asyncio.to_thread(
                _current_wake_service().retire_unclaimed_target,
                wake_target_id=target_id,
                expected_revision=payload.expected_revision,
            )
            return _json_response(
                model=WakeTargetRetireResponse(
                    wake_target_id=retired.wake_target_id,
                    retired_revision=retired.revision,
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

    async def wake_retire_batch(request: Request) -> Response:
        try:
            payload = WakeTargetRetireBatchRequest.model_validate_json(
                await request.body()
            )
            retired_ids = await asyncio.to_thread(
                _current_wake_service().retire_unclaimed_targets_batch,
                project_id=payload.project_id,
                targets=tuple(
                    WakeTargetRetirement(
                        wake_target_id=item.wake_target_id,
                        expected_revision=item.expected_revision,
                    )
                    for item in payload.targets
                ),
            )
            return _json_response(
                model=WakeTargetRetireBatchResponse(
                    project_id=payload.project_id,
                    retired_wake_target_ids=retired_ids,
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

    async def project_binding_list(_request: Request) -> Response:
        try:
            binding_service = _current_project_binding_service()
            bindings = await asyncio.to_thread(binding_service.list_bindings)
            return _json_response(
                model=ProjectBindingsResponse(
                    bindings=tuple(view_from_state(state) for state in bindings)
                )
            )
        except OrchestrationError as error:
            return _project_binding_error_response(error=error)

    async def project_binding_get(request: Request) -> Response:
        try:
            project_id = _project_id_from_path(request=request)
            binding_service = _current_project_binding_service()
            binding = await asyncio.to_thread(
                binding_service.get_binding,
                project_id=project_id,
            )
            return _json_response(
                model=ProjectBindingResponse(binding=view_from_state(binding))
            )
        except ValidationError as error:
            return _error_response(
                error_code="validation_error",
                message=_validation_message(error=error),
                recoverable=True,
                status_code=422,
            )
        except OrchestrationError as error:
            return _project_binding_error_response(error=error)

    async def project_binding_upsert(request: Request) -> Response:
        try:
            project_id = _project_id_from_path(request=request)
            payload = ProjectBindingUpsertRequest.model_validate_json(
                await request.body()
            )
            binding_service = _current_project_binding_service()
            binding = await asyncio.to_thread(
                binding_service.upsert_binding,
                project_id=project_id,
                metadata=payload.adapter_metadata,
                expected_revision=payload.expected_revision,
            )
            return _json_response(
                model=ProjectBindingResponse(binding=view_from_state(binding))
            )
        except ValidationError as error:
            return _error_response(
                error_code="validation_error",
                message=_validation_message(error=error),
                recoverable=True,
                status_code=422,
            )
        except OrchestrationError as error:
            return _project_binding_error_response(error=error)

    async def project_connector_get(request: Request) -> Response:
        try:
            project_id = _project_id_from_path(request=request)
            binding = await asyncio.to_thread(
                _current_project_binding_service().get_binding,
                project_id=project_id,
            )
            metadata = binding.adapter_metadata
            if (
                metadata is None
                or metadata.provider is None
                or metadata.project_ref is None
                or metadata.project_path is None
                or metadata.app_ref is None
                or metadata.app_label is None
            ):
                raise ProjectBindingConfigurationError(
                    message="Project connector metadata is incomplete."
                )
            return _json_response(
                model=ProjectConnectorResponse(
                    connector=ProjectConnectorView(
                        project_id=binding.project_id,
                        provider=metadata.provider,
                        project_ref=metadata.project_ref,
                        project_path=metadata.project_path,
                        app_identity=ConnectorAppIdentity(
                            app_ref=metadata.app_ref,
                            app_label=metadata.app_label,
                        ),
                        connector_epoch=binding.connector_epoch,
                        revision=binding.revision,
                    )
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
            return _project_binding_error_response(error=error)

    async def worker_provisioning_next(_request: Request) -> Response:
        try:
            provisioning = _current_worker_provisioning_service()
            request_state = await asyncio.to_thread(provisioning.admit_next)
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

    async def worker_provisioning_status(_request: Request) -> Response:
        try:
            provisioning = _current_worker_provisioning_service()
            demands, requests = await asyncio.to_thread(provisioning.list_status)
            return _json_response(
                model=WorkerProvisioningStatusResponse(
                    demands=demands,
                    requests=requests,
                )
            )
        except OrchestrationError as error:
            return _provisioning_error_response(error=error)

    async def worker_provisioning_registered(request: Request) -> Response:
        try:
            payload = WorkerProvisioningRegistrationRequest.model_validate_json(
                await request.body()
            )
            provisioning = _current_worker_provisioning_service()
            state = await asyncio.to_thread(
                provisioning.register_callback,
                request_id=str(request.path_params["request_id"]),
                conversation_id=payload.conversation_id,
                wake_target_id=payload.wake_target_id,
                admission_revision=payload.admission_revision,
            )
            return _json_response(
                model=WorkerProvisioningRequestResponse(
                    request=request_view_from_state(state)
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
            return _provisioning_error_response(error=error)

    async def worker_provisioning_failed(request: Request) -> Response:
        try:
            payload = WorkerProvisioningFailureRequest.model_validate_json(
                await request.body()
            )
            provisioning = _current_worker_provisioning_service()
            _state, retry_after_ms = await asyncio.to_thread(
                provisioning.fail_callback,
                request_id=str(request.path_params["request_id"]),
                admission_revision=payload.admission_revision,
                reason=payload.reason,
                rate_limited=payload.rate_limited,
            )
            return _json_response(
                model=WorkerProvisioningFailureResponse(retry_after_ms=retry_after_ms)
            )
        except ValidationError as error:
            return _error_response(
                error_code="validation_error",
                message=_validation_message(error=error),
                recoverable=True,
                status_code=422,
            )
        except OrchestrationError as error:
            return _provisioning_error_response(error=error)

    async def worker_provisioning_progress(request: Request) -> Response:
        try:
            payload = WorkerProvisioningProgressRequest.model_validate_json(
                await request.body()
            )
            provisioning = _current_worker_provisioning_service()
            state = await asyncio.to_thread(
                provisioning.progress_request,
                request_id=str(request.path_params["request_id"]),
                status=ProvisioningRequestStatus(payload.status),
                admission_revision=payload.admission_revision,
            )
            return _json_response(
                model=WorkerProvisioningRequestResponse(
                    request=request_view_from_state(state)
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
            return _provisioning_error_response(error=error)

    async def worker_provisioning_retry(request: Request) -> Response:
        try:
            payload = WorkerProvisioningRetryRequest.model_validate_json(
                await request.body()
            )
            provisioning = _current_worker_provisioning_service()
            state = await asyncio.to_thread(
                provisioning.retry_operator_blocked_request,
                request_id=str(request.path_params["request_id"]),
                expected_revision=payload.expected_revision,
            )
            return _json_response(
                model=WorkerProvisioningRequestResponse(
                    request=request_view_from_state(state)
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
            return _provisioning_error_response(error=error)

    async def main_review_register(request: Request) -> Response:
        try:
            adapter = _current_main_review_adapter()
        except OrchestrationError as error:
            return _orchestration_error_response(error=error)
        return await adapter.register_target(request)

    async def main_review_poll(request: Request) -> Response:
        try:
            adapter = _current_main_review_adapter()
        except OrchestrationError as error:
            return _orchestration_error_response(error=error)
        return await adapter.poll_target(request)

    async def main_review_heartbeat(request: Request) -> Response:
        try:
            adapter = _current_main_review_adapter()
        except OrchestrationError as error:
            return _orchestration_error_response(error=error)
        return await adapter.heartbeat_chat(request)

    def _current_wake_service() -> WakeControlService:
        if runtime_boundary_provider is not None:
            return runtime_boundary_provider.current().wake_service
        return _require_wake_service(wake_service)

    def _current_main_review_adapter() -> MainReviewRouteAdapter:
        if runtime_boundary_provider is not None:
            return runtime_boundary_provider.current().main_review_adapter
        return _require_main_review_adapter(main_review_adapter)

    def _current_project_binding_service() -> ProjectBindingService:
        if runtime_boundary_provider is not None:
            return runtime_boundary_provider.current().project_binding_service
        return _require_project_binding_service(project_binding_service)

    def _current_worker_provisioning_service() -> WorkerProvisioningService:
        if runtime_boundary_provider is not None:
            return runtime_boundary_provider.current().worker_provisioning_service
        return _require_worker_provisioning_service(worker_provisioning_service)

    return Starlette(
        debug=False,
        lifespan=lifespan,
        middleware=[Middleware(LoopbackRequestGuard)],
        routes=[
            Route("/", index, methods=["GET"]),
            Route("/api/status", status, methods=["GET"]),
            Route("/api/preferences", save_preferences, methods=["POST"]),
            Route("/api/start", start_runtime, methods=["POST"]),
            Route("/api/stop", stop_runtime, methods=["POST"]),
            Route("/api/owner-token/rotate", rotate_owner_token, methods=["POST"]),
            Route("/api/folder-picker", folder_picker, methods=["POST"]),
            Route("/api/operations", operations, methods=["GET"]),
            Route(
                "/api/worker-browser/setup",
                worker_browser_setup,
                methods=["POST"],
            ),
            Route(
                "/api/worker-browser/start",
                worker_browser_start,
                methods=["POST"],
            ),
            Route(
                "/api/worker-browser/stop",
                worker_browser_stop,
                methods=["POST"],
            ),
            Route("/api/wake/projects", wake_projects, methods=["GET"]),
            Route("/api/wake/targets/register", wake_register, methods=["POST"]),
            Route(
                "/api/wake/targets/retire-batch",
                wake_retire_batch,
                methods=["POST"],
            ),
            Route(
                "/api/wake/targets/{wake_target_id}",
                wake_target,
                methods=["GET"],
            ),
            Route(
                "/api/wake/targets/{wake_target_id}/runtime-heartbeat",
                wake_runtime_heartbeat,
                methods=["POST"],
            ),
            Route(
                "/api/wake/targets/{wake_target_id}/retire",
                wake_retire,
                methods=["POST"],
            ),
            Route(
                "/api/project-bindings",
                project_binding_list,
                methods=["GET"],
            ),
            Route(
                "/api/project-bindings/{project_id}/connector",
                project_connector_get,
                methods=["GET"],
            ),
            Route(
                "/api/project-bindings/{project_id}",
                project_binding_get,
                methods=["GET"],
            ),
            Route(
                "/api/project-bindings/{project_id}",
                project_binding_upsert,
                methods=["PUT"],
            ),
            Route(
                "/api/worker-provisioning/next",
                worker_provisioning_next,
                methods=["POST"],
            ),
            Route(
                "/api/worker-provisioning/status",
                worker_provisioning_status,
                methods=["GET"],
            ),
            Route(
                "/api/worker-provisioning/{request_id}/registered",
                worker_provisioning_registered,
                methods=["POST"],
            ),
            Route(
                "/api/worker-provisioning/{request_id}/failed",
                worker_provisioning_failed,
                methods=["POST"],
            ),
            Route(
                "/api/worker-provisioning/{request_id}/progress",
                worker_provisioning_progress,
                methods=["POST"],
            ),
            Route(
                "/api/worker-provisioning/{request_id}/retry",
                worker_provisioning_retry,
                methods=["POST"],
            ),
            Route(
                "/api/main-runtime/review-targets/register",
                main_review_register,
                methods=["POST"],
            ),
            Route(
                "/api/main-runtime/review-targets/{main_target_id}",
                main_review_poll,
                methods=["GET"],
            ),
            Route(
                "/api/main-runtime/review-targets/{main_target_id}/chat-heartbeat",
                main_review_heartbeat,
                methods=["POST"],
            ),
            Mount("/static", app=StaticFiles(directory=_STATIC_ROOT), name="static"),
        ],
    )


def _json_response(*, model: BaseModel) -> JSONResponse:
    return JSONResponse(
        model.model_dump(mode="json"),
        headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
    )


def _control_panel_error_response(*, error: ControlPanelError) -> JSONResponse:
    return _error_response(
        error_code=error.error_code,
        message=error.message,
        recoverable=error.recoverable,
        status_code=409 if error.recoverable else 400,
    )


def _orchestration_error_response(*, error: OrchestrationError) -> JSONResponse:
    return _error_response(
        error_code=error.error_code,
        message=error.message,
        recoverable=error.recoverable,
        status_code=409 if error.recoverable else 400,
    )


def _project_binding_error_response(*, error: OrchestrationError) -> JSONResponse:
    """Map a project binding failure to its local HTTP response.

    Args:
        error: Structured project binding or orchestration failure.

    Returns:
        A JSON error response, using HTTP 404 for an absent binding or project.
    """
    status_code = (
        404
        if error.error_code.value == "not_found"
        else (409 if error.recoverable else 400)
    )
    return _error_response(
        error_code=error.error_code,
        message=error.message,
        recoverable=error.recoverable,
        status_code=status_code,
    )


def _provisioning_error_response(*, error: OrchestrationError) -> JSONResponse:
    """Map a Worker provisioning failure to a local HTTP response.

    Args:
        error: Structured provisioning or orchestration failure.

    Returns:
        A structured local Control Panel error response.
    """
    return _error_response(
        error_code=error.error_code,
        message=error.message,
        recoverable=error.recoverable,
        status_code=409 if error.recoverable else 400,
    )


def _require_worker_provisioning_service(
    service: WorkerProvisioningService | None,
) -> WorkerProvisioningService:
    """Require production wiring for Worker provisioning routes.

    Args:
        service: Optional provisioning service supplied by the application factory.

    Returns:
        The configured provisioning service.

    Raises:
        ProjectBindingConfigurationError: If provisioning is not wired.
    """
    if service is None:
        raise ProjectBindingConfigurationError(
            message="Worker provisioning routes are not configured for this Control Panel.",
        )
    return service


def _require_wake_service(service: WakeControlService | None) -> WakeControlService:
    if service is None:
        raise ProjectBindingConfigurationError(
            message="Wake routes are not configured for this Control Panel.",
        )
    return service


def _require_main_review_adapter(
    adapter: MainReviewRouteAdapter | None,
) -> MainReviewRouteAdapter:
    if adapter is None:
        raise ProjectBindingConfigurationError(
            message="Main runtime routes are not configured for this Control Panel.",
        )
    return adapter


def _require_project_binding_service(
    service: ProjectBindingService | None,
) -> ProjectBindingService:
    """Require production Control Panel wiring for binding routes.

    Args:
        service: Optional binding service supplied by the application factory.

    Returns:
        The configured project binding service.

    Raises:
        ProjectBindingConfigurationError: If embedding omitted binding wiring.
    """
    if service is None:
        raise ProjectBindingConfigurationError(
            message="Project binding routes are not configured for this Control Panel.",
        )
    return service


def _project_id_from_path(*, request: Request) -> str:
    """Validate a project binding path parameter as a canonical project ID.

    Args:
        request: Incoming Starlette request containing ``project_id``.

    Returns:
        The validated canonical project identifier.

    Raises:
        ValidationError: If the path parameter does not match the project ID contract.
    """
    return _validate_project_id(value=str(request.path_params["project_id"]))


def _validate_project_id(*, value: str) -> str:
    """Validate a project ID using the shared Pydantic project contract.

    Args:
        value: Raw path parameter.

    Returns:
        The validated project identifier.

    Raises:
        ValidationError: If ``value`` is not a canonical project ID.
    """
    return TypeAdapter(ProjectId).validate_python(value)


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
    details = error.errors(include_url=False)
    if not details:
        return "Invalid request."
    message = details[0].get("msg")
    return str(message) if message else "Invalid request."
