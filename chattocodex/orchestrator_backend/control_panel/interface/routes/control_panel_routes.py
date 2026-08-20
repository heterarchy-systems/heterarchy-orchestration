from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from chattocodex.orchestrator_backend.broker_client.broker_operations_gateway import (
    BrokerOperationsGateway,
)
from chattocodex.orchestrator_backend.control_panel.application.control_panel_service import (
    ControlPanelService,
)
from chattocodex.orchestrator_backend.control_panel.composition.runtime_boundary_provider import (
    RuntimeBoundaryProvider,
)
from chattocodex.orchestrator_backend.control_panel.interface.http.control_panel_http_responses import (
    request_validation_error_response,
)
from chattocodex.orchestrator_backend.control_panel.interface.http.loopback_request_guard import (
    LoopbackRequestGuard,
)
from chattocodex.orchestrator_backend.control_panel.interface.routes.control_panel_main_review_routes import (
    build_main_review_routes,
)
from chattocodex.orchestrator_backend.control_panel.interface.routes.control_panel_project_binding_routes import (
    build_project_binding_routes,
)
from chattocodex.orchestrator_backend.control_panel.interface.routes.control_panel_runtime_routes import (
    build_runtime_routes,
)
from chattocodex.orchestrator_backend.control_panel.interface.routes.control_panel_wake_routes import (
    build_wake_routes,
)
from chattocodex.orchestrator_backend.control_panel.interface.routes.control_panel_worker_provisioning_routes import (
    build_worker_provisioning_routes,
)
from chattocodex.orchestrator_backend.main_runtime.interface.main_review_route_adapter import (
    MainReviewRouteAdapter,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_exceptions import (
    ProjectBindingConfigurationError,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_service import (
    ProjectBindingService,
)
from chattocodex.orchestrator_backend.provisioning.worker_provisioning_service import (
    WorkerProvisioningService,
)
from chattocodex.orchestrator_backend.wake.wake_control_service import (
    WakeControlService,
)

_STATIC_ROOT = Path(__file__).resolve().parents[2] / "static"


def create_control_panel_app(
    service: ControlPanelService,
    wake_service: WakeControlService | None = None,
    main_review_adapter: MainReviewRouteAdapter | None = None,
    project_binding_service: ProjectBindingService | None = None,
    worker_provisioning_service: WorkerProvisioningService | None = None,
    runtime_boundary_provider: RuntimeBoundaryProvider | None = None,
    broker_operations: BrokerOperationsGateway | None = None,
) -> FastAPI:
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
        A FastAPI application bound by the caller to a loopback interface. Args: service, wake_service, main_review_adapter, project_binding_service, worker_provisioning_service, runtime_boundary_provider, broker_operations."""

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        """Lifespan. Args: _app. Returns: AsyncIterator[None]."""
        try:
            yield
        finally:
            await service.shutdown()

    async def index(_request: Request) -> Response:
        """Index. Args: _request. Returns: Response."""
        return FileResponse(
            _STATIC_ROOT / "index.html",
            headers={"Cache-Control": "no-store"},
        )

    async def request_validation_handler(
        _request: Request,
        error: Exception,
    ) -> Response:
        """Request validation handler. Args: _request, error. Returns: Response."""
        if not isinstance(error, RequestValidationError):
            raise error
        return request_validation_error_response(error=error)

    def _current_wake_service() -> WakeControlService:
        """Current wake service. Args: None. Returns: WakeControlService."""
        if runtime_boundary_provider is not None:
            return runtime_boundary_provider.current().wake_service
        return _require_wake_service(wake_service)

    def _current_main_review_adapter() -> MainReviewRouteAdapter:
        """Current main review adapter. Args: None. Returns: MainReviewRouteAdapter."""
        if runtime_boundary_provider is not None:
            return runtime_boundary_provider.current().main_review_adapter
        return _require_main_review_adapter(main_review_adapter)

    def _current_project_binding_service() -> ProjectBindingService:
        """Current project binding service. Args: None. Returns: ProjectBindingService."""
        if runtime_boundary_provider is not None:
            return runtime_boundary_provider.current().project_binding_service
        return _require_project_binding_service(project_binding_service)

    def _current_worker_provisioning_service() -> WorkerProvisioningService:
        """Current worker provisioning service. Args: None. Returns: WorkerProvisioningService."""
        if runtime_boundary_provider is not None:
            return runtime_boundary_provider.current().worker_provisioning_service
        return _require_worker_provisioning_service(worker_provisioning_service)

    application = FastAPI(
        debug=False,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    application.add_middleware(LoopbackRequestGuard)
    application.add_exception_handler(
        RequestValidationError,
        request_validation_handler,
    )
    application.add_api_route("/", index, methods=["GET"], include_in_schema=False)
    application.include_router(
        build_runtime_routes(
            service=service,
            runtime_boundary_provider=runtime_boundary_provider,
            broker_operations=broker_operations,
            resolve_wake_service=_current_wake_service,
            resolve_project_binding_service=_current_project_binding_service,
        )
    )
    application.include_router(build_wake_routes(resolve_service=_current_wake_service))
    application.include_router(
        build_project_binding_routes(resolve_service=_current_project_binding_service)
    )
    application.include_router(
        build_worker_provisioning_routes(
            resolve_service=_current_worker_provisioning_service
        )
    )
    application.include_router(
        build_main_review_routes(resolve_adapter=_current_main_review_adapter)
    )
    application.mount(
        "/static",
        StaticFiles(directory=_STATIC_ROOT),
        name="static",
    )
    return application


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
    """Require wake service. Args: service. Returns: WakeControlService."""
    if service is None:
        raise ProjectBindingConfigurationError(
            message="Wake routes are not configured for this Control Panel.",
        )
    return service


def _require_main_review_adapter(
    adapter: MainReviewRouteAdapter | None,
) -> MainReviewRouteAdapter:
    """Require main review adapter. Args: adapter. Returns: MainReviewRouteAdapter."""
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
