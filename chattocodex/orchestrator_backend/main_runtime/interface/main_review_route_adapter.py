from asyncer import asyncify
from starlette.responses import Response

from chattocodex.orchestrator_backend.control_panel.interface.http.control_panel_http_responses import (
    json_response,
    orchestration_error_response,
)
from chattocodex.orchestrator_backend.main_runtime.application.main_review_project_catalog import (
    MainReviewProjectCatalog,
)
from chattocodex.orchestrator_backend.main_runtime.application.main_review_view_mapper import (
    build_main_review_target_view,
)
from chattocodex.orchestrator_backend.main_runtime.target.main_review_target_coordinator import (
    MainReviewTargetCoordinator,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_service import (
    ProjectBindingService,
)
from chattocodex.shared.contracts.main_runtime.main_runtime_schemas import (
    MainReviewTargetRegisterRequest,
    MainReviewTargetResponse,
    MainTargetId,
)


class MainReviewRouteAdapter:
    def __init__(
        self,
        target_coordinator: MainReviewTargetCoordinator,
        project_catalog: MainReviewProjectCatalog,
        project_binding_service: ProjectBindingService,
    ) -> None:
        """Initialize this object. Args: target_coordinator, project_catalog, project_binding_service. Returns: None."""
        self._target_coordinator = target_coordinator
        self._project_catalog = project_catalog
        self._project_binding_service = project_binding_service

    async def register_target(
        self,
        payload: MainReviewTargetRegisterRequest,
    ) -> Response:
        """Register target. Args: payload. Returns: Response."""
        try:
            project_id = await asyncify(self._project_catalog.require_project_id)(
                project_id=payload.project_id,
            )
            await asyncify(self._project_binding_service.require_connector_binding)(
                project_id=project_id,
            )
            target = await asyncify(self._target_coordinator.register_target)(
                main_target_id=payload.main_target_id,
                project_id=project_id,
            )
            return json_response(
                model=MainReviewTargetResponse(
                    target=build_main_review_target_view(state=target)
                )
            )
        except OrchestrationError as error:
            return orchestration_error_response(error=error)

    async def poll_target(self, main_target_id: MainTargetId) -> Response:
        """Poll target. Args: main_target_id. Returns: Response."""
        try:
            target = await asyncify(self._target_coordinator.poll_target)(
                main_target_id=main_target_id,
            )
            return json_response(
                model=MainReviewTargetResponse(
                    target=build_main_review_target_view(state=target)
                )
            )
        except OrchestrationError as error:
            return orchestration_error_response(error=error)

    async def heartbeat_chat(self, main_target_id: MainTargetId) -> Response:
        """Heartbeat chat. Args: main_target_id. Returns: Response."""
        try:
            target = await asyncify(self._target_coordinator.heartbeat_chat)(
                main_target_id=main_target_id,
            )
            return json_response(
                model=MainReviewTargetResponse(
                    target=build_main_review_target_view(state=target)
                )
            )
        except OrchestrationError as error:
            return orchestration_error_response(error=error)
