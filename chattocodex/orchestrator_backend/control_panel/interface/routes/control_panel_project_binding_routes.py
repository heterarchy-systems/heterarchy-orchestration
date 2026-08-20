from collections.abc import Callable
from typing import Annotated

from asyncer import asyncify
from fastapi import APIRouter, Depends
from starlette.responses import Response

from chattocodex.orchestrator_backend.control_panel.interface.http.control_panel_http_responses import (
    json_response,
    project_binding_error_response,
)
from chattocodex.orchestrator_backend.control_panel.interface.http.fastapi_json_boundary import (
    build_strict_json_parser,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_exceptions import (
    ProjectBindingConfigurationError,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_mapper import (
    view_from_state,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_service import (
    ProjectBindingService,
)
from chattocodex.shared.contracts.project.project_binding_schemas import (
    ConnectorAppIdentity,
    ProjectBindingResponse,
    ProjectBindingsResponse,
    ProjectBindingUpsertRequest,
    ProjectConnectorResponse,
    ProjectConnectorView,
)
from chattocodex.shared.contracts.project.project_schemas import ProjectId

ProjectBindingServiceResolver = Callable[[], ProjectBindingService]
_parse_binding_upsert = build_strict_json_parser(ProjectBindingUpsertRequest)


def build_project_binding_routes(
    resolve_service: ProjectBindingServiceResolver,
) -> APIRouter:
    """Build project binding routes. Args: resolve_service. Returns: APIRouter."""

    async def project_binding_list() -> Response:
        """Project binding list. Args: None. Returns: Response."""
        try:
            bindings = await asyncify(resolve_service().list_bindings)()
            return json_response(
                model=ProjectBindingsResponse(
                    bindings=tuple(view_from_state(state) for state in bindings)
                )
            )
        except OrchestrationError as error:
            return project_binding_error_response(error=error)

    async def project_binding_get(project_id: ProjectId) -> Response:
        """Project binding get. Args: project_id. Returns: Response."""
        try:
            binding = await asyncify(resolve_service().get_binding)(
                project_id=project_id,
            )
            return json_response(
                model=ProjectBindingResponse(binding=view_from_state(binding))
            )
        except OrchestrationError as error:
            return project_binding_error_response(error=error)

    async def project_binding_upsert(
        project_id: ProjectId,
        payload: Annotated[
            ProjectBindingUpsertRequest,
            Depends(_parse_binding_upsert),
        ],
    ) -> Response:
        """Project binding upsert. Args: project_id, payload. Returns: Response."""
        try:
            binding = await asyncify(resolve_service().upsert_binding)(
                project_id=project_id,
                metadata=payload.adapter_metadata,
                expected_revision=payload.expected_revision,
            )
            return json_response(
                model=ProjectBindingResponse(binding=view_from_state(binding))
            )
        except OrchestrationError as error:
            return project_binding_error_response(error=error)

    async def project_connector_get(project_id: ProjectId) -> Response:
        """Project connector get. Args: project_id. Returns: Response."""
        try:
            binding = await asyncify(resolve_service().get_binding)(
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
            return json_response(
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
        except OrchestrationError as error:
            return project_binding_error_response(error=error)

    router = APIRouter()
    router.add_api_route("/api/project-bindings", project_binding_list, methods=["GET"])
    router.add_api_route(
        "/api/project-bindings/{project_id}/connector",
        project_connector_get,
        methods=["GET"],
    )
    router.add_api_route(
        "/api/project-bindings/{project_id}", project_binding_get, methods=["GET"]
    )
    router.add_api_route(
        "/api/project-bindings/{project_id}", project_binding_upsert, methods=["PUT"]
    )
    return router
