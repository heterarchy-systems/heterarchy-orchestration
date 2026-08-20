from collections.abc import Callable
from typing import Annotated

from fastapi import APIRouter, Depends
from starlette.responses import Response

from chattocodex.orchestrator_backend.control_panel.interface.http.control_panel_http_responses import (
    orchestration_error_response,
)
from chattocodex.orchestrator_backend.control_panel.interface.http.fastapi_json_boundary import (
    build_strict_json_parser,
)
from chattocodex.orchestrator_backend.main_runtime.interface.main_review_route_adapter import (
    MainReviewRouteAdapter,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
)
from chattocodex.shared.contracts.main_runtime.main_runtime_schemas import (
    MainReviewTargetRegisterRequest,
    MainTargetId,
)

MainReviewAdapterResolver = Callable[[], MainReviewRouteAdapter]
_parse_main_review_register = build_strict_json_parser(MainReviewTargetRegisterRequest)


def build_main_review_routes(resolve_adapter: MainReviewAdapterResolver) -> APIRouter:
    """Build main review routes. Args: resolve_adapter. Returns: APIRouter."""

    async def main_review_register(
        payload: Annotated[
            MainReviewTargetRegisterRequest,
            Depends(_parse_main_review_register),
        ],
    ) -> Response:
        """Main review register. Args: payload. Returns: Response."""
        try:
            adapter = resolve_adapter()
        except OrchestrationError as error:
            return orchestration_error_response(error=error)
        return await adapter.register_target(payload=payload)

    async def main_review_poll(main_target_id: MainTargetId) -> Response:
        """Main review poll. Args: main_target_id. Returns: Response."""
        try:
            adapter = resolve_adapter()
        except OrchestrationError as error:
            return orchestration_error_response(error=error)
        return await adapter.poll_target(main_target_id=main_target_id)

    async def main_review_heartbeat(main_target_id: MainTargetId) -> Response:
        """Main review heartbeat. Args: main_target_id. Returns: Response."""
        try:
            adapter = resolve_adapter()
        except OrchestrationError as error:
            return orchestration_error_response(error=error)
        return await adapter.heartbeat_chat(main_target_id=main_target_id)

    router = APIRouter()
    router.add_api_route(
        "/api/main-runtime/review-targets/register",
        main_review_register,
        methods=["POST"],
    )
    router.add_api_route(
        "/api/main-runtime/review-targets/{main_target_id}",
        main_review_poll,
        methods=["GET"],
    )
    router.add_api_route(
        "/api/main-runtime/review-targets/{main_target_id}/chat-heartbeat",
        main_review_heartbeat,
        methods=["POST"],
    )
    return router
