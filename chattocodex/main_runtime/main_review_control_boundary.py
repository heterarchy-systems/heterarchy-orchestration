from starlette.routing import Route

from chattocodex.main_runtime.main_review_route_adapter import MainReviewRouteAdapter


def build_main_review_routes(*, adapter: MainReviewRouteAdapter) -> tuple[Route, ...]:
    return (
        Route(
            "/api/main-runtime/review-targets/register",
            adapter.register_target,
            methods=["POST"],
        ),
        Route(
            "/api/main-runtime/review-targets/{main_target_id}",
            adapter.poll_target,
            methods=["GET"],
        ),
    )
