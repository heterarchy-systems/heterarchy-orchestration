from collections.abc import Callable

from chattocodex.orchestrator_backend.wake.wake_dto import WakeTargetState
from chattocodex.orchestrator_backend.wake.wake_exceptions import (
    WakeTargetConflictError,
)


class WakeTargetConnectorPolicy:
    """Own connector-epoch compatibility for wake targets.

    Args:
        None.

    Returns:
        None.
    """

    def __init__(self) -> None:
        """Initialize the connector epoch resolver. Args: None. Returns: None."""
        self._epoch_resolver: Callable[[str], int] = lambda _project_id: 0

    def attach_epoch_resolver(self, resolver: Callable[[str], int]) -> None:
        """Attach the current connector epoch resolver. Args: resolver. Returns: None."""
        self._epoch_resolver = resolver

    def current_epoch(self, project_id: str) -> int:
        """Resolve the current connector epoch. Args: project_id. Returns: int."""
        return self._epoch_resolver(project_id)

    def is_compatible(self, target: WakeTargetState) -> bool:
        """Check whether a target belongs to the current connector epoch. Args: target. Returns: bool."""
        return target.registered_connector_epoch == self.current_epoch(
            project_id=target.project_id
        )

    def require_compatible(self, target: WakeTargetState) -> None:
        """Require the target to belong to the current connector epoch. Args: target. Returns: None."""
        if self.is_compatible(target=target):
            return
        raise WakeTargetConflictError(
            wake_target_id=target.wake_target_id,
            message=(
                "Wake target was registered under a different connector epoch. "
                "Register a replacement Worker Chat for the current connector."
            ),
        )
