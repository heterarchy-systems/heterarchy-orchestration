class ControlPanelError(RuntimeError):
    def __init__(
        self,
        *,
        error_code: str,
        message: str,
        recoverable: bool,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.message = message
        self.recoverable = recoverable


class FolderPickerError(ControlPanelError):
    def __init__(self, *, message: str) -> None:
        super().__init__(
            error_code="folder_picker_error",
            message=message,
            recoverable=True,
        )


class RuntimeLifecycleError(ControlPanelError):
    def __init__(self, *, message: str) -> None:
        super().__init__(
            error_code="runtime_lifecycle_error",
            message=message,
            recoverable=True,
        )


class OwnerTokenRotationConflictError(ControlPanelError):
    def __init__(self) -> None:
        super().__init__(
            error_code="owner_token_rotation_conflict",
            message="Owner Token changed during rotation. Retry the operation.",
            recoverable=True,
        )


class TunnelLifecycleError(ControlPanelError):
    def __init__(self, *, message: str) -> None:
        super().__init__(
            error_code="tunnel_lifecycle_error",
            message=message,
            recoverable=True,
        )
