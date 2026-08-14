import hashlib
import os
import tempfile
from pathlib import Path


class RuntimeLockLayout:
    """Derive user-local lock namespaces outside rotatable runtime state."""

    DIRECTORY_NAME = "chattocodex-runtime-locks"

    @classmethod
    def lock_directory(
        cls,
        *,
        state_root: Path,
        namespace: str,
        base_directory: Path | None = None,
    ) -> Path:
        """Return one opaque, stable directory for a state root and lock domain."""

        normalized_namespace = namespace.strip()
        if not normalized_namespace:
            raise ValueError("Lock namespace must not be empty.")
        canonical_state_root = state_root.expanduser().resolve()
        base = (
            base_directory.expanduser().resolve()
            if base_directory is not None
            else Path(tempfile.gettempdir()).resolve()
            / f"{cls.DIRECTORY_NAME}-{os.getuid()}"
        )
        base.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(base, 0o700)
        identity = hashlib.sha256(
            f"{canonical_state_root}\0{normalized_namespace}".encode()
        ).hexdigest()
        return base / identity
