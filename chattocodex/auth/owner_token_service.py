import hashlib
import hmac
import secrets
from pathlib import Path
from threading import BoundedSemaphore

from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.persistence.atomic_json_file import (
    read_json_model,
    write_json_model,
)
from chattocodex.persistence.runtime_lock_layout import RuntimeLockLayout
from chattocodex.schemas.auth_schemas import OwnerTokenRecord

_OWNER_TOKEN_LOCK_KEY = "owner-token"
_VERIFY_KDF_ADMISSION = BoundedSemaphore(1)


class OwnerTokenRevisionConflictError(RuntimeError):
    """Raised when another writer changes the Owner Token during rotation."""


class OwnerTokenService:
    def __init__(self, *, state_root: Path) -> None:
        normalized_state_root = state_root.expanduser().resolve()
        self._token_path = normalized_state_root / "auth" / "owner-token.json"
        self._locks = EntityLockRegistry(
            lock_directory=RuntimeLockLayout.lock_directory(
                state_root=normalized_state_root,
                namespace="auth",
            )
        )

    def is_configured(self) -> bool:
        return self._token_path.is_file()

    def generate(self) -> str:
        with self._locks.hold(entity_key=_OWNER_TOKEN_LOCK_KEY):
            observed_record = self._read_record_if_configured()

        owner_token, replacement_record = self._prepare_replacement()

        with self._locks.hold(entity_key=_OWNER_TOKEN_LOCK_KEY):
            if self._read_record_if_configured() != observed_record:
                raise OwnerTokenRevisionConflictError(
                    "Owner Token changed during rotation."
                )
            write_json_model(path=self._token_path, record=replacement_record)
        return owner_token

    def ensure_configured(self) -> str | None:
        with self._locks.hold(entity_key=_OWNER_TOKEN_LOCK_KEY):
            if self._read_record_if_configured() is not None:
                return None

        owner_token, record = self._prepare_replacement()

        with self._locks.hold(entity_key=_OWNER_TOKEN_LOCK_KEY):
            if self._read_record_if_configured() is not None:
                return None
            write_json_model(path=self._token_path, record=record)
        return owner_token

    def verify(self, *, candidate: str) -> bool:
        with self._locks.hold(entity_key=_OWNER_TOKEN_LOCK_KEY):
            observed_record = self._read_record_if_configured()
        if observed_record is None:
            return False

        salt = bytes.fromhex(observed_record.salt_hex)
        with _VERIFY_KDF_ADMISSION:
            candidate_digest = _derive_digest(token=candidate, salt=salt)
        if not hmac.compare_digest(candidate_digest.hex(), observed_record.digest_hex):
            return False

        with self._locks.hold(entity_key=_OWNER_TOKEN_LOCK_KEY):
            return self._read_record_if_configured() == observed_record

    def _prepare_replacement(self) -> tuple[str, OwnerTokenRecord]:
        owner_token = f"cto_owner_{secrets.token_urlsafe(32)}"
        salt = secrets.token_bytes(16)
        digest = _derive_digest(token=owner_token, salt=salt)
        return owner_token, OwnerTokenRecord(
            salt_hex=salt.hex(),
            digest_hex=digest.hex(),
        )

    def _read_record_if_configured(self) -> OwnerTokenRecord | None:
        if not self._token_path.is_file():
            return None
        return read_json_model(
            path=self._token_path,
            record_type=OwnerTokenRecord,
        )


def _derive_digest(*, token: str, salt: bytes) -> bytes:
    return hashlib.scrypt(
        token.encode("utf-8"),
        salt=salt,
        n=2**14,
        r=8,
        p=1,
        dklen=32,
    )
