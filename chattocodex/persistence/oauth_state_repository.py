import hashlib
from pathlib import Path

from mcp.shared.auth import OAuthClientInformationFull

from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.persistence.atomic_json_file import read_json_model, write_json_model
from chattocodex.persistence.runtime_lock_layout import RuntimeLockLayout
from chattocodex.schemas.auth_schemas import (
    AccessTokenRecord,
    AuthorizationCodeRecord,
    OAuthClientRecord,
    PendingAuthorizationRecord,
    RefreshTokenRecord,
)


def _token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class OAuthClientRepository:
    def __init__(self, *, state_root: Path) -> None:
        self._root = state_root / "auth" / "clients"

    def save(self, *, client: OAuthClientInformationFull) -> None:
        write_json_model(
            path=self._path(client_id=client.client_id),
            record=OAuthClientRecord(
                client_id=client.client_id,
                client_json=client.model_dump_json(exclude_none=False),
            ),
        )

    def get(self, *, client_id: str) -> OAuthClientInformationFull | None:
        path = self._path(client_id=client_id)
        if not path.is_file():
            return None
        record = read_json_model(path=path, record_type=OAuthClientRecord)
        return OAuthClientInformationFull.model_validate_json(record.client_json)

    def _path(self, *, client_id: str) -> Path:
        safe_name = hashlib.sha256(client_id.encode("utf-8")).hexdigest()
        return self._root / f"{safe_name}.json"


class OAuthAuthorizationRepository:
    def __init__(
        self,
        *,
        state_root: Path,
        lock_registry: EntityLockRegistry | None = None,
    ) -> None:
        auth_root = state_root / "auth"
        self._pending_root = auth_root / "pending"
        self._code_root = auth_root / "codes"
        self._locks = lock_registry or EntityLockRegistry(
            lock_directory=RuntimeLockLayout.lock_directory(
                state_root=state_root,
                namespace="authentication",
            )
        )

    def save_pending(self, *, record: PendingAuthorizationRecord) -> None:
        write_json_model(
            path=self._pending_root / f"{_token_digest(record.pending_id)}.json",
            record=record,
        )

    def get_pending(self, *, pending_id: str) -> PendingAuthorizationRecord | None:
        path = self._pending_root / f"{_token_digest(pending_id)}.json"
        if not path.is_file():
            return None
        return read_json_model(path=path, record_type=PendingAuthorizationRecord)

    def delete_pending(self, *, pending_id: str) -> None:
        digest = _token_digest(pending_id)
        with self._locks.hold(entity_key=f"oauth:pending:{digest}"):
            (self._pending_root / f"{digest}.json").unlink(missing_ok=True)

    def consume_pending_and_save_code(
        self,
        *,
        pending_id: str,
        expected_record: PendingAuthorizationRecord,
        raw_code: str,
        code_record: AuthorizationCodeRecord,
        now: float,
    ) -> bool:
        """Convert one live pending request into one authorization code."""

        digest = _token_digest(pending_id)
        pending_path = self._pending_root / f"{digest}.json"
        with self._locks.hold(entity_key=f"oauth:pending:{digest}"):
            if not pending_path.is_file():
                return False
            current = read_json_model(
                path=pending_path,
                record_type=PendingAuthorizationRecord,
            )
            if current.expires_at < now:
                pending_path.unlink(missing_ok=True)
                return False
            if current != expected_record:
                return False
            code_path = self._code_root / f"{_token_digest(raw_code)}.json"
            pending_path.unlink()
            try:
                write_json_model(path=code_path, record=code_record)
            except BaseException:
                code_path.unlink(missing_ok=True)
                write_json_model(path=pending_path, record=current)
                raise
            return True

    def save_code(self, *, raw_code: str, record: AuthorizationCodeRecord) -> None:
        write_json_model(
            path=self._code_root / f"{_token_digest(raw_code)}.json",
            record=record,
        )

    def get_code(self, *, raw_code: str) -> AuthorizationCodeRecord | None:
        path = self._code_root / f"{_token_digest(raw_code)}.json"
        if not path.is_file():
            return None
        return read_json_model(path=path, record_type=AuthorizationCodeRecord)

    def delete_code(self, *, raw_code: str) -> None:
        digest = _token_digest(raw_code)
        with self._locks.hold(entity_key=f"oauth:code:{digest}"):
            (self._code_root / f"{digest}.json").unlink(missing_ok=True)

    def consume_code(
        self,
        *,
        raw_code: str,
        client_id: str,
        now: float,
    ) -> AuthorizationCodeRecord | None:
        """Return and remove one live code for exactly one caller."""

        digest = _token_digest(raw_code)
        path = self._code_root / f"{digest}.json"
        with self._locks.hold(entity_key=f"oauth:code:{digest}"):
            if not path.is_file():
                return None
            record = read_json_model(path=path, record_type=AuthorizationCodeRecord)
            if record.expires_at < now:
                path.unlink(missing_ok=True)
                return None
            if record.client_id != client_id:
                return None
            path.unlink()
            return record


class OAuthTokenRepository:
    def __init__(
        self,
        *,
        state_root: Path,
        lock_registry: EntityLockRegistry | None = None,
    ) -> None:
        auth_root = state_root / "auth"
        self._access_root = auth_root / "access"
        self._refresh_root = auth_root / "refresh"
        self._locks = lock_registry or EntityLockRegistry(
            lock_directory=RuntimeLockLayout.lock_directory(
                state_root=state_root,
                namespace="authentication",
            )
        )

    def save_access(self, *, raw_token: str, record: AccessTokenRecord) -> None:
        write_json_model(
            path=self._access_root / f"{_token_digest(raw_token)}.json",
            record=record,
        )

    def save_refresh(self, *, raw_token: str, record: RefreshTokenRecord) -> None:
        write_json_model(
            path=self._refresh_root / f"{_token_digest(raw_token)}.json",
            record=record,
        )

    def get_access(self, *, raw_token: str) -> AccessTokenRecord | None:
        path = self._access_root / f"{_token_digest(raw_token)}.json"
        if not path.is_file():
            return None
        return read_json_model(path=path, record_type=AccessTokenRecord)

    def get_refresh(self, *, raw_token: str) -> RefreshTokenRecord | None:
        path = self._refresh_root / f"{_token_digest(raw_token)}.json"
        if not path.is_file():
            return None
        return read_json_model(path=path, record_type=RefreshTokenRecord)

    def delete_access(self, *, raw_token: str) -> None:
        digest = _token_digest(raw_token)
        with self._locks.hold(entity_key=f"oauth:access:{digest}"):
            (self._access_root / f"{digest}.json").unlink(missing_ok=True)

    def delete_refresh(self, *, raw_token: str) -> None:
        digest = _token_digest(raw_token)
        with self._locks.hold(entity_key=f"oauth:refresh:{digest}"):
            (self._refresh_root / f"{digest}.json").unlink(missing_ok=True)

    def rotate_refresh(
        self,
        *,
        raw_token: str,
        client_id: str,
        now: int,
        new_access_token: str,
        new_access_record: AccessTokenRecord,
        new_refresh_token: str,
        new_refresh_record: RefreshTokenRecord,
    ) -> bool:
        """Replace one live refresh token exactly once under its entity lock."""

        digest = _token_digest(raw_token)
        path = self._refresh_root / f"{digest}.json"
        with self._locks.hold(entity_key=f"oauth:refresh:{digest}"):
            if not path.is_file():
                return False
            current = read_json_model(path=path, record_type=RefreshTokenRecord)
            if current.expires_at < now:
                path.unlink(missing_ok=True)
                return False
            if current.client_id != client_id:
                return False
            if (
                new_access_record.client_id != current.client_id
                or new_refresh_record.client_id != current.client_id
                or new_access_record.scopes != new_refresh_record.scopes
                or not set(new_refresh_record.scopes).issubset(current.scopes)
            ):
                return False
            new_access_path = (
                self._access_root / f"{_token_digest(new_access_token)}.json"
            )
            new_refresh_path = (
                self._refresh_root / f"{_token_digest(new_refresh_token)}.json"
            )
            path.unlink()
            try:
                write_json_model(
                    path=new_access_path,
                    record=new_access_record,
                )
                write_json_model(
                    path=new_refresh_path,
                    record=new_refresh_record,
                )
            except BaseException:
                new_access_path.unlink(missing_ok=True)
                new_refresh_path.unlink(missing_ok=True)
                write_json_model(path=path, record=current)
                raise
            return True
