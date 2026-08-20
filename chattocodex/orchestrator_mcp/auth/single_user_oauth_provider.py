import hashlib
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode, urlparse

from asyncer import asyncify
from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    AuthorizeError,
    IdentityAssertionParams,
    RefreshToken,
    RegistrationError,
    TokenError,
    construct_redirect_uri,
)
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken

from chattocodex.orchestrator_mcp.auth.oauth_state_repository import (
    OAuthAuthorizationRepository,
    OAuthClientRepository,
    OAuthTokenRepository,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.shared.contracts.auth_schemas import (
    AccessTokenRecord,
    AuthorizationCodeRecord,
    PendingAuthorizationRecord,
    RefreshTokenRecord,
)
from chattocodex.shared.security.runtime_access_service import OwnerTokenService
from chattocodex.shared.storage.runtime_lock_layout import RuntimeLockLayout


@dataclass(frozen=True)
class _IssuedTokenPair:
    response: OAuthToken
    access_token: str
    access_record: AccessTokenRecord
    refresh_token: str
    refresh_record: RefreshTokenRecord


class SingleUserOAuthProvider:
    """MCP OAuth provider for one local owner.

    The SDK authorization-server protocol requires more than six public hooks. This
    class intentionally implements that protocol as one cohesive boundary; storage
    and owner-token verification remain delegated to separate components.
    """

    def __init__(
        self,
        state_root: Path,
        public_url: str,
        resource_url: str,
        allowed_redirect_hosts: tuple[str, ...],
        access_token_ttl_seconds: int,
        refresh_token_ttl_seconds: int,
        authorization_code_ttl_seconds: int = 300,
        pending_authorization_ttl_seconds: int = 600,
    ) -> None:
        """Initialize this object. Args: state_root, public_url, resource_url, allowed_redirect_hosts, access_token_ttl_seconds, refresh_token_ttl_seconds, authorization_code_ttl_seconds, pending_authorization_ttl_seconds. Returns: None."""
        self._public_url = public_url.rstrip("/")
        self._resource_url = resource_url
        self._allowed_redirect_hosts = frozenset(allowed_redirect_hosts)
        self._access_token_ttl_seconds = access_token_ttl_seconds
        self._refresh_token_ttl_seconds = refresh_token_ttl_seconds
        self._authorization_code_ttl_seconds = authorization_code_ttl_seconds
        self._pending_authorization_ttl_seconds = pending_authorization_ttl_seconds
        self._owner_tokens = OwnerTokenService(state_root=state_root)
        self._clients = OAuthClientRepository(state_root=state_root)
        lock_registry = EntityLockRegistry(
            lock_directory=RuntimeLockLayout.lock_directory(
                state_root=state_root,
                namespace="authentication",
            )
        )
        self._authorizations = OAuthAuthorizationRepository(
            state_root=state_root,
            lock_registry=lock_registry,
        )
        self._tokens = OAuthTokenRepository(
            state_root=state_root,
            lock_registry=lock_registry,
        )

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        """Get client. Args: client_id. Returns: OAuthClientInformationFull | None."""
        return await asyncify(self._clients.get)(client_id=client_id)

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        """Register client. Args: client_info. Returns: None."""
        redirect_uris = client_info.redirect_uris or []
        if not redirect_uris:
            raise RegistrationError(
                error="invalid_redirect_uri",
                error_description="At least one redirect URI is required.",
            )
        for redirect_uri in redirect_uris:
            hostname = urlparse(str(redirect_uri)).hostname
            if hostname not in self._allowed_redirect_hosts:
                raise RegistrationError(
                    error="invalid_redirect_uri",
                    error_description="Redirect URI host is not allowed.",
                )
        await asyncify(self._clients.save)(client=client_info)

    async def authorize(
        self,
        client: OAuthClientInformationFull,
        params: AuthorizationParams,
    ) -> str:
        """Authorize. Args: client, params. Returns: str."""
        if params.resource is None or params.resource != self._resource_url:
            raise AuthorizeError(
                error="invalid_target",
                error_description="OAuth resource must match the configured MCP endpoint.",
            )
        pending_id = f"cto_pending_{secrets.token_urlsafe(32)}"
        pending_record = PendingAuthorizationRecord(
            pending_id=pending_id,
            client_id=client.client_id,
            state=params.state,
            scopes=tuple(params.scopes or ()),
            code_challenge=params.code_challenge,
            redirect_uri=str(params.redirect_uri),
            redirect_uri_provided_explicitly=params.redirect_uri_provided_explicitly,
            resource=params.resource,
            expires_at=time.time() + self._pending_authorization_ttl_seconds,
        )
        await asyncify(self._authorizations.save_pending)(
            record=pending_record,
        )
        return (
            f"{self._public_url}/oauth/approve?{urlencode({'pending_id': pending_id})}"
        )

    async def load_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: str,
    ) -> AuthorizationCode | None:
        """Load authorization code. Args: client, authorization_code. Returns: AuthorizationCode | None."""
        record = await asyncify(self._authorizations.get_code)(
            raw_code=authorization_code,
        )
        if (
            record is None
            or record.client_id != client.client_id
            or record.expires_at < time.time()
        ):
            return None
        return AuthorizationCode(
            code=authorization_code,
            scopes=list(record.scopes),
            expires_at=record.expires_at,
            client_id=record.client_id,
            code_challenge=record.code_challenge,
            redirect_uri=record.redirect_uri,
            redirect_uri_provided_explicitly=record.redirect_uri_provided_explicitly,
            resource=record.resource,
        )

    async def exchange_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: AuthorizationCode,
    ) -> OAuthToken:
        """Exchange authorization code. Args: client, authorization_code. Returns: OAuthToken."""
        if authorization_code.client_id != client.client_id:
            raise TokenError(
                error="invalid_grant",
                error_description="Authorization code belongs to a different client.",
            )
        record = await asyncify(self._authorizations.consume_code)(
            raw_code=authorization_code.code,
            client_id=client.client_id,
            now=time.time(),
        )
        if record is None:
            raise TokenError(
                error="invalid_grant",
                error_description="Authorization code is invalid, expired, or already used.",
            )
        return await asyncify(self._issue_token_pair)(
            client_id=client.client_id,
            scopes=record.scopes,
            resource=record.resource,
        )

    async def load_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: str,
    ) -> RefreshToken | None:
        """Load refresh token. Args: client, refresh_token. Returns: RefreshToken | None."""
        record = await asyncify(self._tokens.get_refresh)(
            raw_token=refresh_token,
        )
        if (
            record is None
            or record.client_id != client.client_id
            or record.expires_at < int(time.time())
        ):
            return None
        return RefreshToken(
            token=refresh_token,
            client_id=record.client_id,
            scopes=list(record.scopes),
            expires_at=record.expires_at,
        )

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        """Exchange refresh token. Args: client, refresh_token, scopes. Returns: OAuthToken."""
        if refresh_token.client_id != client.client_id:
            raise TokenError(
                error="invalid_grant",
                error_description="Refresh token belongs to a different client.",
            )
        requested_scopes = tuple(scopes) if scopes else tuple(refresh_token.scopes)
        if not set(requested_scopes).issubset(refresh_token.scopes):
            raise TokenError(
                error="invalid_scope",
                error_description="Refresh request cannot expand the original token scope.",
            )
        issued = self._new_token_pair(
            client_id=client.client_id,
            scopes=requested_scopes,
            resource=self._resource_url,
        )
        rotated = await asyncify(self._tokens.rotate_refresh)(
            raw_token=refresh_token.token,
            client_id=client.client_id,
            now=int(time.time()),
            new_access_token=issued.access_token,
            new_access_record=issued.access_record,
            new_refresh_token=issued.refresh_token,
            new_refresh_record=issued.refresh_record,
        )
        if not rotated:
            raise TokenError(
                error="invalid_grant",
                error_description="Refresh token is invalid, expired, or already used.",
            )
        return issued.response

    async def load_access_token(self, token: str) -> AccessToken | None:
        """Load access token. Args: token. Returns: AccessToken | None."""
        record = await asyncify(self._tokens.get_access)(raw_token=token)
        if record is None or record.expires_at < int(time.time()):
            return None
        return AccessToken(
            token=token,
            client_id=record.client_id,
            scopes=list(record.scopes),
            expires_at=record.expires_at,
            resource=record.resource,
            subject="local-owner",
        )

    async def revoke_token(self, token: AccessToken | RefreshToken) -> None:
        """Revoke token. Args: token. Returns: None."""
        if isinstance(token, AccessToken):
            await asyncify(self._tokens.delete_access)(raw_token=token.token)
        else:
            await asyncify(self._tokens.delete_refresh)(raw_token=token.token)

    async def exchange_identity_assertion(
        self,
        client: OAuthClientInformationFull,
        params: IdentityAssertionParams,
    ) -> OAuthToken:
        """Exchange identity assertion. Args: client, params. Returns: OAuthToken."""
        del client, params
        raise TokenError(
            error="unsupported_grant_type",
            error_description="Identity Assertion Authorization Grant is not supported.",
        )

    async def approve_owner_authorization(
        self,
        pending_id: str,
        owner_token: str,
    ) -> str | None:
        """Approve owner authorization. Args: pending_id, owner_token. Returns: str | None."""
        pending = await asyncify(self._authorizations.get_pending)(
            pending_id=pending_id,
        )
        if pending is None or pending.expires_at < time.time():
            return None
        is_owner = await asyncify(self._owner_tokens.verify)(
            candidate=owner_token,
        )
        if not is_owner:
            return None
        client = await asyncify(self._clients.get)(client_id=pending.client_id)
        if client is None:
            return None
        if pending.redirect_uri not in [
            str(uri) for uri in (client.redirect_uris or [])
        ]:
            return None
        raw_code = f"cto_code_{secrets.token_urlsafe(32)}"
        code_record = AuthorizationCodeRecord(
            code_digest=_digest_secret(raw_code),
            client_id=pending.client_id,
            scopes=pending.scopes,
            code_challenge=pending.code_challenge,
            redirect_uri=pending.redirect_uri,
            redirect_uri_provided_explicitly=pending.redirect_uri_provided_explicitly,
            resource=pending.resource,
            expires_at=time.time() + self._authorization_code_ttl_seconds,
        )
        consumed = await asyncify(self._authorizations.consume_pending_and_save_code)(
            pending_id=pending_id,
            expected_record=pending,
            raw_code=raw_code,
            code_record=code_record,
            now=time.time(),
        )
        if not consumed:
            return None
        return construct_redirect_uri(
            pending.redirect_uri,
            code=raw_code,
            state=pending.state,
        )

    def _issue_token_pair(
        self,
        client_id: str,
        scopes: tuple[str, ...],
        resource: str | None,
    ) -> OAuthToken:
        """Issue token pair. Args: client_id, scopes, resource. Returns: OAuthToken."""
        issued = self._new_token_pair(
            client_id=client_id,
            scopes=scopes,
            resource=resource,
        )
        self._tokens.save_access(
            raw_token=issued.access_token,
            record=issued.access_record,
        )
        self._tokens.save_refresh(
            raw_token=issued.refresh_token,
            record=issued.refresh_record,
        )
        return issued.response

    def _new_token_pair(
        self,
        client_id: str,
        scopes: tuple[str, ...],
        resource: str | None,
    ) -> _IssuedTokenPair:
        """New token pair. Args: client_id, scopes, resource. Returns: _IssuedTokenPair."""
        now = int(time.time())
        access_token = f"cto_access_{secrets.token_urlsafe(32)}"
        refresh_token = f"cto_refresh_{secrets.token_urlsafe(32)}"
        access_record = AccessTokenRecord(
            token_digest=_digest_secret(access_token),
            client_id=client_id,
            scopes=scopes,
            resource=resource,
            expires_at=now + self._access_token_ttl_seconds,
        )
        refresh_record = RefreshTokenRecord(
            token_digest=_digest_secret(refresh_token),
            client_id=client_id,
            scopes=scopes,
            expires_at=now + self._refresh_token_ttl_seconds,
        )
        return _IssuedTokenPair(
            response=OAuthToken(
                access_token=access_token,
                token_type="Bearer",
                expires_in=self._access_token_ttl_seconds,
                refresh_token=refresh_token,
                scope=" ".join(scopes),
            ),
            access_token=access_token,
            access_record=access_record,
            refresh_token=refresh_token,
            refresh_record=refresh_record,
        )


def _digest_secret(value: str) -> str:
    """Digest secret. Args: value. Returns: str."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
