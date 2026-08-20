from typing import Annotated, Literal

from pydantic import StringConstraints

from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.contracts.types.control_plane.auth_types import (
    ClientId,
    OpaqueToken,
    ScopeName,
    TokenDigest,
    UrlText,
)


class AuthPersistenceModel(StrictSchemaModel):
    pass


class OwnerTokenRecord(AuthPersistenceModel):
    schema_version: Literal[1] = 1
    salt_hex: Annotated[str, StringConstraints(strict=True, pattern=r"^[0-9a-f]{32}$")]
    digest_hex: TokenDigest


class OAuthClientRecord(AuthPersistenceModel):
    schema_version: Literal[1] = 1
    client_id: ClientId
    client_json: Annotated[
        str,
        StringConstraints(strict=True, min_length=2),
        described_field(description="Client json for this o auth client record."),
    ]


class PendingAuthorizationRecord(AuthPersistenceModel):
    schema_version: Literal[1] = 1
    pending_id: OpaqueToken
    client_id: ClientId
    state: str | None
    scopes: tuple[ScopeName, ...]
    code_challenge: OpaqueToken
    redirect_uri: UrlText
    redirect_uri_provided_explicitly: bool
    resource: UrlText | None
    expires_at: float


class AuthorizationCodeRecord(AuthPersistenceModel):
    schema_version: Literal[1] = 1
    code_digest: TokenDigest
    client_id: ClientId
    scopes: tuple[ScopeName, ...]
    code_challenge: OpaqueToken
    redirect_uri: UrlText
    redirect_uri_provided_explicitly: bool
    resource: UrlText | None
    expires_at: float


class AccessTokenRecord(AuthPersistenceModel):
    schema_version: Literal[1] = 1
    token_digest: TokenDigest
    client_id: ClientId
    scopes: tuple[ScopeName, ...]
    expires_at: int
    resource: UrlText | None


class RefreshTokenRecord(AuthPersistenceModel):
    schema_version: Literal[1] = 1
    token_digest: TokenDigest
    client_id: ClientId
    scopes: tuple[ScopeName, ...]
    expires_at: int


class RuntimeConfigRecord(AuthPersistenceModel):
    schema_version: Literal[1] = 1
    workspace_root: Annotated[
        str,
        StringConstraints(strict=True, min_length=1, max_length=4096),
        described_field(description="Workspace root for this runtime config record."),
    ]
    public_url: UrlText | None
    oauth_enabled: bool = True
