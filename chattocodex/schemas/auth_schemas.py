from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

OpaqueToken = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=20, max_length=1024),
]
TokenDigest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
ClientId = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)
]
ScopeName = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)
]
UrlText = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=4096)
]


class AuthPersistenceModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_default=True,
    )


class OwnerTokenRecord(AuthPersistenceModel):
    schema_version: Literal[1] = 1
    salt_hex: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{32}$")]
    digest_hex: TokenDigest


class OAuthClientRecord(AuthPersistenceModel):
    schema_version: Literal[1] = 1
    client_id: ClientId
    client_json: str = Field(min_length=2)


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
    workspace_root: str = Field(min_length=1, max_length=4096)
    public_url: UrlText | None
    oauth_enabled: bool = True
