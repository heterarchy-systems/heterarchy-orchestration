# FastAPI, Dependency Injection, and Schema Contract Rules

## Mandatory Skills

Backend HTTP/MCP/ORM composition work must load and apply both repository skills before implementation:

- `chattocodex/.agents/fastapi-skill/SKILL.md`
- `chattocodex/.agents/dependency-injector-skill/SKILL.md`

The skills are methodology. Repository rules remain authoritative.

## FastAPI Boundary

- FastAPI is the normal HTTP application and routing boundary. Do not construct a Starlette application for ordinary HTTP control-plane routes.
- Split bounded contexts into `APIRouter` instances and compose them with `FastAPI.include_router()`.
- Direct Starlette `Request` / `Route` usage is limited to reviewed low-level ASGI, OAuth, or MCP compatibility adapters where FastAPI does not own the protocol boundary.
- FastAPI/MCP handlers validate, authorize, map, and delegate only.
- Business transitions belong in application services/coordinators.
- SQLAlchemy ORM belongs in infrastructure repositories.
- Application-wide resources use ASGI lifespan; do not initialize pools/clients at import time.
- Prefer `Annotated[..., Depends(...)]` for FastAPI dependencies.
- Keep async request paths `async def` when they await async I/O. Offload unavoidable blocking sync work with Asyncer `asyncify`; do not spread `asyncio.to_thread` across production code.
- Strict Pydantic JSON bodies whose JSON-mode semantics differ from Python-mode validation must pass through one shared FastAPI dependency using `model_validate_json()`. Do not weaken `strict=True` merely because FastAPI pre-decodes JSON.
- Typed Pydantic response models are the normal JSON response path.
- Typed Pydantic return/response models are the primary FastAPI JSON serialization path. Current FastAPI/Pydantic serializes these responses directly; do not configure the deprecated `ORJSONResponse` default.
- Manual `Response` objects are reserved for protocol seams, custom status codes, or required response headers. At those seams serialize typed Pydantic models with `model_dump_json()` instead of dumping to Python objects and serializing a second time.
- Do not use stdlib `json`, manual `JSONResponse`, or deprecated `ORJSONResponse` in production HTTP code. `orjson` may be introduced only for a non-FastAPI JSON hotspot where the boundary and measured benefit justify the extra dependency.

## Settings Boundary

- Process configuration uses `pydantic-settings` `BaseSettings` models.
- `RuntimeSettings` owns `CHATTOCODEX_*` runtime configuration; database settings use a dedicated `BaseSettings` model with a database prefix.
- Services, repositories, and route handlers do not call `os.getenv`, `os.environ.get`, or index `os.environ` to read configuration.
- Passing an inherited environment mapping to a subprocess is not configuration parsing and may use `os.environ.copy()`.

## Dependency Injector Boundary

- `containers.DeclarativeContainer` is the composition root.
- `providers.Resource` owns application-lifetime async resources such as database engines/pools.
- `providers.Factory` is the default for repositories/services requiring fresh request/unit-of-work dependencies.
- `providers.Singleton` is allowed only for genuinely application-scoped concurrency-safe objects.
- SQLAlchemy `Session` / `AsyncSession` must never be a Singleton.
- Business/application code must not import the Container or call providers as a service locator.
- For FastAPI wiring, the route decorator is outer and `@inject` is immediately above the function.
- Tests override providers rather than adding production `if testing` branches.

## Canonical Pydantic Bases

All named-field Pydantic schemas must inherit the backend-wide canonical base:

```python
class StrictSchemaModel(BaseModel):
    """Provide the backend-wide default for named-field schemas."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        use_enum_values=True,
        validate_default=True,
    )
```

All root-value schemas must inherit:

```python
RootValueT = TypeVar("RootValueT")

class StrictRootSchemaModel(RootModel[RootValueT], Generic[RootValueT]):
    """Provide the backend-wide default for root-value schemas."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        use_enum_values=True,
        validate_default=True,
    )
```

Direct `BaseModel` / `RootModel` inheritance outside the canonical schema-base module is prohibited for production schemas.

Boundary input is strict-first. Constrained strings use `Annotated[str, StringConstraints(strict=True, ...)]`; do not encode string length/pattern constraints in the description helper. Explicit `strict=False` requires a narrowly documented protocol exception and dedicated tests.

## Described Field Helper

Production schema modules do not call `pydantic.Field()` directly.

Use the repository helper whose API intentionally has **no `default` or `default_factory` parameter** and requires a non-empty `description` keyword.

Canonical usage:

```python
name: Annotated[
    str,
    described_field(description="Human-readable Team name."),
]
```

A real protocol/domain default is declared outside the helper:

```python
limit: Annotated[
    int,
    described_field(description="Maximum number of Teams returned.", ge=1, le=100),
] = 50
```

Do not write:

```python
Field(default=50, ...)
Field(default_factory=tuple, ...)
Field(...)
```

Every externally persisted/API/MCP schema field must have a meaningful description. Descriptions such as `"value"`, `"field"`, or the field name repeated without semantics do not satisfy review quality.

## Named Collection Schemas

Do not expose a bare top-level collection as an API/MCP schema:

```python
list[TeamResponse]
tuple[TeamResponse, ...]
```

Create a named `StrictRootSchemaModel` instead:

```python
class TeamResponses(StrictRootSchemaModel[tuple[TeamResponse, ...]]):
    """Return Teams in deterministic display order."""
```

Collection fields *inside* a named schema may use `tuple[...]` / `list[...]` when the parent schema owns the contract. Prefer immutable tuples for snapshots unless mutation is part of the boundary contract.

## Optional Field Filtering / model_exclude

Repeated serialization chains such as:

```python
if model.a is not None: ...
if model.b is not None: ...
if model.c is not None: ...
```

used only to build `model_dump(exclude=...)`, PATCH payloads, or response filtering must be replaced by a named Pydantic filter/value object or shared helper that computes `model_exclude` once.

The filter must:

- have an explicit typed contract;
- distinguish omitted from explicit `None` when the API requires it;
- return an immutable `set`/`frozenset`-compatible exclusion shape;
- centralize field-name ownership instead of scattering string literals;
- not hide genuine business branching behind generic reflection.

Do not introduce a generic filter merely to remove a single clear conditional.

## ORM and Persistence

- PostgreSQL is the backend relational database for new control-plane persistence.
- Use SQLAlchemy 2.x ORM with async engine/session APIs for async request paths.
- Engine/pool lifetime is application-scoped; `AsyncSession` is request/unit-of-work scoped.
- Never share one `AsyncSession` between concurrent tasks.
- Repositories do not read environment variables or create engines.
- Transaction ownership is explicit at the application service/unit-of-work boundary.
- ORM rows do not cross the infrastructure boundary as public response models.
- Add indexes/constraints from observed query and invariant requirements, not speculation.

## MCP v2 Contract

The dependency set must not silently downgrade the official `mcp` SDK below v2.

Use the official `mcp` Python SDK v2 as the single MCP implementation. Do not add the standalone `fastmcp` package. The high-level server is `MCPServer`; preserve the official v2 transport, OAuth, tool/resource/prompt, and schema contracts.

## Mechanical Gates

`make rule_check` must fail when production schema code violates the enforceable subset of these rules:

- direct `BaseModel` / `RootModel` inheritance outside the canonical base module;
- direct `Field()` use outside the canonical field-helper module;
- missing `strict=True` on canonical schema bases or `StringConstraints`;
- string length/pattern constraints expressed through `described_field` instead of `StringConstraints`;
- schema fields missing repository field metadata/description where the checker can identify them;
- direct top-level bare collection return schemas on FastAPI/MCP boundary functions;
- missing mandatory FastAPI / Dependency Injector skill files.

`make backend_check` must fail when production backend code violates the enforceable runtime subset:

- direct Starlette application construction or unreviewed direct Starlette routing/request adapters;
- production `asyncio.to_thread` instead of the shared Asyncer offload policy;
- direct environment configuration reads outside `pydantic-settings`;
- stdlib `json`, manual `JSONResponse`, deprecated `ORJSONResponse`, or deprecated FastAPI default response configuration;
- production `Any` or `dict[str, Any/object]` outside the explicitly reviewed upstream MCP override boundary;
- raw request-body parsing outside the shared strict Pydantic JSON dependency.

A mechanical checker is not a substitute for review. Naming, descriptions, DI scoping, transaction ownership, and optional-filter suitability still require human/agent review.
