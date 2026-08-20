---
name: fastapi-production-backend
description: Production FastAPI architecture, lifespan/resource ownership, Pydantic v2 schema discipline, routing, testing, performance, and integration guidance for heterarchy-orchestration. Apply before modifying FastAPI HTTP or MCP-hosting boundaries.
argument-hint: "[backend boundary or use case]"
license: MIT
metadata:
  author: heterarchy-systems
  version: "1.0.0"
  researched_at: "2026-08-19"
---

# FastAPI Production Backend Skill

## Authority

This skill is reusable methodology. Repository-local rules under `chattocodex/.agents/docs/rule/` are mandatory and win on conflict.

Primary sources reviewed:

- FastAPI: Bigger Applications / Multiple Files
- FastAPI: Dependencies and `Annotated`
- FastAPI: Lifespan Events
- FastAPI: Response Model / Return Type
- FastAPI: Return a Response Directly
- Dependency Injector FastAPI examples where framework integration matters

## Architecture

Use a thin-interface architecture:

```text
FastAPI / MCP transport
  -> Pydantic boundary validation
  -> application service / coordinator
  -> repository ports
  -> infrastructure adapters / ORM
```

Path operations validate, authorize, map, and delegate. They do not duplicate business transitions, capacity policy, persistence logic, or Broker authority.

For larger applications, split routers by bounded context and compose them in an application factory. Avoid one giant `main.py` or route module.

## Application Factory

Prefer `create_app()` over import-time resource initialization. Importing a module must not open database pools, start background workers, or perform remote I/O.

```python

def create_app() -> FastAPI:
    container = ApplicationContainer()
    app = FastAPI(lifespan=build_lifespan(container))
    app.include_router(project_router)
    app.include_router(group_folder_router)
    return app
```

The application factory owns composition only. Business services remain framework-agnostic.

## Lifespan and Resources

Use FastAPI/ASGI lifespan for resources shared across requests and requiring deterministic shutdown:

- SQLAlchemy async engine
- connection pools
- async HTTP clients
- provider clients
- application-wide DI resources

Do not use deprecated startup/shutdown event handlers for new code when lifespan can own the resource.

Never open a new engine/pool per request. Never keep a request-scoped ORM session as global mutable state.

## Dependency Boundaries

FastAPI's dependency system is request-boundary glue. Use `Annotated[T, Depends(...)]` for request-scoped HTTP concerns and injected application services.

When `dependency-injector` is present:

- object graph assembly belongs to the DI container;
- FastAPI `Depends` bridges the route to a `Provide[...]` marker;
- application/domain services must not call the container as a service locator.

Keep `@inject` immediately above the function and below the FastAPI route decorator when using Dependency Injector wiring.

## Async Rules

Use `async def` for code that directly awaits async I/O. Do not mark CPU-bound or blocking functions async merely for style.

Blocking file/process/database-driver operations must not run on the event-loop thread. Use an async-native adapter or an explicit thread/process boundary.

Avoid unbounded `asyncio.gather()` over user-controlled collections. Concurrency must have a repository policy and explicit bounds.

## Pydantic v2 Boundaries

All named-field backend schemas inherit the repository `StrictSchemaModel`. Root-value schemas inherit `StrictRootSchemaModel`.

Do not subclass Pydantic `BaseModel` or `RootModel` directly outside the canonical base module.

Every externally visible field requires a meaningful description through the repository field helper. Do not call `pydantic.Field()` directly in production schema modules.

Actual domain/protocol defaults are declared at the annotation assignment, not via `Field(default=...)` or `Field(default_factory=...)`.

Top-level collection contracts are named RootModels instead of bare `list[Item]` / `tuple[Item, ...]` API or MCP schemas. Named models improve OpenAPI/MCP schema identity, documentation, evolution, and type reuse.

## Response Models and Performance

Prefer typed Pydantic response models / return types. FastAPI can serialize response models using Pydantic's Rust-backed serializer and also validates/filters output.

Do not construct `JSONResponse` manually for ordinary JSON merely as an optimization. Direct `Response` subclasses are reserved for cases that intentionally bypass normal Pydantic conversion or use non-JSON/custom streaming behavior.

Avoid serializing ORM entities by dumping `__dict__`. Map ORM records to explicit response/domain objects.

## Error Mapping

Domain/application exceptions remain framework-independent. Map them to HTTP status/error schemas at the interface layer. Do not raise HTTP exceptions deep in repositories or domain services.

Errors must use stable codes/shapes where clients depend on them.

## Database Request Lifecycle

Use an async SQLAlchemy session factory with one session per unit of work/request boundary unless a service explicitly owns a wider transaction.

- Engine/pool: application lifespan.
- Session: request/use-case scope.
- Transaction: application service/use-case boundary.
- ORM model: infrastructure layer.
- Domain DTO/entity: framework-agnostic layer.

Do not share one `AsyncSession` across concurrently executing tasks.

## Testing

Test layers separately:

1. domain/application unit tests without FastAPI;
2. repository integration tests against PostgreSQL where SQL behavior matters;
3. FastAPI ASGI tests for routing/validation/error mapping;
4. MCP transport tests independently;
5. real cross-service E2E only when making a real-integration claim.

Provider/DI overrides should make tests deterministic without weakening production contracts.

## Performance Checklist

Optimize only after correctness and measurement.

High-value defaults:

- application-lifetime engine and connection pool;
- request-scoped sessions;
- typed Pydantic response models;
- no repeated validation of already-validated internal DTOs;
- no repeated full-table/tree scans when indexed queries can answer the use case;
- pagination/bounds for collections;
- select only required ORM columns for hot read paths when a full entity is unnecessary;
- avoid N+1 relationships; choose loader strategy deliberately;
- avoid duplicate model serialization/mapping passes;
- keep MCP/HTTP adapters thin.

## HETERARCHY-specific Boundary

FastAPI belongs to `heterarchy-orchestration`, not `heterarchy-broker`. Broker remains provider/framework neutral.

Project / GroupFolder / Team configuration is orchestration control-plane state. Broker remains authoritative for actual ConsumerGroup membership, generation, task ownership, leases and fencing.

## Completion Gate

Before reporting a FastAPI backend slice complete:

- repository rules loaded;
- focused tests pass;
- Ruff format/lint pass;
- Pyrefly passes;
- relevant DB/MCP integration test passes;
- canonical `make ci` passes or every pre-existing blocker is reported precisely;
- no unexecuted E2E is described as PASS.
