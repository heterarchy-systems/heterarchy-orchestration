---
name: python-dependency-injector-production
description: Production use of python-dependency-injector with FastAPI, async resources, SQLAlchemy, test overrides, explicit wiring, provider scoping, and clean-architecture boundaries.
argument-hint: "[container, provider, or injection boundary]"
license: MIT
metadata:
  author: heterarchy-systems
  version: "1.0.0"
  researched_at: "2026-08-19"
---

# Python Dependency Injector Production Skill

## Authority

This skill is reusable methodology. Repository-local rules under `chattocodex/.agents/docs/rule/` are mandatory and win on conflict.

Primary sources reviewed from the official Dependency Injector documentation:

- FastAPI example
- FastAPI + SQLAlchemy example
- FastAPI + Redis example
- Providers overview
- Factory provider
- Singleton provider
- Resource provider and ASGI Lifespan support
- Wiring
- Asynchronous injections
- Provider overriding / testing

## Core Principle

Use Dependency Injector to make object assembly explicit. The container is the composition root, not a runtime service locator.

```text
Container
  -> Configuration
  -> shared Resources
  -> Repository adapters
  -> Services
  -> Coordinators
  -> FastAPI/MCP boundary injection
```

Business code receives dependencies in constructors/functions and does not import or call the container.

## Declarative Container

Prefer `containers.DeclarativeContainer` for the application composition root. Keep provider definitions centralized and named by responsibility.

```python
class ApplicationContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    database = providers.Resource(...)
    project_repository = providers.Factory(...)
    project_service = providers.Factory(...)
```

For large independent packages, subcontainers are acceptable when they preserve clear ownership. Do not split containers merely to mirror every module.

## Provider Selection

### Factory

Use `providers.Factory` for lightweight stateful/use-case objects that should receive fresh construction at injection time:

- application services when they are intentionally stateless wrappers over dependencies;
- repositories that bind a request/unit-of-work session;
- mappers or strategy objects with per-use state.

### Singleton

Use `providers.Singleton` only for genuinely application-scoped objects whose lifetime and concurrency semantics are safe:

- immutable configuration adapters;
- stateless registries;
- thread/async-safe clients documented for shared use.

Do not use Singleton for SQLAlchemy `Session` / `AsyncSession` or mutable request state.

### Resource

Use `providers.Resource` for initialize/yield/shutdown lifecycles:

- SQLAlchemy engine / async session factory owner;
- async HTTP client;
- connection pool;
- long-lived provider client;
- background runtime resource with deterministic cleanup.

Prefer ASGI lifespan integration so resources start before request handling and close at shutdown.

### Configuration

Use `providers.Configuration` or a validated Pydantic Settings object as the configuration boundary. Environment parsing belongs at startup/composition, not scattered through services.

## FastAPI Integration

Official integration pattern:

```python
@router.get("/items")
@inject
async def list_items(
    service: Annotated[
        ItemService,
        Depends(Provide[ApplicationContainer.item_service]),
    ],
) -> ItemListResponse:
    return await service.list_items()
```

Rules:

- FastAPI route decorator is outer; `@inject` is immediately above the function so it is applied first.
- Prefer `Annotated[T, Depends(Provide[...])]` for endpoint injection.
- Wire only modules/packages that actually contain injection markers.
- Keep route functions thin.

FastAPI's dependency graph and Dependency Injector have different responsibilities: FastAPI owns request extraction/security/context; Dependency Injector owns application object assembly.

## ASGI Lifespan

Dependency Injector provides `dependency_injector.ext.starlette.Lifespan` for Resource providers. Use it or an explicit application lifespan adapter so Resource startup/shutdown is deterministic.

Do not initialize containers/resources through import-time side effects.

If an MCP ASGI app and FastAPI app both have lifespan requirements, compose lifespan explicitly rather than assuming nested mounts execute child lifespan automatically.

## Async Injection

Providers support async mode. If any injection is awaitable, the provider can switch into async mode and dependency resolution can occur concurrently.

Rules:

- know whether a provider returns an awaitable;
- `await` async-mode providers consistently;
- avoid accidental mode switching caused by mixing sync and async resource implementations;
- use explicit async Resource initializers for async engines/clients;
- do not hide blocking I/O behind an async provider.

## SQLAlchemy Pattern

Recommended ownership:

```text
Resource: AsyncEngine / sessionmaker owner
     ↓
request/use-case session
     ↓
Factory: Repository(session)
     ↓
Factory: Service(repository)
```

The engine/pool is application scoped. `AsyncSession` is unit-of-work/request scoped and must not be shared by concurrent tasks.

A repository receives a session; it does not create a new engine or read environment variables.

Transaction commit/rollback ownership must be explicit. Prefer the application service/unit-of-work boundary rather than hidden commits in arbitrary repository methods unless the repository contract explicitly owns a transaction.

## Wiring Discipline

Use `wiring_config` or explicit `container.wire(modules=[...])` / packages deliberately.

Avoid:

- wiring the whole source tree by default;
- `Provide[...]` markers in domain entities;
- importing the Container inside services;
- runtime `container.service()` calls from business logic;
- provider definitions that create circular package dependencies.

## Provider Overrides in Tests

Use provider override contexts for deterministic tests:

```python
with container.external_client.override(mock_client):
    ...
```

Overrides are test composition, not production branching. Do not add `if testing:` behavior to services when provider substitution can express the dependency.

Reset singleton/resource state between tests when test isolation requires it.

## Scope and State Rules

- Container owns assembly, not business state.
- Repository owns persistence interaction.
- Service owns use-case invariants.
- Coordinator owns cross-service policy.
- Request/MCP handler owns transport validation/delegation.
- Broker authority is never duplicated inside the DI container.

## Performance

Dependency Injector is Cython-backed, but DI overhead should still stay outside tight row-processing loops.

Performance guidelines:

- inject services once at request/tool entry rather than resolving providers repeatedly inside loops;
- keep shared expensive clients/resources application scoped;
- keep request/session state scoped narrowly;
- avoid constructing identical immutable configuration objects repeatedly;
- avoid provider graphs with unnecessary wrapper layers;
- do not turn every small pure function into an injected service.

## Error Handling

Provider/configuration initialization errors should fail startup clearly when the application cannot operate safely. Do not defer missing mandatory configuration until an arbitrary request.

Domain/application exceptions stay independent of Dependency Injector types.

## HETERARCHY Composition Recommendation

Use a root orchestration container with explicit providers for:

```text
settings
async database resource
session factory / unit-of-work factory
project repositories/services
group-folder repositories/services
team repositories/services
team resolver
worker runtime coordinator
Broker gateway
FastAPI/MCP composition
```

Broker remains an external coordination dependency; do not represent Broker ConsumerGroup membership/generation/lease state as DI-owned mutable state.

## Completion Gate

Before reporting DI work complete:

- container can be created without import-time network/DB side effects;
- resource startup/shutdown is tested;
- endpoint/tool injection is wired and type-checked;
- provider override tests exist for external dependencies;
- no service-locator calls remain in application/domain code;
- focused tests pass;
- canonical `make ci` passes or exact pre-existing blockers are reported.
