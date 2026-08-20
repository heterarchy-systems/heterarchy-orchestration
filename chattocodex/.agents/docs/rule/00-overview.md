# HETERARCHY Orchestration Rules Overview

## 저장소 성격

`heterarchy-orchestration`은 Main Chat과 N개의 Agent Runtime을 연결하는 **provider-aware orchestration / MCP backend**다.

주요 책임:

- Main-facing MCP / FastAPI boundary
- Project와 Provider Binding
- Run / DAG / dependency / requested parallelism
- GroupFolder와 Team(ConsumerGroup desired specification) 관리
- Worker/Consumer Runtime provisioning, reuse, wake, spawn
- Pydantic validation contract
- PostgreSQL + SQLAlchemy control-plane persistence
- Broker client integration
- Main Review / workflow lifecycle

Broker의 authoritative ConsumerGroup membership, generation, task ownership, lease, epoch, fencing은 이 저장소가 소유하지 않는다.

## Canonical Authority Boundary

> Broker decides who owns the work; Orchestration decides who to make work.

- Orchestration은 Consumer Runtime을 만든다/재사용한다.
- Orchestration은 desired Team topology를 정하고 Broker에 `ensure_group` / `join`을 요청한다.
- Broker `GroupCoordinator`가 실제 ConsumerGroup membership과 generation을 authoritative하게 관리한다.
- Folder/Department, GPT Project, Provider Runtime은 Broker domain state에 들어가지 않는다.

## 모델 역할 분리

```text
FastAPI / MCP / JSON / Config boundary
  -> Strict Pydantic schema

검증 후 내부 전달
  -> frozen dataclass / narrow typed value

PostgreSQL persistence
  -> SQLAlchemy ORM model + repository

알려진 partial mapping
  -> TypedDict

Broker coordination
  -> typed Broker gateway / SDK
```

동일 책임을 이유 없이 Pydantic, dataclass, ORM으로 중복 모델링하지 않는다. 표현이 다른 책임을 가질 때만 mapper를 둔다.

## Mandatory Skills

FastAPI / ORM / DI / MCP backend 변경에서는 다음을 먼저 읽는다.

- `chattocodex/.agents/fastapi-skill/SKILL.md`
- `chattocodex/.agents/dependency-injector-skill/SKILL.md`

Frontend 변경에서는 `frontend/.agents/skills/` 아래 현재 작업과 관련된 Skill을 먼저 읽는다.

## 개발 철학

- Contract와 authority를 명시적으로 만든다.
- Raw 입력은 경계에서 멈춘다.
- 내부 계층은 검증된 값만 전달한다.
- invalid state를 타입/스키마/DB 제약으로 가능한 한 표현 불가능하게 만든다.
- 생 `dict[str, Any]`, broad ignore, convenience Optional로 설계를 흐리지 않는다.
- 기존 abstraction/library를 먼저 재사용하고, 성능은 측정 가능한 병목에서 최적화한다.
- 기존 dirty worktree를 보존한다.
- focused regression -> canonical harness 순으로 검증한다.
- 실행하지 않은 E2E/CI를 PASS로 주장하지 않는다.

## Routing

- Pydantic/FastAPI/DI/ORM/MCP backend: `01`, `02`, `03`, `04`, `05`, `06`, `07`, `08`, `11`, `12`, `13`, `16`
- Project identity / binding: `01`, `03`, `09`, `10`, `16`
- Durable concurrency / repository: `01`, `07`, `10`, `11`, `12`, `13`
- Scoped refactor: `03`, `07`, `08`, `13`, `14`, `15`
