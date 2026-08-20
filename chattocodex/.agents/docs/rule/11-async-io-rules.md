# Async and I/O Rules

## 기본 원칙

순수 Validation, Mapping, Hash, Normalization은 동기 함수로 유지한다.

실제 외부 대기가 있는 경계에만 Async를 사용한다.

좋은 Async 대상:

- HTTP Client
- MCP Remote Call
- Subprocess
- Stream Ingestion
- 비동기 Database Driver
- 외부 File Watcher

동기로 유지할 대상:

- Pydantic Validation
- Dataclass Construction
- Frontmatter Mapping
- Scope Validation
- Search Result Filtering
- Hash Calculation
- Lifecycle Decision

## Blocking I/O

Async Endpoint에서 Blocking File I/O나 Blocking Library를 직접 실행하지 않는다.

기존 저장소의 Async Boundary Wrapper가 있으면 재사용한다.

없다면 최소한의 Boundary에서 `asyncio.to_thread` 또는 저장소 표준 방식을 사용한다.

## Task Group

실제 독립적인 Concurrent I/O에만 Task Group을 사용한다.

순차 Validation이나 Schema Construction을 병렬화하지 않는다.

## Local Import

순환 의존을 피하기 위한 일반 전략으로 Local Import를 사용하지 않는다.

책임 경계를 먼저 분리한다.

외부 Runtime Boundary에서 정말 피할 수 없을 때만 이유를 기록한다.
