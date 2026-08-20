# Architecture and Boundary Rules

## 기본 흐름

```text
Router / MCP Tool
→ Input Pydantic Contract
→ Application Service
→ Internal DTO / Domain Operation
→ Repository / Obsidian / Search Adapter
→ Output Pydantic Contract
```

## Router

Router는 다음만 담당한다.

- 외부 입력 수신
- Pydantic Validation
- 인증 또는 환경 Context 전달
- Application Service 호출
- 구조화된 Response 반환

Router에 다음을 넣지 않는다.

- Scope와 Lifecycle 핵심 규칙
- File Move와 상태 전환
- SQL Query 조립
- Search Aggregation
- Frontmatter Parsing
- 재시도와 복구 알고리즘

## Service

Service는 Use Case와 불변식을 소유한다.

- Validation 이후 업무 규칙
- Lifecycle Transition
- 여러 Repository 작업 조정
- Idempotency
- Audit Result 조립

Service가 FastAPI Request 객체에 직접 의존하지 않도록 한다.

## Repository

Repository는 저장과 조회를 담당한다.

- 업무 정책을 임의로 결정하지 않는다.
- Router Response를 만들지 않는다.
- Raw Dictionary를 상위 계층에 그대로 반환하지 않는다.
- ORM Entity와 Internal DTO 변환 경계를 명확히 한다.

## Obsidian

Frontmatter Parser와 Canonical Mapper를 분리한다.

```text
Raw YAML Mapping
→ TypedDict 또는 제한된 Raw Mapping
→ Pydantic Frontmatter Validation
→ Internal DTO
```

## Search

FTS, Vector, Hybrid, Graph는 검색 Adapter다.

검색 Source의 Transport Shape가 Service까지 확산되지 않도록 Result Contract로 정규화한다.
