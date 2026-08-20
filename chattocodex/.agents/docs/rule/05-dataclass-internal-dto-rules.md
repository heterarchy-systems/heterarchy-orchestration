# Dataclass and Internal DTO Rules

## 기본 역할

Pydantic이 외부·검증 Contract라면 Dataclass는 검증을 통과한 내부 값 전달에 사용한다.

권장 기본:

```python
from dataclasses import dataclass

@dataclass(frozen=True, slots=True, kw_only=True)
class ContextIdentity:
    workspace_id: str
    project: str
    agent_id: str | None
    session_id: str | None
```

## 사용 대상

- Service와 Repository 사이의 Internal DTO
- Parser 이후 정규화된 값
- 변경되지 않아야 하는 Snapshot
- Search Candidate
- Mapping Result
- Application Service 내부 Command 또는 Result
- 외부 직렬화가 주 목적이 아닌 값 객체

## 사용하지 않는 대상

- FastAPI Request와 Response
- MCP Public Contract
- Raw Frontmatter
- ORM Entity
- Validation이 핵심 책임인 객체
- JSON Schema 생성이 필요한 객체

## 기본 옵션

Internal DTO는 특별한 이유가 없다면 다음을 사용한다.

- `frozen=True`
- `slots=True`
- `kw_only=True`

## Mutable Dataclass

다음처럼 실제 누적 상태가 필요할 때만 Mutable Dataclass를 허용한다.

- Reindex Counter
- Streaming Accumulator
- Batch Assembly State

Mutable인 이유를 클래스 Docstring 또는 인접한 짧은 Comment로 설명한다.

## Validation

Dataclass의 `__post_init__`에 외부 입력 검증을 대량으로 넣지 않는다.

외부 검증은 Pydantic Boundary에서 수행한다.

Dataclass에는 이미 검증된 값이 들어온다는 Contract를 유지한다.

단, 내부 불변식이 Dataclass 자체 의미인 경우 작은 방어 Validation은 허용한다.

## 변환

Pydantic ↔ Dataclass ↔ ORM 변환은 명명된 Mapper가 소유한다.

좋은 예:

- `ContextRequestMapper`
- `ContextEntityMapper`
- `FrontmatterContextMapper`

`model_dump()` 결과를 여러 계층에서 임의로 `**kwargs` 전달하지 않는다.

## 중복 모델 방지

모든 Pydantic Model에 대응하는 Dataclass를 자동 생성하지 않는다.

다음 질문에 답이 있을 때만 분리한다.

- 외부 Contract와 내부 표현이 실제로 다른가?
- 내부 불변성과 Public Schema가 다른가?
- ORM과 분리할 실익이 있는가?
- 테스트 또는 변경 안정성이 개선되는가?
