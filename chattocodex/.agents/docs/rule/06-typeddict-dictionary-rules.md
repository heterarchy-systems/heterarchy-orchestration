# TypedDict and Dictionary Rules

## Raw Dictionary 금지

알려진 Field 구조를 Production Code에서 생 `dict[str, Any]` 또는 `dict[str, object]`로 전달하지 않는다.

다음처럼 Shape가 알려져 있으면 명시적 타입을 만든다.

- TypedDict
- Pydantic Model
- Dataclass
- SQLAlchemy Model
- Enum 또는 Literal

## TypedDict 사용 대상

TypedDict는 아직 Pydantic Canonical Contract가 아닌 Mapping Shape에 사용한다.

- Raw YAML Frontmatter
- 부분적 Metadata
- 외부 Library가 반환하는 Mapping
- SQL Row Mapping
- JSON Transport 중간 Shape
- Optional Key Mutation이 필요한 Builder
- Legacy Payload Adapter

## Required와 Optional Key

모든 Key가 필수:

```python
class RawContextRow(TypedDict):
    context_id: str
    content: str
```

모든 Key가 선택적:

```python
class PartialFrontmatter(TypedDict, total=False):
    project: str
    agent_id: str
```

필수와 선택이 섞이는 경우에만 `Required`와 `NotRequired`를 사용한다.

Key 부재와 값의 None을 구분한다.

```python
class Example(TypedDict):
    required_nullable: str | None
    optional_non_null: NotRequired[str]
```

## TypedDict의 한계

TypedDict는 Runtime Validation을 수행하지 않는다.

다음 단계로 넘어가기 전 외부 값은 Pydantic 또는 명시적 Parser로 검증한다.

```text
Raw Mapping
→ TypedDict로 Shape 표현
→ Normalize
→ Pydantic Validate
→ Internal Dataclass
```

## Construction

가능하면 TypedDict 이름을 사용해 생성한다.

```python
payload = RawContextRow(
    context_id=context_id,
    content=content,
)
```

단순 Annotation으로 Raw Dict를 정당화하지 않는다.

## Dynamic JSON

실제로 동적인 JSON 경계는 제한된 Alias를 사용한다.

```python
JsonScalar = str | int | float | bool | None
JsonValue = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
```

동적 JSON Type은 Transport 또는 Metadata 경계 밖으로 확산하지 않는다.

## Cast 금지

`cast(KnownPayload, raw_dict)`로 검증되지 않은 Shape를 통과시키지 않는다.

Parse, Validate, Normalize 중 하나를 수행한다.
