# Pydantic and Validation Rules

## 사용 대상

Pydantic v2는 검증과 직렬화가 필요한 경계에 사용한다.

- FastAPI Request와 Response
- MCP Input과 Output
- JSON에서 읽은 안정된 Contract
- Runtime Configuration / Provider Binding
- Project / GroupFolder / Team Command와 Result
- Durable PostgreSQL record와 별개로 필요한 external storage contract

## 사용하지 않는 대상

다음에 기계적으로 사용하지 않는다.

- SQLAlchemy ORM Entity
- 검증 완료 후 내부 계층 간 전달 DTO
- 단순한 지역 계산 값
- 성능상 중요한 Row Processing
- Pydantic 전환의 실익이 없는 기존 내부 객체

## ConfigDict

안정된 Named-field Boundary Contract는 repository canonical `StrictSchemaModel`을 사용한다.

```python
ConfigDict(
    extra="forbid",
    frozen=True,
    strict=True,
    use_enum_values=True,
    validate_default=True,
)
```

모든 Schema에 동일 Config를 복사하지 않는다. production named-field schema가 `BaseModel`을 직접 상속하는 것도 금지한다.

다음 옵션은 전역 기본이 아니다.

- `validate_assignment=True`
- `str_strip_whitespace=True`
- `populate_by_name=True`
- `arbitrary_types_allowed=True`
- `from_attributes=True`

`strict=True`는 canonical `StrictSchemaModel` / `StrictRootSchemaModel`의 전역 기본이다. Boundary에서 Python 값의 암묵적 coercion에 의존하지 않는다.

문자열에 길이, 패턴, 공백 정규화 같은 제약이 있으면 `Annotated[str, StringConstraints(strict=True, ...)]`를 사용한다. 문자열 범위 제약을 `described_field(min_length=..., max_length=...)`에 넣지 않는다. `described_field`는 의미 설명과 숫자/컬렉션 제약에 사용한다.

정말로 coercion이 프로토콜 요구사항인 예외는 해당 Boundary에서만 좁게 허용하고, 왜 strict contract를 사용할 수 없는지 코드와 테스트로 증명한다. 일반 편의를 위한 `strict=False`는 금지한다.

## Raw Validation

외부 또는 신뢰할 수 없는 값:

```python
request = ContextCreateRequest.model_validate(raw_payload)
frontmatter = ContextFrontmatter.model_validate(raw_frontmatter)
```

JSON 문자열 또는 Byte:

```python
event = EventSchema.model_validate_json(raw_json)
```

## Internal Construction

이미 타입이 확인된 내부 값:

```python
result = ContextCreateResult(
    context_id=context_id,
    status=status,
)
```

같은 함수에서 만든 Dictionary Literal을 `model_validate`로 감싸 내부 Constructor처럼 사용하지 않는다.

## RootModel

Payload 전체가 Root Value일 때만 사용한다.

- 의미 있는 ID Collection
- 중복 금지 Collection
- Collection 자체가 Invariant를 소유하는 경우

일반 Named-field Contract에는 BaseModel을 사용한다.

API/MCP의 top-level collection은 bare `list[T]` / `tuple[T, ...]`로 노출하지 않는다. collection 자체를 설명하는 named `StrictRootSchemaModel`을 만든다.

## Enum

Scope, Lifecycle, Error Code처럼 공유되는 Symbolic Set은 Enum을 사용한다.

작은 Contract 한 곳에서만 쓰는 값은 Literal을 사용할 수 있다.

이 repository의 external/durable Pydantic schema는 canonical base의 `use_enum_values=True`를 따른다. 내부 domain logic에서 Enum object 의미가 필요한 경우 Pydantic boundary model을 그대로 domain model로 재사용하지 않는다.

## Default

Default는 Protocol 또는 Domain에 실제 기본값이 있을 때만 둔다.

Construction 편의를 위해 Required Field를 Default로 완화하지 않는다.

Pydantic metadata는 direct `Field()`가 아니라 `described_field(description=...)`를 사용한다. helper에는 `default` / `default_factory` parameter가 없다. 실제 default는 annotation assignment로 분리한다.

모든 `described_field()` 호출은 의미 있는 non-empty `description`을 반드시 가진다. rule harness가 direct Field 사용과 helper/skill 존재를 fail-closed 검증한다.

## Nullability

`T | None`은 None이 실제 유효 상태일 때만 사용한다.

누락, 미정, 빈 문자열, None을 같은 의미로 취급하지 않는다.
## Pydantic Major Version

이 저장소는 Pydantic v2 전용이다. `pydantic`과 `pydantic-settings`는 `>=2,<3` 범위로 고정하고 v1 compatibility namespace와 v1 validator API를 사용하지 않는다.

## Reusable Constrained Types

Schema 파일의 책임은 Boundary 구조와 validator에 둔다. `Annotated` + `StringConstraints` / 숫자 제약 alias가 길어지거나 여러 schema에서 재사용되면 `shared/contracts/types/<concept>_types.py`로 이동한다.

- 동일 이름의 constrained alias를 여러 schema 파일에서 복제하지 않는다.
- 한 shared contract schema 모듈에 top-level constrained alias가 4개 이상 쌓이기 전에 concept-specific `types` 모듈로 분리한다.
- 이름이 같아도 제약 의미가 다르면 합치지 말고 `RepositoryReadPath` / `RepositoryWritePath`처럼 의미를 분리한다.
- `shared/contracts/types`는 MCP와 Backend가 공유하는 schema primitive contract를 위한 위치이며 일반 utility bucket으로 사용하지 않는다.

## Filter Schema Policy

조회 조건을 여러 개의 `T | None` parameter와 반복적인 `if value is not None` chain으로 표현하지 않는다. 실제 query/search/list filter는 MCP와 Backend가 함께 사용할 수 있도록 `shared/contracts/filter_schema/<concept>_filter_schema.py`의 Pydantic v2 contract로 묶는다.

- Filter contract는 canonical `FilterSchemaModel`을 상속한다.
- Service/Repository query API는 optional filter parameter 여러 개 대신 하나의 named filter contract를 받는다.
- lifecycle state, nullable persistence field, mapper의 명시적 None 검사는 filter가 아니므로 기계적으로 FilterSchema로 바꾸지 않는다.
- FilterSchema를 `model_dump(exclude_none=True)` dictionary로 core layer에 흘리지 않는다. typed field 또는 명시적 query builder로 소비한다.

