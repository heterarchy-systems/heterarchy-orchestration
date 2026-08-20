# Type Strictness Rules

## Production Type Policy

Production Code의 함수, 메서드, Class Field에는 명시적 타입을 작성한다. Public/Private/Nested 함수 여부로 예외를 만들지 않는다.

- 모든 함수와 메서드는 Return Type을 생략하지 않는다. `__init__`도 `-> None`을 명시한다.
- `self`, `cls`를 제외한 모든 Parameter는 명시적 타입을 가진다. `*args`, `**kwargs`도 동일하다.
- Collection Annotation은 element/key/value type을 생략한 bare `list`, `dict`, `set`, `tuple`을 사용하지 않는다.
- Pyrefly가 추론할 수 있다는 이유로 명시적 계약을 생략하지 않는다.

## Any

`Any`는 기본 금지다.

허용 가능한 경계:

- 타입 정보가 없는 Third-party Library
- Raw JSON 또는 YAML Parser의 최초 수신 지점
- Dynamic Plugin 또는 MCP Transport
- ORM 또는 Framework Hook의 피할 수 없는 경계

허용할 때 다음을 지킨다.

- 한 함수 또는 한 모듈의 Boundary에 국한한다.
- 즉시 TypedDict, Pydantic, Dataclass로 정규화한다.
- 왜 Any가 필요한지 짧게 설명하고 해당 줄 또는 바로 위 줄에 `type-contract: allow-any` 표식을 남긴다.
- Core Service와 Repository Public Surface로 전달하지 않는다.

## object

`dict[str, object]`를 Any의 대체품처럼 사용하지 않는다.

실제 Shape가 알려져 있으면 명시적 Contract를 만든다.

## Optional

Optional은 값이 실제로 None일 수 있다는 의미다.

다음 이유로 Optional을 사용하지 않는다.

- Caller가 값을 넘기기 귀찮음
- 아직 설계를 결정하지 않음
- 오류를 피하고 싶음
- 빈 문자열과 None을 구분하지 않음
- Legacy Payload를 그대로 통과시킴

## Omission과 None

- Key가 없을 수 있음: `NotRequired`
- Key는 있으나 값이 None일 수 있음: `T | None`
- 둘 다 가능: `NotRequired[T | None]`

세 의미를 구분한다.

## Strict Types

Pydantic Boundary Model은 canonical base의 Model-level `strict=True`를 기본으로 사용한다.

- `str`, `int`, `bool`, `float` 등 Python input의 암묵적 coercion을 허용하지 않는다.
- 제약 문자열은 `Annotated[str, StringConstraints(strict=True, ...)]`를 사용한다.
- Protocol Discriminator, Lifecycle Status, Scope, Event Kind처럼 문자열 Symbolic Set인 값은 Loose String이나 문자열 `Literal`이 아니라 `StrEnum`을 사용한다.
- JSON protocol 자체가 문자열 표현을 요구하는 UUID/datetime 등의 타입은 `model_validate_json()`의 strict JSON semantics를 사용하되, 내부 Python constructor에서 문자열 coercion에 의존하지 않는다.
- 불가피한 유연 Boundary만 명시적 예외로 둔다. 편의를 위한 `strict=False`는 금지한다.

## Union

넓은 Union을 피한다.

`str | int | dict | list | None` 같은 광범위 Union은 Raw Boundary에만 제한한다.

Canonical Contract에서는 Discriminated Union, Enum, 명명된 Model을 사용한다.

## Literal과 Enum

- 문자열 Symbolic Set: 공유 범위와 무관하게 `StrEnum`
- `class X(str, Enum)` 형태는 금지하고 Python 표준 `StrEnum`을 사용한다.
- `Literal[True]`, `Literal[1]`처럼 문자열이 아닌 고정 Protocol Constant는 `Literal`을 사용할 수 있다.

Loose String으로 Protocol 값을 전달하지 않는다.

## cast

Cast는 타입 검사기를 속이는 수단이 아니다.

허용할 때:

- Runtime 검사 직후 Narrowing
- Library Typing 결함
- 불변식이 코드로 증명되는 경우

Cast 옆에서 근거를 알 수 있어야 한다.

## type: ignore

Broad Ignore를 금지한다.

필요하면 정확한 Error Code와 이유를 적는다.

기존 Checker 정책을 약화해 오류를 없애지 않는다.

## Keyword Arguments

동일 타입 인자가 여러 개이거나 순서가 바뀌기 쉬운 함수는 Keyword Argument를 사용한다.

반대로 bare `*` separator를 모든 함수의 기본 스타일처럼 사용하지 않는다.

- 인자가 하나뿐이거나 타입/의미가 명확해 위치 혼동 위험이 낮으면 positional-or-keyword를 허용한다.
- 3개 이상의 독립 입력, 동일 타입 인자의 반복, 또는 기존 default positional 뒤의 required argument처럼 실제 안전/문법 이유가 있을 때 keyword-only를 사용한다.
- Framework callback signature나 외부 Protocol 때문에 예외가 필요하면 인접한 `type-contract: allow-keyword-only` 주석으로 이유를 남긴다.
- `*args`는 bare `*` separator와 다른 기능이므로 실제 variadic contract가 필요한 경우에만 별도로 사용한다.

## Collection

- 변경하지 않는 Sequence: `tuple[T, ...]`
- 실제 Mutation이 필요한 경우: `list[T]`
- 고유성 의미가 있는 경우: `set[T]` 또는 Root Contract
- 읽기 전용 Mapping: `Mapping[K, V]`
- 변경이 필요한 Mapping: `dict[K, V]`
