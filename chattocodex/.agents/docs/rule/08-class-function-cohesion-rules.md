# Class, Function, and Cohesion Rules

## 클래스 책임

한 클래스는 하나의 응집된 책임을 가진다.

다음 책임을 한 클래스에 모두 넣지 않는다.

- Parsing
- Validation
- Storage
- Search
- Lifecycle
- Orchestration
- Report Rendering

## 메서드 수 제한

클래스는 원칙적으로 행동 메서드를 6개 이하로 유지한다.

계산에서 제외할 수 있는 항목:

- `__init__`
- 명확한 Dunder Method
- 단순한 Read-only Property
- Framework가 요구하는 최소 Hook

6개를 초과하면 반드시 다음을 검토한다.

- 책임이 둘 이상 섞였는가?
- Parser, Builder, Validator, Repository를 분리할 수 있는가?
- Private Helper가 독립 Concept인가?
- Class가 Service Locator 또는 Manager로 변했는가?

초과가 정당하면 Class Docstring에 이유를 짧게 기록한다.

## Public Method

Public Method는 클래스의 핵심 책임만 노출한다.

호출자가 내부 단계를 조합해야만 Use Case가 완성되는 API를 만들지 않는다.

## Function

함수는 한 동작을 수행한다.

다음 신호가 있으면 분리한다.

- 이름에 `and`가 필요함
- Validation과 Side Effect가 섞임
- 여러 Error Recovery 경로가 혼재
- Return Type이 지나치게 복잡
- 중첩 분기가 깊음
- Raw Parsing과 Canonical Construction이 함께 있음

## Module

한 Module은 하나의 Concept 안에서 관련된 역할을 담는다.

약 430줄을 넘으면 `REVIEW TRIGGER`로 본다.

430줄은 자동 분할 기준이 아니다. Generated Code, 선언형 Mapping, 명확한 단일 책임은 예외가 될 수 있다.

## Manager Class 금지

`Manager`가 다음을 모두 소유하면 분리한다.

- File I/O
- Database
- Search
- Validation
- Lifecycle
- Logging
- Report

역할 이름을 명확히 나눈다.

예:

- `ContextRepository`
- `ContextRecallService`
- `FrontmatterParser`
- `CompactPromotionService`

## Property

Property는 안정적인 Derived Attribute에만 사용한다.

일반 Helper 호출이나 비용 있는 I/O를 Property로 숨기지 않는다.

## Return Style

Validation, Transformation, Aggregation Code는 의미 있는 Local Variable에 결과를 담은 뒤 반환하는 것을 선호한다.

직접 Expression Return이 더 명확한 단순 Passthrough는 예외다.
