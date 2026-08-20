# Schema and Normalization Rules

## 경계 단계

```text
1. Raw Transport 수신
2. 최소 Parsing
3. Shape Routing
4. Normalization
5. Pydantic Validation
6. Internal Dataclass 생성
7. Service 전달
```

Parsing, Routing, Validation을 하나의 암묵적 단계로 합치지 않는다.

## Raw Frontmatter

Raw Frontmatter는 Legacy Field와 사용자 Metadata를 포함할 수 있다.

안정된 API Contract와 같은 `extra="forbid"`를 자동 적용하지 않는다.

권장 방식:

- Raw TypedDict 또는 제한된 Mapping
- Known Field 추출
- Null/Empty Normalization
- Pydantic Canonical Validation
- Unknown Field 보존 또는 구조화된 Warning

정책 없이 Unknown Field를 삭제하지 않는다.

## Fallback

다음 표현으로 누락을 숨기지 않는다.

- `value or {}`
- `value or []`
- `value or ""`
- 광범위한 `get(..., default)` 연쇄

Fallback이 실제 Contract라면 이름 있는 Normalizer에서 명시한다.

## Enum Normalization

String 입력은 Boundary에서 Enum으로 검증한다.

내부 로직은 Enum 또는 좁은 Literal을 사용한다.

직렬화 경계에서 String Value로 출력한다.

## Date and Time

외부 입력의 DateTime은 Pydantic 또는 명시적 Parser로 검증한다.

내부에서는 Timezone-aware DateTime을 사용한다.

Naive DateTime을 조용히 UTC로 간주하지 않는다.

## Hash와 Version

Content Hash와 Version은 한 개의 Concept-owned Normalizer가 생성한다.

여러 호출 지점에서 각자 공백 제거, 줄바꿈 변환, Encoding을 결정하지 않는다.
