# Error and Exception Rules

## 구조화된 오류

외부 Boundary는 구조화된 Error Contract를 반환한다.

오류에는 가능한 범위에서 다음을 포함한다.

- error_code
- message
- operation
- resource_id
- note_path
- recoverable
- run_id
- details

## Exception 배치

Exception은 Concept별로 배치한다.

좋은 예:

- `context_exceptions.py`
- `obsidian_exceptions.py`
- `search_exceptions.py`
- `librarian_exceptions.py`

거대한 `exceptions.py` 하나에 모든 오류를 넣지 않는다.

## Catch 범위

`except Exception`은 Boundary 또는 Recovery Loop에서만 제한적으로 사용한다.

잡은 뒤 다음 중 하나를 수행한다.

- 구조화된 Error로 변환
- Context를 포함해 다시 Raise
- 실패 상태와 Audit를 기록
- 명시적 Recovery

조용히 무시하지 않는다.

## Validation Error

Pydantic Validation Error, Domain Invariant Error, Persistence Error, Index Error를 구분한다.

모든 오류를 HTTP 500으로 변환하지 않는다.

## Message

Exception Message는 다음을 포함한다.

- 무엇이 실패했는가
- 어떤 값 또는 Resource인가
- 복구 가능한가
- 다음 조치가 무엇인가

Secret, Token, Password, Raw Credential을 포함하지 않는다.
