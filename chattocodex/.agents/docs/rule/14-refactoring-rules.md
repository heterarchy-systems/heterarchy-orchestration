# Refactoring Rules

## Touch-path Refactoring

현재 기능과 직접 관련된 경로만 리팩토링한다.

허용되는 예:

- Scope Validation 중복 제거
- Frontmatter Mapping 통합
- Raw Dict를 TypedDict 또는 DTO로 교체
- 너무 큰 Service에서 Parser 또는 Mapper 분리
- Broad Any를 Boundary로 이동
- 관련 파일 Naming 개선

## 별도 작업으로 분리할 것

- 저장소 전체 Folder 변경
- 전체 Schema 재작성
- 모든 Dataclass/Pydantic 변환
- 모든 Enum 이동
- 모든 파일 Rename
- Type Checker 교체
- JSON Library 교체
- ORM Layer 재설계
- Dependency 대량 Upgrade

## 기능과 리팩토링

기능 구현과 리팩토링을 함께 할 수 있지만 다음을 지킨다.

- 리팩토링은 기능을 가능하게 하거나 검증을 쉽게 하는 범위
- Behavior 변경과 구조 변경을 Test로 구분
- 무관한 Cleanup을 같은 Commit에 넣지 않음
- Diff가 예상보다 커지면 자동 확대하지 않음

## 삭제

사용되지 않는 Wrapper나 Dead Code를 삭제할 수 있다.

다음이 확인돼야 한다.

- 호출자가 없음
- Public Contract가 아님
- Test와 Script가 의존하지 않음
- Migration 또는 Compatibility 필요 없음

## 추상화

미래에 필요할 수 있다는 이유만으로 Interface, Factory, Plugin Layer를 추가하지 않는다.

두 개 이상의 실제 구현 또는 명확한 테스트 경계가 있을 때 추상화를 검토한다.
