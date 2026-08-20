# Agent Execution Rules

## 모델

GPT-5.5 Reasoning High를 기본 개발 모델로 사용할 수 있다.

모델 성능이 높아도 규칙과 품질 게이트를 생략하지 않는다.

## 시작 순서

1. `AGENTS.md`
2. `backend/.agents/docs/rule/규칙.md`
3. `backend/.agents/docs/rule/README.md`
4. 관련 세부 규칙
5. 사용자가 명시적으로 지정한 PRD 또는 작업 문서
6. 관련 코드와 Test

PRD를 자동으로 개발 규칙에 포함하거나 작업 목표로 간주하지 않는다.

## One Run, One Goal

한 Run은 하나의 명확한 목표를 가진다.

좋은 예:

- 내부 DTO 타입 정리
- Raw Dictionary 제거
- Scope Validator 구현
- Frontmatter Mapper 분리
- 특정 Service Class 분리
- 특정 Recall Filter 타입 강화

나쁜 예:

- Alexandria 전체 리팩토링
- 모든 규칙을 한 번에 적용
- 전체 Backend 재설계
- 기능, Migration, UI를 동시에 변경

## 작업 전 보고

코드를 수정하기 전에 다음을 확인한다.

- 현재 구현
- 실제 테스트
- 관련 타입
- 변경 파일 예상
- Public Contract 변경
- Migration 필요
- 기존 Note 영향
- 검증 명령

## 변경 규칙

- 기존 구현을 보지 않고 새 추상화를 만들지 않는다.
- PRD를 읽었다는 이유만으로 모두 구현하지 않는다.
- 사용자가 요청한 범위만 구현한다.
- 규칙 적용을 위한 무관한 대규모 변경을 하지 않는다.
- 실패한 Test를 숨기지 않는다.

## 완료 보고

다음 순서를 사용한다.

1. 핵심
2. 확인된 사실과 가정
3. 결정
4. 구현
5. 실제 검증
6. 남은 문제
7. 다음 가장 작은 작업

## 상태

- DISCOVERED
- PLANNED
- IMPLEMENTED
- VERIFIED
- BLOCKED
