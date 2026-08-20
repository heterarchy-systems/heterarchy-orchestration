# Lint, Typecheck, and Test Rules

## 저장소 명령 발견

작업 전에 다음에서 공식 명령을 찾는다.

- `pyproject.toml`
- `Makefile`
- `justfile`
- CI Workflow
- README
- 기존 개발 문서

이 저장소의 canonical aggregate gate는 `make ci`다. 다른 repository의 명령을 자동 복사하지 않는다.

## 품질 게이트

의미 있는 Production 변경은 다음을 통과해야 한다.

1. `make rule_check` — rule manifest + mandatory skills + schema contract checker
2. `make format_check` — Ruff format/lint
3. `make type_checking` — Pyrefly
4. focused Unit / Integration Test
5. `make test`
6. `make ci`
7. real integration claim이 있으면 별도 `make broker_e2e` 또는 해당 E2E gate

## Type Checker

Production Python은 현재 repository Pyrefly 설정을 broad ignore 없이 통과해야 한다.

## Source와 Test

Production Code는 Test보다 엄격하게 본다.

Test Fixture의 편의를 위해 Production 타입을 약화하지 않는다.

## Baseline

변경 전 관련 Test를 실행한다.

실패를 다음으로 분류한다.

- pre-existing
- change-induced
- environment
- flaky

기존 baseline 자체가 실패 중이면 원인을 기록하고 현재 요청과 직접 관련된 rule/harness corruption은 새 개발 전에 복구한다. unrelated 실패를 새 변경에 섞어서 고치지 않는다.

## Schema Rule Harness

Pydantic/FastAPI/MCP production schema는 AST 기반 repository checker의 enforceable subset을 통과해야 한다.

- canonical schema base module 밖의 direct `BaseModel` / `RootModel` inheritance 금지
- canonical field-helper module 밖의 direct `Field()` call 금지
- canonical schema base의 model-level `strict=True` 강제
- `StringConstraints(...)`는 `strict=True`를 명시
- direct `Annotated[str, described_field(min_length/max_length=...)]` 금지; 문자열 제약은 `StringConstraints` 사용
- FastAPI/MCP boundary의 bare top-level collection return annotation 금지
- FastAPI / Dependency Injector mandatory Skill 존재 검증

AST checker를 통과했다는 사실이 description 품질, DI scope, transaction boundary review를 대체하지 않는다.

## Suppression

- Broad `# noqa` 금지
- Broad `type: ignore` 금지
- 설정 완화로 Local Error를 숨기지 않음
- 필요한 Suppression은 가장 좁은 범위와 이유를 사용

## 완료 보고

실행한 명령, Exit Code, 실패 개수, 통과 개수를 기록한다.

Test를 작성했지만 실행하지 않았으면 `IMPLEMENTED`다.
