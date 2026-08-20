# Naming Rules

## 목적 중심 Naming

이름은 구현 기술보다 역할과 목적을 설명해야 한다.

좋은 예:

- `ScopeIdentityValidator`
- `FrontmatterContextMapper`
- `ContextRecallFilter`
- `MemoryCompactPromoter`
- `ObsidianReindexService`
- `LibrarianSkillDraft`
- `GraphCurationProposal`

피해야 할 예:

- `DataManager`
- `CommonService`
- `Helper`
- `Util`
- `Processor`
- `Handler`만으로 끝나는 모호한 이름

## 파일명

파일명은 `무엇을 다루는가 + 어떤 역할인가`를 표현한다.

좋은 예:

- `scope_identity_validator.py`
- `frontmatter_reader.py`
- `frontmatter_writer.py`
- `context_repository.py`
- `recall_result_mapper.py`
- `compact_review_service.py`

## 클래스명

- 명사 또는 명사구를 사용한다.
- 책임을 구체적으로 표현한다.
- `Manager`, `Engine`, `Processor`는 실제 역할이 명확할 때만 사용한다.
- 하나의 클래스가 여러 Concept를 포괄하는 이름을 갖지 않게 한다.

## 함수명

함수명은 동작과 결과를 표현한다.

좋은 예:

- `validate_scope_identity`
- `parse_frontmatter`
- `build_context_dto`
- `promote_compact`
- `reindex_note`
- `filter_recall_candidates`

피해야 할 예:

- `handle`
- `process`
- `do_work`
- `run_logic`
- `convert_data`

## Boolean

Boolean은 질문처럼 읽히게 한다.

- `is_current`
- `has_errors`
- `can_promote`
- `should_reindex`

## Collection

복수형 또는 의미 있는 Collection 이름을 사용한다.

- `context_ids`
- `source_refs`
- `recall_candidates`
- `index_errors`

## 약어

도메인에서 확립된 약어만 사용한다.

`ctx`, `obj`, `tmp`, `mgr` 같은 축약을 Public Surface에 사용하지 않는다.
