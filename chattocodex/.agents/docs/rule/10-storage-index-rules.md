# Storage and Index Rules

## Canonical Storage

Obsidian Markdown이 Canonical Storage다.

PostgreSQL의 검색·벡터·임베딩 파생 상태와 Neo4j Graph Projection은 Canonical Markdown에서 재구축 가능해야 한다.

Runtime persistence는 PostgreSQL만 허용한다. SQLite runtime, fallback, test compatibility path를 추가하지 않는다. Redis는 queue/cache/coordination을 위한 비영속 파생 상태로만 사용한다.

## Write Order

권장 흐름:

```text
Validate
→ Internal DTO
→ Canonical Markdown 임시 쓰기
→ Atomic Replace
→ Read-back
→ Index Update
→ Index Verification
→ Report
```

## Index 실패

Markdown 쓰기 성공 후 Index 쓰기가 실패한 경우:

- Canonical Markdown을 자동 삭제하지 않는다.
- 구조화된 Error를 남긴다.
- Reindex로 복구할 수 있어야 한다.
- 정상 Recall에 잘못된 Index 상태가 섞이지 않게 한다.

## Reindex

Reindex는 Best-effort와 Error Report를 지원한다.

잘못된 Note 하나 때문에 모든 정상 Note의 Index를 중단하지 않는다.

Report에는 가능한 범위에서 다음을 포함한다.

- scanned
- indexed
- updated
- skipped
- stale
- errors
- duration
- run_id

## Actual Vault Safety

Test는 실제 사용자 Vault를 수정하거나 삭제하지 않는다.

Temp Vault, Fixture Vault, Test Database를 사용한다.

## SQLAlchemy

ORM Entity는 Persistence Model이다.

Pydantic의 `from_attributes=True`를 전역 기본으로 사용해 ORM과 API를 강하게 결합하지 않는다.

Mapper를 통해 ORM Entity와 Internal DTO를 변환한다.

## Transaction

PostgreSQL Transaction과 File System Atomicity를 동일한 Transaction으로 간주하지 않는다.

여러 저장소를 넘는 상태 전환은 Commit Point, Journal, Reconciliation 또는 Idempotent Recovery 중 적절한 방식을 검토한다.
