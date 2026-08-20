# Folder and Module Rules

## 기본 원칙

기존 heterarchy-alexandria 디렉터리 구조를 우선한다.

새 폴더는 실제 Concept 또는 Responsibility Boundary가 있을 때만 만든다.

Production package 한 단계에 Python module을 계속 평면으로 쌓지 않는다.

- 한 package의 direct `.py` module은 10개를 architecture review trigger로 본다.
- 10개를 넘기기 전에 Alexandria처럼 실제 Concept/Responsibility Boundary 기준의 하위 package로 분리한다.
- 기존 초과 package는 shrink-only debt로 관리하며 새 module을 더 쌓지 않는다.
- 파일 수를 줄이기 위한 가짜 bucket이나 marker-only `__init__.py`는 만들지 않는다.

좋은 방향:

- `routers/`
- `schemas/`
- `services/`
- `repositories/`
- `obsidian/`
- `search/`
- `memory/`
- `librarian/`
- `graph/`
- `shared/`

기존 저장소가 다른 명칭을 사용한다면 기존 명칭을 우선한다.

## 금지되는 Bucket

다음 이름은 새 파일의 기본 선택이 아니다.

- `utils.py`
- `helpers.py`
- `common.py`
- `misc.py`
- `models.py`
- `types.py`
- `schema.py`
- `manager.py`

필요한 경우 역할을 포함한 이름을 사용한다.

예:

- `frontmatter_parser.py`
- `scope_identity_validator.py`
- `context_recall_filter.py`
- `compact_promotion.py`
- `graph_edge_indexer.py`
- `reindex_report.py`

## Schema 배치

Pydantic Schema는 `schemas/` 아래에 Concept별로 둔다.

예:

- `context_schemas.py`
- `memory_compact_schemas.py`
- `librarian_schemas.py`
- `obsidian_schemas.py`
- `search_schemas.py`

하나의 거대한 `schemas.py`를 만들지 않는다.

## 내부 DTO 배치

Internal DTO는 소유 Concept 옆에 둔다.

예:

```text
memory/
├── context_dto.py
├── context_service.py
└── context_mapper.py
```

저장소 전체 DTO를 하나의 `dto.py`에 넣지 않는다.

## Shared

`shared/`는 실제로 여러 Concept에서 사용하는 정의만 둔다.

`shared/`를 잡동사니 폴더로 만들지 않는다.

## __init__.py

기존 Package Convention을 따른다.

필요하지 않은 Export Bucket 또는 Marker 용도로 `__init__.py`를 추가하지 않는다. Packaging, Import Surface, Tooling에 실제 이유가 있을 때만 추가한다.
