HETERARCHY_BROKER_DIR ?= ../heterarchy-broker
HETERARCHY_BROKER_BINARY ?= $(abspath $(HETERARCHY_BROKER_DIR)/target/debug/agentbrokerd)

.PHONY: backend-dev-env-check backend-install-local backend-lock-check backend-rule-check backend-architecture-check backend-contract-check backend-docstring-check backend-database-check backend-async-check backend-format-check backend-type-check backend-node-check backend-test backend-test-free-threaded backend-package-check backend-broker-e2e backend-postgres-mcp-e2e backend-ci backend-db-config backend-db-up backend-db-down backend-db-logs

backend-dev-env-check:
	@if find .venv/lib -type l -path '*/site-packages/chattocodex' -print -quit 2>/dev/null | grep -q .; then \
		echo 'Refusing to run: site-packages/chattocodex is a symlink. Recreate .venv instead of linking it to the source tree.' >&2; \
		exit 1; \
	fi

backend-install-local: backend-dev-env-check
	# CPython 3.14 skips .pth files carrying macOS UF_HIDDEN. Install the
	# current source as a regular local wheel so console entry points also work
	# when a copied or corrupted .venv has stale Finder hidden flags. The normal
	# environment repair is: chflags -R nohidden .venv
	uv pip install --offline --python .venv/bin/python --force-reinstall --no-deps .

backend-lock-check:
	uv lock --check

backend-rule-check: backend-dev-env-check
	uv run python chattocodex/.agents/verify_manifest.py
	uv run python chattocodex/.agents/verify_schema_contracts.py
	uv run python chattocodex/.agents/verify_enum_contracts.py
	uv run python chattocodex/.agents/verify_boundary_contracts.py

backend-architecture-check: backend-dev-env-check
	uv run python chattocodex/.agents/verify_architecture_contracts.py

backend-contract-check: backend-dev-env-check
	uv run python chattocodex/.agents/verify_backend_contracts.py

backend-docstring-check: backend-dev-env-check
	uv run python chattocodex/.agents/verify_docstring_contracts.py

backend-database-check: backend-dev-env-check
	uv run python chattocodex/.agents/verify_database_contracts.py

backend-async-check: backend-dev-env-check
	uv run python chattocodex/.agents/verify_async_contracts.py

backend-format-check: backend-dev-env-check
	uv run ruff format --check chattocodex tests
	uv run ruff check chattocodex tests

backend-type-check: backend-dev-env-check
	uv run python chattocodex/.agents/verify_type_contracts.py
	uv run pyrefly check

backend-node-check:
	@node --version >/dev/null || (echo "Node.js is required for Chrome runtime safety tests." >&2; exit 1)
	@test -x frontend/control-panel/node_modules/.bin/tsc || (echo "Run 'make frontend-install' before backend node checks." >&2; exit 1)
	@frontend/control-panel/node_modules/.bin/tsc -p chattocodex/orchestrator_backend/wake/chrome_extension_ts/tsconfig.json --noEmit
	@node tests/persistent_worker_runtime_policy_harness.js

backend-test: backend-dev-env-check backend-node-check
	uv run python -m pytest -q

backend-test-free-threaded: backend-node-check
	DISABLE_SQLALCHEMY_CEXT=1 uv run --isolated --locked --managed-python --python 3.14t --no-editable --no-binary-package sqlalchemy python -c 'import sys, sysconfig; import cryptography, httptools, pydantic_core, pytest, sqlalchemy, uvloop; assert sysconfig.get_config_var("Py_GIL_DISABLED") == 1; assert not sys._is_gil_enabled(), "GIL enabled after dependency imports"; code = pytest.main(["-q"]); assert not sys._is_gil_enabled(), "GIL enabled during test suite"; raise SystemExit(code)'

backend-package-check: backend-dev-env-check
	uv run python chattocodex/.agents/verify_package_artifacts.py

backend-broker-e2e: backend-dev-env-check
	cargo build -q --manifest-path $(HETERARCHY_BROKER_DIR)/Cargo.toml -p agent-broker-runtime --bin agentbrokerd
	HETERARCHY_BROKER_BINARY=$(HETERARCHY_BROKER_BINARY) uv run python -m pytest -q \
		tests/test_real_broker_e2e.py \
		tests/test_real_broker_mcp_e2e.py

backend-postgres-mcp-e2e: backend-dev-env-check backend-db-up
	CHATTOCODEX_RUN_POSTGRES_E2E=1 uv run python -m pytest -q \
		[REDACTED].py

backend-ci: backend-lock-check backend-rule-check backend-architecture-check backend-contract-check backend-docstring-check backend-database-check backend-async-check backend-format-check backend-type-check backend-test backend-package-check

backend-db-config:
	docker compose config >/dev/null

backend-db-up:
	docker compose up -d postgres

backend-db-down:
	docker compose down

backend-db-logs:
	docker compose logs -f postgres
