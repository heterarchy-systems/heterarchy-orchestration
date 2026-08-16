.PHONY: dev_env_check install-local rule_check format_check type_checking node_check test test_free_threaded ci db-config db-up db-down db-logs

dev_env_check:
	@if find .venv/lib -type l -path '*/site-packages/chattocodex' -print -quit 2>/dev/null | grep -q .; then \
		echo 'Refusing to run: site-packages/chattocodex is a symlink. Recreate .venv instead of linking it to the source tree.' >&2; \
		exit 1; \
	fi

install-local: dev_env_check
	# CPython 3.14 skips .pth files carrying macOS UF_HIDDEN. Install the
	# current source as a regular local wheel so console entry points also work
	# when a copied or corrupted .venv has stale Finder hidden flags. The normal
	# environment repair is: chflags -R nohidden .venv
	uv pip install --offline --python .venv/bin/python --force-reinstall --no-deps .

rule_check: dev_env_check
	uv run python chattocodex/.agents/verify_manifest.py

format_check: dev_env_check
	uv run ruff format --check .
	uv run ruff check .

type_checking: dev_env_check
	uv run pyrefly check

node_check:
	@node --version >/dev/null || (echo "Node.js is required for Chrome runtime safety tests." >&2; exit 1)
	@tsc --version >/dev/null || (echo "TypeScript compiler is required for Chrome runtime safety tests." >&2; exit 1)
	@tsc -p chattocodex/wake/chrome_extension_ts/tsconfig.json --noEmit
	@tsc -p frontend/control-panel/tsconfig.json --noEmit
	@node tests/persistent_worker_runtime_policy_harness.js

test: dev_env_check node_check
	uv run pytest -q

test_free_threaded: node_check
	uv run --isolated --locked --managed-python --python 3.14t --no-editable python -c 'import sys, sysconfig; import cryptography, httptools, pydantic_core, pytest, uvloop; assert sysconfig.get_config_var("Py_GIL_DISABLED") == 1; assert not sys._is_gil_enabled(); raise SystemExit(pytest.main(["-q"]))'

ci: rule_check format_check type_checking test

db-config:
	docker compose config >/dev/null

db-up:
	docker compose up -d postgres

db-down:
	docker compose down

db-logs:
	docker compose logs -f postgres
