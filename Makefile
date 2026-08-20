include make/frontend.mk
include make/backend.mk

.PHONY: ci lock_check dev_env_check install-local frontend_install frontend_check frontend_build package_check rule_check architecture_check backend_check format_check type_checking node_check test test_free_threaded broker_e2e postgres_mcp_e2e db-config db-up db-down db-logs

# Full-project orchestration. Backend and frontend remain independently runnable
# through `make backend-ci` and `make frontend-ci`.
ci: frontend-ci backend-ci

# Compatibility aliases retained for existing local/CI callers while the
# canonical targets are the explicit backend-* / frontend-* names above.
lock_check: backend-lock-check frontend-lock-check

dev_env_check: backend-dev-env-check

install-local: backend-install-local

frontend_install: frontend-install

frontend_check: frontend-check

frontend_build: frontend-build

package_check: frontend-check backend-package-check

rule_check: backend-rule-check

architecture_check: backend-architecture-check

backend_check: backend-contract-check

format_check: backend-format-check

type_checking: backend-type-check

node_check: backend-node-check

test: backend-test

test_free_threaded: backend-test-free-threaded

broker_e2e: backend-broker-e2e

postgres_mcp_e2e: backend-postgres-mcp-e2e

db-config: backend-db-config

db-up: backend-db-up

db-down: backend-db-down

db-logs: backend-db-logs
