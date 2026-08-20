.PHONY: frontend-install frontend-lock-check frontend-check frontend-build frontend-ci

frontend-install:
	@if test -f frontend/control-panel/package-lock.json; then \
		npm --prefix frontend/control-panel ci; \
	else \
		npm --prefix frontend/control-panel install; \
	fi

frontend-lock-check:
	npm --prefix frontend/control-panel run verify:lock

frontend-check:
	@test -x frontend/control-panel/node_modules/.bin/next || (echo "Run 'make frontend-install' before frontend checks." >&2; exit 1)
	npm --prefix frontend/control-panel run check

frontend-build:
	@test -x frontend/control-panel/node_modules/.bin/next || (echo "Run 'make frontend-install' before building the Control Panel." >&2; exit 1)
	npm --prefix frontend/control-panel run build

frontend-ci: frontend-lock-check frontend-check frontend-build
