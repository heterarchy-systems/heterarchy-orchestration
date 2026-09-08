.PHONY: rules fmt check test clippy msrv ci deps doctor extended perf

XTASK := cargo run --quiet --locked -p xtask --

rules fmt check clippy test msrv ci deps doctor extended perf:
	$(XTASK) $@
