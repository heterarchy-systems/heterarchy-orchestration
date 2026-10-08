.PHONY: rules fmt check test clippy msrv ci deps doctor extended perf package

RUST_TOOLCHAIN := 1.98.0
CARGO := $(shell rustup which --toolchain $(RUST_TOOLCHAIN) cargo)
RUSTC := $(shell rustup which --toolchain $(RUST_TOOLCHAIN) rustc)
RUSTDOC := $(shell rustup which --toolchain $(RUST_TOOLCHAIN) rustdoc)
RUST_ENV := env RUSTC="$(RUSTC)" RUSTDOC="$(RUSTDOC)"
XTASK := $(RUST_ENV) "$(CARGO)" run --quiet --locked -p xtask --
PACKAGER := CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=$${CARGO_BUILD_JOBS:-4} $(RUST_ENV) "$(CARGO)" run --quiet --locked -p heterarchy-orchestration-packager --

rules fmt check clippy test msrv ci deps doctor extended perf:
	$(XTASK) $@

package:
	$(PACKAGER) package
