# syntax=docker/dockerfile:1
FROM rust:1.98.0-slim AS build
WORKDIR /workspace/heterarchy-orchestration
ENV CARGO_TARGET_DIR=/workspace/target \
    TMPDIR=/workspace/tmp \
    CARGO_INCREMENTAL=0 \
    CARGO_BUILD_JOBS=2 \
    CARGO_PROFILE_DEV_DEBUG=0 \
    CARGO_PROFILE_TEST_DEBUG=0
COPY heterarchy-orchestration/Cargo.toml heterarchy-orchestration/Cargo.lock heterarchy-orchestration/rust-toolchain.toml ./
COPY heterarchy-orchestration/crates ./crates
COPY heterarchy-orchestration/xtask ./xtask
COPY heterarchy-forge/Cargo.toml heterarchy-forge/Cargo.lock heterarchy-forge/rust-toolchain.toml ../heterarchy-forge/
COPY heterarchy-forge/crates ../heterarchy-forge/crates
COPY heterarchy-forge/xtask ../heterarchy-forge/xtask
COPY heterarchy-forge/vendor ../heterarchy-forge/vendor
COPY heterarchy-broker/Cargo.toml heterarchy-broker/Cargo.lock heterarchy-broker/rust-toolchain.toml ../heterarchy-broker/
COPY heterarchy-broker/crates ../heterarchy-broker/crates
COPY heterarchy-broker/xtask ../heterarchy-broker/xtask
RUN mkdir -p /workspace/tmp && chmod 0700 /workspace/tmp
RUN cargo test --locked -p heterarchy-orchestration-auth --lib
RUN cargo build --locked --release -p heterarchy-orchestration-backend

FROM debian:trixie-slim AS runtime
ARG VERSION=dev
ARG VCS_REF=unknown
LABEL org.opencontainers.image.title="Heterarchy Orchestration" \
      org.opencontainers.image.source="https://github.com/heterarchy-systems/heterarchy-orchestration" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}"
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl git \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 orchestration \
    && useradd --uid 10001 --gid orchestration --home-dir /state --shell /usr/sbin/nologin orchestration \
    && mkdir -p /state/auth /state/runtime \
    && chmod 0700 /state /state/auth /state/runtime \
    && chown -R orchestration:orchestration /state
COPY --from=build /workspace/target/release/heterarchy-orchestration-backend /usr/local/bin/heterarchy-orchestration-backend
ENV XDG_DATA_HOME=/state/auth \
    TMPDIR=/tmp \
    HETERARCHY_ORCHESTRATION_DATA_DIR=/state/runtime \
    HETERARCHY_ORCHESTRATION_BACKEND_ADDR=127.0.0.1:8798
USER 10001:10001
WORKDIR /state
VOLUME ["/state"]
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD curl --fail --silent --show-error --max-time 2 http://127.0.0.1:8798/health || exit 1
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/heterarchy-orchestration-backend"]
CMD []
