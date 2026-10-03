# Multi-stage build for the Rust financial datagen.
# Stage 1 compiles the standalone generator; stage 2 is a slim Python image
# that carries the binary beside a thin entrypoint which runs it and uploads
# the output to S3, matching the datagen K8s Job contract.

# Digest-pinned for freeze reproducibility: a version-only tag could pull a
# patched toolchain on rebuild and emit a different binary.
FROM rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS build
# The commit the image is built from; `generate --version` and every per-node
# marker report it (build with --build-arg LB_BUILD_COMMIT=$(git rev-parse HEAD)).
ARG LB_BUILD_COMMIT=unknown
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Evidence names what produced it: an image without its commit is refused.
RUN test "$LB_BUILD_COMMIT" != "unknown" || { echo "build with --build-arg LB_BUILD_COMMIT=\$(git rev-parse HEAD)" >&2; exit 1; }
RUN LB_BUILD_COMMIT="$LB_BUILD_COMMIT" cargo build --release --locked --bin generate

# Python 3.14-slim, digest-pinned for freeze reproducibility. Bumped from
# 3.13-slim on 2026-09-28: 3.13's floating minor tag could drift under a
# rebuild, and 3.14 (released Oct 2025) has support until 2030-10-31.
# This runtime base is part of generator identity, not just a shell for the
# entrypoint: the binary links this image's glibc, and its libm computes the
# floating-point functions the generator calls, so a different base can change
# corpus bytes. Python itself only runs the argparse entrypoint.
FROM python:3.14-slim@sha256:7bf6c3111fe094f8ee1a1cbcdc63c4cfb345b0e3df42d5aa9a90b3b4b022ab6d
ARG LB_BUILD_COMMIT=unknown
LABEL org.opencontainers.image.revision="${LB_BUILD_COMMIT}" \
      org.opencontainers.image.source="https://github.com/PureStorage-OpenConnect/lakebench-k8s" \
      org.opencontainers.image.version="datagen-v2-rs-0.3" \
      io.lakebench.model-version="datagen-v2-rs-0.3"
WORKDIR /app
COPY --from=build /build/target/release/generate /app/datagen_rs
COPY entrypoint.py /app/entrypoint.py
ENV RUST_BACKTRACE=1
# S3 creds come from env: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_ENDPOINT.
# Node id comes from JOB_COMPLETION_INDEX (K8s Indexed Job) or --node-id.
ENTRYPOINT ["python", "entrypoint.py"]
