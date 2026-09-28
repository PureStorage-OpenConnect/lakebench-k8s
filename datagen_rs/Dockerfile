# Multi-stage build for the Rust financial datagen.
# Stage 1 compiles the standalone generator; stage 2 is a slim Python image
# that carries the binary beside a thin entrypoint which runs it and uploads
# the output to S3, matching the datagen K8s Job contract.

FROM rust:1.98.1-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked --bin generate

# Python 3.14-slim, digest-pinned for freeze reproducibility. Bumped from
# 3.13-slim on 2026-09-28: 3.13's floating minor tag could drift under a
# rebuild, and 3.14 (released Oct 2025) has support until 2030-10-31.
# Corpus bytes are Python-independent (the Rust binary owns generation and
# S3 multipart upload); Python only runs the argparse entrypoint and boto3.
FROM python:3.14-slim@sha256:7bf6c3111fe094f8ee1a1cbcdc63c4cfb345b0e3df42d5aa9a90b3b4b022ab6d
WORKDIR /app
RUN pip install --no-cache-dir boto3
COPY --from=build /build/target/release/generate /app/datagen_rs
COPY entrypoint.py /app/entrypoint.py
ENV RUST_BACKTRACE=1
# S3 creds come from env: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_ENDPOINT.
# Node id comes from JOB_COMPLETION_INDEX (K8s Indexed Job) or --node-id.
ENTRYPOINT ["python", "entrypoint.py"]
