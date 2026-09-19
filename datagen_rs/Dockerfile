# Multi-stage build for the Rust financial datagen.
# Stage 1 compiles the standalone generator; stage 2 is a slim Python image
# that carries the binary beside a thin entrypoint which runs it and uploads
# the output to S3, matching the datagen K8s Job contract.

FROM rust:1-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin generate

FROM python:3.13-slim
WORKDIR /app
RUN pip install --no-cache-dir boto3
COPY --from=build /build/target/release/generate /app/datagen_rs
COPY entrypoint.py /app/entrypoint.py
ENV RUST_BACKTRACE=1
# S3 creds come from env: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_ENDPOINT.
# Node id comes from JOB_COMPLETION_INDEX (K8s Indexed Job) or --node-id.
ENTRYPOINT ["python", "entrypoint.py"]
