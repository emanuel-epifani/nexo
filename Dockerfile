# syntax=docker/dockerfile:1.7
# ==========================================
# STAGE 1: Builder
# ==========================================
FROM rust:1.90-bookworm AS builder

WORKDIR /app

COPY . .

# Build with BuildKit cache mounts: persists cargo registry and target/ across builds.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release && \
    cp target/release/nexo /tmp/nexo

# ==========================================
# STAGE 2: Runtime (Lightweight & Secure)
# ==========================================
FROM debian:bookworm-slim

RUN useradd -ms /bin/bash nexo

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ca-certificates \
    openssl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /tmp/nexo /usr/local/bin/nexo

RUN mkdir -p /app/data && chown nexo:nexo /app/data

USER nexo

ENV SERVER_HOST=0.0.0.0

EXPOSE 7654

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD bash -c '</dev/tcp/127.0.0.1/7654' || exit 1

ENTRYPOINT ["nexo"]