# syntax=docker/dockerfile:1.7
# ==========================================
# STAGE 1: Builder (Solid & Reliable)
# ==========================================
FROM rust:1.90-bookworm AS builder

# 1. Install system dependencies AND Node.js
# We use the official NodeSource method to get a recent and stable Node version (v20).
# Node.js is required by build.rs to compile the frontend assets.
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    pkg-config \
    libssl-dev \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list \
    && apt-get update && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy all source code (Rust + Dashboard).
COPY . .

# Build with BuildKit cache mounts: persists cargo registry and target/ across builds.
# Massively speeds up rebuilds. The final binary is copied out of the cache mount.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release && \
    cp target/release/nexo /tmp/nexo

# ==========================================
# STAGE 2: Runtime (Lightweight & Secure)
# ==========================================
FROM debian:bookworm-slim

# Security: Run as a non-root user
RUN useradd -ms /bin/bash nexo

WORKDIR /app

# Install essential runtime dependencies (SSL certificates, OpenSSL)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    openssl \
    && rm -rf /var/lib/apt/lists/*

# Copy only the compiled binary from the builder stage
# The binary is self-contained and already includes the embedded frontend assets.
COPY --from=builder /tmp/nexo /usr/local/bin/nexo

# Create data directory with correct permissions for persistence
RUN mkdir -p /app/data && chown nexo:nexo /app/data

# Switch to the non-root user
USER nexo

# Only override the default that differs from the binary's built-in default
ENV SERVER_HOST=0.0.0.0

# Port 7654: TCP socket (always exposed).
# Port 8080: dashboard, only started by `nexo dev` (development).
EXPOSE 7654 8080

# Healthcheck: TCP port reachability is enough to confirm the server is up.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD bash -c '</dev/tcp/127.0.0.1/7654' || exit 1

# Default to `serve` (production-safe: TCP only, dashboard OFF).
# For local development run: `docker run nexo dev`
ENTRYPOINT ["nexo"]
CMD ["serve"]