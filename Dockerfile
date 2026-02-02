# ==========================================
# STAGE 1: Builder (Solid & Reliable)
# ==========================================
FROM rust:latest AS builder

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

# 2. Rust dependency caching
COPY Cargo.toml Cargo.lock ./
# RIMOSSO: COPY dashboard/Cargo.toml dashboard/Cargo.toml (NON SERVE)

# Create dummy source files to pre-compile dependencies.
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs && \
    cargo build --release && \
    rm -rf src

# 3. Copy ALL source code (Rust + Dashboard)
COPY . .

# 4. Final Build
# FIX 1: Definiamo le ENV necessarie a Vite per buildare
ENV SERVER_HOST="0.0.0.0"
ENV SERVER_DASHBOARD_PORT="8080"
# (Aggiungi qui altre ENV se vite.config.ts ne richiede altre, es. VITE_API_URL)

# FIX 2: Rimuoviamo RUSTFLAGS specifico per x86 se buildi da Mac (ARM)
# Lascia vuoto o rimuovi la riga. Il compilatore userà il default sicuro per l'architettura corrente.
# ENV RUSTFLAGS="-C target-cpu=x86-64-v2"  <-- RIMOSSO/COMMENTATO

RUN touch src/main.rs && cargo build --release

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
COPY --from=builder /app/target/release/nexo /usr/local/bin/nexo

# Create data directory with correct permissions for persistence
RUN mkdir -p /app/data && chown nexo:nexo /app/data

# Switch to the non-root user
USER nexo

# Default configuration via environment variables
ENV SERVER_HOST=0.0.0.0 \
    SERVER_PORT=7654 \
    DASHBOARD_PORT=8080 \
    DATA_PATH=/app/data

EXPOSE 7654 8080

CMD ["nexo"]