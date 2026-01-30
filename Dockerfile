# === Stage 1: Builder ===
FROM rust:latest AS builder

# Installiamo Node.js (necessario per build.rs che compila il frontend)
RUN apt-get update && apt-get install -y ca-certificates curl gnupg && \
    mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list && \
    apt-get update && apt-get install -y nodejs

WORKDIR /app

# 1. Copiamo SOLO i file di definizione delle dipendenze per cachare il layer di compilazione
COPY Cargo.toml Cargo.lock ./

# 2. Creiamo un main dummy per pre-compilare le dipendenze
RUN mkdir src && \
    echo "fn main() {println!(\"dummy\");}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# 3. Ora copiamo tutto il resto (incluso il frontend dashboard per build.rs)
COPY . .

# 4. Tocchiamo il main vero per forzare Cargo a ricompilare il codice sorgente (ma riutilizzando le deps)
RUN touch src/main.rs && cargo build --release

# === Stage 2: Runtime ===
FROM debian:bookworm-slim

WORKDIR /app

# Installiamo dipendenze runtime minime (SSL è critico)
RUN apt-get update && apt-get install -y ca-certificates openssl && rm -rf /var/lib/apt/lists/*

# Copiamo solo il binario finale (niente Node.js, niente sorgenti Rust)
COPY --from=builder /app/target/release/nexo /usr/local/bin/nexo

ENV SERVER_HOST=0.0.0.0
ENV SERVER_PORT=7654
ENV DASHBOARD_PORT=8080

EXPOSE 7654 8080

CMD ["nexo"]


