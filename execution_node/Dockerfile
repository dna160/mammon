# Stage 1: Build — identical to mammond/project_sniper/agent_0_ingestion/Dockerfile
FROM rust:latest AS builder
WORKDIR /app

# rustls-tls-webpki-roots is pure Rust — no libssl-dev needed.
# Only pkg-config is required for any C-based transitive deps.
RUN apt-get update && apt-get install -y pkg-config && rm -rf /var/lib/apt/lists/*

# Pre-build dependencies (layer cache)
COPY Cargo.toml .
RUN mkdir src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release 2>/dev/null || true
RUN rm -rf src

# Build application
COPY src ./src
RUN touch src/main.rs && cargo build --release

# Stage 2: Runtime — no libssl3 needed, rustls bundles its own TLS and CA roots.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/execution_node .

ENV RUST_LOG=info
CMD ["./execution_node"]
