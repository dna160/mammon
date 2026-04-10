# Stage 1: Build
FROM rust:latest AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# Pre-build dependencies (layer cache)
COPY Cargo.toml .
RUN mkdir src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release 2>/dev/null || true
RUN rm -rf src

# Build application
COPY src ./src
RUN touch src/main.rs && cargo build --release
RUN strip target/release/agent_0_ingestion

# Stage 2: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/agent_0_ingestion .
ENV RUST_LOG=info
CMD ["./agent_0_ingestion"]
