# Stage 1: Build (Upgraded to 1.85 for Edition 2024 support)
FROM rust:1.85 AS builder
WORKDIR /usr/src/app

# Install system dependencies for SSL and Postgres
RUN apt-get update && apt-get install -y pkg-config libssl-dev libpq-dev && rm -rf /var/lib/apt/lists/*

COPY . .
# Using --locked ensures we use the exact versions in your Cargo.lock
RUN cargo build --release --locked

# Stage 2: Runtime (Slim image)
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl-dev ca-certificates libpq5 && rm -rf /var/lib/apt/lists/*

# Copy the exact binary
COPY --from=builder /usr/src/app/target/release/britespeck_engine /usr/local/bin/rust-engine

CMD ["rust-engine"]
