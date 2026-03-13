# Stage 1: Build (Rust 1.83 to match your Cargo.lock version 4)
FROM rust:1.83 AS builder
WORKDIR /usr/src/app

# Install compilation tools for SSL and Postgres
RUN apt-get update && apt-get install -y pkg-config libssl-dev libpq-dev && rm -rf /var/lib/apt/lists/*

COPY . .
# Build the production binary
RUN cargo build --release

# Stage 2: Runtime (The tiny "box" that actually runs on AWS)
FROM debian:bookworm-slim
# Install runtime libraries for SSL and Postgres
RUN apt-get update && apt-get install -y libssl-dev ca-certificates libpq5 && rm -rf /var/lib/apt/lists/*

# Copy ONLY the exact binary we built
COPY --from=builder /usr/src/app/target/release/britespeck_engine /usr/local/bin/rust-engine

# Start the engine
CMD ["rust-engine"]
