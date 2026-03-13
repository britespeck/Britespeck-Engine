# Stage 1: Build using the newer Rust 1.83 to match your Cargo.lock
FROM rust:1.83 AS builder
WORKDIR /usr/src/app

# Install compilation tools needed for SSL and Postgres
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

COPY . .
# Run the build
RUN cargo build --release

# Stage 2: Runtime (keeps the final image tiny)
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the binary from the builder stage
COPY --from=builder /usr/src/app/target/release/britespeck_engine /usr/local/bin/rust-engine

CMD ["rust-engine"]

