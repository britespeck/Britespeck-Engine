# Stage 1: Build
FROM rust:1.75 as builder
WORKDIR /usr/src/app
# Install build-essential and ssl headers for compilation
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release

# Stage 2: Run
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
# Ensure 'britespeck_engine' matches your Cargo.toml name
COPY --from=builder /usr/src/app/target/release/britespeck_engine /usr/local/bin/rust-engine
CMD ["rust-engine"]

