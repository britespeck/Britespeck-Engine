# Stage 1: Build (with Verbose logging)
FROM rust:1.83 AS builder
WORKDIR /usr/src/app

# Standard dev libraries for SSL and Postgres
RUN apt-get update && apt-get install -y pkg-config libssl-dev libpq-dev && rm -rf /var/lib/apt/lists/*

COPY . .
# We add -vv here to see the EXACT error 20-40 lines up
RUN cargo build --release -vv

# Stage 2: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl-dev ca-certificates libpq5 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/app/target/release/britespeck_engine /usr/local/bin/rust-engine

CMD ["rust-engine"]
