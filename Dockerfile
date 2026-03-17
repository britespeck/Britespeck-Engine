# --- Stage 1: Build ---
    FROM rust:1.88 AS builder
    WORKDIR /usr/src/app
    RUN apt-get update && apt-get install -y pkg-config libssl-dev libpq-dev && rm -rf /var/lib/apt/lists/*
    COPY . .
    RUN cargo build --release --locked
    
    # --- Stage 2: Runtime (The To-Go Box) ---
    FROM debian:bookworm-slim
    # This line adds the "napkins" (certificates) so it can talk to HTTPS websites
    RUN apt-get update && apt-get install -y \
        ca-certificates \
        libssl-dev \
        libpq5 \
        && rm -rf /var/lib/apt/lists/*
    
    # Copy the finished engine
    COPY --from=builder /usr/src/app/target/release/britespeck_engine /usr/local/bin/rust-engine
    
    CMD ["rust-engine"]