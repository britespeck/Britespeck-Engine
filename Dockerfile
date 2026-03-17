# --- Stage 1: Build (The Kitchen) ---
    FROM rust:1.88 AS builder
    WORKDIR /usr/src/app
    
    # Install build tools for SSL and Postgres
    RUN apt-get update && apt-get install -y pkg-config libssl-dev libpq-dev && rm -rf /var/lib/apt/lists/*
    
    COPY . .
    RUN cargo build --release --locked
    
    # --- Stage 2: Runtime (The To-Go Box) ---
    FROM debian:bookworm-slim
    
    # Add certificates so it can talk to Kalshi/Polymarket/Supabase
    RUN apt-get update && apt-get install -y \
        ca-certificates \
        libssl-dev \
        libpq5 \
        && rm -rf /var/lib/apt/lists/*
    
    # Copy the finished engine
    COPY --from=builder /usr/src/app/target/release/britespeck_engine /usr/local/bin/rust-engine
    
    # Start the engine
    CMD ["rust-engine"]