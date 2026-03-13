FROM rust:1.75 as builder
WORKDIR /usr/src/app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
# NOTE: Ensure 'britespeck_engine' matches the 'name' in your Cargo.toml
COPY --from=builder /usr/src/app/target/release/britespeck_engine /usr/local/bin/rust-engine
CMD ["rust-engine"]
