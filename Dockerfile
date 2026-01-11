# Build stage
FROM rust:1.75-slim-bookworm AS builder

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy source
COPY server/ ./

# Build release binary
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/magic-queue-server /app/magic-queue-server

# Expose TCP port
EXPOSE 6789

# Environment variables
ENV RUST_LOG=info
ENV PERSIST=0

# Run server
CMD ["./magic-queue-server"]
