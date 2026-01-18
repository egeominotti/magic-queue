# Stage 1: Build
FROM rust:1.92-slim-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    libprotobuf-dev \
    lld \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install newer protoc (bookworm has 3.21 which supports proto3 optional)
# Verify protoc version
RUN protoc --version

# Copy source
COPY server/ ./

# Build release binary with LTO
RUN cargo build --release

# Stage 2: Runtime (minimal)
FROM gcr.io/distroless/cc-debian12:nonroot

# Copy binary
COPY --from=builder /app/target/release/flashq-server /flashq-server

# Expose ports
EXPOSE 6789 6790 6791

# Set environment
ENV RUST_LOG=info
ENV ENV=dev
ENV RATE_LIMIT_REQUESTS=0

# Run as non-root
USER nonroot:nonroot

# Entrypoint
ENTRYPOINT ["/flashq-server"]
