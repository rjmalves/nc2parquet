# Multi-stage build for nc2parquet with AWS SDK support
FROM rust:1.89-bullseye AS builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libnetcdf-dev \
    libhdf5-dev \
    netcdf-bin \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /usr/src/nc2parquet

# Copy manifests (Cargo.lock will be generated if not present)
COPY Cargo.toml ./

# Create a dummy main.rs to cache dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release && rm -rf src/

# Copy source code
COPY src ./src
COPY examples ./examples

# Build the actual application
RUN touch src/main.rs && cargo build --release

# Runtime stage
FROM debian:bullseye-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libnetcdf18 \
    libhdf5-103-1 \
    libssl1.1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -m -u 1000 nc2parquet

# Copy the binary from builder stage
COPY --from=builder /usr/src/nc2parquet/target/release/nc2parquet /usr/local/bin/nc2parquet

# Copy examples and documentation
COPY --from=builder /usr/src/nc2parquet/examples /home/nc2parquet/examples
COPY README.md MIGRATION.md LICENSE /home/nc2parquet/

# Set ownership
RUN chown -R nc2parquet:nc2parquet /home/nc2parquet

# Switch to app user
USER nc2parquet
WORKDIR /home/nc2parquet

# Create output directory
RUN mkdir -p output

# Set environment variables for better defaults
ENV RUST_LOG=info
ENV NC2PARQUET_OUTPUT_DIR=/home/nc2parquet/output

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD nc2parquet --version || exit 1

# Default command
ENTRYPOINT ["nc2parquet"]
CMD ["--help"]

# Labels
LABEL org.opencontainers.image.title="nc2parquet"
LABEL org.opencontainers.image.description="High-performance NetCDF to Parquet converter with S3 support"
LABEL org.opencontainers.image.url="https://github.com/rjmalves/nc2parquet"
LABEL org.opencontainers.image.source="https://github.com/rjmalves/nc2parquet"
LABEL org.opencontainers.image.version="0.1.0"
LABEL org.opencontainers.image.vendor="rjmalves"