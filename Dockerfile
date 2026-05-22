# ── build stage ──────────────────────────────────────────────────────────────
FROM rust:1.87-slim AS builder

WORKDIR /app

# cache dependency compilation separately from source changes
COPY Cargo.toml Cargo.lock ./
COPY crates/core/Cargo.toml    crates/core/Cargo.toml
COPY crates/storage/Cargo.toml crates/storage/Cargo.toml
COPY crates/api/Cargo.toml     crates/api/Cargo.toml

# stub source so cargo can resolve and cache deps without full source
RUN mkdir -p crates/core/src crates/storage/src crates/api/src && \
    echo "pub fn main() {}" > crates/api/src/main.rs && \
    echo "" > crates/core/src/lib.rs && \
    echo "" > crates/storage/src/lib.rs && \
    cargo build --release -p viscacha-api 2>/dev/null || true

# now copy real source and build for real
COPY crates/ crates/
RUN touch crates/api/src/main.rs crates/core/src/lib.rs crates/storage/src/lib.rs && \
    cargo build --release -p viscacha-api

# ── runtime stage ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /data

COPY --from=builder /app/target/release/viscacha /usr/local/bin/viscacha

# 8000 = API   (set VISCACHA_API_KEY env var to enable auth)
EXPOSE 8000

ENTRYPOINT ["viscacha"]
# default: persist to /data/jobs.db, bind on all interfaces
CMD ["jobs.db", "0.0.0.0:8000"]
