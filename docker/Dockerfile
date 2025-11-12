FROM rust:1.91 AS builder

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y --no-install-recommends musl-tools pkg-config \
    && rm -rf /var/lib/apt/lists/*
RUN rustup target add x86_64-unknown-linux-musl

# Cache dependencies
COPY Cargo.toml Cargo.lock build.rs ./
RUN mkdir src
RUN echo 'fn main() {}' > src/main.rs
RUN cargo build --release --locked --target x86_64-unknown-linux-musl

# Build actual sources
RUN rm -rf src
COPY . .
RUN cargo build --release --locked --target x86_64-unknown-linux-musl

FROM debian:trixie-slim AS certs
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

FROM scratch
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/src/app/target/x86_64-unknown-linux-musl/release/searchnos /app/searchnos

WORKDIR /app

CMD ["/app/searchnos", "serve"]
