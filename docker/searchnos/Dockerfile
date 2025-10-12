FROM rust:1.90 AS builder

WORKDIR /usr/src/app

# Cache dependencies
COPY Cargo.toml Cargo.lock build.rs ./
RUN mkdir src
RUN echo 'fn main() {}' > src/main.rs
RUN cargo build --release --locked

# Build actual sources
RUN rm -rf src
COPY . .
RUN cargo build --release --locked

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/app/target/release/searchnos /app/searchnos

WORKDIR /app

CMD ["/app/searchnos", "serve"]
