FROM rust:1.67 AS builder

WORKDIR /usr/src/app

RUN mkdir -p src/searcher  && echo "fn main() {}" > src/searcher/main.rs
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release --bin searcher

COPY . .
RUN cargo install --path .

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates
COPY --from=builder /usr/local/cargo/bin/searcher /app/searcher

WORKDIR /app

CMD ["/app/searcher"]
