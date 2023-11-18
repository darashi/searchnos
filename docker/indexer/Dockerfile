FROM rust:1.74 AS builder

WORKDIR /usr/src/app

COPY ./indexer .
RUN cargo install --path . --bin indexer

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates
COPY --from=builder /usr/local/cargo/bin/indexer /app/indexer

WORKDIR /app

CMD ["/app/indexer"]
