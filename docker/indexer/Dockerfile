FROM rust:1.67 AS builder

WORKDIR /usr/src/app

COPY . .
RUN cargo install --path .

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates
COPY --from=builder /usr/local/cargo/bin/indexer /app/indexer

WORKDIR /app

CMD ["/app/indexer"]
