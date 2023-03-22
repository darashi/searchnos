FROM rust:1.68 AS builder

WORKDIR /usr/src/app

COPY . .
RUN cargo install --path . --bin searcher

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates
COPY --from=builder /usr/local/cargo/bin/searcher /app/searcher

WORKDIR /app

CMD ["/app/searcher"]
