FROM rust:1.80 AS builder

WORKDIR /usr/src/app

COPY . .
RUN cargo install --path .
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates
COPY --from=builder /usr/local/cargo/bin/searchnos /app/searchnos

WORKDIR /app

CMD ["/app/searchnos"]
