# Searchnos: a NIP-50 Relay (Search Notes and Other Stuff)

This is a relay server that provides a Nostr full-text search capability backed by [searchnos-db](https://github.com/darashi/searchnos-db).

Connections must authenticate via [NIP-42](https://github.com/nostr-protocol/nips/blob/master/42.md) using one of the configured admin public keys before they can publish. Authenticated (administrative) connections may send `EVENT`s, while unauthenticated connections are limited to search-only operations.

## Current Limitations

* No spam filtering. 🙁
* No indexing configurations. Just does N-gram indexing with some normalization.

## Usage

Start server (Docker):

    cp .env.example .env
    # Edit .env to configure relays to connect to
    docker compose up

Run without Docker:

    cargo run -- serve --db-path ./data

Import events from JSONL:

    cargo run -- import --db-path ./data path/to/events.jsonl

Search:

    wscat --connect ws://localhost:3000
    Connected (press CTRL+C to quit)
    > ["REQ", "SEARCH_TEST", {"search": "nostr"}]
    (...snip...)
    < ["EOSE","SEARCH_TEST"]
    >

## Configuration

See `compose.yaml` and `.env.example` for the configuration.

`SRC_RELAYS` and `FETCH_KINDS` can be a comma-separated list.

- `ADMIN_PUBKEYS`: comma-separated list of admin public keys (hex or `npub`). Only these keys can authenticate via NIP-42 to publish events.
- `PUBLIC_RELAY_URL` (optional): canonical relay URL used to validate `relay` tags in AUTH events (e.g. `wss://searchnos.example.com`).
- `SRC_RELAYS` (optional): comma-separated list of source relay URLs to fetch events from.
- `FETCH_KINDS` (optional): comma-separated list of numeric event kinds to fetch. When unset but `SRC_RELAYS` is provided, a default set matching the NIP-50 indexer is used (`0,1,5,30023,40,41,42,43,44`).
- `SEARCHNOS_DB_PATH`: directory where searchnos-db keeps its LMDB files.
- `SEARCHNOS_DB_BATCH_SIZE`: number of events buffered before the batch is flushed to LMDB (default `4096`).
- `SEARCHNOS_DB_FLUSH_INTERVAL_MS`: maximum time in milliseconds to wait before flushing pending events (default `100`).
- `SEARCHNOS_DB_PURGE` (optional): purge specifications applied to the database. Leave unset to keep events indefinitely. Example: `SEARCHNOS_DB_PURGE=90d,0:1y,40:1y,41:1y,30023:1y` keeps kinds `0`, `40`, `41`, and `30023` for one year and everything else for 90 days.
- `SEARCHNOS_RESPECT_FORWARDED` (optional): when set (or `--respect-forwarded` is passed to the CLI), WebSocket connection logs prefer the client inferred from the `Forwarded` header. Enable this only when the values are provided by a trusted reverse proxy.

## Static build

Run `cargo build --release --target x86_64-unknown-linux-musl` to produce a fully static binary in `target/x86_64-unknown-linux-musl/release/`.
