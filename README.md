# Searchnos: a NIP-50 Relay (Search Notes and Other Stuff)

This is a relay server that provides a Nostr full-text search capability backed by [searchnos-db](https://github.com/darashi/searchnos-db). Client `EVENT` submissions are rejected; populate the index with source relays or the import/load commands.

## Current Limitations

* No spam filtering. 🙁
* No indexing configurations. Full-text search with normalization.

## Usage

Start server (Docker):

    cp .env.example .env
    # Edit .env to configure relays to connect to
    docker compose up

Run without Docker:

    cargo run -- --db-path ./data serve

Import events from JSONL:

    cargo run -- --db-path ./data import path/to/events.jsonl

Dump stored ndb notes to a length-prefixed binary stream:

    cargo run -- --db-path ./data dump path/to/events.dump

Load stored ndb notes from one or more length-prefixed binary streams, then
compact the loaded events into per-day partitions:

    cargo run -- --db-path ./data load path/to/events-1.dump path/to/events-2.dump

Compact the current hot event file into per-day partitions:

    cargo run -- --db-path ./data compact

Rebuild partition search and visibility sidecars:

    cargo run -- --db-path ./data reindex
    cargo run -- --db-path ./data reindex --force

`searchnos-db` takes an exclusive lock on the storage directory while it is open.
Run commands such as `serve`, `import`, `load`, `dump`, `export`, `stat`,
`compact`, and `reindex` one at a time against the same `--db-path`. A second
process that opens the same storage directory exits with a lock error.

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

- `SRC_RELAYS` (optional): comma-separated list of source relay URLs to fetch events from.
- `FETCH_KINDS` (optional): comma-separated list of numeric event kinds to fetch or reconcile with negentropy. When unset but `SRC_RELAYS` or `NEGENTROPY_RELAYS` is provided, a default set matching the NIP-50 indexer is used (`0,1,5,30023,40,41,42,43,44`).
- `NEGENTROPY_RELAYS` (optional): comma-separated list of relays used for negentropy reconcile. Send `SIGUSR2` to the process to reconcile recent days. Negentropy uses the same kind set as `FETCH_KINDS`.
- `NEGENTROPY_DAYS` (optional): number of recent UTC days reconciled on `SIGUSR2` (default: `2`).
- `SEARCHNOS_DB_PATH`: directory where searchnos-db keeps its storage files.
- `SEARCHNOS_RESPECT_FORWARDED` (optional): when set (or `--respect-forwarded` is passed to the CLI), WebSocket connection logs prefer the client inferred from the `Forwarded` header. Enable this only when the values are provided by a trusted reverse proxy.

## Static build

Run `cargo build --release --target x86_64-unknown-linux-musl` to produce a fully static binary in `target/x86_64-unknown-linux-musl/release/`.
