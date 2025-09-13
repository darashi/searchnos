# Searchnos: a NIP-50 Relay (Search Notes and Other Stuff)

This is a relay server that provides a Nostr full-text search capability by using Elasticsearch as a backend.

Searchnos works like a relay, with an exception; Searchnos does not accept `EVENT` messages from regular connections. When opening a WebSocket connection, if a pre-configured API key is specified as `?api_key=foo` query parameter, the connection is treated specially as an administrative connection. Searchnos only receives `EVENT`s from such connections.

## Current Limitations

* No spam filtering. 🙁
* No indexing configurations. Just does N-gram indexing with some normalization.

## Usage

Start server:

    cp .env.example .env
    # Edit .env to configure relays to connect to
    docker compose up

Search:

    wscat --connect ws://localhost:3000
    Connected (press CTRL+C to quit)
    > ["REQ", "SEARCH_TEST", {"search": "nostr"}]
    (...snip...)
    < ["EOSE","SEARCH_TEST"]
    >

## Configuration

See `compose.yaml` and `.env.example` for the configuration.

`SRC_RELAYS` and `DEST_RELAYS` can be a comma-separated list of relay URLs.

- `DAILY_INDEX_TTL`: TTL for daily indices in days.
- `YEARLY_INDEX_KINDS`: comma-separated list of numeric kinds to store in yearly indices (e.g. `0,40,41,30023`). If unset, all kinds are stored in day-based indices.
- `YEARLY_INDEX_TTL`: TTL for yearly indices in years (e.g. `2` keeps the current and previous year). Purger drops an index at midnight Jan 1 when it exceeds TTL.
