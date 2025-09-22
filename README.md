# Searchnos: a NIP-50 Relay (Search Notes and Other Stuff)

This is a relay server that provides a Nostr full-text search capability by using Elasticsearch as a backend.

Connections must authenticate via [NIP-42](https://github.com/nostr-protocol/nips/blob/master/42.md) using one of the configured admin public keys before they can publish. Authenticated (administrative) connections may send `EVENT`s, while unauthenticated connections are limited to search-only operations.

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

- `ADMIN_PUBKEYS`: comma-separated list of admin public keys (hex or `npub`). Only these keys can authenticate via NIP-42 to publish events.
- `PUBLIC_RELAY_URL` (optional): canonical relay URL used to validate `relay` tags in AUTH events (e.g. `wss://searchnos.example.com`).
- `INDEXER_SECRET_KEY`: secret key (hex or `nsec`) shared with the indexer so it can complete the NIP-42 authentication handshake against Searchnos.
- `DAILY_INDEX_TTL`: TTL for daily indices in days.
- `YEARLY_INDEX_KINDS`: comma-separated list of numeric kinds to store in yearly indices (e.g. `0,40,41,30023`). If unset, all kinds are stored in day-based indices.
- `YEARLY_INDEX_TTL`: TTL for yearly indices in years (e.g. `2` keeps the current and previous year). Purger drops an index at midnight Jan 1 when it exceeds TTL.
