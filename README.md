# Searchnos: an experimental implementation of NIP-50

This is a relay-like bridge server that provides a Nostr full-text search capability by using Elasticsearch as a backend. It emulates real-time search by polling Elasticsearch.

Ssearchnos works like a relay, with a few exceptions.

The most significant difference is that queries without the `search` property are ignored. This is mainly for load control; since Searchnos cannot respond so fast, it is intended to be used in conjunction with other regular relays that can handle non-search queries at great speed.

Another difference is that Searchnos does not accept `EVENT` messages from regular connections. When opening a WebSocket connection, if a pre-configured API key is specified as `?api_key=foo` query parameter, the connection is treated specially as an administrative connection. Searchnos only receives `EVENT`s from such connections.

👻 This project was created as an exercise in Rust programming for the author. 👻

## Current Limitations

* Supports filters that contains `"search"` property.
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
