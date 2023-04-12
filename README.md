# Searchnos: an experimental implementation of NIP-50

This is a relay-like bridge server that provides a Nostr full-text search capability by using Elasticsearch as a backend. It emulates real-time search by polling Elasticsearch.

At first glance, it behaves like a relay, but it only handles requests related to [NIP-50](https://github.com/nostr-protocol/nips/blob/master/50.md). Unlike normal relays, it connects to other relays to retrieve notes. It ignores any events (`EVENT`) from the clients. Queries are interpreted as [Simple query string query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-simple-query-string-query.html).

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
    > ["REQ", "SEARCH_TEST", {"search": "nostr|damus"}]
    (...snip...)
    < ["EOSE","SEARCH_TEST"]
    >

## Configuration

See `compose.yaml` and `.env.example` for the configuration.

`SRC_RELAYS` and `DEST_RELAYS` can be a comma-separated list of relay URLs.
