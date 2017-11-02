# The Archain Network

This (draft!) document aims to provide a breif overview of the key design points of the Archain network.

## Summary

Every node in the Archain network runs a HTTP server, which exposes an interface to a miner (which does not have to be active).

Inside a node, a gossip-based message passing network between Archain ADT app and mining nodes is employed.

## HTTP Interface

The Archain HTTP interface exposes the following endpoints:

```
	GET /block/hash/[hash_id]
	GET /block/height/[height]
	POST /block
	POST /api/tx
```

JSON structures are employed in the bodies of the HTTP requests to represent blocks and transactions in POST requests. Similarly, GET requests return their objects in a JSON encoded format. The JSON object encodings follow the field names (etc.) described in `src/ar.hrl`.

## Gossipping

Archain is essentially a gossip network, in which nodes repeat valid messages that they have received to their peers.

Nodes maintain a list of hashes of the messages they have received, not forwarding those that have already been sent once.
