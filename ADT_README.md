# Arweave App Developer Toolkit API
This file provides an introduction to building applications on top of Arweave.
Two primary APIs exist for interacting with Arweave - Erlang callbacks and
an HTTP interface. You can find the files that implement these at `adt_simple`
and `ar_http_iface`.

To get started building Arweave ADT applications please clone this repository
and consider checking out the sample applications found in `src/apps`. These
sample applications provided a guide through the development of a
number of simple Arweave applications, from a basic monitoring app, to a
de-centralised microblogging service.

# Via Erlang
`adt_simple` provides an easy to use abstraction for building Arweave apps in
Erlang. The module provides a simple method (using a callback module) for
interacting with the Arweave.

All callback functions in the modules that use this system are optional.
Unimplemented callbacks will be ignored.

Supported events and callbacks:
```
  new_transaction             Processes a new transaction that has been
                                  submitted to the weave.
  confirmed_transaction       Processes new transactions that have been
                                  accepted onto a block.
 	new_block                   Processes a new block that has been added
                                  to the weave.
 	message                     Called when non-gossip messages are received
                                  by the server.
```
Each callback can take two optional arguments: the gossip server state
and an arbitrary application state term. The required inputs and outputs of
these functions are defined in the following way:
```
 	callback(NewData) -> _
 	callback(AppState, NewData) -> NewAppState
 	callback(AppState, GossipState, NewData) -> {NewAppState, GossipState}
```

# Via HTTP interface
Each node in the network runs an HTTP server that can be interacted with via
HTTP requests. The HTTP API exposes the following endpoints:
```
  GET /block/hash/[hash_id]     Returns a block by hash.  
  GET /block/height/[height]    Returns a block by block height.
  POST /block                   Adds a block passed in body.
  POST /tx                      Adds a transaction passed in body.
```
For example a GET request to http://192.168.0.0/block/height where
the body of the GET request is the block height will return that block in the
form of a JSON object.

Similarly, you can use POST to send transactions and blocks to be added
and use GET requests to to obtain peers, get a block by its height, hash or
or simply obtain information about it. In this way, it is possible to
perform many actions on the Arweave purely via platform agnostic HTTP.

Information (transactions, blocks) is always passed in the body of requests in
a JSON object format.
