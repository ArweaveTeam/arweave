# Archain App Developer Toolkit API
This file provides an introduction to building applications on top of Archain.
Two primary APIs exist for interacting with Archain - Erlang callbacks and
an HTTP interface. You can find the files that implement these at `adt_simple`
and `ar_http_iface`.

To get started building Archain ADT applications please clone this repository
and consider checking out the sample applications found in `src/apps`. These
sample applications provided a guide through the development of a
number of simple Archain applications, from a basic monitoring app, to a
de-centralised microblogging service.

# Via Erlang
`adt_simple` provides an easy to use abstraction for building Archain apps in
Erlang. The module provides a simple method (using a callback module) for
interacting with the Archain.

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
Information (transaction, block) is passed in the body of requests in JSON
format.

# More information

Visit `adt_simple` and `ar_http_iface` for implementation details and for
comprehensive function documentation.

You can also find detailed tutorials about building Archain apps and services
on the [Archain youtube channel](http://www.youtube.com/archain).

For more information on the Archain project and to read our whitepaper visit
[Archain.org](https://www.archain.org/).

# Disclaimer

As of Monday November 6th 2017 **Archain is in Alpha**.

_Archain is currently in alpha and is in constant, active development.
Please be aware that the API for the Archain ADT will likely be added to such
that its current state is a subset of its future capabilities._
