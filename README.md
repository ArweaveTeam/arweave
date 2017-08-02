# Archain Server and App Developer Toolkit

This repository holds the Archain server prototype, as well as the App Developer Toolkit (ADT).

# Requirements

In order to run the Archain server prototype and ADT, a recent (R19 and above) version of Erlang is required.

# Getting Started

To get started building Archain ADT applications please clone this repository
and consider checking out the sample applications found in `src/apps`. These
sample applications provided a guide through the development of a
number of simple Archain applications, from a basic monitoring app, to a
de-centralised microblogging service.

You can start an Archain server session by running `make session`. The tests
can be executed by calling `make test_all`.

You can also find detailed tutorials about building Archain apps and services
on the [Archain youtube channel](http://www.youtube.com/archain).

Caution: Archain is in active development. Please be aware that the API for the
Archain ADT will likely change before the release of the final product.

# Archain App Developer Toolkit API

`adt_simple` provides an easy to use abstraction for building Archain apps.
The module provides a simple method (using a callback module) for interacting
with the Archain.

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
