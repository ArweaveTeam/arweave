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
