%%% Public lifecycle of the extracted storage service.
-module(arweave_storage).
-behaviour(application).
-export([start/2, stop/1, child_spec/0, activate/0, activate/1, deactivate/0]).
start(normal, []) -> arweave_storage_sup:start_link().
stop(_State) -> ok.
child_spec() -> arweave_storage_lifecycle:child_spec().
activate() -> arweave_storage_sup:activate().
activate(Mode) -> arweave_storage_sup:activate(Mode).
deactivate() -> arweave_storage_sup:deactivate().
