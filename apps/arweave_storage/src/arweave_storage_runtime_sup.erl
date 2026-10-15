%%% Activate only after the host KV, events and packing services are ready.
-module(arweave_storage_runtime_sup).
-behaviour(supervisor).
-export([start_link/1, init/1]).
-include_lib("arweave/include/ar_sup.hrl").

start_link(Mode) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Mode).

init(Mode) ->
    ets:new(ar_entropy_storage, [set, public, named_table]),
    ets:new(arweave_storage_global_sync_record, [set, public, named_table]),
    Global =
        case Mode of
            node -> [?CHILD(arweave_storage_global_sync_record, worker)];
            standalone -> []
        end,
    Children =
        [
            ?CHILD_SUP(arweave_storage_sync_record_sup, supervisor),
            ?CHILD_SUP(arweave_storage_chunk_storage_sup, supervisor)
        ] ++ Global,
    {ok, {{rest_for_one, 5, 10}, Children}}.
