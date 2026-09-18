%%% @doc Public integration surface for the chunk-sync subsystem.
-module(arweave_sync).

-behaviour(application).
-export([
    start/2,
    stop/1,
    prep_stop/1,
    activate/0,
    deactivate/0,
    register_store/1,
    restore_store/1
]).

-export([
    enabled/0,
    start_store/1,
    set_weave_size/2,
    get_peers_for_offset/1
]).

-ifdef(AR_TEST).
-export([
    internal_enqueue_chunk/3,
    internal_with_fetch_result/4,
    internal_deliver_chunk/4,
    internal_store_ready/2,
    internal_scheduler_pid/0,
    internal_inflight_fetches/1
]).
-endif.

start(normal, []) -> arweave_sync_sup:start_link().

prep_stop(State) ->
    ok = deactivate(),
    State.

stop(_State) -> ok.

%% @doc Start network workers after the host's storage services are ready.
activate() -> arweave_sync_runtime:activate().

%% @doc Quiesce sync before the host shuts down packing and storage.
deactivate() -> arweave_sync_runtime:deactivate().

%% @doc Bind a store to the calling writer, reclaiming its previous incarnation.
register_store(StoreID) ->
    arweave_sync_runtime:register_store(StoreID, self()).

%% @doc Return whether network syncing is enabled.
enabled() ->
    arweave_sync_download_limit:enabled().

%% @doc Start one store's work-discovery loop.
start_store(StoreID) ->
    ets:insert(arweave_sync_state, {{ready, StoreID}, true}),
    arweave_sync_store_sweeper:start(StoreID).

%% @doc Update the live sync bounds of one store's sweeper.
set_weave_size(StoreID, WeaveSize) ->
    ets:insert(arweave_sync_state, {{weave_size, StoreID}, WeaveSize}),
    arweave_sync_store_sweeper:set_weave_size(StoreID, WeaveSize).

%% @doc Restore published bounds and readiness when a sweeper restarts.
restore_store(StoreID) ->
    case ets:lookup(arweave_sync_state, {weave_size, StoreID}) of
        [{_, WeaveSize}] ->
            arweave_sync_store_sweeper:set_weave_size(StoreID, WeaveSize);
        [] ->
            ok
    end,
    case ets:lookup(arweave_sync_state, {ready, StoreID}) of
        [{_, true}] -> arweave_sync_store_sweeper:start(StoreID);
        [] -> ok
    end.

%% @doc Return peers whose coarse discovery data includes Offset.
get_peers_for_offset(Offset) ->
    arweave_sync_discovery:get_peers_for_offset(Offset).

-ifdef(AR_TEST).

%% @doc Queue a chunk through the real scheduler without exposing task records.
internal_enqueue_chunk(StoreID, Peer, Offset) ->
    arweave_sync_test_util:enqueue_chunk(StoreID, Peer, Offset).

%% @doc Mock fetches during Test while delivering Proof through real ingestion.
internal_with_fetch_result(Peer, Byte, Proof, Test) ->
    arweave_sync_test_util:with_fetch_result(Peer, Byte, Proof, Test).

%% @doc Admit a fetched proof with its own cache reservation and no queued task.
internal_deliver_chunk(StoreID, Peer, Byte, Proof) ->
    arweave_sync_test_util:deliver_chunk(StoreID, Peer, Byte, Proof).

%% @doc Check that ingestion is ready and bound to the expected writer.
internal_store_ready(StoreID, WriterPID) ->
    arweave_sync_test_util:store_ready(StoreID, WriterPID).

%% @doc Identify the scheduler to detect unexpected restarts in integration tests.
internal_scheduler_pid() ->
    whereis(arweave_sync_scheduler).

%% @doc Count fetch workers still running in the specified scheduler incarnation.
internal_inflight_fetches(SchedulerPID) ->
    gen_server:call(SchedulerPID, inflight_count).

-endif.
