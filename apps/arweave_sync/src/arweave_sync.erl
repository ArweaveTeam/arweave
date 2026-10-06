%%% @doc The sync app callback and API. The node drives sync through these phases:
%%% 1. Start (start/2), at application start: start arweave_sync_sup, with no
%%%    network workers yet.
%%% 2. Activate (activate/0), once ar has started ar_sup: start
%%%    arweave_sync_runtime_sup, which runs discovery, the scheduler, and a
%%%    chunk writer and a sweeper for each store.
%%% 3. Store updates (set_weave_size/2, start_store/1), from ar_data_sync:
%%%    record the weave size on init and on each new tip, and mark the store
%%%    ready once its local chunk copy completes. Both calls are passed on to
%%%    the store's sweeper, and each sweeper calls restore_store/1 on init to
%%%    replay them.
%%% 4. Deactivate (deactivate/0, also from prep_stop/1), before the node
%%%    stops packing and storage: stop arweave_sync_runtime_sup and clear
%%%    arweave_sync_state.
%%%
%%% Other calls:
%%% - enabled/0, from each sweeper: return whether the download rate setting
%%%   allows network sync;
%%% - get_peers_for_offset/1, from ar_mining_server: return the peers whose
%%%   discovery data covers an offset.
%%%
%%% arweave_sync_state keeps each store's weave size and readiness, so a
%%% sweeper that starts after these calls still receives them.
-module(arweave_sync).

-behaviour(application).
-export([
    start/2,
    stop/1,
    prep_stop/1,
    activate/0,
    deactivate/0
]).

-export([
    enabled/0,
    start_store/1,
    set_weave_size/2,
    restore_store/1,
    get_peers_for_offset/1
]).

-ifdef(AR_TEST).
-export([
    internal_enqueue_chunk/3,
    internal_with_fetch_result/4,
    internal_deliver_chunk/4,
    internal_scheduler_pid/0,
    internal_inflight_fetches/1
]).
-endif.

%%%===================================================================
%%% Application callbacks.
%%%===================================================================

start(normal, []) -> arweave_sync_sup:start_link().

prep_stop(State) ->
    ok = deactivate(),
    State.

stop(_State) -> ok.

%%%===================================================================
%%% Lifecycle.
%%%===================================================================

%% @doc Start the sync workers after the node's storage services are ready.
%% They start even when sync.max_download_rate is 0, because the rate can
%% change at runtime.
activate() ->
    case supervisor:start_child(arweave_sync_sup, #{
        id => arweave_sync_runtime_sup,
        start => {arweave_sync_runtime_sup, start_link, []},
        type => supervisor,
        shutdown => infinity
    }) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        Error -> Error
    end.

%% @doc Stop sync before the node shuts down packing and storage.
deactivate() ->
    case whereis(arweave_sync_sup) of
        undefined ->
            ok;
        _ ->
            case supervisor:terminate_child(arweave_sync_sup,
                    arweave_sync_runtime_sup) of
                ok ->
                    supervisor:delete_child(arweave_sync_sup,
                        arweave_sync_runtime_sup);
                {error, not_found} ->
                    ok
            end,
            ets:delete_all_objects(arweave_sync_state),
            ok
    end.

%%%===================================================================
%%% Store updates.
%%%===================================================================

%% @doc Mark the store ready and start the store's sweep loop.
start_store(StoreID) ->
    ets:insert(arweave_sync_state, {{ready, StoreID}, true}),
    arweave_sync_sweeper:start(StoreID).

%% @doc Record the store's weave size and pass it to the store's sweeper.
set_weave_size(StoreID, WeaveSize) ->
    ets:insert(arweave_sync_state, {{weave_size, StoreID}, WeaveSize}),
    arweave_sync_sweeper:set_weave_size(StoreID, WeaveSize).

%% @doc Replay a store's recorded weave size and readiness to its sweeper.
restore_store(StoreID) ->
    case ets:lookup(arweave_sync_state, {weave_size, StoreID}) of
        [{_, WeaveSize}] ->
            arweave_sync_sweeper:set_weave_size(StoreID, WeaveSize);
        [] ->
            ok
    end,
    case ets:lookup(arweave_sync_state, {ready, StoreID}) of
        [{_, true}] -> arweave_sync_sweeper:start(StoreID);
        [] -> ok
    end.

%%%===================================================================
%%% Other calls.
%%%===================================================================

enabled() ->
    arweave_sync_download_limit:enabled().

%% @doc Return the peers that advertise the sync bucket containing Offset.
get_peers_for_offset(Offset) ->
    arweave_sync_discovery:get_peers_for_offset(Offset).

%%%===================================================================
%%% Test support.
%%%===================================================================

-ifdef(AR_TEST).

%% @doc Queue a chunk through the real scheduler without exposing task records.
internal_enqueue_chunk(StoreID, Peer, Offset) ->
    arweave_sync_test_util:enqueue_chunk(StoreID, Peer, Offset).

%% @doc Run Test with mocked fetches that pass Proof to the real chunk writer.
internal_with_fetch_result(Peer, Byte, Proof, Test) ->
    arweave_sync_test_util:with_fetch_result(Peer, Byte, Proof, Test).

%% @doc Reserve cache space and hand Proof to the chunk writer without a
%% queued task.
internal_deliver_chunk(StoreID, Peer, Byte, Proof) ->
    arweave_sync_test_util:deliver_chunk(StoreID, Peer, Byte, Proof).

%% @doc Return the scheduler's PID, so tests can detect unexpected restarts.
internal_scheduler_pid() ->
    whereis(arweave_sync_scheduler).

%% @doc Count the fetch workers that SchedulerPID still has running.
internal_inflight_fetches(SchedulerPID) ->
    gen_server:call(SchedulerPID, inflight_count).

-endif.
