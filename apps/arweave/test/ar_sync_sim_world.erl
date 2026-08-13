%%% @doc Typed state and behavior for the chunk-sync simulator's synthetic world.
%%% The ETS schema is private to this module; callers use domain operations.
-module(ar_sync_sim_world).
-test_category([fast]).

-compile({no_auto_import, [get/1]}).

-export([create/0, delete/0, reset/0, reset/1,
        use_mainnet_replica_2_9_sizes/0,
        storage_modules/0, store_ranges/0, store_ids/0, weave_size/0,
        get/1, update_world/1,
        get_peers_for_offset/1, get_peer_ranges_for_peers/5,
        get_chunk_binary/2,
        wait_for_entropy/2,
        is_chunk_cache_full/0, chunk_cache_size/0, chunk_cache_size/1,
        increment_chunk_cache_size/1,
        admit_store_write/3, write_completed/2,
        unsynced_intervals/3, unsynced_footprint_intervals/3,
        get_next_synced_interval/3,
        peer_sync_kind_enabled/2, peer_sync_intervals/1, snapshot/0]).

-export_type([world/0, peer/0, snapshot/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_sync.hrl").
-include("ar_sync_sim.hrl").

-type world() :: #sim_world{}.
-type peer() :: #sim_peer{}.
-type snapshot() :: #sim_snapshot{}.
-type admission() :: admitted | wait | reject.

-define(STORED_CHUNKS, ar_sync_sim_stored_chunks).

%%%===================================================================
%%% Lifecycle and world configuration.
%%%===================================================================

%% @doc Use production partition and replica.2.9 sizes for scenarios whose
%% footprint behavior depends on mainnet-scale footprints.
use_mainnet_replica_2_9_sizes() ->
    ar_block:override_partition_size(?MAINNET_PARTITION_SIZE),
    ar_replica_2_9:override_entropy_size(?MAINNET_REPLICA_2_9_ENTROPY_SIZE),
    ar_replica_2_9:override_entropy_count(?MAINNET_REPLICA_2_9_ENTROPY_COUNT).

-spec create() -> ok.
create() ->
    _ = catch ets:new(?MODULE,
        [named_table, public, set,
            {read_concurrency, true}, {write_concurrency, true}]),
    _ = catch ets:new(?STORED_CHUNKS,
        [named_table, public, ordered_set,
            {read_concurrency, true}, {write_concurrency, true}]),
    reset().

-spec delete() -> ok.
delete() ->
    _ = catch ets:delete(?MODULE),
    _ = catch ets:delete(?STORED_CHUNKS),
    ok.

%% @doc Reset to the minimal empty world used between scenarios.
-spec reset() -> ok.
reset() ->
    ets:delete_all_objects(?MODULE),
    ets:delete_all_objects(?STORED_CHUNKS),
    true = ets:insert(?MODULE, [
        {cache, 0},
        {cache_limit, 0},
        {chunk_payload, <<0:(?DATA_CHUNK_SIZE * 8)>>},
        {peers, []},
        {world, #sim_world{}}
    ]),
    ok.

%% @doc Reset from a world definition.
-spec reset(world()) -> ok.
reset(#sim_world{} = World) ->
    do_reset(World, ar_data_sync:chunk_cache_size_limit()).

do_reset(#sim_world{} = World, CacheLimit) ->
    reset(),
    put_value(cache_limit, CacheLimit),
    update_world(World).

%% @doc Return the fixed storage-module configuration shared by all scenarios.
-spec storage_modules() -> [map()].
storage_modules() ->
    [arweave_config:storage_module_to_config(Module)
        || Module <- store_modules()].

store_modules() ->
    BaseBucket = ?SIM_STORE_BASE div ?SIM_STORE_SIZE,
    [{?SIM_STORE_SIZE, BaseBucket + N, unpacked}
        || N <- lists:seq(0, ?SIM_STORES - 1)].

%% @doc Return the fixed simulation store IDs and effective byte ranges.
-spec store_ranges() -> [{term(), {non_neg_integer(), non_neg_integer()}}].
store_ranges() ->
    [{ar_storage_module:id(Module), ar_storage_module:module_range(Module)}
        || Module <- store_modules()].

%% @doc Return the fixed non-default simulation store IDs.
-spec store_ids() -> [term()].
store_ids() ->
    [StoreID || {StoreID, _Range} <- store_ranges()].

%% @doc Return the fixed simulated weave size.
-spec weave_size() -> non_neg_integer().
weave_size() ->
    lists:max([End || {_StoreID, {_Start, End}} <- store_ranges()]).

-spec update_world(world()) -> ok.
update_world(#sim_world{ peers = Peers } = World) ->
    set_peers(Peers),
    %% Peer records stay separately indexed because peer/1 is on the fetch hot
    %% path. Node configuration is applied by the runner, not mutable world state.
    put_value(world, World#sim_world{ node_config = #{}, peers = #{} }).

-spec set_peers(#{term() => peer()}) -> ok.
set_peers(Peers) ->
    put_value(peers, maps:keys(Peers)),
    maps:foreach(fun set_peer/2, Peers),
    ok.

-spec set_peer(term(), peer()) -> ok.
set_peer(Peer, Spec) ->
    put_value({peer, Peer}, Spec).

%% @doc Return one stored world value. Scenario behavior remains exposed through
%% named operations; this API replaces one-line wrappers around stored state.
-spec get(term()) -> term().
get(Key) ->
    ets:lookup_element(?MODULE, Key, 2).

%%%===================================================================
%%% Direct peer availability.
%%%===================================================================

-spec get_peers_for_offset(non_neg_integer()) -> [term()].
get_peers_for_offset(_Offset) ->
    %% Direct scenarios bypass coarse discovery. Expose every configured peer
    %% and let get_peer_ranges_for_peers/5 apply exact range availability.
    get(peers).

get_peer_ranges_for_peers(StoreID, Peers, Offset, RangeStart, RangeEnd) ->
    Requested = ar_intervals:from_list([{RangeEnd, RangeStart}]),
    PeerRanges = lists:filtermap(
        fun(Peer) ->
            Intervals = direct_peer_intersection(Peer, Requested),
            case ar_intervals:is_empty(Intervals) of
                true ->
                    false;
                false ->
                    {true, #peer_range{
                        store_id = StoreID,
                        offset = Offset,
                        peer = Peer,
                        intervals = Intervals,
                        footprint = none
                    }}
            end
        end,
        Peers),
    PeerRanges.

direct_peer_intersection(Peer, Requested) ->
    case get({peer, Peer}) of
        #sim_peer{ sync_availability = all } -> Requested;
        #sim_peer{ sync_availability = {stores, StoreIDs} } ->
            ar_intervals:intersection(store_intervals(StoreIDs), Requested);
        #sim_peer{ sync_availability = {intervals, Intervals} } ->
            ar_intervals:intersection(ar_intervals:from_list(Intervals), Requested)
    end.

store_intervals(StoreIDs) ->
    ar_intervals:from_list([
        {RangeEnd, RangeStart}
        || {StoreID, {RangeStart, RangeEnd}} <- store_ranges(),
            lists:member(StoreID, StoreIDs)
    ]).

%%%===================================================================
%%% Fetch and capacity model.
%%%===================================================================

-spec get_chunk_binary(term(), non_neg_integer()) ->
        {ok, map(), term(), non_neg_integer()} | {error, term()}.
get_chunk_binary(Name, Offset) ->
    NumInflight = add_counter({http_inflight, Name}, 1),
    try
        do_get_chunk_binary(Name, Offset, NumInflight)
    after
        _ = add_counter({http_inflight, Name}, -1)
    end.

do_get_chunk_binary(Name, Offset, NumInflight) ->
    Peer = #sim_peer{ latency_ms = Latency } = get({peer, Name}),
    Failure = failure_outcome(Name, Peer#sim_peer.failure_policy),
    DefaultLatencyMS = case Latency of
        Value when is_integer(Value) -> Value;
        LatencyFun -> LatencyFun(NumInflight)
    end,
    LatencyMS = failure_latency(Failure, DefaultLatencyMS),
    Deadline = ar_timer:monotonic_ms() + ?FETCH_TIMEOUT_MS,
    %% Connection overflow still pays the round-trip latency, as it would while
    %% waiting for a live HTTP connection.
    ar_timer:sleep(LatencyMS),
    maybe
        ok ?= check_deadline(Name, Deadline),
        ok ?= check_http_inflight_limit(Name, NumInflight,
            Peer#sim_peer.http_inflight_limit),
        ok ?= check_failure(Name, Failure),
        ok ?= wait_for_peer_capacity(Name, Deadline),
        ok ?= wait_for_link_capacity(Name, Deadline),
        ok ?= wait_for_remote_store(Name, Offset, Deadline),
        _ = add_counter({served, Name}, 1),
        {ok, #{ chunk => get(chunk_payload) }, t, ?DATA_CHUNK_SIZE}
    end.

failure_outcome(_Name, undefined) ->
    none;
failure_outcome(Name, FailurePolicy) ->
    RequestSequence = add_counter({fetch_seq, Name}, 1),
    TickIndex = ar_timer:monotonic_ms()
        div ar_sync_scheduler:tick_interval_ms(),
    FailurePolicy(TickIndex, RequestSequence).

failure_latency({_Failure, LatencyMS}, _DefaultLatencyMS) ->
    LatencyMS;
failure_latency(_Failure, DefaultLatencyMS) ->
    DefaultLatencyMS.

check_http_inflight_limit(Name, NumInflight, Limit) when NumInflight > Limit ->
    _ = add_counter({client_errors, Name}, 1),
    {error, client_error};
check_http_inflight_limit(_Name, _NumInflight, _Limit) ->
    ok.

check_failure(Name, {Failure, _LatencyMS}) ->
    check_failure(Name, Failure);
check_failure(Name, Failure) ->
    case Failure of
        none ->
            ok;
        reject ->
            _ = add_counter({rejected, Name}, 1),
            {error, {ok, {{<<"429">>, <<>>}, [], <<>>, 0, 0}}};
        timeout ->
            _ = add_counter({timeouts, Name}, 1),
            {error, timeout};
        client_error ->
            _ = add_counter({client_errors, Name}, 1),
            {error, client_error}
    end.

%% The deadline is checked at the head of every wait loop, so a request
%% overshoots it by at most the one simulated second a loop sleeps for.
check_deadline(Name, Deadline) ->
    case ar_timer:monotonic_ms() >= Deadline of
        true ->
            _ = add_counter({timeouts, Name}, 1),
            {error, timeout};
        false ->
            ok
    end.

%% A rate-limited peer rejects excess requests. An unlimited peer waits for
%% capacity in the next simulated second.
wait_for_peer_capacity(Name, Deadline) ->
    maybe
        ok ?= check_deadline(Name, Deadline),
        Second = ar_timer:monotonic_ms() div 1000,
        case admit_peer(Name, Second) of
            admitted -> ok;
            reject ->
                {error, {ok, {{<<"429">>, <<>>}, [], <<>>, 0, 0}}};
            wait ->
                sleep_until_next_second(),
                wait_for_peer_capacity(Name, Deadline)
        end
    end.

-spec admit_peer(term(), non_neg_integer()) -> admission().
admit_peer(Peer, Second) ->
    #sim_peer{ max_serve_cps = CPS, limited = Limited } = get({peer, Peer}),
    case consume_cps({peer, Peer}, CPS, Second) of
        available ->
            admitted;
        exhausted when Limited ->
            _ = add_counter({rejected, Peer}, 1),
            reject;
        exhausted ->
            wait
    end.

wait_for_link_capacity(Name, Deadline) ->
    maybe
        ok ?= check_deadline(Name, Deadline),
        Second = ar_timer:monotonic_ms() div 1000,
        case admit_link(Second) of
            admitted -> ok;
            wait ->
                sleep_until_next_second(),
                wait_for_link_capacity(Name, Deadline)
        end
    end.

-spec admit_link(non_neg_integer()) -> admitted | wait.
admit_link(Second) ->
    #sim_world{ link_capacity_cps = CPS } = get(world),
    case consume_cps(link, CPS, Second) of
        available -> admitted;
        exhausted -> wait
    end.

wait_for_remote_store(Peer, Offset, Deadline) ->
    maybe
        ok ?= check_deadline(Peer, Deadline),
        Second = ar_timer:monotonic_ms() div 1000,
        case admit_remote_store(Peer, Offset, Second) of
            admitted -> ok;
            wait ->
                sleep_until_next_second(),
                wait_for_remote_store(Peer, Offset, Deadline)
        end
    end.

-spec admit_remote_store(term(), non_neg_integer(), non_neg_integer()) -> admitted | wait.
admit_remote_store(Peer, Offset, Second) ->
    #sim_world{ remote_store_cps = CPS } = get(world),
    Store = Offset div ?SIM_STORE_SIZE,
    case consume_cps({store, Peer, Store}, CPS, Second) of
        available -> admitted;
        exhausted -> wait
    end.

%% @doc Pay the configured generation time for a footprint source's entropy.
%% The simulator stores unpacked chunks, so it models only the source-footprint
%% working set needed to unpack them: one cache entry per peer, partition, and
%% footprint. The production entropy cache manages capacity and oldest-first
%% eviction; each simulator entry is weighted as one complete footprint.
-spec wait_for_entropy(term(), non_neg_integer()) -> ok.
wait_for_entropy(Peer, Byte) ->
    #sim_world{ entropy_generation_ms = GenerationMs } = get(world),
    case footprint_only_peer(Peer) of
        false ->
            ok;
        true ->
            maybe_wait_for_entropy(Peer, Byte, GenerationMs)
    end.

footprint_only_peer(Peer) ->
    #sim_peer{ sync_kinds = Kinds } = get({peer, Peer}),
    lists:member(footprint, Kinds) andalso not lists:member(byte, Kinds).

maybe_wait_for_entropy(_Peer, _Byte, 0) ->
    ok;
maybe_wait_for_entropy(Peer, Byte, GenerationMs) ->
    {Partition, Footprint} = ar_footprint_record:get_location(
        Byte + ?DATA_CHUNK_SIZE),
    ensure_entropy({ar_sync_sim_entropy, Peer, Partition, Footprint}, GenerationMs).

ensure_entropy(Key, GenerationMs) ->
    case ar_entropy_cache:get(Key) of
        {ok, ready} ->
            ok;
        not_found ->
            generate_entropy(Key, GenerationMs)
    end.

generate_entropy(Key, GenerationMs) ->
    LockKey = {entropy_generation, Key},
    case ets:insert_new(?MODULE, {LockKey, true}) of
        true ->
            try
                %% The key may have been populated after the first cache lookup
                %% but before this process acquired the generation lock.
                case ar_entropy_cache:get(Key) of
                    {ok, ready} ->
                        ok;
                    not_found ->
                        ar_timer:sleep(GenerationMs),
                        Size = ar_block:get_replica_2_9_footprint_size(),
                        MaxSize =
                            arweave_config:get([packing, entropy, cache_size]) * ?MiB,
                        ar_entropy_cache:put_with_limit(Key, ready, Size, MaxSize)
                end
            after
                true = ets:delete(?MODULE, LockKey)
            end;
        false ->
            ar_timer:sleep(?SIM_SUBSTEP_MS),
            ensure_entropy(Key, GenerationMs)
    end.

sleep_until_next_second() ->
    ar_timer:sleep(1000 - (ar_timer:monotonic_ms() rem 1000)).

-spec consume_cps(term(), non_neg_integer() | infinity, non_neg_integer()) ->
        available | exhausted.
consume_cps(_Key, infinity, _Second) ->
    available;
consume_cps(Key, CPS, Second) ->
    case add_counter({cps, Key, Second}, 1) =< CPS of
        true -> available;
        false -> exhausted
    end.

%%%===================================================================
%%% Cache, writes, and observation.
%%%===================================================================

-spec is_chunk_cache_full() -> boolean().
is_chunk_cache_full() ->
    get(cache) >= get(cache_limit).

-spec chunk_cache_size() -> non_neg_integer().
chunk_cache_size() ->
    get(cache).

-spec chunk_cache_size(term()) -> non_neg_integer().
chunk_cache_size(StoreID) ->
    case ets:lookup(?MODULE, {cache, StoreID}) of
        [{{cache, StoreID}, Size}] -> Size;
        [] -> 0
    end.

-spec increment_chunk_cache_size(term()) -> non_neg_integer().
increment_chunk_cache_size(StoreID) ->
    _ = add_counter(cache, 1),
    add_counter({cache, StoreID}, 1).

-spec admit_store_write(term(), non_neg_integer(), non_neg_integer()) ->
        available | exhausted.
admit_store_write(StoreID, TickIndex, Second) ->
    #sim_world{ store_write_cps = StoreWriteCPS } = get(world),
    CPS = case is_function(StoreWriteCPS, 2) of
        true -> StoreWriteCPS(StoreID, TickIndex);
        false -> StoreWriteCPS
    end,
    consume_cps({write, StoreID}, CPS, Second).

%% @doc Retire one stored chunk from the simulated cache.
-spec write_completed(term(), non_neg_integer()) -> ok.
write_completed(StoreID, Byte) ->
    _ = add_counter(cache, -1),
    _ = add_counter({cache, StoreID}, -1),
    FootprintOffset = ar_footprint_record:get_offset(Byte + ?DATA_CHUNK_SIZE) - 1,
    true = ets:insert(?STORED_CHUNKS,
        {{footprint, StoreID, FootprintOffset}, true}),
    case ets:insert_new(?STORED_CHUNKS, {{byte, StoreID, Byte}, true}) of
        true ->
            _ = add_counter({chunks_stored_by_store, StoreID}, 1);
        false ->
            ok
    end,
    ok.

%% @doc A fresh store reports the queried byte range as one contiguous need. A
%% targeted fragmented scenario can retain the old alternating-chunk layout.
-spec unsynced_intervals(non_neg_integer(), non_neg_integer(), term()) ->
        ar_intervals:intervals().
unsynced_intervals(Start, End, StoreID) ->
    #sim_world{ local_data_layout = Layout } = get(world),
    Intervals = case Layout of
        contiguous -> ar_intervals:from_list([{End, Start}]);
        fragmented -> fragmented_unsynced_intervals(Start, End)
    end,
    exclude_stored_chunks(byte, StoreID, Intervals).

%% @doc Return the simulated footprint record after excluding completed writes.
%% Production updates both its byte and footprint records when a chunk is stored;
%% keeping both views aligned prevents a revisit from reoffering stored chunks.
unsynced_footprint_intervals(Partition, Footprint, StoreID) ->
    FirstChunkEnd = Partition * ar_block:partition_size()
        + (Footprint + 1) * ?DATA_CHUNK_SIZE,
    FirstFootprintOffset = ar_footprint_record:get_offset(FirstChunkEnd),
    FootprintSize = ar_replica_2_9:get_footprint_size(),
    Intervals = ar_intervals:from_list([{
        FirstFootprintOffset + FootprintSize - 1,
        FirstFootprintOffset - 1
    }]),
    exclude_stored_chunks(footprint, StoreID, Intervals).

fragmented_unsynced_intervals(Start, End) ->
    First = Start - (Start rem (2 * ?DATA_CHUNK_SIZE)),
    ar_intervals:from_list(
        [{O + ?DATA_CHUNK_SIZE, O}
            || O <- lists:seq(First, End - ?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE),
                O >= Start, O + ?DATA_CHUNK_SIZE =< End]).

exclude_stored_chunks(Mode, StoreID, Intervals) ->
    case ar_intervals:is_empty(Intervals) of
        true ->
            Intervals;
        false ->
            {_FirstEnd, Start} = ar_intervals:smallest(Intervals),
            {End, _LastStart} = ar_intervals:largest(Intervals),
            Stored = stored_intervals(Mode, StoreID, Start, End),
            ar_intervals:outerjoin(Stored, Intervals)
    end.

stored_intervals(Mode, StoreID, Start, End) ->
    Key = ets:next(?STORED_CHUNKS, {Mode, StoreID, Start - 1}),
    collect_stored_intervals(
        Key, Mode, StoreID, End, ar_intervals:new()).

collect_stored_intervals(
        {Mode, StoreID, Offset} = Key,
        Mode, StoreID, End, Intervals) when Offset < End ->
    Width = stored_chunk_width(Mode),
    Intervals2 = ar_intervals:add(
        Intervals, min(Offset + Width, End), Offset),
    collect_stored_intervals(
        ets:next(?STORED_CHUNKS, Key), Mode, StoreID, End, Intervals2);
collect_stored_intervals(
        _Key, _Mode, _StoreID, _End, Intervals) ->
    Intervals.

stored_chunk_width(byte) ->
    ?DATA_CHUNK_SIZE;
stored_chunk_width(footprint) ->
    1.

-spec get_next_synced_interval(term(), non_neg_integer(), non_neg_integer()) ->
        {non_neg_integer(), non_neg_integer()} | not_found.
get_next_synced_interval(StoreID, Byte, End) ->
    case ets:member(?STORED_CHUNKS, {byte, StoreID, Byte}) of
        true -> {min(Byte + ?DATA_CHUNK_SIZE, End), Byte};
        false -> not_found
    end.

-spec snapshot() -> snapshot().
snapshot() ->
    PeerIDs = get(peers),
    StoreIDs = store_ids(),
    #sim_snapshot{
        served_by_peer = counter_map(served, PeerIDs),
        rejected_by_peer = counter_map(rejected, PeerIDs),
        timed_out_by_peer = counter_map(timeouts, PeerIDs),
        client_errors_by_peer = counter_map(client_errors, PeerIDs),
        http_inflight_by_peer = counter_map(http_inflight, PeerIDs),
        chunks_stored_by_store = counter_map(chunks_stored_by_store, StoreIDs)
    }.

counter_map(Tag, IDs) ->
    Initial = maps:from_list([{ID, 0} || ID <- IDs]),
    ets:foldl(
        fun({{RowTag, ID}, Value}, Acc)
                when RowTag =:= Tag, is_integer(Value) ->
            maps:put(ID, Value, Acc);
            (_, Acc) ->
                Acc
        end,
        Initial,
        ?MODULE).

%%%===================================================================
%%% Peer metadata model.
%%%===================================================================

-spec peer_sync_kind_enabled(term(), byte | footprint) -> boolean().
peer_sync_kind_enabled(Peer, Kind) ->
    #sim_peer{ sync_kinds = Kinds } = get({peer, Peer}),
    lists:member(Kind, Kinds).

-spec peer_sync_intervals(term()) -> ar_intervals:intervals().
peer_sync_intervals(Peer) ->
    case get({peer, Peer}) of
        #sim_peer{ sync_availability = all } ->
            default_sync_intervals(store_ids(), true);
        #sim_peer{ sync_availability = {stores, StoreIDs} } ->
            default_sync_intervals(StoreIDs, false);
        #sim_peer{ sync_availability = {intervals, Intervals} } ->
            ar_intervals:from_list(Intervals)
    end.

default_sync_intervals(StoreIDs, IncludeLookbehind) ->
    StepSize = ar_sync_cursor:query_range_step_size(),
    AdvertBytes = 8 * StepSize,
    ar_intervals:from_list([
        {min(RangeEnd, RangeStart + AdvertBytes),
            discovery_range_start(RangeStart, StepSize, IncludeLookbehind)}
        || {StoreID, {RangeStart, RangeEnd}} <- store_ranges(),
            lists:member(StoreID, StoreIDs)
    ]).

discovery_range_start(RangeStart, StepSize, true) ->
    max(0, RangeStart - StepSize);
discovery_range_start(RangeStart, _StepSize, false) ->
    RangeStart.

%%%===================================================================
%%% Private ETS operations.
%%%===================================================================

-spec put_value(term(), term()) -> ok.
put_value(Key, Value) ->
    true = ets:insert(?MODULE, {Key, Value}),
    ok.

-spec add_counter(term(), integer()) -> integer().
add_counter(Key, Delta) ->
    ets:update_counter(?MODULE, Key, Delta, {Key, 0}).

%%%===================================================================
%%% Tests.
%%%===================================================================

queued_rejected_and_rollover_admission_test() ->
    with_world(fun() ->
        %% A one-request budget makes the first admission and first overage exact.
        Capacity = 1,
        StoreSize = ?SIM_STORE_SIZE,
        LimitedPeer = limited_peer,
        QueuedPeer = queued_peer,
        World = #sim_world{
            peers = #{
                LimitedPeer => #sim_peer{ max_serve_cps = Capacity, limited = true },
                QueuedPeer => #sim_peer{ max_serve_cps = Capacity }
            },
            link_capacity_cps = Capacity,
            remote_store_cps = Capacity
        },
        do_reset(World, 1),
        %% Simulated time begins in second zero; the next value tests rollover.
        Second = 0,
        NextSecond = Second + 1,
        ?assertEqual(admitted, admit_peer(LimitedPeer, Second)),
        ?assertEqual(reject, admit_peer(LimitedPeer, Second)),
        ?assertEqual(admitted, admit_peer(LimitedPeer, NextSecond)),
        ?assertEqual(admitted, admit_peer(QueuedPeer, Second)),
        ?assertEqual(wait, admit_peer(QueuedPeer, Second)),
        ?assertEqual(admitted, admit_peer(QueuedPeer, NextSecond)),
        ?assertEqual(admitted, admit_link(Second)),
        ?assertEqual(wait, admit_link(Second)),
        ?assertEqual(admitted, admit_link(NextSecond)),
        ?assertEqual(admitted, admit_remote_store(LimitedPeer, 0, Second)),
        ?assertEqual(wait, admit_remote_store(LimitedPeer, 0, Second)),
        ?assertEqual(admitted,
            admit_remote_store(LimitedPeer, StoreSize, Second)),
        ?assertEqual(admitted,
            admit_remote_store(LimitedPeer, 0, NextSecond)),
        Snapshot = snapshot(),
        ?assertEqual(Capacity,
            maps:get(LimitedPeer, Snapshot#sim_snapshot.rejected_by_peer))
    end).

get_chunk_binary_outcome_precedence_and_cleanup_test() ->
    with_world(fun() ->
        Name = peer,
        %% Zero allowed connections makes the first in-flight fetch overflow.
        ConnectionDepth = 0,
        AlwaysTimeout = fun(_Tick, _RequestSequence) -> timeout end,
        do_reset(#sim_world{
            peers = #{Name => #sim_peer{
                max_serve_cps = 1,
                %% One millisecond keeps this unit test focused on outcomes while
                %% preserving the fetch API's latency behavior.
                latency_ms = 1,
                http_inflight_limit = ConnectionDepth,
                failure_policy = AlwaysTimeout
            }}
        }, 1),
        ?assertEqual({error, client_error}, get_chunk_binary(Name, 0)),
        Snapshot1 = snapshot(),
        ?assertEqual(1,
            maps:get(Name, Snapshot1#sim_snapshot.client_errors_by_peer)),
        ?assertEqual(0,
            maps:get(Name, Snapshot1#sim_snapshot.timed_out_by_peer)),
        ?assertEqual(0,
            maps:get(Name, Snapshot1#sim_snapshot.http_inflight_by_peer)),
        Peer = get({peer, Name}),
        set_peer(Name, Peer#sim_peer{ http_inflight_limit = infinity }),
        ?assertEqual({error, timeout}, get_chunk_binary(Name, 0)),
        Snapshot2 = snapshot(),
        ?assertEqual(1,
            maps:get(Name, Snapshot2#sim_snapshot.timed_out_by_peer)),
        ?assertEqual(0,
            maps:get(Name, Snapshot2#sim_snapshot.http_inflight_by_peer))
    end).

cache_write_and_snapshot_test() ->
    with_world(fun() ->
        Store = store,
        %% A one-chunk cache reaches full exactly after one admission.
        CacheLimit = 1,
        World = #sim_world{},
        do_reset(World, CacheLimit),
        ?assertEqual(CacheLimit, increment_chunk_cache_size(Store)),
        ?assertEqual(CacheLimit, chunk_cache_size()),
        ?assertEqual(CacheLimit, chunk_cache_size(Store)),
        ?assert(is_chunk_cache_full()),
        update_world(World#sim_world{ store_write_cps = 0 }),
        ?assertEqual(exhausted, admit_store_write(Store, 0, 0)),
        update_world(World#sim_world{ store_write_cps = 1 }),
        %% Consumption resets on the next simulated second, not on reconfiguration.
        NextSecond = 1,
        ?assertEqual(available, admit_store_write(Store, 0, NextSecond)),
        ?assertEqual(exhausted, admit_store_write(Store, 0, NextSecond)),
        write_completed(Store, 0),
        ?assertEqual(0, chunk_cache_size()),
        ?assertEqual(0, chunk_cache_size(Store)),
        ?assertNot(is_chunk_cache_full()),
        Intervals = ar_intervals:from_list([
            {2 * ?DATA_CHUNK_SIZE, 0}
        ]),
        ?assertEqual(
            [{2 * ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE}],
            ar_intervals:to_list(
                exclude_stored_chunks(byte, Store, Intervals))),
        FootprintSize = ar_replica_2_9:get_footprint_size(),
        ?assertEqual(
            [{FootprintSize, 1}],
            ar_intervals:to_list(
                unsynced_footprint_intervals(0, 0, Store))),
        ?assertEqual(
            {?DATA_CHUNK_SIZE, 0},
            get_next_synced_interval(Store, 0, 2 * ?DATA_CHUNK_SIZE)),
        ?assertEqual(
            not_found,
            get_next_synced_interval(
                Store, ?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE)),
        %% Add a non-adjacent chunk after the first exclusion. A second query
        %% must observe the new write while preserving the exact one-chunk hole
        %% between the two writes.
        ?assertEqual(CacheLimit, increment_chunk_cache_size(Store)),
        write_completed(Store, 2 * ?DATA_CHUNK_SIZE),
        ThreeChunkIntervals = ar_intervals:from_list([
            {3 * ?DATA_CHUNK_SIZE, 0}
        ]),
        ?assertEqual(
            [{2 * ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE}],
            ar_intervals:to_list(
                exclude_stored_chunks(byte, Store, ThreeChunkIntervals))),
        ?assertEqual(
            [{FootprintSize, 2}],
            ar_intervals:to_list(
                unsynced_footprint_intervals(0, 0, Store))),
        Snapshot = snapshot(),
        %% The original and non-adjacent writes are the two unique completions.
        ExpectedStoredChunks = 2,
        ?assertEqual(ExpectedStoredChunks,
            maps:get(Store, Snapshot#sim_snapshot.chunks_stored_by_store))
    end).

local_data_layout_test() ->
    with_world(fun() ->
        World = #sim_world{},
        do_reset(World, 1),
        %% A five-chunk query is one need for a fresh store. The fragmented
        %% layout retains every other chunk: the first, third, and fifth.
        Start = 2 * ?DATA_CHUNK_SIZE,
        End = Start + 5 * ?DATA_CHUNK_SIZE,
        ?assertEqual([{End, Start}], ar_intervals:to_list(
            ar_sync_deps_sim:unsynced_intervals(Start, End, store))),
        update_world(World#sim_world{ local_data_layout = fragmented }),
        ?assertEqual([
            {Start + ?DATA_CHUNK_SIZE, Start},
            {Start + 3 * ?DATA_CHUNK_SIZE, Start + 2 * ?DATA_CHUNK_SIZE},
            {End, End - ?DATA_CHUNK_SIZE}
        ], ar_intervals:to_list(
            ar_sync_deps_sim:unsynced_intervals(Start, End, store)))
    end).

direct_byte_availability_test() ->
    with_world(fun() ->
        Peer1 = peer_1,
        Peer2 = peer_2,
        Chunk = ?DATA_CHUNK_SIZE,
        do_reset(#sim_world{
            peers = #{
                Peer1 => #sim_peer{},
                Peer2 => #sim_peer{
                    sync_availability = {intervals, [{2 * Chunk, Chunk}]}
                }
            }
        }, 1),
        ?assertEqual(lists:sort([Peer1, Peer2]),
            lists:sort(get_peers_for_offset(Chunk))),
        PeerRanges = get_peer_ranges_for_peers(store,
            [Peer1, Peer2], Chunk, 0, 3 * Chunk),
        ?assertEqual([Peer1, Peer2],
            [Peer || #peer_range{ peer = Peer } <- PeerRanges]),
        ?assertEqual(
            ar_intervals:from_list([{3 * Chunk, 0}]),
            (hd(PeerRanges))#peer_range.intervals),
        ?assertEqual(
            ar_intervals:from_list([{2 * Chunk, Chunk}]),
            (lists:nth(2, PeerRanges))#peer_range.intervals)
    end).

store_availability_test() ->
    with_world(fun() ->
        Peer = peer,
        [{SelectedStore, {SelectedStart, SelectedEnd}},
            {_OtherStore, {OtherStart, OtherEnd}} | _] = store_ranges(),
        do_reset(#sim_world{ peers = #{
            Peer => #sim_peer{ sync_availability = {stores, [SelectedStore]} }
        } }, 1),
        %% One chunk inside the selected store proves inclusion. The second
        %% chunk of the adjacent store sits beyond the configured overlap and
        %% therefore proves exclusion.
        SelectedChunkEnd = min(SelectedEnd, SelectedStart + ?DATA_CHUNK_SIZE),
        [#peer_range{ intervals = SelectedIntervals }] =
            get_peer_ranges_for_peers(SelectedStore, [Peer], SelectedChunkEnd,
                SelectedStart, SelectedChunkEnd),
        ?assertEqual([{SelectedChunkEnd, SelectedStart}],
            ar_intervals:to_list(SelectedIntervals)),
        OtherChunkStart = OtherStart + ?DATA_CHUNK_SIZE,
        OtherChunkEnd = min(OtherEnd, OtherChunkStart + ?DATA_CHUNK_SIZE),
        ?assertEqual([], get_peer_ranges_for_peers(
            other_store, [Peer], OtherChunkEnd, OtherChunkStart, OtherChunkEnd))
    end).

discovery_configuration_test() ->
    with_world(fun() ->
        Peer = peer,
        %% Two known non-serving peers distinguish peer membership from service.
        GossipPeers = [gossip_peer_1, gossip_peer_2],
        WorldIntervals = [{?DATA_CHUNK_SIZE, 0}],
        Peers = maps:merge(
            #{Peer => #sim_peer{
                max_serve_cps = 1,
                release = 101,
                sync_kinds = [footprint],
                sync_availability = {intervals, WorldIntervals}
            }},
            maps:from_list([{P, #sim_peer{ sync_kinds = [] }}
                || P <- GossipPeers])),
        do_reset(#sim_world{
            peers = Peers,
            discovery_enabled = true
        }, 1),
        ?assert((get(world))#sim_world.discovery_enabled),
        ?assertNot(peer_sync_kind_enabled(Peer, byte)),
        ?assert(peer_sync_kind_enabled(Peer, footprint)),
        ?assertEqual(WorldIntervals,
            ar_intervals:to_list(peer_sync_intervals(Peer))),
        ?assertEqual(length(GossipPeers) + 1, length(get(peers))),
        ?assertEqual(101, (get({peer, Peer}))#sim_peer.release),
        [GossipPeer | _] = get(peers) -- [Peer],
        ?assertEqual([], (get({peer, GossipPeer}))#sim_peer.sync_kinds)
    end).

with_world(Fun) ->
    create(),
    try Fun()
    after
        delete()
    end.
