%%% @doc Typed state and behavior for the chunk-sync simulator's synthetic world.
%%% The ETS schema is private to this module; callers use domain operations.
-module(arweave_sim_world).

-compile({no_auto_import, [get/1]}).

-export([
    create/0,
    delete/0,
    reset/0, reset/4,
    start_scenario/0,
    tick_index/0,
    use_mainnet_replica_2_9_sizes/0,
    storage_modules/0, storage_modules/1, default_storage_modules/0,
    store_ranges/0,
    store_ids/0,
    weave_size/0,
    get/1,
    update_world/1,
    get_chunk_binary/3,
    wait_for_entropy/2,
    is_chunk_cache_full/0,
    chunk_cache_size/0, chunk_cache_size/1,
    increment_chunk_cache_size/1,
    admit_store_write/3,
    write_completed/2,
    unsynced_intervals/3,
    unsynced_footprint_intervals/3,
    get_next_synced_interval/3,
    wait_for_chunk_interval_response/1,
    peer_sync_kind_enabled/2,
    peer_sync_intervals/1,
    snapshot/0
]).

-export([
    admit_peer/2,
    admit_link/1,
    admit_remote_store/3,
    set_peer/2,
    exclude_stored_chunks/3
]).

-export_type([world/0, peer/0, snapshot/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sim/include/arweave_sim.hrl").

-type world() :: #sim_world{}.
-type peer() :: #sim_peer{}.
-type snapshot() :: #sim_snapshot{}.

-define(STORED_CHUNKS, arweave_sim_stored_chunks).

%%%===================================================================
%%% Lifecycle and world configuration.
%%%===================================================================

%% @doc Use production partition and replica.2.9 sizes for scenarios whose
%% footprint behavior depends on mainnet-scale footprints.
use_mainnet_replica_2_9_sizes() ->
    arweave_constants:internal_override_partition_size(?MAINNET_PARTITION_SIZE),
    arweave_constants:internal_override_replica_2_9_entropy_size(
        ?MAINNET_REPLICA_2_9_ENTROPY_SIZE
    ),
    arweave_constants:internal_override_replica_2_9_entropy_count(
        ?MAINNET_REPLICA_2_9_ENTROPY_COUNT
    ).

create() ->
    _ =
        catch ets:new(
            ?MODULE,
            [
                named_table,
                public,
                set,
                {read_concurrency, true},
                {write_concurrency, true}
            ]
        ),
    _ =
        catch ets:new(
            ?STORED_CHUNKS,
            [
                named_table,
                public,
                ordered_set,
                {read_concurrency, true},
                {write_concurrency, true}
            ]
        ),
    reset().

delete() ->
    _ = catch ets:delete(?MODULE),
    _ = catch ets:delete(?STORED_CHUNKS),
    ok.

%% @doc Reset to the minimal empty world used between scenarios.
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
reset(#sim_world{} = World, CacheLimit, TickIntervalMS, QueryRangeBytes) ->
    reset(),
    put_value(cache_limit, CacheLimit),
    put_value(tick_interval_ms, TickIntervalMS),
    put_value(query_range_bytes, QueryRangeBytes),
    update_world(World).

%% @doc Mark the end of pipeline startup so scenario predicates use elapsed
%% test time rather than metadata warm-up time.
start_scenario() ->
    put_value(scenario_started_ms, arweave_sim_clock:monotonic_ms()).

%% @doc Return the driver-tick index relative to scenario start.
tick_index() ->
    StartedMS =
        case ets:lookup(?MODULE, scenario_started_ms) of
            [{scenario_started_ms, Value}] -> Value;
            [] -> 0
        end,
    (arweave_sim_clock:monotonic_ms() - StartedMS) div
        get(tick_interval_ms).

%% @doc Return the runtime storage modules of the current world.
storage_modules() ->
    storage_modules(get(world)).

%% @doc Return the runtime storage modules World declares, or the default
%% layout when it declares none.
storage_modules(#sim_world{storage_modules = default}) ->
    default_storage_modules();
storage_modules(#sim_world{storage_modules = Modules}) ->
    Modules.

%% @doc Return the fixed partition-aligned layout: ?SIM_STORES stores of
%% ?SIM_STORE_SIZE bytes from ?SIM_STORE_BASE.
default_storage_modules() ->
    [
        {
            ?SIM_STORE_BASE + N * ?SIM_STORE_SIZE,
            ?SIM_STORE_BASE + (N + 1) * ?SIM_STORE_SIZE,
            unpacked
        }
     || N <- lists:seq(0, ?SIM_STORES - 1)
    ].

%% @doc Return the fixed simulation store byte ranges, including overlap.
store_ranges() ->
    [store_range(Module) || Module <- storage_modules()].

store_range(Module) ->
    #store_info{id = StoreID, effective_range = Range} =
        arweave_storage:store_info(Module),
    {StoreID, Range}.

%% @doc Return the fixed non-default simulation store IDs.
store_ids() ->
    [StoreID || {StoreID, _Range} <- store_ranges()].

%% @doc Return the fixed simulated weave size.
weave_size() ->
    lists:max([End || {_StoreID, {_Start, End}} <- store_ranges()]).

update_world(#sim_world{peers = Peers} = World) ->
    set_peers(Peers),
    %% Peer records stay separately indexed because peer/1 is on the fetch hot
    %% path. Node configuration is applied by the runner, not mutable world state.
    put_value(world, World#sim_world{node_config = #{}, peers = #{}}).

set_peers(Peers) ->
    put_value(peers, maps:keys(Peers)),
    maps:foreach(fun set_peer/2, Peers),
    ok.

set_peer(Peer, Spec) ->
    put_value({peer, Peer}, Spec).

%% @doc Return one stored world value. Scenario behavior remains exposed through
%% named operations; this API replaces one-line wrappers around stored state.
get(Key) ->
    ets:lookup_element(?MODULE, Key, 2).

%%%===================================================================
%%% Fetch and capacity model.
%%%===================================================================

get_chunk_binary(Name, Offset, TimeoutMS) ->
    NumInflight = add_counter({http_inflight, Name}, 1),
    try
        do_get_chunk_binary(Name, Offset, NumInflight, TimeoutMS)
    after
        _ = add_counter({http_inflight, Name}, -1)
    end.

%% @doc Apply detailed-metadata latency while exposing concurrent requests to
%% scenarios that model a peer whose metadata endpoint overloads under fan-out.
wait_for_chunk_interval_response(Peer) ->
    NumInflight = add_counter({chunk_interval_inflight, Peer}, 1),
    _ = add_counter({chunk_interval_requests, Peer}, 1),
    try
        #sim_peer{chunk_interval_latency_ms = Latency} = get({peer, Peer}),
        LatencyMS =
            case Latency of
                Value when is_integer(Value) -> Value;
                LatencyFun -> LatencyFun(NumInflight)
            end,
        arweave_sim_clock:sleep(LatencyMS)
    after
        _ = add_counter({chunk_interval_inflight, Peer}, -1)
    end.

do_get_chunk_binary(Name, Offset, NumInflight, TimeoutMS) ->
    Peer = #sim_peer{latency_ms = Latency} = get({peer, Name}),
    Failure = failure_outcome(Name, Peer#sim_peer.failure_policy),
    DefaultLatencyMS =
        case Latency of
            Value when is_integer(Value) -> Value;
            LatencyFun -> LatencyFun(NumInflight)
        end,
    LatencyMS = failure_latency(Failure, DefaultLatencyMS),
    Deadline = arweave_sim_clock:monotonic_ms() + TimeoutMS,
    %% Connection overflow still pays the round-trip latency, as it would while
    %% waiting for a live HTTP connection.
    arweave_sim_clock:sleep(LatencyMS),
    maybe
        ok ?= check_deadline(Name, Deadline),
        ok ?=
            check_http_inflight_limit(
                Name,
                NumInflight,
                Peer#sim_peer.http_inflight_limit
            ),
        ok ?= check_failure(Name, Failure),
        ok ?= wait_for_peer_capacity(Name, Deadline),
        ok ?= wait_for_link_capacity(Name, Deadline),
        ok ?= wait_for_remote_store(Name, Offset, Deadline),
        _ = add_counter({served, Name}, 1),
        {ok, #{chunk => get(chunk_payload)}, t, ?DATA_CHUNK_SIZE}
    end.

failure_outcome(_Name, undefined) ->
    none;
failure_outcome(Name, FailurePolicy) ->
    RequestSequence = add_counter({fetch_seq, Name}, 1),
    TickIndex = tick_index(),
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
    case arweave_sim_clock:monotonic_ms() >= Deadline of
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
        Second = arweave_sim_clock:monotonic_ms() div 1000,
        case admit_peer(Name, Second) of
            admitted ->
                ok;
            reject ->
                {error, {ok, {{<<"429">>, <<>>}, [], <<>>, 0, 0}}};
            wait ->
                sleep_until_next_second(),
                wait_for_peer_capacity(Name, Deadline)
        end
    end.

admit_peer(Peer, Second) ->
    #sim_peer{max_serve_cps = CPS, limited = Limited} = get({peer, Peer}),
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
        Second = arweave_sim_clock:monotonic_ms() div 1000,
        case admit_link(Second) of
            admitted ->
                ok;
            wait ->
                sleep_until_next_second(),
                wait_for_link_capacity(Name, Deadline)
        end
    end.

admit_link(Second) ->
    #sim_world{link_capacity_cps = CPS} = get(world),
    case consume_cps(link, CPS, Second) of
        available -> admitted;
        exhausted -> wait
    end.

wait_for_remote_store(Peer, Offset, Deadline) ->
    maybe
        ok ?= check_deadline(Peer, Deadline),
        Second = arweave_sim_clock:monotonic_ms() div 1000,
        case admit_remote_store(Peer, Offset, Second) of
            admitted ->
                ok;
            wait ->
                sleep_until_next_second(),
                wait_for_remote_store(Peer, Offset, Deadline)
        end
    end.

admit_remote_store(Peer, Offset, Second) ->
    #sim_world{remote_store_cps = CPS} = World = get(world),
    Store = covering_store(Offset, storage_modules(World)),
    case consume_cps({store, Peer, Store}, CPS, Second) of
        available -> admitted;
        exhausted -> wait
    end.

%% @doc Return the start of the first declared store covering Offset, as
%% arweave_storage:covering_store/2 does for the configured modules. Peers
%% mirror the node's layout, so the store is the peer's too; an offset no
%% store covers shares one budget.
covering_store(Offset, Modules) ->
    case [Start || {Start, End, _} <- Modules, Offset > Start, Offset =< End] of
        [Start | _] -> Start;
        [] -> not_found
    end.

%% @doc Pay the configured generation time for a footprint source's entropy.
%% The simulator stores unpacked chunks, so it models only the source-footprint
%% working set needed to unpack them: one cache entry per peer, partition, and
%% footprint. The production entropy cache manages capacity and oldest-first
%% eviction; each simulator entry is weighted as one complete footprint.
wait_for_entropy(Peer, Byte) ->
    #sim_world{entropy_generation_ms = GenerationMs} = get(world),
    case footprint_only_peer(Peer) of
        false ->
            ok;
        true ->
            maybe_wait_for_entropy(Peer, Byte, GenerationMs)
    end.

footprint_only_peer(Peer) ->
    #sim_peer{sync_kinds = Kinds} = get({peer, Peer}),
    lists:member(footprint, Kinds) andalso not lists:member(byte, Kinds).

maybe_wait_for_entropy(_Peer, _Byte, 0) ->
    ok;
maybe_wait_for_entropy(Peer, Byte, GenerationMs) ->
    {Partition, Footprint} = arweave_storage:get_footprint_location(
        Byte + ?DATA_CHUNK_SIZE
    ),
    ensure_entropy({arweave_sim_entropy, Peer, Partition, Footprint}, GenerationMs).

ensure_entropy(Key, GenerationMs) ->
    case arweave_entropy:internal_get_cached(Key) of
        {ok, ready} ->
            ok;
        not_found ->
            generate_entropy(Key, GenerationMs)
    end.

generate_entropy({arweave_sim_entropy, Peer, _Partition, _Footprint} = Key,
        GenerationMs) ->
    LockKey = {entropy_generation, Key},
    case ets:insert_new(?MODULE, {LockKey, true}) of
        true ->
            try
                %% The key may have been populated after the first cache lookup
                %% but before this process acquired the generation lock.
                case arweave_entropy:internal_get_cached(Key) of
                    {ok, ready} ->
                        ok;
                    not_found ->
                        arweave_sim_clock:sleep(GenerationMs),
                        _ = add_counter({entropy_generations, Peer}, 1),
                        Size = arweave_constants:get_replica_2_9_footprint_size(),
                        MaxSize =
                            arweave_config:get([packing, entropy, cache_size]) * ?MiB,
                        arweave_entropy:internal_cache(Key, ready, Size, MaxSize)
                end
            after
                true = ets:delete(?MODULE, LockKey)
            end;
        false ->
            arweave_sim_clock:sleep(?SIM_SUBSTEP_MS),
            ensure_entropy(Key, GenerationMs)
    end.

sleep_until_next_second() ->
    arweave_sim_clock:sleep(1000 - (arweave_sim_clock:monotonic_ms() rem 1000)).

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

is_chunk_cache_full() ->
    get(cache) >= get(cache_limit).

chunk_cache_size() ->
    get(cache).

chunk_cache_size(StoreID) ->
    case ets:lookup(?MODULE, {cache, StoreID}) of
        [{{cache, StoreID}, Size}] -> Size;
        [] -> 0
    end.

increment_chunk_cache_size(StoreID) ->
    _ = add_counter(cache, 1),
    add_counter({cache, StoreID}, 1).

admit_store_write(StoreID, TickIndex, Second) ->
    #sim_world{store_write_cps = StoreWriteCPS} = get(world),
    CPS =
        case is_function(StoreWriteCPS, 2) of
            true -> StoreWriteCPS(StoreID, TickIndex);
            false -> StoreWriteCPS
        end,
    consume_cps({write, StoreID}, CPS, Second).

%% @doc Retire one stored chunk from the simulated cache.
write_completed(StoreID, Byte) ->
    _ = add_counter(cache, -1),
    _ = add_counter({cache, StoreID}, -1),
    FootprintOffset = arweave_storage:get_footprint_offset(Byte + ?DATA_CHUNK_SIZE) - 1,
    true = ets:insert(
        ?STORED_CHUNKS,
        {{footprint, StoreID, FootprintOffset}, true}
    ),
    case ets:insert_new(?STORED_CHUNKS, {{byte, StoreID, Byte}, true}) of
        true ->
            _ = add_counter({chunks_stored_by_store, StoreID}, 1);
        false ->
            ok
    end,
    ok.

%% @doc A fresh store reports the queried byte range as one contiguous need. A
%% targeted fragmented scenario can retain the old alternating-chunk layout.
unsynced_intervals(Start, End, StoreID) ->
    #sim_world{local_data_layout = Layout} = get(world),
    Intervals =
        case Layout of
            contiguous -> ar_intervals:from_list([{End, Start}]);
            fragmented -> fragmented_unsynced_intervals(Start, End)
        end,
    exclude_stored_chunks(byte, StoreID, Intervals).

%% @doc Return the simulated footprint record after excluding completed writes.
%% Production updates both its byte and footprint records when a chunk is stored;
%% keeping both views aligned prevents a revisit from reoffering stored chunks.
unsynced_footprint_intervals(Partition, Footprint, StoreID) ->
    FirstChunkEnd =
        Partition * arweave_constants:partition_size() +
            (Footprint + 1) * ?DATA_CHUNK_SIZE,
    FirstFootprintOffset = arweave_storage:get_footprint_offset(FirstChunkEnd),
    FootprintSize = arweave_constants:get_sub_chunks_per_replica_2_9_entropy(),
    Intervals = ar_intervals:from_list([
        {
            FirstFootprintOffset + FootprintSize - 1,
            FirstFootprintOffset - 1
        }
    ]),
    exclude_stored_chunks(footprint, StoreID, Intervals).

fragmented_unsynced_intervals(Start, End) ->
    First = Start - (Start rem (2 * ?DATA_CHUNK_SIZE)),
    ar_intervals:from_list(
        [
            {O + ?DATA_CHUNK_SIZE, O}
         || O <- lists:seq(First, End - ?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE),
            O >= Start,
            O + ?DATA_CHUNK_SIZE =< End
        ]
    ).

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
        Key, Mode, StoreID, End, ar_intervals:new()
    ).

collect_stored_intervals(
    {Mode, StoreID, Offset} = Key,
    Mode,
    StoreID,
    End,
    Intervals
) when Offset < End ->
    Width = stored_chunk_width(Mode),
    Intervals2 = ar_intervals:add(
        Intervals, min(Offset + Width, End), Offset
    ),
    collect_stored_intervals(
        ets:next(?STORED_CHUNKS, Key), Mode, StoreID, End, Intervals2
    );
collect_stored_intervals(
    _Key, _Mode, _StoreID, _End, Intervals
) ->
    Intervals.

stored_chunk_width(byte) ->
    ?DATA_CHUNK_SIZE;
stored_chunk_width(footprint) ->
    1.

get_next_synced_interval(StoreID, Byte, End) ->
    case ets:member(?STORED_CHUNKS, {byte, StoreID, Byte}) of
        true -> {min(Byte + ?DATA_CHUNK_SIZE, End), Byte};
        false -> not_found
    end.

snapshot() ->
    PeerIDs = get(peers),
    StoreIDs = store_ids(),
    #sim_snapshot{
        served_by_peer = counter_map(served, PeerIDs),
        rejected_by_peer = counter_map(rejected, PeerIDs),
        timed_out_by_peer = counter_map(timeouts, PeerIDs),
        client_errors_by_peer = counter_map(client_errors, PeerIDs),
        http_inflight_by_peer = counter_map(http_inflight, PeerIDs),
        chunk_interval_requests_by_peer =
            counter_map(chunk_interval_requests, PeerIDs),
        chunk_interval_inflight_by_peer =
            counter_map(chunk_interval_inflight, PeerIDs),
        chunks_stored_by_store = counter_map(chunks_stored_by_store, StoreIDs),
        entropy_generations_by_peer =
            counter_map(entropy_generations, PeerIDs)
    }.

counter_map(Tag, IDs) ->
    Initial = maps:from_list([{ID, 0} || ID <- IDs]),
    ets:foldl(
        fun
            ({{RowTag, ID}, Value}, Acc) when
                RowTag =:= Tag, is_integer(Value)
            ->
                maps:put(ID, Value, Acc);
            (_, Acc) ->
                Acc
        end,
        Initial,
        ?MODULE
    ).

%%%===================================================================
%%% Peer metadata model.
%%%===================================================================

peer_sync_kind_enabled(Peer, Kind) ->
    #sim_peer{sync_kinds = Kinds} = get({peer, Peer}),
    lists:member(Kind, Kinds).

peer_sync_intervals(Peer) ->
    case get({peer, Peer}) of
        #sim_peer{sync_availability = all} ->
            default_sync_intervals(store_ids(), true);
        #sim_peer{sync_availability = {stores, StoreIDs}} ->
            default_sync_intervals(StoreIDs, false);
        #sim_peer{sync_availability = {intervals, Intervals}} ->
            ar_intervals:from_list(Intervals)
    end.

default_sync_intervals(StoreIDs, IncludeLookbehind) ->
    StepSize = get(query_range_bytes),
    AdvertBytes = 8 * StepSize,
    ar_intervals:from_list([
        {
            min(RangeEnd, RangeStart + AdvertBytes),
            discovery_range_start(RangeStart, StepSize, IncludeLookbehind)
        }
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

put_value(Key, Value) ->
    true = ets:insert(?MODULE, {Key, Value}),
    ok.

add_counter(Key, Delta) ->
    ets:update_counter(?MODULE, Key, Delta, {Key, 0}).
