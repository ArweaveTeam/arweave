%%% Synthetic external dependencies for the real sync pipeline.
%%% Selected APIs are implemented here when they need simulated behavior.
-module(arweave_sync_deps_sim).
-behaviour(arweave_sync_deps).

-export([
    clock/0,
    peers/0,
    throttling/0,
    http/0,
    chunk_cache/0,
    data_sync/0,
    storage/0,
    blacklist/0,
    footprint_limit/0,
    node/0,
    device_lock/0,
    disk_pool/0,
    events/0,
    sync_buckets/0,
    replica/0,
    packing/0,
    constants/0
]).
-export([
    get_sync_buckets/1,
    get_footprint_buckets/1,
    get_sync_record/3, get_sync_record/4,
    get_footprints/3,
    pick_peers/2,
    rate_fetched_data/5,
    get_peer_release/1,
    get_peers/1,
    is_throttled/2,
    get_chunk_binary/3,
    log_failed_request/2,
    is_full/0,
    cached_size/0, cached_size/1,
    limit/0,
    interval_limit/0,
    reserve/1,
    transfer/2,
    release/1,
    mark_cached/1,
    completed/1,
    init_sync_status/1,
    is_disk_space_sufficient/1,
    is_footprint_record_initialized/1,
    get_intervals/6,
    get_next_interval/6,
    get_next_not_blacklisted_byte/1,
    get_blacklisted_intervals/2,
    is_joined/0,
    get_weave_size/0,
    acquire_lock/3,
    get_threshold/0,
    subscribe/1
]).

-include_lib("arweave/include/ar_peers.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include_lib("arweave_sim/include/arweave_sim.hrl").

%%%===================================================================
%%% arweave_sync_deps callbacks.
%%%===================================================================

clock() -> arweave_sim.
peers() -> ?MODULE.
throttling() -> ?MODULE.
http() -> ?MODULE.
chunk_cache() -> ?MODULE.
data_sync() -> ?MODULE.
storage() -> ?MODULE.
blacklist() -> ?MODULE.
footprint_limit() -> ar_footprint_limit.
node() -> ?MODULE.
device_lock() -> ?MODULE.
disk_pool() -> ?MODULE.
events() -> ?MODULE.
sync_buckets() -> ar_sync_buckets.
replica() -> ar_replica_2_9.
packing() -> ar_packing_server.
constants() -> arweave_constants.

pick_peers(Peers, Count) ->
    {Picked, _Rest} = arweave_util:split_at_most(Count, Peers),
    Picked.

is_throttled(Peer, _Path) ->
    #sim_peer{is_throttled = IsThrottled} =
        arweave_sim:world_value({peer, Peer}),
    IsThrottled.

%% Peer goodput derives from completed fetches reported through the scheduler;
%% ar_peers plays no part.
rate_fetched_data(_Peer, _DataType, _Result, _ElapsedUs, _Bytes) ->
    ok.

get_chunk_binary(Name, Offset, _Packing) ->
    arweave_sim:get_chunk_binary(Name, Offset, ?FETCH_TIMEOUT_MS).

is_full() ->
    arweave_sim:is_chunk_cache_full().

cached_size() ->
    arweave_sim:chunk_cache_size().

cached_size(StoreID) ->
    arweave_sim:chunk_cache_size(StoreID).

limit() ->
    arweave_sim:world_value(cache_limit).

is_disk_space_sufficient(_StoreID) ->
    true.

is_footprint_record_initialized(_StoreID) ->
    true.

get_next_not_blacklisted_byte(Byte) ->
    Byte.

%%%===================================================================
%%% arweave_sync_discovery's outward dependencies: the peer registry and metadata
%%% endpoints, modeled as real workers paying latency on arweave_sim's clock.
%%%===================================================================

%% Above every release gate (?GET_FOOTPRINT_SUPPORT_RELEASE = 91).
get_peer_release(Peer) ->
    (arweave_sim:world_value({peer, Peer}))#sim_peer.release.

get_peers(current) ->
    arweave_sim:world_value(peers).

is_joined() ->
    true.

get_weave_size() ->
    arweave_sim:weave_size().

get_sync_buckets(Peer) ->
    get_sync_buckets(Peer, byte).

get_footprint_buckets(Peer) ->
    get_sync_buckets(Peer, footprint).

get_sync_buckets(Peer, Mode) ->
    arweave_sim:sleep(?SIM_METADATA_LATENCY_MS),
    do_get_sync_buckets(Peer, Mode).

do_get_sync_buckets(Peer, Mode) ->
    Enabled = arweave_sim:peer_sync_kind_enabled(Peer, Mode),
    do_get_sync_buckets(Peer, Mode, Enabled).

do_get_sync_buckets(_Peer, _Mode, false) ->
    {ok, ar_sync_buckets:new()};
%% Coarse byte buckets over the peer's configured intervals. The default
%% resolves to bounded store prefixes so AR_TEST bucket granularity stays at
%% roughly 1000 rows instead of millions.
do_get_sync_buckets(Peer, byte, true) ->
    {ok,
        ar_sync_buckets:from_intervals(
            arweave_sim:peer_sync_intervals(Peer)
        )};
%% Footprint buckets are the byte intervals mapped into footprint-offset
%% space at the network footprint bucket size.
do_get_sync_buckets(Peer, footprint, true) ->
    {ok, build_footprint_sync_buckets(Peer)}.

build_footprint_sync_buckets(Peer) ->
    %% Footprints interleave in byte space, so a byte interval maps to
    %% individual footprint-offset points rather than one contiguous range.
    EmptyBuckets = ar_sync_buckets:new(
        ar_sync_buckets:get_network_footprint_bucket_size()
    ),
    ar_intervals:fold(
        fun add_interval_to_footprint_buckets/2,
        EmptyBuckets,
        arweave_sim:peer_sync_intervals(Peer)
    ).

add_interval_to_footprint_buckets({End, Start}, SyncBuckets) ->
    add_chunk_ends_to_footprint_buckets(
        Start + ?DATA_CHUNK_SIZE, End, SyncBuckets
    ).

add_chunk_ends_to_footprint_buckets(ChunkEnd, End, SyncBuckets) when
    ChunkEnd > End
->
    SyncBuckets;
add_chunk_ends_to_footprint_buckets(ChunkEnd, End, SyncBuckets) ->
    FootprintOffset = arweave_storage:get_footprint_offset(
        arweave_constants:get_chunk_padded_offset(ChunkEnd)
    ),
    SyncBuckets2 = ar_sync_buckets:add(
        FootprintOffset, FootprintOffset - 1, SyncBuckets
    ),
    add_chunk_ends_to_footprint_buckets(
        ChunkEnd + ?DATA_CHUNK_SIZE, End, SyncBuckets2
    ).

get_sync_record(Peer, Start, Limit) ->
    fetch_chunk_intervals(Peer, {byte, Start, none, Limit}).

get_sync_record(Peer, Start, Right, Limit) ->
    fetch_chunk_intervals(Peer, {byte, Start, Right, Limit}).

get_footprints(Peer, Partition, Footprint) ->
    fetch_chunk_intervals(Peer, {footprint, Partition, Footprint}).

%% Chunk intervals: full coverage of the requested window in one
%% page (fewer intervals than the page limit ends pagination).
fetch_chunk_intervals(Peer, Request) ->
    wait_for_chunk_interval_response(Peer),
    do_fetch_chunk_intervals(Peer, Request).

do_fetch_chunk_intervals(Peer, {byte, Start, none, Limit}) ->
    do_fetch_chunk_intervals(
        Peer,
        {byte, Start, Start + Limit * ?DATA_CHUNK_SIZE, Limit}
    );
do_fetch_chunk_intervals(Peer, {byte, Start, Right, _Limit}) ->
    case arweave_sim:peer_sync_kind_enabled(Peer, byte) of
        false ->
            {ok, ar_intervals:new()};
        true ->
            Requested = ar_intervals:from_list([{Right, max(Start - 1, 0)}]),
            {ok,
                ar_intervals:intersection(
                    Requested,
                    arweave_sim:peer_sync_intervals(Peer)
                )}
    end;
%% Per-footprint record. Most performance scenarios model complete advertised
%% footprints; scenarios exercising partial holdings translate the peer's exact
%% byte intervals into footprint-offset space.
do_fetch_chunk_intervals(Peer, {footprint, Partition, Footprint}) ->
    case arweave_sim:peer_sync_kind_enabled(Peer, footprint) of
        false ->
            not_found;
        true ->
            FirstChunkEnd =
                Partition * arweave_constants:partition_size() +
                    (Footprint + 1) * ?DATA_CHUNK_SIZE,
            FirstFootprintOffset = arweave_storage:get_footprint_offset(FirstChunkEnd),
            FootprintSize = arweave_constants:get_sub_chunks_per_replica_2_9_entropy(),
            case arweave_sim:world_value({peer, Peer}) of
                #sim_peer{footprint_coverage = full} ->
                    {ok,
                        ar_intervals:from_list([
                            {
                                FirstFootprintOffset - 1 + FootprintSize,
                                FirstFootprintOffset - 1
                            }
                        ])};
                #sim_peer{footprint_coverage = exact} ->
                    PeerIntervals = arweave_sim:peer_sync_intervals(Peer),
                    {ok,
                        simulated_footprint_intervals(
                            FirstFootprintOffset, FootprintSize, PeerIntervals
                        )}
            end
    end.

simulated_footprint_intervals(FirstOffset, FootprintSize, PeerIntervals) ->
    lists:foldl(
        fun(FootprintOffset, Acc) ->
            ChunkEnd = arweave_storage:get_padded_offset_from_footprint_offset(
                FootprintOffset
            ),
            case ar_intervals:is_inside(PeerIntervals, ChunkEnd) of
                true ->
                    ar_intervals:add(
                        Acc, FootprintOffset, FootprintOffset - 1
                    );
                false ->
                    Acc
            end
        end,
        ar_intervals:new(),
        lists:seq(FirstOffset, FirstOffset + FootprintSize - 1)
    ).

%% @doc Apply the peer's detailed-availability response time.
wait_for_chunk_interval_response(Peer) ->
    arweave_sim:wait_for_chunk_interval_response(Peer).

init_sync_status(_StoreID) ->
    paused.

acquire_lock(_Mode, _StoreID, _Status) ->
    %% The simulation has always disabled device limiting; write bandwidth is
    %% controlled by the world's per-store model instead.
    active.

get_threshold() ->
    arweave_sim:weave_size().

subscribe(Events) ->
    %% The runner explicitly updates peers and weave bounds.
    [ok || _Event <- Events].

log_failed_request(Error, Context) ->
    ar_http_iface_client:log_failed_request(Error, Context).

%% The model counts fetch workers separately from buffered writes.
reserve(_StoreID) ->
    case is_full() of
        true -> full;
        false -> {ok, simulated}
    end.
transfer(CacheRef, _PID) -> {ok, CacheRef}.
release(_CacheRef) -> ok.
mark_cached(_CacheRef) -> ok.
completed(_StoreID) -> undefined.

%% @doc Read the simulated missing intervals in the selected index.
get_intervals(unsynced, Start, End, any_packing, _Record, _StoreID) when
    Start >= End
->
    ar_intervals:new();
get_intervals(unsynced, Start, End, any_packing, {ar_data_sync, byte}, StoreID) ->
    arweave_sim:unsynced_intervals(Start, End, StoreID);
get_intervals(unsynced, Start, End, any_packing, {ar_data_sync, footprint}, StoreID) ->
    ChunkEnd = arweave_storage:get_padded_offset_from_footprint_offset(Start + 1),
    {Partition, Footprint} = arweave_storage:get_footprint_location(ChunkEnd),
    Intervals = arweave_sim:unsynced_footprint_intervals(Partition, Footprint, StoreID),
    ar_intervals:intersection(Intervals, ar_intervals:from_list([{End, Start}])).

get_next_interval(synced, Byte, End, any_packing, {_ID, byte}, StoreID) ->
    arweave_sim:get_next_synced_interval(StoreID, Byte, End).

get_blacklisted_intervals(_Start, _End) ->
    ar_intervals:new().

interval_limit() ->
    ar_chunk_cache:interval_limit().
