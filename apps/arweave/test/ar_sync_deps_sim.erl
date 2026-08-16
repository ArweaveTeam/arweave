%%% @doc The simulator's ar_sync_deps implementation: a synthetic network
%%% and disk on ar_timer's simulated clock, backed by ar_sync_sim_world.
%%% ar_sync_sim_runner installs it and configures the typed world; the real
%%% pipeline runs against it unmodified. The peer model lives in
%%% ar_sync_sim_world:get_chunk_binary/2
%%% (latency sleep,
%%% per-second capacity, 429/queue overage, timeouts), the disk model in
%%% store_fetched_chunk/5 (simulated write delay, wedge-aware), the weave in
%%% unsynced_intervals/3.
-module(ar_sync_deps_sim).
-test_category([fast]).
-behaviour(ar_sync_deps).

-export([pick_peers/2, is_throttled/2,
        rate_fetched_data/5, get_chunk_binary/3,
        is_chunk_cache_full/0, chunk_cache_size/0, chunk_cache_size/1,
        chunk_cache_size_limit/0,
        increment_chunk_cache_size/1, is_disk_space_sufficient/1,
        is_footprint_record_initialized/1,
        store_fetched_chunk/5, unsynced_intervals/3,
        unsynced_footprint_intervals/3,
        get_next_not_blacklisted_byte/1, get_next_synced_interval/4,
        get_peer_release/1, get_peers/1, is_joined/0, get_weave_size/0,
        get_sync_buckets/2, fetch_chunk_intervals/2]).

-export([complete_write/3]).

-include_lib("arweave/include/ar_peers.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("ar_sync_sim.hrl").

%% A single shared chunk payload: every proof references the same binary,
%% so serving hundreds of thousands of chunks allocates nothing.
-define(WRITE_LATENCY_MS, 1).

%%%===================================================================
%%% ar_sync_deps callbacks.
%%%===================================================================

pick_peers(Peers, Count) ->
    {Picked, _Rest} = arweave_util:split_at_most(Count, Peers),
    Picked.

is_throttled(Peer, _Path) ->
    #sim_peer{ selection_throttled = IsThrottled } =
        ar_sync_sim_world:get({peer, Peer}),
    IsThrottled.

%% Peer goodput derives from completed fetches reported through the scheduler;
%% ar_peers plays no part.
rate_fetched_data(_Peer, _DataType, _Result, _ElapsedUs, _Bytes) ->
    ok.

get_chunk_binary(Name, Offset, _Packing) ->
    ar_sync_sim_world:get_chunk_binary(Name, Offset).

is_chunk_cache_full() ->
    ar_sync_sim_world:is_chunk_cache_full().

chunk_cache_size() ->
    ar_sync_sim_world:chunk_cache_size().

chunk_cache_size(StoreID) ->
    ar_sync_sim_world:chunk_cache_size(StoreID).

chunk_cache_size_limit() ->
    ar_sync_sim_world:get(cache_limit).

increment_chunk_cache_size(StoreID) ->
    ar_sync_sim_world:increment_chunk_cache_size(StoreID),
    ok.

is_disk_space_sufficient(_StoreID) ->
    true.

is_footprint_record_initialized(_StoreID) ->
    true.

%% The disk model: writes complete through the REAL scheduler transition
%% after a short simulated delay; a wedged store retries until it lifts.
store_fetched_chunk(StoreID, Peer, Byte, _Proof, TaskRef) ->
    ok = ar_sync_sim_world:wait_for_entropy(Peer, Byte),
    {ok, _} = ar_timer:apply_after(?WRITE_LATENCY_MS, ?MODULE,
        complete_write, [StoreID, Byte, TaskRef]),
    ok.

complete_write(StoreID, Byte, TaskRef) ->
    Second = ar_timer:monotonic_ms() div 1000,
    case ar_sync_sim_world:admit_store_write(
            StoreID, tick_index(), Second) of
        exhausted ->
            {ok, _} = ar_timer:apply_after(1000, ?MODULE,
                complete_write, [StoreID, Byte, TaskRef]),
            ok;
        available ->
            ar_sync_sim_world:write_completed(StoreID, Byte),
            ar_sync_scheduler:task_write_completed(TaskRef),
            ok
    end.

unsynced_intervals(Start, End, StoreID) ->
    ar_sync_sim_world:unsynced_intervals(Start, End, StoreID).

unsynced_footprint_intervals(Partition, Footprint, StoreID) ->
    ar_sync_sim_world:unsynced_footprint_intervals(
        Partition, Footprint, StoreID).

get_next_not_blacklisted_byte(Byte) ->
    Byte.

get_next_synced_interval(Byte, End, _ID, StoreID) ->
    ar_sync_sim_world:get_next_synced_interval(StoreID, Byte, End).

%%%===================================================================
%%% The simulated world (configured by ar_sync_sim_tests).
%%%===================================================================

%% The ar_sync_scheduler tick cadence keys the scenario predicates.
tick_index() ->
    ar_sync_sim_world:tick_index().

%%%===================================================================
%%% ar_sync_discovery's outward dependencies: the peer registry and metadata
%%% endpoints, modeled as real workers paying latency on ar_timer's clock.
%%%===================================================================

%% Above every release gate (?GET_FOOTPRINT_SUPPORT_RELEASE = 91).
get_peer_release(Peer) ->
    (ar_sync_sim_world:get({peer, Peer}))#sim_peer.release.

get_peers(current) ->
    ar_sync_sim_world:get(peers).

is_joined() ->
    true.

get_weave_size() ->
    ar_sync_sim_world:weave_size().

get_sync_buckets(Peer, Mode) ->
    ar_timer:sleep(?SIM_METADATA_LATENCY_MS),
    do_get_sync_buckets(Peer, Mode).

do_get_sync_buckets(Peer, Mode) ->
    Enabled = ar_sync_sim_world:peer_sync_kind_enabled(Peer, Mode),
    do_get_sync_buckets(Peer, Mode, Enabled).

do_get_sync_buckets(_Peer, _Mode, false) ->
    {ok, ar_sync_buckets:new()};

%% Coarse byte buckets over the peer's configured intervals. The default
%% resolves to bounded store prefixes so AR_TEST bucket granularity stays at
%% roughly 1000 rows instead of millions.
do_get_sync_buckets(Peer, byte, true) ->
    {ok, ar_sync_buckets:from_intervals(
        ar_sync_sim_world:peer_sync_intervals(Peer))};

%% Footprint buckets are the byte intervals mapped into footprint-offset
%% space at the network footprint bucket size.
do_get_sync_buckets(Peer, footprint, true) ->
    {ok, build_footprint_sync_buckets(Peer)}.

build_footprint_sync_buckets(Peer) ->
    %% Footprints interleave in byte space, so a byte interval maps to
    %% individual footprint-offset points rather than one contiguous range.
    EmptyBuckets = ar_sync_buckets:new(
        ar_sync_buckets:get_network_footprint_bucket_size()),
    ar_intervals:fold(
        fun add_interval_to_footprint_buckets/2,
        EmptyBuckets,
        ar_sync_sim_world:peer_sync_intervals(Peer)).

add_interval_to_footprint_buckets({End, Start}, SyncBuckets) ->
    add_chunk_ends_to_footprint_buckets(
        Start + ?DATA_CHUNK_SIZE, End, SyncBuckets).

add_chunk_ends_to_footprint_buckets(ChunkEnd, End, SyncBuckets)
        when ChunkEnd > End ->
    SyncBuckets;
add_chunk_ends_to_footprint_buckets(ChunkEnd, End, SyncBuckets) ->
    FootprintOffset = ar_footprint_record:get_offset(
        ar_block:get_chunk_padded_offset(ChunkEnd)),
    SyncBuckets2 = ar_sync_buckets:add(
        FootprintOffset, FootprintOffset - 1, SyncBuckets),
    add_chunk_ends_to_footprint_buckets(
        ChunkEnd + ?DATA_CHUNK_SIZE, End, SyncBuckets2).

%% Chunk intervals: full coverage of the requested window in one
%% page (fewer intervals than the page limit ends pagination).
fetch_chunk_intervals(Peer, Request) ->
    wait_for_chunk_interval_response(Peer),
    do_fetch_chunk_intervals(Peer, Request).

do_fetch_chunk_intervals(Peer, {byte, Start, none, Limit}) ->
    do_fetch_chunk_intervals(Peer,
        {byte, Start, Start + Limit * ?DATA_CHUNK_SIZE, Limit});
do_fetch_chunk_intervals(Peer, {byte, Start, Right, _Limit}) ->
    case ar_sync_sim_world:peer_sync_kind_enabled(Peer, byte) of
        false ->
            {ok, ar_intervals:new()};
        true ->
            Requested = ar_intervals:from_list([{Right, max(Start - 1, 0)}]),
            {ok, ar_intervals:intersection(Requested,
                ar_sync_sim_world:peer_sync_intervals(Peer))}
    end;

%% Per-footprint record. Most performance scenarios model complete advertised
%% footprints; scenarios exercising partial holdings translate the peer's exact
%% byte intervals into footprint-offset space.
do_fetch_chunk_intervals(Peer, {footprint, Partition, Footprint}) ->
    case ar_sync_sim_world:peer_sync_kind_enabled(Peer, footprint) of
        false ->
            not_found;
        true ->
            FirstChunkEnd = Partition * ar_block:partition_size()
                + (Footprint + 1) * ?DATA_CHUNK_SIZE,
            FirstFootprintOffset = ar_footprint_record:get_offset(FirstChunkEnd),
            FootprintSize = ar_replica_2_9:get_footprint_size(),
            case ar_sync_sim_world:get({peer, Peer}) of
                #sim_peer{ footprint_coverage = full } ->
                    {ok, ar_intervals:from_list([{
                        FirstFootprintOffset - 1 + FootprintSize,
                        FirstFootprintOffset - 1
                    }])};
                #sim_peer{ footprint_coverage = exact } ->
                    PeerIntervals = ar_sync_sim_world:peer_sync_intervals(Peer),
                    {ok, simulated_footprint_intervals(
                        FirstFootprintOffset, FootprintSize, PeerIntervals)}
            end
    end.

simulated_footprint_intervals(FirstOffset, FootprintSize, PeerIntervals) ->
    lists:foldl(
        fun(FootprintOffset, Acc) ->
            ChunkEnd = ar_footprint_record:
                get_padded_offset_from_footprint_offset(FootprintOffset),
            case ar_intervals:is_inside(PeerIntervals, ChunkEnd) of
                true -> ar_intervals:add(
                    Acc, FootprintOffset, FootprintOffset - 1);
                false -> Acc
            end
        end,
        ar_intervals:new(),
        lists:seq(FirstOffset, FirstOffset + FootprintSize - 1)).

%% @doc Apply the peer's detailed-availability response time.
wait_for_chunk_interval_response(Peer) ->
    ar_sync_sim_world:wait_for_chunk_interval_response(Peer).

%%%===================================================================
%%% Tests.
%%%===================================================================

peer_metadata_uses_peer_configuration_test() ->
    ar_sync_sim_world:create(),
    try
        BytePeer = byte_peer,
        FootprintPeer = footprint_peer,
        Chunk = ?DATA_CHUNK_SIZE,
        Intervals = [{2 * Chunk, 0}],
        ar_sync_sim_world:reset(#sim_world{
            peers = #{
                BytePeer => #sim_peer{
                    sync_availability = {intervals, Intervals} },
                FootprintPeer => #sim_peer{
                    sync_kinds = [footprint],
                    sync_availability = {intervals, Intervals} }
            }
        }),
        %% The page limit spans the complete three-chunk query.
        PageLimit = 3,
        {ok, ByteIntervals} = do_fetch_chunk_intervals(BytePeer,
            {byte, 0, 3 * Chunk, PageLimit}),
        ?assertEqual(Intervals, ar_intervals:to_list(ByteIntervals)),
        ?assertEqual({ok, ar_intervals:new()}, do_fetch_chunk_intervals(
            FootprintPeer, {byte, 0, 3 * Chunk, PageLimit})),
        ?assertEqual({ok, ar_sync_buckets:new()},
            do_get_sync_buckets(FootprintPeer, byte)),
        %% The configured two-chunk interval contributes these two chunk ends.
        FootprintOffsets = [
            ar_footprint_record:get_offset(ChunkEnd)
            || ChunkEnd <- [Chunk, 2 * Chunk]
        ],
        ExpectedFootprintIntervals = ar_intervals:from_list([
            {FootprintOffset, FootprintOffset - 1}
            || FootprintOffset <- FootprintOffsets
        ]),
        ExpectedFootprintBuckets = ar_sync_buckets:from_intervals(
            ExpectedFootprintIntervals,
            ar_sync_buckets:new(
                ar_sync_buckets:get_network_footprint_bucket_size())),
        ?assertEqual({ok, ExpectedFootprintBuckets},
            do_get_sync_buckets(FootprintPeer, footprint)),
        {ok, FootprintIntervals} = do_fetch_chunk_intervals(
            FootprintPeer, {footprint, 0, 0}),
        ?assertEqual(
            [{ar_replica_2_9:get_footprint_size(), 0}],
            ar_intervals:to_list(FootprintIntervals))
    after
        ar_sync_sim_world:delete()
    end.
