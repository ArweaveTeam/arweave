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

-export([get_peers_for_offset/1, pick_peers/2, is_throttled/2,
        get_peer_ranges_for_peers/5,
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

get_peers_for_offset(Offset) ->
    case (ar_sync_sim_world:get(world))#sim_world.discovery_enabled of
        true -> ar_sync_discovery:get_peers_for_offset(Offset);
        false -> ar_sync_sim_world:get_peers_for_offset(Offset)
    end.

pick_peers(Peers, Count) ->
    {Picked, _Rest} = arweave_util:split_at_most(Count, Peers),
    Picked.

is_throttled(Peer, _Path) ->
    #sim_peer{ selection_throttled = IsThrottled } =
        ar_sync_sim_world:get({peer, Peer}),
    IsThrottled.

get_peer_ranges_for_peers(StoreID, Peers, Offset, RangeStart, RangeEnd) ->
    case (ar_sync_sim_world:get(world))#sim_world.discovery_enabled of
        true ->
            ar_sync_discovery:get_peer_ranges_for_peers(
                StoreID, Peers, Offset, RangeStart, RangeEnd);
        false ->
            ar_sync_sim_world:get_peer_ranges_for_peers(
                StoreID, Peers, Offset, RangeStart, RangeEnd)
    end.

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
    ar_timer:monotonic_ms() div ar_sync_scheduler:tick_interval_ms().

%%%===================================================================
%%% ar_sync_discovery's outward dependencies (discovery world only):
%%% the peer's metadata endpoints, modeled like get_chunk_binary — real
%%% worker processes paying a simulated latency on ar_timer's clock.
%%%===================================================================

%% Above every release gate (?GET_FOOTPRINT_SUPPORT_RELEASE = 91).
get_peer_release(Peer) ->
    (ar_sync_sim_world:get({peer, Peer}))#sim_peer.release.

get_peers(current) ->
    %% Direct-availability scenarios keep supervised discovery idle. Discovery
    %% scenarios expose both serving peers and known peers with no sync kinds.
    case (ar_sync_sim_world:get(world))#sim_world.discovery_enabled of
        true -> ar_sync_sim_world:get(peers);
        false -> []
    end.

is_joined() ->
    true.

get_weave_size() ->
    ar_sync_sim_world:weave_size().

get_sync_buckets(Peer, Mode) ->
    case (ar_sync_sim_world:get(world))#sim_world.discovery_enabled of
        true -> ar_timer:sleep(?SIM_METADATA_LATENCY_MS);
        false -> ok
    end,
    do_get_sync_buckets(Peer, Mode).

%% Coarse byte buckets over the peer's configured intervals. Discovery worlds
%% resolve the default to bounded store prefixes so AR_TEST bucket granularity
%% stays at roughly 1000 rows instead of millions.
do_get_sync_buckets(Peer, byte) ->
    case ar_sync_sim_world:peer_sync_kind_enabled(Peer, byte) of
        true ->
            {ok, ar_sync_buckets:from_intervals(
                ar_sync_sim_world:peer_sync_intervals(Peer))};
        false ->
            {ok, ar_sync_buckets:new()}
    end;

%% Footprint buckets are the byte intervals mapped into footprint-offset
%% space at the network footprint bucket size.
do_get_sync_buckets(Peer, footprint) ->
    case ar_sync_sim_world:peer_sync_kind_enabled(Peer, footprint) of
        false ->
            {ok, ar_sync_buckets:new()};
        true ->
            %% Footprints INTERLEAVE in byte space (consecutive chunks
            %% belong to consecutive FOOTPRINTS, not consecutive slots),
            %% so a byte prefix's footprint-offset image is a scatter of
            %% single-slot points, one per chunk — mapping it as one
            %% contiguous interval advertised ~the whole partition and
            %% inserted 1.45M bucket rows (measured in the shakeout).
            FpIntervals = lists:foldl(
                fun({End, Start}, Acc) ->
                    lists:foldl(
                        fun(ChunkEnd, Acc2) ->
                            Fp = ar_footprint_record:get_offset(
                                ar_block:get_chunk_padded_offset(ChunkEnd)),
                            ar_intervals:add(Acc2, Fp, Fp - 1)
                        end,
                        Acc,
                        lists:seq(Start + ?DATA_CHUNK_SIZE, End,
                            ?DATA_CHUNK_SIZE))
                end,
                ar_intervals:new(),
                ar_intervals:to_list(ar_sync_sim_world:peer_sync_intervals(Peer))),
            {ok, ar_sync_buckets:from_intervals(FpIntervals,
                ar_sync_buckets:new(
                    ar_sync_buckets:get_network_footprint_bucket_size()))}
    end.

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

%% @doc Apply the peer's detailed-availability response time when discovery is
%% active. Direct adapter tests bypass the delay.
wait_for_chunk_interval_response(Peer) ->
    case (ar_sync_sim_world:get(world))#sim_world.discovery_enabled of
        false ->
            ok;
        true ->
            #sim_peer{ chunk_interval_latency_ms = LatencyMS } =
                ar_sync_sim_world:get({peer, Peer}),
            ar_timer:sleep(LatencyMS)
    end.

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
        {ok, ByteIntervals} = fetch_chunk_intervals(BytePeer,
            {byte, 0, 3 * Chunk, PageLimit}),
        ?assertEqual(Intervals, ar_intervals:to_list(ByteIntervals)),
        ?assertEqual({ok, ar_intervals:new()}, fetch_chunk_intervals(
            FootprintPeer, {byte, 0, 3 * Chunk, PageLimit})),
        ?assertEqual({ok, ar_sync_buckets:new()},
            get_sync_buckets(FootprintPeer, byte)),
        {ok, FootprintIntervals} = fetch_chunk_intervals(
            FootprintPeer, {footprint, 0, 0}),
        ?assertEqual(
            [{ar_replica_2_9:get_footprint_size(), 0}],
            ar_intervals:to_list(FootprintIntervals))
    after
        ar_sync_sim_world:delete()
    end.
