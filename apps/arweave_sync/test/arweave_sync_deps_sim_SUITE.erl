-module(arweave_sync_deps_sim_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include_lib("arweave_sim/include/arweave_sim.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() -> [peer_metadata_uses_peer_configuration, empty_storage_intervals].

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Simulated storage matches real storage for empty byte and footprint
%% queries.
empty_storage_intervals(_Config) ->
    %% A footprint at a partition boundary can have identical native bounds.
    lists:foreach(
        fun(Index) ->
            Record = {ar_data_sync, Index},
            ?assertEqual(
                arweave_storage:get_intervals(
                    unsynced, 0, 0, any_packing, Record, test_store
                ),
                arweave_sync_deps_sim:get_intervals(
                    unsynced, 0, 0, any_packing, Record, test_store
                )
            )
        end,
        [byte, footprint]
    ).

%% @doc Simulated HTTP metadata honors peer modes and configured byte or
%% footprint availability.
peer_metadata_uses_peer_configuration(_Config) ->
    arweave_sim:create_world(),
    arweave_sim:start_clock(),
    try
        BytePeer = byte_peer,
        FootprintPeer = footprint_peer,
        Chunk = ?DATA_CHUNK_SIZE,
        Intervals = [{2 * Chunk, 0}],
        %% Metadata-only queries need no chunk-cache capacity; use the runner's
        %% usual one-second ticks and production query-range size.
        arweave_sim:reset_world(
            #sim_world{
                peers = #{
                    BytePeer => #sim_peer{
                        sync_availability = {intervals, Intervals}
                    },
                    FootprintPeer => #sim_peer{
                        sync_kinds = [footprint],
                        sync_availability = {intervals, Intervals}
                    }
                }
            },
            0,
            1000,
            ?QUERY_RANGE_STEP_SIZE
        ),
        %% The page limit spans the complete three-chunk query.
        PageLimit = 3,
        {ok, ByteIntervals} = metadata_request(
            get_sync_record, [BytePeer, 0, 3 * Chunk, PageLimit]
        ),
        ?assertEqual(Intervals, ar_intervals:to_list(ByteIntervals)),
        ?assertEqual(
            {ok, ByteIntervals},
            metadata_request(get_sync_record, [BytePeer, 0, PageLimit])
        ),
        ?assertEqual(
            {ok, ar_sync_buckets:from_intervals(ByteIntervals)},
            metadata_request(get_sync_buckets, [BytePeer])
        ),
        ?assertEqual(
            {ok, ar_intervals:new()},
            metadata_request(
                get_sync_record, [FootprintPeer, 0, 3 * Chunk, PageLimit]
            )
        ),
        ?assertEqual(
            {ok, ar_sync_buckets:new()},
            metadata_request(get_sync_buckets, [FootprintPeer])
        ),
        %% The configured two-chunk interval contributes these two chunk ends.
        FootprintOffsets = [
            arweave_storage:get_footprint_offset(ChunkEnd)
         || ChunkEnd <- [Chunk, 2 * Chunk]
        ],
        ExpectedFootprintIntervals = ar_intervals:from_list([
            {FootprintOffset, FootprintOffset - 1}
         || FootprintOffset <- FootprintOffsets
        ]),
        ExpectedFootprintBuckets = ar_sync_buckets:from_intervals(
            ExpectedFootprintIntervals,
            ar_sync_buckets:new(
                ar_sync_buckets:get_network_footprint_bucket_size()
            )
        ),
        ?assertEqual(
            {ok, ExpectedFootprintBuckets},
            metadata_request(get_footprint_buckets, [FootprintPeer])
        ),
        {ok, FootprintIntervals} = metadata_request(
            get_footprints, [FootprintPeer, 0, 0]
        ),
        ?assertEqual(
            [{arweave_constants:get_sub_chunks_per_replica_2_9_entropy(), 0}],
            ar_intervals:to_list(FootprintIntervals)
        ),
        ?assertEqual(
            not_found,
            metadata_request(get_footprints, [BytePeer, 0, 0])
        )
    after
        arweave_sim:stop_clock(),
        arweave_sim:delete_world()
    end.

%%====================================================================
%% Helpers
%%====================================================================

metadata_request(Function, Args) ->
    Parent = self(),
    Worker = spawn_link(fun() ->
        HTTP = arweave_sync_deps_sim:http(),
        Result = erlang:apply(HTTP, Function, Args),
        Parent ! {self(), Result}
    end),
    try
        case
            ar_test_await:until(simulated_metadata_waiting, fun() ->
                arweave_sim:sleeping() =:= 1
            end)
        of
            ok ->
                arweave_sim:advance(?SIM_METADATA_LATENCY_MS),
                receive
                    {Worker, Result} -> Result
                end;
            Error ->
                Error
        end
    after
        unlink(Worker),
        exit(Worker, kill)
    end.
