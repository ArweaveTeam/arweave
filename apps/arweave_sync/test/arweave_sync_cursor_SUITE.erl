-module(arweave_sync_cursor_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("arweave/include/ar.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        next_footprint_offset
    ].

init_per_testcase(_Case, Config) ->
    arweave_sync_test_deps:setup(),
    Config.

end_per_testcase(_Case, _Config) ->
    arweave_sync_test_deps:cleanup().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc The footprint cursor advances one chunk at a time within the first
%% sector of a partition, then jumps to the partition end, never passing End.
next_footprint_offset(_Config) ->
    SectorSize = arweave_lib_constants:get_replica_2_9_entropy_sector_size(),
    {P0Start, P0End} =
        arweave_lib_replica_2_9:get_entropy_partition_range(0),
    Chunk = ?DATA_CHUNK_SIZE,
    ?assertEqual(P0Start + Chunk,
        arweave_sync_cursor:next_footprint_offset(P0Start, P0Start, P0End),
        "simple advance"),
    ?assertEqual(P0Start + 1000,
        arweave_sync_cursor:next_footprint_offset(P0Start, P0Start,
            P0Start + 1000),
        "simple advance, limited by End"),
    ?assertEqual(P0End,
        arweave_sync_cursor:next_footprint_offset(P0Start + SectorSize - 1,
            P0Start, P0End),
        "jump to PartitionEnd"),
    ?assertEqual(P0Start + SectorSize,
        arweave_sync_cursor:next_footprint_offset(P0Start + SectorSize - 1,
            P0Start, P0Start + SectorSize),
        "jump to PartitionEnd, limited by End").
