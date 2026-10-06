-module(arweave_storage_footprint_record_SUITE).
-test_category([fast]).
-export([all/0, get_offset/1, get_padded_offset_from_footprint_offset/1, get_offset_get_intervals_from_footprint_intervals_reversal/1, get_unsynced_intervals/1, get_intervals/1, get_offset_get_padded_offset_from_footprint_offset_reversal/1, get_intervals_from_footprint_intervals/1, footprint_geometry/1, next_sector_start_clamps_to_partition/1]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
all() -> [get_offset, get_padded_offset_from_footprint_offset, get_offset_get_intervals_from_footprint_intervals_reversal, get_unsynced_intervals, get_intervals, get_offset_get_padded_offset_from_footprint_offset_reversal, get_intervals_from_footprint_intervals, footprint_geometry, next_sector_start_clamps_to_partition].



get_offset_test() ->
    %% The first chunk of the first footprint.
    ?assertEqual(1, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE)),
    %% The first chunk of the second footprint.
    ?assertEqual(5, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 2)),
    %% The second chunk of the first footprint.
    ?assertEqual(2, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 3)),
    %% The second chunk of the second footprint.
    ?assertEqual(6, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 4)),
    %% The third chunk of the first footprint.
    ?assertEqual(3, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 5)),
    %% The third chunk of the second footprint.
    ?assertEqual(7, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 6)),
    %% The fourth chunk of the first footprint.
    ?assertEqual(4, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 7)),
    %% The fourth chunk of the second footprint.
    ?assertEqual(8, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 8)),
    %% The first chunk of the first footprint of the second partition.
    ?assertEqual(9, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 9)),
    %% The first chunk of the second footprint of the second partition.
    ?assertEqual(13, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 10)),
    %% The second chunk of the first footprint of the second partition.
    ?assertEqual(10, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 11)),
    %% The second chunk of the second footprint of the second partition.
    ?assertEqual(14, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 12)),
    %% The third chunk of the first footprint of the second partition.
    ?assertEqual(11, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 13)),
    %% The third chunk of the second footprint of the second partition.
    ?assertEqual(15, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 14)),
    %% The fourth chunk of the first footprint of the second partition.
    ?assertEqual(12, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 15)),
    %% The fourth chunk of the second footprint of the second partition.
    ?assertEqual(16, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 16)),
    %% The first chunk of the first footprint of the third partition.
    ?assertEqual(17, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 17)),
    %% The first chunk of the second footprint of the third partition.
    ?assertEqual(21, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 18)),
    %% The second chunk of the first footprint of the third partition.
    ?assertEqual(18, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 19)).

get_offset(_Config) ->
    get_offset_test().



get_padded_offset_from_footprint_offset_test() ->
    ?assertEqual(262144, arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(1)),
    ?assertEqual(786432, arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(2)),
    ?assertEqual(1310720, arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(3)),
    ?assertEqual(1835008, arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(4)),
    ?assertEqual(524288, arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(5)),
    ?assertEqual(1048576, arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(6)),
    ?assertEqual(1572864, arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(7)),
    ?assertEqual(2097152, arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(8)),
    ?assertEqual(2359296, arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(9)),
    ?assertEqual(2883584, arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(10)),

    79280870522880 = arweave_lib_constants:get_chunk_padded_offset(79280870522880),
    ?assertEqual(317123481, arweave_storage_footprint_record:get_offset(79280870522880)),
    ?assertEqual(79280870522880, arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(317123481)).

get_padded_offset_from_footprint_offset(_Config) ->
    get_padded_offset_from_footprint_offset_test().



get_offset_get_intervals_from_footprint_intervals_reversal_test() ->
    Offsets = [?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE * 2, ?DATA_CHUNK_SIZE * 3, ?DATA_CHUNK_SIZE * 4,
               ?DATA_CHUNK_SIZE * 8, ?DATA_CHUNK_SIZE * 9],
    [arweave_storage_footprint_record:get_offset_get_intervals_from_footprint_intervals_reversal(Offset) || Offset <- Offsets].

get_offset_get_intervals_from_footprint_intervals_reversal(_Config) ->
    get_offset_get_intervals_from_footprint_intervals_reversal_test().



get_unsynced_intervals_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
      [{arweave_storage_module, get_by_id, fun(test_unsynced_store) -> test_unsynced_store end}],
      fun() ->
              %% Set up a test sync record server.
              TestStoreID = test_unsynced_store,
              TestProcessName = list_to_atom("ar_sync_record_" ++ atom_to_list(TestStoreID)),

              %% Initialize sync_records ETS table if it does not exist.
              case ets:info(sync_records) of
                  undefined ->
                      ets:new(sync_records, [named_table, public, {read_concurrency, true}]);
                  _ ->
                      %% Clear existing data from previous tests.
                      ets:delete_all_objects(sync_records)
              end,

              %% Start the sync record process.
              case whereis(TestProcessName) of
                  undefined ->
                      {ok, _Pid} = arweave_storage_sync_record:start_link(TestProcessName, TestStoreID);
                  _ ->
                      ok
              end,

              Partition = 0,
              Footprint = 0,

              %% Get unsynced intervals before adding any data.
              UnsyncedBefore = arweave_storage_footprint_record:get_unsynced_intervals(Partition, Footprint, TestStoreID),
              UnsyncedBeforeList = arweave_lib_intervals:to_list(UnsyncedBefore),
              ?assertEqual([{4, 0}], UnsyncedBeforeList),

              %% Add some data to the footprint.
              %% This should map to partition 0, footprint 0.
              PaddedOffset = ?DATA_CHUNK_SIZE,
              Packing = unpacked,
              ok = arweave_storage_footprint_record:add(PaddedOffset, Packing, TestStoreID),

              UnsyncedAfter = arweave_storage_footprint_record:get_unsynced_intervals(Partition, Footprint, TestStoreID),
              UnsyncedAfterList = arweave_lib_intervals:to_list(UnsyncedAfter),
              ?assertEqual([{4, 1}], UnsyncedAfterList)
      end).

get_unsynced_intervals(_Config) ->
    get_unsynced_intervals_test_().



get_intervals_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
      [{arweave_storage_module, get_by_id, fun(test_intervals_store) -> test_intervals_store end}],
      fun() ->
              %% Set up a test sync record server.
              TestStoreID = test_intervals_store,
              TestProcessName = list_to_atom("ar_sync_record_" ++ atom_to_list(TestStoreID)),

              %% Initialize sync_records ETS table if it does not exist.
              case ets:info(sync_records) of
                  undefined ->
                      ets:new(sync_records, [named_table, public, {read_concurrency, true}]);
                  _ ->
                      %% Clear existing data from previous tests.
                      ets:delete_all_objects(sync_records)
              end,

              %% Start the sync record process.
              case whereis(TestProcessName) of
                  undefined ->
                      {ok, _Pid} = arweave_storage_sync_record:start_link(TestProcessName, TestStoreID);
                  _ ->
                      ok
              end,

              Packing = unpacked,
              arweave_storage_sync_record:add(32, 0,
                                 Packing, ar_data_sync_footprints, TestStoreID),

              Partition = 0,
              Footprint = 0,
              SyncedIntervals = arweave_storage_footprint_record:get_intervals(Partition, Footprint, TestStoreID),
              SyncedIntervalsList = arweave_lib_intervals:to_list(SyncedIntervals),
              ?assertEqual([{4, 0}], SyncedIntervalsList),
              Partition2 = 0,
              Footprint2 = 1,
              SyncedIntervals2 = arweave_storage_footprint_record:get_intervals(Partition2, Footprint2, TestStoreID),
              SyncedIntervalsList2 = arweave_lib_intervals:to_list(SyncedIntervals2),
              ?assertEqual([{8, 4}], SyncedIntervalsList2),
              Partition3 = 0,
              Footprint3 = 2,
              SyncedIntervals3 = arweave_storage_footprint_record:get_intervals(Partition3, Footprint3, TestStoreID),
              SyncedIntervalsList3 = arweave_lib_intervals:to_list(SyncedIntervals3),
              ?assertEqual([], SyncedIntervalsList3),
              Partition4 = 1,
              Footprint4 = 0,
              SyncedIntervals4 = arweave_storage_footprint_record:get_intervals(Partition4, Footprint4, TestStoreID),
              SyncedIntervalsList4 = arweave_lib_intervals:to_list(SyncedIntervals4),
              ?assertEqual([{12, 8}], SyncedIntervalsList4),
              Partition5 = 1,
              Footprint5 = 1,
              SyncedIntervals5 = arweave_storage_footprint_record:get_intervals(Partition5, Footprint5, TestStoreID),
              SyncedIntervalsList5 = arweave_lib_intervals:to_list(SyncedIntervals5),
              ?assertEqual([{16, 12}], SyncedIntervalsList5),
              Partition6 = 1,
              Footprint6 = 2,
              SyncedIntervals6 = arweave_storage_footprint_record:get_intervals(Partition6, Footprint6, TestStoreID),
              SyncedIntervalsList6 = arweave_lib_intervals:to_list(SyncedIntervals6),
              ?assertEqual([], SyncedIntervalsList6),
              Partition7 = 2,
              Footprint7 = 0,
              SyncedIntervals7 = arweave_storage_footprint_record:get_intervals(Partition7, Footprint7, TestStoreID),
              SyncedIntervalsList7 = arweave_lib_intervals:to_list(SyncedIntervals7),
              ?assertEqual([{20, 16}], SyncedIntervalsList7)
      end).

get_intervals(_Config) ->
    get_intervals_test_().



get_offset_get_padded_offset_from_footprint_offset_reversal_test() ->
    Offsets = [
               ?DATA_CHUNK_SIZE,
               ?DATA_CHUNK_SIZE * 2,
               ?DATA_CHUNK_SIZE * 3,
               ?PARTITION_SIZE,
               ?DATA_CHUNK_SIZE * 8,
               ?DATA_CHUNK_SIZE * 9,
               ?PARTITION_SIZE * 2,
               ?PARTITION_SIZE * 3,
               ?PARTITION_SIZE * 4,
               ?PARTITION_SIZE * 5,
               ?PARTITION_SIZE * 6,
               ?PARTITION_SIZE * 7,
               ?PARTITION_SIZE * 8,
               ?PARTITION_SIZE * 9,
               ?PARTITION_SIZE * 10,
               ?PARTITION_SIZE * 11,
               ?PARTITION_SIZE * 12,
               ?PARTITION_SIZE * 13,
               ?PARTITION_SIZE * 6249,
               ?PARTITION_SIZE * 6250,
               ?PARTITION_SIZE * 6249 + ?DATA_CHUNK_SIZE,
               ?PARTITION_SIZE * 6250 + ?DATA_CHUNK_SIZE,
               ?PARTITION_SIZE * 6249 + ?DATA_CHUNK_SIZE * 2,
               ?PARTITION_SIZE * 6250 + ?DATA_CHUNK_SIZE * 2,
               ?PARTITION_SIZE * 6249 + ?DATA_CHUNK_SIZE * 8,
               ?PARTITION_SIZE * 6250 + ?DATA_CHUNK_SIZE * 8,
               ?PARTITION_SIZE * 6249 + ?DATA_CHUNK_SIZE * 9
              ],
    [arweave_storage_footprint_record:get_offset_get_padded_offset_from_footprint_offset_reversal(F) || F <- Offsets],
    ok.

get_offset_get_padded_offset_from_footprint_offset_reversal(_Config) ->
    get_offset_get_padded_offset_from_footprint_offset_reversal_test().



get_intervals_from_footprint_intervals_test() ->
    TestCases =
        [
         {[], [], "Empty"},
         {[{1, 0}], [{?DATA_CHUNK_SIZE, 0}], "One chunk"},
         {[{2, 0}], [
                     {?DATA_CHUNK_SIZE, 0}, {?DATA_CHUNK_SIZE * 3, ?DATA_CHUNK_SIZE * 2}], "Two chunks"},
         {[{4, 0}], [
                     {?DATA_CHUNK_SIZE, 0},
                     {?DATA_CHUNK_SIZE * 3, ?DATA_CHUNK_SIZE * 2},
                     {?DATA_CHUNK_SIZE * 5, ?DATA_CHUNK_SIZE * 4},
                     {?DATA_CHUNK_SIZE * 7, ?DATA_CHUNK_SIZE * 6}], "Full footprint"},
         {[{5, 0}], [
                     {?DATA_CHUNK_SIZE * 5, ?DATA_CHUNK_SIZE * 4},
                     {?DATA_CHUNK_SIZE * 3, 0},
                     {?DATA_CHUNK_SIZE * 7, ?DATA_CHUNK_SIZE * 6}], "Footprint wraparound"},
         {[{6, 3}], [
                     {?DATA_CHUNK_SIZE * 7, ?DATA_CHUNK_SIZE * 6},
                     {?DATA_CHUNK_SIZE * 2, ?DATA_CHUNK_SIZE * 1},
                     {?DATA_CHUNK_SIZE * 4, ?DATA_CHUNK_SIZE * 3}], "Bits of two footprints"},
         {[{1, 0}, {3, 2}], [
                             {?DATA_CHUNK_SIZE, 0},
                             {?DATA_CHUNK_SIZE * 5,
                              ?DATA_CHUNK_SIZE * 4}], "Two chunks with a hole"},
         {[{8, 0}], [
                     {?DATA_CHUNK_SIZE * 8, 0}], "Completely covered partition"},
         {[{9, 0}], [
                     {?DATA_CHUNK_SIZE * 9, 0}], "Completely covered partition plus one chunk"},
         {[{9, 0}, {13, 12}], [
                               {?DATA_CHUNK_SIZE * 10, 0}], "Completely covered partition plus two chunks"},
         {[{9, 0}, {13, 12}, {15, 14}], [
                                         {?DATA_CHUNK_SIZE * 10, 0},
                                         {?DATA_CHUNK_SIZE * 14, ?DATA_CHUNK_SIZE * 13}],
          "Completely covered partition plus three chunks"}
        ],
    test_get_intervals_from_footprint_intervals(TestCases).

get_intervals_from_footprint_intervals(_Config) ->
    get_intervals_from_footprint_intervals_test().



test_get_intervals_from_footprint_intervals([]) ->
    ok;
test_get_intervals_from_footprint_intervals([{Input, Expected, Title} | Rest]) ->
    ?assertEqual(arweave_lib_intervals:from_list(Expected),
                 arweave_storage_footprint_record:get_intervals_from_footprint_intervals(arweave_lib_intervals:from_list(Input)), Title),
    test_get_intervals_from_footprint_intervals(Rest).


footprint_geometry_test() ->
    %% Test geometry: 512 KiB sectors, 2 chunks per sector, 4 sectors per
    %% partition, partitions of 2,000,000 bytes.
    ?assertEqual(0, arweave_storage_footprint_record:get_footprint(262144)),
    ?assertEqual(1, arweave_storage_footprint_record:get_footprint(524288)),
    ?assertEqual(0, arweave_storage_footprint_record:get_footprint(786432)),
    ?assertEqual(0, arweave_storage_footprint_record:get_footprint(1835008)),
    ?assertEqual(1, arweave_storage_footprint_record:get_footprint(2097152)),
    %% Partition 1 starts at 2,000,000, so its buckets sit 97,152 bytes
    %% into each sector.
    ?assertEqual(0, arweave_storage_footprint_record:get_footprint(2359296)),
    ?assertEqual(1, arweave_storage_footprint_record:get_footprint(2621440)),
    ?assertEqual(786432, arweave_storage_footprint_record:get_next_sector_start(262144)),
    ?assertEqual(786432, arweave_storage_footprint_record:get_next_sector_start(524288)),
    ?assertEqual(1310720, arweave_storage_footprint_record:get_next_sector_start(786432)),
    %% Sector 3 of partition 0 is followed by the first bucket of
    %% partition 1.
    ?assertEqual(2359296, arweave_storage_footprint_record:get_next_sector_start(1835008)),
    ?assertEqual(2883584, arweave_storage_footprint_record:get_next_sector_start(2359296)).

footprint_geometry(_Config) ->
    footprint_geometry_test().



%% With 1,200,000-byte partitions the four 512 KiB sectors overhang the
%% next partition by more than a bucket, as on mainnet; the sector after
%% the last one must start at the next partition's first bucket.
next_sector_start_clamps_to_partition_test_() ->
    ar_test_util:with_mocked(
        [{arweave_lib_constants, partition_size, fun() -> 1200000 end}],
        fun() ->
            %% Bucket [1048576, 1310720) is sector 2 of partition 0; sector
            %% 3 would start at 1572864, past the partition end 1200000.
            ?assertEqual(1572864, arweave_storage_footprint_record:get_next_sector_start(1310720)),
            ?assertEqual(0, arweave_storage_footprint_record:get_footprint(1572864))
        end, 30).

next_sector_start_clamps_to_partition(_Config) ->
    next_sector_start_clamps_to_partition_test_().
