-module(ar_footprint_record).

-export([add/3, delete/2, get_offset/1, get_footprint/1, get_footprint_bucket/1, get_intervals/3,
		get_intervals/4, get_unsynced_intervals/3,
        get_intervals_from_footprint_intervals/1]).

-include("ar.hrl").
-include("ar_config.hrl").
-include("ar_consensus.hrl").
-include("ar_data_discovery.hrl").

-include_lib("eunit/include/eunit.hrl").

-moduledoc """
    This module exports functions for maintaining
    a replica 2.9 entropy-aligned record of the synced chunks.
    It differs from the normal record (ar_data_sync) in that it only registers
    the bucket numbers of the synced chunks and records chunks with the same footprint
    next to each other. For example, a record may contain intervals 0-10, 1000-1024,
    1020-2048. This means the node has the first 10 chunks of the first entropy footprint,
    the last 24 chunks of the first entropy footprint and chunks 20-48 of the second
    entropy footprint. These chunks are from the first partition. The offset of the chunks
    from the second partition is shifted by the number of chunks in the replica 2.9
    entropy generated per partition (which is slightly bigger than the number of chunks
    that can fit in the 3.6 TB partition).

    Note that Packing does not have to be replica_2_9. We maintain this record
    for any packing so that it is convenient to serve the data to any client.
""".

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Add a chunk to the footprint record.
-spec add(PaddedOffset :: non_neg_integer(), Packing :: term(), StoreID :: string()) -> ok.
add(PaddedOffset, Packing, StoreID) ->
	Offset = get_offset(PaddedOffset),
	ar_sync_record:add(Offset, Offset - 1, Packing, ar_data_sync_footprints, StoreID).

%% @doc Get the offset of a chunk in the footprint record.
-spec get_offset(PaddedOffset :: non_neg_integer()) -> non_neg_integer().
get_offset(PaddedOffset) ->
	FootprintsPerPartition = get_footprints_per_partition(),
	FootprintSize = get_footprint_size(),
	Partition = ar_replica_2_9:get_entropy_partition(PaddedOffset),
	PartitionStartOffset = Partition * FootprintsPerPartition * FootprintSize,
	SliceIndex = ar_replica_2_9:get_slice_index(PaddedOffset),
	Footprint = get_footprint(PaddedOffset),
	PartitionStartOffset + FootprintSize * Footprint + SliceIndex + 1.

%% @doc Get the chunk's footprint's number, >= 0, < the maximum number of footprints
%% in a partition.
-spec get_footprint(Offset :: non_neg_integer()) -> non_neg_integer().
get_footprint(Offset) ->
	EntropyIndex = ar_replica_2_9:get_entropy_index(Offset, 0),
	EntropyIndex div ?COMPOSITE_PACKING_SUB_CHUNK_COUNT.

%% @doc Get the footprint bucket number of a chunk.
-spec get_footprint_bucket(Offset :: non_neg_integer()) -> non_neg_integer().
get_footprint_bucket(Offset) ->
	get_offset(Offset) div ?NETWORK_FOOTPRINT_BUCKET_SIZE.

%% @doc Get the synced footprint intervals of a chunk.
-spec get_intervals(
	Partition :: non_neg_integer(),
	Footprint :: non_neg_integer(),
	StoreID :: string()
) -> term().
get_intervals(Partition, Footprint, StoreID) ->
	get_intervals(Partition, Footprint, any, StoreID).

%% @doc Get the synced footprint intervals of a chunk.
-spec get_intervals(
	Partition :: non_neg_integer(),
	Footprint :: non_neg_integer(),
	Packing :: term(),
	StoreID :: string()
) -> term().
get_intervals(Partition, Footprint, Packing, StoreID) ->
	FootprintSize = get_footprint_size(),
	FootprintsPerPartition = ?REPLICA_2_9_ENTROPY_COUNT div ?COMPOSITE_PACKING_SUB_CHUNK_COUNT,
	PartitionStartOffset = Partition * FootprintsPerPartition * FootprintSize,
	FootprintStart = PartitionStartOffset + Footprint * FootprintSize,
	collect_intervals(FootprintStart, FootprintStart + FootprintSize, Packing, StoreID).

%% @doc Get the unsynced footprint intervals of a chunk.
-spec get_unsynced_intervals(
	Partition :: non_neg_integer(),
	Footprint :: non_neg_integer(),
	StoreID :: string()
) -> term().
get_unsynced_intervals(Partition, Footprint, StoreID) ->
	FootprintSize = get_footprint_size(),
	FootprintsPerPartition = ?REPLICA_2_9_ENTROPY_COUNT div ?COMPOSITE_PACKING_SUB_CHUNK_COUNT,
	PartitionStartOffset = Partition * FootprintsPerPartition * FootprintSize,
	FootprintStart = PartitionStartOffset + Footprint * FootprintSize,
	collect_unsynced_intervals(FootprintStart, FootprintStart + FootprintSize, StoreID).

%% @doc Delete a chunk from the footprint record.
-spec delete(Offset :: non_neg_integer(), StoreID :: string()) -> ok.
delete(Offset, StoreID) ->
	FootprintOffset = get_offset(Offset),
	ar_sync_record:delete(FootprintOffset, FootprintOffset - 1, ar_data_sync_footprints, StoreID).

%% @doc Convert a list of footprint intervals to a list of intervals.
-spec get_intervals_from_footprint_intervals(FootprintIntervals :: term()) -> term().
get_intervals_from_footprint_intervals(FootprintIntervals) ->
	get_intervals_from_footprint_intervals(ar_intervals:to_list(FootprintIntervals), ar_intervals:new()).

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_footprints_per_partition() ->
	?REPLICA_2_9_ENTROPY_COUNT div ?COMPOSITE_PACKING_SUB_CHUNK_COUNT.

get_footprint_size() ->
	?REPLICA_2_9_ENTROPY_SIZE div ?COMPOSITE_PACKING_SUB_CHUNK_SIZE.

collect_intervals(Start, End, Packing, StoreID) ->
	collect_intervals(Start, End, Packing, StoreID, ar_intervals:new()).

collect_intervals(Start, End, _Packing, _StoreID, Intervals) when Start >= End ->
	Intervals;
collect_intervals(Start, End, Packing, StoreID, Intervals) ->
	Query =
		case Packing of
			any ->
				ar_sync_record:get_next_synced_interval(Start, End,
					ar_data_sync_footprints, StoreID);
			Packing ->
				ar_sync_record:get_next_synced_interval(Start, End,
						Packing, ar_data_sync_footprints, StoreID)
		end,
	case Query of
		not_found ->
			Intervals;
		{End2, Start2} ->
			End3 = min(End2, End),
			collect_intervals(End3, End, Packing, StoreID,
					ar_intervals:add(Intervals, End3, Start2))
	end.

collect_unsynced_intervals(Start, End, StoreID) ->
	collect_unsynced_intervals(Start, End, StoreID, ar_intervals:new()).

collect_unsynced_intervals(Start, End, _StoreID, Intervals) when Start >= End ->
	Intervals;
collect_unsynced_intervals(Start, End, StoreID, Intervals) ->
	Query = ar_sync_record:get_next_unsynced_interval(Start, End, ar_data_sync_footprints, StoreID),
	case Query of
		not_found ->
			Intervals;
		{End2, Start2} ->
			End3 = min(End2, End),
			collect_unsynced_intervals(End3, End, StoreID,
					ar_intervals:add(Intervals, End3, Start2))
	end.

get_intervals_from_footprint_intervals([], Intervals) ->
	Intervals;
get_intervals_from_footprint_intervals([{End, Start} | Rest], Intervals) ->
	Intervals2 = get_intervals_from_footprint_intervals(Start, End, Intervals),
	get_intervals_from_footprint_intervals(Rest, Intervals2).

get_intervals_from_footprint_intervals(Start, End, Intervals) when Start >= End ->
	Intervals;
get_intervals_from_footprint_intervals(Start, End, Intervals) ->
	FootprintSize = get_footprint_size(),
	PartitionSize = get_footprints_per_partition() * FootprintSize,
	Partition = Start div PartitionSize,
	PartitionOffset = Partition * ?PARTITION_SIZE,
	FootprintOffset = (Start rem FootprintSize) * FootprintSize * ?DATA_CHUNK_SIZE,
	InFootprintOffset = (Start div FootprintSize) * ?DATA_CHUNK_SIZE,
	Start2 = PartitionOffset + FootprintOffset + InFootprintOffset,
	Intervals2 = ar_intervals:add(Intervals, Start2 + ?DATA_CHUNK_SIZE, Start2),
	get_intervals_from_footprint_intervals(Start + 1, End, Intervals2).

%%%===================================================================
%%% Tests.
%%%===================================================================

get_intervals_from_footprint_intervals_test() ->
	TestCases =
	[
		{[], [], "Empty"},
		{[{1, 0}], [{?DATA_CHUNK_SIZE, 0}], "One chunk"},
		{[{2, 0}], [{?DATA_CHUNK_SIZE, 0}, {?DATA_CHUNK_SIZE * 4, ?DATA_CHUNK_SIZE * 3}], "Two chunks"},
		{[{1, 0}, {3, 2}], [{?DATA_CHUNK_SIZE, 0}, {?DATA_CHUNK_SIZE * 7, ?DATA_CHUNK_SIZE * 6}], "Two chunks with a hole"},
		{[{10, 0}], [{?DATA_CHUNK_SIZE * 9, 0}, {?DATA_CHUNK_SIZE * 12, ?DATA_CHUNK_SIZE * 11}], "Chunks in two partitions"}
	],
	test_get_intervals_from_footprint_intervals(TestCases).

test_get_intervals_from_footprint_intervals([]) ->
	ok;
test_get_intervals_from_footprint_intervals([{Input, Expected, Title} | Rest]) ->
	?assertEqual(ar_intervals:from_list(Expected),
			get_intervals_from_footprint_intervals(ar_intervals:from_list(Input)), Title),
	test_get_intervals_from_footprint_intervals(Rest).