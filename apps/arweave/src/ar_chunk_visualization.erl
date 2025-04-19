-module(ar_chunk_visualization).

-export([get_chunk_packings/3, get_chunk_packings/4, generate_bitmap/1, bitmap_to_binary/1, print_chunk_stats/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Build a list of lists where each inner list represents a sector worth of packing
%% formats. Each sector row will form a row in the bitmap.
get_chunk_packings(ModuleStart, ModuleEnd, StoreID) ->
	get_chunk_packings(ModuleStart, ModuleEnd, StoreID, false).
get_chunk_packings(ModuleStart, ModuleEnd, StoreID, PrintProgress) ->
	StartOffset = ar_block:get_chunk_padded_offset(ModuleStart),
	Partition = StartOffset div ar_block:partition_size(),
	PartitionStart = Partition * ar_block:partition_size(),
	SectorSize = ar_replica_2_9:get_sector_size(),
	BucketsPerSector = SectorSize div ?DATA_CHUNK_SIZE,
	NumSectors = ?REPLICA_2_9_ENTROPY_COUNT * ?REPLICA_2_9_ENTROPY_SIZE div SectorSize,

	lists:map(
		fun(SectorIndex) ->
			SectorStart = PartitionStart + SectorIndex * SectorSize,
			SectorEnd = SectorStart + SectorSize - 1,
			case PrintProgress of
				true ->
					ar:console("Partition ~p sector ~4B. Offsets ~p to ~p~n", [
						Partition, SectorIndex, SectorStart, SectorEnd]);
				false ->
					ok
			end,
			{ok, MetadataRange} = ar_data_sync:get_chunk_metadata_range(SectorStart, SectorEnd, StoreID),
			
			% Initialize map with all bucket end offsets set to 'missing'
			BucketMap = lists:foldl(
				fun(J, Acc) ->
					BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(SectorStart + J * ?DATA_CHUNK_SIZE),
					case BucketEndOffset < ModuleStart orelse BucketEndOffset > ModuleEnd of
						true -> Acc;
						false -> maps:put(BucketEndOffset, missing, Acc)
					end
				end,
				#{},
				lists:seq(0, BucketsPerSector - 1)),

			% Process metadata to update the map
			UpdatedMap = maps:fold(
				fun(AbsoluteEndOffset, Metadata, Acc) ->
					BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(AbsoluteEndOffset),
					case maps:is_key(BucketEndOffset, Acc) of
						true ->
							IsRecorded = ar_sync_record:is_recorded(AbsoluteEndOffset, ar_data_sync, StoreID),
							maps:put(BucketEndOffset,
								normalize_sync_record(IsRecorded, AbsoluteEndOffset, Metadata),
								Acc);
						false ->
							Acc
					end
				end,
				BucketMap,
				MetadataRange),

			% Convert map to list in order of bucket end offsets
			lists:map(
				fun(J) ->
					BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(SectorStart + J * ?DATA_CHUNK_SIZE),
					case BucketEndOffset < ModuleStart orelse BucketEndOffset > ModuleEnd of
						true -> none;
						false -> maps:get(BucketEndOffset, UpdatedMap)
					end
				end,
				lists:seq(0, BucketsPerSector - 1))
		end,
		lists:seq(0, NumSectors - 1)).
	
%% @doc Convert packing formats to RGB pixels.
generate_bitmap(PackingRows) ->
	lists:map(
		fun(Row) ->
			lists:map(fun packing_color/1, Row)
		end,
		PackingRows).

%% @doc Convert a bitmap (list of rows; each row a list of {R, G, B} tuples)
%% into a binary PPM image.
bitmap_to_binary(BitmapRows) ->
	Height = length(BitmapRows),
	Width =
		case BitmapRows of
			[Row | _] ->
				length(Row);
			[] ->
				0
		end,
	Header = io_lib:format("P6\n~w ~w\n255\n", [Width, Height]),
	%% Build pixel binary data (each pixel is 3 bytes: R,G,B)
	PixelData = [<<R:8, G:8, B:8>> || Row <- BitmapRows, {R, G, B} <- Row],
	list_to_binary([Header, PixelData]).

print_chunk_stats(ChunkPackings) ->
	Counts = chunk_statistics(ChunkPackings),
	Total = maps:fold(fun(_Format, Count, Acc) -> Count + Acc end, 0, Counts),
	ar:console("Total chunks: ~p~n", [Total]),
	ar:console("Chunk counts by packing format:~n"),
	lists:foreach(
		fun({Packing, Count}) ->
			Percentage =
				case Total of
					0 -> 0.0;
					_ -> Count * 100 / Total
				end,
			ar:console("~p (~p): ~p chunks (~.2f%)~n",
				[ar_serialize:encode_packing(Packing, false),
					packing_color(Packing),
					Count,
					Percentage])
		end,
		lists:sort(
			maps:to_list(Counts))).

%%%===================================================================
%%% Private functions.
%%%===================================================================

normalize_sync_record(false, _, _) ->
	missing;
normalize_sync_record(_, _, not_found) ->
	error;
normalize_sync_record({true, Packing}, PaddedEndOffset, Metadata) ->
	{_, _, _, _, _, ChunkSize} = Metadata,
	case ar_chunk_storage:is_storage_supported(PaddedEndOffset, ChunkSize, Packing) of
		true ->
			Packing;
		false ->
			too_small
	end;
normalize_sync_record(_, _, _) ->
	error.

%% @doc Returns a unique color (as an {R,G,B} tuple) for each recognized packing format.
packing_color(missing) ->
	{0, 0, 0};
packing_color(error) ->
	{255, 0, 0};
packing_color(too_small) ->
	{255, 0, 255};
packing_color(unpacked) ->
	{255, 255, 255};
packing_color(unpacked_padded) ->
	{128, 128, 128};
packing_color(none) ->
	{0, 255, 255};
packing_color({Format, Addr, _PackingDifficulty}) ->
	packing_color({Format, Addr});
packing_color({Format, Addr}) ->
	BaseColor = packing_color(Format),
	%% Compute a hash from Addr and extract offsets
	Hash = erlang:phash2(Addr, 16777216),
	Roffset = Hash band 255,
	Goffset = (Hash bsr 8) band 255,
	Boffset = (Hash bsr 16) band 255,
	{(element(1, BaseColor) + Roffset) rem 256,
	 (element(2, BaseColor) + Goffset) rem 256,
	 (element(3, BaseColor) + Boffset) rem 256};
%% Base colors for known packing formats
packing_color(replica_2_9) ->
	{0, 0, 255}; %% blue
packing_color(spora_2_6) ->
	{0, 255, 0}; %% green
packing_color(composite) ->
	{255, 255, 0}; %% yellow
packing_color(_) ->
	{255, 0, 0}. %% red for unknown packings

chunk_statistics(ChunkPackings) ->
	lists:foldl(
		fun(Row, AccCounts) ->
			lists:foldl(
				fun(Packing, RowAccCounts) ->
					maps:update_with(Packing, fun(N) -> N + 1 end, 1, RowAccCounts)
				end,
				AccCounts,
				Row)
		end,
		#{},
		ChunkPackings).
