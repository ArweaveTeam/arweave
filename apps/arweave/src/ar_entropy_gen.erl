-module(ar_entropy_gen).

-behaviour(gen_server).

-export([name/1, register_workers/1,  initialize_context/2, is_entropy_packing/1,
	iteration_end/2, map_entropies/9, entropy_offsets/1,
	generate_entropies/2, generate_entropies/4, generate_entropy_keys/2, 
	reset_entropy_offset/1, shift_entropy_offset/2]).

-export([start_link/2, init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include("../include/ar.hrl").
-include("../include/ar_sup.hrl").
-include("../include/ar_config.hrl").
-include("../include/ar_consensus.hrl").

-include_lib("eunit/include/eunit.hrl").

-record(state, {
	store_id,
	packing,
	range_start,
	range_end,
	cursor,
	prepare_status = undefined
}).

-ifdef(AR_TEST).
-define(DEVICE_LOCK_WAIT, 100).
-else.
-define(DEVICE_LOCK_WAIT, 5_000).
-endif.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, {StoreID, Packing}) ->
	gen_server:start_link({local, Name}, ?MODULE, {StoreID, Packing}, []).

%% @doc Return the name of the server serving the given StoreID.
name(StoreID) ->
	list_to_atom("ar_entropy_gen_" ++ ar_storage_module:label_by_id(StoreID)).

register_workers(Module) ->
	{ok, Config} = application:get_env(arweave, config),
	ConfiguredWorkers = lists:filtermap(
		fun(StorageModule) ->
				StoreID = ar_storage_module:id(StorageModule),
				Packing = ar_storage_module:get_packing(StoreID),

				case is_entropy_packing(Packing) of
					 true ->
						Worker = ?CHILD_WITH_ARGS(
							Module, worker, Module:name(StoreID),
							[Module:name(StoreID), {StoreID, Packing}]),
						{true, Worker};
					 false ->
						false
				end
		end,
		Config#config.storage_modules
	),
	 
	RepackInPlaceWorkers = lists:filtermap(
		fun({StorageModule, Packing}) ->
				StoreID = ar_storage_module:id(StorageModule),
				%% Note: the config validation will prevent a StoreID from being used in both
				%% `storage_modules` and `repack_in_place_storage_modules`, so there's
				%% no risk of a `Name` clash with the workers spawned above.
				case is_entropy_packing(Packing) of
					 true ->
						Worker = ?CHILD_WITH_ARGS(
							Module, worker, Module:name(StoreID),
							[Module:name(StoreID), {StoreID, Packing}]),
						{true, Worker};
					 false ->
						false
				end
		end,
		Config#config.repack_in_place_storage_modules
	),

	ConfiguredWorkers ++ RepackInPlaceWorkers.

-spec initialize_context(ar_storage_module:store_id(), ar_chunk_storage:packing()) ->
	 {IsPrepared :: boolean(), RewardAddr :: none | ar_wallet:address()}.
initialize_context(StoreID, Packing) ->
	case Packing of
		{replica_2_9, Addr} ->
			{RangeStart, RangeEnd} = ar_storage_module:get_range(StoreID),
			Cursor = read_cursor(StoreID, RangeStart),
			case Cursor =< RangeEnd of
				true ->
					{false, Addr};
				false ->
					{true, Addr}
			end;
		_ ->
			{true, none}
	end.

-spec is_entropy_packing(ar_chunk_storage:packing()) -> boolean().
is_entropy_packing(unpacked_padded) ->
	true;
is_entropy_packing({replica_2_9, _}) ->
	true;
is_entropy_packing(_) ->
	false.

iteration_end(BucketEndOffset, RangeEnd) ->
	%% get_entropy_partition will use bucket *start* offset to determine the partition.
	Partition = ar_replica_2_9:get_entropy_partition(BucketEndOffset),
	%% A set of generated entropies covers slighly more than 3.6TB of
	%% chunks, however we only want to use the first 3.6TB
	%% (+ chunk padding) of it.
	PartitionEnd = (Partition + 1) * ?PARTITION_SIZE,
	PaddedPartitionEnd =
		ar_chunk_storage:get_chunk_bucket_end(
			ar_block:get_chunk_padded_offset(PartitionEnd)),
	%% In addition to limiting this iteration to the PaddedPartitionEnd,
	%% we also want to limit it to the current storage module's range.
	%% This allows us to handle both the storage module range as well
	%% as the small overlap region.
	min(PaddedPartitionEnd, RangeEnd).

%% @doc Return a list of all BucketEndOffsets covered by the entropy needed to encipher
%% the chunk at the given offset. The list returned may include offsets that occur before
%% the provided offset. This is expected if Offset does not refer to a sector 0 chunk.
-spec entropy_offsets(Offset :: non_neg_integer()) -> [non_neg_integer()].
entropy_offsets(Offset) ->
	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(Offset),
	BucketEndOffset2 = reset_entropy_offset(BucketEndOffset),
	Partition = ar_replica_2_9:get_entropy_partition(BucketEndOffset),
	PartitionEnd = (Partition + 1) * ?PARTITION_SIZE,
	PaddedPartitionEnd =
		ar_chunk_storage:get_chunk_bucket_end(
			ar_block:get_chunk_padded_offset(PartitionEnd)),
	?LOG_DEBUG([{event, entropy_offsets}, {bucket_end_offset, BucketEndOffset},
		{bucket_end_offset2, BucketEndOffset2}, {partition, Partition},
		{partition_end, PartitionEnd}, {padded_partition_end, PaddedPartitionEnd}]),
	entropy_offsets(BucketEndOffset2, PaddedPartitionEnd).

entropy_offsets(BucketEndOffset, PaddedPartitionEnd)
	when BucketEndOffset > PaddedPartitionEnd ->
	[];
entropy_offsets(BucketEndOffset, PaddedPartitionEnd) ->
	NextOffset = shift_entropy_offset(BucketEndOffset, 1),
	[BucketEndOffset | entropy_offsets(NextOffset, PaddedPartitionEnd)].

%% @doc If we are not at the beginning of the entropy, shift the offset to
%% the left. store_entropy will traverse the entire 2.9 partition shifting
%% the offset by sector size.
reset_entropy_offset(BucketEndOffset) ->
	%% Sanity checks
	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(BucketEndOffset),
	%% End sanity checks
	SliceIndex = ar_replica_2_9:get_slice_index(BucketEndOffset),
	shift_entropy_offset(BucketEndOffset, -SliceIndex).

shift_entropy_offset(Offset, SectorCount) ->
	SectorSize = ar_replica_2_9:get_sector_size(),
	ar_chunk_storage:get_chunk_bucket_end(Offset + SectorSize * SectorCount).

generate_entropies(StoreID, RewardAddr, BucketEndOffset, ReplyTo) ->
	gen_server:cast(name(StoreID), {generate_entropies, RewardAddr, BucketEndOffset, ReplyTo}).

map_entropies(_Entropies,
			[],
			_RangeStart,
			_RangeEnd,
			_Keys,
			_RewardAddr,
			_Fun,
			_Args,
			Acc) ->
	%% The amount of entropy generated per partition is slightly more than the amount needed.
	%% So at the end of a partition we will have finished processing chunks, but still have
	%% some entropy left. In this case we stop the recursion early and wait for the writes
	%% to complete.
	Acc;
map_entropies(_Entropies,
			[BucketEndOffset | _EntropyOffsets],
			_RangeStart,
			RangeEnd,
			_Keys,
			_RewardAddr,
			_Fun,
			_Args,
			Acc)
		when BucketEndOffset > RangeEnd ->
	%% The amount of entropy generated per partition is slightly more than the amount needed.
	%% So at the end of a partition we will have finished processing chunks, but still have
	%% some entropy left. In this case we stop the recursion early and wait for the writes
	%% to complete.
	Acc;
map_entropies(Entropies,
			[BucketEndOffset | EntropyOffsets],
			RangeStart,
			RangeEnd,
			Keys,
			RewardAddr,
			Fun,
			Args,
			Acc) ->
	% ?LOG_DEBUG([{event, map_entropies}, {bucket_end_offset, BucketEndOffset},
	% 	{entropy_offsets, EntropyOffsets}, {range_start, RangeStart},
	% 	{range_end, RangeEnd}, {partition1, ar_replica_2_9:get_entropy_partition(BucketEndOffset)},
	% 	{partition2, ar_replica_2_9:get_entropy_partition(RangeEnd)}]),
	
	case take_and_combine_entropy_slices(Entropies) of
		{ChunkEntropy, Rest} ->
			%% Sanity checks
			true =
				ar_replica_2_9:get_entropy_partition(BucketEndOffset)
				== ar_replica_2_9:get_entropy_partition(RangeEnd),
			sanity_check_replica_2_9_entropy_keys(BucketEndOffset, RewardAddr, Keys),
			%% End sanity checks

			Acc2 = case BucketEndOffset > RangeStart of
				true ->
					erlang:apply(Fun, [ChunkEntropy, BucketEndOffset] ++ Args ++ [Acc]);
				false ->
					%% Don't write entropy before the start of the storage module.
					Acc
			end,

			%% Jump to the next sector covered by this entropy.
			map_entropies(
				Rest,
				EntropyOffsets,
				RangeStart,
				RangeEnd,
				Keys,
				RewardAddr,
				Fun,
				Args,
				Acc2)
	end.


init({StoreID, Packing}) ->
	?LOG_INFO([{event, ar_entropy_storage_init},
		{name, name(StoreID)}, {store_id, StoreID},
		{packing, ar_serialize:encode_packing(Packing, true)}]),

	%% Senity checks
	{replica_2_9, _} = Packing,
	%% End sanity checks

	{RangeStart, RangeEnd} = ar_storage_module:get_range(StoreID),
	PaddedRangeEnd = ar_chunk_storage:get_chunk_bucket_end(RangeEnd),

	%% Provided Packing will only differ from the StoreID packing when this
	%% module is configured to repack in place.
	IsRepackInPlace = Packing /= ar_storage_module:get_packing(StoreID),
	State = case IsRepackInPlace of
		true ->
			#state{};
		false ->
			%% Only kick of the prepare entropy process if we're not repacking in place.
			Cursor = read_cursor(StoreID, RangeStart),
			?LOG_INFO([{event, read_prepare_replica_2_9_cursor}, {store_id, StoreID},
					{cursor, Cursor}, {range_start, RangeStart},
					{range_end, RangeEnd}, {padded_range_end, PaddedRangeEnd}]),
			PrepareStatus = 
				case initialize_context(StoreID, Packing) of
					{_IsPrepared, none} ->
						%% ar_entropy_gen is only used for replica_2_9 packing
						?LOG_ERROR([{event, invalid_packing_for_entropy}, {module, ?MODULE},
							{store_id, StoreID},
							{packing, ar_serialize:encode_packing(Packing, true)}]),
						off;
					{false, _} ->
						gen_server:cast(self(), prepare_entropy),
						paused;
					{true, _} ->
						%% Entropy generation is complete
						complete
				end,
			ar_device_lock:set_device_lock_metric(StoreID, prepare, PrepareStatus),
			#state{
				cursor = Cursor,
				prepare_status = PrepareStatus
			}
	end,

	State2 = State#state{
		store_id = StoreID,
		packing = Packing, 
		range_start = RangeStart,
		range_end = PaddedRangeEnd
	},

	{ok, State2}.

handle_cast(prepare_entropy, State) ->
	#state{ store_id = StoreID } = State,
	NewStatus = ar_device_lock:acquire_lock(prepare, StoreID, State#state.prepare_status),
	State2 = State#state{ prepare_status = NewStatus },
	State3 = case NewStatus of
		active ->
			do_prepare_entropy(State2);
		paused ->
			ar_util:cast_after(?DEVICE_LOCK_WAIT, self(), prepare_entropy),
			State2;
		_ ->
			State2
	end,
	{noreply, State3};

handle_cast({generate_entropies, RewardAddr, BucketEndOffset, ReplyTo}, State) ->
	?LOG_DEBUG([{event, generate_entropies}, {store_id, State#state.store_id},
			{bucket_end_offset, BucketEndOffset}]),
	Entropies = generate_entropies(RewardAddr, BucketEndOffset),
	ReplyTo ! {entropy, BucketEndOffset, Entropies},
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_call(Call, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {call, Call}]),
	{reply, {error, unhandled_call}, State}.

handle_info(Info, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(Reason, State) ->
	?LOG_INFO([{event, terminate},
				{module, ?MODULE},
				{reason, Reason},
				{name, name(State#state.store_id)},
				{store_id, State#state.store_id}]),
	ok.

do_prepare_entropy(State) ->
	#state{ 
		cursor = Start, range_start = RangeStart, range_end = RangeEnd,
		packing = {replica_2_9, RewardAddr},
		store_id = StoreID
	} = State,

	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(Start),

	%% Sanity checks:
	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(BucketEndOffset),
	true = (
		ar_chunk_storage:get_chunk_bucket_start(Start) ==
		ar_chunk_storage:get_chunk_bucket_start(BucketEndOffset)
	),
	true = (
		max(0, BucketEndOffset - ?DATA_CHUNK_SIZE) == 
		ar_chunk_storage:get_chunk_bucket_start(BucketEndOffset)
	),
	%% End of sanity checks.

	%% Make sure all prior entropy writes are complete.
	ar_entropy_storage:is_ready(StoreID),

	CheckRangeEnd =
		case BucketEndOffset > RangeEnd of
			true ->
				ar_device_lock:release_lock(prepare, StoreID),
				?LOG_INFO([{event, storage_module_entropy_preparation_complete},
						{store_id, StoreID}]),
				ar:console("The storage module ~s is prepared for 2.9 replication.~n",
						[StoreID]),
				ar_chunk_storage:set_entropy_complete(StoreID),
				complete;
			false ->
				false
		end,

	Start2 = advance_entropy_offset(BucketEndOffset, StoreID),
	State2 = State#state{ cursor = Start2 },
	CheckIsRecorded =
		case CheckRangeEnd of
			complete ->
				complete;
			false ->
				ar_entropy_storage:is_entropy_recorded(BucketEndOffset, StoreID)
		end,

	StoreEntropy =
		case CheckIsRecorded of
			complete ->
				complete;
			true ->
				is_recorded;
			false ->
				%% Get all the entropies needed to encipher the chunk at BucketEndOffset.
				Entropies = generate_entropies(RewardAddr, BucketEndOffset),
				case Entropies of
					{error, Reason} ->
						{error, Reason};
					_ ->
						EntropyKeys = generate_entropy_keys(RewardAddr, BucketEndOffset),
						EntropyOffsets = entropy_offsets(BucketEndOffset),
						%% Wait for the previous store_entropy to complete. Should only
						%% return 'false' if the entropy storage process is down (e.g. during
						%% shutdown)
						ar_entropy_storage:store_entropy(
							StoreID, Entropies, EntropyOffsets,
							RangeStart, iteration_end(BucketEndOffset, RangeEnd),
							EntropyKeys, RewardAddr)
				end
		end,
	case StoreEntropy of
		complete ->
			ar_device_lock:set_device_lock_metric(StoreID, prepare, complete),
			State#state{ prepare_status = complete };
		is_recorded ->
			gen_server:cast(self(), prepare_entropy),
			State2;
		{error, Error} ->
			?LOG_WARNING([{event, failed_to_store_entropy},
					{cursor, Start},
					{store_id, StoreID},
					{reason, io_lib:format("~p", [Error])}]),
			ar_util:cast_after(500, self(), prepare_entropy),
			State;
		ok ->
			gen_server:cast(self(), prepare_entropy),
			case store_cursor(Start2, StoreID) of
				ok ->
					ok;
				{error, Error} ->
					?LOG_WARNING([{event, failed_to_store_prepare_entropy_cursor},
							{chunk_cursor, Start2},
							{store_id, StoreID},
							{reason, io_lib:format("~p", [Error])}])
			end,
			State2
	end.

%% @doc Returns all the entropies needed to encipher the chunk at PaddedEndOffset.
generate_entropies(RewardAddr, BucketEndOffset) ->
	prometheus_histogram:observe_duration(replica_2_9_entropy_duration_milliseconds, [], 
		fun() ->
			do_generate_entropies(RewardAddr, BucketEndOffset)
		end).

do_generate_entropies(RewardAddr, BucketEndOffset) ->
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	EntropyTasks =
		lists:map(
			fun(Offset) ->
				Ref = make_ref(),
				ar_packing_server:request_entropy_generation(
					Ref, self(), {RewardAddr, BucketEndOffset, Offset}),
				Ref
			end,
			lists:seq(0, ?DATA_CHUNK_SIZE - SubChunkSize, SubChunkSize)),
	Entropies = collect_entropies(EntropyTasks, []),
	case Entropies of
		{error, _Reason} ->
			flush_entropy_messages();
		_ ->
			ok
	end,
	Entropies.

%% @doc Take the first slice of each entropy and combine into a single binary. This binary
%% can be used to encipher a single chunk.
-spec take_and_combine_entropy_slices(Entropies :: [binary()]) ->
										 {ChunkEntropy :: binary(),
										  RemainingSlicesOfEachEntropy :: [binary()]}.
take_and_combine_entropy_slices(Entropies) ->
	true = ?COMPOSITE_PACKING_SUB_CHUNK_COUNT == length(Entropies),
	take_and_combine_entropy_slices(Entropies, [], []).

take_and_combine_entropy_slices([], Acc, RestAcc) ->
	{iolist_to_binary(Acc), lists:reverse(RestAcc)};
take_and_combine_entropy_slices([<<>> | Entropies], _Acc, _RestAcc) ->
	true = lists:all(fun(Entropy) -> Entropy == <<>> end, Entropies),
	{<<>>, []};
take_and_combine_entropy_slices([<<EntropySlice:?COMPOSITE_PACKING_SUB_CHUNK_SIZE/binary,
								   Rest/binary>>
								 | Entropies],
								Acc,
								RestAcc) ->
	take_and_combine_entropy_slices(Entropies, [Acc, EntropySlice], [Rest | RestAcc]).

sanity_check_replica_2_9_entropy_keys(PaddedEndOffset, RewardAddr, Keys) ->
	sanity_check_replica_2_9_entropy_keys(PaddedEndOffset, RewardAddr, 0, Keys).

sanity_check_replica_2_9_entropy_keys(
		_PaddedEndOffset, _RewardAddr, _SubChunkStartOffset, []) ->
	ok;
sanity_check_replica_2_9_entropy_keys(
		PaddedEndOffset, RewardAddr, SubChunkStartOffset, [Key | Keys]) ->
		Key = ar_replica_2_9:get_entropy_key(RewardAddr, PaddedEndOffset, SubChunkStartOffset),
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	sanity_check_replica_2_9_entropy_keys(PaddedEndOffset,
										RewardAddr,
										SubChunkStartOffset + SubChunkSize,
										Keys).

advance_entropy_offset(BucketEndOffset, StoreID) ->
	Interval = ar_sync_record:get_next_unsynced_interval(
		BucketEndOffset, infinity, ar_chunk_storage_replica_2_9_1_entropy, StoreID),
	case Interval of
		not_found ->
			BucketEndOffset + ?DATA_CHUNK_SIZE;
		{_, Start} ->
			Start + ?DATA_CHUNK_SIZE
	end.

generate_entropy_keys(RewardAddr, Offset) ->
	generate_entropy_keys(RewardAddr, Offset, 0).

generate_entropy_keys(_RewardAddr, _Offset, SubChunkStart)
	when SubChunkStart == ?DATA_CHUNK_SIZE ->
	[];
generate_entropy_keys(RewardAddr, Offset, SubChunkStart) ->
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	[ar_replica_2_9:get_entropy_key(RewardAddr, Offset, SubChunkStart)
	 | generate_entropy_keys(RewardAddr, Offset, SubChunkStart + SubChunkSize)].

collect_entropies([], Acc) ->
	lists:reverse(Acc);
collect_entropies([Ref | Rest], Acc) ->
	receive
		{entropy_generated, Ref, {error, Reason}} ->
			?LOG_ERROR([{event, failed_to_generate_replica_2_9_entropy}, {error, Reason}]),
			{error, Reason};
		{entropy_generated, Ref, Entropy} ->
			collect_entropies(Rest, [Entropy | Acc])
	after 60000 ->
		?LOG_ERROR([{event, entropy_generation_timeout}, {ref, Ref}]),
		{error, timeout}
	end.

flush_entropy_messages() ->
	?LOG_INFO([{event, flush_entropy_messages}]),
	receive
		{entropy_generated, _, _} ->
			flush_entropy_messages()
	after 0 ->
		ok
	end.

read_cursor(StoreID, RangeStart) ->
	Filepath = ar_chunk_storage:get_filepath("prepare_replica_2_9_cursor", StoreID),
	Default = RangeStart + 1,
	case file:read_file(Filepath) of
		{ok, Bin} ->
			case catch binary_to_term(Bin) of
				Cursor when is_integer(Cursor) ->
					Cursor;
				_ ->
					Default
			end;
		_ ->
			Default
	end.

store_cursor(Cursor, StoreID) ->
	Filepath = ar_chunk_storage:get_filepath("prepare_replica_2_9_cursor", StoreID),
	file:write_file(Filepath, term_to_binary(Cursor)).

%%%===================================================================
%%% Tests.
%%%===================================================================

reset_entropy_offset_test() ->
	?assertEqual(786432, ar_replica_2_9:get_sector_size()),
	?assertEqual(786432, ?STRICT_DATA_SPLIT_THRESHOLD),
	%% Slice index of 0 means no shift (all offsets at or below the strict data split
	%% threshold are not padded)
	assert_reset_entropy_offset(262144, 0),
	assert_reset_entropy_offset(262144, 1),
	assert_reset_entropy_offset(262144, ?DATA_CHUNK_SIZE - 1),
	assert_reset_entropy_offset(262144, ?DATA_CHUNK_SIZE),
	assert_reset_entropy_offset(262144, ?DATA_CHUNK_SIZE + 1),
	assert_reset_entropy_offset(524288, ?DATA_CHUNK_SIZE * 2),
	assert_reset_entropy_offset(786432, ?DATA_CHUNK_SIZE * 3),
	%% Slice index of 1 shift down a sector
	assert_reset_entropy_offset(262144, ?DATA_CHUNK_SIZE * 3 + 1),
	assert_reset_entropy_offset(262144, ?DATA_CHUNK_SIZE * 4 - 1),
	assert_reset_entropy_offset(262144, ?DATA_CHUNK_SIZE * 4),
	assert_reset_entropy_offset(524288, ?DATA_CHUNK_SIZE * 4 + 1),
	assert_reset_entropy_offset(524288, ?DATA_CHUNK_SIZE * 5 - 1),
	assert_reset_entropy_offset(524288, ?DATA_CHUNK_SIZE * 5),
	assert_reset_entropy_offset(786432, ?DATA_CHUNK_SIZE * 5 + 1),
	assert_reset_entropy_offset(786432, ?DATA_CHUNK_SIZE * 6),
	%% Slice index of 2 shift down 2 sectors
	assert_reset_entropy_offset(262144, ?DATA_CHUNK_SIZE * 7),
	assert_reset_entropy_offset(524288, ?DATA_CHUNK_SIZE * 8),
	%% First chunk of new partition, restart slice index at 0, so no shift
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 9, ?DATA_CHUNK_SIZE * 8 + 1),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 9, ?DATA_CHUNK_SIZE * 9 - 1),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 9, ?DATA_CHUNK_SIZE * 9),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 10, ?DATA_CHUNK_SIZE * 9 + 1),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 10, ?DATA_CHUNK_SIZE * 10),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 11, ?DATA_CHUNK_SIZE * 11),
	%% Slice index of 1 shift down a sector
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 9, ?DATA_CHUNK_SIZE * 12),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 10, ?DATA_CHUNK_SIZE * 13),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 11, ?DATA_CHUNK_SIZE * 14),
	%% Slice index of 2 shift down 2 sectors
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 9, ?DATA_CHUNK_SIZE * 15),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 10, ?DATA_CHUNK_SIZE * 16),
	%% First chunk of new partition, restart slice index at 0, so no shift
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 17, ?DATA_CHUNK_SIZE * 17).

assert_reset_entropy_offset(ExpectedShiftedOffset, Offset) ->
	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(Offset),
	?assertEqual(ExpectedShiftedOffset,
				 reset_entropy_offset(BucketEndOffset),
				 iolist_to_binary(io_lib:format("Offset: ~p, BucketEndOffset: ~p",
												[Offset, BucketEndOffset]))).

entropy_offsets_test() ->
	SectorSize = ar_replica_2_9:get_sector_size(),
	?assertEqual(3 * ?DATA_CHUNK_SIZE, SectorSize),

	%% Any Offset from any of the chunks in the result should yield the same entropy offsets.
	?assertEqual([262144, 1048576, 1835008], entropy_offsets(0)),
	?assertEqual([262144, 1048576, 1835008], entropy_offsets(1000)),
	?assertEqual([262144, 1048576, 1835008], entropy_offsets(262144)),
	?assertEqual([262144, 1048576, 1835008], entropy_offsets(786433)),
	?assertEqual([262144, 1048576, 1835008], entropy_offsets(1048576)),
	?assertEqual([262144, 1048576, 1835008], entropy_offsets(1835008)),
	?assertEqual([262144, 1048576, 1835008], entropy_offsets(1835007)),

	?assertEqual([524288, 1310720, 2097152], entropy_offsets(524288)),
	?assertEqual([524288, 1310720, 2097152], entropy_offsets(2097152)),
	?assertEqual([786432, 1572864], entropy_offsets(786432)),

	?assertEqual([2359296, 3145728, 3932160], entropy_offsets(2097153)),
	?assertEqual([2621440, 3407872, 4194304], entropy_offsets(2359297)),
	?assertEqual([2883584,3670016], entropy_offsets(2621441)).
