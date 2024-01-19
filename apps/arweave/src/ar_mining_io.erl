-module(ar_mining_io).

-behaviour(gen_server).

-export([start_link/0, set_largest_seen_upper_bound/1, get_partitions/0, get_partitions/1,
			read_recall_range/4]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	partition_upper_bound = 0,
	io_threads = #{},
	io_thread_monitor_refs = #{}
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the gen_server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

set_largest_seen_upper_bound(PartitionUpperBound) ->
	gen_server:call(?MODULE, {set_largest_seen_upper_bound, PartitionUpperBound}).

get_partitions() ->
	gen_server:call(?MODULE, get_partitions).

read_recall_range(WhichChunk, Worker, Candidate, RecallRangeStart) ->
	gen_server:call(?MODULE, {read_recall_range, WhichChunk, Worker, Candidate, RecallRangeStart}).

get_partitions(PartitionUpperBound) when PartitionUpperBound =< 0 ->
	[];
get_partitions(PartitionUpperBound) ->
	Max = ar_node:get_max_partition_number(PartitionUpperBound),
	lists:sort(sets:to_list(
		lists:foldl(
			fun({Partition, MiningAddress, _StoreID}, Acc) ->
				case Partition > Max of
					true ->
						Acc;
					_ ->
						sets:add_element({Partition, MiningAddress}, Acc)
				end
			end,
			sets:new(), %% Ensure only one entry per partition (i.e. collapse storage modules)
			get_io_channels()
		))
	).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	State =
		lists:foldl(
			fun	({PartitionNumber, MiningAddress, StoreID}, Acc) ->
				start_io_thread(PartitionNumber, MiningAddress, StoreID, Acc)
			end,
			#state{},
			get_io_channels()
		),
	{ok, State}.


handle_call({set_largest_seen_upper_bound, PartitionUpperBound}, _From, State) ->
	#state{ partition_upper_bound = CurrentUpperBound } = State,
	case PartitionUpperBound > CurrentUpperBound of
		true ->
			{reply, true, State#state{ partition_upper_bound = PartitionUpperBound }};
		false ->
			{reply, false, State}
	end;

handle_call(get_partitions, _From, #state{ partition_upper_bound = PartitionUpperBound } = State) ->
	{reply, get_partitions(PartitionUpperBound), State};

handle_call({read_recall_range, WhichChunk, Worker, Candidate, RecallRangeStart}, _From,
		#state{ io_threads = IOThreads } = State) ->
	#mining_candidate{ mining_address = MiningAddress } = Candidate,
	PartitionNumber = ar_node:get_partition_number(RecallRangeStart),
	RangeEnd = RecallRangeStart + ?RECALL_RANGE_SIZE,
	case find_thread(PartitionNumber, MiningAddress, RangeEnd, RecallRangeStart, IOThreads) of
		not_found ->
			{reply, false, State};
		Thread ->
			Thread ! {WhichChunk, {Worker, Candidate, RecallRangeStart}},
			{reply, true, State}
	end;

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({'DOWN', Ref, process, _, Reason},
		#state{ io_thread_monitor_refs = IOThreadRefs } = State) ->
	case maps:is_key(Ref, IOThreadRefs) of
		true ->
			{noreply, handle_io_thread_down(Ref, Reason, State)};
		_ ->
			{noreply, State}
	end;

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================
%% @doc Returns tuples {PartitionNumber, MiningAddress, StoreID) covering all defined storage
%% modules (including the "default" storage module). The assumption is that each IO channel
%% represents a distinct 200MiB/s read channel to which we will (later) assign an IO thread.
get_io_channels() ->
	{ok, Config} = application:get_env(arweave, config),
	MiningAddress = Config#config.mining_addr,

	%% First get the start/end ranges for all storage modules configured for the mining address.
	StorageModules =
		lists:foldl(
			fun	({BucketSize, Bucket, {spora_2_6, Addr}} = M, Acc) when Addr == MiningAddress ->
					Start = Bucket * BucketSize,
					End = (Bucket + 1) * BucketSize,
					StoreID = ar_storage_module:id(M),
					[{Start, End, MiningAddress, StoreID} | Acc];
				(_Module, Acc) ->
					Acc
			end,
			[],
			Config#config.storage_modules
		),

	%% And then map those storage modules to partitions.
	get_io_channels(StorageModules, []).

get_io_channels([], Channels) ->
	Channels;
get_io_channels([{Start, End, _MiningAddress, _StoreID} | StorageModules], Channels)
  		when Start >= End ->
	get_io_channels(StorageModules, Channels);
get_io_channels([{Start, End, MiningAddress, StoreID} | StorageModules], Channels) ->
	PartitionNumber = ar_node:get_partition_number(Start),
	Channels2 = [{PartitionNumber, MiningAddress, StoreID} | Channels],
	StorageModules2 = [{Start + ?PARTITION_SIZE, End, MiningAddress, StoreID} | StorageModules],
	get_io_channels(StorageModules2, Channels2).

start_io_thread(PartitionNumber, MiningAddress, StoreID, #state{ io_threads = Threads } = State)
		when is_map_key({PartitionNumber, MiningAddress, StoreID}, Threads) ->
	State;
start_io_thread(PartitionNumber, MiningAddress, StoreID,
		#state{ io_threads = Threads, io_thread_monitor_refs = Refs } = State) ->
	Thread =
		spawn(
			fun() ->
				case StoreID of
					"default" ->
						ok;
					_ ->
						ar_chunk_storage:open_files(StoreID)
				end,
				io_thread(PartitionNumber, MiningAddress, StoreID)
			end
		),
	Ref = monitor(process, Thread),
	Threads2 = maps:put({PartitionNumber, MiningAddress, StoreID}, Thread, Threads),
	Refs2 = maps:put(Ref, {PartitionNumber, MiningAddress, StoreID}, Refs),
	?LOG_DEBUG([{event, started_io_mining_thread}, {partition_number, PartitionNumber},
			{mining_addr, ar_util:safe_encode(MiningAddress)}, {store_id, StoreID}]),
	State#state{ io_threads = Threads2, io_thread_monitor_refs = Refs2 }.

handle_io_thread_down(Ref, Reason,
		#state{ io_threads = Threads, io_thread_monitor_refs = Refs } = State) ->
	?LOG_WARNING([{event, mining_io_thread_down}, {reason, io_lib:format("~p", [Reason])}]),
	ThreadID = {PartitionNumber, MiningAddress, StoreID} = maps:get(Ref, Refs),
	Refs2 = maps:remove(Ref, Refs),
	Threads2 = maps:remove(ThreadID, Threads),
	start_io_thread(PartitionNumber, MiningAddress, StoreID,
			State#state{ io_threads = Threads2, io_thread_monitor_refs = Refs2 }).

io_thread(PartitionNumber, MiningAddress, StoreID) ->
	receive
		{WhichChunk, {Worker, Candidate, RecallRangeStart}} ->
			read_range(WhichChunk, Worker, Candidate, RecallRangeStart, StoreID),
			io_thread(PartitionNumber, MiningAddress, StoreID)
	end.

get_packed_intervals(Start, End, MiningAddress, "default", Intervals) ->
	Packing = {spora_2_6, MiningAddress},
	case ar_sync_record:get_next_synced_interval(Start, End, Packing, ar_data_sync, "default") of
		not_found ->
			Intervals;
		{Right, Left} ->
			get_packed_intervals(Right, End, MiningAddress, "default",
					ar_intervals:add(Intervals, Right, Left))
	end;
get_packed_intervals(_Start, _End, _ReplicaID, _StoreID, _Intervals) ->
	no_interval_check_implemented_for_non_default_store.

filter_by_packing([], _Intervals, _StoreID) ->
	[];
filter_by_packing([{EndOffset, Chunk} | ChunkOffsets], Intervals, "default" = StoreID) ->
	case ar_intervals:is_inside(Intervals, EndOffset) of
		false ->
			filter_by_packing(ChunkOffsets, Intervals, StoreID);
		true ->
			[{EndOffset, Chunk} | filter_by_packing(ChunkOffsets, Intervals, StoreID)]
	end;
filter_by_packing(ChunkOffsets, _Intervals, _StoreID) ->
	ChunkOffsets.

read_range(WhichChunk, Worker, Candidate, RangeStart, StoreID) ->
	StartTime = erlang:monotonic_time(),
	Size = ?RECALL_RANGE_SIZE,
	#mining_candidate{ mining_address = MiningAddress } = Candidate,
	Intervals = get_packed_intervals(RangeStart, RangeStart + Size,
			MiningAddress, StoreID, ar_intervals:new()),
	ChunkOffsets = ar_chunk_storage:get_range(RangeStart, Size, StoreID),
	ChunkOffsets2 = filter_by_packing(ChunkOffsets, Intervals, StoreID),
	NonceMax = max(0, (Size div ?DATA_CHUNK_SIZE - 1)),
	read_range(WhichChunk, Worker, Candidate, RangeStart, 0, NonceMax, ChunkOffsets2),
	log_read_range(Candidate, WhichChunk, length(ChunkOffsets), StartTime).

read_range(_WhichChunk, _Worker, _Candidate, _RangeStart, Nonce, NonceMax, _ChunkOffsets)
		when Nonce > NonceMax ->
	ok;
read_range(WhichChunk, Worker, Candidate, RangeStart, Nonce, NonceMax, []) ->
	ar_mining_worker:recall_chunk(Worker, skipped, WhichChunk, Nonce, Candidate),
	read_range(WhichChunk, Worker, Candidate, RangeStart, Nonce + 1, NonceMax, []);
read_range(WhichChunk, Worker, Candidate, RangeStart, Nonce, NonceMax,
		[{EndOffset, Chunk} | ChunkOffsets])
		%% Only 256 KiB chunks are supported at this point.
		when RangeStart + Nonce * ?DATA_CHUNK_SIZE < EndOffset - ?DATA_CHUNK_SIZE ->
	ar_mining_worker:recall_chunk(Worker, skipped, WhichChunk, Nonce, Candidate),
	read_range(WhichChunk, Worker, Candidate, RangeStart, Nonce + 1, NonceMax,
		[{EndOffset, Chunk} | ChunkOffsets]);
read_range(WhichChunk, Worker, Candidate, RangeStart, Nonce, NonceMax,
		[{EndOffset, _Chunk} | ChunkOffsets])
		when RangeStart + Nonce * ?DATA_CHUNK_SIZE >= EndOffset ->
	read_range(WhichChunk, Worker, Candidate, RangeStart, Nonce, NonceMax, ChunkOffsets);
read_range(WhichChunk, Worker, Candidate, RangeStart, Nonce, NonceMax,
		[{_EndOffset, Chunk} | ChunkOffsets]) ->
	ar_mining_worker:recall_chunk(Worker, WhichChunk, Chunk, Nonce, Candidate),
	read_range(WhichChunk, Worker, Candidate, RangeStart, Nonce + 1, NonceMax, ChunkOffsets).

log_read_range(Candidate, WhichChunk, FoundChunks, StartTime) ->
	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime-StartTime, native, millisecond),
	ReadRate = case ElapsedTime > 0 of 
		true -> (FoundChunks * 1000 div 4) div ElapsedTime; %% MiB per second
		false -> 0
	end,

	#mining_candidate{
		partition_number = Partition1, partition_number2 = Partition2 } = Candidate,

	PartitionNumber = case WhichChunk of
		chunk1 -> Partition1;
		chunk2 -> Partition2
	end,

	ar_mining_stats:raw_read_rate(PartitionNumber, ReadRate).

	% ?LOG_DEBUG([{event, mining_debug_read_recall_range},
	% 		{worker, Worker},
	% 		{thread, self()},
	% 		{elapsed_time_ms, ElapsedTime},
	% 		{read_rate_mibps, ReadRate},
	% 		{chunk, WhichChunk},
	% 		{range_start, RangeStart},
	% 		{size, ?RECALL_RANGE_SIZE},
	% 		{h0, ar_util:safe_encode(H0)},
	% 		{step_number, StepNumber},
	% 		{output, ar_util:safe_encode(Output)},
	% 		{partition_number, PartitionNumber},
	% 		{store_id, StoreID},
	% 		{found_chunks, FoundChunks},
	% 		{found_chunks_with_required_packing, FoundChunksWithRequiredPacking}]).

find_thread(PartitionNumber, MiningAddress, RangeEnd, RangeStart, Threads) ->
	Keys = find_thread2(PartitionNumber, MiningAddress, maps:iterator(Threads)),
	case find_thread3(Keys, RangeEnd, RangeStart, 0, not_found) of
		not_found ->
			not_found;
		Key ->
			maps:get(Key, Threads)
	end.

find_thread2(PartitionNumber, MiningAddress, Iterator) ->
	case maps:next(Iterator) of
		none ->
			[];
		{{PartitionNumber, MiningAddress, _StoreID} = Key, _Thread, Iterator2} ->
			[Key | find_thread2(PartitionNumber, MiningAddress, Iterator2)];
		{_Key, _Thread, Iterator2} ->
			find_thread2(PartitionNumber, MiningAddress, Iterator2)
	end.

find_thread3([Key | Keys], RangeEnd, RangeStart, Max, MaxKey) ->
	{_PartitionNumber, _MiningAddress, StoreID} = Key,
	I = ar_sync_record:get_intersection_size(RangeEnd, RangeStart, ar_chunk_storage, StoreID),
	case I > Max of
		true ->
			find_thread3(Keys, RangeEnd, RangeStart, I, Key);
		false ->
			find_thread3(Keys, RangeEnd, RangeStart, Max, MaxKey)
	end;
find_thread3([], _RangeEnd, _RangeStart, _Max, MaxKey) ->
	MaxKey.

