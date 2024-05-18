-module(ar_mining_io).

-behaviour(gen_server).

-export([start_link/0, set_largest_seen_upper_bound/1, 
			get_partitions/0, get_partitions/1, read_recall_range/4, garbage_collect/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CACHE_TTL_MS, 2000).

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
	gen_server:call(?MODULE, {set_largest_seen_upper_bound, PartitionUpperBound}, 60000).

get_partitions() ->
	gen_server:call(?MODULE, get_partitions, 60000).

read_recall_range(WhichChunk, Worker, Candidate, RecallRangeStart) ->
	gen_server:call(?MODULE,
			{read_recall_range, WhichChunk, Worker, Candidate, RecallRangeStart}, 60000).

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

garbage_collect() ->
	gen_server:cast(?MODULE, garbage_collect).

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
	ThreadFound = case find_thread(PartitionNumber, MiningAddress, RangeEnd, RecallRangeStart, IOThreads) of
		not_found ->
			false;
		Thread ->
			Thread ! {WhichChunk, {Worker, Candidate, RecallRangeStart}},
			true
	end,
	{reply, ThreadFound, State};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(garbage_collect, State) ->
	erlang:garbage_collect(self(),
		[{async, {ar_mining_io, self(), erlang:monotonic_time()}}]),
	maps:fold(
		fun(_Key, Thread, _) ->
			erlang:garbage_collect(Thread,
				[{async, {ar_mining_io_worker, Thread, erlang:monotonic_time()}}])
		end,
		ok,
		State#state.io_threads
	),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({garbage_collect, {Name, Pid, StartTime}, GCResult}, State) ->
	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime-StartTime, native, millisecond),
	case GCResult == false orelse ElapsedTime > ?GC_LOG_THRESHOLD of
		true ->
			?LOG_DEBUG([
				{event, mining_debug_garbage_collect}, {process, Name}, {pid, Pid},
				{gc_time, ElapsedTime}, {gc_result, GCResult}]);
		false ->
			ok
	end,
	{noreply, State};

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

%% @doc Returns tuples {PartitionNumber, MiningAddress, StoreID) covering all attached storage
%% modules (excluding the "default" storage module). The assumption is that each IO channel
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
	Now = os:system_time(millisecond),
	Thread =
		spawn(
			fun() ->
				case StoreID of
					"default" ->
						ok;
					_ ->
						ar_chunk_storage:open_files(StoreID)
				end,
				io_thread(PartitionNumber, MiningAddress, StoreID, #{}, Now)
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

io_thread(PartitionNumber, MiningAddress, StoreID, Cache, LastClearTime) ->
	receive
		{WhichChunk, {Worker, Candidate, RecallRangeStart}} ->
			{ChunkOffsets, Cache2} =
				get_chunks(WhichChunk, Candidate, RecallRangeStart, StoreID, Cache),
			send_chunks(WhichChunk, Worker, Candidate, RecallRangeStart, ChunkOffsets),
			{Cache3, LastClearTime2} = maybe_clear_cached_chunks(Cache2, LastClearTime),
			io_thread(PartitionNumber, MiningAddress, StoreID, Cache3, LastClearTime2)
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

maybe_clear_cached_chunks(Cache, LastClearTime) ->
	Now = os:system_time(millisecond),
	case (Now - LastClearTime) > (?CACHE_TTL_MS div 2) of
		true ->
			CutoffTime = Now - ?CACHE_TTL_MS,
			Cache2 = maps:filter(
				fun(_CachedRangeStart, {CachedTime, _ChunkOffsets}) ->
					%% Remove all ranges that were cached before the CutoffTime
					%% true: keep
					%% false: remove
					case CachedTime > CutoffTime of
						true ->
							true;
						false ->
							false
					end
				end,
				Cache),
			{Cache2, Now};
		false ->
			{Cache, LastClearTime}
	end.

%% @doc When we're reading a range for a CM peer we'll cache it temporarily in case
%% that peer has broken up the batch of H1s into multiple requests. The temporary cache
%% prevents us from reading the same range from disk multiple times.
%% 
%% However if the request is from our local miner there's no need to cache since the H1
%% batch is always handled all at once.
get_chunks(WhichChunk, Candidate, RangeStart, StoreID, Cache) ->
	case Candidate#mining_candidate.cm_lead_peer of
		not_set ->
			ChunkOffsets = read_range(WhichChunk, Candidate, RangeStart, StoreID),
			{ChunkOffsets, Cache};
		_ ->
			cached_read_range(WhichChunk, Candidate, RangeStart, StoreID, Cache)
	end.

cached_read_range(WhichChunk, Candidate, RangeStart, StoreID, Cache) ->
	Now = os:system_time(millisecond),
	case maps:get(RangeStart, Cache, not_found) of
		not_found ->	
			ChunkOffsets = read_range(WhichChunk, Candidate, RangeStart, StoreID),
			Cache2 = maps:put(RangeStart, {Now, ChunkOffsets}, Cache),
			{ChunkOffsets, Cache2};
		{_CachedTime, ChunkOffsets} ->
			?LOG_DEBUG([{event, mining_debug_read_cached_recall_range},
				{pid, self()}, {range_start, RangeStart},
				{store_id, StoreID},
				{partition_number, Candidate#mining_candidate.partition_number},
				{partition_number2, Candidate#mining_candidate.partition_number2},
				{cm_peer, ar_util:format_peer(Candidate#mining_candidate.cm_lead_peer)},
				{cache_ref, Candidate#mining_candidate.cache_ref},
				{session, ar_nonce_limiter:encode_session_key(Candidate#mining_candidate.session_key)}]),
			{ChunkOffsets, Cache}
	end.

read_range(WhichChunk, Candidate, RangeStart, StoreID) ->
	StartTime = erlang:monotonic_time(),
	#mining_candidate{ mining_address = MiningAddress } = Candidate,
	Intervals = get_packed_intervals(RangeStart, RangeStart + ?RECALL_RANGE_SIZE,
			MiningAddress, StoreID, ar_intervals:new()),
	ChunkOffsets = ar_chunk_storage:get_range(RangeStart, ?RECALL_RANGE_SIZE, StoreID),
	ChunkOffsets2 = filter_by_packing(ChunkOffsets, Intervals, StoreID),
	log_read_range(Candidate, WhichChunk, length(ChunkOffsets), StartTime),
	ChunkOffsets2.

send_chunks(WhichChunk, Worker, Candidate, RangeStart, ChunkOffsets) ->
	NonceMax = max(0, (?RECALL_RANGE_SIZE div ?DATA_CHUNK_SIZE - 1)),
	send_chunks(WhichChunk, Worker, Candidate, RangeStart, 0, NonceMax, ChunkOffsets).

send_chunks(_WhichChunk, _Worker, _Candidate, _RangeStart, Nonce, NonceMax, _ChunkOffsets)
		when Nonce > NonceMax ->
	ok;
send_chunks(WhichChunk, Worker, Candidate, RangeStart, Nonce, NonceMax, []) ->
	ar_mining_worker:recall_chunk(Worker, skipped, WhichChunk, Nonce, Candidate),
	send_chunks(WhichChunk, Worker, Candidate, RangeStart, Nonce + 1, NonceMax, []);
send_chunks(WhichChunk, Worker, Candidate, RangeStart, Nonce, NonceMax,
		[{EndOffset, Chunk} | ChunkOffsets])
		%% Only 256 KiB chunks are supported at this point.
		when RangeStart + Nonce * ?DATA_CHUNK_SIZE < EndOffset - ?DATA_CHUNK_SIZE ->
	ar_mining_worker:recall_chunk(Worker, skipped, WhichChunk, Nonce, Candidate),
	send_chunks(WhichChunk, Worker, Candidate, RangeStart, Nonce + 1, NonceMax,
		[{EndOffset, Chunk} | ChunkOffsets]);
send_chunks(WhichChunk, Worker, Candidate, RangeStart, Nonce, NonceMax,
		[{EndOffset, _Chunk} | ChunkOffsets])
		when RangeStart + Nonce * ?DATA_CHUNK_SIZE >= EndOffset ->
	send_chunks(WhichChunk, Worker, Candidate, RangeStart, Nonce, NonceMax, ChunkOffsets);
send_chunks(WhichChunk, Worker, Candidate, RangeStart, Nonce, NonceMax,
		[{_EndOffset, Chunk} | ChunkOffsets]) ->
	ar_mining_worker:recall_chunk(Worker, WhichChunk, Chunk, Nonce, Candidate),
	send_chunks(WhichChunk, Worker, Candidate, RangeStart, Nonce + 1, NonceMax, ChunkOffsets).

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
