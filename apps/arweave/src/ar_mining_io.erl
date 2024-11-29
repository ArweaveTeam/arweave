-module(ar_mining_io).

-behaviour(gen_server).

-export([start_link/0, start_link/1, set_largest_seen_upper_bound/1, 
			get_packing/0, get_partitions/0, get_partitions/1, read_recall_range/4,
			garbage_collect/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CACHE_TTL_MS, 2000).

-record(state, {
	mode = miner,
	partition_upper_bound = 0,
	io_threads = #{},
	io_thread_monitor_refs = #{},
	store_id_to_device = #{},
	partition_to_store_ids = #{}
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the gen_server.
start_link() ->
	start_link(miner).

start_link(Mode) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Mode, []).

set_largest_seen_upper_bound(PartitionUpperBound) ->
	gen_server:call(?MODULE, {set_largest_seen_upper_bound, PartitionUpperBound}, 60000).

get_partitions() ->
	gen_server:call(?MODULE, get_partitions, 60000).

read_recall_range(WhichChunk, Worker, Candidate, RecallRangeStart) ->
	gen_server:call(?MODULE,
			{read_recall_range, WhichChunk, Worker, Candidate, RecallRangeStart}, 60000).

get_packing() ->
	{ok, Config} = application:get_env(arweave, config),
	%% ar_config:validate_storage_modules/1 ensures that we only mine against a single
	%% packing format. So we can grab it any partition.
	case Config#config.storage_modules of
		[] -> undefined;
        [{_, _, Packing} | _Rest] -> Packing
    end.

get_partitions(PartitionUpperBound) when PartitionUpperBound =< 0 ->
	[];
get_partitions(PartitionUpperBound) ->
	{ok, Config} = application:get_env(arweave, config),
	Max = ar_node:get_max_partition_number(PartitionUpperBound),
	AllPartitions = lists:foldl(
		fun	(Module, Acc) ->
				Addr = ar_storage_module:module_address(Module),
				PackingDifficulty = 
					ar_storage_module:module_packing_difficulty(Module),
				{Start, End} = ar_storage_module:module_range(Module, 0),
				Partitions = get_store_id_partitions({Start, End}, []),
				lists:foldl(
					fun(PartitionNumber, AccInner) ->
						sets:add_element({PartitionNumber, Addr, PackingDifficulty}, AccInner)
					end,
					Acc,
					Partitions
				)
		end,
		sets:new(),
		Config#config.storage_modules
	),
	FilteredPartitions = sets:filter(
        fun ({PartitionNumber, Addr, _PackingDifficulty}) ->
            PartitionNumber =< Max andalso Addr == Config#config.mining_addr
        end,
        AllPartitions
    ),
    lists:sort(sets:to_list(FilteredPartitions)).

garbage_collect() ->
	gen_server:cast(?MODULE, garbage_collect).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Mode) ->
	{ok, start_io_threads(#state{ mode = Mode })}.

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

handle_call({read_recall_range, WhichChunk, Worker, Candidate, RecallRangeStart},
		_From, State) ->
	#mining_candidate{ packing_difficulty = PackingDifficulty } = Candidate,
	RangeEnd = RecallRangeStart + ar_block:get_recall_range_size(PackingDifficulty),
	ThreadFound = case find_thread(RecallRangeStart, RangeEnd, State) of
		not_found ->
			false;
		{Thread, StoreID} ->
			Thread ! {WhichChunk, {Worker, Candidate, RecallRangeStart, StoreID}},
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

get_system_device(StorageModule) ->
	{ok, Config} = application:get_env(arweave, config),
	StoreID = ar_storage_module:id(StorageModule),
	Path = ar_chunk_storage:get_chunk_storage_path(Config#config.data_dir, StoreID),
	Command = "df -P " ++ Path ++ " | awk 'NR==2 {print $1}'",
	Device = os:cmd(Command),
	TrimmedDevice = string:trim(Device),
	case TrimmedDevice of
		"" -> StoreID;  % If the command fails or returns an empty string, return StoreID
		_ -> TrimmedDevice
	end.

start_io_threads(State) ->
	#state{ mode = Mode } = State,

    % Step 1: Group StoreIDs by their system device
    DeviceToStoreIDs = map_device_to_store_ids(),

    % Step 2: Start IO threads for each device and populate map indices
	maps:fold(
		fun(Device, StoreIDs, StateAcc) ->
			#state{ io_threads = Threads, io_thread_monitor_refs = Refs,
				store_id_to_device = StoreIDToDevice,
				partition_to_store_ids = PartitionToStoreIDs } = StateAcc,
			
			Thread = start_io_thread(Mode, StoreIDs),
			ThreadRef = monitor(process, Thread),

			StoreIDToDevice2 = lists:foldl(
				fun(StoreID, Acc) -> 
					maps:put(StoreID, Device, Acc) 
				end,
				StoreIDToDevice, StoreIDs),

			PartitionToStoreIDs2 = map_partition_to_store_ids(StoreIDs, PartitionToStoreIDs),
			StateAcc#state{
				io_threads = maps:put(Device, Thread, Threads),
				io_thread_monitor_refs = maps:put(ThreadRef, Device, Refs),
				store_id_to_device = StoreIDToDevice2,
				partition_to_store_ids = PartitionToStoreIDs2
			}
		end,
		State,
		DeviceToStoreIDs
	).

start_io_thread(Mode, StoreIDs) ->
	Now = os:system_time(millisecond),
	spawn(
		fun() ->
			open_files(StoreIDs),
			io_thread(Mode, #{}, Now)
		end
	).

map_partition_to_store_ids([], PartitionToStoreIDs) ->
	PartitionToStoreIDs;
map_partition_to_store_ids([StoreID | StoreIDs], PartitionToStoreIDs) ->
	StorageModule = ar_storage_module:get_by_id(StoreID),
	{Start, End} = ar_storage_module:module_range(StorageModule, 0),
	Partitions = get_store_id_partitions({Start, End}, []),
	PartitionToStoreIDs2 = lists:foldl(
		fun(Partition, Acc) ->
			maps:update_with(Partition,
				fun(PartitionStoreIDs) -> [StoreID | PartitionStoreIDs] end,
			[StoreID], Acc)
		end,
		PartitionToStoreIDs, Partitions),
	map_partition_to_store_ids(StoreIDs, PartitionToStoreIDs2).

map_device_to_store_ids() ->
	{ok, Config} = application:get_env(arweave, config),
	lists:foldl(
        fun(Module, Acc) ->
			StoreID = ar_storage_module:id(Module),
            Device = get_system_device(Module),
            maps:update_with(Device, fun(StoreIDs) -> [StoreID | StoreIDs] end, [StoreID], Acc)
        end,
        #{},
        Config#config.storage_modules
    ).

get_store_ids_for_device(Device, #state{store_id_to_device = StoreIDToDevice}) ->
	maps:fold(
		fun(StoreID, MappedDevice, Acc) ->
			case MappedDevice == Device of
				true -> [StoreID | Acc];
				false -> Acc
			end
		end,
		[],
		StoreIDToDevice
	).

get_store_id_partitions({Start, End}, Partitions) when Start >= End ->
	Partitions;
get_store_id_partitions({Start, End}, Partitions) ->
	PartitionNumber = ar_node:get_partition_number(Start),
	get_store_id_partitions({Start + ?PARTITION_SIZE, End}, [PartitionNumber | Partitions]).

open_files(StoreIDs) ->
	lists:foreach(
		fun(StoreID) ->
			case StoreID of
				"default" ->
					ok;
				_ ->
					ar_chunk_storage:open_files(StoreID)
			end
		end,
		StoreIDs).

handle_io_thread_down(Ref, Reason,
		#state{ mode = Mode, io_threads = Threads, io_thread_monitor_refs = Refs } = State) ->
	?LOG_WARNING([{event, mining_io_thread_down}, {reason, io_lib:format("~p", [Reason])}]),
	Device = maps:get(Ref, Refs),
	Refs2 = maps:remove(Ref, Refs),
	Threads2 = maps:remove(Device, Threads),

	StoreIDs = get_store_ids_for_device(Device, State),
	Thread = start_io_thread(Mode, StoreIDs),
	ThreadRef = monitor(process, Thread),
	State#state{ io_threads = maps:put(Device, Thread, Threads2),	
		io_thread_monitor_refs = maps:put(ThreadRef, Device, Refs2) }.

io_thread(Mode, Cache, LastClearTime) ->
	receive
		{WhichChunk, {Worker, Candidate, RecallRangeStart, StoreID}} ->
			{ChunkOffsets, Cache2} =
				get_chunks(Mode, WhichChunk, Candidate, RecallRangeStart, StoreID, Cache),
			chunks_read(Mode, Worker, WhichChunk, Candidate, RecallRangeStart, ChunkOffsets),
			{Cache3, LastClearTime2} = maybe_clear_cached_chunks(Cache2, LastClearTime),
			io_thread(Mode, Cache3, LastClearTime2)
	end.

chunks_read(miner, Worker, WhichChunk, Candidate, RecallRangeStart, ChunkOffsets) ->
	ar_mining_worker:chunks_read(
		Worker, WhichChunk, Candidate, RecallRangeStart, ChunkOffsets);
chunks_read(standalone, Worker, WhichChunk, Candidate, RecallRangeStart, ChunkOffsets) ->
	Worker ! {chunks_read, WhichChunk, Candidate, RecallRangeStart, ChunkOffsets}.

get_packed_intervals(Start, End, MiningAddress, PackingDifficulty, "default", Intervals) ->
	Packing = ar_block:get_packing(PackingDifficulty, MiningAddress),
	case ar_sync_record:get_next_synced_interval(Start, End, Packing, ar_data_sync, "default") of
		not_found ->
			Intervals;
		{Right, Left} ->
			get_packed_intervals(Right, End, MiningAddress, PackingDifficulty, "default",
					ar_intervals:add(Intervals, Right, Left))
	end;
get_packed_intervals(_Start, _End, _MiningAddr, _PackingDifficulty, _StoreID, _Intervals) ->
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
get_chunks(Mode, WhichChunk, Candidate, RangeStart, StoreID, Cache) ->
	case Candidate#mining_candidate.cm_lead_peer of
		not_set ->
			ChunkOffsets = read_range(Mode, WhichChunk, Candidate, RangeStart, StoreID),
			{ChunkOffsets, Cache};
		_ ->
			cached_read_range(Mode, WhichChunk, Candidate, RangeStart, StoreID, Cache)
	end.

cached_read_range(Mode, WhichChunk, Candidate, RangeStart, StoreID, Cache) ->
	Now = os:system_time(millisecond),
	case maps:get(RangeStart, Cache, not_found) of
		not_found ->	
			ChunkOffsets = read_range(Mode, WhichChunk, Candidate, RangeStart, StoreID),
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
				{session,
				ar_nonce_limiter:encode_session_key(Candidate#mining_candidate.session_key)}]),
			{ChunkOffsets, Cache}
	end.

read_range(Mode, WhichChunk, Candidate, RangeStart, StoreID) ->
	StartTime = erlang:monotonic_time(),
	#mining_candidate{ mining_address = MiningAddress,
			packing_difficulty = PackingDifficulty } = Candidate,
	RecallRangeSize = ar_block:get_recall_range_size(PackingDifficulty),
	Intervals = get_packed_intervals(RangeStart, RangeStart + RecallRangeSize,
			MiningAddress, PackingDifficulty, StoreID, ar_intervals:new()),
	ChunkOffsets = ar_chunk_storage:get_range(RangeStart, RecallRangeSize, StoreID),
	ChunkOffsets2 = filter_by_packing(ChunkOffsets, Intervals, StoreID),
	log_read_range(Mode, Candidate, WhichChunk, length(ChunkOffsets), StartTime),
	ChunkOffsets2.

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

log_read_range(standalone, _Candidate, _WhichChunk, _FoundChunks, _StartTime) ->
	ok;
log_read_range(_Mode, Candidate, WhichChunk, FoundChunks, StartTime) ->
	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime-StartTime, native, millisecond),
	ReadRate = case ElapsedTime > 0 of 
		true -> (FoundChunks * 1000 div 4) div ElapsedTime; %% MiB per second
		false -> 0
	end,

	PartitionNumber = case WhichChunk of
		chunk1 -> Candidate#mining_candidate.partition_number;
		chunk2 -> Candidate#mining_candidate.partition_number2
	end,

	ar_mining_stats:raw_read_rate(PartitionNumber, ReadRate),

	% ?LOG_DEBUG([{event, mining_debug_read_recall_range},
	% 		{thread, self()},
	% 		{elapsed_time_ms, ElapsedTime},
	% 		{chunks_read, FoundChunks},
	% 		{mib_read, FoundChunks / 4},
	% 		{read_rate_mibps, ReadRate},
	% 		{chunk, WhichChunk},
	% 		{partition_number, PartitionNumber}]),
	ok.

find_thread(RangeStart, RangeEnd, State) ->
	PartitionNumber = ar_node:get_partition_number(RangeStart),
	StoreIDs = maps:get(PartitionNumber, State#state.partition_to_store_ids, not_found),
	StoreID = find_largest_intersection(StoreIDs, RangeStart, RangeEnd, 0, not_found),
	Device = maps:get(StoreID, State#state.store_id_to_device, not_found),
	Thread = maps:get(Device, State#state.io_threads, not_found),
	case Thread of
		not_found ->
			not_found;
		_ ->
			{Thread, StoreID}
	end.

find_largest_intersection(not_found, _RangeStart, _RangeEnd, _Max, _MaxKey) ->
	not_found;
find_largest_intersection([StoreID | StoreIDs], RangeStart, RangeEnd, Max, MaxKey) ->
	I = ar_sync_record:get_intersection_size(RangeEnd, RangeStart, ar_chunk_storage, StoreID),
	case I > Max of
		true ->
			find_largest_intersection(StoreIDs, RangeStart, RangeEnd, I, StoreID);
		false ->
			find_largest_intersection(StoreIDs, RangeStart, RangeEnd, Max, MaxKey)
	end;
find_largest_intersection([], _RangeStart, _RangeEnd, _Max, MaxKey) ->
	MaxKey.
