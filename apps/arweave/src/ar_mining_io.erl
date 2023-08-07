-module(ar_mining_io).

-behaviour(gen_server).

-export([start_link/0, stop/0, reset/1, reset_performance_counters/0, get_partitions/1,
			get_thread_count/0, read_recall_range/3]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	session_ref = undefined,	
	io_threads = #{},
	io_thread_monitor_refs = #{}
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the gen_server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
	gen_server:cast(?MODULE, stop).

reset(SessionRef) ->
	gen_server:cast(?MODULE, {reset, SessionRef}).

reset_performance_counters() ->
	gen_server:cast(?MODULE, reset_performance_counters).

get_partitions(PartitionUpperBound) ->
	gen_server:call(?MODULE, {get_partitions, PartitionUpperBound}).

get_thread_count() ->
	gen_server:call(?MODULE, get_thread_count).

read_recall_range(WhichChunk, Candidate, RecallRangeStart) ->
	gen_server:call(?MODULE, {read_recall_range, WhichChunk, Candidate, RecallRangeStart}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	MiningAddr = Config#config.mining_addr,
	State =
		lists:foldl(
			fun	({BucketSize, Bucket, {spora_2_6, Addr}} = M, Acc) when Addr == MiningAddr ->
					Start = Bucket * BucketSize,
					End = (Bucket + 1) * BucketSize,
					StoreID = ar_storage_module:id(M),
					start_io_threads(Start, End, MiningAddr, StoreID, Acc);
				(_Module, Acc) ->
					Acc
			end,
			#state{},
			Config#config.storage_modules
		),
	State2 =
		ar_intervals:fold(
			fun({End, Start}, Acc) ->
				start_io_threads(Start, End, MiningAddr, "default", Acc)
			end,
			State,
			ar_sync_record:get(ar_data_sync, "default")
		),
	{ok, State2}.

handle_call({get_partitions, PartitionUpperBound}, _From,
		#state{ io_threads = IOThreads } = State) ->
	Max = max(0, (PartitionUpperBound-1) div ?PARTITION_SIZE),
	Partitions = lists:sort(sets:to_list(
		maps:fold(
			fun({Partition, MiningAddress, StoreID}, _, Acc) ->
				case Partition > Max of
					true ->
						Acc;
					_ ->
						sets:add_element({Partition, MiningAddress, StoreID}, Acc)
				end
			end,
			sets:new(), % A storage module may be smaller than a partition.
			IOThreads
		))
	),
	{reply, Partitions, State};

handle_call(get_thread_count, _From, #state{ io_threads = IOThreads } = State) ->
	{reply, maps:size(IOThreads), State};

handle_call({read_recall_range, WhichChunk, Candidate, RecallRangeStart}, _From,
		#state{ io_threads = IOThreads } = State) ->
	#mining_candidate{ mining_address = MiningAddress } = Candidate,
	PartitionNumber = ?PARTITION_NUMBER(RecallRangeStart),
	RangeEnd = RecallRangeStart + ?RECALL_RANGE_SIZE,
	case find_thread(PartitionNumber, MiningAddress, RangeEnd, RecallRangeStart, IOThreads) of
		not_found ->
			{reply, false, State};
		Thread ->
			Thread ! {WhichChunk, {Candidate, RecallRangeStart}},
			{reply, true, State}
	end;

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(stop, #state{ io_threads = IOThreads } = State) ->
	[Thread ! stop || Thread <- maps:values(IOThreads)],
	{noreply, State#state{ session_ref = undefined }};

handle_cast({reset, SessionRef}, #state{ io_threads = IOThreads } = State) ->
	[Thread ! {new_mining_session, SessionRef} || Thread <- maps:values(IOThreads)],
	{noreply, State#state{ session_ref = SessionRef }};

handle_cast(reset_performance_counters, #state{ io_threads = IOThreads } = State) ->
	[Thread ! reset_performance_counters || Thread <- maps:values(IOThreads)],
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
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
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(_Reason, #state{ io_threads = IOThreads }) ->
	[Thread ! stop || Thread <- maps:values(IOThreads)],
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

start_io_threads(Start, End, _ReplicaID, _StoreID, State) when Start >= End ->
	State;
start_io_threads(Start, End, MiningAddress, StoreID, State) ->
	PartitionNumber = ?PARTITION_NUMBER(Start),
	State2 = start_io_thread(PartitionNumber, MiningAddress, StoreID, State),
	start_io_threads(Start + ?PARTITION_SIZE, End, MiningAddress, StoreID, State2).

start_io_thread(PartitionNumber, MiningAddress, StoreID, #state{ io_threads = Threads } = State)
		when is_map_key({PartitionNumber, MiningAddress, StoreID}, Threads) ->
	State;
start_io_thread(PartitionNumber, MiningAddress, StoreID,
		#state{ io_threads = Threads, io_thread_monitor_refs = Refs, 
			session_ref = SessionRef } = State) ->
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
	case SessionRef of
		undefined ->
			ok;
		_ ->
			Thread ! {new_mining_session, SessionRef}
	end,
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
	io_thread(PartitionNumber, MiningAddress, StoreID, not_set).

io_thread(PartitionNumber, MiningAddress, StoreID, SessionRef) ->
	receive
		stop ->
			io_thread(PartitionNumber, MiningAddress, StoreID);
		reset_performance_counters ->
			ets:insert(?MODULE, [{{performance, PartitionNumber},
					erlang:monotonic_time(millisecond), 0,
					erlang:monotonic_time(millisecond), 0}]),
			io_thread(PartitionNumber, MiningAddress, StoreID, SessionRef);
		{new_mining_session, Ref} ->
			io_thread(PartitionNumber, MiningAddress, StoreID, Ref);
		{WhichChunk, {Candidate, RecallRangeStart}} ->
			case ar_mining_server:is_session_valid(SessionRef, Candidate) of
				true -> 
					read_range(WhichChunk, Candidate, RecallRangeStart, StoreID);
				false -> 
					ok %% Clear the message queue of requests from outdated mining sessions
			end,
			io_thread(PartitionNumber, MiningAddress, StoreID, SessionRef)
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

read_range(WhichChunk, Candidate, RangeStart, StoreID) ->
	Size = ?RECALL_RANGE_SIZE,
	#mining_candidate{ mining_address = MiningAddress } = Candidate,
	Intervals = get_packed_intervals(RangeStart, RangeStart + Size,
			MiningAddress, StoreID, ar_intervals:new()),
	ChunkOffsets = ar_chunk_storage:get_range(RangeStart, Size, StoreID),
	ChunkOffsets2 = filter_by_packing(ChunkOffsets, Intervals, StoreID),
	?LOG_DEBUG([{event, mining_debug_read_recall_range},
			{chunk, WhichChunk},
			{range_start, RangeStart},
			{store_id, StoreID},
			{found_chunks, length(ChunkOffsets)},
			{found_chunks_with_required_packing, length(ChunkOffsets2)}]),
	NonceMax = max(0, (Size div ?DATA_CHUNK_SIZE - 1)),
	read_range(WhichChunk, Candidate, RangeStart, 0, NonceMax, ChunkOffsets2).

read_range(_WhichChunk, _Candidate, _RangeStart, Nonce, NonceMax, _ChunkOffsets)
		when Nonce > NonceMax ->
	ok;
read_range(WhichChunk, Candidate, RangeStart, Nonce, NonceMax, []) ->
	ar_mining_server:recall_chunk(skipped, undefined, Nonce, Candidate),
	read_range(WhichChunk, Candidate, RangeStart, Nonce + 1, NonceMax, []);
read_range(WhichChunk, Candidate,RangeStart, Nonce, NonceMax, [{EndOffset, Chunk} | ChunkOffsets])
		%% Only 256 KiB chunks are supported at this point.
		when RangeStart + Nonce * ?DATA_CHUNK_SIZE < EndOffset - ?DATA_CHUNK_SIZE ->
	ar_mining_server:recall_chunk(skipped, undefined, Nonce, Candidate),
	read_range(WhichChunk, Candidate, RangeStart, Nonce + 1, NonceMax,
		[{EndOffset, Chunk} | ChunkOffsets]);
read_range(WhichChunk, Candidate, RangeStart, Nonce, NonceMax, [{EndOffset, _Chunk} | ChunkOffsets])
		when RangeStart + Nonce * ?DATA_CHUNK_SIZE >= EndOffset ->
	read_range(WhichChunk, Candidate, RangeStart, Nonce, NonceMax, ChunkOffsets);
read_range(WhichChunk, Candidate, RangeStart, Nonce, NonceMax,
		[{_EndOffset, Chunk} | ChunkOffsets]) ->
	PartitionNumber = Candidate#mining_candidate.partition_number,
	ets:update_counter(?MODULE, {performance, PartitionNumber}, [{3, 1}, {5, 1}],
			{{performance, PartitionNumber},
			 erlang:monotonic_time(millisecond), 1,
			 erlang:monotonic_time(millisecond), 1}),
	ar_mining_server:recall_chunk(WhichChunk, Chunk, Nonce, Candidate),
	read_range(WhichChunk, Candidate, RangeStart, Nonce + 1, NonceMax, ChunkOffsets).

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
	{_PartitionNumber, _ReplicaID, StoreID} = Key,
	I = ar_sync_record:get_intersection_size(RangeEnd, RangeStart, ar_chunk_storage, StoreID),
	case I > Max of
		true ->
			find_thread3(Keys, RangeEnd, RangeStart, I, Key);
		false ->
			find_thread3(Keys, RangeEnd, RangeStart, Max, MaxKey)
	end;
find_thread3([], _RangeEnd, _RangeStart, _Max, MaxKey) ->
	MaxKey.

