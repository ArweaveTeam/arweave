-module(ar_mining_worker).

-behaviour(gen_server).

-export([start_link/2, name/2, reset_difficulty/2, set_sessions/2, chunks_read/5, computed_hash/5,
		set_difficulty/2, set_cache_limits/3, add_task/3, garbage_collect/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	name = not_set,
	partition_number = not_set,
	diff_pair = not_set,
	packing_difficulty = 0,
	task_queue = gb_sets:new(),
	%% The sub_chunk_cache stores either the first or second sub-chunk for a given nonce. This
	%% is because we process both the first and second recall ranges in parallel and don't know
	%% which data will be available first. For spora_2_6 packing (aka difficulty 0), sub-chunks
	%% and chunks are the same size (256KiB), for replica packing each sub-chunk is 8KiB.
	chunk_cache = undefined,
	vdf_queue_limit = 0,
	latest_vdf_step_number = 0,
	is_pool_client = false,
	h1_hashes = #{},
	h2_hashes = #{}
}).

-define(TASK_CHECK_FREQUENCY_MS, 200).
-define(STATUS_CHECK_FREQUENCY_MS, 5000).

%%%===================================================================
%%% Messages
%%%===================================================================

-define(MSG_RESET_DIFFICULTY(DiffPair), {reset_difficulty, DiffPair}).
-define(MSG_SET_SESSIONS(ActiveSessions), {set_sessions, ActiveSessions}).
-define(MSG_ADD_TASK(TaskType, Candidate, ExtraArgs), {add_task, {TaskType, Candidate, ExtraArgs}}).
-define(MSG_CHUNKS_READ(WhichChunk, Candidate, RangeStart, ChunkOffsets), {chunks_read, {WhichChunk, Candidate, RangeStart, ChunkOffsets}}).
-define(MSG_SET_DIFFICULTY(DiffPair), {set_difficulty, DiffPair}).
-define(MSG_SET_CACHE_LIMITS(SubChunkCacheLimitBytes, VDFQueueLimit), {set_cache_limits, SubChunkCacheLimitBytes, VDFQueueLimit}).
-define(MSG_REMOVE_SUB_CHUNKS_FROM_CACHE(SubChunkCount, Candidate), {remove_sub_chunks_from_cache, SubChunkCount, Candidate}).
-define(MSG_CHECK_WORKER_STATUS, check_worker_status).
-define(MSG_HANDLE_TASK, handle_task).
-define(MSG_GARBAGE_COLLECT(StartTime, GCResult), {garbage_collect, StartTime, GCResult}).
-define(MSG_GARBAGE_COLLECT, garbage_collect).
-define(MSG_FETCHED_LAST_MOMENT_PROOF(Any), {fetched_last_moment_proof, Any}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the gen_server.
start_link(Partition, PackingDifficulty) ->
	Name = name(Partition, PackingDifficulty),
	gen_server:start_link({local, Name}, ?MODULE, {Partition, PackingDifficulty}, []).

-spec name(Partition :: non_neg_integer(), PackingDifficulty :: non_neg_integer()) -> atom().
name(Partition, PackingDifficulty) ->
	list_to_atom(lists:flatten(["ar_mining_worker_", integer_to_list(Partition), "_", integer_to_list(PackingDifficulty)])).

-spec reset_difficulty(Worker :: pid(), DiffPair :: {non_neg_integer(), non_neg_integer()}) -> ok.
reset_difficulty(Worker, DiffPair) ->
	gen_server:cast(Worker, ?MSG_RESET_DIFFICULTY(DiffPair)).

-spec set_sessions(Worker :: pid(), ActiveSessions :: [ar_nonce_limiter:session_key()]) -> ok.
set_sessions(Worker, ActiveSessions) ->
	gen_server:cast(Worker, ?MSG_SET_SESSIONS(ActiveSessions)).

-spec add_task(Worker :: pid(), TaskType :: atom(), Candidate :: #mining_candidate{}) -> ok.
add_task(Worker, TaskType, Candidate) ->
	add_task(Worker, TaskType, Candidate, []).

-spec add_task(Worker :: pid(), TaskType :: atom(), Candidate :: #mining_candidate{}, ExtraArgs :: [term()]) -> ok.
add_task(Worker, TaskType, Candidate, ExtraArgs) ->
	gen_server:cast(Worker, ?MSG_ADD_TASK(TaskType, Candidate, ExtraArgs)).

-spec add_delayed_task(Worker :: pid(), TaskType :: atom(), Candidate :: #mining_candidate{}) -> ok.
add_delayed_task(Worker, TaskType, Candidate) ->
	%% Delay task by random amount between ?TASK_CHECK_FREQUENCY_MS and 2*?TASK_CHECK_FREQUENCY_MS
	%% The reason for the randomization to avoid a glut tasks to all get added at the same time -
	%% in particular when the chunk cache fills up it's possible for all queued compute_h0 tasks
	%% to be delayed at about the same time.
	Delay = rand:uniform(?TASK_CHECK_FREQUENCY_MS) + ?TASK_CHECK_FREQUENCY_MS,
	ar_util:cast_after(Delay, Worker, ?MSG_ADD_TASK(TaskType, Candidate, [])).

-spec chunks_read(
	Worker :: pid(),
	WhichChunk :: atom(),
	Candidate :: #mining_candidate{},
	RangeStart :: non_neg_integer(),
	ChunkOffsets :: [non_neg_integer()]
) -> ok.
chunks_read(Worker, WhichChunk, Candidate, RangeStart, ChunkOffsets) ->
	add_task(Worker, WhichChunk, Candidate, [RangeStart, ChunkOffsets]).

%% @doc Callback from the hashing threads when a hash is computed
-spec computed_hash(
	Worker :: pid(),
	TaskType :: atom(),
	Hash :: binary(),
	Preimage :: binary(),
	Candidate :: #mining_candidate{}
) -> ok.
computed_hash(Worker, computed_h0, H0, undefined, Candidate) ->
	add_task(Worker, computed_h0, Candidate#mining_candidate{ h0 = H0 });
computed_hash(Worker, computed_h1, H1, Preimage, Candidate) ->
	add_task(Worker, computed_h1, Candidate#mining_candidate{ h1 = H1, preimage = Preimage });
computed_hash(Worker, computed_h2, H2, Preimage, Candidate) ->
	add_task(Worker, computed_h2, Candidate#mining_candidate{ h2 = H2, preimage = Preimage }).

%% @doc Set the new mining difficulty. We do not recalculate it inside the mining
%% server or worker because we want to completely detach the mining server from the block
%% ordering. The previous block is chosen only after the mining solution is found (if
%% we choose it in advance we may miss a better option arriving in the process).
%% Also, a mining session may (in practice, almost always will) span several blocks.
-spec set_difficulty(Worker :: pid(), DiffPair :: {non_neg_integer(), non_neg_integer()}) -> ok.
set_difficulty(Worker, DiffPair) ->
	gen_server:cast(Worker, ?MSG_SET_DIFFICULTY(DiffPair)).

-spec set_cache_limits(Worker :: pid(), SubChunkCacheLimitBytes :: non_neg_integer(), VDFQueueLimit :: non_neg_integer()) -> ok.
set_cache_limits(Worker, SubChunkCacheLimitBytes, VDFQueueLimit) ->
	gen_server:cast(Worker, ?MSG_SET_CACHE_LIMITS(SubChunkCacheLimitBytes, VDFQueueLimit)).

-spec garbage_collect(Worker :: pid()) -> ok.
garbage_collect(Worker) ->
	gen_server:cast(Worker, ?MSG_GARBAGE_COLLECT).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init({Partition, PackingDifficulty}) ->
	Name = name(Partition, PackingDifficulty),
	?LOG_DEBUG([{event, mining_debug_worker_started},
		{worker, Name}, {pid, self()}, {partition, Partition}]),
	ChuckCache = ar_chunk_cache:new(),
	State0 = #state{
		name = Name,
		chunk_cache = ChuckCache,
		partition_number = Partition,
		is_pool_client = ar_pool:is_client(),
		packing_difficulty = PackingDifficulty
	},
	report_chunk_cache_metrics(State0),
	gen_server:cast(self(), ?MSG_HANDLE_TASK),
	gen_server:cast(self(), ?MSG_CHECK_WORKER_STATUS),
	{ok, State0}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(?MSG_SET_CACHE_LIMITS(SubChunkCacheLimitBytes, VDFQueueLimit), State) ->
	{noreply, State#state{
		%% TODO: Convert to bytes
		chunk_cache = ar_chunk_cache:set_limit(SubChunkCacheLimitBytes, State#state.chunk_cache),
		vdf_queue_limit = VDFQueueLimit
	}};

handle_cast(?MSG_SET_DIFFICULTY(DiffPair), State) ->
	{noreply, State#state{ diff_pair = DiffPair }};

handle_cast(?MSG_RESET_DIFFICULTY(DiffPair), State) ->
	State2 = update_sessions([], State),
	{noreply, State2#state{ diff_pair = DiffPair }};

handle_cast(?MSG_SET_SESSIONS(ActiveSessions), State) ->
	{noreply, update_sessions(ActiveSessions, State)};

handle_cast(?MSG_CHUNKS_READ(WhichChunk, Candidate, RangeStart, ChunkOffsets), State) ->
	case is_session_valid(State, Candidate) of
		true ->
			State2 = process_chunks(WhichChunk, Candidate, RangeStart, ChunkOffsets, State),
			{noreply, State2};
		false ->
			?LOG_DEBUG([{event, mining_debug_add_stale_chunks},
				{worker, State#state.name},
				{active_sessions,
					ar_mining_server:encode_sessions(ar_chunk_cache:get_groups(State#state.chunk_cache))},
				{candidate_session,
					ar_nonce_limiter:encode_session_key(Candidate#mining_candidate.session_key)},
				{partition_number, Candidate#mining_candidate.partition_number},
				{step_number, Candidate#mining_candidate.step_number}]),
			{noreply, State}
	end;

handle_cast(?MSG_ADD_TASK(TaskType, Candidate, _ExtraArgs) = Task, State) ->
	case is_session_valid(State, Candidate) of
		true ->
			{noreply, add_task(Task, State)};
		false ->
			?LOG_DEBUG([{event, mining_debug_add_stale_task},
				{worker, State#state.name},
				{task, TaskType},
				{active_sessions,
					ar_mining_server:encode_sessions(ar_chunk_cache:get_groups(State#state.chunk_cache))},
				{candidate_session,
					ar_nonce_limiter:encode_session_key(Candidate#mining_candidate.session_key)},
				{partition_number, Candidate#mining_candidate.partition_number},
				{step_number, Candidate#mining_candidate.step_number},
				{nonce, Candidate#mining_candidate.nonce}]),
			{noreply, State}
	end;

handle_cast(?MSG_HANDLE_TASK, #state{ task_queue = Q } = State) ->
	case gb_sets:is_empty(Q) of
		true ->
			ar_util:cast_after(?TASK_CHECK_FREQUENCY_MS, self(), handle_task),
			{noreply, State};
		_ ->
			{{_Priority, _ID, Task}, Q2} = gb_sets:take_smallest(Q),
			{TaskType, Candidate, _ExtraArgs} = Task,
			prometheus_gauge:dec(mining_server_task_queue_len, [TaskType]),
			gen_server:cast(self(), handle_task),
			case is_session_valid(State, Candidate) of
				true ->
					handle_task(Task, State#state{ task_queue = Q2 });
				false ->
					?LOG_DEBUG([{event, mining_debug_handle_stale_task},
						{worker, State#state.name},
						{task, TaskType},
						{active_sessions,
							ar_mining_server:encode_sessions(ar_chunk_cache:get_groups(State#state.chunk_cache))},
						{candidate_session, ar_nonce_limiter:encode_session_key(
							Candidate#mining_candidate.session_key)},
						{partition_number, Candidate#mining_candidate.partition_number},
						{step_number, Candidate#mining_candidate.step_number},
						{nonce, Candidate#mining_candidate.nonce}]),
					{noreply, State}
			end
	end;

handle_cast(?MSG_REMOVE_SUB_CHUNKS_FROM_CACHE(SubChunkCount, Candidate), State) ->
	{noreply, remove_sub_chunks_from_cache(Candidate, SubChunkCount, State)};

handle_cast(?MSG_CHECK_WORKER_STATUS, State) ->
	maybe_warn_about_lag(State#state.task_queue, State#state.name),
	ar_util:cast_after(?STATUS_CHECK_FREQUENCY_MS, self(), check_worker_status),
	{noreply, State};

handle_cast(?MSG_GARBAGE_COLLECT, State) ->
	erlang:garbage_collect(self(), [{async, erlang:monotonic_time()}]),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(?MSG_GARBAGE_COLLECT(StartTime, GCResult), State) ->
	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime-StartTime, native, millisecond),
	case GCResult == false orelse ElapsedTime > ?GC_LOG_THRESHOLD of
		true ->
			?LOG_DEBUG([
				{event, mining_debug_garbage_collect}, {process, State#state.name}, {pid, self()},
				{gc_time, ElapsedTime}, {gc_result, GCResult}]);
		false ->
			ok
	end,
	{noreply, State};

handle_info(?MSG_FETCHED_LAST_MOMENT_PROOF(_), State) ->
    %% This is a no-op to handle "slow" response from peers that were queried by `fetch_poa_from_peers`
    %% Only the first peer to respond with a PoA will be handled, all other responses will fall through to here
    %% an be ignored.
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Mining tasks.
%%%===================================================================
add_task(?MSG_ADD_TASK(TaskType, Candidate, _ExtraArgs) = Task, State) ->
	#state{ task_queue = Q } = State,
	StepNumber = Candidate#mining_candidate.step_number,
	Q2 = gb_sets:insert({priority(TaskType, StepNumber), make_ref(), Task}, Q),
	prometheus_gauge:inc(mining_server_task_queue_len, [TaskType]),
	State#state{ task_queue = Q2 }.

process_chunks(WhichChunk, Candidate, RangeStart, ChunkOffsets, State) ->
	PackingDifficulty = Candidate#mining_candidate.packing_difficulty,
	MaxNonce = ar_block:get_max_nonce(PackingDifficulty),
	SubChunkSize = ar_block:get_sub_chunk_size(PackingDifficulty),
	NoncesPerChunk = ar_block:get_nonces_per_chunk(PackingDifficulty),
	process_chunks(WhichChunk, Candidate, RangeStart, 0, NoncesPerChunk, MaxNonce,
			ChunkOffsets, SubChunkSize, 0, State).

process_chunks(WhichChunk, Candidate, _RangeStart, Nonce, _NoncesPerChunk, NonceMax,
		_ChunkOffsets, _SubChunkSize, Count, State) when Nonce > NonceMax ->
	Partition = case WhichChunk of
		chunk1 ->
			Candidate#mining_candidate.partition_number;
		chunk2 ->
			Candidate#mining_candidate.partition_number2
	end,
	ar_mining_stats:chunks_read(Partition, Count),
	State;
process_chunks(WhichChunk, Candidate, RangeStart, Nonce, NoncesPerChunk, NonceMax,
		[], SubChunkSize, Count, State) ->
	%% No more ChunkOffsets means no more chunks have been read. Iterate through all the
	%% remaining nonces and remove the full chunks from the cache.
	gen_server:cast(self(),
			{remove_sub_chunks_from_cache, NoncesPerChunk,
				Candidate#mining_candidate{ nonce = Nonce }}),
	process_chunks(WhichChunk, Candidate, RangeStart, Nonce + NoncesPerChunk, NoncesPerChunk,
			NonceMax, [], SubChunkSize, Count, State);
process_chunks(WhichChunk, Candidate, RangeStart, Nonce, NoncesPerChunk, NonceMax,
		[{EndOffset, Chunk} | ChunkOffsets], SubChunkSize, Count, State)
		when RangeStart + Nonce * SubChunkSize < EndOffset - ?DATA_CHUNK_SIZE ->
	%% Nonce falls in a chunk which wasn't read from disk (e.g. because there are holes
	%% in the recall range).
	gen_server:cast(self(),
			{remove_sub_chunks_from_cache, NoncesPerChunk,
				Candidate#mining_candidate{ nonce = Nonce }}),
	process_chunks(WhichChunk, Candidate, RangeStart, Nonce + NoncesPerChunk, NoncesPerChunk,
			NonceMax, [{EndOffset, Chunk} | ChunkOffsets], SubChunkSize, Count, State);
process_chunks(WhichChunk, Candidate, RangeStart, Nonce, NoncesPerChunk, NonceMax,
		[{EndOffset, _Chunk} | ChunkOffsets], SubChunkSize, Count, State)
		when RangeStart + Nonce * SubChunkSize >= EndOffset ->
	%% Nonce falls in a chunk beyond the current chunk offset. Move ahead to the next
	%% chunk offset.
	process_chunks(WhichChunk, Candidate, RangeStart, Nonce, NoncesPerChunk, NonceMax,
			ChunkOffsets, SubChunkSize, Count, State);
process_chunks(WhichChunk, Candidate, RangeStart, Nonce, NoncesPerChunk, NonceMax,
		[{_EndOffset, Chunk} | ChunkOffsets], SubChunkSize, Count, State) ->
	%% Process all sub-chunks in Chunk, and then advance to the next chunk.
	State2 = process_all_sub_chunks(WhichChunk, Chunk, Candidate, Nonce, State),
	process_chunks(
		WhichChunk, Candidate, RangeStart, Nonce + NoncesPerChunk, NoncesPerChunk, NonceMax,
		ChunkOffsets, SubChunkSize, Count + 1, State2).

process_all_sub_chunks(_WhichChunk, <<>>, _Candidate, _Nonce, State) ->
	State;
process_all_sub_chunks(WhichChunk, Chunk, Candidate, Nonce, State) ->
	{SubChunk, Rest} = extract_sub_chunk(Chunk, Candidate),
	Candidate2 = Candidate#mining_candidate{ nonce = Nonce },
	State2 = process_sub_chunk(WhichChunk, Candidate2, SubChunk, State),
	process_all_sub_chunks(WhichChunk, Rest, Candidate2, Nonce + 1, State2);
process_all_sub_chunks(WhichChunk, Rest, _Candidate, Nonce, State) ->
	?LOG_ERROR([{event, failed_to_split_chunk_into_sub_chunks},
			{remaining_size, byte_size(Rest)},
			{nonce, Nonce},
			{chunk, WhichChunk}]),
	State.

%% @doc Return the sub-chunk and the remaining bytes in the chunk. For spora_2_6 packing
%% (aka difficulty 0), each sub-chunk is the size of a chunk so there are no remaining
%% bytes.
extract_sub_chunk(Chunk, #mining_candidate{ packing_difficulty = 0 }) ->
	{Chunk, <<>>};
extract_sub_chunk(Chunk, _Candidate) ->
	<< SubChunk:?COMPOSITE_PACKING_SUB_CHUNK_SIZE/binary, Rest/binary >> = Chunk,
	{SubChunk, Rest}.

process_sub_chunk(chunk1, Candidate, SubChunk, State) ->
	ar_mining_hash:compute_h1(self(), Candidate#mining_candidate{ chunk1 = SubChunk }),
	State;
process_sub_chunk(chunk2, Candidate, SubChunk, State) ->
	#mining_candidate{ session_key = SessionKey } = Candidate,
	Candidate2 = Candidate#mining_candidate{ chunk2 = SubChunk },
	case cycle_sub_chunk_cache(Candidate2, SubChunk, #{chunk2 => true}, State) of
		{{<<>>, #{chunk1 := true, h1 := H1}}, State2} ->
			ar_mining_hash:compute_h2(self(), Candidate2#mining_candidate{ h1 = H1 }),
			%% Decrement 1 for chunk2:
			%% we're computing h2 for a peer so chunk1 was not previously read or cached
			%% on this node
			State2;
		{Chunk, #{chunk1 := true, h1 := H1}, State2} ->
			ar_mining_hash:compute_h2(
				self(), Candidate2#mining_candidate{ chunk1 = Chunk, h1 = H1 }),
			%% Decrement 2 for chunk1 and chunk2:
			%% 1. chunk1 was previously read and cached
			%% 2. chunk2 that was just read and will shortly be used to compute h2
			State2;
		{{<<>>, _ChunkMeta}, State2} ->
			%% Decrement 1 for chunk2
			%% do_not_cache indicates chunk1 was not and will not be read or cached
			State2;
		{cached, State2} ->
			case Candidate2#mining_candidate.cm_lead_peer of
				not_set ->
					ok;
				_ ->
					?LOG_ERROR([{event, cm_chunk2_cached_before_chunk1},
						{worker, State#state.name},
						{partition_number, Candidate2#mining_candidate.partition_number},
						{partition_number2, Candidate2#mining_candidate.partition_number2},
						{cm_peer, ar_util:format_peer(Candidate2#mining_candidate.cm_lead_peer)},
						{cache_ref, Candidate2#mining_candidate.cache_ref},
						{nonce, Candidate2#mining_candidate.nonce},
						{session, ar_nonce_limiter:encode_session_key(SessionKey)}])
			end,
			State2
	end.

priority(computed_h2, StepNumber) ->
	{1, -StepNumber};
priority(computed_h1, StepNumber) ->
	{2, -StepNumber};
priority(compute_h2_for_peer, StepNumber) ->
	{2, -StepNumber};
priority(chunk2, StepNumber) ->
	{3, -StepNumber};
priority(chunk1, StepNumber) ->
	{4, -StepNumber};
priority(computed_h0, StepNumber) ->
	{5, -StepNumber};
priority(compute_h0, StepNumber) ->
	{6, -StepNumber}.

handle_task({chunk1, Candidate, [RangeStart, ChunkOffsets]}, State) ->
	State2 = process_chunks(chunk1, Candidate, RangeStart, ChunkOffsets, State),
	{noreply, State2};

handle_task({chunk2, Candidate, [RangeStart, ChunkOffsets]}, State) ->
	State2 = process_chunks(chunk2, Candidate, RangeStart, ChunkOffsets, State),
	{noreply, State2};

handle_task({compute_h0, Candidate, _ExtraArgs}, State) ->
	#state{ latest_vdf_step_number = LatestVDFStepNumber,
			vdf_queue_limit = VDFQueueLimit } = State,
	#mining_candidate{ session_key = SessionKey, step_number = StepNumber } = Candidate,
	State2 = report_hashes(State),
	State4 = case try_to_reserve_cache_space(SessionKey, State2) of
		{true, State3} ->
			ar_mining_hash:compute_h0(self(), Candidate),
			case StepNumber > LatestVDFStepNumber of
				true ->
					State3#state{ latest_vdf_step_number = StepNumber };
				false ->
					State3
			end;
		false ->
			case StepNumber >= LatestVDFStepNumber - VDFQueueLimit of
				true ->
					%% Wait a bit, and then re-add the task.
					add_delayed_task(self(), compute_h0, Candidate);
				false ->
					ok
			end,
			State2
	end,
	{noreply, State4};

handle_task({computed_h0, Candidate, _ExtraArgs}, State) ->
	#mining_candidate{ h0 = H0, partition_number = Partition1,
				partition_upper_bound = PartitionUpperBound } = Candidate,
	{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
			Partition1, PartitionUpperBound),
	Partition2 = ar_node:get_partition_number(RecallRange2Start),
	Candidate2 = Candidate#mining_candidate{ partition_number2 = Partition2 },
	Candidate3 = generate_cache_ref(Candidate2),
	Range1Exists = ar_mining_io:read_recall_range(
			chunk1, self(), Candidate3, RecallRange1Start),
	State3 = case Range1Exists of
		true ->
			Range2Exists = ar_mining_io:read_recall_range(
					chunk2, self(), Candidate3, RecallRange2Start),
			case Range2Exists of
				true ->
					State;
				false ->
					%% Release just the Range2 cache space we reserved with
					%% try_to_reserve_cache_space/2
					do_not_cache(Candidate3, State)
			end;
		false ->
			%% Release the Range1 *and* Range2 cache space we reserved with
			%% try_to_reserve_cache_space/2
			State
	end,
	{noreply, State3};

handle_task({computed_h1, Candidate, _ExtraArgs}, State) ->
	#mining_candidate{ h1 = H1, chunk1 = Chunk1 } = Candidate,
	State2 = hash_computed(h1, Candidate, State),
	case h1_passes_diff_checks(H1, Candidate, State2) of
		true ->
			?LOG_INFO([{event, found_h1_solution},
				{step, Candidate#mining_candidate.step_number},
				{worker, State2#state.name},
				{h1, ar_util:encode(H1)},
				{p1, Candidate#mining_candidate.partition_number},
				{difficulty, get_difficulty(State2, Candidate)}]),
			ar_mining_stats:h1_solution(),
			%% Decrement 1 for chunk1:
			%% Since we found a solution we won't need chunk2 (and it will be evicted if
			%% necessary below)
			State3 = remove_sub_chunks_from_cache(Candidate, 1, State2),
			ar_mining_server:prepare_and_post_solution(Candidate),
			{noreply, State3};
		Result ->
			case Result of
				partial ->
					ar_mining_server:prepare_and_post_solution(Candidate);
				_ ->
					ok
			end,
			{ok, Config} = application:get_env(arweave, config),
			case cycle_sub_chunk_cache(Candidate, Chunk1, #{chunk1 => true, h1 => H1}, State2) of
				{cached, State3} ->
					%% Chunk2 hasn't been read yet, so we cache Chunk1 and wait for
					%% Chunk2 to be read.
					{noreply, State3};
				{do_not_cache, State3} ->
					%% This node does not store Chunk2. If we're part of a coordinated
					%% mining set, we can try one of our peers, otherwise we're done.
					case Config#config.coordinated_mining of
						false ->
							ok;
						true ->
							DiffPair =
								case get_partial_difficulty(State3, Candidate) of
									not_set ->
										get_difficulty(State3, Candidate);
									PartialDiffPair ->
										PartialDiffPair
								end,
							ar_coordination:computed_h1(Candidate, DiffPair)
					end,
					%% Decrement 1 for chunk1:
					%% do_not_cache indicates chunk2 was not and will not be read or cached
					{noreply, State3};
				{{chunk2, Chunk2}, State3} ->
					%% Chunk2 has already been read, so we can compute H2 now.
					ar_mining_hash:compute_h2(
						self(), Candidate#mining_candidate{ chunk2 = Chunk2 }),
					%% Decrement 2 for chunk1 and chunk2:
					%% 1. chunk2 was previously read and cached
					%% 2. chunk1 that was just read and used to compute H1
					{noreply, State3}
			end
	end;

handle_task({computed_h2, Candidate, _ExtraArgs}, State) ->
	#mining_candidate{ chunk2 = Chunk2, h2 = H2, cm_lead_peer = Peer } = Candidate,
	State2 = hash_computed(h2, Candidate, State),
	PassesDiffChecks = h2_passes_diff_checks(H2, Candidate, State2),
	case PassesDiffChecks of
		false ->
			ok;
		true ->
			?LOG_INFO([{event, found_h2_solution},
					{worker, State#state.name},
					{step, Candidate#mining_candidate.step_number},
					{h2, ar_util:encode(H2)},
					{p1, Candidate#mining_candidate.partition_number},
					{p2, Candidate#mining_candidate.partition_number2},
					{difficulty, get_difficulty(State2, Candidate)},
					{partial_difficulty, get_partial_difficulty(State2, Candidate)}]),
			ar_mining_stats:h2_solution();
		partial ->
			?LOG_INFO([{event, found_h2_partial_solution},
					{worker, State2#state.name},
					{step, Candidate#mining_candidate.step_number},
					{h2, ar_util:encode(H2)},
					{p1, Candidate#mining_candidate.partition_number},
					{p2, Candidate#mining_candidate.partition_number2},
					{partial_difficulty, get_partial_difficulty(State2, Candidate)}])
	end,
	case {PassesDiffChecks, Peer} of
		{false, _} ->
			ok;
		{_, not_set} ->
			ar_mining_server:prepare_and_post_solution(Candidate);
		_ ->
			PoA2 = case ar_mining_server:prepare_poa(poa2, Candidate, #poa{}) of
				{ok, PoA} ->
					PoA;
				{error, _Error} ->
					%% Fallback. This will probably fail later, but prepare_poa/3 should
					%% have already printed several errors so we'll continue just in case.
					#poa{ chunk = Chunk2 }
			end,
			ar_coordination:computed_h2_for_peer(Candidate#mining_candidate{ poa2 = PoA2 })
	end,
	{noreply, State2};

handle_task({compute_h2_for_peer, Candidate, _ExtraArgs}, State) ->
	#mining_candidate{
		h0 = H0,
		partition_number = Partition1,
		partition_upper_bound = PartitionUpperBound,
		cm_h1_list = H1List,
		cm_lead_peer = Peer
	} = Candidate,

	{_RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
					Partition1, PartitionUpperBound),
	Candidate2 = generate_cache_ref(Candidate),
	%% Clear the list so we aren't copying it around all over the place
	Candidate3 = Candidate2#mining_candidate{ cm_h1_list = [] },

	Range2Exists = ar_mining_io:read_recall_range(chunk2, self(), Candidate3, RecallRange2Start),
	case Range2Exists of
		true ->
			ar_mining_stats:h1_received_from_peer(Peer, length(H1List)),
			%% Note: when processing CM requests we always reserve the cache space and proceed
			%% *even if* this puts us over the chunk cache limit. This may have to be
			%% revisited later if we find that this causes unacceptable memory bloat.
			State2 = State,
			%% First flag all nonces in the range as do_not_cache, then cache the specific
			%% nonces included in the H1 list. This will make sure we don't cache the chunk2s
			%% that are read for the missing nonces.
			State3 = do_not_cache(Candidate3, State2),
			{noreply, cache_h1_list(Candidate3, H1List, State3)};
		false ->
			%% This can happen for two reasons:
			%% 1. (most common) Remote peer has requested a range we don't have from a
			%%    partition that we do have.
			%% 2. (rare, but possible) Remote peer has an outdated partition table and we
			%%    don't even have the requested partition.
			{noreply, State}
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Returns true if the mining candidate belongs to a valid mining session. Always assume
%% that a coordinated mining candidate is valid (its cm_lead_peer is set)
is_session_valid(_State, #mining_candidate{ cm_lead_peer = Peer })
		when Peer /= not_set ->
	true;
is_session_valid(State, #mining_candidate{ session_key = SessionKey }) ->
	ar_chunk_cache:group_exists(SessionKey, State#state.chunk_cache).

h1_passes_diff_checks(H1, Candidate, State) ->
	passes_diff_checks(H1, true, Candidate, State).

h2_passes_diff_checks(H2, Candidate, State) ->
	passes_diff_checks(H2, false, Candidate, State).

passes_diff_checks(SolutionHash, IsPoA1, Candidate, State) ->
	DiffPair = get_difficulty(State, Candidate),
	#mining_candidate{ packing_difficulty = PackingDifficulty } = Candidate,
	case ar_node_utils:passes_diff_check(SolutionHash, IsPoA1, DiffPair, PackingDifficulty) of
		true ->
			true;
		false ->
			case get_partial_difficulty(State, Candidate) of
				not_set ->
					false;
				PartialDiffPair ->
					case ar_node_utils:passes_diff_check(SolutionHash, IsPoA1,
							PartialDiffPair, PackingDifficulty) of
						true ->
							partial;
						false ->
							false
					end
			end
	end.

maybe_warn_about_lag(Q, Name) ->
	case gb_sets:is_empty(Q) of
		true ->
			ok;
		false ->
			case gb_sets:take_smallest(Q) of
				{{_Priority, _ID, {compute_h0, _}}, Q3} ->
					%% Since we sample the queue asynchronously, we expect there to regularly
					%% be a queue of length 1 (i.e. a task may have just been added to the
					%% queue when we run this check).
					%%
					%% To further reduce log spam, we'll only warn if the queue is greater
					%% than 2. We really only care if a queue is consistently long or if
					%% it's getting longer. Temporary blips are fine. We may incrase
					%% the threshold in the future.
					N = count_h0_tasks(Q3) + 1,
					case N > 2 of
						true ->
							?LOG_WARNING([
								{event, mining_worker_lags_behind_the_nonce_limiter},
								{worker, Name},
								{step_count, N}]);
						false ->
							ok
					end;
				_ ->
					ok
			end
	end.

count_h0_tasks(Q) ->
	case gb_sets:is_empty(Q) of
		true ->
			0;
		false ->
			case gb_sets:take_smallest(Q) of
				{{_Priority, _ID, {compute_h0, _Args}}, Q2} ->
					1 + count_h0_tasks(Q2);
				_ ->
					0
			end
	end.

update_sessions(ActiveSessions, State) ->
	CurrentSessions = ar_chunk_cache:get_groups(State#state.chunk_cache),
	AddedSessions = lists:subtract(ActiveSessions, CurrentSessions),
	RemovedSessions = lists:subtract(CurrentSessions, ActiveSessions),
	add_sessions(AddedSessions, remove_sessions(RemovedSessions, State)).

add_sessions([], State) ->
	State;
add_sessions([SessionKey | AddedSessions], State) ->
	ChunkCache = ar_chunk_cache:add_group(SessionKey, State#state.chunk_cache),
	?LOG_DEBUG([{event, mining_debug_add_session},
		{worker, State#state.name}, {partition, State#state.partition_number},
		{session_key, ar_nonce_limiter:encode_session_key(SessionKey)}]),
	add_sessions(AddedSessions, State#state{
		chunk_cache = ChunkCache
	}).

remove_sessions([], State) ->
	State;
remove_sessions([SessionKey | RemovedSessions], State) ->
	ChuckCache = ar_chunk_cache:drop_group(SessionKey, State#state.chunk_cache),
	TaskQueue = remove_tasks(SessionKey, State#state.task_queue),
	?LOG_DEBUG([{event, mining_debug_remove_session},
		{worker, State#state.name}, {partition, State#state.partition_number},
		{session, ar_nonce_limiter:encode_session_key(SessionKey)}]),
	remove_sessions(RemovedSessions, State#state{
		task_queue = TaskQueue,
		chunk_cache = ChuckCache
	}).

remove_tasks(SessionKey, TaskQueue) ->
	gb_sets:filter(
		fun({_Priority, _ID, {TaskType, Candidate, _ExtraArgs}}) ->
			case Candidate#mining_candidate.session_key == SessionKey of
				true ->
					prometheus_gauge:dec(mining_server_task_queue_len, [TaskType]),
					%% remove the task
					false;
				false ->
					%% keep the task
					true
			end
		end,
		TaskQueue
	).

try_to_reserve_cache_space(SessionKey, #state{
	packing_difficulty = PackingDifficulty,
	chunk_cache = ChunkCache0
} = State) ->
	case ar_chunk_cache:reserve(SessionKey, ar_block:get_recall_range_size(PackingDifficulty), ChunkCache0) of
		{ok, ChunkCache1} -> {true, State#state{ chunk_cache = ChunkCache1 }};
		{error, Reason} ->
			?LOG_WARNING([{event, mining_worker_failed_to_reserve_cache_space},
				{worker, State#state.name}, {partition, State#state.partition_number},
				{session_key, ar_nonce_limiter:encode_session_key(SessionKey)},
				{reason, Reason}]),
			false
	end.

do_not_cache(Candidate, State) ->
	#mining_candidate{ packing_difficulty = PackingDifficulty } = Candidate,
	do_not_cache(0, ar_block:get_max_nonce(PackingDifficulty), Candidate, State).

do_not_cache(Nonce, NonceMax, _Candidate, State)
		when Nonce > NonceMax ->
	State;
do_not_cache(Nonce, NonceMax, Candidate, State) ->
	State2 = cache_chunk(<<>>, Candidate#mining_candidate{ nonce = Nonce }, State),
	do_not_cache(Nonce + 1, NonceMax, Candidate, State2).

%% @doc The sub_chunk_cache stores either the first or second chunk for a given nonce. This is
%% because we process both the first and second recall ranges in parallel and don't know
%% which data will be available first. The function manages that shared cache slot by either
%% caching data if its the first to arrive, or "popping" data that was previously cached. The
%% caller is responsible for taking the appropriate action based on the return value.
%%
%% do_not_cache is a special value used to prevent unnecessary data from being cached once a
%% solution has been found for a given nonce.
cycle_sub_chunk_cache(#mining_candidate{ cache_ref = CacheRef } = Candidate, Chunk, ChunkMeta, State)
  		when CacheRef /= not_set ->
	#mining_candidate{ nonce = Nonce, session_key = SessionKey } = Candidate,
	case ar_chunk_cache:take_chunk(SessionKey, {CacheRef, Nonce}, State#state.chunk_cache) of
		{ok, {<<>>, ChunkCache1}} ->
			ChunkCache2 = ar_chunk_cache:add_chunk(SessionKey, {CacheRef, Nonce}, {Chunk, ChunkMeta}, ChunkCache1),
			{do_not_cache, State#state{ chunk_cache = ChunkCache2 }};
		{error, chunk_not_found} ->
			ChunkCache2 = ar_chunk_cache:add_chunk(SessionKey, {CacheRef, Nonce}, {Chunk, ChunkMeta}, State#state.chunk_cache),
			{cached, State#state{ chunk_cache = ChunkCache2 }};
		{ok, {Data, ChunkCache1}} ->
			{Data, State#state{ chunk_cache = ChunkCache1 }}
	end.

%% @doc Remove SubChunkCount sub-chunks from the cache starting at
%% Candidate#mining_candidate.nonce.
remove_sub_chunks_from_cache(_Candidate, 0, State) ->
	State;
remove_sub_chunks_from_cache(#mining_candidate{ cache_ref = CacheRef } = Candidate,
		SubChunkCount, State) when CacheRef /= not_set ->
	#mining_candidate{ nonce = Nonce, session_key = SessionKey } = Candidate,
	%% Decrement the cache size by 1 for each sub-chunk being removed.
	%% We may decrement the cache size further depending on what's already cached.
	State2 = State,
	State3 = case ar_chunk_cache:take_chunk(SessionKey, {CacheRef, Nonce}, State#state.chunk_cache) of
		{ok, {{<<>>, _}, ChunkCache1}} ->
			State2#state{ chunk_cache = ChunkCache1 };
		{error, chunk_not_found} ->
			cache_chunk(<<>>, Candidate, State2);
		{{_Chunk, #{chunk1 := true, h1 := _H1}}, ChunkCache1} ->
			%% If we find data from a CM peer, discard it but don't decrement the cache size
			State2#state{ chunk_cache = ChunkCache1 };
		{_, ChunkCache1} ->
			%% if we find any cached data, discard it and decrement the cache size
			State2#state{ chunk_cache = ChunkCache1 }
	end,
	remove_sub_chunks_from_cache(Candidate#mining_candidate{ nonce = Nonce + 1 },
		SubChunkCount - 1, State3).

cache_chunk(Data, Candidate, State) ->
	#mining_candidate{ cache_ref = CacheRef, nonce = Nonce, session_key = SessionKey } = Candidate,
	State#state{
		chunk_cache = ar_chunk_cache:add_chunk(SessionKey, {CacheRef, Nonce}, Data, State#state.chunk_cache)
	}.

cache_h1_list(_Candidate, [], State) ->
	State;
cache_h1_list(
		#mining_candidate{ cache_ref = CacheRef } = Candidate,
		[ {H1, Nonce} | H1List ], State) when CacheRef /= not_set ->
	State2 = cache_chunk({<<>>, #{chunk1 => true, h1 => H1}}, Candidate#mining_candidate{ nonce = Nonce }, State),
	cache_h1_list(Candidate, H1List, State2).

get_difficulty(State, #mining_candidate{ cm_diff = not_set }) ->
	State#state.diff_pair;
get_difficulty(_State, #mining_candidate{ cm_diff = DiffPair }) ->
	DiffPair.

get_partial_difficulty(#state{ is_pool_client = false }, _Candidate) ->
	not_set;
get_partial_difficulty(_State, #mining_candidate{ cm_diff = DiffPair }) ->
	DiffPair.

generate_cache_ref(Candidate) ->
	#mining_candidate{
		partition_number = Partition1, partition_number2 = Partition2,
		partition_upper_bound = PartitionUpperBound } = Candidate,
	CacheRef = {Partition1, Partition2, PartitionUpperBound, make_ref()},
	Candidate#mining_candidate{ cache_ref = CacheRef }.

hash_computed(WhichHash, Candidate, State) ->
	case WhichHash of
		h1 ->
			PartitionNumber = Candidate#mining_candidate.partition_number,
			Hashes = maps:get(PartitionNumber, State#state.h1_hashes, 0),
			State#state{ h1_hashes = maps:put(PartitionNumber, Hashes+1, State#state.h1_hashes) };
		h2 ->
			PartitionNumber = Candidate#mining_candidate.partition_number2,
			Hashes = maps:get(PartitionNumber, State#state.h2_hashes, 0),
			State#state{ h2_hashes = maps:put(PartitionNumber, Hashes+1, State#state.h2_hashes) }
	end.

report_hashes(State) ->
	maps:foreach(
        fun(Key, Value) ->
            ar_mining_stats:h1_computed(Key, Value)
        end,
        State#state.h1_hashes
    ),
	maps:foreach(
        fun(Key, Value) ->
            ar_mining_stats:h2_computed(Key, Value)
        end,
        State#state.h2_hashes
    ),
	State#state{ h1_hashes = #{}, h2_hashes = #{} }.

report_chunk_cache_metrics(State0) ->
	ChunkCache = State0#state.chunk_cache,
	Partition = State0#state.partition_number,
	prometheus_gauge:set(mining_server_chunk_cache_size, [Partition], ar_chunk_cache:cache_size(ChunkCache)).

%%%===================================================================
%%% Public Test interface.
%%%===================================================================
