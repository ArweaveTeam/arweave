-module(ar_mining_worker).

-behaviour(gen_server).

-export([start_link/2, name/2, reset_mining_session/2, set_sessions/2, chunks_read/5, computed_hash/5,
		set_difficulty/2, set_cache_limits/3, add_task/3, garbage_collect/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("arweave/include/ar_mining_cache.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	name = not_set,
	partition_number = not_set,
	diff_pair = not_set,
	packing_difficulty = 0,
	task_queue = gb_sets:new(),
	chunk_cache = undefined,
	vdf_queue_limit = 0,
	latest_vdf_step_number = 0,
	is_pool_client = false,
	h1_hashes = #{},
	h2_hashes = #{}
}).

-define(TASK_CHECK_FREQUENCY_MS, 200).
-define(STATUS_CHECK_FREQUENCY_MS, 5000).

-define(VDF_STEP_CACHE_KEY(CacheRef, Nonce), {CacheRef, Nonce}).

%%%===================================================================
%%% Messages
%%%===================================================================

-define(MSG_RESET_MINING_SESSION(DiffPair), {reset_mining_session, DiffPair}).
-define(MSG_SET_SESSIONS(ActiveSessions), {set_sessions, ActiveSessions}).
-define(MSG_ADD_TASK(Task), {add_task, Task}).
-define(MSG_CHUNKS_READ(WhichChunk, Candidate, RangeStart, ChunkOffsets), {chunks_read, {WhichChunk, Candidate, RangeStart, ChunkOffsets}}).
-define(MSG_SET_DIFFICULTY(DiffPair), {set_difficulty, DiffPair}).
-define(MSG_SET_CACHE_LIMITS(SubchunkCacheLimitBytes, VDFQueueLimit), {set_cache_limits, SubchunkCacheLimitBytes, VDFQueueLimit}).
-define(MSG_REMOVE_SUB_CHUNKS_FROM_CACHE(SubchunkCount, Candidate), {remove_subchunks_from_cache, SubchunkCount, Candidate}).
-define(MSG_CHECK_WORKER_STATUS, {check_worker_status}).
-define(MSG_HANDLE_TASK, {handle_task}).
-define(MSG_GARBAGE_COLLECT(StartTime, GCResult), {garbage_collect, StartTime, GCResult}).
-define(MSG_GARBAGE_COLLECT, {garbage_collect}).
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

-spec reset_mining_session(Worker :: pid(), DiffPair :: {non_neg_integer(), non_neg_integer()}) -> ok.
reset_mining_session(Worker, DiffPair) ->
	gen_server:cast(Worker, ?MSG_RESET_MINING_SESSION(DiffPair)).

-spec set_sessions(Worker :: pid(), ActiveSessions :: [ar_nonce_limiter:session_key()]) -> ok.
set_sessions(Worker, ActiveSessions) ->
	gen_server:cast(Worker, ?MSG_SET_SESSIONS(ActiveSessions)).

-spec add_task(Worker :: pid(), TaskType :: atom(), Candidate :: #mining_candidate{}) -> ok.
add_task(Worker, TaskType, Candidate) ->
	add_task(Worker, TaskType, Candidate, []).

-spec add_task(Worker :: pid(), TaskType :: atom(), Candidate :: #mining_candidate{}, ExtraArgs :: [term()]) -> ok.
add_task(Worker, TaskType, Candidate, ExtraArgs) ->
	gen_server:cast(Worker, ?MSG_ADD_TASK({TaskType, Candidate, ExtraArgs})).

-spec add_delayed_task(Worker :: pid(), TaskType :: atom(), Candidate :: #mining_candidate{}) -> ok.
add_delayed_task(Worker, TaskType, Candidate) ->
	%% Delay task by random amount between ?TASK_CHECK_FREQUENCY_MS and 2*?TASK_CHECK_FREQUENCY_MS
	%% The reason for the randomization to avoid a glut tasks to all get added at the same time -
	%% in particular when the chunk cache fills up it's possible for all queued compute_h0 tasks
	%% to be delayed at about the same time.
	Delay = rand:uniform(?TASK_CHECK_FREQUENCY_MS) + ?TASK_CHECK_FREQUENCY_MS,
	ar_util:cast_after(Delay, Worker, ?MSG_ADD_TASK({TaskType, Candidate, []})).

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

-spec set_cache_limits(Worker :: pid(), SubchunkCacheLimitBytes :: non_neg_integer(), VDFQueueLimit :: non_neg_integer()) -> ok.
set_cache_limits(Worker, SubchunkCacheLimitBytes, VDFQueueLimit) ->
	gen_server:cast(Worker, ?MSG_SET_CACHE_LIMITS(SubchunkCacheLimitBytes, VDFQueueLimit)).

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
	ChuckCache = ar_mining_cache:new(),
	State0 = #state{
		name = Name,
		chunk_cache = ChuckCache,
		partition_number = Partition,
		is_pool_client = ar_pool:is_client(),
		packing_difficulty = PackingDifficulty
	},
	gen_server:cast(self(), ?MSG_HANDLE_TASK),
	gen_server:cast(self(), ?MSG_CHECK_WORKER_STATUS),
	{ok, report_chunk_cache_metrics(State0)}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(?MSG_SET_CACHE_LIMITS(SubchunkCacheLimitBytes, VDFQueueLimit), State) ->
	State1 = State#state{
		%% TODO: Convert to bytes
		chunk_cache = ar_mining_cache:set_limit(SubchunkCacheLimitBytes, State#state.chunk_cache),
		vdf_queue_limit = VDFQueueLimit
	},
	{noreply, report_chunk_cache_metrics(State1)};

handle_cast(?MSG_SET_DIFFICULTY(DiffPair), State) ->
	State1 = State#state{ diff_pair = DiffPair },
	{noreply, report_chunk_cache_metrics(State1)};

handle_cast(?MSG_RESET_MINING_SESSION(DiffPair), State) ->
	State1 = update_sessions([], State),
	State2 = State1#state{ diff_pair = DiffPair },
	{noreply, report_chunk_cache_metrics(State2)};

handle_cast(?MSG_SET_SESSIONS(ActiveSessions), State) ->
	State1 = update_sessions(ActiveSessions, State),
	{noreply, report_chunk_cache_metrics(State1)};

handle_cast(?MSG_CHUNKS_READ(WhichChunk, Candidate, RangeStart, ChunkOffsets), State) ->
	case is_session_valid(State, Candidate) of
		true ->
			State1 = process_chunks(WhichChunk, Candidate, RangeStart, ChunkOffsets, State),
			{noreply, report_chunk_cache_metrics(State1)};
		false ->
			?LOG_DEBUG([{event, mining_debug_add_stale_chunks},
				{worker, State#state.name},
				{active_sessions,
					ar_mining_server:encode_sessions(ar_mining_cache:get_sessions(State#state.chunk_cache))},
				{candidate_session,
					ar_nonce_limiter:encode_session_key(Candidate#mining_candidate.session_key)},
				{partition_number, Candidate#mining_candidate.partition_number},
				{step_number, Candidate#mining_candidate.step_number}]),
			{noreply, State}
	end;

handle_cast(?MSG_ADD_TASK({TaskType, Candidate, _ExtraArgs} = Task), State) ->
	case is_session_valid(State, Candidate) of
		true ->
			State1 = add_task(Task, State),
			{noreply, report_chunk_cache_metrics(State1)};
		false ->
			?LOG_DEBUG([{event, mining_debug_add_stale_task},
				{worker, State#state.name},
				{task, TaskType},
				{active_sessions,
					ar_mining_server:encode_sessions(ar_mining_cache:get_sessions(State#state.chunk_cache))},
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
			ar_util:cast_after(?TASK_CHECK_FREQUENCY_MS, self(), ?MSG_HANDLE_TASK),
			{noreply, State};
		_ ->
			gen_server:cast(self(), ?MSG_HANDLE_TASK),
			{{_Priority, _ID, {TaskType, Candidate, _ExtraArgs} = Task}, Q2} = gb_sets:take_smallest(Q),
			prometheus_gauge:dec(mining_server_task_queue_len, [TaskType]),
			case is_session_valid(State, Candidate) of
				true ->
					State1 = handle_task(Task, State#state{ task_queue = Q2 }),
					{noreply, report_chunk_cache_metrics(State1)};
				false ->
					?LOG_DEBUG([{event, mining_debug_handle_stale_task},
						{worker, State#state.name},
						{task, TaskType},
						{active_sessions,
							ar_mining_server:encode_sessions(ar_mining_cache:get_sessions(State#state.chunk_cache))},
						{candidate_session, ar_nonce_limiter:encode_session_key(
							Candidate#mining_candidate.session_key)},
						{partition_number, Candidate#mining_candidate.partition_number},
						{step_number, Candidate#mining_candidate.step_number},
						{nonce, Candidate#mining_candidate.nonce}]),
					{noreply, State}
			end
	end;

handle_cast(?MSG_REMOVE_SUB_CHUNKS_FROM_CACHE(SubchunkCount, Candidate), State) ->
	State1 = remove_subchunks_from_cache(Candidate, SubchunkCount, State),
	{noreply, report_chunk_cache_metrics(State1)};

handle_cast(?MSG_CHECK_WORKER_STATUS, State) ->
	maybe_warn_about_lag(State#state.task_queue, State#state.name),
	ar_util:cast_after(?STATUS_CHECK_FREQUENCY_MS, self(), ?MSG_CHECK_WORKER_STATUS),
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

add_task({TaskType, Candidate, _ExtraArgs} = Task, State) ->
	#state{ task_queue = Q } = State,
	StepNumber = Candidate#mining_candidate.step_number,
	Q2 = gb_sets:insert({priority(TaskType, StepNumber), make_ref(), Task}, Q),
	prometheus_gauge:inc(mining_server_task_queue_len, [TaskType]),
	State#state{ task_queue = Q2 }.

-spec handle_task(
	Task :: {
		EventType :: compute_h0 | computed_h0 | chunk1 | chunk2 | computed_h1 | computed_h2 | compute_h2_for_peer,
		Candidate :: #mining_candidate{},
		ExtraArgs :: term()
	},
	State :: #state{}
) -> State :: #state{}.

%% @doc Handle the `compute_h0` task.
%% Indicates that the VDF step has been computed.
handle_task({compute_h0, Candidate, _ExtraArgs}, State) ->
	#state{
		latest_vdf_step_number = LatestVDFStepNumber,
		vdf_queue_limit = VDFQueueLimit
	} = State,
	#mining_candidate{ step_number = StepNumber } = Candidate,
	State1 = report_and_reset_hashes(State),
	% Check if we need to compute h0 early
	case StepNumber >= LatestVDFStepNumber - VDFQueueLimit of
		true ->
			ar_mining_hash:compute_h0(self(), Candidate),
			State1#state{ latest_vdf_step_number = max(StepNumber, LatestVDFStepNumber) };
		false ->
			State1
	end;

%% @doc Handle the `computed_h0` task.
%% Indicates that the hash for the VDF step has been computed.
handle_task({computed_h0, Candidate, _ExtraArgs}, State) ->
	#mining_candidate{
		h0 = H0, partition_number = Partition1, partition_upper_bound = PartitionUpperBound
	} = Candidate,
	{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0, Partition1, PartitionUpperBound),
	Partition2 = ar_node:get_partition_number(RecallRange2Start),
	Candidate2 = generate_cache_ref(Candidate#mining_candidate{ partition_number2 = Partition2 }),
	%% Check if the recall ranges are readable to avoid reserving cache space for non-existent data.
	Range1Exists = ar_mining_io:is_recall_range_readable(Candidate2, RecallRange1Start),
	Range2Exists = ar_mining_io:is_recall_range_readable(Candidate2, RecallRange2Start),

	case {Range1Exists, Range2Exists} of
		{true, true} ->
			%% Both recall ranges are readable, so we need to reserve cache space for both.
			case try_to_reserve_cache_space(2, Candidate2#mining_candidate.session_key, State) of
				{true, State1} ->
					%% Read the recall ranges; the result of the read will be reported by the `chunk1` and `chunk2` tasks.
					ar_mining_io:read_recall_range(chunk1, self(), Candidate2, RecallRange1Start),
					ar_mining_io:read_recall_range(chunk2, self(), Candidate2, RecallRange2Start),
					State1;
				false ->
					%% We don't have enough cache space to read the recall ranges, so we'll try again later.
					add_delayed_task(self(), computed_h0, Candidate),
					State
			end;
		{true, false} ->
			%% Only the first recall range is readable, so we need to reserve cache space for it.
			case try_to_reserve_cache_space(1, Candidate2#mining_candidate.session_key, State) of
				{true, State1} ->
					%% Read the recall range; the result of the read will be reported by the `chunk1` task.
					ar_mining_io:read_recall_range(chunk1, self(), Candidate2, RecallRange1Start),
					%% Mark chunk2 as missing, not to wait for it to arrive.
					State2 = mark_chunk2_missing(Candidate2, State1),
					State2;
				false ->
					%% We don't have enough cache space to read the recall range, so we'll try again later.
					add_delayed_task(self(), computed_h0, Candidate),
					State
			end;
		{false, _} ->
			State
	end;

%% @doc Handle the `chunk1` task.
%% Indicates that the first recall range has been read.
handle_task({chunk1, Candidate, [RangeStart, ChunkOffsets]}, State) ->
	State1 = process_chunks(chunk1, Candidate, RangeStart, ChunkOffsets, State),
	State1;

%% @doc Handle the `chunk2` task.
%% Indicates that the second recall range has been read.
handle_task({chunk2, Candidate, [RangeStart, ChunkOffsets]}, State) ->
	State1 = process_chunks(chunk2, Candidate, RangeStart, ChunkOffsets, State),
	State1;

%% @doc Handle the `computed_h1` task.
%% Indicates that the single hash for the first recall range has been computed.
handle_task({computed_h1, Candidate, _ExtraArgs}, State) ->
	#mining_candidate{ h1 = H1 } = Candidate,
	State1 = hash_computed(h1, Candidate, State),
	State2 = case h1_passes_diff_checks(H1, Candidate, State1) of
		true ->
			%% h1 solution found, report it.
			?LOG_INFO([{event, found_h1_solution},
				{step, Candidate#mining_candidate.step_number},
				{worker, State1#state.name},
				{h1, ar_util:encode(H1)},
				{p1, Candidate#mining_candidate.partition_number},
				{difficulty, get_difficulty(State1, Candidate)}]),
			ar_mining_server:prepare_and_post_solution(Candidate),
			ar_mining_stats:h1_solution(),
			%% Update the cache to store h1.
			case ar_mining_cache:with_cached_value(
				?VDF_STEP_CACHE_KEY(Candidate#mining_candidate.cache_ref, Candidate#mining_candidate.nonce),
				Candidate#mining_candidate.session_key,
				State1#state.chunk_cache,
				fun(CachedValue) -> {ok, CachedValue#ar_mining_cache_value{h1 = H1}} end
			) of
				{ok, ChunkCache1} -> State1#state{ chunk_cache = ChunkCache1 };
				{error, Reason1} ->
					?LOG_ERROR([{event, mining_worker_failed_to_process_h1},
						{worker, State1#state.name}, {partition, State1#state.partition_number},
						{nonce, Candidate#mining_candidate.nonce},
						{session_key, ar_nonce_limiter:encode_session_key(Candidate#mining_candidate.session_key)},
						{reason, Reason1}]),
					State1
			end;
		partial -> ar_mining_server:prepare_and_post_solution(Candidate);
		_ -> State1
	end,
	%% Check if we need to compute h2.
	{ok, Config} = application:get_env(arweave, config),
	case ar_mining_cache:with_cached_value(
		?VDF_STEP_CACHE_KEY(Candidate#mining_candidate.cache_ref, Candidate#mining_candidate.nonce),
		Candidate#mining_candidate.session_key,
		State2#state.chunk_cache,
		fun
			(#ar_mining_cache_value{chunk2_missing = true}) ->
				%% This node does not store chunk2. If we're part of a coordinated
				%% mining set, we can try one of our peers, otherwise we're done.
				case Config#config.coordinated_mining of
					false -> ok;
					true ->
						DiffPair = case get_partial_difficulty(State2, Candidate) of
								not_set -> get_difficulty(State2, Candidate);
								PartialDiffPair -> PartialDiffPair
							end,
						ar_coordination:computed_h1(Candidate, DiffPair)
				end,
				%% Remove the cached value from the cache.
				{ok, drop};
			(#ar_mining_cache_value{chunk2 = undefined} = CachedValue) ->
				%% chunk2 hasn't been read yet, so we cache chunk1 and wait for it.
				{ok, CachedValue#ar_mining_cache_value{h1 = H1}};
			(#ar_mining_cache_value{chunk2 = Chunk2} = CachedValue) ->
				%% chunk2 has already been read, so we can compute H2 now.
				ar_mining_hash:compute_h2(self(), Candidate#mining_candidate{ chunk2 = Chunk2 }),
				{ok, CachedValue#ar_mining_cache_value{h1 = H1}}
		end
	) of
		{ok, ChunkCache2} -> State2#state{ chunk_cache = ChunkCache2 };
		{error, Reason2} ->
			?LOG_ERROR([{event, mining_worker_failed_to_process_h1},
				{worker, State2#state.name}, {partition, State2#state.partition_number},
				{nonce, Candidate#mining_candidate.nonce},
				{session_key, ar_nonce_limiter:encode_session_key(Candidate#mining_candidate.session_key)},
				{reason, Reason2}]),
			State2
	end;

%% @doc Handle the `computed_h2` task.
%% Indicates that the single hash for the second recall range has been computed.
handle_task({computed_h2, Candidate, _ExtraArgs}, State) ->
	#mining_candidate{ chunk2 = Chunk2, h2 = H2, cm_lead_peer = Peer } = Candidate,
	State1 = hash_computed(h2, Candidate, State),
	PassesDiffChecks = h2_passes_diff_checks(H2, Candidate, State1),
	case PassesDiffChecks of
		false -> ok;
		true ->
			?LOG_INFO([{event, found_h2_solution},
					{worker, State#state.name},
					{step, Candidate#mining_candidate.step_number},
					{h2, ar_util:encode(H2)},
					{p1, Candidate#mining_candidate.partition_number},
					{p2, Candidate#mining_candidate.partition_number2},
					{difficulty, get_difficulty(State1, Candidate)},
					{partial_difficulty, get_partial_difficulty(State1, Candidate)}]),
			ar_mining_stats:h2_solution();
		partial ->
			?LOG_INFO([{event, found_h2_partial_solution},
					{worker, State1#state.name},
					{step, Candidate#mining_candidate.step_number},
					{h2, ar_util:encode(H2)},
					{p1, Candidate#mining_candidate.partition_number},
					{p2, Candidate#mining_candidate.partition_number2},
					{partial_difficulty, get_partial_difficulty(State1, Candidate)}])
	end,
	case {PassesDiffChecks, Peer} of
		{false, _} ->
			%% h2 does not pass diff checks, do nothing.
			ok;
		{Check, not_set} when partial == Check orelse true == Check ->
			%% This branch only handles the case where we're not part of a coordinated mining set.
			%% This includes the solo mining setup, and pool mining setup.
			%% In case of solo mining, the `Check` will alwaysbe `true`.
			%% In case of pool mining, the `Check` will be `partial` or `true`.
			%% In either case, we prepare and post the solution.
			ar_mining_server:prepare_and_post_solution(Candidate);
		{Check, _} when partial == Check orelse true == Check ->
			%% This branch only handles the case where we're part of a coordinated mining set.
			%% In this case, we prepare the PoA2 and send it to the lead peer.
			PoA2 = case ar_mining_server:prepare_poa(poa2, Candidate, #poa{}) of
				{ok, PoA} -> PoA;
				{error, _Error} ->
					%% Fallback. This will probably fail later, but prepare_poa/3 should
					%% have already printed several errors so we'll continue just in case.
					%% df: Is this the right fallback?..
					#poa{ chunk = Chunk2 }
			end,
			ar_coordination:computed_h2_for_peer(Candidate#mining_candidate{ poa2 = PoA2 })
	end,
	%% Remove the cached value from the cache.
	case ar_mining_cache:with_cached_value(
		?VDF_STEP_CACHE_KEY(Candidate#mining_candidate.cache_ref, Candidate#mining_candidate.nonce),
		Candidate#mining_candidate.session_key,
		State1#state.chunk_cache,
		fun(_) -> {ok, drop} end
	) of
		{ok, ChunkCache2} -> State1#state{ chunk_cache = ChunkCache2 };
		{error, Reason} ->
			?LOG_ERROR([{event, mining_worker_failed_to_process_computed_h2},
				{worker, State1#state.name}, {partition, State1#state.partition_number},
				{nonce, Candidate#mining_candidate.nonce},
				{session_key, ar_nonce_limiter:encode_session_key(Candidate#mining_candidate.session_key)},
				{reason, Reason}]),
			State1
	end;

%% @doc Handle the `compute_h2_for_peer` task.
%% Indicates that we got a request to compute h2 for a peer.
handle_task({compute_h2_for_peer, Candidate, _ExtraArgs}, State) ->
	#mining_candidate{
		h0 = H0,
		partition_number = Partition1,
		partition_upper_bound = PartitionUpperBound,
		cm_h1_list = H1List,
		cm_lead_peer = Peer
	} = Candidate,
	{_, RecallRange2Start} = ar_block:get_recall_range(H0, Partition1, PartitionUpperBound),
	Candidate2 = generate_cache_ref(Candidate),
	%% Clear the list so we aren't copying it around all over the place.
	Candidate3 = Candidate2#mining_candidate{ cm_h1_list = [] },
	Range2Exists = ar_mining_io:read_recall_range(chunk2, self(), Candidate3, RecallRange2Start),
	case Range2Exists of
		true ->
			ar_mining_stats:h1_received_from_peer(Peer, length(H1List)),
			State1 = mark_chunk2_missing(Candidate3, State),
			cache_h1_list(Candidate3, H1List, State1);
		false ->
			%% This can happen for two reasons:
			%% 1. (most common) Remote peer has requested a range we don't have from a
			%%    partition that we do have.
			%% 2. (rare, but possible) Remote peer has an outdated partition table and we
			%%    don't even have the requested partition.
			State
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

process_chunks(WhichChunk, Candidate, RangeStart, ChunkOffsets, State) ->
	PackingDifficulty = Candidate#mining_candidate.packing_difficulty,
	SubchunksPerRecallRange = ar_block:get_max_nonce(PackingDifficulty),
	SubchunksPerChunk = ar_block:get_nonces_per_chunk(PackingDifficulty),
	SubchunkSize = ar_block:get_sub_chunk_size(PackingDifficulty),
	process_chunks(
		WhichChunk, Candidate, RangeStart, 0, SubchunksPerChunk,
		SubchunksPerRecallRange, ChunkOffsets, SubchunkSize, 0, State
	).

process_chunks(
	WhichChunk, Candidate, _RangeStart, Nonce, _SubchunksPerChunk,
	SubchunksPerRecallRange, _ChunkOffsets, _SubchunkSize, Count, State
) when Nonce > SubchunksPerRecallRange ->
	%% We've processed all the subchunks in the recall range.
	ar_mining_stats:chunks_read(case WhichChunk of
		chunk1 -> Candidate#mining_candidate.partition_number;
		chunk2 -> Candidate#mining_candidate.partition_number2
	end, Count),
	State;
process_chunks(
	WhichChunk, Candidate, RangeStart, Nonce, SubchunksPerChunk,
	SubchunksPerRecallRange, [], SubchunkSize, Count, State
) ->
	%% No more ChunkOffsets means no more chunks have been read. Iterate through all the
	%% remaining nonces and remove the full chunks from the cache.
	State2 = remove_subchunks_from_cache(Candidate#mining_candidate{ nonce = Nonce }, SubchunksPerChunk, State),
	%% Process the next chunk.
	process_chunks(
		WhichChunk, Candidate, RangeStart, Nonce + SubchunksPerChunk,
		SubchunksPerChunk, SubchunksPerRecallRange, [], SubchunkSize, Count, State2
	);
process_chunks(
	WhichChunk, Candidate, RangeStart, Nonce, SubchunksPerChunk,
	SubchunksPerRecallRange, [{ChunkEndOffset, Chunk} | ChunkOffsets], SubchunkSize, Count, State
) ->
	SubchunkStartOffset = RangeStart + Nonce * SubchunkSize,
	ChunkStartOffset = ChunkEndOffset - ?DATA_CHUNK_SIZE,
	case {SubchunkStartOffset < ChunkStartOffset, SubchunkStartOffset >= ChunkEndOffset} of
		{true, false} ->
			%% Skip this nonce.
			%% Nonce falls in a chunk which wasn't read from disk (for example, because there are holes
			%% in the recall range), e.g. the nonce is in the middle of a non-existent chunk.
			%% Remove chunk2 from cache if it is there already, we don't need it (e.g. WhichChunk == chunk1).
			%% Remove chunk1 from cache if it is there already, we already processed it (e.g. WhichChunk == chunk2).
			State2 = remove_subchunks_from_cache(Candidate#mining_candidate{ nonce = Nonce }, SubchunksPerChunk, State),
			process_chunks(
				WhichChunk, Candidate, RangeStart, Nonce + SubchunksPerChunk, SubchunksPerChunk,
				SubchunksPerRecallRange, [{ChunkEndOffset, Chunk} | ChunkOffsets], SubchunkSize, Count, State2
			);
		{false, true} ->
			%% Skip this chunk.
			%% Nonce falls in a chunk beyond the current chunk offset, (for example, because we
			%% read extra chunk in the beginning of recall range). Move ahead to the next
			%% chunk offset.
			%% No need to remove anything from cache, as the nonce is still in the recall range.
			process_chunks(
				WhichChunk, Candidate, RangeStart, Nonce, SubchunksPerChunk,
				SubchunksPerRecallRange, ChunkOffsets, SubchunkSize, Count, State
			);
		{false, false} ->
			%% Process all sub-chunks in Chunk, and then advance to the next chunk.
			State2 = process_all_subchunks(WhichChunk, Chunk, Candidate, Nonce, State),
			process_chunks(
				WhichChunk, Candidate, RangeStart, Nonce + SubchunksPerChunk, SubchunksPerChunk,
				SubchunksPerRecallRange, ChunkOffsets, SubchunkSize, Count + 1, State2
			)
	end.

process_all_subchunks(_WhichChunk, <<>>, _Candidate, _Nonce, State) -> State;
process_all_subchunks(WhichChunk, Chunk, Candidate, Nonce, State)
when Candidate#mining_candidate.packing_difficulty == 0 ->
	%% Spora 2.6 packing (aka difficulty 0).
	Candidate2 = Candidate#mining_candidate{ nonce = Nonce },
	process_subchunk(WhichChunk, Candidate2, Chunk, State);
process_all_subchunks(
	WhichChunk,
	<< Subchunk:?COMPOSITE_PACKING_SUB_CHUNK_SIZE/binary, Rest/binary >>,
	Candidate, Nonce, State
) ->
	%% Composite packing / replica packing (aka difficulty 1+).
	Candidate2 = Candidate#mining_candidate{ nonce = Nonce },
	State2 = process_subchunk(WhichChunk, Candidate2, Subchunk, State),
	process_all_subchunks(WhichChunk, Rest, Candidate2, Nonce + 1, State2);
process_all_subchunks(WhichChunk, Rest, _Candidate, Nonce, State) ->
	%% The chunk is not a multiple of the subchunk size.
	?LOG_ERROR([{event, failed_to_split_chunk_into_subchunks},
			{remaining_size, byte_size(Rest)},
			{nonce, Nonce},
			{chunk, WhichChunk}]),
	State.

process_subchunk(chunk1, Candidate, Subchunk, State) ->
	ar_mining_hash:compute_h1(self(), Candidate#mining_candidate{ chunk1 = Subchunk }),
	State;
process_subchunk(chunk2, Candidate, Subchunk, State) ->
	#mining_candidate{ session_key = SessionKey } = Candidate,
	Candidate2 = Candidate#mining_candidate{ chunk2 = Subchunk },
	case ar_mining_cache:with_cached_value(
		?VDF_STEP_CACHE_KEY(Candidate2#mining_candidate.cache_ref, Candidate2#mining_candidate.nonce),
		SessionKey,
		State#state.chunk_cache,
		fun
			(#ar_mining_cache_value{h1 = undefined} = CachedValue) ->
				%% H1 is not yet calculated, cache the chunk2 for this nonce.
				{ok, CachedValue#ar_mining_cache_value{chunk2 = Subchunk}};
			(#ar_mining_cache_value{h1 = H1} = CachedValue) ->
				%% H1 is already calculated, compute h2 and cache the chunk2 for this nonce.
				ar_mining_hash:compute_h2(self(), Candidate2#mining_candidate{ h1 = H1 }),
				{ok, CachedValue#ar_mining_cache_value{chunk2 = Subchunk}}
		end
	) of
		{ok, ChunkCache2} -> State#state{ chunk_cache = ChunkCache2 };
		{error, Reason} ->
			?LOG_ERROR([{event, mining_worker_failed_to_process_chunk2},
				{worker, State#state.name}, {partition, State#state.partition_number},
				{nonce, Candidate2#mining_candidate.nonce},
				{session_key, ar_nonce_limiter:encode_session_key(SessionKey)},
				{reason, Reason}]),
			State
	end.

priority(computed_h2, StepNumber) -> {1, -StepNumber};
priority(computed_h1, StepNumber) -> {2, -StepNumber};
priority(compute_h2_for_peer, StepNumber) -> {2, -StepNumber};
priority(chunk2, StepNumber) -> {3, -StepNumber};
priority(chunk1, StepNumber) -> {4, -StepNumber};
priority(computed_h0, StepNumber) -> {5, -StepNumber};
priority(compute_h0, StepNumber) -> {6, -StepNumber}.

%% @doc Returns true if the mining candidate belongs to a valid mining session. Always assume
%% that a coordinated mining candidate is valid (its cm_lead_peer is set).
is_session_valid(_State, #mining_candidate{ cm_lead_peer = Peer })
		when Peer /= not_set ->
	true;
is_session_valid(State, #mining_candidate{ session_key = SessionKey }) ->
	ar_mining_cache:session_exists(SessionKey, State#state.chunk_cache).

h1_passes_diff_checks(H1, Candidate, State) ->
	passes_diff_checks(H1, true, Candidate, State).

h2_passes_diff_checks(H2, Candidate, State) ->
	passes_diff_checks(H2, false, Candidate, State).

passes_diff_checks(SolutionHash, IsPoA1, Candidate, State) ->
	DiffPair = get_difficulty(State, Candidate),
	#mining_candidate{ packing_difficulty = PackingDifficulty } = Candidate,
	case ar_node_utils:passes_diff_check(SolutionHash, IsPoA1, DiffPair, PackingDifficulty) of
		true -> true;
		false ->
			case get_partial_difficulty(State, Candidate) of
				not_set -> false;
				PartialDiffPair ->
					case ar_node_utils:passes_diff_check(
						SolutionHash, IsPoA1, PartialDiffPair, PackingDifficulty
					) of
						true -> partial;
						false -> false
					end
			end
	end.

maybe_warn_about_lag(Q, Name) ->
	case gb_sets:is_empty(Q) of
		true -> ok;
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
						false -> ok;
						true ->
							?LOG_WARNING([
								{event, mining_worker_lags_behind_the_nonce_limiter},
								{worker, Name},
								{step_count, N}])
					end;
				_ -> ok
			end
	end.

count_h0_tasks(Q) ->
	case gb_sets:is_empty(Q) of
		true -> 0;
		false ->
			case gb_sets:take_smallest(Q) of
				{{_Priority, _ID, {compute_h0, _Args}}, Q2} ->
					1 + count_h0_tasks(Q2);
				_ -> 0
			end
	end.

update_sessions(ActiveSessions, State) ->
	CurrentSessions = ar_mining_cache:get_sessions(State#state.chunk_cache),
	AddedSessions = lists:subtract(ActiveSessions, CurrentSessions),
	RemovedSessions = lists:subtract(CurrentSessions, ActiveSessions),
	add_sessions(AddedSessions, remove_sessions(RemovedSessions, State)).

add_sessions([], State) -> State;
add_sessions([SessionKey | AddedSessions], State) ->
	ChunkCache = ar_mining_cache:add_session(SessionKey, State#state.chunk_cache),
	?LOG_DEBUG([{event, mining_debug_add_session},
		{worker, State#state.name}, {partition, State#state.partition_number},
		{session_key, ar_nonce_limiter:encode_session_key(SessionKey)}]),
	add_sessions(AddedSessions, State#state{chunk_cache = ChunkCache}).

remove_sessions([], State) -> State;
remove_sessions([SessionKey | RemovedSessions], State) ->
	ChunkCache = ar_mining_cache:drop_session(SessionKey, State#state.chunk_cache),
	TaskQueue = remove_tasks(SessionKey, State#state.task_queue),
	?LOG_DEBUG([{event, mining_debug_remove_session},
		{worker, State#state.name}, {partition, State#state.partition_number},
		{session, ar_nonce_limiter:encode_session_key(SessionKey)}]),
	remove_sessions(RemovedSessions, State#state{
		task_queue = TaskQueue,
		chunk_cache = ChunkCache
	}).

remove_tasks(SessionKey, TaskQueue) ->
	gb_sets:filter(
		fun({_Priority, _ID, {TaskType, Candidate, _ExtraArgs}}) ->
			case Candidate#mining_candidate.session_key == SessionKey of
				true ->
					prometheus_gauge:dec(mining_server_task_queue_len, [TaskType]),
					false;
				false ->
					true
			end
		end,
		TaskQueue
	).

try_to_reserve_cache_space(Multiplier, SessionKey, #state{
	packing_difficulty = PackingDifficulty,
	chunk_cache = ChunkCache0
} = State) ->
	case ar_mining_cache:reserve_for_session(
		SessionKey, Multiplier * ar_block:get_recall_range_size(PackingDifficulty), ChunkCache0
	) of
		{ok, ChunkCache1} ->
			State1 = State#state{ chunk_cache = ChunkCache1 },
			{true, State1};
		{error, Reason} ->
			?LOG_WARNING([{event, mining_worker_failed_to_reserve_cache_space},
				{worker, State#state.name}, {partition, State#state.partition_number},
				{session_key, ar_nonce_limiter:encode_session_key(SessionKey)},
				{reason, Reason}]),
			false
	end.

mark_chunk2_missing(Candidate, State) ->
	#mining_candidate{ packing_difficulty = PackingDifficulty } = Candidate,
	mark_chunk2_missing(0, ar_block:get_max_nonce(PackingDifficulty), Candidate, State).

mark_chunk2_missing(Nonce, SubchunksPerRecallRange, _Candidate, State)
when Nonce > SubchunksPerRecallRange ->
	State;
mark_chunk2_missing(Nonce, SubchunksPerRecallRange, Candidate, State) ->
	case ar_mining_cache:with_cached_value(
		?VDF_STEP_CACHE_KEY(Candidate#mining_candidate.cache_ref, Nonce),
		Candidate#mining_candidate.session_key,
		State#state.chunk_cache,
		fun(CachedValue) -> {ok, CachedValue#ar_mining_cache_value{chunk2_missing = true}} end
	) of
		{ok, ChunkCache1} ->
			mark_chunk2_missing(Nonce + 1, SubchunksPerRecallRange, Candidate, State#state{ chunk_cache = ChunkCache1 });
		{error, Reason} ->
			%% NB: this clause may cause a memory leak, because mining worker will wait for
			%% chunk2 to arrive.
			?LOG_ERROR([{event, mining_worker_failed_to_add_chunk_to_cache}, {reason, Reason}]),
			mark_chunk2_missing(Nonce + 1, SubchunksPerRecallRange, Candidate, State)
	end.

%% @doc Remove SubchunkCount sub-chunks from the cache starting at
%% Candidate#mining_candidate.nonce.
remove_subchunks_from_cache(_Candidate, 0, State) ->
	State;
remove_subchunks_from_cache(#mining_candidate{ cache_ref = CacheRef } = Candidate,
		SubchunkCount, State) when CacheRef /= not_set ->
	#mining_candidate{ nonce = Nonce, session_key = SessionKey } = Candidate,
	State2 = case ar_mining_cache:with_cached_value(
		?VDF_STEP_CACHE_KEY(Candidate#mining_candidate.cache_ref, Candidate#mining_candidate.nonce),
		SessionKey,
		State#state.chunk_cache,
		fun(_) -> {ok, drop} end
	) of
		{ok, ChunkCache1} ->
			State#state{ chunk_cache = ChunkCache1 };
		{error, Reason} ->
			?LOG_ERROR([{event, mining_worker_failed_to_remove_subchunks_from_cache},
				{worker, State#state.name}, {partition, State#state.partition_number},
				{nonce, Nonce}, {session_key, ar_nonce_limiter:encode_session_key(SessionKey)},
				{reason, Reason}]),
			State
	end,
	remove_subchunks_from_cache(Candidate#mining_candidate{ nonce = Nonce + 1 }, SubchunkCount - 1, State2).

cache_h1_list(_Candidate, [], State) -> State;
cache_h1_list(#mining_candidate{ cache_ref = not_set } = _Candidate, [], State) -> State;
cache_h1_list(Candidate, [ {H1, Nonce} | H1List ], State) ->
	case ar_mining_cache:with_cached_value(
		?VDF_STEP_CACHE_KEY(Candidate#mining_candidate.cache_ref, Nonce),
		Candidate#mining_candidate.session_key,
		State#state.chunk_cache,
		fun(CachedValue) -> {ok, CachedValue#ar_mining_cache_value{h1 = H1}} end
	) of
		{ok, ChunkCache1} ->
			cache_h1_list(Candidate, H1List, State#state{ chunk_cache = ChunkCache1 });
		{error, Reason} ->
			?LOG_ERROR([{event, mining_worker_failed_to_cache_h1},
				{worker, State#state.name}, {partition, State#state.partition_number},
				{nonce, Nonce}, {session_key, ar_nonce_limiter:encode_session_key(Candidate#mining_candidate.session_key)},
				{reason, Reason}]),
			cache_h1_list(Candidate, H1List, State)
	end.

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

report_and_reset_hashes(State) ->
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

report_chunk_cache_metrics(#state{chunk_cache = ChunkCache, partition_number = Partition} = State) ->
	prometheus_gauge:set(mining_server_chunk_cache_size, [Partition], ar_mining_cache:cache_size(ChunkCache)),
	State.

%%%===================================================================
%%% Public Test interface.
%%%===================================================================
