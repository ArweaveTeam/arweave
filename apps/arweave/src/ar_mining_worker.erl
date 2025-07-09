-module(ar_mining_worker).

-behaviour(gen_server).

-export([start_link/2, name/2, reset_mining_session/2, set_sessions/2, chunks_read/5, computed_hash/5,
		set_difficulty/2, set_cache_limits/3, add_task/3, garbage_collect/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_config.hrl").
-include("ar_mining.hrl").
-include("ar_mining_cache.hrl").
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

-define(TASK_CHECK_INTERVAL_MS, 200).
-define(STATUS_CHECK_INTERVAL_MS, 5000).
-define(REPORT_CHUNK_CACHE_METRICS_INTERVAL_MS, 30000).
-define(CACHE_KEY(CacheRef, Nonce), {CacheRef, Nonce}).

%%%===================================================================
%%% Messages
%%%===================================================================

-define(MSG_RESET_MINING_SESSION(DiffPair), {reset_mining_session, DiffPair}).
-define(MSG_SET_SESSIONS(ActiveSessions), {set_sessions, ActiveSessions}).
-define(MSG_ADD_TASK(Task), {add_task, Task}).
-define(MSG_CHUNKS_READ(WhichChunk, Candidate, RangeStart, ChunkOffsets), {chunks_read, {WhichChunk, Candidate, RangeStart, ChunkOffsets}}).
-define(MSG_SET_DIFFICULTY(DiffPair), {set_difficulty, DiffPair}).
-define(MSG_SET_CACHE_LIMITS(CacheLimitBytes, VDFQueueLimit), {set_cache_limits, CacheLimitBytes, VDFQueueLimit}).
-define(MSG_CHECK_WORKER_STATUS, {check_worker_status}).
-define(MSG_HANDLE_TASK, {handle_task}).
-define(MSG_GARBAGE_COLLECT(StartTime, GCResult), {garbage_collect, StartTime, GCResult}).
-define(MSG_GARBAGE_COLLECT, {garbage_collect}).
-define(MSG_FETCHED_LAST_MOMENT_PROOF(Any), {fetched_last_moment_proof, Any}).
-define(MSG_REPORT_CHUNK_CACHE_METRICS, {report_chunk_cache_metrics}).

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
	%% Delay task by random amount between ?TASK_CHECK_INTERVAL_MS and 2*?TASK_CHECK_INTERVAL_MS
	%% The reason for the randomization to avoid a glut tasks to all get added at the same time -
	%% in particular when the chunk cache fills up it's possible for all queued compute_h0 tasks
	%% to be delayed at about the same time.
	Delay = rand:uniform(?TASK_CHECK_INTERVAL_MS) + ?TASK_CHECK_INTERVAL_MS,
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

-spec set_cache_limits(Worker :: pid(), CacheLimitBytes :: non_neg_integer(), VDFQueueLimit :: non_neg_integer()) -> ok.
set_cache_limits(Worker, CacheLimitBytes, VDFQueueLimit) ->
	gen_server:cast(Worker, ?MSG_SET_CACHE_LIMITS(CacheLimitBytes, VDFQueueLimit)).

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
	ChunkCache = ar_mining_cache:new(),
	State0 = #state{
		name = Name,
		chunk_cache = ChunkCache,
		partition_number = Partition,
		is_pool_client = ar_pool:is_client(),
		packing_difficulty = PackingDifficulty
	},
	gen_server:cast(self(), ?MSG_HANDLE_TASK),
	gen_server:cast(self(), ?MSG_CHECK_WORKER_STATUS),
	gen_server:cast(self(), ?MSG_REPORT_CHUNK_CACHE_METRICS),
	{ok, report_chunk_cache_metrics(State0)}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(?MSG_SET_CACHE_LIMITS(CacheLimitBytes, VDFQueueLimit), State) ->
	State1 = State#state{
		chunk_cache = ar_mining_cache:set_limit(CacheLimitBytes, State#state.chunk_cache),
		vdf_queue_limit = VDFQueueLimit
	},
	{noreply, State1};

handle_cast(?MSG_SET_DIFFICULTY(DiffPair), State) ->
	State1 = State#state{ diff_pair = DiffPair },
	{noreply, State1};

handle_cast(?MSG_RESET_MINING_SESSION(DiffPair), State) ->
	State1 = update_sessions([], State),
	State2 = State1#state{ diff_pair = DiffPair },
	{noreply, State2};

handle_cast(?MSG_SET_SESSIONS(ActiveSessions), State) ->
	State1 = update_sessions(ActiveSessions, State),
	{noreply, State1};

handle_cast(?MSG_CHUNKS_READ(WhichChunk, Candidate, RangeStart, ChunkOffsets), State) ->
	case is_session_valid(State, Candidate) of
		true ->
			State1 = process_chunks(WhichChunk, Candidate, RangeStart, ChunkOffsets, State),
			{noreply, State1};
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
			{noreply, State1};
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
			ar_util:cast_after(?TASK_CHECK_INTERVAL_MS, self(), ?MSG_HANDLE_TASK),
			{noreply, State};
		_ ->
			gen_server:cast(self(), ?MSG_HANDLE_TASK),
			{{_Priority, _ID, {TaskType, Candidate, _ExtraArgs} = Task}, Q2} = gb_sets:take_smallest(Q),
			prometheus_gauge:dec(mining_server_task_queue_len, [TaskType]),
			case is_session_valid(State, Candidate) of
				true ->
					State1 = handle_task(Task, State#state{ task_queue = Q2 }),
					{noreply, State1};
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

handle_cast(?MSG_CHECK_WORKER_STATUS, State) ->
	maybe_warn_about_lag(State#state.task_queue, State#state.name),
	ar_util:cast_after(?STATUS_CHECK_INTERVAL_MS, self(), ?MSG_CHECK_WORKER_STATUS),
	{noreply, State};

handle_cast(?MSG_GARBAGE_COLLECT, State) ->
	erlang:garbage_collect(self(), [{async, erlang:monotonic_time()}]),
	{noreply, State};

handle_cast(?MSG_REPORT_CHUNK_CACHE_METRICS, State) ->
	ar_util:cast_after(?REPORT_CHUNK_CACHE_METRICS_INTERVAL_MS, self(), ?MSG_REPORT_CHUNK_CACHE_METRICS),
	{noreply, report_chunk_cache_metrics(State)};

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
	% Only mine recent VDF Steps
	case StepNumber >= LatestVDFStepNumber - VDFQueueLimit of
		true ->
			%% Try to reserve the cache space for both partitions, as we do not know if we have both, one, or none.
			case try_to_reserve_cache_range_space(2, Candidate#mining_candidate.session_key, State1) of
				{true, State2} ->
					%% Cache space reserved, compute h0.
					ar_mining_hash:compute_h0(self(), Candidate),
					State2#state{ latest_vdf_step_number = max(StepNumber, LatestVDFStepNumber) };
				false ->
					%% We don't have enough cache space to read the recall ranges, so we'll try again later.
					add_delayed_task(self(), compute_h0, Candidate),
					State1#state{ latest_vdf_step_number = max(StepNumber, LatestVDFStepNumber) }
			end;
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

	%% We already reserved the cache space for both partitions, so we need to release the reserved space
	%% if we're missing one or both of the recall ranges.
	case {Range1Exists, Range2Exists} of
		{true, true} ->
			%% Both recall ranges are readable, no release needed.
			%% Read the recall ranges; the result of the read will be reported by the `chunk1` and `chunk2` tasks.
			ar_mining_io:read_recall_range(chunk1, self(), Candidate2, RecallRange1Start),
			ar_mining_io:read_recall_range(chunk2, self(), Candidate2, RecallRange2Start),
			State;
		{true, false} ->
			%% Only the first recall range is readable, so we need to release the reserved space for the second
			%% recall range.
			State1 = release_cache_range_space(1, Candidate2#mining_candidate.session_key, State),
			%% Mark second recall range as missing, not to wait for it to arrive.
			State2 = mark_second_recall_range_missing(Candidate2, State1),
			%% Read the recall range; the result of the read will be reported by the `chunk1` task.
			ar_mining_io:read_recall_range(chunk1, self(), Candidate2, RecallRange1Start),
			State2;
		{false, _} ->
			%% We don't have the recall ranges, so we need to release the reserved space for both partitions.
			State1 = release_cache_range_space(2, Candidate2#mining_candidate.session_key, State),
			State1
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
	H1PassesDiffChecks = h1_passes_diff_checks(H1, Candidate, State1),
	case H1PassesDiffChecks of
		false -> ok;
		partial -> ar_mining_server:prepare_and_post_solution(Candidate);
		true ->
			%% H1 solution found, report it.
			?LOG_INFO([{event, found_h1_solution},
				{step, Candidate#mining_candidate.step_number},
				{worker, State1#state.name},
				{h1, ar_util:encode(H1)},
				{p1, Candidate#mining_candidate.partition_number},
				{difficulty, get_difficulty(State1, Candidate)}]),
			ar_mining_server:prepare_and_post_solution(Candidate),
			ar_mining_stats:h1_solution()
	end,
	%% Check if we need to compute H2.
	%% Also store H1 in the cache if needed.
	case ar_mining_cache:with_cached_value(
		?CACHE_KEY(Candidate#mining_candidate.cache_ref, Candidate#mining_candidate.nonce),
		Candidate#mining_candidate.session_key,
		State1#state.chunk_cache,
		fun
			(#ar_mining_cache_value{chunk2_missing = true}) ->
				%% This node does not store chunk2. If we're part of a coordinated
				%% mining set, we can try one of our peers, but this node is done with
				%% this VDF step.
				{ok, Config} = application:get_env(arweave, config),
				case Config#config.coordinated_mining of
					false -> ok;
					true ->
						DiffPair = case get_partial_difficulty(State1, Candidate) of
								not_set -> get_difficulty(State1, Candidate);
								PartialDiffPair -> PartialDiffPair
							end,
						ar_coordination:computed_h1(Candidate, DiffPair)
				end,
				%% Remove the cached value from the cache.
				{ok, drop};
			(#ar_mining_cache_value{chunk2 = undefined} = CachedValue) ->
				%% chunk2 hasn't been read yet, so we cache H1 and wait for it.
				%% If H1 passes diff checks, we will skip H2 for this nonce.
				{ok, CachedValue#ar_mining_cache_value{h1 = H1, h1_passes_diff_checks = H1PassesDiffChecks}};
			(#ar_mining_cache_value{chunk2 = Chunk2} = CachedValue) when not H1PassesDiffChecks ->
				%% chunk2 has already been read, so we can compute H2 now.
				ar_mining_hash:compute_h2(self(), Candidate#mining_candidate{ chunk2 = Chunk2 }),
				{ok, CachedValue#ar_mining_cache_value{h1 = H1}};
			(#ar_mining_cache_value{chunk2 = _Chunk2} = _CachedValue) when H1PassesDiffChecks ->
				%% H1 passes diff checks, so we skip H2 for this nonce.
				%% Might as well drop the cached data, we don't need it anymore.
				{ok, drop}
		end
	) of
		{ok, ChunkCache2} -> State1#state{ chunk_cache = ChunkCache2 };
		{error, Reason} ->
			?LOG_ERROR([{event, mining_worker_failed_to_process_h1},
				{worker, State1#state.name}, {partition, State1#state.partition_number},
				{nonce, Candidate#mining_candidate.nonce},
				{session_key, ar_nonce_limiter:encode_session_key(Candidate#mining_candidate.session_key)},
				{reason, Reason}]),
			State1
	end;

%% @doc Handle the `computed_h2` task.
%% Indicates that the single hash for the second recall range has been computed.
handle_task({computed_h2, Candidate, _ExtraArgs}, State) ->
	#mining_candidate{ h2 = H2, cm_lead_peer = Peer } = Candidate,
	State1 = hash_computed(h2, Candidate, State),
	PassesDiffChecks = h2_passes_diff_checks(H2, Candidate, State1),
	case PassesDiffChecks of
		false -> ok;
		partial ->
			?LOG_INFO([{event, found_h2_partial_solution},
					{worker, State1#state.name},
					{step, Candidate#mining_candidate.step_number},
					{h2, ar_util:encode(H2)},
					{p1, Candidate#mining_candidate.partition_number},
					{p2, Candidate#mining_candidate.partition_number2},
					{partial_difficulty, get_partial_difficulty(State1, Candidate)}]);
		true ->
			?LOG_INFO([{event, found_h2_solution},
					{worker, State#state.name},
					{step, Candidate#mining_candidate.step_number},
					{h2, ar_util:encode(H2)},
					{p1, Candidate#mining_candidate.partition_number},
					{p2, Candidate#mining_candidate.partition_number2},
					{difficulty, get_difficulty(State1, Candidate)},
					{partial_difficulty, get_partial_difficulty(State1, Candidate)}]),
			ar_mining_stats:h2_solution()
	end,
	case {PassesDiffChecks, Peer} of
		{false, _} ->
			%% H2 does not pass diff checks, do nothing.
			ok;
		{Check, not_set} when partial == Check orelse true == Check ->
			%% This branch only handles the case where we're not part of a coordinated mining set.
			%% This includes the solo mining setup, and pool mining setup.
			%% In case of solo mining, the `Check` will always be `true`.
			%% In case of pool mining, the `Check` will be `partial` or `true`.
			%% In either case, we prepare and post the solution.
			case Candidate#mining_candidate.chunk1 of
				not_set ->
					?LOG_ERROR([{event, received_solution_candidate_without_chunk1_in_solo_mining},
							{worker, State#state.name},
							{step, Candidate#mining_candidate.step_number},
							{nonce, Candidate#mining_candidate.nonce},
							{session_key, ar_nonce_limiter:encode_session_key(Candidate#mining_candidate.session_key)},
							{h2, ar_util:encode(H2)}]),
					ok;
				_ ->
					ar_mining_server:prepare_and_post_solution(Candidate)
			end;
		{Check, _} when partial == Check orelse true == Check ->
			%% This branch only handles the case where we're part of a coordinated mining set.
			%% In this case, we prepare the PoA2 and send it to the lead peer.
			ar_coordination:computed_h2_for_peer(Candidate)
	end,
	%% Remove the cached value from the cache.
	case ar_mining_cache:with_cached_value(
		?CACHE_KEY(Candidate#mining_candidate.cache_ref, Candidate#mining_candidate.nonce),
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
%% Indicates that we got a request to compute H2 for a peer.
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
			%% Mark second recall range as missing, just like if we didn't have the recall range
			%% in a regular mining setup.
			%% This is done because we only have a part of H1 list that passes diff checks.
			State1 = mark_second_recall_range_missing(Candidate3, State),
			%% After we marked the whole second recall range as missing, we can cache the H1 list.
			%% During this process, we also reset the chunk2_missing flag to false for the entries
			%% we have H1 for.
			%% After these manipulations we will only handle the second recall range nonces that
			%% have corresponding H1s.
			cache_h1_list(Candidate3, H1List, State1);
		false ->
			%% This can happen for two reasons:
			%% 1. (most common) Remote peer has requested a range we don't have from a
			%%    partition that we do have.
			%% 2. (rare, but possible) Remote peer has an outdated partition table and we
			%%    don't even have the requested partition.
			%% In both cases, we don't even need to cache the H1 list as we cannot
			%% find a valid H2.
			State
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

process_chunks(WhichChunk, Candidate, RangeStart, ChunkOffsets, State) ->
	PackingDifficulty = Candidate#mining_candidate.packing_difficulty,
	NoncesPerRecallRange = ar_block:get_max_nonce(PackingDifficulty),
	NoncesPerChunk = ar_block:get_nonces_per_chunk(PackingDifficulty),
	SubChunkSize = ar_block:get_sub_chunk_size(PackingDifficulty),
	process_chunks(
		WhichChunk, Candidate, RangeStart, 0, NoncesPerChunk,
		NoncesPerRecallRange, ChunkOffsets, SubChunkSize, 0, State
	).

%% Processing chunks for a recall range.
%%
%% Recall range offset is not aligned to chunk size.
%% When reading data from disk, we always read the entire chunk.
%% This means that the amount of data read from disk is always bigger than the
%% recall range size:
%%
%%         |<-      recall range       ->|
%% [    ][ 1  ][ 2  ] .... [n-2 ][n-1 ][ n  ]
%%         ^
%%         recall range start offset
%%         falls into chunk 1
%%
%% When determining which chunks to process, we find the first chunk that
%% contains the first nonce of the recall range, and start processing from this
%% chunk. This effectively shifts the recall range to the left:
%%
%%         |<-      recall range       ->|
%% [    ][ 1  ][ 2  ] .... [n-2 ][n-1 ][ n  ]
%%       |<- effective recall range ->|
%%
%% If the recall range start offset aligns with the chunk size accidentally,
%% current implementation skips the first chunk completely. Fixing this
%% inconsistency will require a hard fork:
%%
%%       |<-      recall range      ->|
%% [    ][ 1  ][ 2  ] .... [n-2 ][n-1 ][ n  ]
%%             |<- effective recall range ->|
%%
%% The ultimate goal is to process all the sub-chunks in the recall range.
%% The count of subchunks in the recall range is `NoncesPerRecallRange`.
%% replica packing: 10 chunks, 32 nonces per chunk, 320 nonces per recall range.
%% spora 2.6: 200 chunks, 1 nonce per chunk, 200 nonces per recall range.
%%
%% Some of the chunks inside (including first and last) might be missing.
%% This cases must be handled correctly to avoid keeping not needed chunks in
%% the cache.
process_chunks(
	WhichChunk, Candidate, _RangeStart, Nonce, _NoncesPerChunk,
	NoncesPerRecallRange, _ChunkOffsets, _SubChunkSize, Count, State
) when Nonce > NoncesPerRecallRange ->
	%% We've processed all the sub_chunks in the recall range.
	ar_mining_stats:chunks_read(case WhichChunk of
		chunk1 -> Candidate#mining_candidate.partition_number;
		chunk2 -> Candidate#mining_candidate.partition_number2
	end, Count),
	State;
process_chunks(
	WhichChunk, Candidate, RangeStart, Nonce, NoncesPerChunk,
	NoncesPerRecallRange, [], SubChunkSize, Count, State
) ->
	%% No more ChunkOffsets means no more chunks have been read. Iterate through all the
	%% remaining nonces and remove the full chunks from the cache.
	State1 = case WhichChunk of
		chunk1 -> mark_single_chunk1_missing_or_drop(Nonce, Candidate, State);
		chunk2 -> mark_single_chunk2_missing_or_drop(Nonce, Candidate, State)
	end,
	%% Drop the reservation for the current nonce group (from Nonce to Nonce + NoncesPerChunk - 1).
	State2 = case ar_mining_cache:release_for_session(
		Candidate#mining_candidate.session_key,
		?DATA_CHUNK_SIZE,
		State1#state.chunk_cache
	) of
		{ok, ChunkCache1} -> State1#state{ chunk_cache = ChunkCache1 };
		{error, Reason} ->
			?LOG_ERROR([{event, mining_worker_failed_to_release_reservation_for_session}, {reason, Reason}]),
			State1
	end,
	%% Process the next chunk.
	process_chunks(
		WhichChunk, Candidate, RangeStart, Nonce + NoncesPerChunk,
		NoncesPerChunk, NoncesPerRecallRange, [], SubChunkSize, Count, State2
	);
process_chunks(
	WhichChunk, Candidate, RangeStart, Nonce, NoncesPerChunk,
	NoncesPerRecallRange, [{ChunkEndOffset, Chunk} | ChunkOffsets], SubChunkSize, Count, State
) ->
	NonceOffset = RangeStart + Nonce * SubChunkSize,
	ChunkStartOffset = ChunkEndOffset - ?DATA_CHUNK_SIZE,
	case {NonceOffset < ChunkStartOffset, NonceOffset >= ChunkEndOffset, WhichChunk} of
		{true, _, chunk1} ->
			%% Skip these nonces (starting from Nonce to Nonce + NoncesPerChunk - 1).
			%% Nonce falls in a chunk which wasn't read from disk (for example, because there are holes
			%% in the recall range), e.g. the nonce is in the middle of a non-existent chunk.
			%% Mark single chunk1 as missing or remove it if the corresponding chunk is already read or marked as missing.
			State1 = mark_single_chunk1_missing_or_drop(Nonce, Candidate, State),
			process_chunks(
				WhichChunk, Candidate, RangeStart, Nonce + NoncesPerChunk, NoncesPerChunk,
				NoncesPerRecallRange, [{ChunkEndOffset, Chunk} | ChunkOffsets], SubChunkSize, Count, State1
			);
		{true, _, chunk2} ->
			%% Skip these nonces (starting from Nonce to Nonce + NoncesPerChunk - 1).
			%% Nonce falls in a chunk which wasn't read from disk (for example, because there are holes
			%% in the recall range), e.g. the nonce is in the middle of a non-existent chunk.
			%% Mark single chunk2 as missing or remove it if the corresponding chunk is already read and H1 is calculated.
			State1 = mark_single_chunk2_missing_or_drop(Nonce, Candidate, State),
			process_chunks(
				WhichChunk, Candidate, RangeStart, Nonce + NoncesPerChunk, NoncesPerChunk,
				NoncesPerRecallRange, [{ChunkEndOffset, Chunk} | ChunkOffsets], SubChunkSize, Count, State1
			);
		{_, true, _} ->
			%% Skip this chunk.
			%% Nonce falls in a chunk beyond the current chunk offset, (for example, because we
			%% read extra chunk in the beginning of recall range). Move ahead to the next
			%% chunk offset.
			%% No need to remove anything from cache, as the nonce is still in the recall range.
			process_chunks(
				WhichChunk, Candidate, RangeStart, Nonce, NoncesPerChunk,
				NoncesPerRecallRange, ChunkOffsets, SubChunkSize, Count, State
			);
		{false, false, _} ->
			%% Process all sub-chunks in Chunk, and then advance to the next chunk.
			State1 = process_all_sub_chunks(WhichChunk, Chunk, Candidate, Nonce, State),
			process_chunks(
				WhichChunk, Candidate, RangeStart, Nonce + NoncesPerChunk, NoncesPerChunk,
				NoncesPerRecallRange, ChunkOffsets, SubChunkSize, Count + 1, State1
			)
	end.

process_all_sub_chunks(_WhichChunk, <<>>, _Candidate, _Nonce, State) -> State;
process_all_sub_chunks(WhichChunk, Chunk, Candidate, Nonce, State)
when Candidate#mining_candidate.packing_difficulty == 0 ->
	%% Spora 2.6 packing (aka difficulty 0).
	Candidate2 = Candidate#mining_candidate{ nonce = Nonce },
	process_sub_chunk(WhichChunk, Candidate2, Chunk, State);
process_all_sub_chunks(
	WhichChunk,
	<< SubChunk:?COMPOSITE_PACKING_SUB_CHUNK_SIZE/binary, Rest/binary >>,
	Candidate, Nonce, State
) ->
	%% Composite packing / replica packing (aka difficulty 1+).
	Candidate2 = Candidate#mining_candidate{ nonce = Nonce },
	State1 = process_sub_chunk(WhichChunk, Candidate2, SubChunk, State),
	process_all_sub_chunks(WhichChunk, Rest, Candidate2, Nonce + 1, State1);
process_all_sub_chunks(WhichChunk, Rest, _Candidate, Nonce, State) ->
	%% The chunk is not a multiple of the subchunk size.
	?LOG_ERROR([{event, failed_to_split_chunk_into_sub_chunks},
			{remaining_size, byte_size(Rest)},
			{nonce, Nonce},
			{chunk, WhichChunk}]),
	State.

process_sub_chunk(chunk1, Candidate, SubChunk, State) ->
	%% Compute h1.
	ar_mining_hash:compute_h1(self(), Candidate#mining_candidate{ chunk1 = SubChunk }),
	%% Store the chunk1 in the cache.
	case ar_mining_cache:with_cached_value(
		?CACHE_KEY(Candidate#mining_candidate.cache_ref, Candidate#mining_candidate.nonce),
		Candidate#mining_candidate.session_key,
		State#state.chunk_cache,
		fun(CachedValue) -> {ok, CachedValue#ar_mining_cache_value{chunk1 = SubChunk}} end
	) of
		{ok, ChunkCache2} -> State#state{ chunk_cache = ChunkCache2 };
		{error, Reason} ->
			?LOG_ERROR([{event, mining_worker_failed_to_process_chunk1},
				{worker, State#state.name}, {partition, State#state.partition_number},
				{nonce, Candidate#mining_candidate.nonce},
				{session_key, ar_nonce_limiter:encode_session_key(Candidate#mining_candidate.session_key)},
				{reason, Reason}]),
			State
	end;
process_sub_chunk(chunk2, Candidate, SubChunk, State) ->
	Candidate2 = Candidate#mining_candidate{ chunk2 = SubChunk },
	case ar_mining_cache:with_cached_value(
		?CACHE_KEY(Candidate2#mining_candidate.cache_ref, Candidate2#mining_candidate.nonce),
		Candidate#mining_candidate.session_key,
		State#state.chunk_cache,
		fun
			(#ar_mining_cache_value{h1_passes_diff_checks = true} = _CachedValue) ->
				%% H1 passes diff checks, so we skip H2 for this nonce.
				%% Drop the cached data, we don't need it anymore.
				%% Since we already reserved the cache size for chunk2, but we never store it,
				%% we need to drop the reservation here.
				{ok, drop, -ar_block:get_sub_chunk_size(Candidate#mining_candidate.packing_difficulty)};
			(#ar_mining_cache_value{h1 = undefined} = CachedValue) ->
				%% H1 is not yet calculated, cache the chunk2 for this nonce.
				{ok, CachedValue#ar_mining_cache_value{chunk2 = SubChunk}};
			(#ar_mining_cache_value{h1 = H1} = CachedValue) ->
				%% H1 is already calculated, compute H2 and cache the chunk2 for this nonce.
				ar_mining_hash:compute_h2(self(), Candidate2#mining_candidate{ h1 = H1 }),
				{ok, CachedValue#ar_mining_cache_value{chunk2 = SubChunk}}
		end
	) of
		{ok, ChunkCache2} -> State#state{ chunk_cache = ChunkCache2 };
		{error, Reason} ->
			?LOG_ERROR([{event, mining_worker_failed_to_process_chunk2},
				{worker, State#state.name}, {partition, State#state.partition_number},
				{nonce, Candidate2#mining_candidate.nonce},
				{session_key, ar_nonce_limiter:encode_session_key(Candidate#mining_candidate.session_key)},
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

try_to_reserve_cache_range_space(Multiplier, SessionKey, #state{
	packing_difficulty = PackingDifficulty,
	chunk_cache = ChunkCache0
} = State) ->
	ReserveSize = Multiplier * ar_block:get_recall_range_size(PackingDifficulty),
	case ar_mining_cache:reserve_for_session(SessionKey, ReserveSize, ChunkCache0) of
		{ok, ChunkCache1} ->
			State1 = State#state{ chunk_cache = ChunkCache1 },
			{true, State1};
		{error, Reason} ->
			?LOG_WARNING([{event, mining_worker_failed_to_reserve_cache_space},
				{worker, State#state.name}, {partition, State#state.partition_number},
				{session_key, ar_nonce_limiter:encode_session_key(SessionKey)},
				{cache_size, ar_mining_cache:cache_size(ChunkCache0)},
				{cache_limit, ar_mining_cache:get_limit(ChunkCache0)},
				{reserved_size, ar_mining_cache:reserved_size(ChunkCache0)},
				{reserve_size, ReserveSize},
				{reason, Reason}]),
			false
	end.

release_cache_range_space(Multiplier, SessionKey, #state{
	packing_difficulty = PackingDifficulty,
	chunk_cache = ChunkCache0
} = State) ->
	ReleaseSize = Multiplier * ar_block:get_recall_range_size(PackingDifficulty),
	case ar_mining_cache:release_for_session(SessionKey, ReleaseSize, ChunkCache0) of
		{ok, ChunkCache1} -> State#state{ chunk_cache = ChunkCache1 };
		{error, Reason} ->
			?LOG_ERROR([{event, mining_worker_failed_to_release_cache_space},
				{worker, State#state.name}, {partition, State#state.partition_number},
				{session_key, ar_nonce_limiter:encode_session_key(SessionKey)},
				{cache_size, ar_mining_cache:cache_size(ChunkCache0)},
				{cache_limit, ar_mining_cache:get_limit(ChunkCache0)},
				{reserved_size, ar_mining_cache:reserved_size(ChunkCache0)},
				{release_size, ReleaseSize},
				{reason, Reason}]),
			State
	end.

%% @doc Mark the chunk1 as missing or drop the cache and reservation for this chunk.
%% This function is called for one chunk1.
mark_single_chunk1_missing_or_drop(Nonce, Candidate, State) ->
	#mining_candidate{ packing_difficulty = PackingDifficulty } = Candidate,
	SubChunksPerChunk = ar_block:get_nonces_per_chunk(PackingDifficulty),
	mark_single_chunk1_missing_or_drop(Nonce, SubChunksPerChunk, Candidate, State).

mark_single_chunk1_missing_or_drop(_Nonce, 0, _Candidate, State) -> State;
mark_single_chunk1_missing_or_drop(Nonce, NoncesLeft, Candidate, State) ->
	%% Mark the chunk1 as missing.
	%% The cache reservation for this chunk1 will be dropped in the final (first) clause of the function.
	case ar_mining_cache:with_cached_value(
		?CACHE_KEY(Candidate#mining_candidate.cache_ref, Nonce),
		Candidate#mining_candidate.session_key,
		State#state.chunk_cache,
		fun
			(#ar_mining_cache_value{chunk2_missing = true}) ->
				%% We've already marked the chunk2 as missing, so there was no reservation for it.
				%% We can just drop the cached value.
				{ok, drop, -ar_block:get_sub_chunk_size(Candidate#mining_candidate.packing_difficulty)};
			(#ar_mining_cache_value{chunk2 = Chunk2}) when is_binary(Chunk2) ->
				%% We've already read the chunk2 from disk, so we can just drop the cached value.
				%% The cache reservation for corresponding chunk2 was already consumed.
				{ok, drop, -ar_block:get_sub_chunk_size(Candidate#mining_candidate.packing_difficulty)};
			(#ar_mining_cache_value{chunk2 = undefined} = CachedValue) ->
				%% Mark the chunk1 as missing.
				%% When the corresponding chunk2 will be read from disk, it will be dropped immediately.
				{ok, CachedValue#ar_mining_cache_value{chunk1_missing = true}, -ar_block:get_sub_chunk_size(Candidate#mining_candidate.packing_difficulty)}
		end
	) of
		{ok, ChunkCache1} ->
			mark_single_chunk1_missing_or_drop(Nonce + 1, NoncesLeft - 1, Candidate, State#state{ chunk_cache = ChunkCache1 });
		{error, Reason} ->
			?LOG_ERROR([{event, mining_worker_failed_to_mark_chunk1_missing}, {reason, Reason}]),
			mark_single_chunk1_missing_or_drop(Nonce + 1, NoncesLeft - 1, Candidate, State)
	end.

%% @doc Mark the chunk2 as missing for a single chunk.
mark_single_chunk2_missing_or_drop(Nonce, Candidate, State) ->
	#mining_candidate{ packing_difficulty = PackingDifficulty } = Candidate,
	SubChunksPerChunk = ar_block:get_nonces_per_chunk(PackingDifficulty),
	mark_single_chunk2_missing_or_drop(Nonce, SubChunksPerChunk, Candidate, State).

mark_single_chunk2_missing_or_drop(_Nonce, 0, _Candidate, State) -> State;
mark_single_chunk2_missing_or_drop(Nonce, NoncesLeft, Candidate, State) ->
	case ar_mining_cache:with_cached_value(
		?CACHE_KEY(Candidate#mining_candidate.cache_ref, Nonce),
		Candidate#mining_candidate.session_key,
		State#state.chunk_cache,
		fun
			(#ar_mining_cache_value{chunk1_missing = true}) ->
				%% We've already marked the chunk1 as missing, so the reservation for it was released.
				%% We can just drop the cached value and release the reservation for a single subchunk.
				{ok, drop, -ar_block:get_sub_chunk_size(Candidate#mining_candidate.packing_difficulty)};
			(#ar_mining_cache_value{chunk1 = Chunk1, h1 = undefined} = CachedValue) when is_binary(Chunk1) ->
				%% We have the corresponding chunk1, but we didn't calculate H1 yet.
				%% Mark chunk2 as missing to drop the cached value after we calculate H1.
				%% Drop the reservation for a single subchunk.
				{ok, CachedValue#ar_mining_cache_value{chunk2_missing = true}, -ar_block:get_sub_chunk_size(Candidate#mining_candidate.packing_difficulty)};
			(#ar_mining_cache_value{h1 = H1}) when is_binary(H1) ->
				%% We've already calculated H1, so we can drop the cached value.
				%% Drop the reservation for a single subchunk.
				{ok, drop, -ar_block:get_sub_chunk_size(Candidate#mining_candidate.packing_difficulty)};
			(CachedValue) ->
				%% The corresponding chunk1 is not missing but we didn't read it yet, so
				%% we just mark the chunk2 as missing and continue.
				%% Drop the reservation for a single subchunk.
				{ok, CachedValue#ar_mining_cache_value{chunk2_missing = true}, -ar_block:get_sub_chunk_size(Candidate#mining_candidate.packing_difficulty)}
		end
	) of
		{ok, ChunkCache1} ->
			mark_single_chunk2_missing_or_drop(Nonce + 1, NoncesLeft - 1, Candidate, State#state{ chunk_cache = ChunkCache1 });
		{error, Reason} ->
			%% NB: this clause may cause a memory leak, because mining worker will wait for
			%% chunk2 to arrive.
			?LOG_ERROR([{event, mining_worker_failed_to_mark_chunk2_missing}, {reason, Reason}]),
			mark_single_chunk2_missing_or_drop(Nonce + 1, NoncesLeft - 1, Candidate, State)
	end.

%% @doc Mark the chunk2 as missing for the whole recall range.
mark_second_recall_range_missing(Candidate, State) ->
	#mining_candidate{ packing_difficulty = PackingDifficulty } = Candidate,
	mark_second_recall_range_missing(0, ar_block:get_max_nonce(PackingDifficulty), Candidate, State).

mark_second_recall_range_missing(_Nonce, 0, _Candidate, State) -> State;
mark_second_recall_range_missing(Nonce, NoncesLeft, Candidate, State) ->
	case ar_mining_cache:with_cached_value(
		?CACHE_KEY(Candidate#mining_candidate.cache_ref, Nonce),
		Candidate#mining_candidate.session_key,
		State#state.chunk_cache,
		fun(CachedValue) -> {ok, CachedValue#ar_mining_cache_value{chunk2_missing = true}} end
	) of
		{ok, ChunkCache1} ->
			mark_second_recall_range_missing(Nonce + 1, NoncesLeft - 1, Candidate, State#state{ chunk_cache = ChunkCache1 });
		{error, Reason} ->
			%% NB: this clause may cause a memory leak, because mining worker will wait for
			%% chunk2 to arrive.
			?LOG_ERROR([{event, mining_worker_failed_to_add_chunk_to_cache}, {reason, Reason}]),
			mark_second_recall_range_missing(Nonce + 1, NoncesLeft - 1, Candidate, State)
	end.

cache_h1_list(_Candidate, [], State) -> State;
cache_h1_list(#mining_candidate{ cache_ref = not_set } = _Candidate, [], State) -> State;
cache_h1_list(Candidate, [ {H1, Nonce} | H1List ], State) ->
	case ar_mining_cache:with_cached_value(
		?CACHE_KEY(Candidate#mining_candidate.cache_ref, Nonce),
		Candidate#mining_candidate.session_key,
		State#state.chunk_cache,
		fun(CachedValue) ->
			%% Store the H1 received from peer, and set chunk2_missing to false,
			%% marking that we have a recall range for this H1 list.
			{ok, CachedValue#ar_mining_cache_value{h1 = H1, chunk2_missing = false}}
		end
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
	prometheus_gauge:set(mining_server_chunk_cache_size, [Partition, "total"], ar_mining_cache:cache_size(ChunkCache)),
	case ar_mining_cache:reserved_size(ChunkCache) of
		{ok, ReservedSize} -> prometheus_gauge:set(mining_server_chunk_cache_size, [Partition, "reserved"], ReservedSize);
		{error, Reason} ->
			?LOG_ERROR([{event, mining_worker_failed_to_report_chunk_cache_metrics}, {reason, Reason}])
	end,
	State.

%%%===================================================================
%%% Public Test interface.
%%%===================================================================
