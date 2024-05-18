-module(ar_mining_worker).

-behaviour(gen_server).

-export([start_link/1, name/1, reset/2, set_sessions/2,  recall_chunk/5, computed_hash/5,
		set_difficulty/2, set_cache_limits/3, add_task/3, garbage_collect/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	name						= not_set,
	partition_number			= not_set,
	diff_pair					= not_set,
	task_queue					= gb_sets:new(),
	active_sessions				= sets:new(),
	chunk_cache 				= #{},
	chunk_cache_size			= #{},
	chunk_cache_limit			= 0,
	vdf_queue_limit				= 0,
	latest_vdf_step_number		= 0,
	is_pool_client				= false
}).

-define(TASK_CHECK_FREQUENCY_MS, 200).
-define(STATUS_CHECK_FREQUENCY_MS, 5000).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the gen_server.
start_link(Partition) ->
	Name = name(Partition),
	gen_server:start_link({local, Name}, ?MODULE, Partition, []).

name(Partition) ->
	list_to_atom("ar_mining_worker_" ++ integer_to_list(Partition)).

reset(Worker, DiffPair) ->
	gen_server:cast(Worker, {reset, DiffPair}).

set_sessions(Worker, ActiveSessions) ->
	gen_server:cast(Worker, {set_sessions, ActiveSessions}).

add_task(Worker, TaskType, Candidate) ->
	gen_server:cast(Worker, {add_task, {TaskType, Candidate}}).

add_delayed_task(Worker, TaskType, Candidate) ->
	%% Delay task by random amount between ?TASK_CHECK_FREQUENCY_MS and 2*?TASK_CHECK_FREQUENCY_MS
	%% The reason for the randomization to avoid a glut tasks to all get added at the same time - 
	%% in particular when the chunk cache fills up it's possible for all queued compute_h0 tasks
	%% to be delayed at about the same time.
	Delay = rand:uniform(?TASK_CHECK_FREQUENCY_MS) + ?TASK_CHECK_FREQUENCY_MS,
	ar_util:cast_after(Delay, Worker, {add_task, {TaskType, Candidate}}).

%% @doc Callback from ar_mining_io when a chunk is read
recall_chunk(Worker, chunk1, Chunk, Nonce, Candidate) ->
	ar_mining_stats:chunk_read(Candidate#mining_candidate.partition_number),
	add_task(Worker, chunk1, Candidate#mining_candidate{ chunk1 = Chunk, nonce = Nonce });
recall_chunk(Worker, chunk2, Chunk, Nonce, Candidate) ->
	ar_mining_stats:chunk_read(Candidate#mining_candidate.partition_number2),
	add_task(Worker, chunk2, Candidate#mining_candidate{ chunk2 = Chunk, nonce = Nonce });
recall_chunk(Worker, skipped, _WhichChunk, Nonce, Candidate) ->
	gen_server:cast(Worker,
		{remove_chunk_from_cache, Candidate#mining_candidate{ nonce = Nonce }}).

%% @doc Callback from the hashing threads when a hash is computed
computed_hash(Worker, computed_h0, H0, undefined, Candidate) ->
	add_task(Worker, computed_h0, Candidate#mining_candidate{ h0 = H0 });
computed_hash(Worker, computed_h1, H1, Preimage, Candidate) ->
	ar_mining_stats:h1_computed(Candidate#mining_candidate.partition_number),
	add_task(Worker, computed_h1, Candidate#mining_candidate{ h1 = H1, preimage = Preimage });
computed_hash(Worker, computed_h2, H2, Preimage, Candidate) ->
	ar_mining_stats:h2_computed(Candidate#mining_candidate.partition_number2),
	add_task(Worker, computed_h2, Candidate#mining_candidate{ h2 = H2, preimage = Preimage }).

%% @doc Set the new mining difficulty. We do not recalculate it inside the mining
%% server or worker because we want to completely detach the mining server from the block
%% ordering. The previous block is chosen only after the mining solution is found (if
%% we choose it in advance we may miss a better option arriving in the process).
%% Also, a mining session may (in practice, almost always will) span several blocks.
set_difficulty(Worker, DiffPair) ->
	gen_server:cast(Worker, {set_difficulty, DiffPair}).

set_cache_limits(Worker, ChunkCacheLimit, VDFQueueLimit) ->
	gen_server:cast(Worker, {set_cache_limits, ChunkCacheLimit, VDFQueueLimit}).

%% @doc Returns true if the mining candidate belongs to a valid mining session. Always assume
%% that a coordinated mining candidate is valid (its cm_lead_peer is set)
is_session_valid(_State, #mining_candidate{ cm_lead_peer = Peer })
		when Peer /= not_set ->
	true;
is_session_valid(
		#state{ active_sessions = Sessions },
		#mining_candidate{ session_key = SessionKey }) ->
	sets:is_element(SessionKey, Sessions).

garbage_collect(Worker) ->
	gen_server:cast(Worker, garbage_collect).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Partition) ->
	Name = name(Partition),
	?LOG_DEBUG([{event, mining_debug_worker_started},
		{worker, Name}, {pid, self()}, {partition, Partition}]),
	process_flag(trap_exit, true),
	ar_chunk_storage:open_files("default"),
	gen_server:cast(self(), handle_task),
	gen_server:cast(self(), check_worker_status),
	prometheus_gauge:set(mining_server_chunk_cache_size, [Partition], 0),
	{ok, #state{ name = Name, partition_number = Partition,
			is_pool_client = ar_pool:is_client() }}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({set_difficulty, DiffPair}, State) ->
	{noreply, State#state{ diff_pair = DiffPair }};

handle_cast({set_cache_limits, ChunkCacheLimit, VDFQueueLimit}, State) ->
	{noreply, State#state{ chunk_cache_limit = ChunkCacheLimit,
			vdf_queue_limit = VDFQueueLimit }};

handle_cast({reset, DiffPair}, State) ->
	State2 = update_sessions(sets:new(), State),
	{noreply, State2#state{ diff_pair = DiffPair }};

handle_cast({set_sessions, ActiveSessions}, State) ->
	{noreply, update_sessions(ActiveSessions, State)};

handle_cast({add_task, {TaskType, Candidate} = Task}, State) ->
	#state{ task_queue = Q } = State,
	case is_session_valid(State, Candidate) of
		true ->
			StepNumber = Candidate#mining_candidate.step_number,
			Q2 = gb_sets:insert({priority(TaskType, StepNumber), make_ref(), Task}, Q),
			prometheus_gauge:inc(mining_server_task_queue_len, [TaskType]),
			{noreply, State#state{ task_queue = Q2 }};
		false ->
			?LOG_DEBUG([{event, mining_debug_add_stale_task},
				{worker, State#state.name},
				{task, TaskType},
				{active_sessions,
					ar_mining_server:encode_sessions(State#state.active_sessions)},
				{candidate_session, 
					ar_nonce_limiter:encode_session_key(Candidate#mining_candidate.session_key)},
				{partition_number, Candidate#mining_candidate.partition_number},
				{step_number, Candidate#mining_candidate.step_number},
				{nonce, Candidate#mining_candidate.nonce}]),
			{noreply, State}
	end;
	
handle_cast(handle_task, #state{ task_queue = Q } = State) ->
	case gb_sets:is_empty(Q) of
		true ->
			ar_util:cast_after(?TASK_CHECK_FREQUENCY_MS, self(), handle_task),
			{noreply, State};
		_ ->
			{{_Priority, _ID, Task}, Q2} = gb_sets:take_smallest(Q),
			{TaskType, Candidate} = Task,
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
							ar_mining_server:encode_sessions(State#state.active_sessions)},
						{candidate_session, ar_nonce_limiter:encode_session_key(
							Candidate#mining_candidate.session_key)},
						{partition_number, Candidate#mining_candidate.partition_number},
						{step_number, Candidate#mining_candidate.step_number},
						{nonce, Candidate#mining_candidate.nonce}]),
					{noreply, State}
			end
	end;

handle_cast({remove_chunk_from_cache, Candidate}, State) ->
	{noreply, remove_chunk_from_cache(Candidate, State)};

handle_cast(check_worker_status, State) ->
	maybe_warn_about_lag(State#state.task_queue, State#state.name),
	maybe_warn_about_stale_chunks(State),
	ar_util:cast_after(?STATUS_CHECK_FREQUENCY_MS, self(), check_worker_status),
	{noreply, State};

handle_cast(garbage_collect, State) ->
	erlang:garbage_collect(self(), [{async, erlang:monotonic_time()}]),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({garbage_collect, StartTime, GCResult}, State) ->
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

handle_info({fetched_last_moment_proof, _}, State) ->
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

handle_task({chunk1, Candidate}, State) ->
	ar_mining_hash:compute_h1(self(), Candidate),
	{noreply, State};

handle_task({chunk2, Candidate}, State) ->
	#mining_candidate{ chunk2 = Chunk2, session_key = SessionKey } = Candidate,
	case cycle_chunk_cache(Candidate, {chunk2, Chunk2}, State) of
		{{chunk1, Chunk1, H1}, State2} ->
			ar_mining_hash:compute_h2(
				self(), Candidate#mining_candidate{ chunk1 = Chunk1, h1 = H1 }),
			%% Decrement 2 for chunk1 and chunk2:
			%% 1. chunk1 was previously read and cached
			%% 2. chunk2 that was just read and will shortly be used to compute h2
			{noreply, update_chunk_cache_size(-2, SessionKey, State2)};
		{{chunk1, H1}, State2} ->
			ar_mining_hash:compute_h2(self(), Candidate#mining_candidate{ h1 = H1 }),
			%% Decrement 1 for chunk2:
			%% we're computing h2 for a peer so chunk1 was not previously read or cached 
			%% on this node
			{noreply, update_chunk_cache_size(-1, SessionKey, State2)};
		{do_not_cache, State2} ->
			%% Decrement 1 for chunk2
			%% do_not_cache indicates chunk1 was not and will not be read or cached
			{noreply, update_chunk_cache_size(-1, SessionKey, State2)};
		{cached, State2} ->
			case Candidate#mining_candidate.cm_lead_peer of
				not_set ->
					ok;
				_ ->
					?LOG_ERROR([{event, cm_chunk2_cached_before_chunk1},
						{worker, State#state.name},
						{partition_number, Candidate#mining_candidate.partition_number},
						{partition_number2, Candidate#mining_candidate.partition_number2},
						{cm_peer, ar_util:format_peer(Candidate#mining_candidate.cm_lead_peer)},
						{cache_ref, Candidate#mining_candidate.cache_ref},
						{nonce, Candidate#mining_candidate.nonce},
						{session, ar_nonce_limiter:encode_session_key(SessionKey)}])
			end,
			{noreply, State2}
	end;

handle_task({compute_h0, Candidate}, State) ->
	#state{ latest_vdf_step_number = LatestVDFStepNumber,
			vdf_queue_limit = VDFQueueLimit } = State,
	#mining_candidate{ session_key = SessionKey, step_number = StepNumber } = Candidate,
	State3 = case try_to_reserve_cache_space(SessionKey, State) of
		{true, State2} ->
			ar_mining_hash:compute_h0(self(), Candidate),
			case StepNumber > LatestVDFStepNumber of
				true ->
					State2#state{ latest_vdf_step_number = StepNumber };
				false ->
					State2
			end;
		false ->
			case StepNumber >= LatestVDFStepNumber - VDFQueueLimit of
				true ->
					%% Wait a bit, and then re-add the task.
					add_delayed_task(self(), compute_h0, Candidate);
				false ->
					ok
			end,
			
			State
	end,
	{noreply, State3};

handle_task({computed_h0, Candidate}, State) ->
	#mining_candidate{ session_key = SessionKey, h0 = H0, partition_number = Partition1,
				partition_upper_bound = PartitionUpperBound } = Candidate,
	RecallRangeChunks = nonce_max() + 1,
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
					State2 = update_chunk_cache_size(-RecallRangeChunks, SessionKey, State),
					do_not_cache(Candidate3, State2)
			end;
		false ->
			?LOG_DEBUG([{event, mining_debug_no_io_thread_found_for_range},
				{worker, State#state.name},
				{partition_number, Partition1},
				{range_start, RecallRange1Start},
				{range_end, RecallRange1Start + ?RECALL_RANGE_SIZE}]),
			%% Release the Range1 *and* Range2 cache space we reserved with
			%% try_to_reserve_cache_space/2
			update_chunk_cache_size(-(2*RecallRangeChunks), SessionKey, State)	
	end,
	{noreply, State3};

handle_task({computed_h1, Candidate}, State) ->
	#mining_candidate{ h1 = H1, chunk1 = Chunk1, session_key = SessionKey } = Candidate,
	case h1_passes_diff_checks(H1, Candidate, State) of
		true ->
			?LOG_INFO([{event, found_h1_solution}, {worker, State#state.name},
				{h1, ar_util:encode(H1)}, {difficulty, get_difficulty(State, Candidate)}]),
			ar_mining_stats:h1_solution(),
			%% Decrement 1 for chunk1:
			%% Since we found a solution we won't need chunk2 (and it will be evicted if
			%% necessary below)
			State2 = remove_chunk_from_cache(Candidate, State),
			ar_mining_server:prepare_and_post_solution(Candidate),
			{noreply, State2};
		Result ->
			case Result of
				partial ->
					ar_mining_server:prepare_and_post_solution(Candidate);
				_ ->
					ok
			end,
			{ok, Config} = application:get_env(arweave, config),
			case cycle_chunk_cache(Candidate, {chunk1, Chunk1, H1}, State) of
				{cached, State2} ->
					%% Chunk2 hasn't been read yet, so we cache Chunk1 and wait for
					%% Chunk2 to be read.
					{noreply, State2};
				{do_not_cache, State2} ->
					%% This node does not store Chunk2. If we're part of a coordinated
					%% mining set, we can try one of our peers, otherwise we're done.
					case Config#config.coordinated_mining of
						false ->
							ok;
						true ->
							DiffPair =
								case get_partial_difficulty(State, Candidate) of
									not_set ->
										get_difficulty(State, Candidate);
									PartialDiffPair ->
										PartialDiffPair
								end,
							ar_coordination:computed_h1(Candidate, DiffPair)
					end,
					%% Decrement 1 for chunk1:
					%% do_not_cache indicates chunk2 was not and will not be read or cached
					{noreply, update_chunk_cache_size(-1, SessionKey, State2)};
				{{chunk2, Chunk2}, State2} ->
					%% Chunk2 has already been read, so we can compute H2 now.
					ar_mining_hash:compute_h2(
						self(), Candidate#mining_candidate{ chunk2 = Chunk2 }),
					%% Decrement 2 for chunk1 and chunk2:
					%% 1. chunk2 was previously read and cached
					%% 2. chunk1 that was just read and used to compute H1	
					{noreply, update_chunk_cache_size(-2, SessionKey, State2)}
			end
	end;

handle_task({computed_h2, Candidate}, State) ->
	#mining_candidate{
		chunk2 = Chunk2, h0 = H0, h2 = H2, mining_address = MiningAddress,
		nonce = Nonce, partition_number = Partition1, 
		partition_upper_bound = PartitionUpperBound, cm_lead_peer = Peer
	} = Candidate,
	PassesDiffChecks = h2_passes_diff_checks(H2, Candidate, State),
	case PassesDiffChecks of
		false ->
			ok;
		true ->
			?LOG_INFO([{event, found_h2_solution},
					{worker, State#state.name},
					{h2, ar_util:encode(H2)},
					{difficulty, get_difficulty(State, Candidate)},
					{partial_difficulty, get_partial_difficulty(State, Candidate)}]),
			ar_mining_stats:h2_solution();
		partial ->
			?LOG_INFO([{event, found_h2_partial_solution},
					{worker, State#state.name},
					{h2, ar_util:encode(H2)},
					{partial_difficulty, get_partial_difficulty(State, Candidate)}])
	end,
	case {PassesDiffChecks, Peer} of
		{false, _} ->
			ok;
		{_, not_set} ->
			ar_mining_server:prepare_and_post_solution(Candidate);
		_ ->
			{_RecallByte1, RecallByte2} = ar_mining_server:get_recall_bytes(H0, Partition1,
					Nonce, PartitionUpperBound),
			LocalPoA2 = ar_mining_server:read_poa(RecallByte2, Chunk2, MiningAddress),
			PoA2 =
				case LocalPoA2 of
					{ok, LocalPoA3} ->
						LocalPoA3;
					_ ->
						ar:console("WARNING: we have found an H2 solution but did not find "
							"the PoA2 proofs locally - searching the peers...~n"),
						case ar_mining_server:fetch_poa_from_peers(RecallByte2) of
							not_found ->
								?LOG_WARNING([{event,
										mined_block_but_failed_to_read_second_chunk_proof},
										{worker, State#state.name},
										{recall_byte2, RecallByte2},
										{mining_address, ar_util:safe_encode(MiningAddress)}]),
								ar:console("WARNING: we found an H2 solution but failed to find "
										"the proof for the second chunk. See logs for more "
										"details.~n"),
								not_found;
							PeerPoA2 ->
								PeerPoA2
						end
				end,
			case PoA2 of
				not_found ->
					ok;
				_ ->
					ar_coordination:computed_h2_for_peer(
							Candidate#mining_candidate{ poa2 = PoA2 })
			end
	end,
	{noreply, State};

handle_task({compute_h2_for_peer, Candidate}, State) ->
	#mining_candidate{
		session_key = SessionKey,
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
			%% *even if* this puts us over the chunk cache limit. This may have to be revisited
			%% later if we find that this causes unacceptable memory bloat.
			RecallRangeChunks = nonce_max() + 1,
			State2 = update_chunk_cache_size(RecallRangeChunks, SessionKey, State),
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

h1_passes_diff_checks(H1, Candidate, State) ->
	passes_diff_checks(H1, true, Candidate, State).

h2_passes_diff_checks(H2, Candidate, State) ->
	passes_diff_checks(H2, false, Candidate, State).

passes_diff_checks(SolutionHash, IsPoA1, Candidate, State) ->
	DiffPair = get_difficulty(State, Candidate),
	case ar_node_utils:passes_diff_check(SolutionHash, IsPoA1, DiffPair) of
		true ->
			true;
		false ->
			case get_partial_difficulty(State, Candidate) of
				not_set ->
					false;
				PartialDiffPair ->
					case ar_node_utils:passes_diff_check(SolutionHash, IsPoA1,
							PartialDiffPair) of
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

maybe_warn_about_stale_chunks(State) ->
	TotalChunkKeys = 
		maps:fold(
		fun(_SesssionKey, SessionCache, Acc) ->
			Acc + maps:size(SessionCache)
		end,
		0,
		State#state.chunk_cache),
	TotalCacheSize = total_cache_size(State),
	case TotalChunkKeys > TotalCacheSize orelse TotalCacheSize < 0 of
		true ->
			?LOG_DEBUG([
				{event, mining_worker_chunk_cache_mismatch},
				{worker, State#state.name},
				{partition, State#state.partition_number},
				{chunk_cache_keys, TotalChunkKeys},
				{chunk_cache_size, TotalCacheSize}]);
		false ->
			ok
	end.

update_sessions(ActiveSessions, State) ->
	CurrentSessions = State#state.active_sessions,
	AddedSessions = sets:to_list(sets:subtract(ActiveSessions, CurrentSessions)),
	RemovedSessions = sets:to_list(sets:subtract(CurrentSessions, ActiveSessions)),

	State2 = remove_sessions(RemovedSessions, State),
	State3 = add_sessions(AddedSessions, State2),
	State3#state{ active_sessions = ActiveSessions }.

%% We no longer have to do anything when adding a session as the chunk cache will be
%% automatically created the first time it is updated. This function now serves to log
%% the sessions being added.
add_sessions([], State) ->
	State;
add_sessions([SessionKey | AddedSessions], State) ->
	?LOG_DEBUG([{event, mining_debug_add_session},
		{worker, State#state.name}, {partition, State#state.partition_number},
		{session_key, ar_nonce_limiter:encode_session_key(SessionKey)}]),
	add_sessions(AddedSessions, State).

remove_sessions([], State) ->
	State;
remove_sessions([SessionKey | RemovedSessions], State) ->
	ChunksDiscarded = maps:get(SessionKey, State#state.chunk_cache_size, 0),
	TaskQueue = remove_tasks(SessionKey, State#state.task_queue),
	TasksDiscarded = gb_sets:size(State#state.task_queue) - gb_sets:size(TaskQueue),

	State2 = update_chunk_cache_size(-ChunksDiscarded, SessionKey, State),
	?LOG_DEBUG([{event, mining_debug_remove_session},
		{worker, State#state.name}, {partition, State#state.partition_number},
		{session, ar_nonce_limiter:encode_session_key(SessionKey)},
		{tasks_discarded, TasksDiscarded },
		{chunks_discarded, ChunksDiscarded}]),

	State3 = State2#state{
		task_queue = TaskQueue,
		chunk_cache = maps:remove(SessionKey, State#state.chunk_cache),
		chunk_cache_size = maps:remove(SessionKey, State#state.chunk_cache_size)
	},
	remove_sessions(RemovedSessions, State3).

remove_tasks(SessionKey, TaskQueue) ->
	gb_sets:filter(
		fun({_Priority, _ID, {TaskType, Candidate}}) ->
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

total_cache_size(State) ->
	maps:fold(
		fun(_Key, Value, Acc) ->
			Value + Acc
		end,
		0,
		State#state.chunk_cache_size).

update_chunk_cache_size(0, _SessionKey, State) ->
	State;
update_chunk_cache_size(Delta, SessionKey, State) ->
	CacheSize = maps:get(SessionKey, State#state.chunk_cache_size, 0),
	prometheus_gauge:inc(mining_server_chunk_cache_size, [State#state.partition_number], Delta),
	State#state{
		chunk_cache_size = maps:put(SessionKey, CacheSize + Delta, State#state.chunk_cache_size) }.

try_to_reserve_cache_space(SessionKey, State) ->
	RecallRangeChunks = nonce_max() + 1,
	case total_cache_size(State) =< State#state.chunk_cache_limit of
		true ->
			%% reserve for both h1 and h2
			{true, update_chunk_cache_size(2 * RecallRangeChunks, SessionKey, State)};
		false ->
			false
	end.

do_not_cache(Candidate, State) ->
	do_not_cache(0, nonce_max(), Candidate, State).

do_not_cache(Nonce, NonceMax, _Candidate, State)
		when Nonce > NonceMax ->
	State;
do_not_cache(Nonce, NonceMax, Candidate, State) ->
	State2 = cache_chunk(do_not_cache, Candidate#mining_candidate{ nonce = Nonce }, State),
	do_not_cache(Nonce + 1, NonceMax, Candidate, State2).

%% @doc The chunk_cache stores either the first or second chunk for a given nonce. This is because
%% we process both the first and second recall ranges in parallel and don't know
%% which data will be available first. The function manages that shared cache slot by either
%% caching data if its the first to arrive, or "popping" data that was previously cached. The
%% caller is responsible for taking the appropriate action based on the return value.
%%
%% do_not_cache is a special value used to prevent unnecessary data from being cached once a
%% solution has been found for a given nonce.
cycle_chunk_cache(#mining_candidate{ cache_ref = CacheRef } = Candidate, Data, State)
  		when CacheRef /= not_set ->
	#mining_candidate{ nonce = Nonce, session_key = SessionKey } = Candidate,
	Cache = State#state.chunk_cache,
	SessionCache = maps:get(SessionKey, Cache, #{}),
	case maps:take({CacheRef, Nonce}, SessionCache) of
		{do_not_cache, SessionCache2} ->
			Cache2 = maps:put(SessionKey, SessionCache2, Cache),
			{do_not_cache, State#state{ chunk_cache = Cache2 }};
		error ->
			{cached, cache_chunk(Data, Candidate, State)};
		{CachedData, SessionCache2} ->
			Cache2 = maps:put(SessionKey, SessionCache2, Cache),
			{CachedData, State#state{ chunk_cache = Cache2 }}
	end.

remove_chunk_from_cache(#mining_candidate{ cache_ref = CacheRef } = Candidate, State)
		when CacheRef /= not_set ->
	#mining_candidate{ nonce = Nonce, session_key = SessionKey } = Candidate,
	Cache = State#state.chunk_cache,
	SessionCache = maps:get(SessionKey, Cache, #{}),
	%% Decrement the cache size by 1 for the chunk being removed. We may decrement the cache
	%% size further depending on what's already cached.
	State2 = update_chunk_cache_size(-1, SessionKey, State),
	case maps:take({CacheRef, Nonce}, SessionCache) of
		{do_not_cache, SessionCache2} ->
			Cache2 = maps:put(SessionKey, SessionCache2, Cache),
			State2#state{ chunk_cache = Cache2 };
		error ->
			cache_chunk(do_not_cache, Candidate, State2);
		{{chunk1, _H1}, SessionCache2} ->
			%% If we find data from a CM peer, discard it but don't decrement the cache size
			Cache2 = maps:put(SessionKey, SessionCache2, Cache),
			State2#state{ chunk_cache = Cache2 };
		{_, SessionCache2} ->
			%% if we find any cached data, discard it and decrement the cache size
			Cache2 = maps:put(SessionKey, SessionCache2, Cache),
			update_chunk_cache_size(-1, SessionKey, State2#state{ chunk_cache = Cache2 })
	end.

cache_chunk(Data, Candidate, State) ->
	#mining_candidate{ cache_ref = CacheRef, nonce = Nonce, session_key = SessionKey } = Candidate,
	Cache = State#state.chunk_cache,
	SessionCache = maps:get(SessionKey, Cache, #{}),
	SessionCache2 = maps:put({CacheRef, Nonce}, Data, SessionCache),
	Cache2 = maps:put(SessionKey, SessionCache2, Cache),
	State#state{ chunk_cache = Cache2 }.

cache_h1_list(_Candidate, [], State) ->
	State;
cache_h1_list(
		#mining_candidate{ cache_ref = CacheRef } = Candidate,
		[ {H1, Nonce} | H1List ], State) when CacheRef /= not_set ->
	State2 = cache_chunk({chunk1, H1}, Candidate#mining_candidate{ nonce = Nonce }, State),
	cache_h1_list(Candidate, H1List, State2).

get_difficulty(State, #mining_candidate{ cm_diff = not_set }) ->
	State#state.diff_pair;
get_difficulty(_State, #mining_candidate{ cm_diff = DiffPair }) ->
	DiffPair.

get_partial_difficulty(#state{ is_pool_client = false }, _Candidate) ->
	not_set;
get_partial_difficulty(_State, #mining_candidate{ cm_diff = DiffPair }) ->
	DiffPair.

nonce_max() ->
	max(0, ((?RECALL_RANGE_SIZE) div ?DATA_CHUNK_SIZE - 1)).

generate_cache_ref(Candidate) ->
	#mining_candidate{
		partition_number = Partition1, partition_number2 = Partition2,
		partition_upper_bound = PartitionUpperBound } = Candidate,
	CacheRef = {Partition1, Partition2, PartitionUpperBound, make_ref()},
	Candidate#mining_candidate{ cache_ref = CacheRef }.

%%%===================================================================
%%% Public Test interface.
%%%===================================================================
