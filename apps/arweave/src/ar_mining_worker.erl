-module(ar_mining_worker).

-behaviour(gen_server).

-export([start_link/1, reset/2, new_session/2, 
	recall_chunk/5, computed_hash/5, set_difficulty/2, add_task/3]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	name						= not_set,
	diff						= infinity,
	task_queue					= gb_sets:new(),
	session_key					= not_set,
	seed						= not_set,
	chunk_cache 				= #{},
	chunk_cache_size			= #{}
}).

-define(TASK_CHECK_FREQUENCY_MS, 200).
-define(LAG_CHECK_FREQUENCY_MS, 5000).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the gen_server.
start_link(Name) ->
	gen_server:start_link({local, Name}, ?MODULE, Name, []).

reset(Worker, Diff) ->
	gen_server:cast(Worker, {reset, Diff}).

new_session(Worker, SessionKey) ->
	gen_server:cast(Worker, {new_session, SessionKey}).

add_task(Worker, TaskType, Candidate) ->
	gen_server:cast(Worker, {add_task, {TaskType, Candidate}}).

add_delayed_task(Worker, TaskType, Candidate) ->
	ar_util:cast_after(?TASK_CHECK_FREQUENCY_MS, Worker, {add_task, {TaskType, Candidate}}).

%% @doc Callback from ar_mining_io when a chunk is read
recall_chunk(Worker, chunk1, Chunk, Nonce, Candidate) ->
	ar_mining_stats:chunk_read(Candidate#mining_candidate.partition_number),
	add_task(Worker, chunk1, Candidate#mining_candidate{ chunk1 = Chunk, nonce = Nonce });
recall_chunk(Worker, chunk2, Chunk, Nonce, Candidate) ->
	ar_mining_stats:chunk_read(Candidate#mining_candidate.partition_number2),
	add_task(Worker, chunk2, Candidate#mining_candidate{ chunk2 = Chunk, nonce = Nonce });
recall_chunk(Worker, skipped, WhichChunk, Nonce, Candidate) ->
	gen_server:cast(Worker,
		{remove_chunk_from_cache, WhichChunk,  Candidate#mining_candidate{ nonce = Nonce }}).

%% @doc Callback from the hashing threads when a hash is computed
computed_hash(Worker, computed_h0, H0, undefined, Candidate) ->
	ar_mining_stats:hash_computed(Candidate#mining_candidate.partition_number),
	add_task(Worker, computed_h0, Candidate#mining_candidate{ h0 = H0 });
computed_hash(Worker, computed_h1, H1, Preimage, Candidate) ->
	ar_mining_stats:hash_computed(Candidate#mining_candidate.partition_number),
	add_task(Worker, computed_h1, Candidate#mining_candidate{ h1 = H1, preimage = Preimage });
computed_hash(Worker, computed_h2, H2, Preimage, Candidate) ->
	ar_mining_stats:hash_computed(Candidate#mining_candidate.partition_number2),
	add_task(Worker, computed_h2, Candidate#mining_candidate{ h2 = H2, preimage = Preimage }).

%% @doc Set the new mining difficulty. We do not recalculate it inside the mining
%% server or worker because we want to completely detach the mining server from the block
%% ordering. The previous block is chosen only after the mining solution is found (if
%% we choose it in advance we may miss a better option arriving in the process).
%% Also, a mining session may (in practice, almost always will) span several blocks.
set_difficulty(Worker, Diff) ->
	gen_server:cast(Worker, {set_difficulty, Diff}).

%% @doc Returns true if the mining candidate belongs to a valid mining session. Always assume
%% that a coordinated mining candidate is valid (its session_key is not_set)
is_session_valid(
		#state{ session_key = SessionKey },
		#mining_candidate{ session_key = SessionKey }) ->
	true;
is_session_valid(_State, _Candidate) ->
	false.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Name) ->
	?LOG_DEBUG([{event, mining_debug_worker_started}, {worker, Name}]),
	process_flag(trap_exit, true),
	ar_chunk_storage:open_files("default"),
	gen_server:cast(self(), handle_task),
	gen_server:cast(self(), maybe_warn_about_lag),
	{ok, #state{ name = Name }}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({set_difficulty, Diff}, State) ->
	{noreply, State#state{ diff = Diff }};

handle_cast({reset, Diff}, State) ->
	{State2, TasksDiscarded, ChunksDiscarded} = reset(State),
	?LOG_DEBUG([{event, mining_debug_reset}, {worker, State#state.name}, {diff, Diff},
		{tasks_discarded, TasksDiscarded },
		{chunks_discarded, ChunksDiscarded}]),
	{noreply, State2#state{ diff = Diff }};

handle_cast({new_session, SessionKey}, State) ->
	Session = ar_nonce_limiter:get_session(SessionKey),
	#vdf_session{ seed = Seed } = Session,	

	{State2, TasksDiscarded, ChunksDiscarded} = reset(State),
	?LOG_DEBUG([{event, mining_debug_new_session}, {worker, State#state.name},
		{session_key, ar_nonce_limiter:encode_session_key(SessionKey)},
		{tasks_discarded, TasksDiscarded },
		{chunks_discarded, ChunksDiscarded}]),

	{noreply, State2#state{ session_key = SessionKey, seed = Seed }};

handle_cast({add_task, {TaskType, Candidate} = Task}, State) ->
	#state{ task_queue = Q } = State,
	case is_session_valid(State, Candidate) of
		true ->
			StepNumber = Candidate#mining_candidate.step_number,
			Q2 = gb_sets:insert({priority(TaskType, StepNumber), make_ref(), Task}, Q),
			prometheus_gauge:inc(mining_server_task_queue_len, [TaskType]),
			{noreply, State#state{ task_queue = Q2 }};
		false ->
			#state{ session_key = SessionKey } = State,
			?LOG_DEBUG([{event, mining_debug_add_stale_task},
				{worker, State#state.name},
				{task, TaskType},
				{current_session, ar_nonce_limiter:encode_session_key(SessionKey)},
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
					#state{ session_key = SessionKey } = State,
					?LOG_DEBUG([{event, mining_debug_handle_stale_task},
						{worker, State#state.name},
						{task, TaskType},
						{current_session, ar_nonce_limiter:encode_session_key(SessionKey)},
						{candidate_session, ar_nonce_limiter:encode_session_key(
							Candidate#mining_candidate.session_key)},
						{partition_number, Candidate#mining_candidate.partition_number},
						{step_number, Candidate#mining_candidate.step_number},
						{nonce, Candidate#mining_candidate.nonce}]),
					{noreply, State}
			end
	end;

handle_cast({remove_chunk_from_cache, WhichChunk, Candidate}, State) ->
	{noreply, remove_chunk_from_cache(WhichChunk, Candidate, State)};

handle_cast(maybe_warn_about_lag, State) ->
	maybe_warn_about_lag(State#state.task_queue, State#state.name),
	ar_util:cast_after(?LAG_CHECK_FREQUENCY_MS, self(), maybe_warn_about_lag),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

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
	#state{ chunk_cache = Cache } = State,
	#mining_candidate{ chunk2 = Chunk2,
		partition_number = Partition1, partition_number2 = Partition2 } = Candidate,
	case cycle_chunk_cache(Candidate, {chunk2, Chunk2}, Cache) of
		{{chunk1, Chunk1, H1}, Cache2} ->
			ar_mining_hash:compute_h2(
				self(), Candidate#mining_candidate{ chunk1 = Chunk1, h1 = H1 }),
			%% Decrement 2 for chunk1 and chunk2:
			%% 1. chunk1 was previously read and cached
			%% 2. chunk2 that was just read and will shortly be used to compute h2
			State2 = update_chunk_cache_size(Partition1, -1, State#state{ chunk_cache = Cache2 }),
			State3 = update_chunk_cache_size(Partition2, -1, State2),
			{noreply, State3};
		{{chunk1, H1}, Cache2} ->
			ar_mining_hash:compute_h2(self(), Candidate#mining_candidate{ h1 = H1 }),
			%% Decrement 1 for chunk2:
			%% we're computing h2 for a peer so chunk1 was not previously read or cached 
			%% on this node
			{noreply, update_chunk_cache_size(Partition2, -1, State#state{ chunk_cache = Cache2 })};
		{do_not_cache, Cache2} ->
			%% Decrement 1 for chunk2
			%% do_not_cache indicates chunk1 was not and will not be read or cached
			{noreply, update_chunk_cache_size(Partition2, -1, State#state{ chunk_cache = Cache2 })};
		{cached, Cache2} ->
			{noreply, State#state{ chunk_cache = Cache2 }}
	end;

handle_task({compute_h0, Candidate}, State) ->
	#state{ seed = Seed } = State,
	ar_mining_hash:compute_h0(
		self(),
		Candidate#mining_candidate{
			seed = Seed
		}),
	{noreply, State};

handle_task({computed_h0, Candidate}, State) ->
	#mining_candidate{ h0 = H0, partition_number = Partition1,
				partition_upper_bound = PartitionUpperBound } = Candidate,
	%% We only check if Partition1 has cache space so it's possible that we will exceed
	%% the cache when mining Partition2. However in the worst case this should only exceed
	%% the cache limit by a marginal amount, since all further chunk1 reads on Partition2 will
	%% be blocked until the cache frees up.
	case ar_mining_server:has_cache_space(Partition1) of
		true ->
			{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
					Partition1, PartitionUpperBound),
			Partition2 = ?PARTITION_NUMBER(RecallRange2Start),
			Candidate2 = Candidate#mining_candidate{ partition_number2 = Partition2 },
			Candidate3 = generate_cache_ref(Candidate2),
			Range1Exists = ar_mining_io:read_recall_range(
					chunk1, self(), Candidate3, RecallRange1Start),
			State3 = case Range1Exists of
				true ->
					State2 = reserve_cache_space(Partition1, State),
					Range2Exists = ar_mining_io:read_recall_range(
							chunk2, self(), Candidate3, RecallRange2Start),
					case Range2Exists of
						true -> 
							reserve_cache_space(Partition2, State2);
						false ->
							do_not_cache(Candidate3, State2)
					end;
				false ->
					?LOG_DEBUG([{event, mining_debug_no_io_thread_found_for_range},
						{worker, State#state.name},
						{partition_number, Partition1},
						{range_start, RecallRange1Start},
						{range_end, RecallRange1Start + ?RECALL_RANGE_SIZE}]),
					State
			end,
			{noreply, State3};
		false ->
			%% Wait a bit, and then re-add the task. This is to allow other, lower priority,
			%% computed_h0 tasks to process while we wait for cache space to free up for this
			%% partition.
			add_delayed_task(self(), computed_h0, Candidate),
			{noreply, State}
	end;

handle_task({computed_h1, Candidate}, State) ->
	#state{ chunk_cache = Cache, diff = Diff } = State,
	#mining_candidate{ h1 = H1, chunk1 = Chunk1,
		partition_number = Partition1, partition_number2 = Partition2,
		partial_diff = PartialDiff } = Candidate,
	case passes_diff_check(H1, Diff, PartialDiff) of
		true ->
			?LOG_DEBUG([{event, mining_debug_found_h1_solution}, {worker, State#state.name},
				{h1, ar_util:encode(H1)}, {difficulty, Diff}]),
			%% Decrement 1 for chunk1:
			%% Since we found a solution we won't need chunk2 (and it will be evicted if
			%% necessary below)
			State2 = remove_chunk_from_cache(chunk1, Candidate, State),
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
			case cycle_chunk_cache(Candidate, {chunk1, Chunk1, H1}, Cache) of
				{cached, Cache2} ->
					%% Chunk2 hasn't been read yet, so we cache Chunk1 and wait for
					%% Chunk2 to be read.
					{noreply, State#state{ chunk_cache = Cache2 }};
				{do_not_cache, Cache2} ->
					%% This node does not store Chunk2. If we're part of a coordinated
					%% mining set, we can try one of our peers, otherwise we're done.
					case Config#config.coordinated_mining of
						false ->
							ok;
						true ->
							ar_coordination:computed_h1(Candidate, Diff)
					end,
					%% Decrement 1 for chunk1:
					%% do_not_cache indicates chunk2 was not and will not be read or cached
					{noreply, update_chunk_cache_size(
						Partition1, -1, State#state{ chunk_cache = Cache2 })};
				{{chunk2, Chunk2}, Cache2} ->
					%% Chunk2 has already been read, so we can compute H2 now.
					ar_mining_hash:compute_h2(
						self(), Candidate#mining_candidate{ chunk2 = Chunk2 }),
					%% Decrement 2 for chunk1 and chunk2:
					%% 1. chunk2 was previously read and cached
					%% 2. chunk1 that was just read and used to compute H1	
					State2 = update_chunk_cache_size(Partition1, -1,
							State#state{ chunk_cache = Cache2 }),
					State3 = update_chunk_cache_size(Partition2, -1, State2),
					{noreply, State3}
			end
	end;

handle_task({computed_h2, Candidate}, State) ->
	#mining_candidate{
		chunk2 = Chunk2, h0 = H0, h2 = H2, mining_address = MiningAddress,
		nonce = Nonce, partition_number = Partition1, 
		partition_upper_bound = PartitionUpperBound, cm_lead_peer = Peer,
		partial_diff = PartialDiff
	} = Candidate,
	case passes_diff_check(H2, get_difficulty(State, Candidate), PartialDiff) of
		true ->
			?LOG_DEBUG([{event, mining_debug_found_h2_solution}, {worker, State#state.name},
				{h2, ar_util:encode(H2)}, {difficulty, get_difficulty(State, Candidate)}]),
			case Peer of
				not_set ->
					ar_mining_server:prepare_and_post_solution(Candidate);
				_ ->
					{_RecallByte1, RecallByte2} = ar_mining_server:get_recall_bytes(
							H0, Partition1, Nonce, PartitionUpperBound),
					PoA2 = ar_mining_server:read_poa(RecallByte2, Chunk2, MiningAddress),
					case PoA2 of
						error ->
							?LOG_WARNING([{event,
									mined_block_but_failed_to_read_second_chunk_proof},
									{worker, State#state.name},
									{recall_byte2, RecallByte2},
									{mining_address, ar_util:safe_encode(MiningAddress)}]),
							ar:console("WARNING: we found a solution but failed to read "
									"the proof for the second chunk. See logs for more "
									"details.~n");
						_ ->
							ar_coordination:computed_h2_for_peer(
								Candidate#mining_candidate{ poa2 = PoA2 })
					end
			end;
		partial ->
			ar_mining_server:prepare_and_post_solution(Candidate);
		false ->
			ok
	end,
	{noreply, State};

handle_task({compute_h2_for_peer, Candidate}, State) ->
	#mining_candidate{
		h0 = H0,
		partition_number = Partition1,
		partition_upper_bound = PartitionUpperBound
	} = Candidate,

	{_RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
					Partition1, PartitionUpperBound),
	Partition2 = ?PARTITION_NUMBER(RecallRange2Start),
	Candidate2 = Candidate#mining_candidate{ partition_number2 = Partition2 },
	Candidate3 = generate_cache_ref(Candidate2),
	Range2Exists = ar_mining_io:read_recall_range(
		chunk2, self(), Candidate3#mining_candidate{ cm_h1_list = [] }, RecallRange2Start),
	case Range2Exists of
		true ->
			{noreply, cache_h1_list(Candidate3, State)};
		false ->
			%% This can happen if the remote peer has an outdated partition table
			{noreply, State}
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

passes_diff_check(H, Diff, not_set) ->
	binary:decode_unsigned(H, big) > Diff;
passes_diff_check(H, Diff, PartialDiff) ->
	Decoded = binary:decode_unsigned(H, big),
	case Decoded > Diff of
		true ->
			true;
		false ->
			case Decoded > PartialDiff of
				true ->
					partial;
				false ->
					false
			end
	end.

maybe_warn_about_lag(Q, Name) ->
	case gb_sets:is_empty(Q) of
		true ->
			ok;
		false ->
			case gb_sets:take_smallest(Q) of
				{{_Priority, _ID, {compute_h0, _}}, Q3} ->
					N = count_h0_tasks(Q3) + 1,
					?LOG_WARNING([
						{event, mining_server_lags_behind_the_nonce_limiter},
						{worker, Name},
						{step_count, N}]);
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

reset(State) ->
	ChunksDiscarded = maps:fold(
		fun(PartitionNumber, CacheSize, Total) ->
			ar_mining_server:update_chunk_cache_size(PartitionNumber, -CacheSize),
			Total + CacheSize
		end,
		0,
		State#state.chunk_cache_size
	),
	TasksDiscarded = gb_sets:size(State#state.task_queue),
	State2 = clear_task_queue(State),
	{
		State2#state{ chunk_cache = maps:new(), chunk_cache_size = #{} },
		TasksDiscarded,
		ChunksDiscarded
	}.

clear_task_queue(State) ->
	#state{ task_queue = Q } = State,
	gb_sets:fold(
		fun({_Priority, _ID, {TaskType, _Candidate}}, _Acc) ->
			prometheus_gauge:dec(mining_server_task_queue_len, [TaskType])
		end,
		ok,
		Q
	),
	State#state{ task_queue = gb_sets:new() }.

update_chunk_cache_size(PartitionNumber, Delta, State) ->
	ar_mining_server:update_chunk_cache_size(PartitionNumber, Delta),
	PartitionCacheSize = maps:get(PartitionNumber, State#state.chunk_cache_size, 0),
	CacheSize = maps:put(PartitionNumber, PartitionCacheSize + Delta, State#state.chunk_cache_size),
	State#state{ chunk_cache_size = CacheSize }.

reserve_cache_space(PartitionNumber, State) ->
	reserve_cache_space(PartitionNumber, nonce_max() + 1, State).

reserve_cache_space(PartitionNumber, NumChunks, State) ->
	ar_mining_server:reserve_cache_space(PartitionNumber, NumChunks),
	PartitionCacheSize = maps:get(PartitionNumber, State#state.chunk_cache_size, 0),
	CacheSize = maps:put(PartitionNumber, PartitionCacheSize + NumChunks,
		State#state.chunk_cache_size),
	State#state{ chunk_cache_size = CacheSize }.

do_not_cache(Candidate, State) ->
	do_not_cache(0, nonce_max(), Candidate, State).

do_not_cache(Nonce, NonceMax, _Candidate, State)
		when Nonce > NonceMax ->
	State;
do_not_cache(Nonce, NonceMax, Candidate, State) ->
	#state{ chunk_cache = Cache } = State,
	#mining_candidate{ cache_ref = CacheRef } = Candidate,
	Cache2 = maps:put({CacheRef, Nonce}, do_not_cache, Cache),
	do_not_cache(Nonce + 1, NonceMax, Candidate, State#state{ chunk_cache = Cache2 }).

%% @doc The chunk_cache stores either the first or second chunk for a given nonce. This is because
%% we process both the first and second recall ranges in parallel and don't know
%% which data will be available first. The function manages that shared cache slot by either
%% caching data if its the first to arrive, or "popping" data that was previously cached. The
%% caller is responsible for taking the appropriate action based on the return value.
%%
%% do_not_cache is a special value used to prevent unnecessary data from being cached once a
%% solution has been found for a given nonce.
cycle_chunk_cache(#mining_candidate{ cache_ref = CacheRef, nonce = Nonce }, Data, Cache)
		when CacheRef /= not_set ->
	case maps:take({CacheRef, Nonce}, Cache) of
		{do_not_cache, Cache2} ->
			{do_not_cache, Cache2};
		error ->
			Cache2 = maps:put({CacheRef, Nonce}, Data, Cache),
			{cached, Cache2};
		{CachedData, Cache2} ->
			{CachedData, Cache2}
	end.

remove_chunk_from_cache(WhichChunk, #mining_candidate{ cache_ref = CacheRef } = Candidate, State)
		when CacheRef /= not_set ->
	#mining_candidate{ nonce = Nonce,
		partition_number = Partition1, partition_number2 = Partition2 } = Candidate,
	State2 = case WhichChunk of
		chunk1 -> update_chunk_cache_size(Partition1, -1, State);
		chunk2 -> update_chunk_cache_size(Partition2, -1, State)
	end,
	#state{ chunk_cache = Cache } = State2,
	case maps:take({CacheRef, Nonce}, Cache) of
		{do_not_cache, Cache2} ->
			State2#state{ chunk_cache = Cache2 };
		error ->
			Cache2 = maps:put({CacheRef, Nonce}, do_not_cache, Cache),
			State2#state{ chunk_cache = Cache2 };
		{{chunk2, _}, Cache2} ->
			%% if we find any cached data, discard it and decrement the cache size
			update_chunk_cache_size(Partition2, -1, State2#state{ chunk_cache = Cache2 });
		{_, Cache2} ->
			%% if we find any cached data, discard it and decrement the cache size
			update_chunk_cache_size(Partition1, -1, State2#state{ chunk_cache = Cache2 })
	end.

cache_h1_list(Candidate, State) ->
	#mining_candidate{ cm_h1_list = H1List, partition_number2 = Partition2 } = Candidate,
	%% Only reserve enough space to process the H1 list provided. If this is less
	%% than the full recall range, some of the chunk2s will be read, cached, and ignored
	%% (since we don't have the H1 needed to compute the H2).
	%%
	%% This may cause some temporary memory bloat - but it will be cleaned up with the
	%% next mining session (e.g. when the next block is applied).
	%%
	%% If however we reserve cache space for these "orphan" chunk2s the cache space they
	%% consume will prevent usable chunks from being read and negatively impact the mining
	%% rate.
	State2 = reserve_cache_space(Partition2, length(H1List), State),
	cache_h1_list(Candidate, H1List, State2).

cache_h1_list(_Candidate, [], State) ->
	State;
cache_h1_list(
		#mining_candidate{ cache_ref = CacheRef } = Candidate,
		[ {H1, Nonce} | H1List ], State) when CacheRef /= not_set ->
	#state{ chunk_cache = Cache } = State,
	Cache2 = maps:put({CacheRef, Nonce}, {chunk1, H1}, Cache),
	cache_h1_list(Candidate, H1List, State#state{ chunk_cache = Cache2 }).

generate_cache_ref(Candidate) ->
	#mining_candidate{
		partition_number = Partition1, partition_number2 = Partition2,
		partition_upper_bound = PartitionUpperBound } = Candidate,
	CacheRef = {Partition1, Partition2, PartitionUpperBound, make_ref()},
	Candidate#mining_candidate{ cache_ref = CacheRef }.

get_difficulty(State, #mining_candidate{ cm_diff = not_set }) ->
	State#state.diff;
get_difficulty(_State, #mining_candidate{ cm_diff = Diff }) ->
	Diff.

nonce_max() ->
	max(0, ((?RECALL_RANGE_SIZE) div ?DATA_CHUNK_SIZE - 1)).

%%%===================================================================
%%% Public Test interface.
%%%===================================================================
