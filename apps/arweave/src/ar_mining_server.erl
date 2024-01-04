%%% @doc The 2.6 mining server.
-module(ar_mining_server).
% TODO Seed, NextSeed, StartIntervalNumber, StepNumber, NonceLimiterOutput -> #vdf

-behaviour(gen_server).

-export([start_link/1, start_mining/1,
		has_cache_space/1, update_chunk_cache_size/2, reserve_cache_space/2,
		set_difficulty/1, set_merkle_rebase_threshold/1, 
		compute_h2_for_peer/1, prepare_and_post_solution/1, post_solution/1,
		read_poa/3, get_recall_bytes/4]).
-export([pause/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	paused 						= true,
	workers						= #{},
	chunk_cache_size_limit		= infinity,
	diff						= infinity,
	merkle_rebase_threshold		= infinity,
	is_pool_client				= false
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the gen_server.
start_link(Workers) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Workers, []).

%% @doc Start mining.
start_mining(Args) ->
	gen_server:cast(?MODULE, {start_mining, Args}).

has_cache_space(PartitionNumber) ->
	try
        gen_server:call(?MODULE, {has_cache_space, PartitionNumber})
    catch
        Error:Reason ->
			?LOG_WARNING([{event, has_cache_space_failed}, {Error, Reason}]),
            false
    end.

%% @doc Before loading a recall range we reserve enough cache space for the whole range. This
%% helps make sure we don't exceed the cache limit (too much) when there are many parallel
%% reads. 
%%
%% As we compute hashes we'll decrement the chunk_cache_size to indicate available cache space.
reserve_cache_space(PartitionNumber, NumChunks) ->
	gen_server:cast(?MODULE, {reserve_cache_space, PartitionNumber, NumChunks}).

%% @doc Compute H2 for a remote peer (used in coordinated mining).
compute_h2_for_peer(Candidate) ->
	gen_server:cast(?MODULE, {compute_h2_for_peer, Candidate}).

%% @doc Set the new mining difficulty. We do not recalculate it inside the mining
%% server because we want to completely detach the mining server from the block
%% ordering. The previous block is chosen only after the mining solution is found (if
%% we choose it in advance we may miss a better option arriving in the process).
%% Also, a mining session may (in practice, almost always will) span several blocks.
set_difficulty(Diff) ->
	gen_server:cast(?MODULE, {set_difficulty, Diff}).

set_merkle_rebase_threshold(Threshold) ->
	gen_server:cast(?MODULE, {set_merkle_rebase_threshold, Threshold}).

prepare_and_post_solution(Candidate) ->
	gen_server:cast(?MODULE, {prepare_and_post_solution, Candidate}).

post_solution(Solution) ->
	gen_server:cast(?MODULE, {post_solution, Solution}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Workers) ->
	process_flag(trap_exit, true),
	[ok, ok] = ar_events:subscribe([nonce_limiter, pool]),
	ar_chunk_storage:open_files("default"),
	reset_chunk_cache_size(),
	%% Initialize the map with dummy values for the VDF session keys. Once we receive our first
	%% VDF step we'll refresh the map with the actual VDF session keys.
	HalfLength = length(Workers) div 2,
	{A, B} = lists:split(HalfLength, Workers),
	WorkersBySession = #{
		<<"current_session">> => A,
		<<"previous_session">> => B
	},

	{ok, #state{
		workers = WorkersBySession,
		is_pool_client = ar_pool:is_client()
	}}.

handle_call({has_cache_space, PartitionNumber}, _From, State) ->
	#state{ chunk_cache_size_limit = Limit } = State,
	case ets:lookup(?MODULE, {chunk_cache_size, PartitionNumber}) of
		[{_, Size}] ->
			{reply, Size < Limit, State};
		[] ->
			{reply, true, State}
	end;

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(pause, State) ->
	ar:console("Pausing mining.~n"),
	?LOG_INFO([{event, pause_mining}]),
	ar_mining_stats:mining_paused(),
	%% Setting paused to true allows all pending tasks to complete, but prevents new output to be 
	%% distributed. Setting diff to infinity ensures that no solutions are found.
	State2 = set_difficulty(infinity, State),
	{noreply, State2#state{ paused = true }};

handle_cast({start_mining, Args}, State) ->
	{Diff, RebaseThreshold} = Args,
	ar:console("Starting mining.~n"),
	?LOG_INFO([{event, start_mining}, {difficulty, Diff}, {rebase_threshold, RebaseThreshold}]),
	ar_mining_stats:start_performance_reports(),

	maps:foreach(
		fun(_Key, Workers) ->
			[ar_mining_worker:reset(Worker, Diff) || Worker <- Workers]
		end,
		State#state.workers
	),

	reset_chunk_cache_size(),

	{noreply, State#state{ 
		paused = false,
		diff = Diff,
		merkle_rebase_threshold = RebaseThreshold }};

handle_cast({set_difficulty, Diff}, State) ->
	State2 = set_difficulty(Diff, State),
	{noreply, State2};

handle_cast({set_merkle_rebase_threshold, Threshold}, State) ->
	{noreply, State#state{ merkle_rebase_threshold = Threshold }};


handle_cast({reserve_cache_space, PartitionNumber, NumChunks}, State) ->
	update_chunk_cache_size(PartitionNumber, NumChunks),
	{noreply, State};

handle_cast({compute_h2_for_peer, Candidate}, State) ->
	#mining_candidate{ session_key = SessionKey } = Candidate,

	case get_worker(SessionKey, State) of
		not_found ->
			ok;
		Worker ->
			ar_mining_worker:add_task(Worker, compute_h2_for_peer, Candidate)
	end,
	{noreply, State};

handle_cast({prepare_and_post_solution, Candidate}, State) ->
	prepare_and_post_solution(Candidate, State),
	{noreply, State};

handle_cast({post_solution, Solution}, State) ->
	post_solution(Solution, State),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({event, nonce_limiter, {computed_output, _}}, #state{ paused = true } = State) ->
	?LOG_DEBUG([{event, mining_debug_skipping_vdf_output}]),
	{noreply, State};
handle_info({event, nonce_limiter, {computed_output, Args}}, State) ->
	case ar_pool:is_client() of
		true ->
			%% Ignore VDF events because we are receiving jobs from the pool.
			{noreply, State};
		false ->
			Args2 = erlang:append_element(Args, not_set),
			handle_computed_output(Args2, State)
	end;

handle_info({event, nonce_limiter, Message}, State) ->
	?LOG_DEBUG([{event, mining_debug_skipping_nonce_limiter}, {message, Message}]),
	{noreply, State};

handle_info({event, pool, {job, Args}}, State) ->
	handle_computed_output(Args, State);

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================
get_worker(SessionKey, State) ->
	case get_workers(SessionKey, State) of
		[] ->
			not_found;
		Workers ->
			ar_util:pick_random(Workers)
	end.

get_workers(SessionKey, State) ->
	#state{ workers = WorkersBySession } = State,
	Workers = maps:get(SessionKey, WorkersBySession, []),
	?LOG_DEBUG([{event, mining_debug_get_workers},
		{session_key, ar_nonce_limiter:encode_session_key(SessionKey)},
		{workers, Workers}]),
	Workers.

set_difficulty(Diff, State) ->
	maps:foreach(
		fun(_Key, Workers) ->
			[ar_mining_worker:set_difficulty(Worker, Diff) || Worker <- Workers]
		end,
		State#state.workers
	),
	State#state{ diff = Diff }.

refresh_workers(State) ->
	#state{ workers = WorkersBySession } = State,

	NewSessions = get_current_session_keys(),
	MappedSessions = maps:keys(WorkersBySession),

	PreviousSessionLogEntries =
		case length(NewSessions) > 1 of
			false ->
				[];
			true ->
				[{previous_session_key,
					ar_nonce_limiter:encode_session_key(lists:nth(2, NewSessions))}]
		end,
	RefreshLogEntries = [{event, mining_debug_refreshing_workers},
			{current_session_key, ar_nonce_limiter:encode_session_key(hd(NewSessions))}]
		++ PreviousSessionLogEntries
		++ [{mapped_sessions,
				[ar_nonce_limiter:encode_session_key(SessionKey)
				|| SessionKey <- MappedSessions]}],
	?LOG_DEBUG(RefreshLogEntries),

	SessionsToAdd = [SessionKey || SessionKey <- NewSessions,
			SessionKey /= undefined andalso not maps:is_key(SessionKey, WorkersBySession)],
	SessionsToRemove = [SessionKey || SessionKey <- MappedSessions,
			not lists:member(SessionKey, NewSessions)],
	SessionsToRemove2 = lists:sublist(SessionsToRemove, length(SessionsToAdd)),

	WorkersBySession2 = refresh_workers(SessionsToAdd, SessionsToRemove2, WorkersBySession),
	State#state{ workers = WorkersBySession2 }.

get_current_session_keys() ->
	case ar_pool:is_client() of
		false ->
			{CurrentSessionKey, CurrentSession} = ar_nonce_limiter:get_current_session(),
			PreviousSessionKey = CurrentSession#vdf_session.prev_session_key,
			[CurrentSessionKey, PreviousSessionKey];
		true ->
			ar_pool:get_current_sessions()
	end.

refresh_workers([], [], WorkersBySession) ->
	WorkersBySession;
refresh_workers(
	[AddKey | SessionsToAdd], [RemoveKey | SessionsToRemove], WorkersBySession) ->
	{NextSeed, StartIntervalNumber, NextVDFDifficulty} = AddKey,
	ar:console("Starting new mining session: "
		"next entropy nonce: ~s, interval number: ~B, next vdf difficulty: ~B.~n",
		[ar_util:safe_encode(NextSeed), StartIntervalNumber, NextVDFDifficulty]),
	?LOG_INFO([{event, new_mining_session}, 
		{session_key, ar_nonce_limiter:encode_session_key(AddKey)},
		{retired_session_key, ar_nonce_limiter:encode_session_key(RemoveKey)}]),

	Workers = maps:get(RemoveKey, WorkersBySession, []),
	reset_workers(Workers, AddKey),

	WorkersBySession2 = maps:put(AddKey, Workers, WorkersBySession),
	WorkersBySession3 = maps:remove(RemoveKey, WorkersBySession2),
	refresh_workers(SessionsToAdd, SessionsToRemove, WorkersBySession3).

reset_workers([], _SessionKey) ->
	ok;
reset_workers([Worker | Workers], SessionKey) ->
	ar_mining_worker:new_session(Worker, SessionKey),
	reset_workers(Workers, SessionKey).

get_chunk_cache_size_limit(State) ->
	#state{ chunk_cache_size_limit = CurrentLimit } = State,
	ThreadCount = ar_mining_io:get_thread_count(),
	% Two ranges per output.
	OptimalLimit = ar_util:ceil_int(
		(?RECALL_RANGE_SIZE * 2 * ThreadCount) div ?DATA_CHUNK_SIZE,
		100),

	{ok, Config} = application:get_env(arweave, config),
	Limit = case Config#config.mining_server_chunk_cache_size_limit of
		undefined ->
			Total = proplists:get_value(total_memory,
					memsup:get_system_memory_data(), 2000000000),
			Bytes = Total * 0.7 / 3,
			CalculatedLimit = erlang:ceil(Bytes / ?DATA_CHUNK_SIZE),
			min(ar_util:ceil_int(CalculatedLimit, 100), OptimalLimit);
		N ->
			N
	end,

	NumPartitions = max(1, length(ar_mining_io:get_partitions())),
	LimitPerPartition = max(1, Limit div NumPartitions),

	case LimitPerPartition == CurrentLimit of
		true ->
			CurrentLimit;
		false ->
			ar:console("~nSetting the chunk cache size limit to ~B chunks (~B chunks per partition).~n",
				[Limit, LimitPerPartition]),
			?LOG_INFO([{event, setting_chunk_cache_size_limit},
				{limit, Limit}, {per_partition, LimitPerPartition}]),
			case Limit < OptimalLimit of
				true ->
					ar:console("~nChunk cache size limit is below optimal limit of ~p. "
						"Mining performance may be impacted.~n"
						"Consider adding more RAM or changing the "
						"'mining_server_chunk_cache_size_limit' option.", [OptimalLimit]);
				false -> ok
			end,
			LimitPerPartition
	end.

distribute_output(Workers, Candidate) ->
	distribute_output(Workers, ar_mining_io:get_partitions(), Candidate, 0).

distribute_output(_Workers, [], _Candidate, N) ->
	N;
distribute_output(
		Workers, [{PartitionNumber, MiningAddress} | Partitions], Candidate, N) ->
	MaxPartitionNumber = ?MAX_PARTITION_NUMBER(Candidate#mining_candidate.partition_upper_bound),
	case PartitionNumber > MaxPartitionNumber of
		true ->
			%% Skip this partition
			distribute_output(Workers, Partitions, Candidate, N);
		false ->
			Worker = ar_util:pick_random(Workers),
			?LOG_DEBUG([{event, mining_debug_distributing_output},
				{worker, Worker},
				{session_key, ar_nonce_limiter:encode_session_key(Candidate#mining_candidate.session_key)},
				{step_number, Candidate#mining_candidate.step_number},
				{partition_number, PartitionNumber},
				{partition_upper_bound, Candidate#mining_candidate.partition_upper_bound},
				{max_partition, MaxPartitionNumber}]),
			ar_mining_worker:add_task(
				Worker, compute_h0,
				Candidate#mining_candidate{
					partition_number = PartitionNumber,
					mining_address = MiningAddress
				}),
			distribute_output(Workers, Partitions, Candidate, N + 1)
	end.

get_recall_bytes(H0, PartitionNumber, Nonce, PartitionUpperBound) ->
	{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
			PartitionNumber, PartitionUpperBound),
	RelativeOffset = Nonce * (?DATA_CHUNK_SIZE),
	{RecallRange1Start + RelativeOffset, RecallRange2Start + RelativeOffset}.

reset_chunk_cache_size() ->
	Pattern = {{chunk_cache_size, '$1'}, '_'}, % '$1' matches any PartitionNumber
    Entries = ets:match(?MODULE, Pattern),
    lists:foreach(
        fun(PartitionNumber) ->
			ets:insert(?MODULE, {{chunk_cache_size, PartitionNumber}, 0}),
			prometheus_gauge:set(mining_server_chunk_cache_size, [PartitionNumber], 0)
        end, 
        Entries
    ).

update_chunk_cache_size(PartitionNumber, Delta) ->
	ets:update_counter(?MODULE, {chunk_cache_size, PartitionNumber}, {2, Delta},
		{{chunk_cache_size, PartitionNumber}, 0}),
	prometheus_gauge:inc(mining_server_chunk_cache_size, [PartitionNumber], Delta),
	Delta.

prepare_and_post_solution(Candidate, State) ->
	Solution = prepare_solution(Candidate, State),
	post_solution(Solution, State).

prepare_solution(Candidate, State) ->
	#state{ merkle_rebase_threshold = RebaseThreshold, is_pool_client = IsPoolClient } = State,
	#mining_candidate{
		mining_address = MiningAddress, next_seed = NextSeed, 
		next_vdf_difficulty = NextVDFDifficulty, nonce = Nonce,
		nonce_limiter_output = NonceLimiterOutput, partition_number = PartitionNumber,
		partition_upper_bound = PartitionUpperBound, poa2 = PoA2, preimage = Preimage,
		seed = Seed, start_interval_number = StartIntervalNumber, step_number = StepNumber
	} = Candidate,
	
	Solution = #mining_solution{
		mining_address = MiningAddress,
		merkle_rebase_threshold = RebaseThreshold,
		next_seed = NextSeed,
		next_vdf_difficulty = NextVDFDifficulty,
		nonce = Nonce,
		nonce_limiter_output = NonceLimiterOutput,
		partition_number = PartitionNumber,
		partition_upper_bound = PartitionUpperBound,
		poa2 = PoA2,
		preimage = Preimage,
		seed = Seed,
		start_interval_number = StartIntervalNumber,
		step_number = StepNumber
	},
	case IsPoolClient of
		true ->
			prepare_solution(proofs, Candidate, Solution);
		false ->
			prepare_solution(last_step_checkpoints, Candidate, Solution)
	end.

prepare_solution(last_step_checkpoints, Candidate, Solution) ->
	#mining_candidate{
		next_seed = NextSeed, next_vdf_difficulty = NextVDFDifficulty, 
		start_interval_number = StartIntervalNumber, step_number = StepNumber } = Candidate,
	LastStepCheckpoints = ar_nonce_limiter:get_step_checkpoints(
			StepNumber, NextSeed, StartIntervalNumber, NextVDFDifficulty),
	case LastStepCheckpoints of
		not_found ->
			error;
		_ ->
			prepare_solution(steps, Candidate, Solution#mining_solution{
				last_step_checkpoints = LastStepCheckpoints })
	end;

prepare_solution(steps, Candidate, Solution) ->
	#mining_candidate{ step_number = StepNumber } = Candidate,
	[{_, TipNonceLimiterInfo}] = ets:lookup(node_state, nonce_limiter_info),
	#nonce_limiter_info{ global_step_number = PrevStepNumber, next_seed = PrevNextSeed,
			next_vdf_difficulty = PrevNextVDFDifficulty } = TipNonceLimiterInfo,
	case StepNumber > PrevStepNumber of
		true ->
			Steps = ar_nonce_limiter:get_steps(
					PrevStepNumber, StepNumber, PrevNextSeed, PrevNextVDFDifficulty),
			case Steps of
				not_found ->
					?LOG_WARNING([{event, found_solution_but_failed_to_find_checkpoints},
							{start_step_number, PrevStepNumber},
							{next_step_number, StepNumber},
							{next_seed, ar_util:safe_encode(PrevNextSeed)},
							{next_vdf_difficulty, PrevNextVDFDifficulty}]),
					ar:console("WARNING: found a solution but failed to find checkpoints, "
							"start step number: ~B, end step number: ~B, next_seed: ~s.",
							[PrevStepNumber, StepNumber, PrevNextSeed]),
					error;
				_ ->
					prepare_solution(proofs, Candidate,
							Solution#mining_solution{ steps = Steps })
			end;
		false ->
			?LOG_WARNING([{event, found_solution_but_stale_step_number},
							{start_step_number, PrevStepNumber},
							{next_step_number, StepNumber},
							{next_seed, ar_util:safe_encode(PrevNextSeed)},
							{next_vdf_difficulty, PrevNextVDFDifficulty}]),
			error
	end;

prepare_solution(proofs, Candidate, Solution) ->
	#mining_candidate{
		h0 = H0, h1 = H1, h2 = H2, nonce = Nonce, partition_number = PartitionNumber,
		partition_upper_bound = PartitionUpperBound } = Candidate,
	{RecallByte1, RecallByte2} = get_recall_bytes(H0, PartitionNumber, Nonce,
			PartitionUpperBound),
	case { H1, H2 } of
		{not_set, not_set} ->
			error;
		{H1, not_set} ->
			prepare_solution(poa1, Candidate, Solution#mining_solution{
				solution_hash = H1, recall_byte1 = RecallByte1, poa2 = #poa{} });
		{_, H2} ->
			prepare_solution(poa2, Candidate, Solution#mining_solution{
				solution_hash = H2, recall_byte1 = RecallByte1, recall_byte2 = RecallByte2 })
	end;

prepare_solution(poa1, Candidate,
		#mining_solution{ poa1 = #poa{ chunk = <<>> } } = Solution) ->
	#mining_solution{
		mining_address = MiningAddress, partition_number = PartitionNumber,
		recall_byte1 = RecallByte1 } = Solution,
	#mining_candidate{
		chunk1 = Chunk1, h0 = H0, nonce = Nonce,
		partition_upper_bound = PartitionUpperBound } = Candidate,
	case read_poa(RecallByte1, Chunk1, MiningAddress) of
		error ->
			{RecallRange1Start, _RecallRange2Start} = ar_block:get_recall_range(H0,
					PartitionNumber, PartitionUpperBound),
			?LOG_WARNING([{event, mined_block_but_failed_to_read_chunk_proofs},
					{recall_byte1, RecallByte1},
					{recall_range_start1, RecallRange1Start},
					{nonce, Nonce},
					{partition, PartitionNumber},
					{mining_address, ar_util:safe_encode(MiningAddress)}]),
			ar:console("WARNING: we have mined a block but failed to fetch "
					"the chunk proofs required for publishing it. "
					"Check logs for more details~n"),
			error;
		PoA1 ->
			Solution#mining_solution{ poa1 = PoA1 }
	end;
prepare_solution(poa2, Candidate,
		#mining_solution{ poa2 = #poa{ chunk = <<>> } } = Solution) ->
	#mining_solution{ mining_address = MiningAddress, partition_number = PartitionNumber,
		recall_byte2 = RecallByte2 } = Solution,
	#mining_candidate{
		chunk2 = Chunk2, h0 = H0, nonce = Nonce,
		partition_upper_bound = PartitionUpperBound } = Candidate,
	case read_poa(RecallByte2, Chunk2, MiningAddress) of
		error ->
			{_RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
					PartitionNumber, PartitionUpperBound),
			?LOG_WARNING([{event, mined_block_but_failed_to_read_chunk_proofs},
					{recall_byte2, RecallByte2},
					{recall_range_start2, RecallRange2Start},
					{nonce, Nonce},
					{partition, PartitionNumber},
					{mining_address, ar_util:safe_encode(MiningAddress)}]),
			ar:console("WARNING: we have mined a block but failed to fetch "
					"the chunk proofs required for publishing it. "
					"Check logs for more details~n"),
			error;
		PoA2 ->
			prepare_solution(poa1, Candidate, Solution#mining_solution{ poa2 = PoA2 })
	end;
prepare_solution(poa2, Candidate,
		#mining_solution{ poa1 = #poa{ chunk = <<>> } } = Solution) ->
	prepare_solution(poa1, Candidate, Solution);
prepare_solution(_, _Candidate, Solution) ->
	Solution.

post_solution(error, _State) ->
	error;
post_solution(Solution, State) ->
	{ok, Config} = application:get_env(arweave, config),
	post_solution(Config#config.cm_exit_peer, Solution, State).

post_solution(not_set, Solution, #state{ is_pool_client = true }) ->
	ar_pool:post_partial_solution(Solution);
post_solution(not_set, Solution, State) ->
	#state{ diff = Diff } = State,
	#mining_solution{
		mining_address = MiningAddress, nonce_limiter_output = NonceLimiterOutput,
		partition_number = PartitionNumber, recall_byte1 = RecallByte1,
		recall_byte2 = RecallByte2,
		solution_hash = H, step_number = StepNumber } = Solution,
	case validate_solution(Solution, Diff) of
		error ->
			?LOG_WARNING([{event, failed_to_validate_solution},
					{partition, PartitionNumber},
					{step_number, StepNumber},
					{mining_address, ar_util:safe_encode(MiningAddress)},
					{recall_byte1, RecallByte1},
					{recall_byte2, RecallByte2},
					{solution_h, ar_util:safe_encode(H)},
					{nonce_limiter_output, ar_util:safe_encode(NonceLimiterOutput)}]),
			ar:console("WARNING: we failed to validate our solution. Check logs for more "
					"details~n");
		{false, Reason} ->
			?LOG_WARNING([{event, found_invalid_solution},
					{reason, Reason},
					{partition, PartitionNumber},
					{step_number, StepNumber},
					{mining_address, ar_util:safe_encode(MiningAddress)},
					{recall_byte1, RecallByte1},
					{recall_byte2, RecallByte2},
					{solution_h, ar_util:safe_encode(H)},
					{nonce_limiter_output, ar_util:safe_encode(NonceLimiterOutput)}]),
			ar:console("WARNING: the solution we found is invalid. Check logs for more "
					"details~n");
		{true, PoACache, PoA2Cache} ->
			ar_events:send(miner, {found_solution, miner, Solution, PoACache, PoA2Cache})
	end;
post_solution(ExitPeer, Solution, #state{ is_pool_client = true }) ->
	case ar_http_iface_client:post_partial_solution(ExitPeer, Solution, false) of
		{ok, _} ->
			ok;
		{error, Reason} ->
			?LOG_WARNING([{event, found_partial_solution_but_failed_to_reach_exit_node},
					{reason, io_lib:format("~p", [Reason])}]),
			ar:console("We found a partial solution but failed to reach the exit node, "
					"error: ~p.", [io_lib:format("~p", [Reason])])
	end;
post_solution(ExitPeer, Solution, _State) ->
	case ar_http_iface_client:cm_publish_send(ExitPeer, Solution) of
		{ok, _} ->
			ok;
		{error, Reason} ->
			?LOG_WARNING([{event, found_solution_but_failed_to_reach_exit_node},
					{reason, io_lib:format("~p", [Reason])}]),
			ar:console("We found a solution but failed to reach the exit node, "
					"error: ~p.", [io_lib:format("~p", [Reason])])
	end.

handle_computed_output(Args, State) ->
	{SessionKey, StepNumber, Output, PartitionUpperBound, PartialDiff} = Args,
	true = is_integer(StepNumber),
	ar_mining_stats:vdf_computed(),

	State2 = case ar_mining_io:set_largest_seen_upper_bound(PartitionUpperBound) of
		true ->
			%% If the largest seen upper bound changed, a new partition may have been added
			%% to the mining set, so we may need to update the chunk cache size limit.
			State#state{
				chunk_cache_size_limit = get_chunk_cache_size_limit(State)
			};
		false ->
			State
	end,

	{NextSeed, StartIntervalNumber, NextVDFDifficulty} = SessionKey,
	{Workers, State3} =
		case get_workers(SessionKey, State2) of
			[] ->
				RefreshedState = refresh_workers(State2),
				{get_workers(SessionKey, RefreshedState), RefreshedState};
			Workers2 ->
				{Workers2, State2}
		end,

	case Workers of
		[] ->
			?LOG_DEBUG([{event, mining_debug_skipping_vdf_output},
				{step_number, StepNumber},
				{session_key, ar_nonce_limiter:encode_session_key(SessionKey)}]),
			ok;
		_ ->
			Candidate = #mining_candidate{
				session_key = SessionKey,
				next_seed = NextSeed,
				next_vdf_difficulty = NextVDFDifficulty,
				start_interval_number = StartIntervalNumber,
				step_number = StepNumber,
				nonce_limiter_output = Output,
				partition_upper_bound = PartitionUpperBound,
				partial_diff = PartialDiff
			},
			N = distribute_output(Workers, Candidate),
			?LOG_DEBUG([{event, mining_debug_processing_vdf_output}, {found_io_threads, N},
				{step_number, StepNumber}, {output, ar_util:safe_encode(Output)},
				{start_interval_number, StartIntervalNumber},
				{session_key, ar_nonce_limiter:encode_session_key(SessionKey)}])
	end,
	{noreply, State3}.

read_poa(RecallByte, Chunk, MiningAddress) ->
	PoA = read_poa(RecallByte, MiningAddress),
	case {Chunk, PoA} of
		{_, error} -> error;
		{not_set, _} -> PoA;
		{Chunk, #poa{ chunk = Chunk }} -> PoA;
		_ -> error
	end.
read_poa(RecallByte, MiningAddress) ->
	Options = #{ pack => true, packing => {spora_2_6, MiningAddress} },
	case ar_data_sync:get_chunk(RecallByte + 1, Options) of
		{ok, #{ chunk := Chunk, tx_path := TXPath, data_path := DataPath }} ->
			#poa{ option = 1, chunk = Chunk, tx_path = TXPath, data_path = DataPath };
		_ ->
			error
	end.

validate_solution(Solution, Diff) ->
	#mining_solution{
		mining_address = MiningAddress,
		nonce = Nonce, nonce_limiter_output = NonceLimiterOutput,
		partition_number = PartitionNumber, partition_upper_bound = PartitionUpperBound,
		poa1 = PoA1, poa2 = PoA2, recall_byte1 = RecallByte1, recall_byte2 = RecallByte2,
		seed = Seed, solution_hash = SolutionHash } = Solution,
	H0 = ar_block:compute_h0(NonceLimiterOutput, PartitionNumber, Seed, MiningAddress),
	{H1, _Preimage1} = ar_block:compute_h1(H0, Nonce, PoA1#poa.chunk),
	{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
			PartitionNumber, PartitionUpperBound),
	%% Assert recall_byte1 is computed correctly.
	RecallByte1 = RecallRange1Start + Nonce * ?DATA_CHUNK_SIZE,
	{BlockStart1, BlockEnd1, TXRoot1} = ar_block_index:get_block_bounds(RecallByte1),
	BlockSize1 = BlockEnd1 - BlockStart1,
	case ar_poa:validate({BlockStart1, RecallByte1, TXRoot1, BlockSize1, PoA1,
			{spora_2_6, MiningAddress}, not_set}) of
		{true, ChunkID} ->
			PoACache = {{BlockStart1, RecallByte1, TXRoot1, BlockSize1,
					{spora_2_6, MiningAddress}}, ChunkID},
			case binary:decode_unsigned(H1, big) > Diff of
				true ->
					%% validates solution_hash
					SolutionHash = H1,
					{true, PoACache, undefined};
				false ->
					case RecallByte2 of
						undefined ->
							%% This can happen if the difficulty has increased between the
							%% time the H1 solution was found and now. In this case,
							%% there is no H2 solution, so we flag the solution invalid.
							{false, h1_diff_check};
						_ ->
							{H2, _Preimage2} = ar_block:compute_h2(H1, PoA2#poa.chunk, H0),
							case binary:decode_unsigned(H2, big) > Diff of
								false ->
									{false, h2_diff_check};
								true ->
									%% validates solution_hash
									SolutionHash = H2,
									%% validates recall_byte2
									RecallByte2 = RecallRange2Start + Nonce * ?DATA_CHUNK_SIZE,
									{BlockStart2, BlockEnd2, TXRoot2} =
											ar_block_index:get_block_bounds(RecallByte2),
									BlockSize2 = BlockEnd2 - BlockStart2,
									case ar_poa:validate({BlockStart2, RecallByte2, TXRoot2,
											BlockSize2, PoA2,
											{spora_2_6, MiningAddress}, not_set}) of
										{true, Chunk2ID} ->
											PoA2Cache = {{BlockStart2, RecallByte2, TXRoot2,
													BlockSize2, {spora_2_6, MiningAddress}},
													Chunk2ID},
											{true, PoACache, PoA2Cache};
										error ->
											error;
										false ->
											{false, poa2}
									end
							end
					end
			end;
		error ->
			error;
		false ->
			{false, poa1}
	end.

%%%===================================================================
%%% Public Test interface.
%%%===================================================================

%% @doc Pause the mining server. Only used in tests.
pause() ->
	gen_server:cast(?MODULE, pause).
