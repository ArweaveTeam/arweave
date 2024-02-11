%%% @doc The 2.6 mining server.
-module(ar_mining_server).
% TODO Seed, NextSeed, StartIntervalNumber, StepNumber, NonceLimiterOutput -> #vdf

-behaviour(gen_server).

-export([start_link/0, start_mining/1, set_difficulty/1, set_merkle_rebase_threshold/1, 
		compute_h2_for_peer/1, prepare_and_post_solution/1, post_solution/1, read_poa/3,
		get_recall_bytes/4, active_sessions/0, encode_sessions/1]).
-export([pause/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-record(state, {
	paused 						= true,
	workers						= #{},
	active_sessions				= sets:new(),
	diff						= infinity,
	chunk_cache_limit 			= 0,
	gc_frequency_ms				= undefined,
	merkle_rebase_threshold		= infinity
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the gen_server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Start mining.
start_mining(Args) ->
	gen_server:cast(?MODULE, {start_mining, Args}).

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

active_sessions() ->
	gen_server:call(?MODULE, active_sessions).

encode_sessions(Sessions) ->
	lists:map(fun(SessionKey) ->
		ar_nonce_limiter:encode_session_key(SessionKey)
	end, sets:to_list(Sessions)).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(nonce_limiter),
	ar_chunk_storage:open_files("default"),

	Workers = lists:foldl(
		fun({Partition, _}, Acc) ->
			maps:put(Partition, ar_mining_worker:name(Partition), Acc)
		end,
		#{},
		ar_mining_io:get_partitions(infinity)
	),

	{ok, #state{ workers = Workers }}.

handle_call(active_sessions, _From, State) ->
	{reply, State#state.active_sessions, State};

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
		fun(_Partition, Worker) ->
			ar_mining_worker:reset(Worker, Diff)
		end,
		State#state.workers
	),

	{noreply, State#state{ 
		paused = false,
		active_sessions	= sets:new(),
		diff = Diff,
		merkle_rebase_threshold = RebaseThreshold }};

handle_cast({set_difficulty, Diff}, State) ->
	State2 = set_difficulty(Diff, State),
	{noreply, State2};

handle_cast({set_merkle_rebase_threshold, Threshold}, State) ->
	{noreply, State#state{ merkle_rebase_threshold = Threshold }};

handle_cast({compute_h2_for_peer, Candidate}, State) ->
	case get_worker(Candidate#mining_candidate.partition_number2, State) of
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

handle_cast(garbage_collect, State) ->
	%% Reading recall ranges from disk causes a large amount of binary data to be allocated and
	%% references to that data is spread among all the different mining processes. Because of this
	%% it can take the default garbage collection to clean up all references and deallocate the
	%% memory - which in turn can cause memory to be exhausted.
	%% 
	%% To address this the mining server will force a garbage collection on all mining processes
	%% every time we process a few VDF steps. The exact number of VDF steps is determined by
	%% the chunk cache size limit in order to roughly align garbage collection with when we
	%% expect all references to a recall range's chunks to be evicted from the cache.
	?LOG_DEBUG([{event, mining_debug_garbage_collect_start}]),
	ar_mining_io:garbage_collect(),
	ar_mining_hash:garbage_collect(),
	erlang:garbage_collect(self(), [{async, erlang:monotonic_time()}]),
	maps:foreach(
		fun(_Partition, Worker) ->
			ar_mining_worker:garbage_collect(Worker)
		end,
		State#state.workers
	),
	ar_util:cast_after(State#state.gc_frequency_ms, ?MODULE, garbage_collect),
	{noreply, State};


handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({event, nonce_limiter, {computed_output, _Args}}, #state{ paused = true } = State) ->
	{noreply, State};
handle_info({event, nonce_limiter, {computed_output, Args}}, State) ->
	{SessionKey, StepNumber, Output, PartitionUpperBound} = Args,

	true = is_integer(StepNumber),
	ar_mining_stats:vdf_computed(),

	State2 = case ar_mining_io:set_largest_seen_upper_bound(PartitionUpperBound) of
		true ->
			%% If the largest seen upper bound changed, a new partition may have been added
			%% to the mining set, so we may need to update the chunk cache size limit.
			update_cache_limits(State);
		false ->
			State
	end,

	State3 = maybe_update_sessions(SessionKey, State2),
	
	State4 = case sets:is_element(SessionKey, State3#state.active_sessions) of
		false ->
			?LOG_DEBUG([{event, mining_debug_skipping_vdf_output}, {reason, stale_session},
				{step_number, StepNumber},
				{session_key, ar_nonce_limiter:encode_session_key(SessionKey)},
				{active_sessions, encode_sessions(State#state.active_sessions)}]),
			State3;
		true ->
			{NextSeed, StartIntervalNumber, NextVDFDifficulty} = SessionKey,
			Candidate = #mining_candidate{
				session_key = SessionKey,
				next_seed = NextSeed,
				next_vdf_difficulty = NextVDFDifficulty,
				start_interval_number = StartIntervalNumber,
				step_number = StepNumber,
				nonce_limiter_output = Output,
				partition_upper_bound = PartitionUpperBound
			},
			distribute_output(Candidate, State3),
			?LOG_DEBUG([{event, mining_debug_processing_vdf_output},
				{step_number, StepNumber}, {output, ar_util:safe_encode(Output)},
				{start_interval_number, StartIntervalNumber},
				{session_key, ar_nonce_limiter:encode_session_key(SessionKey)}])
	end,
	{noreply, State4};

handle_info({event, nonce_limiter, Message}, State) ->
	?LOG_DEBUG([{event, mining_debug_skipping_nonce_limiter}, {message, Message}]),
	{noreply, State};

handle_info({garbage_collect, StartTime, GCResult}, State) ->
	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime-StartTime, native, millisecond),
	case GCResult == false orelse ElapsedTime > 100 of
		true ->
			?LOG_DEBUG([
				{event, mining_debug_garbage_collect}, {process, ar_mining_server}, {pid, self()},
				{gc_time, ElapsedTime}, {gc_result, GCResult}]);
		false ->
			ok
	end,
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================
get_worker(Partition, State) ->
	maps:get(Partition, State#state.workers, not_found).

set_difficulty(Diff, State) ->
	maps:foreach(
		fun(_Partition, Worker) ->
			ar_mining_worker:set_difficulty(Worker, Diff)
		end,
		State#state.workers
	),
	State#state{ diff = Diff }.

maybe_update_sessions(SessionKey, State) ->
	CurrentActiveSessions = State#state.active_sessions,
	case sets:is_element(SessionKey, CurrentActiveSessions) of
		true ->
			State;
		false ->
			NewSession = ar_nonce_limiter:get_session(SessionKey),
			{CurrentSessionKey, CurrentSession} = ar_nonce_limiter:get_current_session(),
			NewActiveSessions = build_active_session_set(
					SessionKey, NewSession, CurrentSessionKey,
					CurrentSession#vdf_session.prev_session_key),
			case sets:to_list(sets:subtract(NewActiveSessions, CurrentActiveSessions)) of
				[] ->
					State;
				AddedSessions ->
					update_sessions(NewActiveSessions, AddedSessions, State)
			end
	end.

build_active_session_set(NewSessionKey, NewSession, CurrentSessionKey, _PrevSessionKey)
	when NewSession#vdf_session.prev_session_key == CurrentSessionKey ->
	sets:from_list([NewSessionKey, CurrentSessionKey]);
build_active_session_set(_NewSessionKey, _NewSession, CurrentSessionKey, PrevSessionKey)
	when PrevSessionKey == undefined ->
	sets:from_list([CurrentSessionKey]);
build_active_session_set(_NewSessionKey, _NewSession, CurrentSessionKey, PrevSessionKey) ->
	sets:from_list([CurrentSessionKey, PrevSessionKey]).

update_sessions(NewActiveSessions, AddedSessions, State) ->
	maps:foreach(
		fun(_Partition, Worker) ->
			ar_mining_worker:set_sessions(Worker, NewActiveSessions)
		end,
		State#state.workers
	),

	lists:foreach(
		fun(SessionKey) ->
			{NextSeed, StartIntervalNumber, NextVDFDifficulty} = SessionKey,
			ar:console("Starting new mining session: "
				"next entropy nonce: ~s, interval number: ~B, next vdf difficulty: ~B.~n",
				[ar_util:safe_encode(NextSeed), StartIntervalNumber, NextVDFDifficulty]),
			?LOG_INFO([{event, new_mining_session}, 
				{session_key, ar_nonce_limiter:encode_session_key(SessionKey)}])
		end,
		AddedSessions
	),

	State#state{ active_sessions = NewActiveSessions }.

update_cache_limits(State) ->
	NumActivePartitions = length(ar_mining_io:get_partitions()),
	%% This allows the cache to store enough chunks for 2 concurrent VDF steps per partition.
	IdealRangesPerStep = 4,
	ChunksPerRange = ?RECALL_RANGE_SIZE div ?DATA_CHUNK_SIZE,
	IdealCacheLimit = ar_util:ceil_int(
		IdealRangesPerStep * ChunksPerRange * NumActivePartitions, 100),

	{ok, Config} = application:get_env(arweave, config),
	OverallCacheLimit = case Config#config.mining_server_chunk_cache_size_limit of
		undefined ->
			IdealCacheLimit;
		N ->
			N
	end,

	%% We shard the chunk cache across every active worker. Only workers that mine a partition
	%% included in the current weave are active.
	NewCacheLimit = max(1, OverallCacheLimit div NumActivePartitions),

	case NewCacheLimit == State#state.chunk_cache_limit of
		true ->
			State;
		false ->
			%% Allow enough compute_h0 tasks to be queued to completely refill the chunk cache.
			VDFQueueLimit = NewCacheLimit div (2 * ChunksPerRange),
			maps:foreach(
				fun(_Partition, Worker) ->
					ar_mining_worker:set_cache_limits(Worker, NewCacheLimit, VDFQueueLimit)
				end,
				State#state.workers
			),
			ar_mining_hash:set_cache_limit(OverallCacheLimit),

			ar:console(
				"~nSetting the mining chunk cache size limit to ~B chunks "
				"(~B chunks per partition).~n", [OverallCacheLimit, NewCacheLimit]),
			?LOG_INFO([{event, update_mining_cache_limits},
				{limit, OverallCacheLimit}, {per_partition, NewCacheLimit},
				{vdf_queue_limit, VDFQueueLimit}]),
			case OverallCacheLimit < IdealCacheLimit of
				true ->
					ar:console("~nChunk cache size limit is below minimum limit of ~p. "
						"Mining performance may be impacted.~n"
						"Consider changing the 'mining_server_chunk_cache_size_limit' option.",
						[IdealCacheLimit]);
				false -> ok
			end,
			GarbageCollectionFrequency = 2 * VDFQueueLimit * 1000,
			case State#state.gc_frequency_ms == undefined of
				true ->
					%% This is the first time setting the garbage collection frequency, so kick
					%% off the periodic call.
					ar_util:cast_after(GarbageCollectionFrequency, ?MODULE, garbage_collect);
				false ->
					ok
			end,
			State#state{
				chunk_cache_limit = NewCacheLimit,
				gc_frequency_ms = GarbageCollectionFrequency
			}
	end.

distribute_output(Candidate, State) ->
	distribute_output(ar_mining_io:get_partitions(), Candidate, State).

distribute_output([], _Candidate, _State) ->
	ok;
distribute_output([{Partition, MiningAddress} | Partitions], Candidate, State) ->
	case get_worker(Partition, State) of
		not_found ->
			?LOG_ERROR([{event, worker_not_found}, {partition, Partition}]),
			ok;
		Worker ->
			ar_mining_worker:add_task(
				Worker, compute_h0,
				Candidate#mining_candidate{
					partition_number = Partition,
					mining_address = MiningAddress
				})
	end,
	distribute_output(Partitions, Candidate, State).

get_recall_bytes(H0, PartitionNumber, Nonce, PartitionUpperBound) ->
	{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
			PartitionNumber, PartitionUpperBound),
	RelativeOffset = Nonce * (?DATA_CHUNK_SIZE),
	{RecallRange1Start + RelativeOffset, RecallRange2Start + RelativeOffset}.

prepare_and_post_solution(Candidate, State) ->
	Solution = prepare_solution(Candidate, State),
	post_solution(Solution, State).

prepare_solution(Candidate, State) ->
	#state{ merkle_rebase_threshold = RebaseThreshold } = State,
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
	prepare_solution(last_step_checkpoints, Candidate, Solution).
	
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
					prepare_solution(proofs, Candidate, Solution#mining_solution{ steps = Steps })
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

prepare_solution(poa1, Candidate, #mining_solution{ poa1 = not_set } = Solution ) ->
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
prepare_solution(poa2, Candidate, #mining_solution{ poa2 = not_set } = Solution) ->
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
prepare_solution(poa2, Candidate, #mining_solution{ poa1 = not_set } = Solution) ->
	prepare_solution(poa1, Candidate, Solution);
prepare_solution(_, _Candidate, Solution) ->
	Solution.

post_solution(error, _State) ->
	error;
post_solution(Solution, State) ->
	{ok, Config} = application:get_env(arweave, config),
	post_solution(Config#config.cm_exit_peer, Solution, State).
post_solution(not_set, Solution, State) ->
	#state{ diff = Diff } = State,
	#mining_solution{
		mining_address = MiningAddress, nonce_limiter_output = NonceLimiterOutput,
		partition_number = PartitionNumber, recall_byte1 = RecallByte1, recall_byte2 = RecallByte2,
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
		false ->
			?LOG_WARNING([{event, found_invalid_solution},
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
			ar_events:send(miner, {found_solution, Solution, PoACache, PoA2Cache})
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
		merkle_rebase_threshold = RebaseThreshold, mining_address = MiningAddress,
		nonce = Nonce, nonce_limiter_output = NonceLimiterOutput,
		partition_number = PartitionNumber, partition_upper_bound = PartitionUpperBound,
		poa1 = PoA1, poa2 = PoA2, recall_byte1 = RecallByte1, recall_byte2 = RecallByte2,
		seed = Seed, solution_hash = SolutionHash } = Solution,
	H0 = ar_block:compute_h0(NonceLimiterOutput, PartitionNumber, Seed, MiningAddress),
	{H1, _Preimage1} = ar_block:compute_h1(H0, Nonce, PoA1#poa.chunk),
	{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
			PartitionNumber, PartitionUpperBound),
	%% validates recall_byte1
	RecallByte1 = RecallRange1Start + Nonce * ?DATA_CHUNK_SIZE,
	{BlockStart1, BlockEnd1, TXRoot1} = ar_block_index:get_block_bounds(RecallByte1),
	BlockSize1 = BlockEnd1 - BlockStart1,
	case ar_poa:validate({BlockStart1, RecallByte1, TXRoot1, BlockSize1, PoA1,
			?STRICT_DATA_SPLIT_THRESHOLD, {spora_2_6, MiningAddress}, RebaseThreshold,
			not_set}) of
		{true, ChunkID} ->
			PoACache = {{BlockStart1, RecallByte1, TXRoot1, BlockSize1,
					?STRICT_DATA_SPLIT_THRESHOLD, {spora_2_6, MiningAddress}, RebaseThreshold},
					ChunkID},
			case binary:decode_unsigned(H1, big) > Diff of
				true ->
					%% validates solution_hash
					SolutionHash = H1,
					{true, PoACache, undefined};
				false ->
					case RecallByte2 of
						undefined ->
							%% This can happen if the difficulty has increased between when the H1
							%% solution was found and now. In this case there is no H2 solution, so
							%% we flag the solution invalid.
							false;
						_ ->
							{H2, _Preimage2} = ar_block:compute_h2(H1, PoA2#poa.chunk, H0),
							case binary:decode_unsigned(H2, big) > Diff of
								false ->
									false;
								true ->
									%% validates solution_hash
									SolutionHash = H2,
									%% validates recall_byte2
									RecallByte2 = RecallRange2Start + Nonce * ?DATA_CHUNK_SIZE,
									{BlockStart2, BlockEnd2, TXRoot2} =
											ar_block_index:get_block_bounds(RecallByte2),
									BlockSize2 = BlockEnd2 - BlockStart2,
									case ar_poa:validate({BlockStart2, RecallByte2, TXRoot2,
											BlockSize2, PoA2, ?STRICT_DATA_SPLIT_THRESHOLD,
											{spora_2_6, MiningAddress}, RebaseThreshold, not_set}) of
										{true, Chunk2ID} ->
											PoA2Cache = {{BlockStart2, RecallByte2, TXRoot2,
													BlockSize2, ?STRICT_DATA_SPLIT_THRESHOLD,
													{spora_2_6, MiningAddress}, RebaseThreshold},
													Chunk2ID},
											{true, PoACache, PoA2Cache};
										Result2 ->
											Result2
									end
							end
					end
			end;
		Result ->
			Result
	end.

%%%===================================================================
%%% Public Test interface.
%%%===================================================================

%% @doc Pause the mining server. Only used in tests.
pause() ->
	gen_server:cast(?MODULE, pause).