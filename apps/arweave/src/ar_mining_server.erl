%%% @doc The 2.6 mining server.
-module(ar_mining_server).

-behaviour(gen_server).

-export([start_link/0,
		start_mining/1, set_difficulty/1, set_merkle_rebase_threshold/1, set_height/1,
		compute_h2_for_peer/1, prepare_and_post_solution/1, post_solution/1, read_poa/3,
		get_recall_bytes/5, active_sessions/0, encode_sessions/1, add_pool_job/6,
		is_one_chunk_solution/1, fetch_poa_from_peers/2]).
-export([pause/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_data_discovery.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-record(state, {
	paused 						= true,
	workers						= #{},
	active_sessions				= sets:new(),
	seeds						= #{},
	diff_pair					= not_set,
	chunk_cache_limit 			= 0,
	gc_frequency_ms				= undefined,
	gc_process_ref				= undefined,
	merkle_rebase_threshold		= infinity,
	is_pool_client				= false,
	allow_composite_packing		= false
}).

-ifdef(DEBUG).
-define(POST_2_8_COMPOSITE_PACKING_DELAY_BLOCKS, 0).
-else.
-define(POST_2_8_COMPOSITE_PACKING_DELAY_BLOCKS, 10).
-endif.

-define(FETCH_POA_FROM_PEERS_TIMEOUT_MS, 10000).

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
set_difficulty(DiffPair) ->
	gen_server:cast(?MODULE, {set_difficulty, DiffPair}).

set_merkle_rebase_threshold(Threshold) ->
	gen_server:cast(?MODULE, {set_merkle_rebase_threshold, Threshold}).

set_height(Height) ->
	gen_server:cast(?MODULE, {set_height, Height}).

%% @doc Add a pool job to the mining queue.
add_pool_job(SessionKey, StepNumber, Output, PartitionUpperBound, Seed, PartialDiff) ->
	Args = {SessionKey, StepNumber, Output, PartitionUpperBound, Seed, PartialDiff},
	gen_server:cast(?MODULE, {add_pool_job, Args}).

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

is_one_chunk_solution(Solution) ->
	Solution#mining_solution.recall_byte2 == undefined.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	%% Trap exit to avoid corrupting any open files on quit.
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(nonce_limiter),
	ar_chunk_storage:open_files("default"),

	Workers = lists:foldl(
		fun({Partition, _MiningAddr, PackingDifficulty}, Acc) ->
			maps:put({Partition, PackingDifficulty},
					ar_mining_worker:name(Partition, PackingDifficulty), Acc)
		end,
		#{},
		ar_mining_io:get_partitions(infinity)
	),

	{ok, #state{
		workers = Workers,
		is_pool_client = ar_pool:is_client()
	}}.

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
	State2 = set_difficulty({infinity, infinity}, State),
	{noreply, State2#state{ paused = true }};

handle_cast({start_mining, Args}, State) ->
	{DiffPair, RebaseThreshold, Height} = Args,
	ar:console("Starting mining.~n"),
	?LOG_INFO([{event, start_mining}, {difficulty, DiffPair},
			{rebase_threshold, RebaseThreshold}, {height, Height}]),
	ar_mining_stats:start_performance_reports(),

	maps:foreach(
		fun(_Partition, Worker) ->
			ar_mining_worker:reset(Worker, DiffPair)
		end,
		State#state.workers
	),

	{noreply, State#state{ 
		paused = false,
		active_sessions	= sets:new(),
		diff_pair = DiffPair,
		merkle_rebase_threshold = RebaseThreshold,
		allow_composite_packing
			= Height - ?POST_2_8_COMPOSITE_PACKING_DELAY_BLOCKS >= ar_fork:height_2_8() }};

handle_cast({set_difficulty, DiffPair}, State) ->
	State2 = set_difficulty(DiffPair, State),
	{noreply, State2};

handle_cast({set_merkle_rebase_threshold, Threshold}, State) ->
	{noreply, State#state{ merkle_rebase_threshold = Threshold }};

handle_cast({set_height, Height}, State) ->
	{noreply, State#state{ allow_composite_packing
			= Height - ?POST_2_8_COMPOSITE_PACKING_DELAY_BLOCKS >= ar_fork:height_2_8() }};

handle_cast({add_pool_job, Args}, State) ->
	{SessionKey, StepNumber, Output, PartitionUpperBound, Seed, PartialDiff} = Args,
	State2 = set_seed(SessionKey, Seed, State),
	handle_computed_output(
		SessionKey, StepNumber, Output, PartitionUpperBound, PartialDiff, State2);

handle_cast({compute_h2_for_peer, Candidate}, State) ->
	#mining_candidate{ partition_number2 = Partition2,
			packing_difficulty = PackingDifficulty } = Candidate,
	case get_worker({Partition2, PackingDifficulty}, State) of
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

handle_cast({manual_garbage_collect, Ref}, #state{ gc_process_ref = Ref } = State) ->
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
	ar_coordination:garbage_collect(),
	ar_util:cast_after(State#state.gc_frequency_ms, ?MODULE, {manual_garbage_collect, Ref}),
	{noreply, State};
handle_cast({manual_garbage_collect, _}, State) ->
	%% Does not originate from the running instance of the server; happens in tests.
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({event, nonce_limiter, {computed_output, _Args}}, #state{ paused = true } = State) ->
	{noreply, State};
handle_info({event, nonce_limiter, {computed_output, Args}}, State) ->
	case ar_pool:is_client() of
		true ->
			%% Ignore VDF events because we are receiving jobs from the pool.
			{noreply, State};
		false ->
			{SessionKey, StepNumber, Output, PartitionUpperBound} = Args,
			handle_computed_output(
				SessionKey, StepNumber, Output, PartitionUpperBound, not_set, State)
	end;

handle_info({event, nonce_limiter, Message}, State) ->
	?LOG_DEBUG([{event, mining_debug_skipping_nonce_limiter}, {message, Message}]),
	{noreply, State};

handle_info({garbage_collect, StartTime, GCResult}, State) ->
	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime-StartTime, native, millisecond),
	case GCResult == false orelse ElapsedTime > ?GC_LOG_THRESHOLD of
		true ->
			?LOG_DEBUG([
				{event, mining_debug_garbage_collect}, {process, ar_mining_server},
				{pid, self()}, {gc_time, ElapsedTime}, {gc_result, GCResult}]);
		false ->
			ok
	end,
	{noreply, State};

handle_info({fetched_last_moment_proof, _}, State) ->
    %% This is a no-op to handle "slow" response from peers that were queried by
	%% `fetch_poa_from_peers`. Only the first peer to respond with a PoA will be handled,
	%% all other responses will fall through to here an be ignored.
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_worker(Key, State) ->
	maps:get(Key, State#state.workers, not_found).

set_difficulty(DiffPair, State) ->
	maps:foreach(
		fun(_Partition, Worker) ->
			ar_mining_worker:set_difficulty(Worker, DiffPair)
		end,
		State#state.workers
	),
	State#state{ diff_pair = DiffPair }.

maybe_update_sessions(SessionKey, State) ->
	CurrentActiveSessions = State#state.active_sessions,
	case sets:is_element(SessionKey, CurrentActiveSessions) of
		true ->
			State;
		false ->
			NewActiveSessions = build_active_session_set(SessionKey, CurrentActiveSessions),
			case sets:to_list(sets:subtract(NewActiveSessions, CurrentActiveSessions)) of
				[] ->
					State;
				_ ->
					update_sessions(NewActiveSessions, CurrentActiveSessions, State)
			end
	end.

build_active_session_set(SessionKey, CurrentActiveSessions) ->
	CandidateSessions = [SessionKey | sets:to_list(CurrentActiveSessions)],
	SortedSessions = lists:sort(
		fun({_, StartIntervalA, _}, {_, StartIntervalB, _}) ->
			StartIntervalA > StartIntervalB
		end, CandidateSessions),
	build_active_session_set(SortedSessions).

build_active_session_set([A, B | _]) ->
	sets:from_list([A, B]);
build_active_session_set([A]) ->
	sets:from_list([A]);
build_active_session_set([]) ->
	sets:new().

update_sessions(NewActiveSessions, CurrentActiveSessions, State) ->
	AddedSessions = sets:to_list(sets:subtract(NewActiveSessions, CurrentActiveSessions)),
	RemovedSessions = sets:to_list(sets:subtract(CurrentActiveSessions, NewActiveSessions)),

	maps:foreach(
		fun(_Partition, Worker) ->
			ar_mining_worker:set_sessions(Worker, NewActiveSessions)
		end,
		State#state.workers
	),

	State2 = add_sessions(AddedSessions, State),
	State3 = remove_sessions(RemovedSessions, State2),

	State3#state{ active_sessions = NewActiveSessions }.

add_sessions([], State) ->
	State;
add_sessions([SessionKey | AddedSessions], State) ->
	{NextSeed, StartIntervalNumber, NextVDFDifficulty} = SessionKey,
	ar:console("Starting new mining session: "
		"next entropy nonce: ~s, interval number: ~B, next vdf difficulty: ~B.~n",
		[ar_util:safe_encode(NextSeed), StartIntervalNumber, NextVDFDifficulty]),
	?LOG_INFO([{event, new_mining_session}, 
		{session_key, ar_nonce_limiter:encode_session_key(SessionKey)}]),
	add_sessions(AddedSessions, add_seed(SessionKey, State)).

remove_sessions([], State) ->
	State;
remove_sessions([SessionKey | RemovedSessions], State) ->
	remove_sessions(RemovedSessions, remove_seed(SessionKey, State)).

get_seed(SessionKey, State) ->
	maps:get(SessionKey, State#state.seeds, not_found).

set_seed(SessionKey, Seed, State) ->
	State#state{ seeds = maps:put(SessionKey, Seed, State#state.seeds) }.

remove_seed(SessionKey, State) ->
	State#state{ seeds = maps:remove(SessionKey, State#state.seeds) }.

add_seed(SessionKey, State) ->
	case get_seed(SessionKey, State) of
		not_found ->
			Session = ar_nonce_limiter:get_session(SessionKey),
			case Session of
				not_found ->
					?LOG_ERROR([{event, mining_session_not_found},
						{session_key, ar_nonce_limiter:encode_session_key(SessionKey)}]),
					State;
				_ ->
					set_seed(SessionKey, Session#vdf_session.seed, State)
			end;
		_ ->
			State
	end.

update_cache_limits(State) ->
	NumActivePartitions = length(ar_mining_io:get_partitions()),
	update_cache_limits(NumActivePartitions, State).

update_cache_limits(0, State) ->
	State;
update_cache_limits(NumActivePartitions, State) ->
	%% This allows the cache to store enough chunks for 4 concurrent VDF steps per partition.
	IdealStepsPerPartition = 4,
	IdealRangesPerStep = 2,
	ChunksPerRange = ar_mining_worker:recall_range_sub_chunks(),
	IdealCacheLimit = ar_util:ceil_int(
		IdealStepsPerPartition * IdealRangesPerStep * ChunksPerRange * NumActivePartitions, 100),

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
			GarbageCollectionFrequency = 4 * VDFQueueLimit * 1000,
			GCRef =
				case State#state.gc_frequency_ms == undefined of
					true ->
						%% This is the first time setting the garbage collection frequency,
						%% so kick off the periodic call.
						Ref = make_ref(),
						ar_util:cast_after(GarbageCollectionFrequency, ?MODULE,
								{manual_garbage_collect, Ref}),
						Ref;
					false ->
						State#state.gc_process_ref
				end,
			State#state{
				chunk_cache_limit = NewCacheLimit,
				gc_frequency_ms = GarbageCollectionFrequency,
				gc_process_ref = GCRef
			}
	end.

distribute_output(Candidate, State) ->
	distribute_output(ar_mining_io:get_partitions(), Candidate, State).

distribute_output([], _Candidate, _State) ->
	ok;
distribute_output([{_Partition, _MiningAddress, PackingDifficulty} | _Partitions],
		_Candidate, #state{ allow_composite_packing = false }) when PackingDifficulty >= 1 ->
	%% Do not mine with the composite packing until some time after the fork 2.8.
	ok;
distribute_output([{Partition, MiningAddress, PackingDifficulty} | Partitions],
		Candidate, State) ->
	case get_worker({Partition, PackingDifficulty}, State) of
		not_found ->
			?LOG_ERROR([{event, worker_not_found}, {partition, Partition}]),
			ok;
		Worker ->
			ar_mining_worker:add_task(
				Worker, compute_h0,
				Candidate#mining_candidate{
					partition_number = Partition,
					mining_address = MiningAddress,
					packing_difficulty = PackingDifficulty
				})
	end,
	distribute_output(Partitions, Candidate, State).

get_recall_bytes(H0, PartitionNumber, Nonce, PartitionUpperBound, PackingDifficulty) ->
	{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
			PartitionNumber, PartitionUpperBound),
	RecallByte1 = ar_block:get_recall_byte(RecallRange1Start, Nonce, PackingDifficulty),
	RecallByte2 = ar_block:get_recall_byte(RecallRange2Start, Nonce, PackingDifficulty),
	{RecallByte1, RecallByte2}.

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
		seed = Seed, start_interval_number = StartIntervalNumber, step_number = StepNumber,
		packing_difficulty = PackingDifficulty
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
		step_number = StepNumber,
		packing_difficulty = PackingDifficulty
	},
	%% A pool client does not validate VDF before sharing a solution.
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
	LastStepCheckpoints2 =
		case LastStepCheckpoints of
			not_found ->
				?LOG_WARNING([{event,
						found_solution_but_failed_to_find_last_step_checkpoints}]),
				[];
			_ ->
				LastStepCheckpoints
		end,
	prepare_solution(steps, Candidate, Solution#mining_solution{
			last_step_checkpoints = LastStepCheckpoints2 });

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
		partition_upper_bound = PartitionUpperBound,
		packing_difficulty = PackingDifficulty } = Candidate,
	#mining_solution{ poa1 = PoA1, poa2 = PoA2 } = Solution,
	{RecallByte1, RecallByte2} = get_recall_bytes(H0, PartitionNumber, Nonce,
			PartitionUpperBound, PackingDifficulty),
	case { H1, H2 } of
		{not_set, not_set} ->
			?LOG_WARNING([{event, found_solution_but_h1_h2_not_set}]),
			error;
		{H1, not_set} ->
			prepare_solution(poa1, Candidate, Solution#mining_solution{
				solution_hash = H1, recall_byte1 = RecallByte1,
				poa1 = may_be_empty_poa(PoA1), poa2 = #poa{} });
		{_, H2} ->
			prepare_solution(poa2, Candidate, Solution#mining_solution{
				solution_hash = H2, recall_byte1 = RecallByte1, recall_byte2 = RecallByte2,
				poa1 = may_be_empty_poa(PoA1), poa2 = may_be_empty_poa(PoA2) })
	end;

prepare_solution(poa1, Candidate,
		#mining_solution{ poa1 = #poa{ chunk = <<>> },
				packing_difficulty = PackingDifficulty } = Solution) ->
	#mining_solution{
		mining_address = MiningAddress, partition_number = PartitionNumber,
		recall_byte1 = RecallByte1 } = Solution,
	#mining_candidate{
		chunk1 = Chunk1, h0 = H0, nonce = Nonce,
		partition_upper_bound = PartitionUpperBound } = Candidate,
	case read_poa(RecallByte1, Chunk1,
			ar_block:get_packing(PackingDifficulty, MiningAddress)) of
		{ok, PoA1} ->
			Solution#mining_solution{ poa1 = PoA1 };
		{error, Error} ->
			Modules = ar_storage_module:get_all(RecallByte1 + 1),
			ModuleIDs = [ar_storage_module:id(Module) || Module <- Modules],
			?LOG_ERROR([{event, failed_to_find_poa_proofs_locally},
					{error, io_lib:format("~p", [Error])},
					{tags, [solution_proofs]},
					{recall_byte, RecallByte1},
					{modules_covering_recall_byte, ModuleIDs}]),
			ar:console("WARNING: we have mined a block but did not find the PoA1 proofs "
					"locally - searching the peers...~n"),
			case fetch_poa_from_peers(RecallByte1, PackingDifficulty) of
				not_found ->
					{RecallRange1Start, _RecallRange2Start} = ar_block:get_recall_range(H0,
							PartitionNumber, PartitionUpperBound),
					?LOG_WARNING([{event, mined_block_but_failed_to_read_chunk_proofs},
							{recall_byte1, RecallByte1},
							{recall_range_start1, RecallRange1Start},
							{nonce, Nonce},
							{partition, PartitionNumber},
							{mining_address, ar_util:safe_encode(MiningAddress)}]),
					ar:console("WARNING: we have mined a block but failed to find "
							"the PoA1 proofs required for publishing it. "
							"Check logs for more details~n"),
					error;
				PoA1 ->
					Solution#mining_solution{ poa1 = PoA1#poa{ chunk = Chunk1 } }
			end
	end;
prepare_solution(poa2, Candidate,
		#mining_solution{ poa2 = #poa{ chunk = <<>> },
				packing_difficulty = PackingDifficulty } = Solution) ->
	#mining_solution{ mining_address = MiningAddress, partition_number = PartitionNumber,
		recall_byte2 = RecallByte2 } = Solution,
	#mining_candidate{
		chunk2 = Chunk2, h0 = H0, nonce = Nonce,
		partition_upper_bound = PartitionUpperBound } = Candidate,
	case read_poa(RecallByte2, Chunk2,
			ar_block:get_packing(PackingDifficulty, MiningAddress)) of
		{ok, PoA2} ->
			prepare_solution(poa1, Candidate, Solution#mining_solution{ poa2 = PoA2 });
		{error, Error} ->
			Modules = ar_storage_module:get_all(RecallByte2 + 1),
			ModuleIDs = [ar_storage_module:id(Module) || Module <- Modules],
			?LOG_ERROR([{event, failed_to_find_poa2_proofs_locally},
					{error, io_lib:format("~p", [Error])},
					{tags, [solution_proofs]},
					{recall_byte2, RecallByte2},
					{modules_covering_recall_byte2, ModuleIDs}]),
			ar:console("WARNING: we have mined a block but did not find the PoA2 proofs "
					"locally - searching the peers...~n"),
			case fetch_poa_from_peers(RecallByte2, PackingDifficulty) of
				not_found ->
					{_RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
							PartitionNumber, PartitionUpperBound),
					?LOG_ERROR([{event, mined_block_but_failed_to_read_chunk_proofs},
							{tags, [solution_proofs]},
							{recall_byte2, RecallByte2},
							{recall_range_start2, RecallRange2Start},
							{nonce, Nonce},
							{partition, PartitionNumber},
							{mining_address, ar_util:safe_encode(MiningAddress)}]),
					ar:console("WARNING: we have mined a block but failed to find "
							"the PoA2 proofs required for publishing it. "
							"Check logs for more details~n"),
					error;
				PoA2 ->
					prepare_solution(poa1, Candidate,
							Solution#mining_solution{ poa2 = PoA2#poa{ chunk = Chunk2 } })
			end
	end;
prepare_solution(poa2, Candidate,
		#mining_solution{ poa1 = #poa{ chunk = <<>> } } = Solution) ->
	prepare_solution(poa1, Candidate, Solution);
prepare_solution(_, _Candidate, Solution) ->
	Solution.

post_solution(error, _State) ->
	?LOG_WARNING([{event, found_solution_but_could_not_build_a_block}]),
	error;
post_solution(Solution, State) ->
	{ok, Config} = application:get_env(arweave, config),
	post_solution(Config#config.cm_exit_peer, Solution, State).

post_solution(not_set, Solution, #state{ is_pool_client = true }) ->
	%% When posting a partial solution the pool client will skip many of the validation steps
	%% that are normally performed before sharing a solution.
	ar_pool:post_partial_solution(Solution);
post_solution(not_set, Solution, State) ->
	#state{ diff_pair = DiffPair } = State,
	#mining_solution{
		mining_address = MiningAddress, nonce_limiter_output = NonceLimiterOutput,
		partition_number = PartitionNumber, recall_byte1 = RecallByte1,
		recall_byte2 = RecallByte2,
		solution_hash = H, step_number = StepNumber } = Solution,
	case validate_solution(Solution, DiffPair) of
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
	case ar_http_iface_client:post_partial_solution(ExitPeer, Solution) of
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

may_be_empty_poa(not_set) ->
	#poa{};
may_be_empty_poa(#poa{} = PoA) ->
	PoA.

fetch_poa_from_peers(_RecallByte, PackingDifficulty) when PackingDifficulty >= 1 ->
	not_found;
fetch_poa_from_peers(RecallByte, _PackingDifficulty) ->
	Peers = ar_data_discovery:get_bucket_peers(RecallByte div ?NETWORK_DATA_BUCKET_SIZE),
	From = self(),
	lists:foreach(
		fun(Peer) ->
			spawn(
				fun() ->
					?LOG_INFO([{event, last_moment_proof_search},
							{peer, ar_util:format_peer(Peer)}, {recall_byte, RecallByte}]),
					case fetch_poa_from_peer(Peer, RecallByte) of
						not_found ->
							ok;
						PoA ->
							From ! {fetched_last_moment_proof, PoA}
					end
				end)
		end,
		Peers
	),
	receive
         %% The first spawned process to fetch a PoA from a peer will trigger this `receive`
		 %% and allow `fetch_poa_from_peers` to exit. All other processes that complete later
		 %% will trigger the
		 %% `handle_info({fetched_last_moment_proof, _}, State) ->` above (which is a no-op).
		{fetched_last_moment_proof, PoA} ->
			PoA
		after ?FETCH_POA_FROM_PEERS_TIMEOUT_MS ->
			not_found
	end.

fetch_poa_from_peer(Peer, RecallByte) ->
	case ar_http_iface_client:get_chunk_binary(Peer, RecallByte + 1, any) of
		{ok, #{ data_path := DataPath, tx_path := TXPath }, _, _} ->
			#poa{ data_path = DataPath, tx_path = TXPath };
		_ ->
			not_found
	end.

handle_computed_output(SessionKey, StepNumber, Output, PartitionUpperBound,
		PartialDiff, State) ->
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

	case sets:is_element(SessionKey, State3#state.active_sessions) of
		false ->
			?LOG_DEBUG([{event, mining_debug_skipping_vdf_output}, {reason, stale_session},
				{step_number, StepNumber},
				{session_key, ar_nonce_limiter:encode_session_key(SessionKey)},
				{active_sessions, encode_sessions(State#state.active_sessions)}]);
		true ->
			{NextSeed, StartIntervalNumber, NextVDFDifficulty} = SessionKey,
			Candidate = #mining_candidate{
				session_key = SessionKey,
				seed = get_seed(SessionKey, State3),
				next_seed = NextSeed,
				next_vdf_difficulty = NextVDFDifficulty,
				start_interval_number = StartIntervalNumber,
				step_number = StepNumber,
				nonce_limiter_output = Output,
				partition_upper_bound = PartitionUpperBound,
				cm_diff = PartialDiff
			},
			distribute_output(Candidate, State3),
			?LOG_DEBUG([{event, mining_debug_processing_vdf_output},
				{step_number, StepNumber}, {output, ar_util:safe_encode(Output)},
				{start_interval_number, StartIntervalNumber},
				{session_key, ar_nonce_limiter:encode_session_key(SessionKey)}])
	end,
	{noreply, State3}.

read_poa(RecallByte, ChunkOrSubChunk, Packing) ->
	PoAReply = read_poa(RecallByte, Packing),
	case {ChunkOrSubChunk, PoAReply, Packing} of
		{not_set, _PoAReply, _Packing} ->
			PoAReply;
		{ChunkOrSubChunk, {ok, #poa{ chunk = Chunk } = PoA}, {composite, _, _}} ->
			case sub_chunk_belongs_to_chunk(ChunkOrSubChunk, Chunk) of
				true ->
					{ok, PoA#poa{ chunk = ChunkOrSubChunk }};
				false ->
					{error, sub_chunk_mismatch};
				Error2 ->
					Error2
			end;
		{Chunk, {ok, #poa{ chunk = Chunk }}, _Packing} ->
			PoAReply;
		{_Chunk, {ok, #poa{}}, _Packing} ->
			{error, chunk_mismatch};
		{_Chunk, Error, _Packing} ->
			Error
	end.

sub_chunk_belongs_to_chunk(SubChunk,
		<< SubChunk:?PACKING_DIFFICULTY_ONE_SUB_CHUNK_SIZE/binary, _Rest/binary >>) ->
	true;
sub_chunk_belongs_to_chunk(SubChunk,
		<< _SubChunk:?PACKING_DIFFICULTY_ONE_SUB_CHUNK_SIZE/binary, Rest/binary >>) ->
	sub_chunk_belongs_to_chunk(SubChunk, Rest);
sub_chunk_belongs_to_chunk(_SubChunk, <<>>) ->
	false;
sub_chunk_belongs_to_chunk(_SubChunk, _Chunk) ->
	{error, uneven_chunk}.

read_poa(RecallByte, Packing) ->
	Options = #{ pack => true, packing => Packing, is_miner_request => true },
	case ar_data_sync:get_chunk(RecallByte + 1, Options) of
		{ok, #{ chunk := Chunk, tx_path := TXPath, data_path := DataPath } = Proof} ->
			case Packing of
				{composite, _Addr, _PackingDifficulty} ->
					case maps:get(unpacked_chunk, Proof, not_found) of
						not_found ->
							read_unpacked_chunk(RecallByte, Proof);
						UnpackedChunk ->
							{ok, #poa{ option = 1, chunk = Chunk,
								unpacked_chunk = ar_packing_server:pad_chunk(UnpackedChunk),
								tx_path = TXPath, data_path = DataPath }}
					end;
				_ ->
					{ok, #poa{ option = 1, chunk = Chunk,
							tx_path = TXPath, data_path = DataPath }}
			end;
		Error ->
			Error
	end.

read_unpacked_chunk(RecallByte, Proof) ->
	Options = #{ pack => true, packing => unpacked, is_miner_request => true },
	case ar_data_sync:get_chunk(RecallByte + 1, Options) of
		{ok, #{ chunk := UnpackedChunk, tx_path := TXPath, data_path := DataPath }} ->
			{ok, #poa{ option = 1, chunk = maps:get(chunk, Proof),
				unpacked_chunk = ar_packing_server:pad_chunk(UnpackedChunk),
				tx_path = TXPath, data_path = DataPath }};
		Error ->
			Error
	end.

validate_solution(Solution, DiffPair) ->
	#mining_solution{
		mining_address = MiningAddress,
		nonce = Nonce, nonce_limiter_output = NonceLimiterOutput,
		partition_number = PartitionNumber, partition_upper_bound = PartitionUpperBound,
		poa1 = PoA1, recall_byte1 = RecallByte1, seed = Seed,
		solution_hash = SolutionHash,
		packing_difficulty = PackingDifficulty } = Solution,
	H0 = ar_block:compute_h0(NonceLimiterOutput, PartitionNumber, Seed, MiningAddress,
			PackingDifficulty),
	{H1, _Preimage1} = ar_block:compute_h1(H0, Nonce, PoA1#poa.chunk),
	{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
			PartitionNumber, PartitionUpperBound),
	%% Assert recall_byte1 is computed correctly.
	RecallByte1 = ar_block:get_recall_byte(RecallRange1Start, Nonce, PackingDifficulty),
	{BlockStart1, BlockEnd1, TXRoot1} = ar_block_index:get_block_bounds(RecallByte1),
	BlockSize1 = BlockEnd1 - BlockStart1,
	Packing = ar_block:get_packing(PackingDifficulty, MiningAddress),
	case ar_poa:validate({BlockStart1, RecallByte1, TXRoot1, BlockSize1, PoA1,
			Packing, not_set}) of
		{true, ChunkID} ->
			PoACache = {{BlockStart1, RecallByte1, TXRoot1, BlockSize1, Packing}, ChunkID},
			case ar_node_utils:h1_passes_diff_check(H1, DiffPair, PackingDifficulty) of
				true ->
					%% validates solution_hash
					SolutionHash = H1,
					{true, PoACache, undefined};
				false ->
					case is_one_chunk_solution(Solution) of
						true ->
							%% This can happen if the difficulty has increased between the
							%% time the H1 solution was found and now. In this case,
							%% there is no H2 solution, so we flag the solution invalid.
							{false, h1_diff_check};
						false ->
							#mining_solution{
								recall_byte2 = RecallByte2, poa2 = PoA2 } = Solution,
							{H2, _Preimage2} = ar_block:compute_h2(H1, PoA2#poa.chunk, H0),
							case ar_node_utils:h2_passes_diff_check(H2, DiffPair,
									PackingDifficulty) of
								false ->
									{false, h2_diff_check};
								true ->
									SolutionHash = H2,
									RecallByte2 = ar_block:get_recall_byte(RecallRange2Start,
											Nonce, PackingDifficulty),
									{BlockStart2, BlockEnd2, TXRoot2} =
											ar_block_index:get_block_bounds(RecallByte2),
									BlockSize2 = BlockEnd2 - BlockStart2,
									case ar_poa:validate({BlockStart2, RecallByte2, TXRoot2,
											BlockSize2, PoA2,
											Packing, not_set}) of
										{true, Chunk2ID} ->
											PoA2Cache = {{BlockStart2, RecallByte2, TXRoot2,
													BlockSize2, Packing}, Chunk2ID},
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
