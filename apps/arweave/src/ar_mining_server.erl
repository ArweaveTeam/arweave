%%% @doc The 2.6 mining server.
-module(ar_mining_server).

-behaviour(gen_server).

-export([start_link/0,
		start_mining/1, set_difficulty/1, set_merkle_rebase_threshold/1, set_height/1,
		compute_h2_for_peer/1, prepare_and_post_solution/1, prepare_poa/3,
		get_recall_bytes/5, active_sessions/0, encode_sessions/1, add_pool_job/6,
		is_one_chunk_solution/1, fetch_poa_from_peers/2, log_prepare_solution_failure/5,
		get_packing_difficulty/1, get_packing_type/1]).
-export([pause/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
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
	allow_composite_packing		= false,
	allow_replica_2_9_mining	= false,
	packing_difficulty			= 0
}).

-ifdef(AR_TEST).
-define(POST_2_8_COMPOSITE_PACKING_DELAY_BLOCKS, 0).
-define(MINIMUM_CACHE_LIMIT_BYTES, 100 * ?MiB).
-else.
-define(POST_2_8_COMPOSITE_PACKING_DELAY_BLOCKS, 10).
-define(MINIMUM_CACHE_LIMIT_BYTES, 1).
-endif.

%% The number of concurrent VDF steps per partition that will fit in the cache. The higher this
%% number the more memory the cache can use (roughly ?IDEAL_STEPS_PER_PARTITION * 5 MiB per
%% partition). Also the higher the number the more the miner is able to respond to temporary
%% hashrate slowdowns (e.g. a system process temporarily consumes all CPU) or temporary VDF
%% step spikes (e.g. the node validates an block with an advanced VDF step and unlocks many
%% VDF steps at once) without losing hashrate.
-define(IDEAL_STEPS_PER_PARTITION, 20).

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

prepare_and_post_solution(CandidateOrSolution) ->
	gen_server:cast(?MODULE, {prepare_and_post_solution, CandidateOrSolution}).

active_sessions() ->
	gen_server:call(?MODULE, active_sessions).

encode_sessions(Sessions) ->
	lists:map(fun(SessionKey) ->
		ar_nonce_limiter:encode_session_key(SessionKey)
	end, Sessions).

is_one_chunk_solution(Solution) ->
	Solution#mining_solution.recall_byte2 == undefined.

%% @doc Use this function every time the miner finds a solution but fails to prepare a block.
%% It may happen to a standalone miner, a worker in a coordinated mining setup, the exit node,
%% a pool worker in a pool or the pool server.

-spec log_prepare_solution_failure(
	Solution :: #mining_solution{},
	FailureType :: stale | rejected,
	FailureReason :: atom(),
	Source :: atom(),
	AdditionalLogData :: list({atom(), term()})
) ->
	Ret :: ok.

-ifdef(AR_TEST).
log_prepare_solution_failure(_Solution, stale, _FailureReason, _Source, _AdditionalLogData) ->
	ok;
log_prepare_solution_failure(Solution, FailureType, FailureReason, Source, AdditionalLogData) ->
	log_prepare_solution_failure2(Solution, FailureType, FailureReason, Source, AdditionalLogData).
-else.
log_prepare_solution_failure(Solution, FailureType, FailureReason, Source, AdditionalLogData) ->
	log_prepare_solution_failure2(Solution, FailureType, FailureReason, Source, AdditionalLogData).
-endif.

log_prepare_solution_failure2(Solution, FailureType, FailureReason, Source, AdditionalLogData) ->
	#mining_solution{
		solution_hash = SolutionH,
		packing_difficulty = PackingDifficulty } = Solution,

	ar_events:send(solution, {FailureType,
		#{ solution_hash => SolutionH,
			reason => FailureReason,
			source => Source }}),

	ar:console("~nFailed to prepare block from the mining solution.. Reason: ~p~n",
			[FailureReason]),
	?LOG_ERROR([{event, failed_to_prepare_block_from_mining_solution},
			{reason, FailureReason},
			{solution_hash, ar_util:safe_encode(SolutionH)},
			{packing_difficulty, PackingDifficulty} | AdditionalLogData]),
	prometheus_gauge:inc(mining_solution, [FailureReason]).

-spec get_packing_difficulty(Packing :: ar_storage_module:packing()) ->
	PackingDifficulty :: non_neg_integer().
get_packing_difficulty({composite, _, Difficulty}) ->
	Difficulty;
get_packing_difficulty({replica_2_9, _}) ->
	?REPLICA_2_9_PACKING_DIFFICULTY;
get_packing_difficulty(_) ->
	0.

-spec get_packing_type(Packing :: ar_storage_module:packing()) ->
	PackingType :: atom().
get_packing_type({composite, _, _}) ->
	composite;
get_packing_type({replica_2_9, _}) ->
	replica_2_9;
get_packing_type({spora_2_6, _}) ->
	spora_2_6;
get_packing_type(Packing) ->
	Packing.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	%% Trap exit to avoid corrupting any open files on quit.
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(nonce_limiter),
	ar_chunk_storage:open_files(?DEFAULT_MODULE),

	Partitions = ar_mining_io:get_partitions(infinity),
	Packing = ar_mining_io:get_packing(),
	PackingDifficulty = get_packing_difficulty(Packing),

	Workers = lists:foldl(
		fun({Partition, _Addr, Difficulty}, Acc) ->
			maps:put({Partition, Difficulty},
					ar_mining_worker:name(Partition, Difficulty), Acc)
		end,
		#{},
		Partitions
	),

	?LOG_INFO([{event, mining_server_init},
			{packing, ar_serialize:encode_packing(Packing, false)},
			{partitions, [ Partition || {Partition, _, _} <- Partitions]}]),

	{ok, #state{
		workers = Workers,
		is_pool_client = ar_pool:is_client(),
		packing_difficulty = PackingDifficulty
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
			ar_mining_worker:reset_mining_session(Worker, DiffPair)
		end,
		State#state.workers
	),

	{noreply, State#state{
		paused = false,
		active_sessions	= sets:new(),
		diff_pair = DiffPair,
		merkle_rebase_threshold = RebaseThreshold,
		allow_composite_packing = allow_composite_packing(Height),
		allow_replica_2_9_mining = allow_replica_2_9_mining(Height) }};

handle_cast({set_difficulty, DiffPair}, State) ->
	State2 = set_difficulty(DiffPair, State),
	{noreply, State2};

handle_cast({set_merkle_rebase_threshold, Threshold}, State) ->
	{noreply, State#state{ merkle_rebase_threshold = Threshold }};

handle_cast({set_height, Height}, State) ->
	{noreply, State#state{ allow_composite_packing = allow_composite_packing(Height),
			allow_replica_2_9_mining = allow_replica_2_9_mining(Height) }};

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

handle_cast({prepare_and_post_solution, CandidateOrSolution}, State) ->
	prepare_and_post_solution(CandidateOrSolution, State),
	{noreply, State};

handle_cast({manual_garbage_collect, Ref}, #state{ gc_process_ref = Ref } = State) ->
	%% Reading recall ranges from disk causes a large amount of binary data to be allocated and
	%% references to that data is spread among all the different mining processes. Because of
	%% this it can take the default garbage collection to clean up all references and
	%% deallocate the memory - which in turn can cause memory to be exhausted.
	%%
	%% To address this the mining server will force a garbage collection on all mining
	%% processes every time we process a few VDF steps. The exact number of VDF steps is
	%% determined by the chunk cache size limit in order to roughly align garbage collection
	%% with when we expect all references to a recall range's chunks to be evicted from
	%% the cache.
	?LOG_DEBUG([{event, mining_debug_garbage_collect_start},
		{frequency, State#state.gc_frequency_ms}]),
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

handle_info({event, nonce_limiter, {valid, _}}, State) ->
	%% Silently ignore validation messages
	{noreply, State};

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

terminate(Reason, _State) ->
	?LOG_INFO([{module, ?MODULE},{pid, self()},{callback, terminate},{reason, Reason}]),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================


allow_composite_packing(Height) ->
	Height - ?POST_2_8_COMPOSITE_PACKING_DELAY_BLOCKS >= ar_fork:height_2_8()
		andalso Height - ?COMPOSITE_PACKING_EXPIRATION_PERIOD_BLOCKS < ar_fork:height_2_9().

allow_replica_2_9_mining(Height) ->
	Height >= ar_fork:height_2_9().

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
			ar_mining_worker:set_sessions(Worker, sets:to_list(NewActiveSessions))
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
	Limits = calculate_cache_limits(NumActivePartitions, State#state.packing_difficulty),
	maybe_update_cache_limits(Limits, State).

calculate_cache_limits(NumActivePartitions, PackingDifficulty) ->
	IdealRangesPerStep = 2,
	RecallRangeSize = ar_block:get_recall_range_size(PackingDifficulty),

	MinimumCacheLimitBytes = max(
		?MINIMUM_CACHE_LIMIT_BYTES,
		(?IDEAL_STEPS_PER_PARTITION * IdealRangesPerStep * RecallRangeSize * NumActivePartitions)
	),

	OverallCacheLimitBytes = case arweave_config:get(mining_cache_size_mb) of
		undefined ->
			MinimumCacheLimitBytes;
		N ->
			N * ?MiB
	end,

	%% We shard the chunk cache across every active worker. Only workers that mine a partition
	%% included in the current weave are active.
	PartitionCacheLimitBytes = OverallCacheLimitBytes div NumActivePartitions,

	%% Allow enough compute_h0 tasks to be queued to completely refill the chunk cache.
	VDFQueueLimit = max(
		1,
		PartitionCacheLimitBytes div (2 * ar_block:get_recall_range_size(PackingDifficulty))
	),

	GarbageCollectionFrequency = 4 * VDFQueueLimit * 1000,

	{MinimumCacheLimitBytes, OverallCacheLimitBytes, PartitionCacheLimitBytes, VDFQueueLimit,
		GarbageCollectionFrequency}.

maybe_update_cache_limits({_, _, PartitionCacheLimit, _, _},
		#state{chunk_cache_limit = PartitionCacheLimit} = State) ->
	State;
maybe_update_cache_limits(Limits, State) ->
	{MinimumCacheLimitBytes, OverallCacheLimitBytes, PartitionCacheLimitBytes, VDFQueueLimit,
		GarbageCollectionFrequency} = Limits,
	maps:foreach(
		fun(_Partition, Worker) ->
			ar_mining_worker:set_cache_limits(
				Worker, PartitionCacheLimitBytes, VDFQueueLimit)
		end,
		State#state.workers
	),

	ar:console(
		"~nSetting the mining chunk cache size limit to ~B MiB "
		"(~B MiB per partition).~n",
			[OverallCacheLimitBytes div ?MiB, PartitionCacheLimitBytes div ?MiB]),
	?LOG_INFO([{event, update_mining_cache_limits},
		{overall_limit_mb, OverallCacheLimitBytes div ?MiB},
		{per_partition_limit_mb, PartitionCacheLimitBytes div ?MiB},
		{vdf_queue_limit_steps, VDFQueueLimit}]),
		case OverallCacheLimitBytes < MinimumCacheLimitBytes of
		true ->
			ar:console("~nChunk cache size limit (~p MiB) is below minimum limit of "
				"~p MiB. Mining performance may be impacted.~n"
				"Consider changing the 'mining_cache_size_mb' option.",
				[OverallCacheLimitBytes div ?MiB, MinimumCacheLimitBytes div ?MiB]);
		false -> ok
	end,

	State2 = reset_gc_timer(GarbageCollectionFrequency, State),
	State2#state{
		chunk_cache_limit = PartitionCacheLimitBytes
	}.

distribute_output(Candidate, State) ->
	distribute_output(ar_mining_io:get_partitions(), Candidate, State).

distribute_output([], _Candidate, _State) ->
	ok;
distribute_output([{_Partition, _MiningAddress, PackingDifficulty} | _Partitions],
		_Candidate, #state{ allow_composite_packing = false })
		when PackingDifficulty >= 1, PackingDifficulty /= ?REPLICA_2_9_PACKING_DIFFICULTY ->
	%% Only mine with composite packing until some time after the fork 2.9.
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
					packing_difficulty = PackingDifficulty,
					replica_format
						= ar_mining_io:get_replica_format_from_packing_difficulty(PackingDifficulty)
				})
	end,
	distribute_output(Partitions, Candidate, State).

get_recall_bytes(H0, PartitionNumber, Nonce, PartitionUpperBound, PackingDifficulty) ->
	{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
			PartitionNumber, PartitionUpperBound),
	RecallByte1 = ar_block:get_recall_byte(RecallRange1Start, Nonce, PackingDifficulty),
	RecallByte2 = ar_block:get_recall_byte(RecallRange2Start, Nonce, PackingDifficulty),
	{RecallByte1, RecallByte2}.

prepare_and_post_solution(#mining_candidate{} = Candidate, State) ->
	%% A solo miner builds a solution from a candidate here or a CM miner who received
	%% H2 from another peer reads chunk1 and sends it to the exit peer.
	Solution = prepare_solution_from_candidate(Candidate, State),
	post_solution(Solution, State);
prepare_and_post_solution(#mining_solution{} = Solution, State) ->
	%% An exit peer receives a mining solution, possibly without the VDF data and chunk proofs.
	Solution2 = prepare_solution(Solution, State),
	post_solution(Solution2, State).

prepare_solution(Solution, State) ->
	#state{ merkle_rebase_threshold = RebaseThreshold, is_pool_client = IsPoolClient } = State,
	#mining_solution{
		mining_address = MiningAddress, next_seed = NextSeed,
		next_vdf_difficulty = NextVDFDifficulty, nonce = Nonce,
		nonce_limiter_output = NonceLimiterOutput, partition_number = PartitionNumber,
		partition_upper_bound = PartitionUpperBound, poa1 = PoA1, poa2 = PoA2,
		preimage = Preimage, seed = Seed, start_interval_number = StartIntervalNumber,
		step_number = StepNumber, packing_difficulty = PackingDifficulty,
		replica_format = ReplicaFormat
	} = Solution,
	Candidate = #mining_candidate{
		mining_address = MiningAddress, next_seed = NextSeed,
		next_vdf_difficulty = NextVDFDifficulty, nonce = Nonce,
		nonce_limiter_output = NonceLimiterOutput, partition_number = PartitionNumber,
		partition_upper_bound = PartitionUpperBound, poa2 = PoA2,
		preimage = Preimage, seed = Seed, start_interval_number = StartIntervalNumber,
		step_number = StepNumber, packing_difficulty = PackingDifficulty,
		replica_format = ReplicaFormat
	},
	H0 = ar_block:compute_h0(NonceLimiterOutput, PartitionNumber,
			Seed, MiningAddress, PackingDifficulty),
	Chunk1 = PoA1#poa.chunk,
	{H1, Preimage1} = ar_block:compute_h1(H0, Nonce, Chunk1),
	Candidate2 = Candidate#mining_candidate{
		h0 = H0,
		h1 = H1,
		chunk1 = Chunk1
	},
	Candidate3 =
		case PoA2#poa.chunk of
			<<>> ->
				Preimage = Preimage1,
				Candidate2;
			Chunk2 ->
				{H2, Preimage} = ar_block:compute_h2(H1, Chunk2, H0),
				Candidate2#mining_candidate{ h2 = H2, chunk2 = Chunk2 }
		end,
	Solution2 = Solution#mining_solution{ merkle_rebase_threshold = RebaseThreshold },
	%% A pool client does not validate VDF before sharing a solution.
	case IsPoolClient of
		true ->
			prepare_solution(proofs, Candidate3, Solution2);
		false ->
			prepare_solution(last_step_checkpoints, Candidate3, Solution2)
	end.

prepare_solution_from_candidate(Candidate, State) ->
	#state{ merkle_rebase_threshold = RebaseThreshold, is_pool_client = IsPoolClient } = State,
	#mining_candidate{
		mining_address = MiningAddress, next_seed = NextSeed,
		next_vdf_difficulty = NextVDFDifficulty, nonce = Nonce,
		nonce_limiter_output = NonceLimiterOutput, partition_number = PartitionNumber,
		partition_upper_bound = PartitionUpperBound, poa2 = PoA2, preimage = Preimage,
		seed = Seed, start_interval_number = StartIntervalNumber, step_number = StepNumber,
		packing_difficulty = PackingDifficulty, replica_format = ReplicaFormat
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
		packing_difficulty = PackingDifficulty,
		replica_format = ReplicaFormat
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
	#nonce_limiter_info{ global_step_number = PrevStepNumber, seed = PrevSeed,
			next_seed = PrevNextSeed,
			next_vdf_difficulty = PrevNextVDFDifficulty } = TipNonceLimiterInfo,
	case StepNumber > PrevStepNumber of
		true ->
			Steps = ar_nonce_limiter:get_steps(
					PrevStepNumber, StepNumber, PrevNextSeed, PrevNextVDFDifficulty),
			case Steps of
				not_found ->
					CurrentSessionKey = ar_nonce_limiter:session_key(TipNonceLimiterInfo),
					SolutionSessionKey = Candidate#mining_candidate.session_key,
					LogData = [
						{current_session_key,
							ar_nonce_limiter:encode_session_key(CurrentSessionKey)},
						{solution_session_key,
							ar_nonce_limiter:encode_session_key(SolutionSessionKey)},
						{start_step_number, PrevStepNumber},
						{next_step_number, StepNumber},
						{seed, ar_util:safe_encode(PrevSeed)},
						{next_seed, ar_util:safe_encode(PrevNextSeed)},
						{next_vdf_difficulty, PrevNextVDFDifficulty},
						{h1, ar_util:safe_encode(Candidate#mining_candidate.h1)},
						{h2, ar_util:safe_encode(Candidate#mining_candidate.h2)}],
					?LOG_INFO([{event, found_solution_but_failed_to_find_checkpoints}
						| LogData]),
					may_be_leave_it_to_exit_peer(
							prepare_solution(proofs, Candidate,
									Solution#mining_solution{ steps = [] }),
							step_checkpoints_not_found, LogData);
				_ ->
					prepare_solution(proofs, Candidate,
							Solution#mining_solution{ steps = Steps })
			end;
		false ->
			log_prepare_solution_failure(Solution, stale, stale_step_number, miner, [
					{start_step_number, PrevStepNumber},
					{next_step_number, StepNumber},
					{next_seed, ar_util:safe_encode(PrevNextSeed)},
					{next_vdf_difficulty, PrevNextVDFDifficulty},
					{h1, ar_util:safe_encode(Candidate#mining_candidate.h1)},
					{h2, ar_util:safe_encode(Candidate#mining_candidate.h2)}
					]),
			error
	end;

prepare_solution(proofs, Candidate, Solution) ->
	#mining_candidate{
		h0 = H0, h1 = H1, h2 = H2, nonce = Nonce, partition_number = PartitionNumber,
		partition_upper_bound = PartitionUpperBound,
		packing_difficulty = PackingDifficulty,
		seed = Seed,
		mining_address = MiningAddress,
		nonce_limiter_output = NonceLimiterOutput,
		chunk2 = Chunk2
	} = Candidate,
	#mining_solution{ poa1 = PoA1, poa2 = PoA2 } = Solution,
	{RecallByte1, RecallByte2} = get_recall_bytes(H0, PartitionNumber, Nonce,
			PartitionUpperBound, PackingDifficulty),
	ExpectedH0 = ar_block:compute_h0(NonceLimiterOutput,
			PartitionNumber, Seed, MiningAddress,
			PackingDifficulty),
	case {H0, H1, H2} of
		{_, not_set, not_set} ->
			%% We should never end up here..
			log_prepare_solution_failure(Solution, rejected, h1_h2_not_set, miner, []),
			error;
		{ExpectedH0, _H1, not_set} ->
			prepare_solution(poa1, Candidate, Solution#mining_solution{
				solution_hash = H1, recall_byte1 = RecallByte1,
				poa1 = may_be_empty_poa(PoA1), poa2 = #poa{} });
		{_H0, _H1, not_set} ->
			log_prepare_solution_failure(Solution, rejected, incorrect_h0, miner, []),
			error;
		{ExpectedH0, _H1, _H2} ->
			case is_h2_valid(Chunk2, H0, H1, H2) of
				true ->
					prepare_solution(poa2, Candidate, Solution#mining_solution{
						solution_hash = H2, recall_byte1 = RecallByte1, recall_byte2 = RecallByte2,
						poa1 = may_be_empty_poa(PoA1), poa2 = may_be_empty_poa(PoA2) });
				false ->
					log_prepare_solution_failure(Solution, rejected, incorrect_h2, miner, []),
					error
			end;
		_ ->
			log_prepare_solution_failure(Solution, rejected, incorrect_h0, miner, []),
			error
	end;

prepare_solution(poa1, Candidate, Solution) ->
	#mining_solution{
		poa1 = CurrentPoA1, recall_byte1 = RecallByte1,
		mining_address = MiningAddress, packing_difficulty = PackingDifficulty,
		replica_format = ReplicaFormat
	} = Solution,
	#mining_candidate{ h0 = H0, h1 = H1, chunk1 = Chunk1, nonce = Nonce,
			partition_number = PartitionNumber } = Candidate,

	case prepare_poa(poa1, Candidate, CurrentPoA1) of
		{ok, PoA1} ->
			case is_h1_valid(Chunk1, PoA1, H0, H1, Nonce) of
				true ->
					Solution#mining_solution{ poa1 = PoA1 };
				false ->
					log_prepare_solution_failure(Solution, rejected, incorrect_h1, miner, []),
					error
			end;
		{error, Error} ->
			Modules = ar_storage_module:get_all(RecallByte1 + 1),
			ModuleIDs = [ar_storage_module:id(Module) || Module <- Modules],
			LogData = [{recall_byte, RecallByte1},
					{modules_covering_recall_byte, ModuleIDs},
					{fetch_proofs_error, io_lib:format("~p", [Error])},
					{nonce, Nonce},
					{partition_number, PartitionNumber}],
			case Chunk1 of
				not_set ->
					Packing = ar_block:get_packing(PackingDifficulty, MiningAddress,
							ReplicaFormat),
					?LOG_WARNING([{event, failed_to_find_poa1_proofs_for_h2_solution},
							{error, io_lib:format("~p", [Error])},
							{tags, [solution_proofs]} | LogData]),
					case ar_storage_module:get(RecallByte1 + 1, Packing) of
						{_BucketSize, _Bucket, Packing} = StorageModule ->
							StoreID = ar_storage_module:id(StorageModule),
							case ar_chunk_storage:get(RecallByte1, StoreID) of
								not_found ->
									log_prepare_solution_failure(Solution,
											rejected, chunk1_for_h2_solution_not_found, miner,
											LogData),
									error;
								{_EndOffset, Chunk} ->
									SubChunk = get_sub_chunk(Chunk, PackingDifficulty, Nonce),
									%% If we are a coordinated miner and not an exit node -
									%% the exit node will fetch the proofs.
									may_be_leave_it_to_exit_peer(
											Solution#mining_solution{
												poa1 = #poa{ chunk = SubChunk } },
											chunk1_proofs_for_h2_solution_not_found,
											LogData)
							end;
						_ ->
							log_prepare_solution_failure(Solution, rejected,
									storage_module_for_chunk1_for_h2_solution_not_found, miner,
									LogData),
							error
					end;
				_ ->
					%% If we are a coordinated miner and not an exit node - the exit
					%% node will fetch the proofs.
					may_be_leave_it_to_exit_peer(
						Solution#mining_solution{ poa1 = #poa{ chunk = Chunk1 } },
						chunk1_proofs_not_found,
						LogData)
			end
	end;

prepare_solution(poa2, Candidate, Solution) ->
	#mining_solution{ poa2 = CurrentPoA2, recall_byte2 = RecallByte2 } = Solution,
	#mining_candidate{ chunk2 = Chunk2 } = Candidate,

	case prepare_poa(poa2, Candidate, CurrentPoA2) of
		{ok, PoA2} ->
			prepare_solution(poa1, Candidate, Solution#mining_solution{ poa2 = PoA2 });
		{error, _Error} ->
			Modules = ar_storage_module:get_all(RecallByte2 + 1),
			ModuleIDs = [ar_storage_module:id(Module) || Module <- Modules],
			LogData = [{recall_byte2, RecallByte2}, {modules_covering_recall_byte, ModuleIDs}],
			%% If we are a coordinated miner and not an exit node - the exit
			%% node will fetch the proofs.
			may_be_leave_it_to_exit_peer(
				prepare_solution(poa1, Candidate,
						Solution#mining_solution{
								poa2 = #poa{ chunk = Chunk2 } }),
				chunk2_proofs_not_found, LogData)
	end.

prepare_poa(PoAType, Candidate, CurrentPoA) ->
	#mining_candidate{
		packing_difficulty = PackingDifficulty,
		replica_format = ReplicaFormat,
		mining_address = MiningAddress,
		h0 = H0, nonce = Nonce, partition_number = PartitionNumber,
		partition_upper_bound = PartitionUpperBound,
		chunk1 = Chunk1, chunk2 = Chunk2
	} = Candidate,
	{RecallByte1, RecallByte2} = get_recall_bytes(H0, PartitionNumber, Nonce,
		PartitionUpperBound, PackingDifficulty),

	{RecallByte, Chunk} = case PoAType of
		poa1 -> {RecallByte1, Chunk1};
		poa2 -> {RecallByte2, Chunk2}
	end,

	Packing = ar_block:get_packing(PackingDifficulty, MiningAddress, ReplicaFormat),
	case is_poa_complete(CurrentPoA, PackingDifficulty) of
		true ->
			{ok, CurrentPoA};
		false ->
			case read_poa(RecallByte, Chunk, Packing, Nonce) of
				{ok, PoA} ->
					{ok, PoA};
				{error, Error} ->
					Modules = ar_storage_module:get_all(RecallByte + 1),
					ModuleIDs = [ar_storage_module:id(Module) || Module <- Modules],
					?LOG_INFO([{event, failed_to_find_poa_proofs_locally},
							{poa, PoAType},
							{error, io_lib:format("~p", [Error])},
							{tags, [solution_proofs]},
							{recall_byte, RecallByte},
							{packing, ar_serialize:encode_packing(Packing, true)},
							{packing_difficulty, PackingDifficulty},
							{modules_covering_recall_byte, ModuleIDs}]),
					ChunkBinary = case Chunk of
						not_set ->
							<<>>;
						_ ->
							Chunk
					end,
					case fetch_poa_from_peers(RecallByte, PackingDifficulty) of
						not_found ->
							?LOG_INFO([{event, failed_to_fetch_proofs_from_peers},
									{tags, [solution_proofs]},
									{poa, PoAType},
									{recall_byte, RecallByte},
									{nonce, Nonce},
									{partition, PartitionNumber},
									{mining_address, ar_util:safe_encode(MiningAddress)},
									{packing, ar_serialize:encode_packing(Packing, true)},
									{packing_difficulty, PackingDifficulty}]),
							{error, Error};
						PoA ->
							{ok, PoA#poa{ chunk = ChunkBinary }}
					end
			end
	end.

%% Check if the chunk proof has been assembed already by the mining nodes.
%% If not, we are the exit node and we will fetch the missing data.
is_poa_complete(#poa{ chunk = <<>> }, _) ->
	false;
is_poa_complete(#poa{ data_path = <<>> }, _) ->
	false;
is_poa_complete(#poa{ tx_path = <<>> }, _) ->
	false;
is_poa_complete(#poa{ unpacked_chunk = <<>> }, PackingDifficulty)
		when PackingDifficulty >= 1 ->
	false;
is_poa_complete(_, _) ->
	true.

may_be_leave_it_to_exit_peer(error, _FailureReason, _AdditionalLogData) ->
	error;
may_be_leave_it_to_exit_peer(Solution, FailureReason, AdditionalLogData) ->
	case ar_coordination:is_coordinated_miner() andalso
			not ar_coordination:is_exit_peer() of
		true ->
			Solution;
		false ->
			log_prepare_solution_failure(
				Solution, rejected, FailureReason, miner, AdditionalLogData),
			error
	end.

is_h1_valid(Chunk, PoA, H0, H1, Nonce) ->
	Chunk1 = case Chunk of
		not_set ->
			PoA#poa.chunk;
		_ ->
			Chunk
	end,
	{ExpectedH1, _} = ar_block:compute_h1(H0, Nonce, Chunk1),
	H1 == ExpectedH1.

is_h2_valid(Chunk, H0, H1, H2) ->
	{ExpectedH2, _} =
		case Chunk of
			not_set ->
				{H2, not_set};
			_ ->
				ar_block:compute_h2(H1, Chunk, H0)
		end,
	H2 == ExpectedH2.

post_solution(error, _State) ->
	?LOG_WARNING([{event, found_solution_but_could_not_build_a_block}]),
	error;
post_solution(Solution, State) ->
	post_solution(arweave_config:get(cm_exit_peer), Solution, State).

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
					{nonce_limiter_output, ar_util:safe_encode(NonceLimiterOutput)},
					{diff_pair, DiffPair}]),
			ar:console("WARNING: we failed to validate our solution. Check logs for more "
					"details~n");
		{false, Reason} ->
			ar_events:send(solution, {rejected,
					#{ reason => Reason, source => miner }}),
			?LOG_WARNING([{event, found_invalid_solution},
					{reason, Reason},
					{partition, PartitionNumber},
					{step_number, StepNumber},
					{mining_address, ar_util:safe_encode(MiningAddress)},
					{recall_byte1, RecallByte1},
					{recall_byte2, RecallByte2},
					{solution_h, ar_util:safe_encode(H)},
					{nonce_limiter_output, ar_util:safe_encode(NonceLimiterOutput)},
					{diff_pair, DiffPair}]),
			ar:console("WARNING: the solution we found is invalid. Check logs for more "
					"details~n");
		{true, PoACache, PoA2Cache} ->
			ar_node_worker:found_solution(miner, Solution, PoACache, PoA2Cache)
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
	BucketPeers = ar_data_discovery:get_bucket_peers(RecallByte div ?NETWORK_DATA_BUCKET_SIZE),
	Peers = ar_data_discovery:pick_peers(BucketPeers, ?QUERY_BEST_PEERS_COUNT),
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
				{active_sessions, encode_sessions(sets:to_list(State#state.active_sessions))}]);
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
			prometheus_gauge:inc(mining_vdf_step),
			distribute_output(Candidate, State3),
			?LOG_DEBUG([{event, mining_debug_processing_vdf_output},
				{step_number, StepNumber}, {output, ar_util:safe_encode(Output)},
				{start_interval_number, StartIntervalNumber},
				{session_key, ar_nonce_limiter:encode_session_key(SessionKey)},
				{partition_upper_bound, PartitionUpperBound}])
	end,
	{noreply, State3}.

read_poa(RecallByte, ChunkOrSubChunk, Packing, Nonce) ->
	PoAReply = read_poa(RecallByte, Packing),
	case {ChunkOrSubChunk, PoAReply, Packing} of
		{not_set, {ok, #poa{ chunk = Chunk } = PoA}, {replica_2_9, _}} ->
			PackingDifficulty = ?REPLICA_2_9_PACKING_DIFFICULTY,
			SubChunk = get_sub_chunk(Chunk, PackingDifficulty, Nonce),
			{ok, PoA#poa{ chunk = SubChunk }};
		{not_set, {ok, #poa{ chunk = Chunk } = PoA}, {composite, _, PackingDifficulty}} ->
			SubChunk = get_sub_chunk(Chunk, PackingDifficulty, Nonce),
			{ok, PoA#poa{ chunk = SubChunk }};
		{_ChunkOrSubChunk, {ok, #poa{ chunk = Chunk } = PoA}, {replica_2_9, _}} ->
			case sub_chunk_belongs_to_chunk(ChunkOrSubChunk, Chunk) of
				true ->
					{ok, PoA#poa{ chunk = ChunkOrSubChunk }};
				false ->
					dump_invalid_solution_data({sub_chunk_mismatch, RecallByte,
							ChunkOrSubChunk, PoA, Packing, PoAReply, Nonce}),
					{error, sub_chunk_mismatch};
				Error2 ->
					Error2
			end;
		{_ChunkOrSubChunk, {ok, #poa{ chunk = Chunk } = PoA}, {composite, _, _}} ->
			case sub_chunk_belongs_to_chunk(ChunkOrSubChunk, Chunk) of
				true ->
					{ok, PoA#poa{ chunk = ChunkOrSubChunk }};
				false ->
					dump_invalid_solution_data({sub_chunk_mismatch, RecallByte,
							ChunkOrSubChunk, PoA, Packing, PoAReply, Nonce}),
					{error, sub_chunk_mismatch};
				Error2 ->
					Error2
			end;
		{not_set, {ok, #poa{} = PoA}, _Packing} ->
			{ok, PoA};
		{_ChunkOrSubChunk, {ok, #poa{ chunk = ChunkOrSubChunk } = PoA}, _Packing} ->
			{ok, PoA};
		{_ChunkOrSubChunk, {ok, #poa{} = PoA}, _Packing} ->
			dump_invalid_solution_data({chunk_mismatch, RecallByte,
					ChunkOrSubChunk, PoA, Packing, PoAReply, Nonce}),
			{error, chunk_mismatch};
		{_ChunkOrSubChunk, Error, _Packing} ->
			Error
	end.

dump_invalid_solution_data(Data) ->
	DataDir = arweave_config:get(data_dir),
	ID = binary_to_list(ar_util:encode(crypto:strong_rand_bytes(16))),
	File = filename:join(DataDir, "invalid_solution_data_dump_" ++ ID),
	file:write_file(File, term_to_binary(Data)).

get_sub_chunk(Chunk, 0, _Nonce) ->
	Chunk;
get_sub_chunk(Chunk, PackingDifficulty, Nonce) ->
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	SubChunkIndex = ar_block:get_sub_chunk_index(PackingDifficulty, Nonce),
	SubChunkStartOffset = SubChunkSize * SubChunkIndex,
	binary:part(Chunk, SubChunkStartOffset, SubChunkSize).

sub_chunk_belongs_to_chunk(SubChunk,
		<< SubChunk:?COMPOSITE_PACKING_SUB_CHUNK_SIZE/binary, _Rest/binary >>) ->
	true;
sub_chunk_belongs_to_chunk(SubChunk,
		<< _SubChunk:?COMPOSITE_PACKING_SUB_CHUNK_SIZE/binary, Rest/binary >>) ->
	sub_chunk_belongs_to_chunk(SubChunk, Rest);
sub_chunk_belongs_to_chunk(_SubChunk, <<>>) ->
	false;
sub_chunk_belongs_to_chunk(_SubChunk, _Chunk) ->
	{error, uneven_chunk}.

read_poa(RecallByte, Packing) ->
	Options = #{ pack => true, packing => Packing, origin => miner },
	case ar_data_sync:get_chunk(RecallByte + 1, Options) of
		{ok, Proof} ->
			#{ chunk := Chunk, tx_path := TXPath, data_path := DataPath } = Proof,
			case get_packing_type(Packing) of
				Type when Type == replica_2_9; Type == composite ->
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
	Options = #{ pack => true, packing => unpacked, origin => miner },
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
		packing_difficulty = PackingDifficulty, replica_format = ReplicaFormat } = Solution,
	H0 = ar_block:compute_h0(NonceLimiterOutput, PartitionNumber, Seed, MiningAddress,
			PackingDifficulty),
	{H1, _Preimage1} = ar_block:compute_h1(H0, Nonce, PoA1#poa.chunk),
	{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
			PartitionNumber, PartitionUpperBound),
	%% Assert recall_byte1 is computed correctly.
	RecallByte1 = ar_block:get_recall_byte(RecallRange1Start, Nonce, PackingDifficulty),
	{BlockStart1, BlockEnd1, TXRoot1} = ar_block_index:get_block_bounds(RecallByte1),
	BlockSize1 = BlockEnd1 - BlockStart1,
	Packing = ar_block:get_packing(PackingDifficulty, MiningAddress, ReplicaFormat),
	SubChunkIndex = ar_block:get_sub_chunk_index(PackingDifficulty, Nonce),
	case ar_poa:validate({BlockStart1, RecallByte1, TXRoot1, BlockSize1, PoA1,
			Packing, SubChunkIndex, not_set}) of
		{true, ChunkID} ->
			PoACache = {{BlockStart1, RecallByte1, TXRoot1, BlockSize1, Packing,
					SubChunkIndex}, ChunkID},
			case ar_node_utils:h1_passes_diff_check(H1, DiffPair, PackingDifficulty) of
				true ->
					%% validates solution_hash
					case SolutionHash of
						H1 -> ok;
						_ ->
							?LOG_ERROR([{event, invalid_solution_hash}, {solution_hash, SolutionHash}, {h1, H1}])
					end,
					SolutionHash = H1,
					{true, PoACache, undefined};
				false ->
					case is_one_chunk_solution(Solution) of
						true ->
							%% This can happen if the difficulty has increased between the
							%% time the H1 solution was found and now. In this case,
							%% there is no H2 solution, so we flag the solution invalid.
							{Diff1, _} = DiffPair,
							{false, {h1_diff_check,
								ar_util:safe_encode(H0),
								ar_util:safe_encode(H1),
								binary:decode_unsigned(H1),
								ar_node_utils:scaled_diff(Diff1, PackingDifficulty)
							}};
						false ->
							#mining_solution{
								recall_byte2 = RecallByte2, poa2 = PoA2 } = Solution,
							{H2, _Preimage2} = ar_block:compute_h2(H1, PoA2#poa.chunk, H0),
							case ar_node_utils:h2_passes_diff_check(H2, DiffPair,
									PackingDifficulty) of
								false ->
									{_, Diff2} = DiffPair,
									{false, {h2_diff_check,
										ar_util:safe_encode(H0),
										ar_util:safe_encode(H1),
										ar_util:safe_encode(H2),
										binary:decode_unsigned(H2),
										ar_node_utils:scaled_diff(Diff2, PackingDifficulty)
									}};
								true ->
									SolutionHash = H2,
									RecallByte2 = ar_block:get_recall_byte(RecallRange2Start,
											Nonce, PackingDifficulty),
									{BlockStart2, BlockEnd2, TXRoot2} =
											ar_block_index:get_block_bounds(RecallByte2),
									BlockSize2 = BlockEnd2 - BlockStart2,
									case ar_poa:validate({BlockStart2, RecallByte2, TXRoot2,
											BlockSize2, PoA2,
											Packing, SubChunkIndex, not_set}) of
										{true, Chunk2ID} ->
											PoA2Cache = {{BlockStart2, RecallByte2, TXRoot2,
													BlockSize2, Packing, SubChunkIndex},
													Chunk2ID},
											{true, PoACache, PoA2Cache};
										error ->
											log_prepare_solution_failure(Solution,
												rejected, poa2_validation_error, miner, []),
											error;
										false ->
											{false, poa2}
									end
							end
					end
			end;
		error ->
			log_prepare_solution_failure(Solution, rejected, poa1_validation_error, miner, []),
			error;
		false ->
			{false, poa1}
	end.

reset_gc_timer(GarbageCollectionFrequency, State) ->
	State2 = maybe_cancel_gc_timer(State),
	Ref = erlang:make_ref(),
	ar_util:cast_after(GarbageCollectionFrequency, ?MODULE,
			{manual_garbage_collect, Ref}),
	State2#state{ gc_process_ref = Ref, gc_frequency_ms = GarbageCollectionFrequency }.

maybe_cancel_gc_timer(#state{gc_process_ref = undefined} = State) ->
	State;
maybe_cancel_gc_timer(State) ->
	erlang:cancel_timer(State#state.gc_process_ref),
	State#state{ gc_process_ref = undefined }.

%%%===================================================================
%%% Public Test interface.
%%%===================================================================

%% @doc Pause the mining server. Only used in tests.
pause() ->
	gen_server:cast(?MODULE, pause).

setup() ->
	{ok, Config} = application:get_env(arweave, config),
	Config.

cleanup(Config) ->
	application:set_env(arweave, config, Config).

calculate_cache_limits_test_() ->
	{setup, fun setup/0, fun cleanup/1,
		[
			{timeout, 30, fun test_calculate_cache_limits_default/0},
			{timeout, 30, fun test_calculate_cache_limits_custom_low/0},
			{timeout, 30, fun test_calculate_cache_limits_custom_high/0}
		]
	}.

test_calculate_cache_limits_default() ->
	arweave_config:set(mining_cache_size_mb, undefined),
	?assertEqual(
		{
			?IDEAL_STEPS_PER_PARTITION * 100 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 100 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * ?MiB,
			?IDEAL_STEPS_PER_PARTITION,
			?IDEAL_STEPS_PER_PARTITION * 4000},
		calculate_cache_limits(100, 0)
	),
	?assertEqual(
		{
			?IDEAL_STEPS_PER_PARTITION * 200 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 200 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * ?MiB,
			?IDEAL_STEPS_PER_PARTITION,
			?IDEAL_STEPS_PER_PARTITION * 4000},
		calculate_cache_limits(200, 0)
	),
	?assertEqual(
		{
			?IDEAL_STEPS_PER_PARTITION * 1000 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 1000 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * ?MiB,
			?IDEAL_STEPS_PER_PARTITION,
			?IDEAL_STEPS_PER_PARTITION * 4000},
		calculate_cache_limits(1000, 0)
	),
	?assertEqual(
		{
			?IDEAL_STEPS_PER_PARTITION * 25 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 25 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 256 * ?KiB,
			?IDEAL_STEPS_PER_PARTITION,
			?IDEAL_STEPS_PER_PARTITION * 4000},
		calculate_cache_limits(100, 1)
	),
	?assertEqual(
		{
			?IDEAL_STEPS_PER_PARTITION * 50 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 50 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 256 * ?KiB,
			?IDEAL_STEPS_PER_PARTITION,
			?IDEAL_STEPS_PER_PARTITION * 4000},
		calculate_cache_limits(200, 1)
	),
	?assertEqual(
		{
			?IDEAL_STEPS_PER_PARTITION * 250 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 250 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 256 * ?KiB,
			?IDEAL_STEPS_PER_PARTITION,
			?IDEAL_STEPS_PER_PARTITION * 4000},
		calculate_cache_limits(1000, 1)
	),
	?assertEqual(
		{
			?IDEAL_STEPS_PER_PARTITION * 25 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 25 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 128 * ?KiB,
			?IDEAL_STEPS_PER_PARTITION,
			?IDEAL_STEPS_PER_PARTITION * 4000},
		calculate_cache_limits(200, 2)
	),
	?assertEqual(
		{
			?IDEAL_STEPS_PER_PARTITION * 50 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 50 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 128 * ?KiB,
			?IDEAL_STEPS_PER_PARTITION,
			?IDEAL_STEPS_PER_PARTITION * 4000},
		calculate_cache_limits(400, 2)
	),
	?assertEqual(
		{
			?IDEAL_STEPS_PER_PARTITION * 125 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 125 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 128 * ?KiB,
			?IDEAL_STEPS_PER_PARTITION,
			?IDEAL_STEPS_PER_PARTITION * 4000},
		calculate_cache_limits(1000, 2)
	),
	?assertEqual(
		{
			?IDEAL_STEPS_PER_PARTITION * 50 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 50 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 8 * ?KiB,
			?IDEAL_STEPS_PER_PARTITION,
			?IDEAL_STEPS_PER_PARTITION * 4000},
		calculate_cache_limits(6_400, 32)
	),
	?assertEqual(
		{
			?IDEAL_STEPS_PER_PARTITION * 100 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 100 * ?MiB,
			?IDEAL_STEPS_PER_PARTITION * 8 * ?KiB,
			?IDEAL_STEPS_PER_PARTITION,
			?IDEAL_STEPS_PER_PARTITION * 4000},
		calculate_cache_limits(12_800, 32)
	),
	?assertEqual(
		{
			trunc(?IDEAL_STEPS_PER_PARTITION * 156.25 * ?MiB),
			trunc(?IDEAL_STEPS_PER_PARTITION * 156.25 * ?MiB),
			?IDEAL_STEPS_PER_PARTITION * 8 * ?KiB,
			?IDEAL_STEPS_PER_PARTITION,
			?IDEAL_STEPS_PER_PARTITION * 4000},
		calculate_cache_limits(20_000, 32)
	).

test_calculate_cache_limits_custom_low() ->
	arweave_config:set(mining_cache_size_mb, 1),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 1 * ?MiB, 1 * ?MiB, 1, 4_000},
		calculate_cache_limits(1, 0)
	),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 1 * ?MiB, 512 * ?KiB, 1, 4_000},
		calculate_cache_limits(2, 0)
	),
	?assertEqual(
		{?IDEAL_STEPS_PER_PARTITION * 1000 * ?MiB, 1 * ?MiB, (1 * ?MiB) div 1_000, 1, 4_000},
		calculate_cache_limits(1000, 0)
	),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 1 * ?MiB, 1 * ?MiB, 4, 16_000},
		calculate_cache_limits(1, 1)
	),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 1 * ?MiB, 512 * ?KiB, 2, 8_000},
		calculate_cache_limits(2, 1)
	),
	?assertEqual(
		{?IDEAL_STEPS_PER_PARTITION * 250 * ?MiB, 1 * ?MiB, (1 * ?MiB) div 1_000, 1, 4_000},
		calculate_cache_limits(1000, 1)
	),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 1 * ?MiB, 1 * ?MiB, 8, 32_000},
		calculate_cache_limits(1, 2)
	),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 1 * ?MiB, 512 * ?KiB, 4, 16_000},
		calculate_cache_limits(2, 2)
	),
	?assertEqual(
		{?IDEAL_STEPS_PER_PARTITION * 128_000 * ?KiB, 1 * ?MiB, (1 * ?MiB) div 1_000, 1, 4_000},
		calculate_cache_limits(1000, 2)
	),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 1 * ?MiB, 1 * ?MiB, 128, 512_000},
		calculate_cache_limits(1, 32)
	),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 1 * ?MiB, 512 * ?KiB, 64, 256_000},
		calculate_cache_limits(2, 32)
	),
	?assertEqual(
		{?IDEAL_STEPS_PER_PARTITION * 500 * ?MiB, 1 * ?MiB, (1 * ?MiB) div 64_000, 1, 4_000},
		calculate_cache_limits(64_000, 32)
	).

test_calculate_cache_limits_custom_high() ->
	arweave_config:set(mining_cache_size_mb, 500_000),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 512_000_000 * ?KiB, 512_000_000 * ?KiB, 500_000, 2_000_000_000},
		calculate_cache_limits(1, 0)
	),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 512_000_000 * ?KiB, 256_000_000 * ?KiB, 250_000, 1_000_000_000},
		calculate_cache_limits(2, 0)
	),
	?assertEqual(
		{?IDEAL_STEPS_PER_PARTITION * 1000 * ?MiB, 512_000_000 * ?KiB, 512_000 * ?KiB, 500, 2_000_000},
		calculate_cache_limits(1000, 0)
	),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 512_000_000 * ?KiB, 512_000_000 * ?KiB, 2_000_000, 8_000_000_000},
		calculate_cache_limits(1, 1)
	),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 512_000_000 * ?KiB, 256_000_000 * ?KiB, 1_000_000, 4_000_000_000},
		calculate_cache_limits(2, 1)
	),
	?assertEqual(
		{?IDEAL_STEPS_PER_PARTITION * 250 * ?MiB, 512_000_000 * ?KiB, 512_000 * ?KiB, 2_000, 8_000_000},
		calculate_cache_limits(1000, 1)
	),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 512_000_000 * ?KiB, 512_000_000 * ?KiB, 4_000_000, 16_000_000_000},
		calculate_cache_limits(1, 2)
	),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 512_000_000 * ?KiB, 256_000_000 * ?KiB, 2_000_000, 8_000_000_000},
		calculate_cache_limits(2, 2)
	),
	?assertEqual(
		{?IDEAL_STEPS_PER_PARTITION * 128_000 * ?KiB, 512_000_000 * ?KiB, 512_000 * ?KiB, 4_000, 16_000_000},
		calculate_cache_limits(1000, 2)
	),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 512_000_000 * ?KiB, 512_000_000 * ?KiB, 64_000_000, 256_000_000_000},
		calculate_cache_limits(1, 32)
	),
	?assertEqual(
		{?MINIMUM_CACHE_LIMIT_BYTES, 512_000_000 * ?KiB, 256_000_000 * ?KiB, 32_000_000, 128_000_000_000},
		calculate_cache_limits(2, 32)
	),
	?assertEqual(
		{(?IDEAL_STEPS_PER_PARTITION * 2 * (?RECALL_RANGE_SIZE div 32) * 1000), 512_000_000 * ?KiB, 512_000 * ?KiB, 64_000, 256_000_000},
		calculate_cache_limits(1000, 32)
	).
