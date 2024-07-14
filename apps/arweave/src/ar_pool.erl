%%% @doc The module defines the core pool mining functionality.
%%%
%%% The key actors are a pool client, a pool proxy, and a pool server. The pool client may be
%%% a standalone mining node or an exit peer in a coordinated mining setup. The other CM peers
%%% communicate with the pool via the exit peer. The proxy is NOT an Arweave node.
%%%
%%% Communication Scheme
%%%
%%%                                 +---> Standalone Pool Client
%%%                                 |
%%% Pool Server <--> Pool Proxy <---+
%%%                                 |
%%%                                 +---> CM Exit Node Pool Client <--> CM Miner Pool Client
%%%
%%% Job Assignment
%%%
%%% 1. Solo Mining
%%%
%%%   Pool Server -> Pool Proxy -> Standalone Pool Client
%%%
%%% 2. Coordinated Mining
%%%
%%%   Pool Server -> Pool Proxy -> CM Exit Node Pool Client -> CM Miner Pool Client
%%%
%%% Partial Solution Lifecycle
%%%
%%% 1. Solo Mining
%%%
%%%   Standalone Pool Client -> Pool Proxy -> Pool Sever
%%%
%%% 2. Coordinated Mining
%%%
%%%   CM Miner Pool Client -> CM Exit Node Pool Client -> Pool Proxy -> Pool Server
-module(ar_pool).

-behaviour(gen_server).

-export([start_link/0, is_client/0, get_current_session_key_seed_pairs/0, get_jobs/1,
		get_latest_job/0, cache_jobs/1, process_partial_solution/1,
		post_partial_solution/1, pool_peer/0, process_cm_jobs/2]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("arweave/include/ar_pool.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	%% The most recent keys come first.
	session_keys = [],
	%% Key => [{Output, StepNumber, PartitionUpperBound, Seed, Diff}, ...]
	jobs_by_session_key = maps:new(),
	request_pid_by_ref = maps:new()
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Return true if we are a pool client.
is_client() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.is_pool_client == true.

%% @doc Return a list of up to two most recently cached VDF session key, seed pairs.
get_current_session_key_seed_pairs() ->
	gen_server:call(?MODULE, get_current_session_key_seed_pairs, infinity).

%% @doc Return a set of the most recent cached jobs.
get_jobs(PrevOutput) ->
	gen_server:call(?MODULE, {get_jobs, PrevOutput}, infinity).

%% @doc Return the most recent cached #job{}. Return an empty record if the
%% cache is empty.
get_latest_job() ->
	gen_server:call(?MODULE, get_latest_job, infinity).

%% @doc Cache the given jobs.
cache_jobs(Jobs) ->
	gen_server:cast(?MODULE, {cache_jobs, Jobs}).

%% @doc Validate the given (partial) solution. If the solution is eligible for
%% producing a block, produce and publish a block.
process_partial_solution(Solution) ->
	gen_server:call(?MODULE, {process_partial_solution, Solution}, infinity).

%% @doc Send the partial solution to the pool.
post_partial_solution(Solution) ->
	gen_server:cast(?MODULE, {post_partial_solution, Solution}).

%% @doc Return the pool server as a "peer" recognized by ar_http_iface_client.
pool_peer() ->
	{ok, Config} = application:get_env(arweave, config),
	{pool, Config#config.pool_server_address}.

%% @doc Process the set of coordinated mining jobs received from the pool.
process_cm_jobs(Jobs, Peer) ->
	#pool_cm_jobs{ h1_to_h2_jobs = H1ToH2Jobs, h1_read_jobs = H1ReadJobs } = Jobs,
	{ok, Config} = application:get_env(arweave, config),
	Partitions = ar_mining_io:get_partitions(infinity),
	case Config#config.mine of
		true ->
			process_h1_to_h2_jobs(H1ToH2Jobs, Peer, Partitions);
		_ ->
			ok
	end,
	process_h1_read_jobs(H1ReadJobs, Partitions).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	ok = ar_events:subscribe(solution),
	{ok, #state{}}.

handle_call(get_current_session_key_seed_pairs, _From, State) ->
	JobsBySessionKey = State#state.jobs_by_session_key,
	Keys = lists:sublist(State#state.session_keys, 2),
	KeySeedPairs = [{Key, element(4, hd(maps:get(Key, JobsBySessionKey)))} || Key <- Keys],
	{reply, KeySeedPairs, State};

handle_call({get_jobs, PrevOutput}, _From, State) ->
	SessionKeys = State#state.session_keys,
	JobCache = State#state.jobs_by_session_key,
	{reply, get_jobs(PrevOutput, SessionKeys, JobCache), State};

handle_call(get_latest_job, _From, State) ->
	case State#state.session_keys of
		[] ->
			{reply, #job{}, State};
		[Key | _] ->
			{O, SN, U, _S, _Diff} = hd(maps:get(Key, State#state.jobs_by_session_key)),
			{reply, #job{ output = O, global_step_number = SN,
					partition_upper_bound = U }, State}
	end;

handle_call({process_partial_solution, Solution}, From, State) ->
	#state{ request_pid_by_ref = Map } = State,
	Ref = make_ref(),
	case process_partial_solution(Solution, Ref) of
		noreply ->
			{noreply, State#state{ request_pid_by_ref = maps:put(Ref, From, Map) }};
		Reply ->
			{reply, Reply, State}
	end;

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({cache_jobs, #jobs{ jobs = [] }}, State) ->
	{noreply, State};
handle_cast({cache_jobs, Jobs}, State) ->
	#jobs{ jobs = JobList, partial_diff = PartialDiff,
			next_seed = NextSeed, seed = Seed,
			interval_number = IntervalNumber,
			next_vdf_difficulty = NextVDFDifficulty } = Jobs,
	SessionKey = {NextSeed, IntervalNumber, NextVDFDifficulty},
	SessionKeys = State#state.session_keys,
	SessionKeys2 =
		case lists:member(SessionKey, SessionKeys) of
			true ->
				SessionKeys;
			false ->
				[SessionKey | SessionKeys]
		end,
	JobList2 = [{Job#job.output, Job#job.global_step_number,
			Job#job.partition_upper_bound, Seed, PartialDiff} || Job <- JobList],
	PrevJobList = maps:get(SessionKey, State#state.jobs_by_session_key, []),
	JobList3 = JobList2 ++ PrevJobList,
	JobsBySessionKey = maps:put(SessionKey, JobList3, State#state.jobs_by_session_key),
	{SessionKeys3, JobsBySessionKey2} =
		case length(SessionKeys2) == 3 of
			true ->
				[SK1, SK2, RemoveKey] = SessionKeys2,
				{[SK1, SK2], maps:remove(RemoveKey, JobsBySessionKey)};
			false ->
				{SessionKeys2, JobsBySessionKey}
		end,
	{noreply, State#state{ session_keys = SessionKeys3,
			jobs_by_session_key = JobsBySessionKey2 }};

handle_cast({post_partial_solution, Solution}, State) ->
	case ar_http_iface_client:post_partial_solution(pool_peer(), Solution) of
		{ok, _Response} ->
			ok;
		{error, Error} ->
			?LOG_WARNING([{event, failed_to_submit_partial_solution},
					{reason, io_lib:format("~p", [Error])}])
	end,
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({event, solution,
		{rejected, #{ reason := mining_address_banned, source := {pool, Ref} }}}, State) ->
	#state{ request_pid_by_ref = Map } = State,
	PID = maps:get(Ref, Map),
	gen_server:reply(PID,
			#partial_solution_response{ status = <<"rejected_mining_address_banned">> }),
	{noreply, State#state{ request_pid_by_ref = maps:remove(Ref, Map) }};

handle_info({event, solution,
		{rejected, #{ reason := missing_key_file, source := {pool, Ref} }}}, State) ->
	#state{ request_pid_by_ref = Map } = State,
	PID = maps:get(Ref, Map),
	gen_server:reply(PID,
			#partial_solution_response{ status = <<"rejected_missing_key_file">> }),
	{noreply, State#state{ request_pid_by_ref = maps:remove(Ref, Map) }};

handle_info({event, solution,
		{rejected, #{ reason := vdf_not_found, source := {pool, Ref} }}}, State) ->
	#state{ request_pid_by_ref = Map } = State,
	PID = maps:get(Ref, Map),
	gen_server:reply(PID, #partial_solution_response{ status = <<"rejected_vdf_not_found">> }),
	{noreply, State#state{ request_pid_by_ref = maps:remove(Ref, Map) }};

handle_info({event, solution,
		{rejected, #{ reason := bad_vdf, source := {pool, Ref} }}}, State) ->
	#state{ request_pid_by_ref = Map } = State,
	PID = maps:get(Ref, Map),
	gen_server:reply(PID, #partial_solution_response{ status = <<"rejected_bad_vdf">> }),
	{noreply, State#state{ request_pid_by_ref = maps:remove(Ref, Map) }};

handle_info({event, solution, {partial, #{ source := {pool, Ref} }}}, State) ->
	#state{ request_pid_by_ref = Map } = State,
	PID = maps:get(Ref, Map),
	gen_server:reply(PID, #partial_solution_response{ status = <<"accepted">> }),
	{noreply, State#state{ request_pid_by_ref = maps:remove(Ref, Map) }};

handle_info({event, solution, {stale, #{ source := {pool, Ref} }}}, State) ->
	#state{ request_pid_by_ref = Map } = State,
	PID = maps:get(Ref, Map),
	gen_server:reply(PID, #partial_solution_response{ status = <<"stale">> }),
	{noreply, State#state{ request_pid_by_ref = maps:remove(Ref, Map) }};

handle_info({event, solution,
		{accepted, #{ indep_hash := H, source := {pool, Ref} }}}, State) ->
	#state{ request_pid_by_ref = Map } = State,
	PID = maps:get(Ref, Map),
	gen_server:reply(PID,
			#partial_solution_response{ indep_hash = H, status = <<"accepted_block">> }),
	{noreply, State#state{ request_pid_by_ref = maps:remove(Ref, Map) }};

handle_info({event, solution, _Event}, State) ->
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_jobs(PrevOutput, SessionKeys, JobCache) ->
	case SessionKeys of
		[] ->
			#jobs{};
		[{NextSeed, Interval, NextVDFDifficulty} = SessionKey | _] ->
			Jobs = maps:get(SessionKey, JobCache),
			{Seed, PartialDiff, Jobs2} = collect_jobs(Jobs, PrevOutput, ?GET_JOBS_COUNT),
			Jobs3 = [#job{ output = O, global_step_number = SN,
					partition_upper_bound = U } || {O, SN, U} <- Jobs2],
			#jobs{ jobs = Jobs3, seed = Seed, partial_diff = PartialDiff,
					next_seed = NextSeed,
					interval_number = Interval, next_vdf_difficulty = NextVDFDifficulty }
	end.

collect_jobs([], _PrevO, _N) ->
	{<<>>, {0, 0}, []};
collect_jobs(_Jobs, _PrevO, 0) ->
	{<<>>, {0, 0}, []};
collect_jobs([{O, _SN, _U, _S, _PartialDiff} | _Jobs], O, _N) ->
	{<<>>, {0, 0}, []};
collect_jobs([{O, SN, U, S, PartialDiff} | Jobs], PrevO, N) ->
	{S, PartialDiff, [{O, SN, U} | collect_jobs(Jobs, PrevO, N - 1, PartialDiff)]}.

collect_jobs([], _PrevO, _N, _PartialDiff) ->
	[];
collect_jobs(_Jobs, _PrevO, 0, _PartialDiff) ->
	[];
collect_jobs([{O, _SN, _U, _S, _PartialDiff} | _Jobs], O, _N, _PartialDiff2) ->
	[];
collect_jobs([{O, SN, U, _S, PartialDiff} | Jobs], PrevO, N, PartialDiff) ->
	[{O, SN, U} | collect_jobs(Jobs, PrevO, N - 1, PartialDiff)];
collect_jobs(_Jobs, _PrevO, _N, _PartialDiff) ->
	%% PartialDiff mismatch.
	[].

process_partial_solution(Solution, Ref) ->
	PoA1 = Solution#mining_solution.poa1,
	PoA2 = Solution#mining_solution.poa2,
	case ar_block:validate_proof_size(PoA1) andalso ar_block:validate_proof_size(PoA2) of
		true ->
			process_partial_solution_field_size(Solution, Ref);
		false ->
			#partial_solution_response{ status = <<"rejected_bad_poa">> }
	end.

process_partial_solution_field_size(Solution, Ref) ->
	#mining_solution{
		nonce_limiter_output = Output,
		seed = Seed,
		next_seed = NextSeed,
		mining_address = MiningAddress,
		preimage = Preimage,
		solution_hash = SolutionH,
		poa1 = #poa{ chunk = C1 },
		poa2 = #poa{ chunk = C2 }
	} = Solution,
	%% We have less strict deserialization in the pool pipeline to simplify
	%% the pool "proxy" implementation. Therefore, we validate the field sizes here
	%% and return the "rejected_bad_poa" status in case of a failure.
	case {byte_size(Output), byte_size(Seed), byte_size(NextSeed), byte_size(MiningAddress),
			byte_size(Preimage), byte_size(SolutionH), byte_size(C1), byte_size(C2)} of
		{32, 48, 48, 32, 32, 32, L1, L2} when L1 =< ?DATA_CHUNK_SIZE, L2 =< ?DATA_CHUNK_SIZE ->
			%% The second chunk may be either empty or 256 KiB. ar_poa:validate/1 does
			%% the proper verification - here we simply protect against payload size abuse.
			%% We are not strict about the first chunk here to simplify tests.
			process_partial_solution_poa2_size(Solution, Ref);
		_ ->
			#partial_solution_response{ status = <<"rejected_bad_poa">> }
	end.

process_partial_solution_poa2_size(Solution, Ref) ->
	#mining_solution{
		poa2 = #poa{ chunk = C, data_path = DP, tx_path = TP }
	} = Solution,
	case ar_mining_server:is_one_chunk_solution(Solution) of
		true ->
			case {C, DP, TP} of
				{<<>>, <<>>, <<>>} ->
					process_partial_solution_partition_number(Solution, Ref);
				_ ->
					#partial_solution_response{ status = <<"rejected_bad_poa">> }
			end;
		false ->
			process_partial_solution_partition_number(Solution, Ref)
	end.

process_partial_solution_partition_number(Solution, Ref) ->
	PartitionNumber = Solution#mining_solution.partition_number,
	PartitionUpperBound = Solution#mining_solution.partition_upper_bound,
	Max = ar_node:get_max_partition_number(PartitionUpperBound),
	case PartitionNumber > Max of
		false ->
			process_partial_solution_nonce(Solution, Ref);
		true ->
			#partial_solution_response{ status = <<"rejected_bad_poa">> }
	end.

process_partial_solution_nonce(Solution, Ref) ->
	Max = max(0, (?RECALL_RANGE_SIZE) div ?DATA_CHUNK_SIZE - 1),
	case Solution#mining_solution.nonce > Max of
		false ->
			process_partial_solution_quick_pow(Solution, Ref);
		true ->
			#partial_solution_response{ status = <<"rejected_bad_poa">> }
	end.

process_partial_solution_quick_pow(Solution, Ref) ->
	#mining_solution{
		nonce_limiter_output = NonceLimiterOutput,
		partition_number = PartitionNumber,
		seed = Seed,
		mining_address = MiningAddress,
		preimage = Preimage,
		solution_hash = SolutionH
	} = Solution,
	H0 = ar_block:compute_h0(NonceLimiterOutput, PartitionNumber, Seed, MiningAddress),
	case ar_block:compute_solution_h(H0, Preimage) of
		SolutionH ->
			process_partial_solution_pow(Solution, Ref, H0);
		_ ->
			%% Solution hash mismatch (pattern matching against solution_hash = SolutionH).
			#partial_solution_response{ status = <<"rejected_wrong_hash">> }
	end.

process_partial_solution_pow(Solution, Ref, H0) ->
	#mining_solution{
		nonce = Nonce,
		poa1 = #poa{ chunk = Chunk1 },
		solution_hash = SolutionH,
		preimage = Preimage,
		poa2 = #poa{ chunk = Chunk2 }
	} = Solution,
	{H1, Preimage1} = ar_block:compute_h1(H0, Nonce, Chunk1),
	case H1 == SolutionH andalso Preimage1 == Preimage
			andalso ar_mining_server:is_one_chunk_solution(Solution) of
		true ->
			process_partial_solution_partition_upper_bound(Solution, Ref, H0, H1);
		false ->
			{H2, Preimage2} = ar_block:compute_h2(H1, Chunk2, H0),
			case H2 == SolutionH andalso Preimage2 == Preimage of
				false ->
					#partial_solution_response{ status = <<"rejected_wrong_hash">> };
				true ->
					process_partial_solution_partition_upper_bound(Solution, Ref, H0, H1)
			end
	end.

process_partial_solution_partition_upper_bound(Solution, Ref, H0, H1) ->
	#mining_solution{ partition_upper_bound = PartitionUpperBound } = Solution,
	%% We are going to validate the VDF data later anyways; here we simply want to
	%% make sure the upper bound is positive so that the recall byte calculation
	%% does not fail as it takes a remainder of the division by partition upper bound.
	case PartitionUpperBound > 0 of
		true ->
			process_partial_solution_poa(Solution, Ref, H0, H1);
		_ ->
			#partial_solution_response{ status = <<"rejected_bad_poa">> }
	end.

process_partial_solution_poa(Solution, Ref, H0, H1) ->
	#mining_solution{
		partition_number = PartitionNumber,
		partition_upper_bound = PartitionUpperBound,
		nonce = Nonce,
		recall_byte1 = RecallByte1,
		poa1 = PoA1,
		mining_address = MiningAddress,
		solution_hash = SolutionH,
		recall_byte2 = RecallByte2,
		poa2 = PoA2
	} = Solution,
	{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
			PartitionNumber, PartitionUpperBound),
	ComputedRecallByte1 = RecallRange1Start + Nonce * ?DATA_CHUNK_SIZE,
	{BlockStart1, BlockEnd1, TXRoot1} = ar_block_index:get_block_bounds(ComputedRecallByte1),
	BlockSize1 = BlockEnd1 - BlockStart1,
	case RecallByte1 == ComputedRecallByte1 andalso
			ar_poa:validate({BlockStart1, RecallByte1, TXRoot1, BlockSize1, PoA1,
					{spora_2_6, MiningAddress}, not_set}) of
		error ->
			?LOG_ERROR([{event, pool_failed_to_validate_proof_of_access}]),
			#partial_solution_response{ status = <<"rejected_bad_poa">> };
		false ->
			#partial_solution_response{ status = <<"rejected_bad_poa">> };
		{true, ChunkID} when H1 == SolutionH ->
			PoACache = {{BlockStart1, RecallByte1, TXRoot1, BlockSize1,
					{spora_2_6, MiningAddress}}, ChunkID},
			PoA2Cache = undefined,
			process_partial_solution_difficulty(Solution, Ref, PoACache, PoA2Cache);
		{true, ChunkID} ->
			ComputedRecallByte2 = RecallRange2Start + Nonce * ?DATA_CHUNK_SIZE,
			{BlockStart2, BlockEnd2, TXRoot2} = ar_block_index:get_block_bounds(
					ComputedRecallByte2),
			BlockSize2 = BlockEnd2 - BlockStart2,
			case RecallByte2 == ComputedRecallByte2 andalso
					ar_poa:validate({BlockStart2, RecallByte2, TXRoot2, BlockSize2,
									PoA2, {spora_2_6, MiningAddress}, not_set}) of
				error ->
					?LOG_ERROR([{event, pool_failed_to_validate_proof_of_access}]),
					#partial_solution_response{ status = <<"rejected_bad_poa">> };
				false ->
					#partial_solution_response{ status = <<"rejected_bad_poa">> };
				{true, Chunk2ID} ->
					PoA2Cache = {{BlockStart2, RecallByte2, TXRoot2, BlockSize2,
							{spora_2_6, MiningAddress}}, Chunk2ID},
					PoACache = {{BlockStart1, RecallByte1, TXRoot1, BlockSize1,
							{spora_2_6, MiningAddress}}, ChunkID},
					process_partial_solution_difficulty(Solution, Ref, PoACache, PoA2Cache)
			end
	end.

process_partial_solution_difficulty(Solution, Ref, PoACache, PoA2Cache) ->
	#mining_solution{ solution_hash = SolutionH, recall_byte2 = RecallByte2 } = Solution,
	IsPoA1 = (RecallByte2 == undefined),
	case ar_node_utils:passes_diff_check(SolutionH, IsPoA1, ar_node:get_current_diff()) of
		false ->
			#partial_solution_response{ status = <<"accepted">> };
		true ->
			process_partial_solution_vdf(Solution, Ref, PoACache, PoA2Cache)
	end.

process_partial_solution_vdf(Solution, Ref, PoACache, PoA2Cache) ->
	#mining_solution{
		step_number = StepNumber,
		next_seed = NextSeed,
		start_interval_number = StartIntervalNumber,
		next_vdf_difficulty = NextVDFDifficulty,
		nonce_limiter_output = Output,
		seed = Seed,
		partition_upper_bound = PartitionUpperBound
	} = Solution,
	SessionKey = {NextSeed, StartIntervalNumber, NextVDFDifficulty},
	MayBeLastStepCheckpoints = ar_nonce_limiter:get_step_checkpoints(StepNumber, SessionKey),
	MayBeSeed = ar_nonce_limiter:get_seed(SessionKey),
	MayBeUpperBound = ar_nonce_limiter:get_active_partition_upper_bound(StepNumber, SessionKey),
	case {MayBeLastStepCheckpoints, MayBeSeed, MayBeUpperBound} of
		{not_found, _, _} ->
			#partial_solution_response{ status = <<"rejected_vdf_not_found">> };
		{_, not_found, _} ->
			#partial_solution_response{ status = <<"rejected_vdf_not_found">> };
		{_, _, not_found} ->
			#partial_solution_response{ status = <<"rejected_vdf_not_found">> };
		{[Output | _] = LastStepCheckpoints, Seed, PartitionUpperBound} ->
			Solution2 =
				Solution#mining_solution{
					last_step_checkpoints = LastStepCheckpoints,
					%% ar_node_worker will fetch the required steps based on the prev block.
					steps = not_found
				},
			ar_events:send(miner, {found_solution, {pool, Ref},
					Solution2, PoACache, PoA2Cache}),
			noreply;
		_ ->
			%% {Output, Seed, PartitionUpperBound} mismatch (pattern matching against
			%% the solution fields deconstructed above).
			#partial_solution_response{ status = <<"rejected_bad_vdf">> }
	end.

process_h1_to_h2_jobs([], _Peer, _Partitions) ->
	ok;
process_h1_to_h2_jobs([Candidate | Jobs], Peer, Partitions) ->
	case we_have_partition_for_the_second_recall_byte(Candidate, Partitions) of
		true ->
			ar_coordination:compute_h2_for_peer(Peer, Candidate);
		false ->
			ok
	end,
	process_h1_to_h2_jobs(Jobs, Peer, Partitions).

process_h1_read_jobs([], _Partitions) ->
	ok;
process_h1_read_jobs([Candidate | Jobs], Partitions) ->
	case we_have_partition_for_the_first_recall_byte(Candidate, Partitions) of
		true ->
			ar_mining_server:prepare_and_post_solution(Candidate),
			ar_mining_stats:h2_received_from_peer(pool);
		false ->
			ok
	end,
	process_h1_read_jobs(Jobs, Partitions).

we_have_partition_for_the_first_recall_byte(_Candidate, []) ->
	false;
we_have_partition_for_the_first_recall_byte(
		#mining_candidate{ mining_address = Addr, partition_number = PartitionID },
		[{PartitionID, Addr} | _Partitions]) ->
	true;
we_have_partition_for_the_first_recall_byte(Candidate, [_Partition | Partitions]) ->
	%% Mining address or partition number mismatch.
	we_have_partition_for_the_first_recall_byte(Candidate, Partitions).

we_have_partition_for_the_second_recall_byte(_Candidate, []) ->
	false;
we_have_partition_for_the_second_recall_byte(
		#mining_candidate{ mining_address = Addr, partition_number2 = PartitionID },
		[{PartitionID, Addr} | _Partitions]) ->
	true;
we_have_partition_for_the_second_recall_byte(Candidate, [_Partition | Partitions]) ->
	%% Mining address or partition number mismatch.
	we_have_partition_for_the_second_recall_byte(Candidate, Partitions).

%%%===================================================================
%%% Tests.
%%%===================================================================

get_jobs_test() ->
	?assertEqual(#jobs{}, get_jobs(<<>>, [], maps:new())),

	?assertEqual(#jobs{ next_seed = ns, interval_number = in, next_vdf_difficulty = nvd },
			get_jobs(o, [{ns, in, nvd}],
						#{ {ns, in, nvd} => [{o, gsn, u, s, d}] })),

	?assertEqual(#jobs{ jobs = [#job{ output = o, global_step_number = gsn,
							partition_upper_bound = u }],
						partial_diff = d,
						seed = s,
						next_seed = ns,
						interval_number = in,
						next_vdf_difficulty = nvd },
			get_jobs(a, [{ns, in, nvd}],
						#{ {ns, in, nvd} => [{o, gsn, u, s, d}] })),

	%% d2 /= d (the difficulties are different) => only take the latest job.
	?assertEqual(#jobs{ jobs = [#job{ output = o, global_step_number = gsn,
							partition_upper_bound = u }],
						partial_diff = d,
						seed = s,
						next_seed = ns,
						interval_number = in,
						next_vdf_difficulty = nvd },
			get_jobs(a, [{ns, in, nvd}, {ns2, in2, nvd2}],
						#{ {ns, in, nvd} => [{o, gsn, u, s, d}, {o2, gsn2, u2, s, d2}],
							%% Same difficulty, but a different VDF session => not picked.
							{ns2, in2, nvd2} => [{o3, gsn3, u3, s3, d}] })),

	%% d2 == d => take both.
	?assertEqual(#jobs{ jobs = [#job{ output = o, global_step_number = gsn,
							partition_upper_bound = u }, #job{ output = o2,
									global_step_number = gsn2, partition_upper_bound = u2 }],
						partial_diff = d,
						seed = s,
						next_seed = ns,
						interval_number = in,
						next_vdf_difficulty = nvd },
			get_jobs(a, [{ns, in, nvd}, {ns2, in2, nvd2}],
						#{ {ns, in, nvd} => [{o, gsn, u, s, d}, {o2, gsn2, u2, s, d}],
							{ns2, in2, nvd2} => [{o2, gsn2, u2, s2, d2}] })),

	%% Take strictly above the previous output.
	?assertEqual(#jobs{ jobs = [#job{ output = o, global_step_number = gsn,
								partition_upper_bound = u }],
						partial_diff = d,
						seed = s,
						next_seed = ns,
						interval_number = in,
						next_vdf_difficulty = nvd },
			get_jobs(o2, [{ns, in, nvd}, {ns2, in2, nvd2}],
						#{ {ns, in, nvd} => [{o, gsn, u, s, d}, {o2, gsn2, u2, s, d}],
							{ns2, in2, nvd2} => [{o2, gsn2, u2, s2, d2}] })).

process_partial_solution_test_() ->
	ar_test_node:test_with_mocked_functions([
		{ar_block, compute_h0,
			fun(O, P, S, M) ->
					crypto:hash(sha256, << O/binary, P:256, S/binary, M/binary >>) end},
		{ar_block_index, get_block_bounds,
			fun(_Byte) ->
				{10, 110, << 1:256 >>}
			end},
		{ar_poa, validate,
			fun(Args) ->
				PoA = #poa{ tx_path = << 0:(2176 * 8) >>, data_path = << 0:(349504 * 8) >> },
				case Args of
					{10, _, << 1:256 >>, 100, PoA, {spora_2_6, << 0:256 >>}, not_set} ->
						{true, << 2:256 >>};
					_ ->
						false
				end
			end},
		{ar_node, get_current_diff, fun() -> {?MAX_DIFF, ?MAX_DIFF} end}],
		fun test_process_partial_solution/0
	).

test_process_partial_solution() ->
	Zero = << 0:256 >>,
	Zero48 = << 0:(8*48) >>,
	H0 = ar_block:compute_h0(Zero, 0, Zero48, Zero),
	SolutionHQuick = ar_block:compute_solution_h(H0, Zero),
	{H1, Preimage1} = ar_block:compute_h1(H0, 1, <<>>),
	SolutionH = ar_block:compute_solution_h(H0, Preimage1),
	{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0, 0, 1),
	RecallByte1 = RecallRange1Start + 1 * ?DATA_CHUNK_SIZE,
	{H2, Preimage2} = ar_block:compute_h2(H1, <<>>, H0),
	RecallByte2 = RecallRange2Start + 1 * ?DATA_CHUNK_SIZE,
	TestCases = [
		{"Bad proof size 1",
			#mining_solution{ poa1 = #poa{ tx_path = << 0:(2177 * 8) >> } },
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad proof size 2",
			#mining_solution{ poa2 = #poa{ tx_path = << 0:(2177 * 8) >> } },
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad proof size 3",
			#mining_solution{ poa1 = #poa{ data_path = << 0:(349505 * 8) >> } },
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad proof size 4",
			#mining_solution{ poa2 = #poa{ data_path = << 0:(349505 * 8) >> } },
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad field size 1",
			#mining_solution{ next_seed = <<>> },
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad field size 2",
			#mining_solution{ seed = <<>> },
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad field size 3",
			#mining_solution{ preimage = <<>> },
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad field size 4",
			#mining_solution{ mining_address = <<>> },
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad field size 5",
			#mining_solution{ nonce_limiter_output = <<>> },
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad field size 6",
			#mining_solution{ solution_hash = <<>> },
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad field size 7",
			#mining_solution{ poa1 = #poa{ chunk = << 0:((?DATA_CHUNK_SIZE + 1) * 8) >> }},
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad field size 8",
			#mining_solution{ poa2 = #poa{ chunk = << 0:((?DATA_CHUNK_SIZE + 1) * 8) >> }},
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},

		{"Bad partition number",
			#mining_solution{ partition_number = 1 },
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad nonce",
			#mining_solution{ nonce = 2 }, % We have 2 nonces per recall range in debug mode.
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad quick pow",
			#mining_solution{},
			#partial_solution_response{ status = <<"rejected_wrong_hash">> }},
		{"Bad pow",
			#mining_solution{ nonce = 1, solution_hash = SolutionHQuick },
			#partial_solution_response{ status = <<"rejected_wrong_hash">> }},
		{"Bad partition upper bound",
			#mining_solution{ nonce = 1, solution_hash = SolutionH, preimage = Preimage1 },
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad poa 1",
			#mining_solution{ nonce = 1, solution_hash = SolutionH, preimage = Preimage1,
					partition_upper_bound = 1 },
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad poa 2",
			#mining_solution{ nonce = 1, solution_hash = SolutionH,
					preimage = Preimage1, partition_upper_bound = 1,
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},
		{"Bad poa 3",
			#mining_solution{ nonce = 1, solution_hash = SolutionH,
					preimage = Preimage1, partition_upper_bound = 1,
					recall_byte1 = RecallByte1,
					poa2 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> },
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},

		{"Two-chunk bad poa 1",
			#mining_solution{ nonce = 1, solution_hash = SolutionH,
					preimage = Preimage1, partition_upper_bound = 1,
					recall_byte1 = RecallByte1, recall_byte2 = 0,
					poa2 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> },
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"rejected_wrong_hash">> }},
		{"Two-chunk bad poa 2",
			#mining_solution{ nonce = 1, solution_hash = SolutionH,
					preimage = Preimage2, partition_upper_bound = 1,
					recall_byte1 = RecallByte1, recall_byte2 = 0,
					poa2 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> },
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"rejected_wrong_hash">> }},
		{"Two-chunk bad poa 3",
			#mining_solution{ nonce = 1, solution_hash = H2,
					preimage = Preimage2, partition_upper_bound = 1,
					recall_byte1 = RecallByte1, recall_byte2 = 0,
					poa2 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> },
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"rejected_bad_poa">> }},

		{"Accepted",
			#mining_solution{ nonce = 1, solution_hash = SolutionH,
					preimage = Preimage1, partition_upper_bound = 1,
					recall_byte1 = RecallByte1,
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"accepted">> }},
		{"Accepted 2",
			#mining_solution{ nonce = 1, solution_hash = H2,
					preimage = Preimage2, partition_upper_bound = 1,
					recall_byte1 = RecallByte1, recall_byte2 = RecallByte2,
					poa2 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> },
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"accepted">> }}
	],
	lists:foreach(
		fun({Title, Solution, ExpectedReply}) ->
			Ref = make_ref(),
			?assertEqual(ExpectedReply, process_partial_solution(Solution, Ref), Title)
		end,
		TestCases
	).

process_solution_test_() ->
	ar_test_node:test_with_mocked_functions([
		{ar_block, compute_h0,
			fun(O, P, S, M) ->
					crypto:hash(sha256, << O/binary, P:256, S/binary, M/binary >>) end},
		{ar_block_index, get_block_bounds,
			fun(_Byte) ->
				{10, 110, << 1:256 >>}
			end},
		{ar_poa, validate,
			fun(Args) ->
				PoA = #poa{ tx_path = << 0:(2176 * 8) >>, data_path = << 0:(349504 * 8) >> },
				case Args of
					{10, _, << 1:256 >>, 100, PoA, {spora_2_6, << 0:256 >>}, not_set} ->
						{true, << 2:256 >>};
					_ ->
						false
				end
			end},
		{ar_node, get_current_diff, fun() -> {0, 0} end},
		{ar_nonce_limiter, get_step_checkpoints,
			fun(S, {N, SIN, D}) ->
				case {S, N, SIN, D} of
					{0, << 10:(48*8) >>, 0, 0} ->
						%% Test not found.
						not_found;
					{0, << 3:(48*8) >>, 0, 0} ->
						%% Test output mismatch (<< 1:256 >> /= << 0:256 >>).
						[<< 1:256 >>];
					_ ->
						[<< 0:256 >>]
				end
			end},
		{ar_nonce_limiter, get_seed,
			fun({N, SIN, D}) ->
				case {N, SIN, D} of
					{<< 11:(48*8) >>, 0, 0} ->
						%% Test not_found.
						not_found;
					{<< 2:(48*8) >>, 0, 0} ->
						%% Test seed mismatch (<< 3:(48*8) >> /= << 0:(48*8) >>).
						<< 3:(48*8) >>;
					_ ->
						<< 0:(48*8) >>
				end
			end},
		{ar_nonce_limiter, get_active_partition_upper_bound,
			fun(S, {N, SIN, D}) ->
				case {S, N, SIN, D} of
					{0, << 12:(48*8) >>, 0, 0} ->
						%% Test not_found.
						not_found;
					{0, << 1:(48*8) >>, 0, 0} ->
						%% Test partition upper bound mismatch (2 /= 1).
						2;
					_ ->
						1
				end
			end},
		{ar_events, send, fun(_Type, _Payload) -> ok end}],
		fun test_process_solution/0
	).

test_process_solution() ->
	Zero = << 0:256 >>,
	Zero48 = << 0:(48*8) >>,
	H0 = ar_block:compute_h0(Zero, 0, Zero48, Zero),
	{_H1, Preimage1} = ar_block:compute_h1(H0, 1, <<>>),
	SolutionH = ar_block:compute_solution_h(H0, Preimage1),
	{RecallRange1Start, _RecallRange2Start} = ar_block:get_recall_range(H0, 0, 1),
	RecallByte1 = RecallRange1Start + 1 * ?DATA_CHUNK_SIZE,
	TestCases = [
		{"VDF not found",
			#mining_solution{ next_seed = << 10:(48*8) >>, nonce = 1, solution_hash = SolutionH,
					preimage = Preimage1, partition_upper_bound = 1,
					recall_byte1 = RecallByte1,
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"rejected_vdf_not_found">> }},
		{"VDF not found 2",
			#mining_solution{ next_seed = << 11:(48*8) >>, nonce = 1, solution_hash = SolutionH,
					preimage = Preimage1, partition_upper_bound = 1,
					recall_byte1 = RecallByte1,
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"rejected_vdf_not_found">> }},
		{"VDF not found 3",
			#mining_solution{ next_seed = << 12:(48*8) >>, nonce = 1, solution_hash = SolutionH,
					preimage = Preimage1, partition_upper_bound = 1,
					recall_byte1 = RecallByte1,
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"rejected_vdf_not_found">> }},
		{"Bad VDF 1",
			#mining_solution{ next_seed = << 1:(48*8) >>, nonce = 1, solution_hash = SolutionH,
					preimage = Preimage1, partition_upper_bound = 1,
					recall_byte1 = RecallByte1,
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"rejected_bad_vdf">> }},
		{"Bad VDF 2",
			#mining_solution{ next_seed = << 2:(48*8) >>, nonce = 1, solution_hash = SolutionH,
					preimage = Preimage1, partition_upper_bound = 1,
					recall_byte1 = RecallByte1,
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"rejected_bad_vdf">> }},
		{"Bad VDF 3",
			#mining_solution{ next_seed = << 3:(48*8) >>, nonce = 1, solution_hash = SolutionH,
					preimage = Preimage1, partition_upper_bound = 1,
					recall_byte1 = RecallByte1,
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"rejected_bad_vdf">> }},
		{"Accepted",
			#mining_solution{ next_seed = << 4:(48*8) >>, nonce = 1, solution_hash = SolutionH,
					preimage = Preimage1, partition_upper_bound = 1,
					recall_byte1 = RecallByte1,
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			noreply}
	],
	lists:foreach(
		fun({Title, Solution, ExpectedReply}) ->
			Ref = make_ref(),
			?assertEqual(ExpectedReply, process_partial_solution(Solution, Ref), Title)
		end,
		TestCases
	).
