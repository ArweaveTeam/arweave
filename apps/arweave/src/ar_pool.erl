-module(ar_pool).

-behaviour(gen_server).

-export([start_link/0, is_client/0, get_current_sessions/0, get_jobs/1,
		get_latest_output/0, cache_jobs/1, process_partial_solution/1,
		post_partial_solution/1]).

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
	Config#config.pool_client == true.

%% @doc Return a list of up to two most recently cached VDF session keys.
get_current_sessions() ->
	gen_server:call(?MODULE, get_current_sessions, infinity).

%% @doc Return a set of the most recent cached jobs.
get_jobs(PrevOutput) ->
	gen_server:call(?MODULE, {get_jobs, PrevOutput}, infinity).

%% @doc Return the most recent cached VDF output. Return an empty binary if the
%% cache is empty.
get_latest_output() ->
	gen_server:call(?MODULE, get_latest_output, infinity).

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

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(solution),
	{ok, #state{}}.

handle_call(get_current_sessions, _From, State) ->
	{reply, lists:sublist(State#state.session_keys, 2), State};

handle_call({get_jobs, PrevOutput}, _From, State) ->
	SessionKeys = State#state.session_keys,
	JobCache = State#state.jobs_by_session_key,
	{reply, get_jobs(PrevOutput, SessionKeys, JobCache), State};

handle_call(get_latest_output, _From, State) ->
	case State#state.session_keys of
		[] ->
			{reply, <<>>, State};
		[Key | _] ->
			{O, _SN, _U, _S, _Diff} = hd(maps:get(Key, State#state.jobs_by_session_key)),
			{reply, O, State}
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

handle_cast({cache_jobs, #jobs{ vdf = [] }}, State) ->
	{noreply, State};
handle_cast({cache_jobs, Jobs}, State) ->
	#jobs{ vdf = VDF, diff = Diff, next_seed = NextSeed, seed = Seed,
			interval_number = IntervalNumber,
			next_vdf_difficulty = NextVDFDifficulty } = Jobs,
	SessionKey = {NextSeed, IntervalNumber, NextVDFDifficulty},
	SessionKeys = State#state.session_keys,
	SessionKeys2 = [SessionKey | SessionKeys],
	JobList = [{Job#job.output, Job#job.global_step_number,
			Job#job.partition_upper_bound, Seed, Diff} || Job <- VDF],
	PrevJobList = maps:get(SessionKey, State#state.jobs_by_session_key, []),
	JobList2 = JobList ++ PrevJobList,
	JobsBySessionKey = maps:put(SessionKey, JobList2, State#state.jobs_by_session_key),
	{SessionKeys3, JobsBySessionKey2} =
		case length(SessionKeys2) == 3 of
			true ->
				{RemoveKey, SessionKeys4} = lists:last(SessionKeys2),
				{SessionKeys4, maps:remove(RemoveKey, JobsBySessionKey)};
			false ->
				{SessionKeys2, JobsBySessionKey}
		end,
	{noreply, State#state{ session_keys = SessionKeys3,
			jobs_by_session_key = JobsBySessionKey2 }};

handle_cast({post_partial_solution, Solution}, State) ->
	{ok, Config} = application:get_env(arweave, config),
	case ar_http_iface_client:post_partial_solution(Config#config.pool_host, Solution, true) of
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

handle_info({event, solution, {processed, #{ source := {pool, Ref} }}}, State) ->
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
			{Seed, Diff, Jobs2} = collect_jobs(Jobs, PrevOutput, ?GET_JOBS_COUNT),
			Jobs3 = [#job{ output = O, global_step_number = SN,
					partition_upper_bound = U } || {O, SN, U} <- Jobs2],
			#jobs{ vdf = Jobs3, seed = Seed, diff = Diff, next_seed = NextSeed,
					interval_number = Interval, next_vdf_difficulty = NextVDFDifficulty }
	end.

collect_jobs([], _PrevO, _N) ->
	{<<>>, 0, []};
collect_jobs(_Jobs, _PrevO, 0) ->
	{<<>>, 0, []};
collect_jobs([{O, _SN, _U, _S, _Diff} | _Jobs], O, _N) ->
	{<<>>, 0, []};
collect_jobs([{O, SN, U, S, Diff} | Jobs], PrevO, N) ->
	{S, Diff, [{O, SN, U} | collect_jobs(Jobs, PrevO, N - 1, Diff)]}.

collect_jobs([], _PrevO, _N, _Diff) ->
	[];
collect_jobs(_Jobs, _PrevO, 0, _Diff) ->
	[];
collect_jobs([{O, _SN, _U, _S, _Diff} | _Jobs], O, _N, _Diff2) ->
	[];
collect_jobs([{O, SN, U, _S, Diff} | Jobs], PrevO, N, Diff) ->
	[{O, SN, U} | collect_jobs(Jobs, PrevO, N - 1, Diff)];
collect_jobs(_Jobs, _PrevO, _N, _Diff) ->
	%% Diff mismatch.
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
	%% the pool proxy implementation. Therefore, we validate the field sizes here
	%% and return the "rejected_bad_poa" status in case of a failure.
	case {byte_size(Output), byte_size(Seed), byte_size(NextSeed), byte_size(MiningAddress),
			byte_size(Preimage), byte_size(SolutionH), byte_size(C1), byte_size(C2)} of
		{32, 32, 32, 32, 32, 32, L1, L2} when L1 =< ?DATA_CHUNK_SIZE, L2 =< ?DATA_CHUNK_SIZE ->
			%% The second chunk may be either empty or 256 KiB. ar_poa:validate/1 does
			%% the proper verification - here we simply protect against payload size abuse.
			%% We are not strict about the first chunk here to simplify tests.
			process_partial_solution_poa2(Solution, Ref);
		_ ->
			#partial_solution_response{ status = <<"rejected_bad_poa">> }
	end.

process_partial_solution_poa2(Solution, Ref) ->
	#mining_solution{
		recall_byte2 = RecallByte2,
		poa2 = #poa{ chunk = C, data_path = DP, tx_path = TP }
	} = Solution,
	case RecallByte2 of
		undefined ->
			case {C, DP, TP} of
				{<<>>, <<>>, <<>>} ->
					process_partial_solution_partition_number(Solution, Ref);
				_ ->
					#partial_solution_response{ status = <<"rejected_bad_poa">> }
			end;
		_ ->
			process_partial_solution_partition_number(Solution, Ref)
	end.

process_partial_solution_partition_number(Solution, Ref) ->
	PartitionNumber = Solution#mining_solution.partition_number,
	PartitionUpperBound = Solution#mining_solution.partition_upper_bound,
	Max = max(0, PartitionUpperBound div ?PARTITION_SIZE - 1),
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
			#partial_solution_response{ status = <<"rejected_wrong_hash">> }
	end.

process_partial_solution_pow(Solution, Ref, H0) ->
	#mining_solution{
		nonce = Nonce,
		poa1 = #poa{ chunk = Chunk1 },
		solution_hash = SolutionH,
		preimage = Preimage,
		recall_byte2 = RecallByte2,
		poa2 = #poa{ chunk = Chunk2 }
	} = Solution,
	{H1, Preimage1} = ar_block:compute_h1(H0, Nonce, Chunk1),
	case H1 == SolutionH andalso Preimage1 == Preimage
			andalso RecallByte2 == undefined of
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
	#mining_solution{ solution_hash = SolutionH } = Solution,
	Diff = ar_node:get_current_diff(),
	case binary:decode_unsigned(SolutionH, big) > Diff of
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
	case ar_nonce_limiter:get_step_checkpoints_seed_upper_bound(StepNumber, NextSeed,
			StartIntervalNumber, NextVDFDifficulty) of
		not_found ->
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
			#partial_solution_response{ status = <<"rejected_bad_vdf">> }
	end.

get_jobs_test() ->
	?assertEqual(#jobs{}, get_jobs(<<>>, [], maps:new())),

	?assertEqual(#jobs{ next_seed = ns, interval_number = in, next_vdf_difficulty = nvd },
			get_jobs(o, [{ns, in, nvd}],
			#{ {ns, in, nvd} => [{o, gsn, u, s, d}] })),

	?assertEqual(#jobs{ vdf = [#job{ output = o, global_step_number = gsn,
							partition_upper_bound = u }],
						diff = d,
						seed = s,
						next_seed = ns,
						interval_number = in,
						next_vdf_difficulty = nvd },
			get_jobs(a, [{ns, in, nvd}],
						#{ {ns, in, nvd} => [{o, gsn, u, s, d}] })),

	%% d2 /= d (the difficulties are different) => only take the latest job.
	?assertEqual(#jobs{ vdf = [#job{ output = o, global_step_number = gsn,
							partition_upper_bound = u }],
						diff = d,
						seed = s,
						next_seed = ns,
						interval_number = in,
						next_vdf_difficulty = nvd },
			get_jobs(a, [{ns, in, nvd}, {ns2, in2, nvd2}],
						#{ {ns, in, nvd} => [{o, gsn, u, s, d}, {o2, gsn2, u2, s, d2}],
							%% Same difficulty, but a different VDF session => not picked.
							{ns2, in2, nvd2} => [{o3, gsn3, u3, s3, d}] })),

	%% d2 == d => take both.
	?assertEqual(#jobs{ vdf = [#job{ output = o, global_step_number = gsn,
							partition_upper_bound = u }, #job{ output = o2,
									global_step_number = gsn2, partition_upper_bound = u2 }],
						diff = d,
						seed = s,
						next_seed = ns,
						interval_number = in,
						next_vdf_difficulty = nvd },
			get_jobs(a, [{ns, in, nvd}, {ns2, in2, nvd2}],
						#{ {ns, in, nvd} => [{o, gsn, u, s, d}, {o2, gsn2, u2, s, d}],
							{ns2, in2, nvd2} => [{o2, gsn2, u2, s2, d2}] })),

	%% Take strictly above the previous output.
	?assertEqual(#jobs{ vdf = [#job{ output = o, global_step_number = gsn,
								partition_upper_bound = u }],
						diff = d,
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
		{ar_node, get_current_diff, fun() -> ?MAX_DIFF end}],
		fun test_process_partial_solution/0
	).

test_process_partial_solution() ->
	Zero = << 0:256 >>,
	H0 = ar_block:compute_h0(Zero, 0, Zero, Zero),
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
		{ar_node, get_current_diff, fun() -> 0 end},
		{ar_nonce_limiter, get_step_checkpoints_seed_upper_bound,
			fun(S, N, SIN, D) ->
				case {S, N, SIN, D} of
					{0, << 1:256 >>, 0, 0} ->
						%% Test partition upper bound mismatch (2 /= 1).
						{[<< 0:256 >>], << 0:256 >>, 2};
					{0, << 2:256 >>, 0, 0} ->
						%% Test partition upper seed mismatch (<< 3:256 >> /= << 0:256 >>).
						{[<< 0:256 >>], << 3:256 >>, 1};
					{0, << 3:256 >>, 0, 0} ->
						%% Test output mismatch (<< 1:256 >> /= << 0:256 >>).
						{[<< 1:256 >>], << 0:256 >>, 1};
					{0, << 4:256 >>, 0, 0} ->
						%% Happy path (achieved with next_seed = << 4:256 >>).
						{[<< 0:256 >>], << 0:256 >>, 1};
					_ ->
						not_found
				end
			end},
		{ar_events, send, fun(_Type, _Payload) -> ok end}],
		fun test_process_solution/0
	).

test_process_solution() ->
	Zero = << 0:256 >>,
	H0 = ar_block:compute_h0(Zero, 0, Zero, Zero),
	{_H1, Preimage1} = ar_block:compute_h1(H0, 1, <<>>),
	SolutionH = ar_block:compute_solution_h(H0, Preimage1),
	{RecallRange1Start, _RecallRange2Start} = ar_block:get_recall_range(H0, 0, 1),
	RecallByte1 = RecallRange1Start + 1 * ?DATA_CHUNK_SIZE,
	TestCases = [
		{"VDF not found",
			#mining_solution{ next_seed = << 10:256 >>, nonce = 1, solution_hash = SolutionH,
					preimage = Preimage1, partition_upper_bound = 1,
					recall_byte1 = RecallByte1,
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"rejected_vdf_not_found">> }},
		{"Bad VDF 1",
			#mining_solution{ next_seed = << 1:256 >>, nonce = 1, solution_hash = SolutionH,
					preimage = Preimage1, partition_upper_bound = 1,
					recall_byte1 = RecallByte1,
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"rejected_bad_vdf">> }},
		{"Bad VDF 2",
			#mining_solution{ next_seed = << 2:256 >>, nonce = 1, solution_hash = SolutionH,
					preimage = Preimage1, partition_upper_bound = 1,
					recall_byte1 = RecallByte1,
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"rejected_bad_vdf">> }},
		{"Bad VDF 3",
			#mining_solution{ next_seed = << 3:256 >>, nonce = 1, solution_hash = SolutionH,
					preimage = Preimage1, partition_upper_bound = 1,
					recall_byte1 = RecallByte1,
					poa1 = #poa{ tx_path = << 0:(2176 * 8) >>,
						data_path = << 0:(349504 * 8) >> }},
			#partial_solution_response{ status = <<"rejected_bad_vdf">> }},
		{"Accepted",
			#mining_solution{ next_seed = << 4:256 >>, nonce = 1, solution_hash = SolutionH,
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
