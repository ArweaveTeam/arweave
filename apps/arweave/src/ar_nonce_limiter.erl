-module(ar_nonce_limiter).

-behaviour(gen_server).

-export([start_link/0, account_tree_initialized/1, encode_session_key/1, session_key/1,
		is_ahead_on_the_timeline/2,
		get_current_step_number/0, get_current_step_number/1, get_step_triplets/3,
		get_seed_data/2, get_step_checkpoints/2, get_step_checkpoints/4, get_steps/4,
		get_seed/1, get_active_partition_upper_bound/2,
		get_reset_frequency/0, get_entropy_reset_point/2,
		validate_last_step_checkpoints/3, request_validation/3,
		get_or_init_nonce_limiter_info/1, get_or_init_nonce_limiter_info/2,
		apply_external_update/2, get_session/1, get_current_session/0,
		get_current_sessions/0,
		compute/3,
		maybe_add_entropy/4, mix_seed/2]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_vdf.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include("ar_consensus.hrl").

-include_lib("eunit/include/eunit.hrl").

-record(state, {
	current_session_key,
	sessions = gb_sets:new(),
	session_by_key = #{}, % {NextSeed, StartIntervalNumber, NextVDFDifficulty} => #vdf_session
	worker,
	worker_monitor_ref,
	autocompute = true,
	computing = false,
	last_external_update = {not_set, 0},
	emit_initialized_event = true
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

account_tree_initialized(Blocks) ->
	gen_server:cast(?MODULE, {account_tree_initialized, Blocks}).

encode_session_key({NextSeed, StartIntervalNumber, NextVDFDifficulty}) ->
	{ar_util:safe_encode(NextSeed), StartIntervalNumber, NextVDFDifficulty};
encode_session_key(SessionKey) ->
	SessionKey.

%% @doc Return true if the first solution is above the second one according
%% to the protocol ordering.
is_ahead_on_the_timeline(NonceLimiterInfo1, NonceLimiterInfo2) ->
	#nonce_limiter_info{ global_step_number = N1 } = NonceLimiterInfo1,
	#nonce_limiter_info{ global_step_number = N2 } = NonceLimiterInfo2,
	N1 > N2.

session_key(#nonce_limiter_info{ 
		next_seed = NextSeed, global_step_number = StepNumber,
		next_vdf_difficulty = NextVDFDifficulty }) ->
	session_key(NextSeed, StepNumber, NextVDFDifficulty).

%% @doc Return the nonce limiter session with the given key.
get_session(SessionKey) ->
	gen_server:call(?MODULE, {get_session, SessionKey}, ?DEFAULT_CALL_TIMEOUT).

%% @doc Return {SessionKey, Session} for the current VDF session.
get_current_session() ->
	gen_server:call(?MODULE, get_current_session, ?DEFAULT_CALL_TIMEOUT).

%% @doc Return a list of up to two {SessionKey, Session} pairs
%% where the first pair corresponds to the current VDF session
%% and the second pair is its previous session, if any.
get_current_sessions() ->
	gen_server:call(?MODULE, get_current_sessions, ?DEFAULT_CALL_TIMEOUT).

%% @doc Return the latest known step number.
get_current_step_number() ->
	gen_server:call(?MODULE, get_current_step_number, ?DEFAULT_CALL_TIMEOUT).

%% @doc Return the latest known step number in the session of the given (previous) block.
%% Return not_found if the session is not found.
get_current_step_number(B) ->
	SessionKey = session_key(B#block.nonce_limiter_info),
	gen_server:call(?MODULE, {get_current_step_number, SessionKey}, ?DEFAULT_CALL_TIMEOUT).

%% @doc Return {Output, StepNumber, PartitionUpperBound} for up to N latest steps
%% from the VDF session of Info, if any. If PrevOutput is among the N latest steps,
%% return only the steps strictly above PrevOutput.
get_step_triplets(Info, PrevOutput, N) ->
	SessionKey = session_key(Info),
	Steps = gen_server:call(?MODULE, {get_latest_step_triplets, SessionKey, N}, ?DEFAULT_CALL_TIMEOUT),
	filter_step_triplets(Steps, [PrevOutput, Info#nonce_limiter_info.output]).

%% @doc Return {Seed, NextSeed, PartitionUpperBound, NextPartitionUpperBound, VDFDifficulty}
%% for the block mined at StepNumber considering its previous block PrevB.
%% The previous block's independent hash, weave size, and VDF difficulty
%% become the new NextSeed, NextPartitionUpperBound, and NextVDFDifficulty
%% accordingly when we cross the next reset line.
%% Note: next_vdf_difficulty is not part of the seed data as it is computed using the
%% block_time_history - which is a heavier operation handled separate from the (quick) seed data
%% retrieval
get_seed_data(StepNumber, PrevB) ->
	NonceLimiterInfo = PrevB#block.nonce_limiter_info,
	#nonce_limiter_info{
		global_step_number = PrevStepNumber,
		seed = Seed, next_seed = NextSeed,
		partition_upper_bound = PartitionUpperBound,
		next_partition_upper_bound = NextPartitionUpperBound,
		%% VDF difficulty in use at the previous block
		vdf_difficulty = VDFDifficulty,
		%% Next VDF difficulty scheduled at the previous block
		next_vdf_difficulty = PrevNextVDFDifficulty
	} = NonceLimiterInfo,
	true = StepNumber > PrevStepNumber,
	case get_entropy_reset_point(PrevStepNumber, StepNumber) of
		none ->
			%% Entropy reset line was not crossed between previous and current block
			{ Seed, NextSeed, PartitionUpperBound, NextPartitionUpperBound, VDFDifficulty };
		_ ->
			%% Entropy reset line was crossed between previous and current block
			{
				NextSeed, PrevB#block.indep_hash,
				NextPartitionUpperBound, PrevB#block.weave_size,
				%% The next VDF difficulty that was scheduled at the previous block
				%% (PrevNextVDFDifficulty) was applied when we crossed the entropy reset line and
				%% is now the current VDF difficulty.
				PrevNextVDFDifficulty
			}
	end.

%% @doc Return the cached checkpoints for the given step. Return not_found if
%% none found.
get_step_checkpoints(StepNumber, NextSeed, StartIntervalNumber, NextVDFDifficulty) ->
	SessionKey = {NextSeed, StartIntervalNumber, NextVDFDifficulty},
	get_step_checkpoints(StepNumber, SessionKey).
get_step_checkpoints(StepNumber, SessionKey) ->
	gen_server:call(?MODULE, {get_step_checkpoints, StepNumber, SessionKey}, ?DEFAULT_CALL_TIMEOUT).

%% @doc Return the entropy seed of the given session.
%% Return not_found if the VDF session is not found.
get_seed(SessionKey) ->
	gen_server:call(?MODULE, {get_seed, SessionKey}, ?DEFAULT_CALL_TIMEOUT).

%% @doc Return the active partition upper bound for the given step (chosen among
%% session's upper_bound and next_upper_bound depending on whether the step number has
%% reached the entropy reset point).
%% Return not_found if the VDF session is not found.
get_active_partition_upper_bound(StepNumber, SessionKey) ->
	gen_server:call(?MODULE, {get_active_partition_upper_bound, StepNumber, SessionKey},
			?DEFAULT_CALL_TIMEOUT).

%% @doc Return the steps of the given interval. The steps are chosen
%% according to the protocol. Return not_found if the corresponding hash chain is not
%% computed yet.
get_steps(StartStepNumber, EndStepNumber, NextSeed, NextVDFDifficulty)
		when EndStepNumber > StartStepNumber ->
	SessionKey = session_key(NextSeed, StartStepNumber, NextVDFDifficulty),
	gen_server:call(?MODULE, {get_steps, StartStepNumber, EndStepNumber, SessionKey},
			?DEFAULT_CALL_TIMEOUT).

%% @doc Quickly validate the checkpoints of the latest step.
validate_last_step_checkpoints(#block{ nonce_limiter_info = #nonce_limiter_info{
		global_step_number = StepNumber } },
		#block{ nonce_limiter_info = #nonce_limiter_info{
				global_step_number = StepNumber } }, _PrevOutput) ->
	false;
validate_last_step_checkpoints(#block{
		nonce_limiter_info = #nonce_limiter_info{ output = Output,
				global_step_number = StepNumber, seed = Seed,
				vdf_difficulty = VDFDifficulty,
				last_step_checkpoints = [Output | _] = LastStepCheckpoints } }, PrevB,
				PrevOutput)
		when length(LastStepCheckpoints) == ?VDF_CHECKPOINT_COUNT_IN_STEP ->
	PrevInfo = get_or_init_nonce_limiter_info(PrevB),
	#nonce_limiter_info{ global_step_number = PrevBStepNumber } = PrevInfo,
	SessionKey = session_key(PrevInfo),
	case get_step_checkpoints(StepNumber, SessionKey) of
		LastStepCheckpoints ->
			{true, cache_match};
		not_found ->
			PrevOutput2 = ar_nonce_limiter:maybe_add_entropy(
				PrevOutput, PrevBStepNumber, StepNumber, Seed),
			PrevStepNumber = StepNumber - 1,
			{ok, Config} = arweave_config:get_env(),
			ThreadCount = Config#config.max_nonce_limiter_last_step_validation_thread_count,
			case verify_no_reset(PrevStepNumber, PrevOutput2, 1,
					lists:reverse(LastStepCheckpoints), ThreadCount, VDFDifficulty) of
				{true, _Steps} ->
					true;
				false ->
					false
			end;
		CachedSteps ->
			{false, cache_mismatch, CachedSteps}
	end;
validate_last_step_checkpoints(_B, _PrevB, _PrevOutput) ->
	false.

get_reset_frequency() ->
	?NONCE_LIMITER_RESET_FREQUENCY.

%% @doc Determine whether StepNumber has passed the entropy reset line. If it has return the
%% reset line, otherwise return none.
get_entropy_reset_point(PrevStepNumber, StepNumber) ->
	ResetLine = (PrevStepNumber div ar_nonce_limiter:get_reset_frequency() + 1)
			* ar_nonce_limiter:get_reset_frequency(),
	case ResetLine > StepNumber of
		true ->
			none;
		false ->
			ResetLine
	end.

%% @doc Conditionally add entropy to PrevOutput if the configured number of steps have
%% passed. See ar_nonce_limiter:get_reset_frequency() for more details.
maybe_add_entropy(PrevOutput, PrevStepNumber, StepNumber, Seed) ->
	case get_entropy_reset_point(PrevStepNumber, StepNumber) of
		StepNumber ->
			mix_seed(PrevOutput, Seed);
		_ ->
			PrevOutput
	end.

%% @doc Add entropy to an earlier VDF output to mitigate the impact of a miner with a
%% fast VDF compute. See ar_nonce_limiter:get_reset_frequency() for more details.
mix_seed(PrevOutput, Seed) ->
	SeedH = crypto:hash(sha256, Seed),
	mix_seed2(PrevOutput, SeedH).

mix_seed2(PrevOutput, SeedH) ->
	crypto:hash(sha256, << PrevOutput/binary, SeedH/binary >>).

%% @doc Validate the nonce limiter chain between two blocks in the background.
%% Assume the seeds are correct and the first block is above the second one
%% according to the protocol.
%% Emit {nonce_limiter, {invalid, H, ErrorCode}} or {nonce_limiter, {valid, H}}.
request_validation(H, #nonce_limiter_info{ global_step_number = N },
		#nonce_limiter_info{ global_step_number = N }) ->
	spawn(fun() -> ar_events:send(nonce_limiter, {invalid, H, 1}) end);
request_validation(H, #nonce_limiter_info{ output = Output,
		steps = [Output | _] = StepsToValidate } = Info, PrevInfo) ->
	#nonce_limiter_info{ output = PrevOutput,
			global_step_number = PrevStepNumber, vdf_difficulty = PrevVDFDifficulty } = PrevInfo,
	#nonce_limiter_info{ output = Output, seed = Seed,
			vdf_difficulty = VDFDifficulty, next_vdf_difficulty = NextVDFDifficulty,
			partition_upper_bound = UpperBound,
			next_partition_upper_bound = NextUpperBound, global_step_number = StepNumber,
			steps = StepsToValidate } = Info,
	EntropyResetPoint = get_entropy_reset_point(PrevStepNumber, StepNumber),
	SessionKey = session_key(PrevInfo),
	%% The steps that fall at the intersection of the PrevStepNumber to StepNumber range
	%% and the SessionKey session.
	SessionSteps = gen_server:call(?MODULE, {get_session_steps, PrevStepNumber, StepNumber,
			SessionKey}, ?DEFAULT_CALL_TIMEOUT),
	NextSessionKey = session_key(Info),

	%% We need to validate all the steps from PrevStepNumber to StepNumber:
	%% PrevStepNumber <--------------------------------------------> StepNumber
	%%     PrevOutput x
	%%                                      |----------------------| StepsToValidate
	%%                |-----------------------------------| SessionSteps
	%%                      StartStepNumber x
	%%                          StartOutput x
	%%                                      |-------------| ComputedSteps
	%%                                      --------------> NumAlreadyComputed
	%%                                   StartStepNumber2 x
	%%                                       StartOutput2 x
	%%                                                    |--------| RemainingStepsToValidate
	%%
	{StartStepNumber, StartOutput, ComputedSteps} =
		skip_already_computed_steps(PrevStepNumber, StepNumber, PrevOutput,
			StepsToValidate, SessionSteps),
	?LOG_INFO([{event, vdf_validation_start}, {block, ar_util:encode(H)},
			{session_key, encode_session_key(SessionKey)},
			{next_session_key, encode_session_key(NextSessionKey)},
			{prev_step_number, PrevStepNumber}, {step_number, StepNumber},
			{start_step_number, StartStepNumber},
			{step_count, StepNumber - PrevStepNumber}, {steps, length(StepsToValidate)},
			{session_steps, length(SessionSteps)}, {prev_vdf_difficulty, PrevVDFDifficulty},
			{vdf_difficulty, VDFDifficulty}, {next_vdf_difficulty, NextVDFDifficulty},
			{pid, self()}]),
	case exclude_computed_steps_from_steps_to_validate(
			lists:reverse(StepsToValidate), ComputedSteps) of
		invalid ->
			ErrorID = dump_error({PrevStepNumber, StepNumber, StepsToValidate, SessionSteps}),
			?LOG_WARNING([{event, nonce_limiter_validation_failed},
					{step, exclude_computed_steps_from_steps_to_validate},
					{error_dump, ErrorID}]),
			spawn(fun() -> ar_events:send(nonce_limiter, {invalid, H, 2}) end);

		{[], NumAlreadyComputed} when StartStepNumber + NumAlreadyComputed == StepNumber ->
			%% We've already computed up to StepNumber, so we can use the checkpoints from the
			%% current session
			LastStepCheckpoints = get_step_checkpoints(StepNumber, SessionKey),
			Args = {StepNumber, SessionKey, NextSessionKey, Seed, UpperBound, NextUpperBound,
					VDFDifficulty, NextVDFDifficulty, SessionSteps, LastStepCheckpoints},
			gen_server:cast(?MODULE, {validated_steps, Args}),
			spawn(fun() -> ar_events:send(nonce_limiter, {valid, H}) end);

		{_, NumAlreadyComputed} when StartStepNumber + NumAlreadyComputed >= StepNumber ->
			ErrorID = dump_error({PrevStepNumber, StepNumber, StepsToValidate, SessionSteps,
					StartStepNumber, NumAlreadyComputed}),
			?LOG_WARNING([{event, nonce_limiter_validation_failed},
					{step, exclude_computed_steps_from_steps_to_validate_shift},
					{start_step_number, StartStepNumber}, {shift2, NumAlreadyComputed},
					{error_dump, ErrorID}]),
			spawn(fun() -> ar_events:send(nonce_limiter, {invalid, H, 2}) end);
		{RemainingStepsToValidate, NumAlreadyComputed}
		  		when StartStepNumber + NumAlreadyComputed < StepNumber ->
			case ar_config:use_remote_vdf_server() and not ar_config:compute_own_vdf() of
				true ->
					%% Wait for our VDF server(s) to validate the remaining steps.
					%% Alternatively, the network may abandon this block.
					ar_nonce_limiter_client:maybe_request_sessions(SessionKey),
					spawn(fun() -> ar_events:send(nonce_limiter, {refuse_validation, H}) end);
				false ->
					%% Validate the remaining steps.
					StartOutput2 = case NumAlreadyComputed of
							0 -> StartOutput;
							_ -> lists:nth(NumAlreadyComputed, ComputedSteps)
					end,
					spawn(fun() ->
						StartStepNumber2 = StartStepNumber + NumAlreadyComputed,
						{ok, Config} = arweave_config:get_env(),
						ThreadCount = Config#config.max_nonce_limiter_validation_thread_count,
						Result =
							case is_integer(EntropyResetPoint) andalso
									EntropyResetPoint > StartStepNumber2 of
								true ->
									catch verify(StartStepNumber2, StartOutput2,
											?VDF_CHECKPOINT_COUNT_IN_STEP,
											RemainingStepsToValidate, EntropyResetPoint,
											crypto:hash(sha256, Seed), ThreadCount,
											PrevVDFDifficulty, VDFDifficulty);
								_ ->
									catch verify_no_reset(StartStepNumber2, StartOutput2,
											?VDF_CHECKPOINT_COUNT_IN_STEP,
											RemainingStepsToValidate, ThreadCount,
											VDFDifficulty)
							end,
						case Result of
							{'EXIT', Exc} ->
								ErrorID = dump_error(
									{StartStepNumber2, StartOutput2,
									?VDF_CHECKPOINT_COUNT_IN_STEP,
									RemainingStepsToValidate,
									EntropyResetPoint, crypto:hash(sha256, Seed),
									ThreadCount, VDFDifficulty}),
								?LOG_ERROR([{event, nonce_limiter_validation_failed},
										{block, ar_util:encode(H)},
										{start_step_number, StartStepNumber2},
										{error_id, ErrorID},
										{prev_output, ar_util:encode(StartOutput2)},
										{exception, io_lib:format("~p", [Exc])}]),
								ar_events:send(nonce_limiter, {validation_error, H});
							false ->
								ar_events:send(nonce_limiter, {invalid, H, 3});
							{true, ValidatedSteps} ->
								AllValidatedSteps = ValidatedSteps ++ SessionSteps,
								%% The last_step_checkpoints in Info were validated as part
								%% of an earlier call to
								%% ar_block_pre_validator:pre_validate_nonce_limiter, so
								%% we can trust them here.
								LastStepCheckpoints = get_last_step_checkpoints(Info),
								Args = {StepNumber, SessionKey, NextSessionKey,
										Seed, UpperBound, NextUpperBound,
										VDFDifficulty, NextVDFDifficulty,
										AllValidatedSteps, LastStepCheckpoints},
								gen_server:cast(?MODULE, {validated_steps, Args}),
								ar_events:send(nonce_limiter, {valid, H})
						end
					end)
			end;
		Data ->
			ErrorID = dump_error(Data),
			ar_events:send(nonce_limiter, {validation_error, H}),
			?LOG_ERROR([{event, unexpected_error_during_nonce_limiter_validation},
					{error_id, ErrorID}])
	end;
request_validation(H, _Info, _PrevInfo) ->
	spawn(fun() -> ar_events:send(nonce_limiter, {invalid, H, 4}) end).

get_last_step_checkpoints(Info) ->
	Info#nonce_limiter_info.last_step_checkpoints.

get_or_init_nonce_limiter_info(#block{ height = Height, indep_hash = H } = B) ->
	case Height >= ar_fork:height_2_6() of
		true ->
			B#block.nonce_limiter_info;
		false ->
			{Seed, PartitionUpperBound} =
					ar_node:get_recent_partition_upper_bound_by_prev_h(H),
			get_or_init_nonce_limiter_info(B, Seed, PartitionUpperBound)
	end.

get_or_init_nonce_limiter_info(#block{ height = Height } = B, RecentBI) ->
	case Height >= ar_fork:height_2_6() of
		true ->
			B#block.nonce_limiter_info;
		false ->
			{Seed, PartitionUpperBound, _TXRoot}
					= lists:last(lists:sublist(RecentBI, ?SEARCH_SPACE_UPPER_BOUND_DEPTH)),
			get_or_init_nonce_limiter_info(B, Seed, PartitionUpperBound)
	end.

%% @doc Apply the nonce limiter update provided by the configured trusted peer.
apply_external_update(Update, Peer) ->
	gen_server:call(?MODULE, {apply_external_update, Update, Peer}, ?DEFAULT_CALL_TIMEOUT).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	?LOG_INFO([{event, nonce_limiter_init}]),
	ok = ar_events:subscribe(node_state),
	State =
		case ar_node:is_joined() of
			true ->
				Blocks = get_blocks(),
				handle_initialized(Blocks, #state{});
			_ ->
				#state{}
		end,
	case ar_config:use_remote_vdf_server() and not ar_config:compute_own_vdf() of
		true ->
			gen_server:cast(?MODULE, check_external_vdf_server_input);
		false ->
			ok
	end,
	{ok, start_worker(State#state{ autocompute = ar_config:compute_own_vdf() })}.

get_blocks() ->
	B = ar_node:get_current_block(),
	[B | get_blocks(B#block.previous_block, 1)].

get_blocks(_H, N) when N >= ?STORE_BLOCKS_BEHIND_CURRENT ->
	[];
get_blocks(H, N) ->
	#block{} = B = ar_block_cache:get(block_cache, H),
	[B | get_blocks(B#block.previous_block, N + 1)].

handle_call(get_current_step_number, _From,
		#state{ current_session_key = undefined } = State) ->
	{reply, 0, State};
handle_call(get_current_step_number, _From, State) ->
	#state{ current_session_key = Key } = State,
	#vdf_session{ step_number = StepNumber } = get_session(Key, State),
	{reply, StepNumber, State};

handle_call({get_current_step_number, SessionKey}, _From, State) ->
	case get_session(SessionKey, State) of
		not_found ->
			{reply, not_found, State};
		#vdf_session{ step_number = StepNumber } ->
			{reply, StepNumber, State}
	end;

handle_call({get_latest_step_triplets, SessionKey, N}, _From, State) ->
	case get_session(SessionKey, State) of
		not_found ->
			{reply, [], State};
		#vdf_session{ step_number = StepNumber, steps = Steps,
				step_checkpoints_map = Map,
				upper_bound = UpperBound, next_upper_bound = NextUpperBound } ->
			{_, IntervalNumber, _} = SessionKey,
			IntervalStart = IntervalNumber * ar_nonce_limiter:get_reset_frequency(),
			ResetPoint = get_entropy_reset_point(IntervalStart, StepNumber),
			Triplets = get_triplets(StepNumber, Steps, ResetPoint, UpperBound,
					NextUpperBound, N),
			{Triplets2, NSkipped} = filter_step_triplets_with_checkpoints(Triplets, Map),
			case NSkipped > 0 of
				true ->
					?LOG_INFO([{event, missing_step_checkpoints},
							{count, NSkipped}]);
				false ->
					ok
			end,
			{reply, Triplets2, State}
	end;

handle_call({get_step_checkpoints, StepNumber, SessionKey}, _From, State) ->
	case get_session(SessionKey, State) of
		not_found ->
			{reply, not_found, State};
		#vdf_session{ step_checkpoints_map = Map } ->
			{reply, maps:get(StepNumber, Map, not_found), State}
	end;

handle_call({get_seed, SessionKey}, _From, State) ->
	case get_session(SessionKey, State) of
		not_found ->
			{reply, not_found, State};
		#vdf_session{ seed = Seed } ->
			{reply, Seed, State}
	end;

handle_call({get_active_partition_upper_bound, StepNumber, SessionKey}, _From, State) ->
	case get_session(SessionKey, State) of
		not_found ->
			{reply, not_found, State};
		#vdf_session{ upper_bound = UpperBound, next_upper_bound = NextUpperBound } ->
			{_NextSeed, IntervalNumber, _NextVDFDifficulty} = SessionKey,
			IntervalStart = IntervalNumber * ar_nonce_limiter:get_reset_frequency(),
			UpperBound2 =
				case get_entropy_reset_point(IntervalStart, StepNumber) of
					none ->
						UpperBound;
					_ ->
						NextUpperBound
				end,
			{reply, UpperBound2, State}
	end;

handle_call({get_steps, StartStepNumber, EndStepNumber, SessionKey}, _From, State) ->
	case get_steps2(StartStepNumber, EndStepNumber, SessionKey, State) of
		not_found ->
			{reply, not_found, State};
		Steps ->
			TakeN = min(?NONCE_LIMITER_MAX_CHECKPOINTS_COUNT, EndStepNumber - StartStepNumber),
			{reply, lists:sublist(Steps, TakeN), State}
	end;

%% @doc Get all the steps in the current session that fall between
%% StartStepNumber+1 and EndStepNumber (inclusive)
handle_call({get_session_steps, StartStepNumber, EndStepNumber, SessionKey}, _From, State) ->
	Session = get_session(SessionKey, State),
	{_, Steps} = get_step_range(Session, StartStepNumber + 1, EndStepNumber),
	{reply, Steps, State};

handle_call(get_steps, _From, #state{ current_session_key = undefined } = State) ->
	{reply, [], State};
handle_call(get_steps, _From, State) ->
	#state{ current_session_key = SessionKey } = State,
	#vdf_session{ step_number = StepNumber } = get_session(SessionKey, State),
	{reply, get_steps2(1, StepNumber, SessionKey, State), State};

handle_call({apply_external_update, Update, Peer}, _From, State) ->
	Now = os:system_time(millisecond),
	#nonce_limiter_update{ session_key = SessionKey } = Update,
	%% The client consults the latest session key by peer to decide whether to request the
	%% missing VDF session when we call ar_nonce_limiter_client:maybe_request_sessions/1
	%% during VDF validation.
	gen_server:cast(ar_nonce_limiter_client,
			{update_latest_session_key, Peer, SessionKey}),
	apply_external_update2(Update, State#state{ last_external_update = {Peer, Now} });

handle_call({get_session, SessionKey}, _From, State) ->
	{reply, get_session(SessionKey, State), State};

handle_call(get_current_session, _From, State) ->
	#state{ current_session_key = CurrentSessionKey } = State,
	{reply, {CurrentSessionKey, get_session(CurrentSessionKey, State)}, State};

handle_call(get_current_sessions, _From, State) ->
	#state{ current_session_key = CurrentSessionKey } = State,
	Session = get_session(CurrentSessionKey, State),
	PreviousSessionKey = Session#vdf_session.prev_session_key,
	case get_session(PreviousSessionKey, State) of
		not_found ->
			?LOG_DEBUG([{event, request_current_sessions_missing_previous_session},
					{current_session_key, encode_session_key(CurrentSessionKey)},
					{previous_session_key, encode_session_key(PreviousSessionKey)}]),
			{reply, [{CurrentSessionKey, Session}], State};
		PrevSession ->
			{reply, [{CurrentSessionKey, Session}, {PreviousSessionKey, PrevSession}], State}
	end;

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(check_external_vdf_server_input,
		#state{ last_external_update = {_, 0} } = State) ->
	ar_util:cast_after(1000, ?MODULE, check_external_vdf_server_input),
	{noreply, State};
handle_cast(check_external_vdf_server_input,
		#state{ last_external_update = {_, Time} } = State) ->
	Now = os:system_time(millisecond),
	case Now - Time > 2000 of
		true ->
			?LOG_WARNING([{event, no_message_from_any_vdf_servers},
					{last_message_seconds_ago, (Now - Time) div 1000}]),
			ar_util:cast_after(30000, ?MODULE, check_external_vdf_server_input);
		false ->
			ar_util:cast_after(1000, ?MODULE, check_external_vdf_server_input)
	end,
	{noreply, State};

handle_cast(initialized, State) ->
	gen_server:cast(?MODULE, schedule_step),
	case State#state.emit_initialized_event of
		true ->
			ar_events:send(nonce_limiter, initialized);
		false ->
			ok
	end,
	{noreply, State};

handle_cast({initialize, [PrevB, B | Blocks]}, State) ->
	apply_chain(B#block.nonce_limiter_info, PrevB#block.nonce_limiter_info),
	gen_server:cast(?MODULE, {apply_tip, B, PrevB}),
	gen_server:cast(?MODULE, {initialize, [B | Blocks]}),
	{noreply, State};
handle_cast({initialize, _}, State) ->
	gen_server:cast(?MODULE, initialized),
	{noreply, State};

handle_cast({account_tree_initialized, Blocks}, State) ->
	{noreply, handle_initialized(lists:sublist(Blocks, ?STORE_BLOCKS_BEHIND_CURRENT), State)};

handle_cast({apply_tip, B, PrevB}, State) ->
	{noreply, apply_tip2(B, PrevB, State)};

handle_cast({validated_steps, Args}, State) ->
	{StepNumber, SessionKey, NextSessionKey, Seed, UpperBound, NextUpperBound,
			VDFDifficulty, NextVDFDifficulty, Steps, LastStepCheckpoints} = Args,
	case get_session(SessionKey, State) of
		not_found ->
			%% The corresponding fork origin should have just dropped below the
			%% checkpoint height.
			?LOG_WARNING([{event, session_not_found_for_validated_steps},
					{session_key, encode_session_key(SessionKey)},
					{interval, element(2, SessionKey)},
					{vdf_difficulty, element(3, SessionKey)}]),
			{noreply, State};
		Session ->
			#vdf_session{ step_number = CurrentStepNumber } = Session,
			Session2 =
				case CurrentStepNumber < StepNumber of
					true ->
						%% Update the current Session with all the newly validated steps and
						%% as well as the checkpoints associated with step StepNumber.
						%% This branch occurs when a block is received that is ahead of us
						%% in the VDF chain.
						?LOG_DEBUG([{event, new_vdf_step}, {source, validated_steps},
							{session_key, encode_session_key(SessionKey)},
							{step_number, StepNumber}]),
						{_, Steps2} =
							get_step_range(Steps, StepNumber, CurrentStepNumber + 1, StepNumber),
						update_session(Session, StepNumber,
								#{ StepNumber => LastStepCheckpoints }, Steps2,
								validated_steps);
					false ->
						Session
				end,
			State2 = cache_session(State, SessionKey, Session2),
			State3 = cache_block_session(State2, NextSessionKey, SessionKey,
					#{}, Seed, UpperBound, NextUpperBound, VDFDifficulty, NextVDFDifficulty),
			{noreply, State3}
	end;

handle_cast(schedule_step, #state{ autocompute = false } = State) ->
	{noreply, State#state{ computing = false }};
handle_cast(schedule_step, State) ->
	{noreply, schedule_step(State#state{ computing = true })};

handle_cast(compute_step, State) ->
	{noreply, schedule_step(State)};

handle_cast(reset_and_pause, State) ->
	{noreply, State#state{ autocompute = false, computing = false,
			current_session_key = undefined, sessions = gb_sets:new(), session_by_key = #{} }};

handle_cast(turn_off_initialized_event, State) ->
	{noreply, State#state{ emit_initialized_event = false }};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({event, node_state, {new_tip, B, PrevB}}, State) ->
	{noreply, apply_tip(B, PrevB, State)};

handle_info({event, node_state, {checkpoint_block, _B}},
		#state{ current_session_key = undefined } = State) ->
	%% The server has been restarted after a crash and a base block has not been
	%% applied yet.
	{noreply, State};
handle_info({event, node_state, {checkpoint_block, B}}, State) ->
	case B#block.height < ar_fork:height_2_6() of
		true ->
			{noreply, State};
		false ->
			#state{ sessions = Sessions, session_by_key = SessionByKey,
					current_session_key = CurrentSessionKey } = State,
			StepNumber = ar_block:vdf_step_number(B),
			BaseInterval = StepNumber div ar_nonce_limiter:get_reset_frequency(),
			{Sessions2, SessionByKey2} = prune_old_sessions(Sessions, SessionByKey,
					BaseInterval),
			true = maps:is_key(CurrentSessionKey, SessionByKey2),
			{noreply, State#state{ sessions = Sessions2, session_by_key = SessionByKey2 }}
	end;

handle_info({event, node_state, _}, State) ->
	{noreply, State};

handle_info({'DOWN', Ref, process, _, Reason}, #state{ worker_monitor_ref = Ref } = State) ->
	?LOG_WARNING([{event, nonce_limiter_worker_down},
			{reason, io_lib:format("~p", [Reason])}]),
	{noreply, start_worker(State)};

handle_info({computed, Args}, State) ->
	#state{ current_session_key = CurrentSessionKey } = State,
	{StepNumber, PrevOutput, Output, Checkpoints, SessionKey} = Args,
	Session = get_session(CurrentSessionKey, State),
	#vdf_session{ next_vdf_difficulty = NextVDFDifficulty, steps = [SessionOutput | _] } = Session,
	{NextSeed, IntervalNumber, NextVDFDifficulty} = CurrentSessionKey,
	IntervalStart = IntervalNumber * ar_nonce_limiter:get_reset_frequency(),
	SessionOutput2 = ar_nonce_limiter:maybe_add_entropy(
			SessionOutput, IntervalStart, StepNumber, NextSeed),
	gen_server:cast(?MODULE, schedule_step),
	case {PrevOutput == SessionOutput2, SessionKey == CurrentSessionKey} of
		{true, false} ->
			?LOG_INFO([{event, received_computed_output_for_different_session_key}]),
			{noreply, State};
		{false, _} ->
			case ar_config:use_remote_vdf_server() of
				true ->
					ok;
				false ->
					?LOG_WARNING([{event, computed_for_outdated_key}, {step_number, StepNumber},
						{output, ar_util:encode(Output)},
						{prev_output, ar_util:encode(PrevOutput)},
						{session_output, ar_util:encode(SessionOutput2)},
						{current_session_key, encode_session_key(CurrentSessionKey)},
						{session_key, encode_session_key(SessionKey)}])
			end,
			{noreply, State};
		{true, true} ->
			Session2 = update_session(Session, StepNumber,
					#{ StepNumber => Checkpoints }, [Output], computed_step),
			State2 = cache_session(State, CurrentSessionKey, Session2),
			?LOG_DEBUG([{event, new_vdf_step}, {source, computed},
				{session_key, encode_session_key(CurrentSessionKey)}, {step_number, StepNumber}]),
			send_output(CurrentSessionKey, Session2),
			{noreply, State2}
	end;

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(Reason, #state{ worker = W }) ->
	W ! stop,
	?LOG_INFO([{module, ?MODULE},{pid, self()},{callback, terminate},{reason, Reason}]),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

session_key(NextSeed, StepNumber, NextVDFDifficulty) ->
	{NextSeed, StepNumber div ar_nonce_limiter:get_reset_frequency(), NextVDFDifficulty}.

get_session(SessionKey, #state{ session_by_key = SessionByKey }) ->
	maps:get(SessionKey, SessionByKey, not_found).

update_session(Session, StepNumber, StepCheckpointsMap, Steps, Source) ->
	#vdf_session{ step_checkpoints_map = Map } = Session,
	case find_step_checkpoints_mismatch(StepCheckpointsMap, Map) of
		{true, MismatchStepNumber} ->
			?LOG_ERROR([{event, step_checkpoints_mismatch},
					{step_number, StepNumber},
					{mismatch_step_number, MismatchStepNumber},
					{source, Source}]);
		false ->
			false
	end,
	Map2 = maps:merge(StepCheckpointsMap, Map),
	update_session(Session#vdf_session{ step_checkpoints_map = Map2 }, StepNumber, Steps).

find_step_checkpoints_mismatch(StepCheckpointsMap, Map) ->
	maps:fold(fun(StepNumber, Checkpoints, Acc) ->
		case maps:get(StepNumber, Map, not_found) of
			not_found ->
				Acc;
			Checkpoints ->
				Acc;
			_Checkpoints2 ->
				{true, StepNumber}
		end
	end, false, StepCheckpointsMap).

update_session(Session, StepNumber, Steps) ->
	#vdf_session{ steps = CurrentSteps } = Session,
	Session#vdf_session{ step_number = StepNumber, steps = Steps ++ CurrentSteps }.

send_output(SessionKey, Session) ->
	{_, IntervalNumber, _} = SessionKey,
	#vdf_session{ step_number = StepNumber, steps = [Output | _] } = Session,
	IntervalStart = IntervalNumber * ar_nonce_limiter:get_reset_frequency(),
	UpperBound =
		case get_entropy_reset_point(IntervalStart, StepNumber) of
			none ->
				Session#vdf_session.upper_bound;
			_ ->
				Session#vdf_session.next_upper_bound
		end,
	ar_events:send(nonce_limiter, {computed_output, {SessionKey, StepNumber, Output, UpperBound}}).

dump_error(Data) ->
	{ok, Config} = arweave_config:get_env(),
	ErrorID = binary_to_list(ar_util:encode(crypto:strong_rand_bytes(8))),
	ErrorDumpFile = filename:join(Config#config.data_dir, "error_dump_" ++ ErrorID),
	file:write_file(ErrorDumpFile, term_to_binary(Data)),
	ErrorID.

%% @doc
%% PrevStepNumber <------------------------------------------------------> StepNumber
%%     PrevOutput x
%%                                                |----------------------| StepsToValidate
%%                |-------------------------------| NumStepsBefore
%%                |---------------------------------------------| SessionSteps
%%
skip_already_computed_steps(PrevStepNumber, StepNumber, PrevOutput, StepsToValidate,
		SessionSteps) ->
	ComputedSteps = lists:reverse(SessionSteps),
	%% Number of steps in the PrevStepNumber to StepNumber range that fall before the
	%% beginning of the StepsToValidate list. To avoid computing these steps we will look for
	%% them in the current VDF session (i.e. in the SessionSteps list)
	NumStepsBefore = StepNumber - PrevStepNumber - length(StepsToValidate),
	case NumStepsBefore > 0 andalso length(ComputedSteps) >= NumStepsBefore of
		false ->
			{PrevStepNumber, PrevOutput, ComputedSteps};
		true ->
			{
				PrevStepNumber + NumStepsBefore,
				lists:nth(NumStepsBefore, ComputedSteps),
				lists:nthtail(NumStepsBefore, ComputedSteps)
			}
	end.

exclude_computed_steps_from_steps_to_validate(StepsToValidate, ComputedSteps) ->
	exclude_computed_steps_from_steps_to_validate(StepsToValidate, ComputedSteps, 1, 0).

exclude_computed_steps_from_steps_to_validate(StepsToValidate, [], _I, NumAlreadyComputed) ->
	{StepsToValidate, NumAlreadyComputed};
exclude_computed_steps_from_steps_to_validate(StepsToValidate, [_Step | ComputedSteps], I,
		NumAlreadyComputed) when I /= 1 ->
	exclude_computed_steps_from_steps_to_validate(StepsToValidate, ComputedSteps, I + 1,
			NumAlreadyComputed);
exclude_computed_steps_from_steps_to_validate([Step], [Step | _ComputedSteps], _I,
			NumAlreadyComputed) ->
	{[], NumAlreadyComputed + 1};
exclude_computed_steps_from_steps_to_validate([Step | StepsToValidate], [Step | ComputedSteps],
		_I, NumAlreadyComputed) ->
	exclude_computed_steps_from_steps_to_validate(StepsToValidate, ComputedSteps, 1,
		NumAlreadyComputed + 1);
exclude_computed_steps_from_steps_to_validate(_StepsToValidate, _ComputedSteps, _I,
		_NumAlreadyComputed) ->
	invalid.

handle_initialized([B | Blocks], State) ->
	?LOG_INFO([{event, handle_initialized},
		{module, ar_nonce_limiter}, {blocks, length([B | Blocks])}]),
	Blocks2 = take_blocks_after_fork([B | Blocks]),
	handle_initialized2(lists:reverse(Blocks2), State).

take_blocks_after_fork([#block{ height = Height } = B | Blocks]) ->
	case Height + 1 >= ar_fork:height_2_6() of
		true ->
			[B | take_blocks_after_fork(Blocks)];
		false ->
			[]
	end;
take_blocks_after_fork([]) ->
	[].

handle_initialized2([B | Blocks], State) ->
	State2 = apply_base_block(B, State),
	gen_server:cast(?MODULE, {initialize, [B | Blocks]}),
	State2.

apply_base_block(B, State) ->
	#nonce_limiter_info{ seed = Seed, output = Output,
			partition_upper_bound = UpperBound,
			next_partition_upper_bound = NextUpperBound,
			global_step_number = StepNumber,
			last_step_checkpoints = LastStepCheckpoints,
			vdf_difficulty = VDFDifficulty,
			next_vdf_difficulty = NextVDFDifficulty } = B#block.nonce_limiter_info,
	Session = #vdf_session{
			seed = Seed, step_number = StepNumber,
			upper_bound = UpperBound, next_upper_bound = NextUpperBound,
			vdf_difficulty = VDFDifficulty, next_vdf_difficulty = NextVDFDifficulty ,
			step_checkpoints_map = #{ StepNumber => LastStepCheckpoints },
			steps = [Output] },
	SessionKey = session_key(B#block.nonce_limiter_info),
	?LOG_DEBUG([{event, new_vdf_step}, {source, base_block},
		{session_key, encode_session_key(SessionKey)}, {step_number, StepNumber}]),
	State2 = set_current_session(State, SessionKey),
	cache_session(State2, SessionKey, Session).

apply_chain(#nonce_limiter_info{ global_step_number = StepNumber },
		#nonce_limiter_info{ global_step_number = PrevStepNumber })
		when StepNumber - PrevStepNumber > ?NONCE_LIMITER_MAX_CHECKPOINTS_COUNT ->
	ar:console("Cannot do a trusted join - there are not enough checkpoints"
			" to apply quickly; step number: ~B, previous step number: ~B.",
			[StepNumber, PrevStepNumber]),
	timer:sleep(1000),
	init:stop(1);
%% @doc Apply the pre-validated / trusted nonce_limiter_info. Since the info is trusted
%% we don't validate it here.
apply_chain(Info, PrevInfo) ->
	#nonce_limiter_info{ global_step_number = PrevStepNumber } = PrevInfo,
	#nonce_limiter_info{ output = Output, seed = Seed,
			vdf_difficulty = VDFDifficulty,
			next_vdf_difficulty = NextVDFDifficulty,
			partition_upper_bound = UpperBound,
			next_partition_upper_bound = NextUpperBound, global_step_number = StepNumber,
			steps = Steps, last_step_checkpoints = LastStepCheckpoints } = Info,
	Count = StepNumber - PrevStepNumber,
	Output = hd(Steps),
	Count = length(Steps),
	SessionKey = session_key(PrevInfo),
	NextSessionKey = session_key(Info),
	Args = {StepNumber, SessionKey, NextSessionKey, Seed, UpperBound, NextUpperBound,
			VDFDifficulty, NextVDFDifficulty, Steps, LastStepCheckpoints},
	gen_server:cast(?MODULE, {validated_steps, Args}).

apply_tip(#block{ height = Height } = B, PrevB, #state{ sessions = Sessions } = State) ->
	case Height + 1 < ar_fork:height_2_6() of
		true ->
			State;
		false ->
			State2 =
				case State#state.computing of
					false ->
						gen_server:cast(?MODULE, schedule_step),
						State#state{ computing = true };
					true ->
						State
				end,
			case gb_sets:is_empty(Sessions) of
				true ->
					true = (Height + 1) == ar_fork:height_2_6(),
					State3 = apply_base_block(B, State2),
					State3;
				false ->
					apply_tip2(B, PrevB, State2)
			end
	end.

apply_tip2(B, PrevB, State) ->
	#nonce_limiter_info{ seed = Seed, partition_upper_bound = UpperBound,
			next_partition_upper_bound = NextUpperBound, global_step_number = StepNumber,
			last_step_checkpoints = LastStepCheckpoints,
			vdf_difficulty = VDFDifficulty,
			next_vdf_difficulty = NextVDFDifficulty } = B#block.nonce_limiter_info,
	SessionKey = session_key(B#block.nonce_limiter_info),
	PrevSessionKey = session_key(PrevB#block.nonce_limiter_info),
	State2 = set_current_session(State, SessionKey),
	State3 = cache_block_session(State2, SessionKey, PrevSessionKey,
			#{ StepNumber => LastStepCheckpoints }, Seed, UpperBound, NextUpperBound,
			VDFDifficulty, NextVDFDifficulty),
	State3.

prune_old_sessions(Sessions, SessionByKey, BaseInterval) ->
	{{Interval, NextSeed, NextVdfDifficulty}, Sessions2} = gb_sets:take_smallest(Sessions),
	SessionKey = {NextSeed, Interval, NextVdfDifficulty},
	case BaseInterval > Interval + 10 of
		true ->
			?LOG_DEBUG([{event, prune_old_vdf_session},
				{session_key, encode_session_key(SessionKey)}]),
			SessionByKey2 = maps:remove(SessionKey, SessionByKey),
			prune_old_sessions(Sessions2, SessionByKey2, BaseInterval);
		false ->
			{Sessions, SessionByKey}
	end.

start_worker(State) ->
	Worker = spawn(fun() -> process_flag(priority, high), worker() end),
	Ref = monitor(process, Worker),
	State#state{ worker = Worker, worker_monitor_ref = Ref }.

compute(StepNumber, PrevOutput, VDFDifficulty) ->
	{ok, Output, Checkpoints} = ar_vdf:compute2(StepNumber, PrevOutput, VDFDifficulty),
	debug_double_check(
		"compute",
		{ok, Output, Checkpoints},
		fun ar_vdf:compute_legacy/3,
		[StepNumber, PrevOutput, VDFDifficulty]).

verify(StartStepNumber, PrevOutput, NumCheckpointsBetweenHashes, Hashes, ResetStepNumber,
		ResetSeed, ThreadCount, VDFDifficulty, NextVDFDifficulty) ->
	{Result1, PrevOutput2, ValidatedSteps1} =
		case lists:sublist(Hashes, ResetStepNumber - StartStepNumber - 1) of
			[] ->
				{true, mix_seed2(PrevOutput, ResetSeed), []};
			Hashes1 ->
				case verify_no_reset(StartStepNumber, PrevOutput,
						NumCheckpointsBetweenHashes, Hashes1, ThreadCount, VDFDifficulty) of
					{true, ValidatedSteps} ->
						{true, mix_seed2(hd(ValidatedSteps), ResetSeed), ValidatedSteps};
					false ->
						{false, undefined, undefined}
				end
		end,
	case Result1 of
		false ->
			false;
		true ->
			Hashes2 = lists:nthtail(ResetStepNumber - StartStepNumber - 1, Hashes),
			case verify_no_reset(ResetStepNumber - 1, PrevOutput2, NumCheckpointsBetweenHashes,
					Hashes2, ThreadCount, NextVDFDifficulty) of
				{true, ValidatedSteps2} ->
					{true, ValidatedSteps2 ++ ValidatedSteps1};
				false ->
					false
			end
	end.

verify_no_reset(StartStepNumber, PrevOutput, NumCheckpointsBetweenHashes, Hashes, ThreadCount,
		VDFDifficulty) ->
	Garbage = crypto:strong_rand_bytes(32),
	Result = ar_vdf:verify2(StartStepNumber, PrevOutput, NumCheckpointsBetweenHashes, Hashes,
			0, Garbage, ThreadCount, VDFDifficulty),
	debug_double_check(
		"verify_no_reset",
		Result,
		fun ar_vdf:debug_sha_verify_no_reset/6,
		[StartStepNumber, PrevOutput, NumCheckpointsBetweenHashes, Hashes, ThreadCount,
				VDFDifficulty]).

worker() ->
	receive
		{compute, {StepNumber, PrevOutput, VDFDifficulty, SessionKey}, From} ->
			{ok, Output, Checkpoints} = prometheus_histogram:observe_duration(
					vdf_step_time_milliseconds, [], fun() -> compute(StepNumber, PrevOutput,
							VDFDifficulty) end),
			From ! {computed, {StepNumber, PrevOutput, Output, Checkpoints, SessionKey}},
			worker();
		stop ->
			ok
	end.

%% @doc Get all the steps that fall between StartStepNumber and EndStepNumber, traversing
%% multiple sessions if needed.
get_steps2(StartStepNumber, EndStepNumber, SessionKey, State) ->
	case get_session(SessionKey, State) of
		#vdf_session{ step_number = StepNumber, prev_session_key = PrevSessionKey } = Session
				when StepNumber >= EndStepNumber ->
			%% Get the steps within the current session that fall within the StartStepNumber+1
			%% and EndStepNumber (inclusive) range.
			{_, Steps} = get_step_range(Session, StartStepNumber + 1, EndStepNumber),
			TotalCount = EndStepNumber - StartStepNumber - 1,
			Count = length(Steps),
			%% If we haven't found all the steps, recurse into the previous session.
			case TotalCount > Count of
				true ->
					case get_steps2(StartStepNumber, EndStepNumber - Count,
							PrevSessionKey, State) of
						not_found ->
							not_found;
						PrevSteps ->
							Steps ++ PrevSteps
					end;
				false ->
					Steps
			end;
		_ ->
			not_found
	end.

schedule_step(State) ->
	#state{ current_session_key = {NextSeed, IntervalNumber, NextVDFDifficulty} = Key,
			worker = Worker } = State,
	#vdf_session{ step_number = PrevStepNumber,
		vdf_difficulty = VDFDifficulty, next_vdf_difficulty = NextVDFDifficulty,
		steps = Steps } = get_session(Key, State),
	PrevOutput = hd(Steps),
	StepNumber = PrevStepNumber + 1,
	IntervalStart = IntervalNumber * ar_nonce_limiter:get_reset_frequency(),
	PrevOutput2 = ar_nonce_limiter:maybe_add_entropy(
		PrevOutput, IntervalStart, StepNumber, NextSeed),
	VDFDifficulty2 =
		case get_entropy_reset_point(IntervalStart, StepNumber) of
			none ->
				VDFDifficulty;
			_ ->
				?LOG_DEBUG([{event, entropy_reset_point_found}, {step_number, StepNumber},
					{interval_start, IntervalStart}, {vdf_difficulty, VDFDifficulty},
					{next_vdf_difficulty, NextVDFDifficulty},
					{session_key, encode_session_key(Key)}]),
				NextVDFDifficulty
		end,
	Worker ! {compute, {StepNumber, PrevOutput2, VDFDifficulty2, Key}, self()},
	State.

get_or_init_nonce_limiter_info(#block{ height = Height } = B, Seed, PartitionUpperBound) ->
	NextSeed = B#block.indep_hash,
	NextPartitionUpperBound = B#block.weave_size,
	case Height + 1 == ar_fork:height_2_6() of
		true ->
			Output = crypto:hash(sha256, Seed),
			#nonce_limiter_info{ output = Output, seed = Seed, next_seed = NextSeed,
					partition_upper_bound = PartitionUpperBound,
					next_partition_upper_bound = NextPartitionUpperBound };
		false ->
			undefined
	end.

apply_external_update2(Update, State) ->
	#state{ last_external_update = {Peer, _} } = State,
	#nonce_limiter_update{ session_key = SessionKey,
			session = #vdf_session{
				step_number = StepNumber }
			} = Update,
	case get_session(SessionKey, State) of
		not_found ->
			apply_external_update_session_not_found(Update, State);
		#vdf_session{ step_number = CurrentStepNumber } = CurrentSession ->
			case CurrentStepNumber >= StepNumber of
				true ->
					%% Inform the peer we are ahead.
					case CurrentStepNumber > StepNumber of
						true ->
							?LOG_DEBUG([{event, apply_external_vdf},
									{result, ahead_of_server},
									{vdf_server, ar_util:format_peer(Peer)},
									{session_key, encode_session_key(SessionKey)},
									{client_step_number, CurrentStepNumber},
									{server_step_number, StepNumber}]);
						false ->
							ok
					end,
					{reply, #nonce_limiter_update_response{
							step_number = CurrentStepNumber }, State};
				false ->
					apply_external_update3(Update, CurrentSession, State)
			end
	end.

apply_external_update_session_not_found(Update, State) ->
	#state{ last_external_update = {Peer, _} } = State,
	#nonce_limiter_update{ session_key = SessionKey,
			session = #vdf_session{
				prev_session_key = PrevSessionKey, step_number = StepNumber } = Session,
			is_partial = IsPartial } = Update,
	{_SessionSeed, SessionInterval, _SessionVDFDifficulty} = SessionKey,
	case IsPartial of
		true ->
			%% Inform the peer we have not initialized the corresponding session yet.
			?LOG_DEBUG([{event, apply_external_vdf},
				{result, session_not_found},
				{vdf_server, ar_util:format_peer(Peer)},
				{is_partial, IsPartial},
				{session_key, encode_session_key(SessionKey)},
				{server_step_number, StepNumber}]),
			{reply, #nonce_limiter_update_response{ session_found = false }, State};
		false ->
			%% Handle the case where The VDF server has processed a block and re-allocated
			%% steps from the previous session to the new session. In this case we only
			%% want to apply new steps - steps in Session that weren't already applied as
			%% part of PrevSession.

			%% Start after the last step of the previous session
			RangeStart = case get_session(PrevSessionKey, State) of
				not_found -> 0;
				PrevSession -> PrevSession#vdf_session.step_number + 1
			end,
			%% But start no later than the beginning of the session 2 after PrevSession.
			%% This is because the steps in that session - which may have been previously
			%% computed - have now been invalidated.
			NextSessionStart = (SessionInterval + 1) * ar_nonce_limiter:get_reset_frequency(),
			{_, Steps} = get_step_range(Session,
					min(RangeStart, NextSessionStart), StepNumber),
			State2 = apply_external_update4(State, SessionKey, Session, Steps),
			{reply, ok, State2}
	end.

apply_external_update3(Update, CurrentSession, State) ->
	#state{ last_external_update = {Peer, _} } = State,
	#nonce_limiter_update{ session_key = SessionKey,
			session = #vdf_session{
				step_checkpoints_map = StepCheckpointsMap,
				step_number = StepNumber,
				steps = Steps } = Session,
			is_partial = IsPartial } = Update,
	#vdf_session{ step_number = CurrentStepNumber } = CurrentSession,
	%% CurrentStepNumber < StepNumber by construction.
	StepCount = length(Steps),
	StartStepNumber = StepNumber - StepCount,
	case CurrentStepNumber >= StartStepNumber of
		true ->
			Steps2 = lists:sublist(Steps,
					StepNumber - max(CurrentStepNumber, StartStepNumber)),
			CurrentSession2 = update_session(CurrentSession, StepNumber,
					StepCheckpointsMap, Steps2, apply_external_update),
			State2 = apply_external_update4(State, SessionKey, CurrentSession2, Steps2),
			{reply, ok, State2};
		false ->
			case IsPartial of
				true ->
					%% Inform the peer we miss some steps.
					?LOG_DEBUG([{event, apply_external_vdf},
							{result, missing_steps},
							{vdf_server, ar_util:format_peer(Peer)},
							{is_partial, IsPartial},
							{session_key, encode_session_key(SessionKey)},
							{client_step_number, CurrentStepNumber},
							{server_step_number, StepNumber}]),
					{reply, #nonce_limiter_update_response{
							step_number = CurrentStepNumber }, State};
				false ->
					%% Handle the case where the VDF client has dropped of the
					%% network briefly and the VDF server has advanced several
					%% steps within the same session. In this case the client has
					%% noticed the gap and requested the full VDF session be sent -
					%% which may contain previously processed steps in a addition to
					%% the missing ones.
					%%
					%% To avoid processing those steps twice, the client grabs
					%% CurrentStepNumber (our most recently processed step number)
					%% and ignores it and any lower steps found in Session.
					{_, Steps} = get_step_range(Session, CurrentStepNumber + 1, StepNumber),
					State2 = apply_external_update4(State, SessionKey, Session, Steps),
					{reply, ok, State2}
			end
	end.

%% Note: we do not take the VDF steps from Session but accept them separately in Steps,
%% where only unique steps are included to ensure that VDF steps are only processed once.
%% In the first place, it is important for avoiding extra mining work.
apply_external_update4(State, SessionKey, Session, Steps) ->
	#state{ last_external_update = {Peer, _} } = State,

	?LOG_DEBUG([{event, new_vdf_step}, {source, apply_external_vdf},
			{vdf_server, ar_util:format_peer(Peer)},
			{session_key, encode_session_key(SessionKey)},
			{step_number, Session#vdf_session.step_number},
			{length, length(Steps)}]),

	State2 = cache_session(State, SessionKey, Session),
	send_events_for_external_update(SessionKey, Session#vdf_session{ steps = Steps }),
	State2.

%% @doc Returns a sub-range of steps out of a larger list of steps. This is
%% primarily used to manage "overflow" steps.
%%
%% Between blocks nodes will add all computed VDF steps to the same session -
%% *even if* the new steps have crossed the entropy reset line and therefore
%% could be added to a new session (i.e. "overflow steps"). Once a block is
%% processed the node will open a new session and re-allocate all the steps past
%% the entropy reset line to that new session. However, any steps that have crossed
%% *TWO* entropy reset lines are no longer valid (the seed they were generated with
%% has changed with the arrival of a new block)
%%
%% Note: This overlap in session caching is intentional. The intention is to
%% quickly access the steps when validating B1 -> reset line -> B2 given the
%% current fork of B1 -> B2' -> reset line -> B3 i.e. we can query all steps by
%% B1.next_seed even though on our fork the reset line determined a different
%% next_seed for the latest session.
get_step_range_from_interval(Session, SessionInterval, ResetFrequency) ->
	SessionStart = SessionInterval * ResetFrequency,
	SessionEnd = (SessionInterval + 1) * ResetFrequency - 1,
	get_step_range(Session, SessionStart, SessionEnd).

get_step_range(not_found, _RangeStart, _RangeEnd) ->
	{0, []};
get_step_range(Session, RangeStart, RangeEnd) ->
	#vdf_session{ step_number = StepNumber, steps = Steps } = Session,
	get_step_range(Steps, StepNumber, RangeStart, RangeEnd).

get_step_range([], _StepNumber, _RangeStart, _RangeEnd) ->
	{0, []};
get_step_range(_Steps, _StepNumber, RangeStart, RangeEnd)
		when RangeStart > RangeEnd ->
	{0, []};
get_step_range(_Steps, StepNumber, RangeStart, _RangeEnd)
		when StepNumber < RangeStart ->
	{0, []};
get_step_range(Steps, StepNumber, _RangeStart, RangeEnd)
		when StepNumber - length(Steps) + 1 > RangeEnd ->
	{0, []};
get_step_range(Steps, StepNumber, RangeStart, RangeEnd) ->
	%% Clip RangeStart to the earliest step number in Steps
	RangeStart2 = max(RangeStart, StepNumber - length(Steps) + 1),
	RangeSteps =
		case StepNumber > RangeEnd of
			true ->
				%% Exclude steps beyond the end of the session
				lists:nthtail(StepNumber - RangeEnd, Steps);
			false ->
				Steps
		end,
	%% The highest step number in the range
	RangeEnd2 = min(StepNumber, RangeEnd),
	%% Exclude the steps before the start of the session
	RangeSteps2 = lists:sublist(RangeSteps, RangeEnd2 - RangeStart2 + 1),
	{RangeEnd2, RangeSteps2}.

set_current_session(State, SessionKey) ->
	?LOG_DEBUG([{event, set_current_session},
		{new_session_key, encode_session_key(SessionKey)},
		{old_session_key, encode_session_key(State#state.current_session_key)}]),
	State#state{ current_session_key = SessionKey }.

%% @doc Update the VDF session cache based on new info from a validated block.
cache_block_session(State, SessionKey, PrevSessionKey, StepCheckpointsMap, Seed,
		UpperBound, NextUpperBound, VDFDifficulty, NextVDFDifficulty) ->
	Session =
		case get_session(SessionKey, State) of
			not_found ->
				{_, Interval, NextVDFDifficulty} = SessionKey,
				PrevSession = get_session(PrevSessionKey, State),
				{StepNumber, Steps} = get_step_range_from_interval(
					PrevSession, Interval, ar_nonce_limiter:get_reset_frequency()),
				?LOG_DEBUG([{event, new_vdf_step}, {source, block},
					{session_key, encode_session_key(SessionKey)}, {step_number, StepNumber}]),
				#vdf_session{ step_number = StepNumber, seed = Seed,
						upper_bound = UpperBound, next_upper_bound = NextUpperBound,
						prev_session_key = PrevSessionKey,
						vdf_difficulty = VDFDifficulty, next_vdf_difficulty = NextVDFDifficulty,
						step_checkpoints_map = StepCheckpointsMap,
						steps = Steps };
			ExistingSession ->
				ExistingSession
		end,
	cache_session(State, SessionKey, Session).

cache_session(State, SessionKey, Session) ->
	#state{ current_session_key = CurrentSessionKey, session_by_key = SessionByKey,
		sessions = Sessions } = State,
	{NextSeed, Interval, NextVDFDifficulty} = SessionKey,
	maybe_set_vdf_metrics(SessionKey, CurrentSessionKey, Session),
	SessionByKey2 = maps:put(SessionKey, Session, SessionByKey),
	%% If Session exists, then {Interval, NextSeed} will already exist in the Sessions set and
	%% gb_sets:add_element will not cause a change.
	Sessions2 = gb_sets:add_element({Interval, NextSeed, NextVDFDifficulty}, Sessions),
	State#state{ sessions = Sessions2, session_by_key = SessionByKey2 }.

maybe_set_vdf_metrics(SessionKey, CurrentSessionKey, Session) ->
	case SessionKey == CurrentSessionKey of
		true ->
			#vdf_session{
				step_number = StepNumber,
				vdf_difficulty = VDFDifficulty,
				next_vdf_difficulty = NextVDFDifficulty } = Session,
			prometheus_gauge:set(vdf_step, StepNumber),
			prometheus_gauge:set(vdf_difficulty, [current], VDFDifficulty),
			prometheus_gauge:set(vdf_difficulty, [next], NextVDFDifficulty);
		false ->
			ok
	end.

send_events_for_external_update(_SessionKey, #vdf_session{ steps = [] }) ->
	ok;
send_events_for_external_update(SessionKey, Session) ->
	send_output(SessionKey, Session),
	#vdf_session{ step_number = StepNumber, steps = [_ | RemainingSteps] } = Session,
	send_events_for_external_update(SessionKey,
		Session#vdf_session{ step_number = StepNumber-1, steps = RemainingSteps }).

debug_double_check(Label, Result, Func, Args) ->
	{ok, Config} = arweave_config:get_env(),
	case lists:member(double_check_nonce_limiter, Config#config.enable) of
		false ->
			Result;
		true ->
			Check = apply(Func, Args),
			case Result == Check of
				true ->
					Result;
				false ->
					ID = ar_util:encode(crypto:strong_rand_bytes(16)),
					file:write_file(Label ++ "_" ++ binary_to_list(ID),
							term_to_binary(Args)),
					Event = "nonce_limiter_" ++ Label ++ "_mismatch",
					?LOG_ERROR([{event, list_to_atom(Event)}, {report_id, ID}]),
					Result
			end
	end.

filter_step_triplets([], _LowerBounds) ->
	[];
filter_step_triplets([{O, _, _} = Triplet | Triplets], LowerBounds) ->
	case lists:member(O, LowerBounds) of
		true ->
			[];
		false ->
			[Triplet | filter_step_triplets(Triplets, LowerBounds)]
	end.

get_triplets(_StepNumber, _Steps, _ResetPoint, _UpperBound, _NextUpperBound, 0) ->
	[];
get_triplets(_StepNumber, [], _ResetPoint, _UpperBound, _NextUpperBound, _N) ->
	[];
get_triplets(StepNumber, [Step | Steps], ResetPoint, UpperBound, NextUpperBound, N) ->
	U =
		case ResetPoint of
			none ->
				UpperBound;
			_ when StepNumber >= ResetPoint ->
				NextUpperBound;
			_ ->
				UpperBound
		end,
	[{Step, StepNumber, U}
		| get_triplets(StepNumber - 1, Steps, ResetPoint, UpperBound, NextUpperBound, N - 1)].

filter_step_triplets_with_checkpoints([], _Map) ->
	{[], 0};
filter_step_triplets_with_checkpoints([{_, StepNumber, _} = Triplet | Triplets], Map) ->
	{List, NSkipped} = filter_step_triplets_with_checkpoints(Triplets, Map),
	case maps:is_key(StepNumber, Map) of
		true ->
			{[Triplet | List], NSkipped};
		false ->
			{List, NSkipped + 1}
	end.

%%%===================================================================
%%% Tests.
%%%===================================================================


exclude_computed_steps_from_steps_to_validate_test() ->
	C1 = crypto:strong_rand_bytes(32),
	C2 = crypto:strong_rand_bytes(32),
	C3 = crypto:strong_rand_bytes(32),
	C4 = crypto:strong_rand_bytes(32),
	C5 = crypto:strong_rand_bytes(32),
	Cases = [
		{{[], []}, {[], 0}, "Case 1"},
		{{[C1], []}, {[C1], 0}, "Case 2"},
		{{[C1], [C1]}, {[], 1}, "Case 3"},
		{{[C1, C2], []}, {[C1, C2], 0}, "Case 4"},
		{{[C1, C2], [C2]}, invalid, "Case 5"},
		{{[C1, C2], [C1]}, {[C2], 1}, "Case 6"},
		{{[C1, C2], [C2, C1]}, invalid, "Case 7"},
		{{[C1, C2], [C1, C2]}, {[], 2}, "Case 8"},
		{{[C1, C2], [C1, C2, C3, C4, C5]}, {[], 2}, "Case 9"}
	],
	test_exclude_computed_steps_from_steps_to_validate(Cases).

test_exclude_computed_steps_from_steps_to_validate([Case | Cases]) ->
	{Input, Expected, Title} = Case,
	{StepsToValidate, ComputedSteps} = Input,
	Got = exclude_computed_steps_from_steps_to_validate(StepsToValidate, ComputedSteps),
	?assertEqual(Expected, Got, Title),
	test_exclude_computed_steps_from_steps_to_validate(Cases);
test_exclude_computed_steps_from_steps_to_validate([]) ->
	ok.

get_entropy_reset_point_test() ->
	ResetFreq = ar_nonce_limiter:get_reset_frequency(),
	?assertEqual(none, get_entropy_reset_point(1, ResetFreq - 1)),
	?assertEqual(ResetFreq, get_entropy_reset_point(1, ResetFreq)),
	?assertEqual(none, get_entropy_reset_point(ResetFreq, ResetFreq + 1)),
	?assertEqual(2 * ResetFreq, get_entropy_reset_point(ResetFreq, ResetFreq * 2)),
	?assertEqual(ResetFreq * 3, get_entropy_reset_point(ResetFreq * 3 - 1, ResetFreq * 3 + 2)),
	?assertEqual(ResetFreq * 4, get_entropy_reset_point(ResetFreq * 3, ResetFreq * 4 + 1)).

reorg_after_join_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun test_reorg_after_join/0}.

test_reorg_after_join() ->
	[B0] = ar_weave:init(),
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:connect_to_peer(peer1),
	ar_test_node:mine(),
	ar_test_node:assert_wait_until_height(peer1, 1),
	ar_test_node:disconnect_from(peer1),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:join_on(#{ node => main, join_on => peer1 }),
	ar_test_node:mine(peer1),
	ar_test_node:assert_wait_until_height(peer1, 1),
	ar_test_node:mine(peer1),
	ar_test_node:wait_until_height(main, 2).

reorg_after_join2_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun test_reorg_after_join2/0}.

test_reorg_after_join2() ->
	[B0] = ar_weave:init(),
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:connect_to_peer(peer1),
	ar_test_node:mine(),
	ar_test_node:assert_wait_until_height(peer1, 1),
	ar_test_node:join_on(#{ node => main, join_on => peer1 }),
	ar_test_node:mine(),
	ar_test_node:wait_until_height(main, 2),
	ar_test_node:disconnect_from(peer1),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:mine(peer1),
	ar_test_node:assert_wait_until_height(peer1, 1),
	ar_test_node:mine(peer1),
	ar_test_node:assert_wait_until_height(peer1, 2),
	ar_test_node:connect_to_peer(peer1),
	ar_test_node:mine(peer1),
	ar_test_node:wait_until_height(main, 3).

get_step_range_test() ->
	?assertEqual(
		{0, []},
		get_step_range(lists:seq(9, 5, -1), 9, 0, 4),
		"Disjoint range A"
	),
	?assertEqual(
		{0, []},
		get_step_range(lists:seq(9, 5, -1), 9 , 10, 14),
		"Disjoint range B"
	),
	?assertEqual(
		{0, []},
		get_step_range([], 9, 0, 4),
		"Empty steps"
	),
	?assertEqual(
		{0, []},
		get_step_range(lists:seq(9, 5, -1), 9, 9, 5),
		"Invalid range"
	),
	?assertEqual(
		{9, [9, 8, 7, 6, 5]},
		get_step_range(lists:seq(9, 5, -1), 9, 5, 9),
		"Full intersection"
	),
	?assertEqual(
		{9, [9, 8, 7, 6, 5]},
		get_step_range(lists:seq(9, 5, -1), 9, 3, 9),
		"Clipped RangeStart"
	),
	?assertEqual(
		{9, [9, 8, 7, 6]},
		get_step_range(lists:seq(9, 5, -1), 9, 6, 12),
		"Clipped RangeEnd"
	),
	?assertEqual(
		{8, [8, 7]},
		get_step_range(lists:seq(20, 5, -1), 20, 7, 8),
		"Clipped Steps above"
	),
	?assertEqual(
		{9, [9, 8, 7, 6, 5]},
		get_step_range(lists:seq(9, 0, -1), 9, 5, 9),
		"Clipped Steps below"
	),
	?assertEqual(
		{6, [6]},
		get_step_range(lists:seq(9, 5, -1), 9, 6, 6),
		"Range length 1"
	),
	?assertEqual(
		{8, [8]},
		get_step_range([8], 8, 8, 8),
		"Steps length 1"
	),
	ResetFrequency = 5,
	?assertEqual(
		{9, [9, 8, 7, 6, 5]},
		get_step_range_from_interval(
			#vdf_session{ step_number = 12, steps = lists:seq(12, 0, -1) }, 1, ResetFrequency),
		"Session and Interval"
	),
	?assertEqual(
		{0, []},
		get_step_range_from_interval(not_found, 1, ResetFrequency),
		"not_found and Interval"
	),
	?assertEqual(
		{9, [9, 8, 7]},
		get_step_range(
			#vdf_session{ step_number = 12, steps = lists:seq(12, 0, -1) }, 7, 9),
		"Session and Range"
	),
	?assertEqual(
		{0, []},
		get_step_range(not_found, 7, 9),
		"not_found and Range"
	),
	ok.

filter_step_triplets_test() ->
	?assertEqual([], filter_step_triplets([], [a, b])),
	?assertEqual([], filter_step_triplets([{a, 1, s}], [a, b])),
	?assertEqual([], filter_step_triplets([{b, 1, s}], [a, b])),
	?assertEqual([], filter_step_triplets([{b, 1, s}, {x, 1, s}], [a, b])),
	?assertEqual([{y, 1, s}], filter_step_triplets([{y, 1, s}, {a, 1, s}, {x, 1, s}], [a, b])),
	?assertEqual([{y, 1, s}, {x, 1, s}], filter_step_triplets([{y, 1, s}, {x, 1, s}], [a, b])).

get_triplets_test() ->
	?assertEqual([], get_triplets(1, [a], none, 2, 3, 0)),
	?assertEqual([], get_triplets(2, [], 2, 4, 5, 2)),
	?assertEqual([{a, 1, 2}], get_triplets(1, [a], none, 2, 3, 2)),
	?assertEqual([{a, 1, 2}], get_triplets(1, [a], 2, 2, 3, 2)),
	?assertEqual([{a, 1, 3}], get_triplets(1, [a], 1, 2, 3, 2)),
	?assertEqual([{a, 2, 3}, {b, 1, 2}], get_triplets(2, [a, b], 2, 2, 3, 2)),
	?assertEqual([{a, 2, 3}, {b, 1, 2}], get_triplets(2, [a, b, c], 2, 2, 3, 2)),
	?assertEqual([{a, 3, 3}, {b, 2, 3}, {c, 1, 3}], get_triplets(3, [a, b, c], 0, 2, 3, 3)),
	?assertEqual([{a, 3, 2}, {b, 2, 2}, {c, 1, 2}], get_triplets(3, [a, b, c], none, 2, 3, 4)).
