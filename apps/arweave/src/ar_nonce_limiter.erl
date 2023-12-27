-module(ar_nonce_limiter).

-behaviour(gen_server).

-export([start_link/0, account_tree_initialized/1, encode_session_key/1,
		is_ahead_on_the_timeline/2, 
		get_current_step_number/0, get_current_step_number/1, get_step_triplets/3,
		get_seed_data/2, get_step_checkpoints/4, get_steps/4,
		validate_last_step_checkpoints/3, request_validation/3,
		get_or_init_nonce_limiter_info/1, get_or_init_nonce_limiter_info/2,
		apply_external_update/2, get_session/1, get_current_session/0,
		compute/3, resolve_remote_server_raw_peers/0,
		maybe_add_entropy/4, mix_seed/2]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
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

%% @doc Return the nonce limiter session with the given key.
get_session(SessionKey) ->
	gen_server:call(?MODULE, {get_session, SessionKey}, infinity).

get_current_session() ->
	gen_server:call(?MODULE, get_current_session, infinity).

%% @doc Return the latest known step number.
get_current_step_number() ->
	gen_server:call(?MODULE, get_current_step_number, infinity).

%% @doc Return the latest known step number in the session of the given (previous) block.
%% Return not_found if the session is not found.
get_current_step_number(B) ->
	SessionKey = session_key(B#block.nonce_limiter_info),
	gen_server:call(?MODULE, {get_current_step_number, SessionKey}, infinity).

%% @doc Return {Output, StepNumber, PartitionUpperBound} for up to N latest steps
%% from the VDF session of Info, if any. If PrevOutput is among the N latest steps,
%% return only the steps strictly above PrevOutput.
get_step_triplets(Info, PrevOutput, N) ->
	SessionKey = session_key(Info),
	Steps = gen_server:call(?MODULE, {get_latest_step_triplets, SessionKey, N}, infinity),
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
	gen_server:call(?MODULE, {get_step_checkpoints, StepNumber, SessionKey}, infinity).

%% @doc Return the steps of the given interval. The steps are chosen
%% according to the protocol. Return not_found if the corresponding hash chain is not
%% computed yet.
get_steps(StartStepNumber, EndStepNumber, NextSeed, NextVDFDifficulty)
		when EndStepNumber > StartStepNumber ->
	SessionKey = session_key(NextSeed, StartStepNumber, NextVDFDifficulty),
	gen_server:call(?MODULE, {get_steps, StartStepNumber, EndStepNumber, SessionKey},
			infinity).

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
			{ok, Config} = application:get_env(arweave, config),
			ThreadCount = Config#config.max_nonce_limiter_last_step_validation_thread_count,
			case verify_no_reset(PrevStepNumber, PrevOutput2, 1,
					lists:reverse(LastStepCheckpoints), ThreadCount, VDFDifficulty) of
				{true, _Steps} ->
					true;
				false ->
					false
			end;
		_ ->
			{false, cache_mismatch}
	end;
validate_last_step_checkpoints(_B, _PrevB, _PrevOutput) ->
	false.

%% @doc Determine whether StepNumber has passed the entropy reset line. If it has return the
%% reset line, otherwise return none.
get_entropy_reset_point(PrevStepNumber, StepNumber) ->
	ResetLine = (PrevStepNumber div ?NONCE_LIMITER_RESET_FREQUENCY + 1)
			* ?NONCE_LIMITER_RESET_FREQUENCY,
	case ResetLine > StepNumber of
		true ->
			none;
		false ->
			ResetLine
	end.

%% @doc Conditionally add entropy to PrevOutput if the configured number of steps have
%% passed. See ?NONCE_LIMITER_RESET_FREQUENCY for more details.
maybe_add_entropy(PrevOutput, PrevStepNumber, StepNumber, Seed) ->
	case get_entropy_reset_point(PrevStepNumber, StepNumber) of
		StepNumber ->
			mix_seed(PrevOutput, Seed);
		_ ->
			PrevOutput
	end.

%% @doc Add entropy to an earlier VDF output to mitigate the impact of a miner with a
%% fast VDF compute. See ?NONCE_LIMITER_RESET_FREQUENCY for more details.
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
			SessionKey}, infinity),
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
			case ar_config:use_remote_vdf_server() of
				true ->
					%% Wait for our VDF server(s) to validate the remaining steps.
					spawn(fun() ->
						timer:sleep(1000),
						request_validation(H, Info, PrevInfo) end);
				false ->
					%% Validate the remaining steps.
					StartOutput2 = case NumAlreadyComputed of
							0 -> StartOutput;
							_ -> lists:nth(NumAlreadyComputed, ComputedSteps)
					end,
					spawn(fun() ->
						StartStepNumber2 = StartStepNumber + NumAlreadyComputed,
						{ok, Config} = application:get_env(arweave, config),
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
	gen_server:call(?MODULE, {apply_external_update, Update, Peer}, infinity).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	ok = ar_events:subscribe(node_state),
	State =
		case ar_node:is_joined() of
			true ->
				Blocks = get_blocks(),
				handle_initialized(Blocks, #state{});
			_ ->
				#state{}
		end,
	State2 =
		case ar_config:use_remote_vdf_server() of
			false ->
				State;
			true ->
				resolve_remote_server_raw_peers(),
				gen_server:cast(?MODULE, check_external_vdf_server_input),
				State#state{ autocompute = false }
		end,
	{ok, start_worker(State2)}.

resolve_remote_server_raw_peers() ->
	{ok, Config} = application:get_env(arweave, config),
	resolve_remote_server_raw_peers(Config#config.nonce_limiter_server_trusted_peers),
	timer:apply_after(10000, ?MODULE, resolve_remote_server_raw_peers, []).

resolve_remote_server_raw_peers([]) ->
	ok;
resolve_remote_server_raw_peers([RawPeer | RawPeers]) ->
	case ar_peers:resolve_and_cache_peer(RawPeer, vdf_server_peer) of
		{ok, _Peer} ->
			resolve_remote_server_raw_peers(RawPeers);
		{error, Reason} ->
			?LOG_WARNING([{event, failed_to_resolve_vdf_server_peer},
					{reason, io_lib:format("~p", [Reason])}]),
			resolve_remote_server_raw_peers(RawPeers)
	end.

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
				upper_bound = UpperBound, next_upper_bound = NextUpperBound } ->
			{_, IntervalNumber, _} = SessionKey,
			IntervalStart = IntervalNumber * ?NONCE_LIMITER_RESET_FREQUENCY,
			ResetPoint = get_entropy_reset_point(IntervalStart, StepNumber),
			{reply,
				get_triplets(StepNumber, Steps, ResetPoint, UpperBound, NextUpperBound, N),
				State}
	end;

handle_call({get_step_checkpoints, StepNumber, SessionKey}, _From, State) ->
	case get_session(SessionKey, State) of
		not_found ->
			{reply, not_found, State};
		#vdf_session{ step_checkpoints_map = Map } ->
			{reply, maps:get(StepNumber, Map, not_found), State}
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
	apply_external_update2(Update, State#state{ last_external_update = {Peer, Now} });

handle_call({get_session, SessionKey}, _From, State) ->
	{reply, get_session(SessionKey, State), State};

handle_call(get_current_session, _From, State) ->
	#state{ current_session_key = CurrentSessionKey } = State,
	{reply, {CurrentSessionKey, get_session(CurrentSessionKey, State)}, State};

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
	#state{ current_session_key = CurrentSessionKey } = State,
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
						{_, Steps2} =
							get_step_range(Steps, StepNumber, CurrentStepNumber + 1, StepNumber),
						update_session(Session, StepNumber, LastStepCheckpoints, Steps2);
					false ->
						Session
				end,
			State2 = cache_session(State, SessionKey, CurrentSessionKey, Session2),
			State3 = cache_block_session(State2, NextSessionKey, SessionKey, CurrentSessionKey,
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

handle_info({event, node_state, {validated_pre_fork_2_6_block, B}}, State) ->
	#state{ sessions = Sessions } = State,
	case gb_sets:is_empty(Sessions) of
		true ->
			{noreply, apply_base_block(B, State)};
		false ->
			%% The fork block is seeded from the STORE_BLOCKS_BEHIND_CURRENT's past block
			%% and we do not reorg past that point so even if there are competing
			%% pre-fork 2.6 blocks, they have the same {NextSeed, IntervalNumber} key.
			{noreply, State}
	end;

handle_info({event, node_state, {new_tip, B, PrevB}}, State) ->
	{noreply, apply_tip(B, PrevB, State)};

handle_info({event, node_state, {checkpoint_block, B}}, State) ->
	case B#block.height < ar_fork:height_2_6() of
		true ->
			{noreply, State};
		false ->
			#state{ sessions = Sessions, session_by_key = SessionByKey,
					current_session_key = CurrentSessionKey } = State,
			StepNumber = (B#block.nonce_limiter_info)#nonce_limiter_info.global_step_number,
			BaseInterval = StepNumber div ?NONCE_LIMITER_RESET_FREQUENCY,
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
	{StepNumber, PrevOutput, Output, Checkpoints} = Args,
	Session = get_session(CurrentSessionKey, State),
	#vdf_session{ next_vdf_difficulty = NextVDFDifficulty, steps = [SessionOutput | _] } = Session,
	{NextSeed, IntervalNumber, NextVDFDifficulty} = CurrentSessionKey,
	IntervalStart = IntervalNumber * ?NONCE_LIMITER_RESET_FREQUENCY,
	SessionOutput2 = ar_nonce_limiter:maybe_add_entropy(
			SessionOutput, IntervalStart, StepNumber, NextSeed),
	gen_server:cast(?MODULE, schedule_step),
	case PrevOutput == SessionOutput2 of
		false ->
			?LOG_INFO([{event, computed_for_outdated_key}, {step_number, StepNumber},
				{output, ar_util:encode(Output)}]),
			{noreply, State};
		true ->
			Session2 = update_session(Session, StepNumber, Checkpoints, [Output]),
			State2 = cache_session(State, CurrentSessionKey, CurrentSessionKey, Session2),
			?LOG_DEBUG([{event, computed_nonce_limiter_output},
				{session_key, encode_session_key(CurrentSessionKey)}, {step_number, StepNumber}]),
			send_output(CurrentSessionKey, Session2),
			{noreply, State2}
	end;

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, #state{ worker = W }) ->
	W ! stop,
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

session_key(#nonce_limiter_info{ next_seed = NextSeed, global_step_number = StepNumber,
		next_vdf_difficulty = NextVDFDifficulty }) ->
	session_key(NextSeed, StepNumber, NextVDFDifficulty).
session_key(NextSeed, StepNumber, NextVDFDifficulty) ->
	{NextSeed, StepNumber div ?NONCE_LIMITER_RESET_FREQUENCY, NextVDFDifficulty}.

get_session(SessionKey, #state{ session_by_key = SessionByKey }) ->
	maps:get(SessionKey, SessionByKey, not_found).

update_session(Session, StepNumber, Checkpoints, Steps) ->
	#vdf_session{ step_checkpoints_map = Map } = Session,
	Map2 = maps:put(StepNumber, Checkpoints, Map),
	update_session(Session#vdf_session{ step_checkpoints_map = Map2 }, StepNumber, Steps).

update_session(Session, StepNumber, Steps) ->
	#vdf_session{ steps = CurrentSteps } = Session,
	Session#vdf_session{ step_number = StepNumber, steps = Steps ++ CurrentSteps }.

send_output(SessionKey, Session) ->
	{_, IntervalNumber, _} = SessionKey,
	#vdf_session{ step_number = StepNumber, steps = [Output | _] } = Session,
	IntervalStart = IntervalNumber * ?NONCE_LIMITER_RESET_FREQUENCY,
	UpperBound =
		case get_entropy_reset_point(IntervalStart, StepNumber) of
			none ->
				Session#vdf_session.upper_bound;
			_ ->
				Session#vdf_session.next_upper_bound
		end,
	ar_events:send(nonce_limiter, {computed_output, {SessionKey, StepNumber, Output, UpperBound}}).

dump_error(Data) ->
	{ok, Config} = application:get_env(arweave, config),
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
	cache_session(State, SessionKey, SessionKey, Session).

apply_chain(#nonce_limiter_info{ global_step_number = StepNumber },
		#nonce_limiter_info{ global_step_number = PrevStepNumber })
		when StepNumber - PrevStepNumber > ?NONCE_LIMITER_MAX_CHECKPOINTS_COUNT ->
	ar:console("Cannot do a trusted join - there are not enough checkpoints"
			" to apply quickly; step number: ~B, previous step number: ~B.",
			[StepNumber, PrevStepNumber]),
	timer:sleep(1000),
	erlang:halt();
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
	State2 = cache_block_session(State, SessionKey, PrevSessionKey, SessionKey,
			#{ StepNumber => LastStepCheckpoints }, Seed, UpperBound, NextUpperBound,
			VDFDifficulty, NextVDFDifficulty),
	State2.

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
		fun ar_vdf:debug_sha2/3,
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
		{compute, {StepNumber, PrevOutput, VDFDifficulty}, From} ->
			{ok, Output, Checkpoints} = prometheus_histogram:observe_duration(
					vdf_step_time_milliseconds, [], fun() -> compute(StepNumber, PrevOutput,
							VDFDifficulty) end),
			From ! {computed, {StepNumber, PrevOutput, Output, Checkpoints}},
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
	IntervalStart = IntervalNumber * ?NONCE_LIMITER_RESET_FREQUENCY,
	PrevOutput2 = ar_nonce_limiter:maybe_add_entropy(
		PrevOutput, IntervalStart, StepNumber, NextSeed),
	VDFDifficulty2 =
		case get_entropy_reset_point(IntervalStart, StepNumber) of
			none ->
				VDFDifficulty;
			_ ->
				?LOG_DEBUG([{event, entropy_reset_point_found}, {step_number, StepNumber},
					{interval_start, IntervalStart}, {vdf_difficulty, VDFDifficulty},
					{next_vdf_difficulty, NextVDFDifficulty}]),
				NextVDFDifficulty
		end,
	Worker ! {compute, {StepNumber, PrevOutput2, VDFDifficulty2}, self()},
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
	#state{ current_session_key = CurrentSessionKey, last_external_update = {Peer, _} } = State,
	#nonce_limiter_update{ session_key = SessionKey,
			session = #vdf_session{
				prev_session_key = PrevSessionKey, step_number = StepNumber } = Session,
			checkpoints = Checkpoints, is_partial = IsPartial } = Update,
	{_SessionSeed, SessionInterval, _SessionVDFDifficulty} = SessionKey,
	case get_session(SessionKey, State) of
		not_found ->
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
					NextSessionStart = (SessionInterval + 1) * ?NONCE_LIMITER_RESET_FREQUENCY,
					{_, Steps} = get_step_range(
						Session, min(RangeStart, NextSessionStart), StepNumber),					
					State2 = apply_external_update3(State,
						SessionKey, CurrentSessionKey, Session, Steps),
					{reply, ok, State2}
			end;
		CurrentSession ->
			#vdf_session{ step_number = CurrentStepNumber } = CurrentSession,
			case CurrentStepNumber + 1 == StepNumber of
				true ->
					[Output | _] = Session#vdf_session.steps,
					CurrentSession2 = update_session(CurrentSession, StepNumber, Checkpoints, [Output]),
					State2 = apply_external_update3(State,
							SessionKey, CurrentSessionKey, CurrentSession2, [Output]),
					{reply, ok, State2};
				false ->
					case CurrentStepNumber >= StepNumber of
						true ->
							%% Inform the peer we are ahead.
							?LOG_DEBUG([{event, apply_external_vdf},
								{result, ahead_of_server},
								{vdf_server, ar_util:format_peer(Peer)},
								{session_key, encode_session_key(SessionKey)},
								{client_step_number, CurrentStepNumber},
								{server_step_number, StepNumber}]),
							{reply, #nonce_limiter_update_response{
											step_number = CurrentStepNumber }, State};
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
									%% noticed the gap and requested the  full VDF session be sent -
									%% which may contain previously processed steps in a addition to
									%% the missing ones.
									%% 
									%% To avoid processing those steps twice, the client grabs
									%% CurrentStepNumber (our most recently processed step number)
									%% and ignores it and any lower steps found in Session.
									{_, Steps} = get_step_range(
											Session, CurrentStepNumber + 1, StepNumber),
									State2 = apply_external_update3(State,
										SessionKey, CurrentSessionKey, Session, Steps),
									{reply, ok, State2}
							end
					end
			end
	end.

%% @doc Final step of applying a VDF update pushed by a VDF server
%% @param CurrentSessionKey Session key of the session currently tracked by ar_nonce_limiter state
%% @param SessionKey Session key of the session pushed by the VDF server
%% @param PrevSessionKey Session key of the session before the session pushed by the VDF server
%% @param Session Session to apply (either the session pushed by the VDF server or the one tracked
%%                by ar_nonce_limiter state)
%% @param SessionByKey Session map maintained by ar_nonce_limiter state
%% @param NumSteps Number of Session steps to apply. Steps are sorted in descending order.
%% @param UpperBound Upper bound of the session pushed by the VDF server
%%
%% Note: an important job of this function is to ensure that VDF steps are only processed once.
%% We truncate Session.steps such the previously processed steps are not sent to
%% send_events_for_external_update.
apply_external_update3(State, SessionKey, CurrentSessionKey, Session, Steps) ->
	#state{ last_external_update = {Peer, _} } = State,
	?LOG_DEBUG([{event, apply_external_vdf},
		{result, ok},
		{vdf_server, ar_util:format_peer(Peer)},
		{session_key, encode_session_key(SessionKey)},
		{step_number, Session#vdf_session.step_number},
		{length, length(Steps)}]),
	State2 = cache_session(State, SessionKey, CurrentSessionKey, Session),
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
get_step_range(Session, SessionInterval) ->
	SessionStart = SessionInterval * ?NONCE_LIMITER_RESET_FREQUENCY,
	SessionEnd = (SessionInterval + 1) * ?NONCE_LIMITER_RESET_FREQUENCY - 1,
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

%% @doc Update the VDF session cache based on new info from a validated block.
cache_block_session(State, SessionKey, PrevSessionKey, CurrentSessionKey, 
		StepCheckpointsMap, Seed, UpperBound, NextUpperBound, VDFDifficulty, NextVDFDifficulty) ->
	Session =
		case get_session(SessionKey, State) of
			not_found ->
				{_, Interval, NextVDFDifficulty} = SessionKey,
				PrevSession = get_session(PrevSessionKey, State),
				{StepNumber, Steps} = get_step_range(PrevSession, Interval),
				#vdf_session{ step_number = StepNumber, seed = Seed,
						upper_bound = UpperBound, next_upper_bound = NextUpperBound,
						prev_session_key = PrevSessionKey,
						vdf_difficulty = VDFDifficulty, next_vdf_difficulty = NextVDFDifficulty,
						step_checkpoints_map = StepCheckpointsMap,
						steps = Steps };
			ExistingSession ->
				ExistingSession
		end,
	cache_session(State, SessionKey, CurrentSessionKey, Session).

cache_session(State, SessionKey, CurrentSessionKey, Session) ->
	#state{ session_by_key = SessionByKey, sessions = Sessions } = State,
	{NextSeed, Interval, NextVDFDifficulty} = SessionKey,
	maybe_set_vdf_metrics(SessionKey, CurrentSessionKey, Session),
	?LOG_DEBUG([{event, add_session}, {session_key, encode_session_key(SessionKey)},
		{step_number, Session#vdf_session.step_number}]),
	SessionByKey2 = maps:put(SessionKey, Session, SessionByKey),
	%% If Session exists, then {Interval, NextSeed} will already exist in the Sessions set and
	%% gb_sets:add_element will not cause a change.
	Sessions2 = gb_sets:add_element({Interval, NextSeed, NextVDFDifficulty}, Sessions),
	State#state{ current_session_key = CurrentSessionKey, sessions = Sessions2,
					session_by_key = SessionByKey2 }.

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
	{ok, Config} = application:get_env(arweave, config),
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

%%%===================================================================
%%% Tests.
%%%===================================================================

%% @doc Reset the state and stop computing steps automatically. Used in tests.
reset_and_pause() ->
	gen_server:cast(?MODULE, reset_and_pause).

%% @doc Do not emit the initialized event. Used in tests.
turn_off_initialized_event() ->
	gen_server:cast(?MODULE, turn_off_initialized_event).

%% @doc Get all steps starting from the latest on the current tip. Used in tests.
get_steps() ->
	gen_server:call(?MODULE, get_steps).

%% @doc Compute a single step. Used in tests.
step() ->
	Self = self(),
	spawn(
		fun() ->
			ok = ar_events:subscribe(nonce_limiter),
			gen_server:cast(?MODULE, compute_step),
			receive
				{event, nonce_limiter, {computed_output, _}} ->
					Self ! done
			end
		end
	),
	receive
		done ->
			ok
	end.

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
	ResetFreq = ?NONCE_LIMITER_RESET_FREQUENCY,
	?assertEqual(none, get_entropy_reset_point(1, ResetFreq - 1)),
	?assertEqual(ResetFreq, get_entropy_reset_point(1, ResetFreq)),
	?assertEqual(none, get_entropy_reset_point(ResetFreq, ResetFreq + 1)),
	?assertEqual(2 * ResetFreq, get_entropy_reset_point(ResetFreq, ResetFreq * 2)),
	?assertEqual(ResetFreq * 3, get_entropy_reset_point(ResetFreq * 3 - 1, ResetFreq * 3 + 2)),
	?assertEqual(ResetFreq * 4, get_entropy_reset_point(ResetFreq * 3, ResetFreq * 4 + 1)).

applies_validated_steps_test_() ->
	{timeout, 60, fun test_applies_validated_steps/0}.

test_applies_validated_steps() ->
	reset_and_pause(),
	Seed = crypto:strong_rand_bytes(32),
	NextSeed = crypto:strong_rand_bytes(32),
	NextSeed2 = crypto:strong_rand_bytes(32),
	InitialOutput = crypto:strong_rand_bytes(32),
	B1VDFDifficulty = 3,
	B1NextVDFDifficulty = 3,
	B1 = test_block(1, InitialOutput, Seed, NextSeed, [], [],
			B1VDFDifficulty, B1NextVDFDifficulty),
	turn_off_initialized_event(),
	ar_nonce_limiter:account_tree_initialized([B1]),
	true = ar_util:do_until(fun() -> get_current_step_number() == 1 end, 100, 1000),
	assert_session(B1, B1),
	{ok, Output2, _} = compute(2, InitialOutput, B1VDFDifficulty),
	B2VDFDifficulty = 3,
	B2NextVDFDifficulty = 4,
	B2 = test_block(2, Output2, Seed, NextSeed, [], [Output2], 
			B2VDFDifficulty, B2NextVDFDifficulty),
	ok = ar_events:subscribe(nonce_limiter),
	assert_validate(B2, B1, valid),
	assert_validate(B2, B1, valid),
	assert_validate(B2#block{ nonce_limiter_info = #nonce_limiter_info{} }, B1, {invalid, 1}),
	N2 = B2#block.nonce_limiter_info,
	assert_validate(B2#block{ nonce_limiter_info = N2#nonce_limiter_info{ steps = [] } },
			B1, {invalid, 4}),
	assert_validate(B2#block{
			nonce_limiter_info = N2#nonce_limiter_info{ steps = [Output2, Output2] } },
			B1, {invalid, 2}),
	assert_step_number(2),
	[step() || _ <- lists:seq(1, 3)],
	assert_step_number(5),
	ar_events:send(node_state, {new_tip, B2, B1}),
	%% We have just applied B2 with a VDF difficulty update => a new session has to be opened.
	assert_step_number(2),
	assert_session(B2, B1),
	{ok, Output3, _} = compute(3, Output2, B2VDFDifficulty),
	{ok, Output4, _} = compute(4, Output3, B2VDFDifficulty),
	B3VDFDifficulty = 3,
	B3NextVDFDifficulty = 4,
	B3 = test_block(4, Output4, Seed, NextSeed, [], [Output4, Output3],
			B3VDFDifficulty, B3NextVDFDifficulty),
	assert_validate(B3, B2, valid),
	assert_validate(B3, B1, valid),
	%% Entropy reset line crossed at step 5, add entropy and apply next_vdf_difficulty
	{ok, Output5, _} = compute(5, mix_seed(Output4, NextSeed), B3NextVDFDifficulty),
	B4VDFDifficulty = 4,
	B4NextVDFDifficulty = 5,
	B4 = test_block(5, Output5, NextSeed, NextSeed2, [], [Output5],
			B4VDFDifficulty, B4NextVDFDifficulty),
	[step() || _ <- lists:seq(1, 6)],
	assert_step_number(10),
	assert_validate(B4, B3, valid),
	ar_events:send(node_state, {new_tip, B4, B3}),
	assert_step_number(9),
	assert_session(B4, B3),
	assert_validate(B4, B4, {invalid, 1}),
	% % 5, 6, 7, 8, 9, 10
	B5VDFDifficulty = 5,
	B5NextVDFDifficulty = 6,
	B5 = test_block(10, <<>>, NextSeed, NextSeed2, [], [<<>>],
			B5VDFDifficulty, B5NextVDFDifficulty),
	assert_validate(B5, B4, {invalid, 3}),
	B6VDFDifficulty = 5,
	B6NextVDFDifficulty = 6,
	B6 = test_block(10, <<>>, NextSeed, NextSeed2, [],
			% Steps 10, 9, 8, 7, 6.
			[<<>> | lists:sublist(get_steps(), 4)],
			B6VDFDifficulty, B6NextVDFDifficulty),
	assert_validate(B6, B4, {invalid, 3}),
	Invalid = crypto:strong_rand_bytes(32),
	B7VDFDifficulty = 5,
	B7NextVDFDifficulty = 6,
	B7 = test_block(10, Invalid, NextSeed, NextSeed2, [],
			% Steps 10, 9, 8, 7, 6.
			[Invalid | lists:sublist(get_steps(), 4)],
			B7VDFDifficulty, B7NextVDFDifficulty),
	assert_validate(B7, B4, {invalid, 3}),
	%% Last valid block was B4, so that's the vdf_difficulty to use (not next_vdf_difficulty cause
	%% the next entropy reset line isn't until step 10)
	{ok, Output6, _} = compute(6, Output5, B4VDFDifficulty),
	{ok, Output7, _} = compute(7, Output6, B4VDFDifficulty),
	{ok, Output8, _} = compute(8, Output7, B4VDFDifficulty),
	B8VDFDifficulty = 4,
	%% Change the next_vdf_difficulty to confirm that apply_tip2 handles updating an
	%% existing VDF session
	B8NextVDFDifficulty = 6, 
	B8 = test_block(8, Output8, NextSeed, NextSeed2, [], [Output8, Output7, Output6],
			B8VDFDifficulty, B8NextVDFDifficulty),
	ar_events:send(node_state, {new_tip, B8, B4}),
	timer:sleep(1000),
	assert_session(B8, B4),
	assert_validate(B8, B4, valid),
	ok.

reorg_after_join_test_() ->
	{timeout, 120, fun test_reorg_after_join/0}.

test_reorg_after_join() ->
	[B0] = ar_weave:init(),
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:connect_to_peer(peer1),
	ar_test_node:mine(),
	ar_test_node:assert_wait_until_height(peer1, 1),
	ar_test_node:join_on(#{ node => main, join_on => peer1 }),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:mine(peer1),
	ar_test_node:assert_wait_until_height(peer1, 1),
	ar_test_node:mine(peer1),
	ar_test_node:wait_until_height(2).

reorg_after_join2_test_() ->
	{timeout, 120, fun test_reorg_after_join2/0}.

test_reorg_after_join2() ->
	[B0] = ar_weave:init(),
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:connect_to_peer(peer1),
	ar_test_node:mine(),
	ar_test_node:assert_wait_until_height(peer1, 1),
	ar_test_node:join_on(#{ node => main, join_on => peer1 }),
	ar_test_node:mine(),
	ar_test_node:wait_until_height(2),
	ar_test_node:disconnect_from(peer1),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:mine(peer1),
	ar_test_node:assert_wait_until_height(peer1, 1),
	ar_test_node:mine(peer1),
	ar_test_node:assert_wait_until_height(peer1, 2),
	ar_test_node:connect_to_peer(peer1),
	ar_test_node:mine(peer1),
	ar_test_node:wait_until_height(3).

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
	?assertEqual(
		{9, [9, 8, 7, 6, 5]},
		get_step_range(
			#vdf_session{ step_number = 12, steps = lists:seq(12, 0, -1) }, 1),
		"Session and Interval"
	),
	?assertEqual(
		{0, []},
		get_step_range(not_found, 1),
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

assert_session(B, PrevB) ->
	%% vdf_diffic ulty and next_vdf_difficulty in cached VDF sessions should be
	%% updated whenever a new block is validated.
	#nonce_limiter_info{
		vdf_difficulty = PrevBVDFDifficulty, next_vdf_difficulty = PrevBNextVDFDifficulty
	} = PrevB#block.nonce_limiter_info,
	#nonce_limiter_info{
		vdf_difficulty = BVDFDifficulty, next_vdf_difficulty = BNextVDFDifficulty
	} = B#block.nonce_limiter_info,
	PrevBSessionKey = session_key(PrevB#block.nonce_limiter_info),
	BSessionKey = session_key(B#block.nonce_limiter_info),

	BSession = ar_nonce_limiter:get_session(BSessionKey),
	?assertEqual(BVDFDifficulty, BSession#vdf_session.vdf_difficulty),
	?assertEqual(BNextVDFDifficulty,
		BSession#vdf_session.next_vdf_difficulty),
	case PrevBSessionKey == BSessionKey of
		true ->
			ok;
		false ->
			PrevBSession = ar_nonce_limiter:get_session(PrevBSessionKey),
			?assertEqual(PrevBVDFDifficulty,
				PrevBSession#vdf_session.vdf_difficulty),
			?assertEqual(PrevBNextVDFDifficulty,
				PrevBSession#vdf_session.next_vdf_difficulty)
	end.

assert_validate(B, PrevB, ExpectedResult) ->
	request_validation(B#block.indep_hash, B#block.nonce_limiter_info,
			PrevB#block.nonce_limiter_info),
	BH = B#block.indep_hash,
	receive
		{event, nonce_limiter, {valid, BH}} ->
			case ExpectedResult of
				valid ->
					assert_session(B, PrevB),
					ok;
				_ ->
					?assert(false, iolist_to_binary(io_lib:format("Unexpected "
							"validation success. Expected: ~p.", [ExpectedResult])))
			end;
		{event, nonce_limiter, {invalid, BH, Code}} ->
			case ExpectedResult of
				{invalid, Code} ->
					ok;
				_ ->
					?assert(false, iolist_to_binary(io_lib:format("Unexpected "
							"validation failure: ~p. Expected: ~p.",
							[Code, ExpectedResult])))
			end
	after 2000 ->
		?assert(false, "Validation timeout.")
	end.

assert_step_number(N) ->
	timer:sleep(200),
	?assert(ar_util:do_until(fun() -> get_current_step_number() == N end, 100, 1000)).

test_block(StepNumber, Output, Seed, NextSeed, LastStepCheckpoints, Steps,
		VDFDifficulty, NextVDFDifficulty) ->
	#block{ indep_hash = crypto:strong_rand_bytes(32),
			nonce_limiter_info = #nonce_limiter_info{ output = Output,
					global_step_number = StepNumber, seed = Seed, next_seed = NextSeed,
					last_step_checkpoints = LastStepCheckpoints, steps = Steps,
					vdf_difficulty = VDFDifficulty, next_vdf_difficulty = NextVDFDifficulty }
	}.

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
