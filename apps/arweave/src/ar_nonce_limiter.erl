-module(ar_nonce_limiter).

-behaviour(gen_server).

-export([start_link/0, is_ahead_on_the_timeline/2, get_current_step_number/0,
		get_current_step_number/1, get_seed_data/4, get_last_step_checkpoints/3,
		get_checkpoints/3, validate_last_step_checkpoints/3, request_validation/3,
		get_or_init_nonce_limiter_info/1, get_or_init_nonce_limiter_info/2]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

%% The functions used in tests.
-export([reset_and_pause/0, step/0, get_steps/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(session, {
	step_number,
	seed,
	last_step_checkpoints_map = #{},
	steps,
	prev_session_key,
	upper_bound,
	next_upper_bound
}).

-record(state, {
	current_session_key,
	sessions = gb_sets:new(),
	session_by_key = #{}, % {NextSeed, StartIntervalNumber} => #session
	worker,
	worker_monitor_ref,
	autocompute = true,
	computing = false
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Return true if the first solution is above the second one according
%% to the protocol ordering.
is_ahead_on_the_timeline(NonceLimiterInfo1, NonceLimiterInfo2) ->
	#nonce_limiter_info{ global_step_number = N1 } = NonceLimiterInfo1,
	#nonce_limiter_info{ global_step_number = N2 } = NonceLimiterInfo2,
	N1 > N2.

%% @doc Return the latest known step number.
get_current_step_number() ->
	gen_server:call(?MODULE, get_current_step_number, infinity).

%% @doc Return the latest known step number in the session of the given (previous) block.
%% Return not_found if the session is not found.
get_current_step_number(B) ->
	#block{ nonce_limiter_info = #nonce_limiter_info{ next_seed = NextSeed,
			global_step_number = StepNumber } } = B,
	SessionKey = {NextSeed, StepNumber div ?NONCE_LIMITER_RESET_FREQUENCY},
	gen_server:call(?MODULE, {get_current_step_number, SessionKey}, infinity).

%% @doc Return {Seed, NextSeed, PartitionUpperBound, NextPartitionUpperBound} for
%% the block mined at StepNumber considering its previous block is mined with
%% NonceLimiterInfo. NextSeedOption and NextUpperBoundOption become the new NextSeed and
%% NextPartitionUpperBound accordingly when we cross a reset line.
get_seed_data(StepNumber, NonceLimiterInfo, NextSeedOption, NextUpperBoundOption) ->
	#nonce_limiter_info{ global_step_number = N, seed = Seed, next_seed = NextSeed,
			partition_upper_bound = PartitionUpperBound,
			next_partition_upper_bound = NextPartitionUpperBound } = NonceLimiterInfo,
	true = StepNumber >= N,
	case get_entropy_reset_point(N, StepNumber) of
		none ->
			{Seed, NextSeed, PartitionUpperBound, NextPartitionUpperBound};
		_ ->
			{NextSeed, NextSeedOption, NextPartitionUpperBound, NextUpperBoundOption}
	end.

%% @doc Return the cached step checkpoints for the given step. Return not_found if
%% none found.
get_last_step_checkpoints(StartIntervalNumber, StepNumber, NextSeed) ->
	SessionKey = {NextSeed, StartIntervalNumber},
	gen_server:call(?MODULE, {get_last_step_checkpoints, StepNumber, SessionKey}, infinity).

%% @doc Return the checkpoints of the given interval. The checkpoints are chosen
%% according to the protocol. Return not_found if the corresponding hash chain is not
%% computed yet.
get_checkpoints(StartStepNumber, EndStepNumber, NextSeed)
		when EndStepNumber > StartStepNumber ->
	SessionKey = {NextSeed, StartStepNumber div ?NONCE_LIMITER_RESET_FREQUENCY},
	gen_server:call(?MODULE, {get_checkpoints, StartStepNumber, EndStepNumber, SessionKey},
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
				last_step_checkpoints = [Output | _] = LastStepCheckpoints } }, PrevB,
				PrevOutput)
		when length(LastStepCheckpoints) == ?LAST_STEP_NONCE_LIMITER_CHECKPOINTS_COUNT ->
	PrevInfo = get_or_init_nonce_limiter_info(PrevB),
	#nonce_limiter_info{ next_seed = PrevNextSeed,
			global_step_number = PrevBStepNumber } = PrevInfo,
	SessionKey = {PrevNextSeed, PrevBStepNumber div ?NONCE_LIMITER_RESET_FREQUENCY},
	case gen_server:call(?MODULE,
			{get_last_step_checkpoints, StepNumber, SessionKey}, infinity) of
		LastStepCheckpoints ->
			true;
		not_found ->
			PrevOutput2 =
				case get_entropy_reset_point(PrevBStepNumber, StepNumber) of
					StepNumber ->
						mix_seed(PrevOutput, Seed);
					_ ->
						PrevOutput
				end,
			Buffer = iolist_to_binary(lists:reverse(LastStepCheckpoints)),
			Groups = [{1, ?LAST_STEP_NONCE_LIMITER_CHECKPOINTS_COUNT, Buffer}],
			PrevStepNumber = StepNumber - 1,
			{ok, Config} = application:get_env(arweave, config),
			ThreadCount = Config#config.max_nonce_limiter_last_step_validation_thread_count,
			case verify_no_reset(PrevStepNumber, PrevOutput2, Groups, ThreadCount) of
				{true, _Steps} ->
					true;
				false ->
					false
			end;
		_ ->
			false
	end;
validate_last_step_checkpoints(_B, _PrevB, _PrevOutput) ->
	false.

mix_seed(PrevOutput, Seed) ->
	SeedH = crypto:hash(sha256, Seed),
	crypto:hash(sha256, << PrevOutput/binary, SeedH/binary >>).

%% @doc Validate the nonce limiter chain between two blocks in the background.
%% Assume the seeds are correct and the first block is above the second one
%% according to the protocol.
%% Emit {nonce_limiter, {invalid, H, ErrorCode}} or {nonce_limiter, {valid, H}}.
request_validation(H, #nonce_limiter_info{ global_step_number = N },
		#nonce_limiter_info{ global_step_number = N }) ->
	spawn(fun() -> ar_events:send(nonce_limiter, {invalid, H, 1}) end);
request_validation(H, #nonce_limiter_info{ output = Output,
		checkpoints = [Output | _] = Checkpoints } = Info, PrevInfo) ->
	#nonce_limiter_info{ output = PrevOutput, next_seed = PrevNextSeed,
			global_step_number = PrevStepNumber } = PrevInfo,
	#nonce_limiter_info{ output = Output, seed = Seed, next_seed = NextSeed,
			partition_upper_bound = UpperBound,
			next_partition_upper_bound = NextUpperBound, global_step_number = StepNumber,
			checkpoints = Checkpoints } = Info,
	EntropyResetPoint = get_entropy_reset_point(PrevStepNumber, StepNumber),
	SessionKey = {PrevNextSeed, PrevStepNumber div ?NONCE_LIMITER_RESET_FREQUENCY},
	Steps = gen_server:call(?MODULE, {get_partial_steps, PrevStepNumber, StepNumber,
			SessionKey}, infinity),
	NextSessionKey = {NextSeed, StepNumber div ?NONCE_LIMITER_RESET_FREQUENCY},
	Buffer = steps_to_buffer(lists:reverse(Checkpoints)),
	Group = {?LAST_STEP_NONCE_LIMITER_CHECKPOINTS_COUNT, length(Checkpoints), Buffer},
	ReversedSteps = lists:reverse(Steps),
	case exclude_computed_steps_from_checkpoints([Group], ReversedSteps) of
		invalid ->
			spawn(fun() -> ar_events:send(nonce_limiter, {invalid, H, 2}) end);
		{[], Shift} when PrevStepNumber + Shift == StepNumber ->
			Args = {StepNumber, SessionKey, NextSessionKey, Seed, UpperBound, NextUpperBound,
					Steps},
			gen_server:cast(?MODULE, {validated_steps, Args}),
			spawn(fun() -> ar_events:send(nonce_limiter, {valid, H}) end);
		{[_Group], Shift} when PrevStepNumber + Shift >= StepNumber ->
			spawn(fun() -> ar_events:send(nonce_limiter, {invalid, H, 2}) end);
		{[_Group] = Groups, Shift} when PrevStepNumber + Shift < StepNumber ->
			PrevOutput2 = case Shift of 0 -> PrevOutput;
					_ -> lists:nth(Shift, ReversedSteps) end,
			spawn(
				fun() ->
					StartStepNumber = PrevStepNumber + Shift,
					{ok, Config} = application:get_env(arweave, config),
					ThreadCount = Config#config.max_nonce_limiter_validation_thread_count,
					Result =
						case is_integer(EntropyResetPoint) of
							true when EntropyResetPoint > StartStepNumber ->
								SeedH = crypto:hash(sha256, Seed),
								verify(StartStepNumber, PrevOutput2, Groups, EntropyResetPoint,
										SeedH, ThreadCount);
							_ ->
								verify_no_reset(StartStepNumber, PrevOutput2, Groups,
										ThreadCount)
						end,
					case Result of
						false ->
							ar_events:send(nonce_limiter, {invalid, H, 3});
						{true, Steps2} ->
							Args = {StepNumber, SessionKey, NextSessionKey, Seed, UpperBound,
									NextUpperBound, Steps2 ++ Steps},
							gen_server:cast(?MODULE, {validated_steps, Args}),
							ar_events:send(nonce_limiter, {valid, H})
					end
				end
			);
		Data ->
			ErrorID = ar_util:encode(crypto:strong_rand_bytes(16)),
			file:write_file("error_dump_" ++ binary_to_list(ErrorID),
					term_to_binary(Data)),
			?LOG_ERROR([{event, unexpected_error_during_nonce_limiter_validation},
					{error_id, ErrorID}])
	end;
request_validation(H, _Info, _PrevInfo) ->
	spawn(fun() -> ar_events:send(nonce_limiter, {invalid, H, 4}) end).

%% @doc Reset the state and stop computing steps automatically. Used in tests.
reset_and_pause() ->
	gen_server:cast(?MODULE, reset_and_pause).

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

%% @doc Get all steps starting from the latest on the current tip. Used in tests.
get_steps() ->
	gen_server:call(?MODULE, get_steps).

get_or_init_nonce_limiter_info(#block{ height = Height, indep_hash = H } = B) ->
	case Height >= ar_fork:height_2_7() of
		true ->
			B#block.nonce_limiter_info;
		false ->
			{Seed, PartitionUpperBound} =
					ar_node:get_recent_partition_upper_bound_by_prev_h(H),
			get_or_init_nonce_limiter_info(B, Seed, PartitionUpperBound)
	end.

get_or_init_nonce_limiter_info(#block{ height = Height } = B, RecentBI) ->
	case Height >= ar_fork:height_2_7() of
		true ->
			B#block.nonce_limiter_info;
		false ->
			{Seed, PartitionUpperBound, _TXRoot}
					= lists:last(lists:sublist(RecentBI, ?SEARCH_SPACE_UPPER_BOUND_DEPTH)),
			get_or_init_nonce_limiter_info(B, Seed, PartitionUpperBound)
	end.

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
	{ok, start_worker(State)}.

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
	#state{ current_session_key = Key, session_by_key = SessionByKey } = State,
	Session = maps:get(Key, SessionByKey),
	{reply, Session#session.step_number, State};

handle_call({get_current_step_number, SessionKey}, _From, State) ->
	#state{ session_by_key = SessionByKey } = State,
	case maps:get(SessionKey, SessionByKey, not_found) of
		not_found ->
			{reply, not_found, State};
		#session{ step_number = StepNumber } ->
			{reply, StepNumber, State}
	end;

handle_call({get_last_step_checkpoints, StepNumber, SessionKey}, _From, State) ->
	#state{ session_by_key = SessionByKey } = State,
	case maps:get(SessionKey, SessionByKey, not_found) of
		not_found ->
			{reply, not_found, State};
		Session ->
			Map = Session#session.last_step_checkpoints_map,
			{reply, maps:get(StepNumber, Map, not_found), State}
	end;

handle_call({get_checkpoints, StartStepNumber, EndStepNumber, SessionKey}, _From, State) ->
	case get_steps(StartStepNumber, EndStepNumber, SessionKey, State) of
		not_found ->
			{reply, not_found, State};
		Steps ->
			TakeN = min(?NONCE_LIMITER_MAX_CHECKPOINTS_COUNT, EndStepNumber - StartStepNumber),
			{reply, lists:sublist(Steps, TakeN), State}
	end;

handle_call({get_partial_steps, StartStepNumber, EndStepNumber, SessionKey}, _From, State) ->
	{reply, get_partial_steps(StartStepNumber, EndStepNumber, SessionKey, State), State};

handle_call(get_steps, _From, #state{ current_session_key = undefined } = State) ->
	{reply, [], State};
handle_call(get_steps, _From, State) ->
	#state{ current_session_key = SessionKey, session_by_key = SessionByKey } = State,
	#session{ step_number = StepNumber } = maps:get(SessionKey, SessionByKey),
	{reply, get_steps(1, StepNumber, SessionKey, State), State};

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(initialized, State) ->
	gen_server:cast(?MODULE, schedule_step),
	ar_events:send(nonce_limiter, initialized),
	{noreply, State};

handle_cast({initialize, [PrevB, B | Blocks]}, State) ->
	apply_chain(B#block.nonce_limiter_info, PrevB#block.nonce_limiter_info),
	gen_server:cast(?MODULE, {apply_tip, B, PrevB}),
	gen_server:cast(?MODULE, {initialize, [B | Blocks]}),
	{noreply, State};
handle_cast({initialize, _}, State) ->
	gen_server:cast(?MODULE, initialized),
	{noreply, State};

handle_cast({apply_tip, B, PrevB}, State) ->
	{noreply, apply_tip2(B, PrevB, State)};

handle_cast({validated_steps, Args}, State) ->
	{StepNumber, SessionKey, NextSessionKey, Seed, UpperBound, NextUpperBound, Steps} = Args,
	#state{ session_by_key = SessionByKey, sessions = Sessions } = State,
	case maps:get(SessionKey, SessionByKey, not_found) of
		not_found ->
			%% The corresponding fork origin should have just dropped below the
			%% checkpoint height.
			?LOG_WARNING([{event, session_not_found_for_validated_steps},
					{next_seed, ar_util:encode(element(1, SessionKey))},
					{interval, element(2, SessionKey)}]),
			{noreply, State};
		#session{ step_number = CurrentStepNumber, steps = CurrentSteps } = Session ->
			Session2 =
				case CurrentStepNumber < StepNumber of
					true ->
						Steps2 = lists:sublist(Steps, StepNumber - CurrentStepNumber)
								++ CurrentSteps,
						Session#session{ step_number = StepNumber, steps = Steps2 };
					false ->
						Session
				end,
			SessionByKey2 = maps:put(SessionKey, Session2, SessionByKey),
			Steps3 = Session2#session.steps,
			StepNumber2 = Session2#session.step_number,
			Session3 =
				case maps:get(NextSessionKey, SessionByKey2, not_found) of
					not_found ->
						{_, Interval} = NextSessionKey,
						SessionStart = Interval * ?NONCE_LIMITER_RESET_FREQUENCY,
						SessionEnd = (Interval + 1) * ?NONCE_LIMITER_RESET_FREQUENCY - 1,
						Steps4 =
							case StepNumber2 > SessionEnd of
								true ->
									lists:nthtail(StepNumber2 - SessionEnd, Steps3);
								false ->
									Steps3
							end,
						StepNumber3 = min(StepNumber2, SessionEnd),
						Steps5 = lists:sublist(Steps4, StepNumber3 - SessionStart + 1),
						#session{ step_number = StepNumber3, seed = Seed,
							last_step_checkpoints_map = #{}, steps = Steps5,
							upper_bound = UpperBound, next_upper_bound = NextUpperBound,
							prev_session_key = SessionKey };
					Session4 ->
						Session4
				end,
			SessionByKey3 = maps:put(NextSessionKey, Session3, SessionByKey2),
			Sessions2 = gb_sets:add_element(NextSessionKey, Sessions),
			{noreply, State#state{ session_by_key = SessionByKey3, sessions = Sessions2 }}
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

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info({event, node_state, {initializing, Blocks}}, State) ->
	{noreply, handle_initialized(lists:sublist(Blocks, ?STORE_BLOCKS_BEHIND_CURRENT), State)};

handle_info({event, node_state, {validated_pre_fork_2_7_block, B}}, State) ->
	#state{ sessions = Sessions } = State,
	case gb_sets:is_empty(Sessions) of
		true ->
			{noreply, apply_base_block(B, State)};
		false ->
			%% The fork block is seeded from the STORE_BLOCKS_BEHIND_CURRENT's past block
			%% and we do not reorg past that point so even if there are competing
			%% pre-fork 2.7 blocks, they have the same {NextSeed, IntervalNumber} key.
			{noreply, State}
	end;

handle_info({event, node_state, {new_tip, B, PrevB}}, State) ->
	{noreply, apply_tip(B, PrevB, State)};

handle_info({event, node_state, {checkpoint_block, B}}, State) ->
	case B#block.height < ar_fork:height_2_7() of
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
	#state{ session_by_key = SessionByKey, current_session_key = CurrentSessionKey } = State,
	{StepNumber, PrevOutput, Output, UpperBound, LastStepCheckpoints} = Args,
	Session = maps:get(CurrentSessionKey, SessionByKey),
	#session{ steps = [CurrentOutput | _] = Steps, seed = Seed,
			last_step_checkpoints_map = Map } = Session,
	{NextSeed, IntervalNumber} = CurrentSessionKey,
	IntervalStart = IntervalNumber * ?NONCE_LIMITER_RESET_FREQUENCY,
	CurrentOutput2 =
		case get_entropy_reset_point(IntervalStart, StepNumber) of
			StepNumber ->
				mix_seed(CurrentOutput, NextSeed);
			_ ->
				CurrentOutput
		end,
	gen_server:cast(?MODULE, schedule_step),
	case PrevOutput == CurrentOutput2 of
		false ->
			?LOG_INFO([{event, computed_for_outdated_key}]),
			{noreply, State};
		true ->
			Map2 = maps:put(StepNumber, LastStepCheckpoints, Map),
			Session2 = Session#session{ step_number = StepNumber,
					last_step_checkpoints_map = Map2, steps = [Output | Steps] },
			SessionByKey2 = maps:put(CurrentSessionKey, Session2, SessionByKey),
			ar_events:send(nonce_limiter, {computed_output, {Seed, NextSeed, UpperBound,
					StepNumber, IntervalNumber, Output}}),
			{noreply, State#state{ session_by_key = SessionByKey2 }}
	end;

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(_Reason, #state{ worker = W }) ->
	W ! stop,
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_entropy_reset_point(StepNumber, EndStepNumber) ->
	ResetLine = (StepNumber div ?NONCE_LIMITER_RESET_FREQUENCY + 1)
			* ?NONCE_LIMITER_RESET_FREQUENCY,
	case ResetLine > EndStepNumber of
		true ->
			none;
		false ->
			ResetLine
	end.

steps_to_buffer([Step | Steps]) ->
	<< Step/binary, (steps_to_buffer(Steps))/binary >>;
steps_to_buffer([]) ->
	<<>>.

exclude_computed_steps_from_checkpoints(Groups, Steps) ->
	exclude_computed_steps_from_checkpoints(Groups, Steps, 0).

exclude_computed_steps_from_checkpoints([], _Steps, Shift) ->
	{[], Shift};
exclude_computed_steps_from_checkpoints([{Size, N, Buffer} | Groups], Steps, Shift) ->
	Skip = Size div ?LAST_STEP_NONCE_LIMITER_CHECKPOINTS_COUNT,
	exclude_computed_steps_from_checkpoints([{Size, N, Buffer} | Groups], Steps, 1, Skip,
			Shift).

exclude_computed_steps_from_checkpoints(Groups, [], _I, _Skip, Shift) ->
	{lists:reverse(Groups), Shift};
exclude_computed_steps_from_checkpoints(Groups, [_Step | Steps], I, Skip, Shift)
		when I /= Skip ->
	exclude_computed_steps_from_checkpoints(Groups, Steps, I + 1, Skip, Shift);
exclude_computed_steps_from_checkpoints([{Size, 1, << Step/binary >>} | Groups],
		[Step | Steps], _I, _Skip, Shift) ->
	exclude_computed_steps_from_checkpoints(Groups, Steps,
			Shift + Size div ?LAST_STEP_NONCE_LIMITER_CHECKPOINTS_COUNT);
exclude_computed_steps_from_checkpoints([{Size, N, << Step:32/binary, Buffer/binary >>}
		| Groups], [Step | Steps], _I, Skip, Shift) ->
	exclude_computed_steps_from_checkpoints([{Size, N - 1, Buffer} | Groups], Steps, 1, Skip,
			Shift + Size div ?LAST_STEP_NONCE_LIMITER_CHECKPOINTS_COUNT);
exclude_computed_steps_from_checkpoints(_Groups, _Steps, _I, _Skip, _Shift) ->
	invalid.

handle_initialized([#block{ height = Height } = B | Blocks], State) ->
	case Height + 1 < ar_fork:height_2_7() of
		true ->
			ar_events:send(nonce_limiter, initialized),
			State;
		false ->
			Blocks2 = take_blocks_after_fork([B | Blocks]),
			handle_initialized2(lists:reverse(Blocks2), State)
	end.

take_blocks_after_fork([#block{ height = Height } = B | Blocks]) ->
	case Height + 1 >= ar_fork:height_2_7() of
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
	#nonce_limiter_info{ seed = Seed, next_seed = NextSeed, output = Output,
			partition_upper_bound = UpperBound,
			next_partition_upper_bound = NextUpperBound,
			global_step_number = StepNumber,
			last_step_checkpoints = LastStepCheckpoints } = B#block.nonce_limiter_info,
	Session = #session{ step_number = StepNumber,
			last_step_checkpoints_map = #{ StepNumber => LastStepCheckpoints },
			steps = [Output], upper_bound = UpperBound, next_upper_bound = NextUpperBound,
			seed = Seed },
	SessionKey = {NextSeed, StepNumber div ?NONCE_LIMITER_RESET_FREQUENCY},
	Sessions = gb_sets:from_list([SessionKey]),
	SessionByKey = #{ SessionKey => Session },
	State#state{ current_session_key = SessionKey, sessions = Sessions,
			session_by_key = SessionByKey }.

apply_chain(#nonce_limiter_info{ global_step_number = StepNumber },
		#nonce_limiter_info{ global_step_number = PrevStepNumber })
		when StepNumber - PrevStepNumber > ?NONCE_LIMITER_MAX_CHECKPOINTS_COUNT ->
	ar:console("Cannot do a trusted join - there are not enough checkpoints"
			" to apply quickly; step number: ~B, previous step number: ~B.",
			[StepNumber, PrevStepNumber]),
	timer:sleep(1000),
	erlang:halt();
apply_chain(Info, PrevInfo) ->
	#nonce_limiter_info{ next_seed = PrevNextSeed,
			global_step_number = PrevStepNumber } = PrevInfo,
	#nonce_limiter_info{ output = Output, seed = Seed, next_seed = NextSeed,
			partition_upper_bound = UpperBound,
			next_partition_upper_bound = NextUpperBound, global_step_number = StepNumber,
			checkpoints = Checkpoints } = Info,
	Count = StepNumber - PrevStepNumber,
	Output = hd(Checkpoints),
	Count = length(Checkpoints),
	SessionKey = {PrevNextSeed, PrevStepNumber div ?NONCE_LIMITER_RESET_FREQUENCY},
	NextSessionKey = {NextSeed, StepNumber div ?NONCE_LIMITER_RESET_FREQUENCY},
	Args = {StepNumber, SessionKey, NextSessionKey, Seed, UpperBound, NextUpperBound,
			Checkpoints},
	gen_server:cast(?MODULE, {validated_steps, Args}).

apply_tip(#block{ height = Height } = B, PrevB, #state{ sessions = Sessions } = State) ->
	case Height + 1 < ar_fork:height_2_7() of
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
					true = (Height + 1) == ar_fork:height_2_7(),
					State3 = apply_base_block(B, State2),
					State3;
				false ->
					apply_tip2(B, PrevB, State2)
			end
	end.

apply_tip2(B, PrevB, State) ->
	#state{ session_by_key = SessionByKey, sessions = Sessions } = State,
	#nonce_limiter_info{ next_seed = NextSeed, seed = Seed,
			partition_upper_bound = UpperBound,
			next_partition_upper_bound = NextUpperBound, global_step_number = StepNumber,
			last_step_checkpoints = LastStepCheckpoints } = B#block.nonce_limiter_info,
	#nonce_limiter_info{ next_seed = PrevNextSeed,
			global_step_number = PrevStepNumber } = PrevB#block.nonce_limiter_info,
	Interval = StepNumber div ?NONCE_LIMITER_RESET_FREQUENCY,
	SessionKey = {NextSeed, Interval},
	PrevInterval = PrevStepNumber div ?NONCE_LIMITER_RESET_FREQUENCY,
	PrevSessionKey = {PrevNextSeed, PrevInterval},
	Session2 =
		case maps:get(SessionKey, SessionByKey, not_found) of
			not_found ->
				PrevSession = maps:get(PrevSessionKey, SessionByKey),
				#session{ steps = Steps, step_number = StepNumber2 } = PrevSession,
				SessionStart = Interval * ?NONCE_LIMITER_RESET_FREQUENCY,
				SessionEnd = (Interval + 1) * ?NONCE_LIMITER_RESET_FREQUENCY - 1,
				Steps2 =
					case StepNumber2 > SessionEnd of
						true ->
							lists:nthtail(StepNumber2 - SessionEnd, Steps);
						false ->
							Steps
					end,
				StepNumber3 = min(StepNumber2, SessionEnd),
				Steps3 = lists:sublist(Steps2, StepNumber3 - SessionStart + 1),
				#session{ step_number = StepNumber3, seed = Seed,
						last_step_checkpoints_map = #{ StepNumber => LastStepCheckpoints },
						steps = Steps3, upper_bound = UpperBound,
						next_upper_bound = NextUpperBound, prev_session_key = PrevSessionKey };
			_Session ->
				none
		end,
	case Session2 of
		none ->
			State#state{ current_session_key = SessionKey };
		_ ->
			SessionByKey2 = maps:put(SessionKey, Session2, SessionByKey),
			Sessions2 = gb_sets:add_element(SessionKey, Sessions),
			State#state{ current_session_key = SessionKey, sessions = Sessions2,
					session_by_key = SessionByKey2 }
	end.

prune_old_sessions(Sessions, SessionByKey, BaseInterval) ->
	{{_NextSeed, Interval} = Key, Sessions2} = gb_sets:take_smallest(Sessions),
	case BaseInterval > Interval of
		true ->
			SessionByKey2 = maps:remove(Key, SessionByKey),
			prune_old_sessions(Sessions2, SessionByKey2, BaseInterval);
		false ->
			{Sessions, SessionByKey}
	end.

start_worker(State) ->
	Worker = spawn(fun() -> process_flag(priority, high), worker() end),
	Ref = monitor(process, Worker),
	State#state{ worker = Worker, worker_monitor_ref = Ref }.

compute(StepNumber, Output) ->
	{ok, O1, L1} = ar_vdf:compute2(step_number_to_salt_number(StepNumber - 1), Output,
			?VDF_DIFFICULTY),
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(double_check_nonce_limiter, Config#config.enable) of
		false ->
			{ok, O1, L1};
		true ->
			{ok, O2, L2} = ar_mine_randomx:vdf_sha2(step_number_to_salt_number(StepNumber - 1),
					Output),
			case {O1, L1} == {O2, L2} of
				true ->
					{ok, O1, L1};
				false ->
					ID = ar_util:encode(crypto:strong_rand_bytes(16)),
					file:write_file("compute_" ++ binary_to_list(ID),
							term_to_binary({StepNumber, Output})),
					?LOG_ERROR([{event, nonce_limiter_compute_mismatch},
							{report_id, ID}]),
					{ok, O1, L1}
			end
	end.

verify(StartStepNumber, PrevOutput, Groups, ResetStepNumber, ResetSeed, ThreadCount) ->
	Rep1 = ar_vdf:verify2(step_number_to_salt_number(StartStepNumber), PrevOutput, Groups,
			step_number_to_salt_number(ResetStepNumber - 1), ResetSeed, ThreadCount,
			?VDF_DIFFICULTY),
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(double_check_nonce_limiter, Config#config.enable) of
		false ->
			Rep1;
		true ->
			Rep2 = ar_mine_randomx:vdf_parallel_sha_verify(
					step_number_to_salt_number(StartStepNumber), PrevOutput, Groups,
					step_number_to_salt_number(ResetStepNumber - 1), ResetSeed, ThreadCount),
			case Rep1 == Rep2 of
				true ->
					Rep1;
				false ->
					ID = ar_util:encode(crypto:strong_rand_bytes(16)),
					file:write_file("verify_" ++ binary_to_list(ID),
							term_to_binary({StartStepNumber, PrevOutput, Groups,
									ResetStepNumber, ResetSeed, ThreadCount})),
					?LOG_ERROR([{event, nonce_limiter_verify_mismatch},
							{report_id, ID}]),
					Rep1
			end
	end.

verify_no_reset(StartStepNumber, PrevOutput, Groups, ThreadCount) ->
	Garbage = crypto:strong_rand_bytes(32),
	Rep1 = ar_vdf:verify2(step_number_to_salt_number(StartStepNumber), PrevOutput, Groups, 0,
			Garbage, ThreadCount, ?VDF_DIFFICULTY),
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(double_check_nonce_limiter, Config#config.enable) of
		false ->
			Rep1;
		true ->
			Rep2 = ar_mine_randomx:vdf_parallel_sha_verify_no_reset(
					step_number_to_salt_number(StartStepNumber), PrevOutput, Groups,
					ThreadCount),
			case Rep1 == Rep2 of
				true ->
					Rep1;
				false ->
					ID = ar_util:encode(crypto:strong_rand_bytes(16)),
					file:write_file("verify_no_reset_" ++ binary_to_list(ID),
							term_to_binary({StartStepNumber, PrevOutput, Groups,
									ThreadCount})),
					?LOG_ERROR([{event, nonce_limiter_verify_no_reset_mismatch},
							{report_id, ID}]),
					Rep1
			end
	end.

step_number_to_salt_number(StepNumber) ->
	(StepNumber - 1) * 25 + 1.

worker() ->
	receive
		{compute, {StepNumber, Output, UpperBound}, From} ->
			{ok, Output2, Checkpoints} = prometheus_histogram:observe_duration(
					vdf_step_time_milliseconds, [], fun() -> compute(StepNumber, Output) end),
			Args2 = {StepNumber, Output, Output2, UpperBound, Checkpoints},
			From ! {computed, Args2},
			worker();
		stop ->
			ok
	end.

get_steps(StartStepNumber, EndStepNumber, SessionKey, State) ->
	#state{ session_by_key = SessionByKey } = State,
	case maps:get(SessionKey, SessionByKey, not_found) of
		#session{ step_number = StepNumber, steps = Steps, prev_session_key = PrevSessionKey }
				when StepNumber >= EndStepNumber ->
			Steps2 = lists:nthtail(StepNumber - EndStepNumber, Steps),
			Count = EndStepNumber - StartStepNumber,
			SessionCount = length(Steps2),
			case Count > SessionCount of
				true ->
					EndStepNumber2 = EndStepNumber - SessionCount,
					case get_steps(StartStepNumber, EndStepNumber2, PrevSessionKey, State) of
						not_found ->
							not_found;
						Steps3 ->
							Steps2 ++ Steps3
					end;
				false ->
					lists:sublist(Steps2, Count)
			end;
		_ ->
			not_found
	end.

get_partial_steps(StartStepNumber, EndStepNumber, SessionKey, State) ->
	#state{ session_by_key = SessionByKey } = State,
	case maps:get(SessionKey, SessionByKey, not_found) of
		#session{ step_number = StepNumber, steps = Steps }
				when StepNumber > StartStepNumber ->
			End = min(StepNumber, EndStepNumber),
			Steps2 = lists:nthtail(StepNumber - End, Steps),
			lists:sublist(Steps2, End - StartStepNumber);
		_ ->
			[]
	end.

schedule_step(State) ->
	#state{ current_session_key = {NextSeed, IntervalNumber} = Key,
			session_by_key = SessionByKey, worker = Worker } = State,
	#session{ step_number = StepNumber, steps = Steps, upper_bound = UpperBound,
			next_upper_bound = NextUpperBound } = maps:get(Key, SessionByKey),
	PrevOutput = hd(Steps),
	StepNumber2 = StepNumber + 1,
	IntervalStart = IntervalNumber * ?NONCE_LIMITER_RESET_FREQUENCY,
	{PrevOutput2, UpperBound2} =
		case get_entropy_reset_point(IntervalStart, StepNumber2) of
			StepNumber2 ->
				{mix_seed(PrevOutput, NextSeed), NextUpperBound};
			none ->
				{PrevOutput, UpperBound};
			_ ->
				{PrevOutput, NextUpperBound}
		end,
	Worker ! {compute, {StepNumber2, PrevOutput2, UpperBound2}, self()},
	State.

get_or_init_nonce_limiter_info(#block{ height = Height } = B, Seed, PartitionUpperBound) ->
	NextSeed = B#block.indep_hash,
	NextPartitionUpperBound = B#block.weave_size,
	case Height + 1 == ar_fork:height_2_7() of
		true ->
			Output = crypto:hash(sha256, Seed),
			#nonce_limiter_info{ output = Output, seed = Seed, next_seed = NextSeed,
					partition_upper_bound = PartitionUpperBound,
					next_partition_upper_bound = NextPartitionUpperBound };
		false ->
			undefined
	end.

%%%===================================================================
%%% Tests.
%%%===================================================================

exclude_computed_steps_from_checkpoints_test() ->
	C1 = crypto:strong_rand_bytes(32),
	C2 = crypto:strong_rand_bytes(32),
	C3 = crypto:strong_rand_bytes(32),
	C4 = crypto:strong_rand_bytes(32),
	C5 = crypto:strong_rand_bytes(32),
	Cases = [
		{{[], []}, {[], 0}, "Case 1"},
		{{[{25, 1, C1}], []}, {[{25, 1, C1}], 0}, "Case 2"},
		{{[{25, 1, C1}], [C1]}, {[], 1}, "Case 3"},
		{{[{25, 2, << C1/binary, C2/binary >>}], []},
				{[{25, 2, << C1/binary, C2/binary >>}], 0}, "Case 4"},
		{{[{25, 2, << C1/binary, C2/binary >>}], [C2]}, invalid, "Case 5"},
		{{[{25, 2, << C1/binary, C2/binary >>}], [C1]}, {[{25, 1, C2}], 1}, "Case 6"},
		{{[{25, 2, << C1/binary, C2/binary >>}], [C2, C1]}, invalid, "Case 7"},
		{{[{25, 2, << C1/binary, C2/binary >>}], [C1, C2]}, {[], 2}, "Case 8"},
		{{[{25, 2, << C1/binary, C2/binary >>}], [C1, C2, C3, C4, C5]}, {[], 2}, "Case 9"}
	],
	test_exclude_computed_steps_from_checkpoints(Cases).

test_exclude_computed_steps_from_checkpoints([Case | Cases]) ->
	{Input, Expected, Title} = Case,
	{Groups, ReversedSteps} = Input,
	Got = exclude_computed_steps_from_checkpoints(Groups, ReversedSteps),
	?assertEqual(Expected, Got, Title),
	test_exclude_computed_steps_from_checkpoints(Cases);
test_exclude_computed_steps_from_checkpoints([]) ->
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
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end},
			{ar_fork, height_2_7, fun() -> 0 end}], fun test_applies_validated_steps/0).

test_applies_validated_steps() ->
	reset_and_pause(),
	Seed = crypto:strong_rand_bytes(32),
	NextSeed = crypto:strong_rand_bytes(32),
	NextSeed2 = crypto:strong_rand_bytes(32),
	InitialOutput = crypto:strong_rand_bytes(32),
	B1 = test_block(1, InitialOutput, Seed, NextSeed, [], []),
	ar_events:send(node_state, {initializing, [B1]}),
	true = ar_util:do_until(fun() -> get_current_step_number() == 1 end, 100, 1000),
	{ok, Output2, _} = compute(2, InitialOutput),
	B2 = test_block(2, Output2, Seed, NextSeed, [], [Output2]),
	ok = ar_events:subscribe(nonce_limiter),
	assert_validate(B2, B1, valid),
	assert_validate(B2, B1, valid),
	assert_validate(B2#block{ nonce_limiter_info = #nonce_limiter_info{} }, B1, {invalid, 1}),
	N2 = B2#block.nonce_limiter_info,
	assert_validate(B2#block{ nonce_limiter_info = N2#nonce_limiter_info{ checkpoints = [] } },
			B1, {invalid, 4}),
	assert_validate(B2#block{
			nonce_limiter_info = N2#nonce_limiter_info{ checkpoints = [Output2, Output2] } },
			B1, {invalid, 2}),
	assert_step_number(2),
	[step() || _ <- lists:seq(1, 3)],
	assert_step_number(5),
	ar_events:send(node_state, {new_tip, B2, B1}),
	assert_step_number(5),
	{ok, Output3, _} = compute(3, Output2),
	{ok, Output4, _} = compute(4, Output3),
	B3 = test_block(4, Output4, Seed, NextSeed, [], [Output4, Output3]),
	assert_validate(B3, B2, valid),
	assert_validate(B3, B1, {invalid, 2}),
	{ok, Output5, _} = compute(5, mix_seed(Output4, NextSeed)),
	B4 = test_block(5, Output5, NextSeed, NextSeed2, [], [Output5]),
	[step() || _ <- lists:seq(1, 6)],
	assert_step_number(11),
	assert_validate(B4, B3, valid),
	ar_events:send(node_state, {new_tip, B4, B3}),
	assert_step_number(9),
	assert_validate(B4, B4, {invalid, 1}),
	% 5, 6, 7, 8, 9, 10
	B5 = test_block(10, <<>>, NextSeed, NextSeed2, [], [<<>>]),
	assert_validate(B5, B4, {invalid, 2}),
	B6 = test_block(10, <<>>, NextSeed, NextSeed2, [],
			% Steps 10, 9, 8, 7, 6.
			[<<>> | lists:sublist(get_steps(), 4)]),
	assert_validate(B6, B4, {invalid, 3}),
	Invalid = crypto:strong_rand_bytes(32),
	B7 = test_block(10, Invalid, NextSeed, NextSeed2, [],
			% Steps 10, 9, 8, 7, 6.
			[Invalid | lists:sublist(get_steps(), 4)]),
	assert_validate(B7, B4, {invalid, 3}),
	{ok, Output6, _} = compute(6, Output5),
	{ok, Output7, _} = compute(7, Output6),
	{ok, Output8, _} = compute(8, Output7),
	B8 = test_block(8, Output8, NextSeed, NextSeed2, [], [Output8, Output7, Output6]),
	assert_validate(B8, B4, valid).

reorg_after_join_test_() ->
	{timeout, 60, fun test_reorg_after_join/0}.

test_reorg_after_join() ->
	[B0] = ar_weave:init(),
	ar_test_node:start(B0),
	ar_test_node:slave_start(B0),
	ar_test_node:connect_to_slave(),
	ar_node:mine(),
	ar_test_node:assert_slave_wait_until_height(1),
	ar_test_node:join_on_slave(),
	ar_test_node:slave_start(B0),
	ar_test_node:slave_mine(),
	ar_test_node:assert_slave_wait_until_height(1),
	ar_test_node:slave_mine(),
	ar_test_node:wait_until_height(2).

reorg_after_join2_test_() ->
	{timeout, 60, fun test_reorg_after_join2/0}.

test_reorg_after_join2() ->
	[B0] = ar_weave:init(),
	ar_test_node:start(B0),
	ar_test_node:slave_start(B0),
	ar_test_node:connect_to_slave(),
	ar_node:mine(),
	ar_test_node:assert_slave_wait_until_height(1),
	ar_test_node:join_on_slave(),
	ar_node:mine(),
	ar_test_node:wait_until_height(2),
	ar_test_node:slave_start(B0),
	ar_test_node:slave_mine(),
	ar_test_node:assert_slave_wait_until_height(1),
	ar_test_node:slave_mine(),
	ar_test_node:assert_slave_wait_until_height(2),
	ar_test_node:slave_mine(),
	ar_test_node:wait_until_height(3).

assert_validate(B, PrevB, ExpectedResult) ->
	request_validation(B#block.indep_hash, B#block.nonce_limiter_info,
			PrevB#block.nonce_limiter_info),
	BH = B#block.indep_hash,
	receive
		{event, nonce_limiter, {valid, BH}} ->
			case ExpectedResult of
				valid ->
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

test_block(StepNumber, Output, Seed, NextSeed, LastStepCheckpoints, Checkpoints) ->
	#block{ indep_hash = crypto:strong_rand_bytes(32),
			nonce_limiter_info = #nonce_limiter_info{ output = Output,
					global_step_number = StepNumber, seed = Seed, next_seed = NextSeed,
					last_step_checkpoints = LastStepCheckpoints, checkpoints = Checkpoints } }.
