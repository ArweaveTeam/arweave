-module(ar_nonce_limiter_server).

-behaviour(gen_server).

-export([start_link/0, make_full_nonce_limiter_update/2, make_partial_nonce_limiter_update/4,
		get_update/1, get_full_update/1, get_full_prev_update/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {
	session_key,
	step_number
}).

%% @doc The number of steps and the corresponding step checkpoints to include in every
%% regular update.
-define(REGULAR_UPDATE_INCLUDE_STEPS_COUNT, 2).

%% @doc The number of steps for which we include step checkpoints in the full session update.
%% Does not apply to previous session updates.
-define(SESSION_UPDATE_INCLUDE_STEP_CHECKPOINTS_COUNT, 20).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

make_partial_nonce_limiter_update(SessionKey, Session, StepNumber, Output) ->
	#vdf_session{ steps = Steps, step_number = SessionStepNumber } = Session,
	StepNumberMinusOne = StepNumber - 1,
	Steps2 =
		case SessionStepNumber of
			StepNumber ->
				Steps;
			StepNumberMinusOne ->
				[Output | Steps];
			_ ->
				?LOG_WARNING([{event, vdf_gap},
						{session_step_number, SessionStepNumber},
						{computed_output, StepNumber}]),
				[Output]
		end,
	make_nonce_limiter_update(
		SessionKey,
		Session#vdf_session{
			step_number = StepNumber,
			steps = lists:sublist(Steps2, ?REGULAR_UPDATE_INCLUDE_STEPS_COUNT)
		},
		true).

make_full_nonce_limiter_update(SessionKey, Session) ->
	make_nonce_limiter_update(SessionKey, Session, false).

%% @doc Return the minimal VDF update, i.e., the latest computed output.
get_update(Format) ->
	case ets:lookup(?MODULE, {partial_update, Format}) of
		[] ->
			not_found;
		[{_, PartialUpdate}] ->
			PartialUpdate
	end.

%% @doc Return the "full update" including the latest VDF session.
get_full_update(Format) ->
	case ets:lookup(?MODULE, {full_update, Format}) of
		[] ->
			not_found;
		[{_, Session}] ->
			Session
	end.

%% @doc Return the "full previous update" including the latest but one VDF session.
get_full_prev_update(Format) ->
	case ets:lookup(?MODULE, {full_prev_update, Format}) of
		[] ->
			not_found;
		[{_, PrevSession}] ->
			PrevSession
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(nonce_limiter),
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({event, nonce_limiter, {computed_output, Args}}, State) ->
	handle_computed_output(Args, State);

handle_info({event, nonce_limiter, _Args}, State) ->
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

make_nonce_limiter_update(_SessionKey, not_found, _IsPartial) ->
	not_found;
make_nonce_limiter_update(SessionKey, Session, IsPartial) ->
	#vdf_session{ step_number = StepNumber, steps = Steps,
			step_checkpoints_map = StepCheckpointsMap } = Session,
	%% Clear the step_checkpoints_map to cut down on the amount of data pushed to each client.
	RecentStepNumbers =
		case IsPartial of
			false ->
				%% There is an upper bound on the number of steps with step checkpoints
				%% because the total number of steps in the session updates is often large.
				get_recent_step_numbers(StepNumber);
			true ->
				%% Include step checkpoints for every step included in the regular
				%% update.
				get_recent_step_numbers_from_steps(StepNumber, Steps)
		end,
	StepCheckpointsMap2 = maps:with(RecentStepNumbers, StepCheckpointsMap),
	#nonce_limiter_update{ session_key = SessionKey,
			is_partial = IsPartial,
			session = Session#vdf_session{ step_checkpoints_map = StepCheckpointsMap2 } }.

handle_computed_output({SessionKey, StepNumber, _, _},
		#state{ session_key = SessionKey, step_number = CurrentStepNumber } = State)
			when CurrentStepNumber >= StepNumber ->
	{noreply, State};
handle_computed_output(Args, State) ->
	{SessionKey, StepNumber, Output, _PartitionUpperBound} = Args,
	case ar_nonce_limiter:get_session(SessionKey) of
		not_found ->
			?LOG_WARNING([{event, computed_output_session_not_found},
					{session_key, ar_nonce_limiter:encode_session_key(SessionKey)},
					{step_number, StepNumber}]),
			{noreply, State};
		Session ->
			PrevSessionKey = Session#vdf_session.prev_session_key,
			PrevSession = ar_nonce_limiter:get_session(PrevSessionKey),
			PartialUpdate = make_partial_nonce_limiter_update(SessionKey, Session,
					StepNumber, Output),
			FullUpdate = make_full_nonce_limiter_update(SessionKey, Session),

			PartialUpdateBin2 = ar_serialize:nonce_limiter_update_to_binary(2, PartialUpdate),
			PartialUpdateBin3 = ar_serialize:nonce_limiter_update_to_binary(3, PartialUpdate),
			FullUpdateBin2 = ar_serialize:nonce_limiter_update_to_binary(2, FullUpdate),
			FullUpdateBin3 = ar_serialize:nonce_limiter_update_to_binary(3, FullUpdate),
			FullUpdateBin4 = ar_serialize:nonce_limiter_update_to_binary(4, FullUpdate),
			Keys = [
				{{partial_update, 2}, PartialUpdateBin2},
				{{partial_update, 3}, PartialUpdateBin3},
				{{full_update, 2}, FullUpdateBin2},
				{{full_update, 3}, FullUpdateBin3},
				{{full_update, 4}, FullUpdateBin4}
			],
			Keys2 =
				case PrevSession of
					not_found ->
						Keys;
					_ ->
						FullPrevUpdate = make_full_nonce_limiter_update(
								PrevSessionKey, PrevSession),
						FullPrevUpdateBin2 = ar_serialize:nonce_limiter_update_to_binary(
								2, FullPrevUpdate),
						FullPrevUpdateBin3 = ar_serialize:nonce_limiter_update_to_binary(
								3, FullPrevUpdate),
						FullPrevUpdateBin4 = ar_serialize:nonce_limiter_update_to_binary(
								4, FullPrevUpdate),
						Keys ++ [
							{{full_prev_update, 2}, FullPrevUpdateBin2},
							{{full_prev_update, 3}, FullPrevUpdateBin3},
							{{full_prev_update, 4}, FullPrevUpdateBin4}]
				end,
			ets:insert(?MODULE, Keys2),
			{noreply, State#state{ session_key = SessionKey, step_number = StepNumber }}
	end.

get_recent_step_numbers(StepNumber) ->
	get_recent_step_numbers(StepNumber, 0).

get_recent_step_numbers(_, Taken)
		when Taken == ?SESSION_UPDATE_INCLUDE_STEP_CHECKPOINTS_COUNT ->
	[];
get_recent_step_numbers(-1, _Taken) ->
	[];
get_recent_step_numbers(StepNumber, Taken) ->
	[StepNumber | get_recent_step_numbers(StepNumber - 1, Taken + 1)].

get_recent_step_numbers_from_steps(_StepNumber, []) ->
	[];
get_recent_step_numbers_from_steps(StepNumber, [_Step | Steps]) ->
	[StepNumber | get_recent_step_numbers_from_steps(StepNumber - 1, Steps)].
