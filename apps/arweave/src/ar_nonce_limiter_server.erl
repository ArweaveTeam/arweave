-module(ar_nonce_limiter_server).

-behaviour(gen_server).

-export([start_link/0, make_full_nonce_limiter_update/2, make_partial_nonce_limiter_update/4,
		get_update/0, get_full_update/0, get_full_prev_update/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

make_partial_nonce_limiter_update(SessionKey, Session, StepNumber, Output) ->
	make_nonce_limiter_update(
		SessionKey,
		Session#vdf_session{
			step_number = StepNumber, steps = [Output]
		},
		true).

make_full_nonce_limiter_update(SessionKey, Session) ->
	make_nonce_limiter_update(SessionKey, Session, false).

%% @doc Return the minimal VDF update, i.e., the latest computed output.
get_update() ->
	case ets:lookup(?MODULE, partial_update) of
		[] ->
			not_found;
		[{_, PartialUpdate}] ->
			PartialUpdate
	end.

%% @doc Return the "full update" including the latest VDF session.
get_full_update() ->
	case ets:lookup(?MODULE, full_update) of
		[] ->
			not_found;
		[{_, Session}] ->
			Session
	end.

%% @doc Return the "full previous update" including the latest but one VDF session.
get_full_prev_update() ->
	case ets:lookup(?MODULE, full_prev_update) of
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
			FullPrevUpdate = make_full_nonce_limiter_update(PrevSessionKey, PrevSession),
			ets:insert(?MODULE, [
				{partial_update, PartialUpdate},
				{full_update, FullUpdate},
				{full_prev_update, FullPrevUpdate}]),
			{noreply, State}
	end;

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
	StepNumber = Session#vdf_session.step_number,
	Checkpoints = maps:get(StepNumber, Session#vdf_session.step_checkpoints_map, []),
	%% Clear the step_checkpoints_map to cut down on the amount of data pushed to each client.
	#nonce_limiter_update{ session_key = SessionKey,
			is_partial = IsPartial, checkpoints = Checkpoints,
			session = Session#vdf_session{ step_checkpoints_map = #{} } }.
