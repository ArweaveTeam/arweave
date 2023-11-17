-module(ar_nonce_limiter_server).

-behaviour(gen_server).

-export([start_link/0, make_full_nonce_limiter_update/2, make_partial_nonce_limiter_update/4]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {
	current_session,
	current_prev_session,
	current_output,
	current_partition_upper_bound
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

make_partial_nonce_limiter_update(SessionKey, Session, Output, PartitionUpperBound) ->
	make_nonce_limiter_update(
		SessionKey,
		Session#vdf_session{
			upper_bound = PartitionUpperBound,
			steps = [Output]
		},
		true).

make_full_nonce_limiter_update(SessionKey, Session) ->
	make_nonce_limiter_update(SessionKey, Session, false).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(nonce_limiter),
	{ok, #state{}}.

handle_call(get_update, _From, #state{ current_output = undefined } = State) ->
	{reply, not_found, State};
handle_call(get_update, _From, State) ->
	#state{ current_output = Output, current_session = {SessionKey, Session},
			current_partition_upper_bound = PartitionUpperBound } = State,
	{reply, 
		make_partial_nonce_limiter_update(SessionKey, Session, Output, PartitionUpperBound),
		State};

handle_call(get_session, _From, #state{ current_session = undefined } = State) ->
	{reply, not_found, State};
handle_call(get_session, _From, State) ->
	#state{ current_session = {SessionKey, Session} } = State,
	{reply, make_full_nonce_limiter_update(SessionKey, Session), State};

handle_call(get_previous_session, _From, #state{ current_prev_session = undefined } = State) ->
	{reply, not_found, State};
handle_call(get_previous_session, _From, State) ->
	#state{ current_prev_session = {SessionKey, Session} } = State,
	{reply, make_full_nonce_limiter_update(SessionKey, Session), State};

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info({event, nonce_limiter, {computed_output, Args}}, State) ->
	{SessionKey, PrevSessionKey, _Seed, _StepNumber, Output, PartitionUpperBound} = Args,
	Session = ar_nonce_limiter:get_session(SessionKey),
	PrevSession = ar_nonce_limiter:get_session(PrevSessionKey),
	{noreply, State#state{ current_session = {SessionKey, Session},
			current_prev_session = {PrevSessionKey, PrevSession},
			current_output = Output, current_partition_upper_bound = PartitionUpperBound }};

handle_info({event, nonce_limiter, _Args}, State) ->
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
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
