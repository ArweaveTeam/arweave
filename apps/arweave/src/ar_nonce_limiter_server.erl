-module(ar_nonce_limiter_server).

-behaviour(gen_server).

-export([start_link/0, make_full_nonce_limiter_update/2, make_partial_nonce_limiter_update/4]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {
	partial_update,
	full_update,
	full_prev_update
}).

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

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(nonce_limiter),
	{ok, #state{}}.

handle_call(get_update, _From, #state{ partial_update = undefined } = State) ->
	{reply, not_found, State};
handle_call(get_update, _From, State) ->
	#state{ partial_update = PartialUpdate } = State,
	{reply, PartialUpdate, State};

handle_call(get_session, _From, #state{ full_update = undefined } = State) ->
	{reply, not_found, State};
handle_call(get_session, _From, State) ->
	#state{ full_update = FullUpdate } = State,
	{reply, FullUpdate, State};

handle_call(get_previous_session, _From, #state{ full_prev_update = undefined } = State) ->
	{reply, not_found, State};
handle_call(get_previous_session, _From, State) ->
	#state{ full_prev_update = FullPrevUpdate } = State,
	{reply, FullPrevUpdate, State};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({event, nonce_limiter, {computed_output, Args}}, State) ->
	{SessionKey, StepNumber, Output, _PartitionUpperBound} = Args,
	Session = ar_nonce_limiter:get_session(SessionKey),
	PrevSessionKey = Session#vdf_session.prev_session_key,
	PrevSession = ar_nonce_limiter:get_session(PrevSessionKey),

	PartialUpdate = make_partial_nonce_limiter_update(SessionKey, Session, StepNumber, Output),
	FullUpdate = make_full_nonce_limiter_update(SessionKey, Session),
	FullPrevUpdate = make_full_nonce_limiter_update(PrevSessionKey, PrevSession),

	{noreply, State#state{ partial_update = PartialUpdate, full_update = FullUpdate,
			full_prev_update = FullPrevUpdate }};

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
