-module(ar_nonce_limiter_server_worker).

-behaviour(gen_server).

-export([start_link/2]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").

-record(state, {
	peer
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, Peer) ->
	gen_server:start_link({local, Name}, ?MODULE, Peer, []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Peer) ->
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(nonce_limiter),
	{ok, #state{ peer = Peer }}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info({event, nonce_limiter, {computed_output, Args}}, State) ->
	#state{ peer = Peer } = State,
	{Seed, NextSeed, UpperBound, StepNumber, IntervalNumber, Output, Checkpoints} = Args,
	Update = #nonce_limiter_update{ session_key = {NextSeed, IntervalNumber},
			is_partial = true, checkpoints = Checkpoints,
			session = #vdf_session{ seed = Seed, upper_bound = UpperBound,
					step_number = StepNumber, steps = [Output] } },
	case ar_http_iface_client:push_nonce_limiter_update(Peer, Update) of
		ok ->
			ok;
		{ok, Response} ->
			case Response#nonce_limiter_update_response.session_found of
				false ->
					push_session(Update, Peer);
				true ->
					CurrentStepNumber = Response#nonce_limiter_update_response.step_number,
					case CurrentStepNumber >= StepNumber - 1 of
						true ->
							ok;
						false ->
							push_session(Update, Peer)
					end
			end;
		{error, Error} ->
			?LOG_WARNING([{event, failed_to_push_nonce_limiter_update_to_peer},
					{peer, ar_util:format_peer(Peer)},
					{reason, io_lib:format("~p", [Error])}])
	end,
	{noreply, State};

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

push_session(Update, Peer) ->
	SessionKey = Update#nonce_limiter_update.session_key,
	case ar_nonce_limiter:get_session(SessionKey) of
		#vdf_session{} = Session ->
			Session2 = Session#vdf_session{ last_step_checkpoints_map = #{} },
			Update2 = Update#nonce_limiter_update{ session = Session2, is_partial = false },
			case ar_http_iface_client:push_nonce_limiter_update(Peer, Update2) of
				ok ->
					ok;
				{ok, #nonce_limiter_update_response{ step_number = ReportedStepNumber,
						session_found = ReportedSessionFound }} ->
					?LOG_WARNING([{event, failed_to_push_nonce_limiter_update_to_peer},
							{peer, ar_util:format_peer(Peer)},
							{session_found, ReportedSessionFound},
							{reported_step_number, ReportedStepNumber}]);
				{error, Error} ->
					?LOG_WARNING([{event, failed_to_push_nonce_limiter_update_to_peer},
						{peer, ar_util:format_peer(Peer)},
						{reason, io_lib:format("~p", [Error])}])
			end;
		not_found ->
			?LOG_WARNING([{event, failed_to_push_nonce_limiter_update_to_peer},
					{peer, ar_util:format_peer(Peer)}, {reason, session_not_found},
					{next_seed, ar_util:encode(element(1, SessionKey))},
					{interval_number, element(2, SessionKey)}])
	end.
