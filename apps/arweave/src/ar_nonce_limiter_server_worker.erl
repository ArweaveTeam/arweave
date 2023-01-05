-module(ar_nonce_limiter_server_worker).

-behaviour(gen_server).

-export([start_link/2]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").

-record(state, {
	peer,
	pause_until = 0
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
	#state{ peer = Peer, pause_until = Timestamp } = State,
	case os:system_time(second) < Timestamp of
		true ->
			{noreply, State};
		false ->
			{noreply, push_update(Args, Peer, State)}
	end;

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

push_update(Args, Peer, State) ->
	{Seed, NextSeed, UpperBound, StepNumber, IntervalNumber, Output, Checkpoints} = Args,
	Update = #nonce_limiter_update{ session_key = {NextSeed, IntervalNumber},
			is_partial = true, checkpoints = Checkpoints,
			session = #vdf_session{ seed = Seed, upper_bound = UpperBound,
					step_number = StepNumber, steps = [Output] } },
	case ar_http_iface_client:push_nonce_limiter_update(Peer, Update) of
		ok ->
			State;
		{ok, Response} ->
			Postpone = Response#nonce_limiter_update_response.postpone,
			case Postpone > 0 of
				true ->
					Now = os:system_time(second),
					State#state{ pause_until = Now + Postpone };
				false ->
					case Response#nonce_limiter_update_response.session_found of
						false ->
							push_session(Update, Peer);
						true ->
							StepNumber2 = Response#nonce_limiter_update_response.step_number,
							case StepNumber2 >= StepNumber - 1 of
								true ->
									ok;
								false ->
									push_session(Update, Peer)
							end
					end,
					State
			end;
		{error, Error} ->
			?LOG_WARNING([{event, failed_to_push_nonce_limiter_update_to_peer},
					{peer, ar_util:format_peer(Peer)},
					{reason, io_lib:format("~p", [Error])}]),
			State
	end.

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
