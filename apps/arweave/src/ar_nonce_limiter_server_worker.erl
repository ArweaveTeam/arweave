-module(ar_nonce_limiter_server_worker).

-behaviour(gen_server).

-export([start_link/2]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").

-record(state, {
	peer,
	raw_peer,
	pause_until = 0,
	format = 1
}).

%% The frequency of re-resolving the domain names of the VDF client peers (who are
%% configured via the domain names as opposed to IP addresses).
-define(RESOLVE_DOMAIN_NAME_FREQUENCY_MS, 30000).

-define(NONCE_LIMITER_UPDATE_VERSION, 67).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, RawPeer) ->
	gen_server:start_link({local, Name}, ?MODULE, RawPeer, []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(RawPeer) ->
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(nonce_limiter),
	gen_server:cast(self(), resolve_raw_peer),
	{ok, #state{ raw_peer = RawPeer }}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(resolve_raw_peer, #state{ raw_peer = RawPeer } = State) ->
	case ar_peers:resolve_and_cache_peer(RawPeer, vdf_client_peer) of
		{ok, Peer} ->
			ar_util:cast_after(?RESOLVE_DOMAIN_NAME_FREQUENCY_MS, self(), resolve_raw_peer),
			{noreply, State#state{ peer = Peer }};
		{error, Reason} ->
			?LOG_WARNING([{event, failed_to_resolve_vdf_client_peer},
					{reason, io_lib:format("~p", [Reason])}]),
			ar_util:cast_after(10000, self(), resolve_raw_peer),
			{noreply, State}
	end;

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info({event, nonce_limiter, _Event}, #state{ peer = undefined } = State) ->
	{noreply, State};
handle_info({event, nonce_limiter, {computed_output, Args}}, State) ->
	#state{ peer = Peer, pause_until = Timestamp, format = Format } = State,
	{SessionKey, Session, PrevSessionKey, PrevSession, Output, PartitionUpperBound} = Args,
	CurrentStepNumber = ar_nonce_limiter:get_current_step_number(),
	case os:system_time(second) < Timestamp of
		true ->
			{noreply, State};
		false ->
			case Session#vdf_session.step_number < CurrentStepNumber of
				true ->
					{noreply, State};
				false ->
					{noreply, push_update(SessionKey, Session, PrevSessionKey,
							PrevSession, Output, PartitionUpperBound,
							Peer, Format, State)}
			end
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

push_update(SessionKey, Session, PrevSessionKey, PrevSession, Output, PartitionUpperBound,
		Peer, Format, State) ->
	Update = ar_nonce_limiter_server:make_nonce_limiter_update(
		SessionKey,
		Session#vdf_session{
			upper_bound = PartitionUpperBound,
			steps = [Output]
		},
		true),
	StepNumber = Session#vdf_session.step_number,
	case ar_http_iface_client:push_nonce_limiter_update(Peer, Update, Format) of
		ok ->
			State;
		{ok, Response} ->
			RequestedFormat = Response#nonce_limiter_update_response.format,
			Postpone = Response#nonce_limiter_update_response.postpone,
			SessionFound = Response#nonce_limiter_update_response.session_found,
			RequestedStepNumber = Response#nonce_limiter_update_response.step_number,

			case { 
					RequestedFormat == Format,
					Postpone == 0,
					SessionFound,
					RequestedStepNumber >= StepNumber - 1
			} of
				{false, _, _, _} ->
					%% Client requested a different payload format
					?LOG_DEBUG([{event, vdf_client_requested_different_format},
						{peer, ar_util:format_peer(Peer)},
						{format, Format}, {requested_format, RequestedFormat}]),
					push_update(SessionKey, Session, PrevSessionKey, PrevSession,
							Output, PartitionUpperBound, Peer, RequestedFormat,
							State#state{ format = RequestedFormat });
				{true, false, _, _} ->
					%% Client requested we pause updates
					Now = os:system_time(second),
					State#state{ pause_until = Now + Postpone };
				{true, true, false, _} ->
					%% Client requested the full session
					case PrevSession of
						undefined ->
							ok;
						_ ->
							push_session(PrevSessionKey, PrevSession, Peer, Format)
					end,
					push_session(SessionKey, Session, Peer, Format),
					State;
				{true, true, true, false} ->
					%% Client requested missing steps
					push_session(SessionKey, Session, Peer, Format),
					State;
				_ ->
					%% Client is ahead of the server
					State
			end;
		{error, Error} ->
			?LOG_WARNING([{event, failed_to_push_nonce_limiter_update_to_peer},
					{peer, ar_util:format_peer(Peer)},
					{reason, io_lib:format("~p", [Error])}]),
			State
	end.

push_session(SessionKey, Session, Peer, Format) ->
	Update = ar_nonce_limiter_server:make_nonce_limiter_update(SessionKey, Session, false),
	{SessionSeed, SessionInterval, _NextVDFDifficulty} = SessionKey,
	case ar_http_iface_client:push_nonce_limiter_update(Peer, Update, Format) of
		ok ->
			ok;
		{ok, #nonce_limiter_update_response{ step_number = ClientStepNumber,
				session_found = ReportedSessionFound }} ->
			?LOG_WARNING([{event, failed_to_push_nonce_limiter_update_to_peer},
					{peer, ar_util:format_peer(Peer)},
					{session_seed, ar_util:encode(SessionSeed)},
					{session_interval, SessionInterval},
					{session_found, ReportedSessionFound},
					{client_step_number, ClientStepNumber},
					{server_step_number, Session#vdf_session.step_number}]);
		{error, Error} ->
			?LOG_WARNING([{event, failed_to_push_nonce_limiter_update_to_peer},
				{peer, ar_util:format_peer(Peer)},
				{session_seed, ar_util:encode(SessionSeed)},
				{session_interval, SessionInterval},
				{reason, io_lib:format("~p", [Error])},
				{server_step_number, Session#vdf_session.step_number}])
	end.
