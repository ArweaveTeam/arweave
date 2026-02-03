%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2026 (c) Arweave
%%% @author Arweave Team
%%% @doc
%%% @end
%%%===================================================================
-module(ar_nonce_limiter_client).
-behaviour(gen_server).
-export([start_link/0, maybe_request_sessions/1]).
-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).
-include("ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-record(state, {
	remote_servers,
	latest_session_keys = #{},
	%% request_sessions is set to true when the node is unable to validate a block due
	%% to a gap in its cached step numbers. When true, the node will query the full
	%% session and previous session from a VDF server.
	request_sessions = false,
	latest_remote_server_rotation_timestamp = erlang:system_time(millisecond)
}).

-define(PULL_FREQUENCY_MS, 800).
-define(NO_UPDATE_PULL_FREQUENCY_MS, 200).
-define(PULL_THROTTLE_MS, 200).

-ifdef(AR_TEST).
-define(ROTATE_REMOTE_SERVERS_MS, 2_000).
-else.
-define(ROTATE_REMOTE_SERVERS_MS, 30_000).
-endif.

%%--------------------------------------------------------------------
%% @doc Start the server.
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc Look at the session key of the last update from the VDF server
%% we are  currently working with  and in case  it does not  match the
%% given session key, request complete sessions from this VDF server.
%%
%% The client may need this additional request around a VDF reset when
%% a new session is created but  the previous session is not completed
%% because the VDF server instantiated  the new session before sending
%% the last computed output(s) of the previous session to the client.
%% @end
%%--------------------------------------------------------------------
maybe_request_sessions(SessionKey) ->
	gen_server:cast(?MODULE, {maybe_request_sessions, SessionKey}).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(_) ->
	case ar_config:use_remote_vdf_server() of
		false ->
			ok;
		true ->
			gen_server:cast(?MODULE, pull)
	end,
	{ok, Config} = arweave_config:get_env(),
	RemoteServers = queue:from_list(Config#config.nonce_limiter_server_trusted_peers),
	{ok, #state{
		remote_servers = RemoteServers
	}}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_call(Request, _From, State) ->
	?LOG_WARNING([
		{event, unhandled_call},
		{module, ?MODULE},
		{request, Request}
	]),
	{reply, ok, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_cast(pull, State = #state{ request_sessions = RequestSessions }) ->
	DoPull = (
		ar_config:pull_from_remote_vdf_server() orelse
		RequestSessions == true
	),
	case DoPull of
		true ->
			{Delay, State1} = do_pull(State),
			ar_util:cast_after(Delay, ?MODULE, pull),
			{noreply, State1};
		false ->
			ar_util:cast_after(?PULL_FREQUENCY_MS, ?MODULE, pull),
			{noreply, State}
	end;
handle_cast({maybe_request_sessions, SessionKey}, State) ->
	#state{ remote_servers = Q } = State,
	{{value, RawPeer}, _Q2} = queue:out(Q),
	case ar_peers:resolve_and_cache_peer(RawPeer, vdf_server_peer) of
		{error, _} ->
   			%% Push the peer to the back of  the queue. We'll also
			%% wait and see if another  `maybe_request_sessions` 
			%% message comes in  before we fetch the full session.
			{noreply, handle_rotate_servers(State)};
		{ok, Peer} ->
			case get_latest_session_key(Peer, State) of
				SessionKey ->
					%% No reason to make extra requests.
					%% And don't rotate the peers.
					{noreply, State};
				_ ->
					%% Ensure the current and previous sessions are fetched and applied on
					%% the next `pull` message.
					?LOG_DEBUG([
						{event, vdf_request_sessions},
						{peer, ar_util:format_peer(Peer)},
						{session_key, ar_nonce_limiter:encode_session_key(SessionKey)}
					]),
					{noreply, State#state{ request_sessions = true }}
			end
	end;
handle_cast({update_latest_session_key, Peer, SessionKey}, State) ->
	{noreply, update_latest_session_key(Peer, SessionKey, State)};
handle_cast(Cast, State) ->
	?LOG_WARNING([
		{event, unhandled_cast},
		{module, ?MODULE},
		{cast, Cast}
	]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_info(Message, State) ->
	?LOG_WARNING([
		{event, unhandled_info},
		{module, ?MODULE},
		{message, Message}
	]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
terminate(Reason, _State) ->
	?LOG_INFO([
		{module, ?MODULE},
		{pid, self()},
		{callback, terminate},
		{reason, Reason}
	]),
	ok.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
do_pull(State) ->
	LocalState = #{
		state => State
	},
	do_pull2(LocalState).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
do_pull2(LocalState = #{ state := State }) ->
	{RawPeer, State2} = get_raw_peer_and_update_remote_servers(State),
	case ar_peers:resolve_and_cache_peer(RawPeer, vdf_server_peer) of
		{ok, Peer} ->
			NewLocalState = LocalState#{
				raw_peer => RawPeer,
				peer => Peer,
				state => State2
			},
			handle_vdf_update(NewLocalState);
		{error, _} ->
			?LOG_WARNING([
				{event, failed_to_resolve_peer},
				{raw_peer, io_lib:format("~p", [RawPeer])}
			]),
			%% Push the peer to the back of the queue.
			{0, handle_rotate_servers(State)}
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_vdf_update(LocalState) ->
	State = maps:get(state, LocalState),
	Peer = maps:get(peer, LocalState),
	case ar_http_iface_client:get_vdf_update(Peer) of
		{ok, Update} ->
			#nonce_limiter_update{
				session_key = SessionKey,
				session = #vdf_session{
					step_number = SessionStepNumber
				}
			} = Update,
			State2 = update_latest_session_key(Peer, SessionKey, State),
			UpdateResponse = ar_nonce_limiter:apply_external_update(Update, Peer),
			SessionFound = case UpdateResponse of
				#nonce_limiter_update_response{ session_found = false } ->
					false;
				_ ->
					true
			end,
			RequestSessions = (
				State2#state.request_sessions == true orelse
				not SessionFound
			),
			NewLocalState = #{
				update => Update,
				session_step_number => SessionStepNumber,
				state => State2,
				update_response => UpdateResponse,
				session_found => SessionFound,
				request_sessions => RequestSessions
			},
			handle_request_sessions(NewLocalState);
		{error, not_found} ->
			?LOG_WARNING([
				{event, failed_to_fetch_vdf_update},
				{peer, ar_util:format_peer(Peer)},
				{error, not_found}
			]),
			%% The server might be restarting.
			%% Try another one, if there are any.
			{0, handle_rotate_servers(State)};
		{error, Reason} ->
			?LOG_WARNING([
				{event, failed_to_fetch_vdf_update},
				{peer, ar_util:format_peer(Peer)},
				{error, io_lib:format("~p", [Reason])}
			]),
			%% Try another server, if there are any.
			{0, handle_rotate_servers(State)}
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_request_sessions(LocalState) ->
	State = maps:get(state, LocalState),
	Peer = maps:get(peer, LocalState),
	RequestSessions = maps:get(request_sessions, LocalState),
	case RequestSessions of
		true ->
			case fetch_and_apply_session_and_previous_session(Peer) of
				{error, _} ->
					{0, handle_rotate_servers(State)};
				_ ->
					NewState = State#state{
						request_sessions = false
					},
					{?PULL_FREQUENCY_MS, NewState}
			end;
		false ->
			handle_update_response(LocalState)
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_update_response(LocalState) ->
	Peer = maps:get(peer, LocalState),
	State = maps:get(state, LocalState),
	UpdateResponse = maps:get(update_response, LocalState),
	SessionStepNumber = maps:get(session_step_number, LocalState),
	case UpdateResponse of
		ok ->
			{?PULL_FREQUENCY_MS, State};

		#nonce_limiter_update_response{ step_number = StepNumber }
			when StepNumber > SessionStepNumber ->
				%% We are ahead of the server - may be, it is not
				%% the fastest server in the list so try another one,
				%% if there are more servers in the configuration
				%% and they are not on timeout.
				{0, handle_rotate_servers(State)};

		#nonce_limiter_update_response{ step_number = StepNumber }
			when StepNumber == SessionStepNumber ->
				%% We are in sync with the server. Re-try soon.
				{?NO_UPDATE_PULL_FREQUENCY_MS, State};

		_ ->
			%% We have received a partial session, but there's a gap
			%% in the step numbers, e.g., the update we received is at
			%% step 100, but our last seen step was 90.
			case fetch_and_apply_session(Peer) of
				{error, _} ->
					{0, handle_rotate_servers(State)};
				_ ->
					{?PULL_FREQUENCY_MS, State}
			end
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
get_raw_peer_and_update_remote_servers(State) ->
	#state{
		remote_servers = Q,
		latest_remote_server_rotation_timestamp = Timestamp
	} = State,
	{{value, RawPeer}, Q2} = queue:out(Q),
	Now = erlang:system_time(millisecond),
	case Now < Timestamp + ?ROTATE_REMOTE_SERVERS_MS of
		true ->
			{RawPeer, State};
		false ->
			{RawPeer, State#state{
				latest_remote_server_rotation_timestamp = Now,
				remote_servers = queue:in(RawPeer, Q2)
			}}
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_rotate_servers(State = #state{ remote_servers = Q }) ->
	State#state{
		remote_servers = rotate_servers(Q)
	}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
rotate_servers(Q) ->
	case queue:out(Q) of
		{{value, RawPeer}, Q2} ->
			queue:out(Q),
			queue:in(RawPeer, Q2);
		_ ->
			throw({error, queue})
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
fetch_and_apply_session_and_previous_session(Peer) ->
	case ar_http_iface_client:get_vdf_session(Peer) of
		{ok, Update = #nonce_limiter_update{
			session = #vdf_session{
				prev_session_key = PrevSessionKey } 
		       }
		} ->
			case ar_http_iface_client:get_previous_vdf_session(Peer) of
				{ok, #nonce_limiter_update{ session_key = PrevSessionKey } = Update2} ->
					ar_nonce_limiter:apply_external_update(Update2, Peer),
					ar_nonce_limiter:apply_external_update(Update, Peer);
				{ok, _} ->
					%% The session should have just changed, retry.
					fetch_and_apply_session_and_previous_session(Peer);
				{error, Reason} = Error ->
					?LOG_WARNING([
						{event, failed_to_fetch_previous_vdf_session},
						{peer, ar_util:format_peer(Peer)},
						{error, io_lib:format("~p", [Reason])}
					]),
					Error
			end;
		{error, Reason2} = Error2 ->
			?LOG_WARNING([
				{event, failed_to_fetch_vdf_session},
				{peer, ar_util:format_peer(Peer)},
				{error, io_lib:format("~p", [Reason2])
			}]),
			Error2
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
fetch_and_apply_session(Peer) ->
	case ar_http_iface_client:get_vdf_session(Peer) of
		{ok, Update} ->
			ar_nonce_limiter:apply_external_update(Update, Peer);
		{error, Reason} = Error ->
			?LOG_WARNING([
				{event, failed_to_fetch_vdf_session},
				{peer, ar_util:format_peer(Peer)},
				{error, io_lib:format("~p", [Reason])}
			]),
			Error
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
get_latest_session_key(Peer, State) ->
	#state{ latest_session_keys = Map } = State,
	maps:get(Peer, Map, not_found).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
update_latest_session_key(Peer, SessionKey, State) ->
	#state{ latest_session_keys = Map } = State,
	State#state{ latest_session_keys = maps:put(Peer, SessionKey, Map) }.
