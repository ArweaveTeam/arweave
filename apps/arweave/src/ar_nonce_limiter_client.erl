-module(ar_nonce_limiter_client).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {
	remote_servers
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	case ar_config:use_remote_vdf_server() andalso ar_config:pull_from_remote_vdf_server() of
		false ->
			ok;
		true ->
			gen_server:cast(?MODULE, pull)
	end,
	{ok, Config} = application:get_env(arweave, config),
	RawPeers = queue:from_list(Config#config.nonce_limiter_server_trusted_peers),
	{ok, #state{ remote_servers = RawPeers }}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(pull, State) ->
	ar_util:cast_after(800, ?MODULE, pull),
	#state{ remote_servers = Q } = State,
	{{value, RawPeer}, Q2} = queue:out(Q),
	State2 = State#state{ remote_servers = queue:in(RawPeer, Q2) },
	case resolve_peer(RawPeer) of
		error ->
			%% Push the peer to the back of the queue.
			{noreply, State2};
		{ok, Peer} ->
			case ar_http_iface_client:get_vdf_update(Peer) of
				{ok, Update} ->
					#nonce_limiter_update{ session = #vdf_session{
							step_number = SessionStepNumber } } = Update,
					case ar_nonce_limiter:apply_external_update(Update, Peer) of
						ok ->
							{noreply, State};
						#nonce_limiter_update_response{ session_found = false } ->
							case fetch_and_apply_session_and_previous_session(Peer) of
								ok ->
									{noreply, State};
								_ ->
									{noreply, State2}
							end;
						#nonce_limiter_update_response{ step_number = StepNumber }
								when StepNumber >= SessionStepNumber ->
							%% We are ahead of this server; try another one, if there are any.
							{noreply, State2};
						_ ->
							%% We have received a partial session, but there's a gap in the
							%% step numbers, e.g., the update we received is at step 100,
							%% but our last seen step was 90.
							case fetch_and_apply_session(Peer) of
								ok ->
									{noreply, State};
								_ ->
									{noreply, State2}
							end
					end;
				not_found ->
					%% The server might be restarting. Try another one, if there are any.
					{noreply, State#state{ remote_servers = queue:in(RawPeer, Q2) }};
				{error, Reason} ->
					?LOG_WARNING([{event, failed_to_fetch_vdf_update},
							{peer, ar_util:format_peer(Peer)},
							{error, io_lib:format("~p", [Reason])}]),
					%% Try another server, if there are any.
					{noreply, State#state{ remote_servers = queue:in(RawPeer, Q2) }}
			end
	end;

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

resolve_peer(RawPeer) ->
	case ets:lookup(ar_nonce_limiter, {vdf_server_raw_peer, RawPeer}) of
		[{_, Peer}] ->
			{ok, Peer};
		_ ->
			case ar_util:safe_parse_peer(RawPeer) of
				{ok, Peer} ->
					{ok, Peer};
				_ ->
					error
			end
	end.

fetch_and_apply_session_and_previous_session(Peer) ->
	case ar_http_iface_client:get_vdf_session(Peer) of
		{ok, #nonce_limiter_update{ session = #vdf_session{
				prev_session_key = PrevSessionKey } } = Update} ->
			case ar_http_iface_client:get_previous_vdf_session(Peer) of
				{ok, #nonce_limiter_update{ session_key = PrevSessionKey } = Update2} ->
					ar_nonce_limiter:apply_external_update(Update2, Peer),
					ar_nonce_limiter:apply_external_update(Update, Peer),
					ok;
				{ok, _} ->
					%% The session should have just changed, retry.
					fetch_and_apply_session_and_previous_session(Peer);
				{error, Reason} = Error ->
					?LOG_WARNING([{event, failed_to_fetch_previous_vdf_session},
						{peer, ar_util:format_peer(Peer)},
						{error, io_lib:format("~p", [Reason])}]),
					Error
			end;
		{error, Reason2} = Error2 ->
			?LOG_WARNING([{event, failed_to_fetch_vdf_session},
				{peer, ar_util:format_peer(Peer)},
				{error, io_lib:format("~p", [Reason2])}]),
			Error2
	end.

fetch_and_apply_session(Peer) ->
	case ar_http_iface_client:get_vdf_session(Peer) of
		{ok, Update} ->
			ar_nonce_limiter:apply_external_update(Update, Peer),
			ok;
		not_found ->
			not_found;
		{error, Reason} = Error ->
			?LOG_WARNING([{event, failed_to_fetch_vdf_session},
					{peer, ar_util:format_peer(Peer)},
					{error, io_lib:format("~p", [Reason])}]),
			Error
	end.
