%%%===================================================================
%%% @doc Handle http requests.
%%%===================================================================

-module(ar_http_iface_server).

-export([start/0, stop/0]).
-export([split_path/1, label_http_path/1, label_req/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(HTTP_IFACE_MIDDLEWARES, [
	ar_blacklist_middleware,
	ar_network_middleware,
	cowboy_router,
	ar_http_iface_middleware,
	cowboy_handler
]).

-define(HTTP_IFACE_ROUTES, [
	{"/metrics/[:registry]", ar_prometheus_cowboy_handler, []},
	{"/[...]", ar_http_iface_handler, []}
]).

-define(ENDPOINTS, ["info", "block", "block_announcement", "block2", "tx", "tx2",
		"queue", "recent_hash_list", "recent_hash_list_diff", "tx_anchor", "arql", "time",
		"chunk", "chunk2", "data_sync_record", "sync_buckets", "wallet", "unsigned_tx",
		"peers", "hash_list", "block_index", "block_index2", "total_supply", "wallet_list",
		"height", "metrics", "rates", "vdf", "vdf2", "partial_solution", "pool_cm_jobs"]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

split_path(Path) ->
	binary:split(Path, <<"/">>, [global, trim_all]).

%% @doc Return the HTTP path label,
%% Used for cowboy_requests_total and gun_requests_total metrics, as well as P3 handling.
label_http_path(Path) when is_list(Path) ->
	name_route(Path);
label_http_path(Path) ->
	label_http_path(split_path(Path)).

label_req(Req) ->
	SplitPath = ar_http_iface_server:split_path(cowboy_req:path(Req)),
	ar_http_iface_server:label_http_path(SplitPath).

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Start the server
start() ->
	{ok, Config} = application:get_env(arweave, config),
	Semaphores = Config#config.semaphores,
	maps:map(
		fun(Name, N) ->
			ok = ar_semaphore:start_link(Name, N)
		end,
		Semaphores
	),
	ok = ar_blacklist_middleware:start(),
	ok = start_http_iface_listener(Config),
	ok.

start_http_iface_listener(Config) ->
	Dispatch = cowboy_router:compile([{'_', ?HTTP_IFACE_ROUTES}]),
	TlsCertfilePath = Config#config.tls_cert_file,
	TlsKeyfilePath = Config#config.tls_key_file,
	TransportOpts = [
		{port, Config#config.port},
		{keepalive, true},
		{max_connections, Config#config.max_connections}
	],
	ProtocolOpts = #{
		inactivity_timeout => 120000,
		idle_timeout => 30000,
		middlewares => ?HTTP_IFACE_MIDDLEWARES,
		env => #{ dispatch => Dispatch },
		metrics_callback => fun prometheus_cowboy2_instrumenter:observe/1,
		stream_handlers => [cowboy_metrics_h, cowboy_stream_h]
	},
	case TlsCertfilePath of
		not_set ->
			{ok, _} = cowboy:start_clear(ar_http_iface_listener, TransportOpts, ProtocolOpts);
		_ ->
			{ok, _} = cowboy:start_tls(ar_http_iface_listener, TransportOpts ++ [
				{certfile, TlsCertfilePath},
				{keyfile, TlsKeyfilePath}
			], ProtocolOpts)
	end,
	ok.

%%--------------------------------------------------------------------
%% @doc stop the application and execute the shutdown procedure.
%% @end
%%--------------------------------------------------------------------
stop() ->
	{ok, Config} = application:get_env(arweave, config),
	Delay = Config#config.shutdown_tcp_connection_timeout,
	ok = ranch:suspend_listener(ar_http_iface_listener),
	Pids = terminate_connections(Delay),
	terminate_listener(Pids, Delay).

%%--------------------------------------------------------------------
%% @doc ensure all connections have been closed before stopping the
%%      cowboy listener.
%% @end
%%--------------------------------------------------------------------
terminate_listener(Pids, Delay) ->
	AfterDelay = Delay*3,
	receive
		{'DOWN', Ref, process, Pid, _} ->
			Filter = fun({Pid, Ref}) -> false; (_) -> true end,
			NewPids = lists:filter(Filter, Pids),
			terminate_listener(NewPids, Delay);
		_ -> terminate_listener(Pids, Delay)
	after
		AfterDelay ->
			cowboy:stop_listener(ar_http_iface_listener)
	end.

%%--------------------------------------------------------------------
%% @doc terminate all connections. A delay can be set to force close
%%      or kill the remaining connections.
%% @end
%%--------------------------------------------------------------------
-spec terminate_connections(Delay) -> Return when
	Delay :: pos_integer(),
	Return :: [{pid(), reference()}, ...].

terminate_connections(Delay) ->
	Processes = ranch:procs(ar_http_iface_listener, connections),
	[ terminate_connection(P, Delay) || P <- Processes ].

%%--------------------------------------------------------------------
%% @doc terminate a ranch/cowboy connection. this follows the drain
%%      procedure explaining in ranch and cowboy documentation.
%% @end
%%--------------------------------------------------------------------
-spec terminate_connection(Connection, Delay) -> Return when
	Connection :: ranch:ref(),
	Delay :: pos_integer(),
	Return :: pid().

terminate_connection(Connection, Delay) ->
	spawn_monitor(fun() ->
		terminate_connection_init(Connection, Delay)
	end).

%%--------------------------------------------------------------------
%% @hidden
%% @doc terminate connection init procedure.
%% @end
%%--------------------------------------------------------------------
-spec terminate_connection_init(Connection, Delay) -> Return when
	Connection :: ranch:ref(),
	Delay :: pos_integer(),
	Return :: pid().

terminate_connection_init(Connection, Delay) ->
	% let monitor the connection first. a ranch connection is a process
	% used as "interface" to a socket. the procedure must know if the
	% process is still alive.
	_Ref = erlang:monitor(process, Connection),

	% it seems ranch_tcp does not have a way to "kill" or "close" the
	% connection from the process. The port needs to be extracted, to
	% accomplish that, one can check using process_info/2 functions or
	% extract the process state with sys:get_state/1.
	{links, Links} = erlang:process_info(Connection, links),

	% in normal situation, a ranch connection process must have only one
	% port (socket). If it's not the case, this is abnormal.
	[Socket|_] = [ P || P <- Links, is_port(P) ],

	% let check if information about the sockets can be retrieved, in this
	% case, the address/port is the information required. It also check if
	% the socket is still active. If an exception is returned, we assume
	% the socket is already down.
	try inet:peername(Socket) of
		{ok, {PeerAddress, PeerPort}} ->
			% let  prepare the state used for the termination loop procedure.
			Peer = string:join([inet:ntoa(PeerAddress), integer_to_list(PeerPort)], ":"),
			State = #{
				socket => Socket,
				peer => Peer,
				delay => Delay,
				connection => Connection
			},

			% now everything is ready, the socket is set in read-only mode. the
			% remote peer will receive this information and will try to fetch
			% the last piece of data from the buffer. It does not guarantee the
			% socket will be closed, in particular if the connection is slow.
			logger:warning(#{ connection => Peer, socket => Connection, reason => "shutdown", msg => "shutdown (read-only)"}),
			ranch_tcp:shutdown(Socket, write),

			% in this case, two timeouts are configured, one simply closing the
			% socket to announce another time to the remote peer that the node is
			% shutting down...
			{ok, _} = timer:send_after(Delay, {timeout, close}),

			% ... and a second delay. This time, the socket is killed. linger
			% option is set to 0 on the port and we reclose the socket.
			{ok, _} = timer:send_after(Delay*2, {timeout, kill}),

			% let start the terminate loop to shutdown properly the connection
			terminate_connection_loop(State);
		_ ->
			ok
	catch
		_:_ ->
			ok
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @doc shutdown procedure loop, in charge of dealing with monitoring
%%      messages and timeouts.
%% @end
%%--------------------------------------------------------------------
-spec terminate_connection_loop(State) -> Return when
	Socket :: port(),
	Connection :: pid(),
	Peer :: string(),
	Delay :: pos_integer(),
	State :: #{
		socket => Socket,
		connection => Connection,
		peer => Peer,
		delay => Delay
	},
	Return :: ok.

terminate_connection_loop(State) ->
	Socket = maps:get(socket, State),
	Connection = maps:get(connection, State),
	Peer = maps:get(peer, State),
	Delay = maps:get(delay, State),
	AfterDelay = Delay*3,
	receive
		{timeout, close} ->
			% the first timeout simply close the socket a
			% socket time.
			logger:warning(#{ connection => Peer, socket => Connection, reason => "shutdown", msg => "closed (timeout)"}),
			ranch_tcp:close(Socket),
			terminate_connection_loop(State);

		{timeout, kill} ->
			% the second timeout set linger tcp parameter
			% to 0 and close the connection (again). At
			% this stage, the connection should be closed.
			% and we should receive a message from
			% monitoring.
			logger:warning(#{ connection => Peer, socket => Connection, reason => "shutdown", msg => "killed (timeout)"}),
			ranch_tcp:setopts(Socket, [{linger, {true, 0}}]),
			ranch_tcp:close(Socket),
			terminate_connection_loop(State);

		{'DOWN', _Ref, process, Connection, _} ->
			logger:notice(#{ connection => Peer, socket => Connection, reason => "shutdown", msg => "terminated"});
	Msg ->
		logger:warning("received: ~p", [Msg])
	after
		AfterDelay ->
			logger:error(#{ connection => Peer, socket => Connection, reason => "shutdown", msg => "can't kill"})
	end.

name_route([]) ->
	"/";
name_route([<<"current_block">>]) ->
	"/current/block";
name_route([<<_Hash:43/binary, _MaybeExt/binary>>]) ->
	"/{hash}[.{ext}]";
name_route([Bin]) ->
	L = binary_to_list(Bin),
	case lists:member(L, ?ENDPOINTS) of
		true ->
			"/" ++ L;
		false ->
			undefined
	end;
name_route([<<"peer">> | _]) ->
	"/peer/...";

name_route([<<"jobs">>, _PrevOutput]) ->
	"/jobs/{prev_output}";

name_route([<<"vdf">>, <<"session">>]) ->
	"/vdf/session";
name_route([<<"vdf2">>, <<"session">>]) ->
	"/vdf2/session";
name_route([<<"vdf3">>, <<"session">>]) ->
	"/vdf3/session";
name_route([<<"vdf4">>, <<"session">>]) ->
	"/vdf4/session";

name_route([<<"vdf">>, <<"previous_session">>]) ->
	"/vdf/previous_session";
name_route([<<"vdf2">>, <<"previous_session">>]) ->
	"/vdf2/previous_session";
name_route([<<"vdf4">>, <<"previous_session">>]) ->
	"/vdf4/previous_session";

name_route([<<"tx">>, <<"pending">>]) ->
	"/tx/pending";
name_route([<<"tx">>, _Hash, <<"status">>]) ->
	"/tx/{hash}/status";
name_route([<<"tx">>, _Hash]) ->
	"/tx/{hash}";
name_route([<<"tx2">>, _Hash]) ->
	"/tx2/{hash}";
name_route([<<"unconfirmed_tx">>, _Hash]) ->
	"/unconfirmed_tx/{hash}";
name_route([<<"unconfirmed_tx2">>, _Hash]) ->
	"/unconfirmed_tx2/{hash}";
name_route([<<"tx">>, _Hash, << "data" >>]) ->
	"/tx/{hash}/data";
name_route([<<"tx">>, _Hash, << "data.", _/binary >>]) ->
	"/tx/{hash}/data.{ext}";
name_route([<<"tx">>, _Hash, << "offset" >>]) ->
	"/tx/{hash}/offset";
name_route([<<"tx">>, _Hash, _Field]) ->
	"/tx/{hash}/{field}";

name_route([<<"chunk">>, _Offset]) ->
	"/chunk/{offset}";
name_route([<<"chunk2">>, _Offset]) ->
	"/chunk2/{offset}";

name_route([<<"chunk_proof">>, _Offset]) ->
	"/chunk_proof/{offset}";
name_route([<<"chunk_proof2">>, _Offset]) ->
	"/chunk_proof2/{offset}";

name_route([<<"data_sync_record">>, _Start, _Limit]) ->
	"/data_sync_record/{start}/{limit}";

name_route([<<"price">>, _SizeInBytes]) ->
	"/price/{bytes}";
name_route([<<"price">>, _SizeInBytes, _Addr]) ->
	"/price/{bytes}/{address}";

name_route([<<"price2">>, _SizeInBytes]) ->
	"/price2/{bytes}";
name_route([<<"price2">>, _SizeInBytes, _Addr]) ->
	"/price2/{bytes}/{address}";

name_route([<<"v2price">>, _SizeInBytes]) ->
	"/v2price/{bytes}";
name_route([<<"v2price">>, _SizeInBytes, _Addr]) ->
	"/v2price/{bytes}/{address}";

name_route([<<"optimistic_price">>, _SizeInBytes]) ->
	"/optimistic_price/{bytes}";
name_route([<<"optimistic_price">>, _SizeInBytes, _Addr]) ->
	"/optimistic_price/{bytes}/{address}";

name_route([<<"reward_history">>, _BH]) ->
	"/reward_history/{block_hash}";

name_route([<<"block_time_history">>, _BH]) ->
	"/block_time_history/{block_hash}";

name_route([<<"wallet">>, _Addr, <<"balance">>]) ->
	"/wallet/{addr}/balance";
name_route([<<"wallet">>, _Addr, <<"last_tx">>]) ->
	"/wallet/{addr}/last_tx";
name_route([<<"wallet">>, _Addr, <<"txs">>]) ->
	"/wallet/{addr}/txs";
name_route([<<"wallet">>, _Addr, <<"txs">>, _EarliestTX]) ->
	"/wallet/{addr}/txs/{earliest_tx}";
name_route([<<"wallet">>, _Addr, <<"deposits">>]) ->
	"/wallet/{addr}/deposits";
name_route([<<"wallet">>, _Addr, <<"deposits">>, _EarliestDeposit]) ->
	"/wallet/{addr}/deposits/{earliest_deposit}";

name_route([<<"wallet_list">>, _Root]) ->
	"/wallet_list/{root_hash}";
name_route([<<"wallet_list">>, _Root, _Cursor]) ->
	"/wallet_list/{root_hash}/{cursor}";
name_route([<<"wallet_list">>, _Root, _Addr, <<"balance">>]) ->
	"/wallet_list/{root_hash}/{addr}/balance";

name_route([<<"block_index">>, _From, _To]) ->
	"/block_index/{from}/{to}";
name_route([<<"block_index2">>, _From, _To]) ->
	"/block_index2/{from}/{to}";
name_route([<<"hash_list">>, _From, _To]) ->
	"/hash_list/{from}/{to}";
name_route([<<"hash_list2">>, _From, _To]) ->
	"/hash_list2/{from}/{to}";

name_route([<<"block">>, <<"hash">>, _IndepHash]) ->
	"/block/hash/{indep_hash}";
name_route([<<"block">>, <<"height">>, _Height]) ->
	"/block/height/{height}";
name_route([<<"block2">>, <<"hash">>, _IndepHash]) ->
	"/block2/hash/{indep_hash}";
name_route([<<"block2">>, <<"height">>, _Height]) ->
	"/block2/height/{height}";
name_route([<<"block">>, _Type, _IDBin, _Field]) ->
	"/block/{type}/{id_bin}/{field}";
name_route([<<"block">>, <<"height">>, _Height, <<"wallet">>, _Addr, <<"balance">>]) ->
	"/block/height/{height}/wallet/{addr}/balance";
name_route([<<"block">>, <<"current">>]) ->
	"/block/current";

name_route([<<"balance">>, _Addr, _Network, _Token]) ->
	"/balance/{address}/{network}/{token}";
name_route([<<"rates">>]) ->
	"/rates";

name_route([<<"coordinated_mining">>, <<"h1">>]) ->
	"/coordinated_mining/h1";
name_route([<<"coordinated_mining">>, <<"h2">>]) ->
	"/coordinated_mining/h2";
name_route([<<"coordinated_mining">>, <<"partition_table">>]) ->
	"/coordinated_mining/partition_table";
name_route([<<"coordinated_mining">>, <<"publish">>]) ->
	"/coordinated_mining/publish";
name_route([<<"coordinated_mining">>, <<"state">>]) ->
	"/coordinated_mining/state";


name_route(_) ->
	undefined.
