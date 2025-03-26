%%%===================================================================
%%% @doc Handle http requests.
%%%===================================================================

-module(ar_http_iface_server).
-behavior(gen_server).

-export([start_link/0]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([split_path/1, label_http_path/1, label_req/1]).
-export([prep_stop/1]).

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
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
	?LOG_INFO([{start, ?MODULE}, {pid, self()}]),
	% this process needs to be stopped in a clean way,
	% if something goes wrong, the connections must
	% be cleaned before leaving.
	erlang:process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	case start_http_iface_listener(Config) of
		{ok, Pid} -> {ok, Pid};
		Elsewise -> {error, Elsewise}
	end.

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

handle_call(Msg, From, State) ->
	?LOG_WARNING([{process, ?MODULE}, {received, Msg}, {from, From}]),
	{noreply, State}.

handle_cast(Msg, State) ->
	?LOG_WARNING([{process, ?MODULE}, {received, Msg}]),
	{noreply, State}.

handle_info(Msg = {'EXIT', _From, Reason}, _State) ->
	?LOG_ERROR([{process, ?MODULE}, {received, Msg}]),
	{stop, Reason};
handle_info(Msg, State) ->
	?LOG_WARNING([{process, ?MODULE}, {received, Msg}]),
	{noreply, State}.

%%%===================================================================
%%% Private functions.
%%%===================================================================
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
		idle_timeout => Config#config.http_api_transport_idle_timeout,
		middlewares => ?HTTP_IFACE_MIDDLEWARES,
		env => #{ dispatch => Dispatch },
		metrics_callback => fun prometheus_cowboy2_instrumenter:observe/1,
		stream_handlers => [cowboy_metrics_h, cowboy_stream_h]
	},
	case TlsCertfilePath of
		not_set ->
			cowboy:start_clear(ar_http_iface_listener, TransportOpts, ProtocolOpts);
		_ ->
			cowboy:start_tls(ar_http_iface_listener, TransportOpts ++ [
				{certfile, TlsCertfilePath},
				{keyfile, TlsKeyfilePath}
			], ProtocolOpts)
	end.

terminate(_Reason, State) ->
	prep_stop(State).

%%--------------------------------------------------------------------
%% @doc stop the application and execute the shutdown procedure.
%% @end
%%--------------------------------------------------------------------
-spec prep_stop(State) -> Return when
	State :: any(),
	Return :: any.

prep_stop(_State) ->
	% During the shutdown process of this part of the
	% code, the caller must not close until every connections
	% have been closed or killed.
	{ok, Config} = application:get_env(arweave, config),
	Delay = 1000*Config#config.shutdown_tcp_connection_timeout,
	ok = ranch:suspend_listener(ar_http_iface_listener),
	Pids = terminate_connections(Delay),
	terminate_listener(Pids, Delay),
	?LOG_INFO([{stop, ?MODULE}]).

%%--------------------------------------------------------------------
%% @doc ensure all connections have been closed before stopping the
%%      cowboy listener.
%% @end
%%--------------------------------------------------------------------
-spec terminate_listener(Pids, Delay) -> Return when
	  Pids :: [pid()],
	  Delay :: pos_integer(),
	  Return :: ok.

terminate_listener([], _Delay) ->
	?LOG_INFO("All tcp connections have been closed."),
	cowboy:stop_listener(ar_http_iface_listener);
terminate_listener(Pids, Delay) ->
	%
	receive
		{'DOWN', Ref, process, Pid, _} ->
			% one of the process spawned to close tcp connections
			% has been stopped, it can be removed from the PID
			% list safely.
			Filter = fun
				({P, R}) when P =:= Pid, R =:= Ref -> false;
				(_) -> true
			end,
			NewPids = lists:filter(Filter, Pids),
			terminate_listener(NewPids, Delay);

		Elsewise ->
			% received an abnormal message, it will be logged
			% but the loop should not be broken until all PIDs
			% are still up. If the message is important, in doubt
			% we put it at the end of the mailbox.
			?LOG_ERROR([{msg, Elsewise}]),
			self() ! Elsewise,
			terminate_listener(Pids, Delay)

	after
		(?SHUTDOWN_TCP_MAX_CONNECTION_TIMEOUT*1000) ->
			terminate_listener([], Delay)
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
%%      procedure explained in ranch and cowboy documentation.
%% @end
%%--------------------------------------------------------------------
-spec terminate_connection(Connection, Delay) -> Return when
	Connection :: ranch:ref(),
	Delay :: pos_integer(),
	Return :: pid().

terminate_connection(Connection, Delay) ->
	% to improve the speed, all terminations are done in
	% their own processes, only monitored by the caller.
	spawn_monitor(fun() ->
		try
			terminate_connection_init(Connection, Delay)
		catch
			E:R ->
				?LOG_ERROR([{error, E}, {reason, R}])
		end
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
	try
		% it seems ranch_tcp does not have a way to "kill" or "close" the
		% connection from the process. The port needs to be extracted, to
		% accomplish that, one can check using process_info/2 functions or
		% extract the process state with sys:get_state/1.
		{links, Links} = erlang:process_info(Connection, links),

		% in normal situation, a ranch connection process must have only one
		% port (socket). If it's not the case, this is abnormal.
		[Socket|_] = [ P || P <- Links, is_port(P) ],

		% let monitor the connection first. a ranch connection is a process
		% used as "interface" to a socket. the procedure must know if the
		% process is still alive.
		Ref = erlang:monitor(port, Socket),

		% let check if information about the sockets can be retrieved, in this
		% case, the address/port is the information required. It also check if
		% the socket is still active. If an exception is returned, we assume
		% the socket is already down.
		{ok, {PeerAddress, PeerPort}} = inet:peername(Socket),

		% let convert the ip address to something one can print.
		Peer = string:join([inet:ntoa(PeerAddress), integer_to_list(PeerPort)], ":"),

		% create the timer for the kill timeout
		{ok, KillRef} = timer:send_after(Delay, {timeout, kill}),
		?LOG_DEBUG([{timer, KillRef}, {msg, "tcp socket kill timeout"}]),

		% now everything is ready, the socket is set in read-only mode. the
		% remote peer will receive this information and will try to fetch
		% the last piece of data from the buffer. It does not guarantee the
		% socket will be closed, in particular if the connection is slow.
		?LOG_INFO([{connection, Peer}, {socket, Connection}, {reason, "shutdown"},
			   {msg, "shutdown (read-only)"}]),
		ranch_tcp:shutdown(Socket, write),

		% let prepare the state used for the termination loop procedure.
		State = #{
			kill_reference => KillRef,
			reference => Ref,
			socket => Socket,
			peer => Peer,
			delay => Delay,
			connection => Connection
		},

		% let start the terminate loop to shutdown properly the connection
		terminate_connection_loop(State)
	catch
		E:R ->
			?LOG_ERROR([{error, E}, {reason, R}])
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
	MonitorRef :: reference(),
	State :: #{
		socket => Socket,
		connection => Connection,
		peer => Peer,
		delay => Delay,
		ref => MonitorRef
	},
	Return :: ok.

terminate_connection_loop(State) ->
	AfterDelay = maps:get(delay, State)*2,
	Ref = maps:get(reference, State),
	Socket = maps:get(socket, State),
	Connection = maps:get(connection, State),
	Peer = maps:get(peer, State),
	receive
		{'DOWN', Ref, port, Socket, _} ->
			% the ranch process monitored is down, the loop
			% now can be stopped.
			?LOG_INFO([{connection, Peer}, {socket, Connection}, {reason, "shutdown"},
				{msg, "terminated (normal)"}]),
			ok;

		{timeout, kill} ->
			% this timeout set linger tcp parameter
			% to 0 and close the connection (again). At
			% this stage, the connection should be closed.
			% and we should receive a message from
			% monitoring.
			?LOG_WARNING([{connection, Peer}, {socket, Connection}, {reason, "shutdown"},
				{msg, "kill (timeout)"}]),
			ranch_tcp:setopts(Socket, [{linger, {true, 0}}]),
			ranch_tcp:close(Socket),
			terminate_connection_loop(State);

		Msg ->
			% a message without matching one of the patterns.
			% this is abnormal but it should not crash the process
			% until the end of the shutdown procedure.
			?LOG_WARNING([{received, Msg}]),
			terminate_connection_loop(State)

	after
		AfterDelay ->
			% the ranch process is still up, even after
			% the shutdown procedure. This is abnormal, and
			% it must be logged. Instead of doing something
			% clean, the process is directly killed and the
			% loop stopped.
			?LOG_ERROR([{connection, Peer}, {socket, Connection}, {reason, "shutdown"},
				{msg, "can't kill"}]),
			erlang:port_close(Socket)
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
