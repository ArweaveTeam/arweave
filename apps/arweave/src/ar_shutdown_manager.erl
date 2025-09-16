%%%===================================================================
%%% This Source Code Form is subject to the terms of the GNU General
%%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%%% with this file, You can obtain one at
%%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html
%%%
%%% @doc Arweave Shutdown Manager.
%%%
%%% This module was created to ensure all remaining connections or
%%% processes related to arweave have been correctly terminated. This
%%% process should be started first and stopped last.
%%%
%%% The module export few functions to help diagnose arweave network
%%% connections.
%%%
%%% When arweave application is stopped, this application should
%%% receive `shutdown' due to trap exist. In this situation, terminate
%%% functon is then called.
%%%
%%% @end
%%%===================================================================
-module(ar_shutdown_manager).
-export([start_link/0]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([
	apply/3,
	apply/4,
	connections/0,
	connections/1,
	list_connections/0,
	list_connections/1,
	shutdown/0,
	socket_info/1,
	socket_info/2,
	start_killer/1,
	state/0,
	terminate_connections/0
]).
-include_lib("eunit/include/eunit.hrl").
-include("ar_config.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
start_link() ->
	start_link(#{}).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%--------------------------------------------------------------------
%% @doc returns service state.
%% @end
%%--------------------------------------------------------------------
-spec state() -> shutdown | running.

state() ->
	case ets:lookup(?MODULE, state) of
		[{state, shutdown}] -> shutdown;
		_ -> running
	end.

%%--------------------------------------------------------------------
%% @doc set state value to shutdown.
%% @end
%%--------------------------------------------------------------------
-spec shutdown() -> boolean().

shutdown() ->
	ets:insert(?MODULE, {state, shutdown}).

%%--------------------------------------------------------------------
%% @doc apply a function only if the service is running.
%% @see apply/4
%% @end
%%--------------------------------------------------------------------
-spec apply(Module, Function, Arguments) -> Return when
	Module :: atom(),
	Function :: atom(),
	Arguments :: [term()],
	Return :: any() | {error, shutdown}.

apply(Module, Function, Arguments) ->
	apply(Module, Function, Arguments, #{}).

%%--------------------------------------------------------------------
%% @doc execute a MFA with extra option for filtering.
%% @end
%%--------------------------------------------------------------------
-spec apply(Module, Function, Arguments, Opts) -> Return when
	Module :: atom(),
	Function :: atom(),
	Arguments :: [term()],
	Opts :: #{ skip_on_shutdown => boolean() },
	Return :: any() | {error, shutdown}.

apply(Module, Function, Arguments, #{ skip_on_shutdown := false }) ->
	erlang:apply(Module, Function, Arguments);
apply(Module, Function, Arguments, Opts) ->
	case state() of
		running ->
			erlang:apply(Module, Function, Arguments);
		shutdown ->
			?LOG_WARNING([
				{state, shutdown},
				{module, Module},
				{function, Function},
				{function, Arguments}
			]),
			{error, shutdown}
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(_Args) ->
	erlang:process_flag(trap_exit, true),
	StartedAt = erlang:system_time(),
	?LOG_INFO([{start, ?MODULE}, {pid, self()}, {started_at, StartedAt}]),
	ets:insert(?MODULE, {state, running}),
	{ok, #{ started_at => StartedAt }}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_call(uptime, _From, State = #{ started_at := StartedAt }) ->
	Now = erlang:system_time(),
	{reply, Now-StartedAt, State};
handle_call(Msg, From, State) ->
	?LOG_WARNING([{message, Msg}, {from, From}]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
	?LOG_WARNING([{message, Msg}]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_info(Msg, State) ->
	?LOG_WARNING([{message, Msg}]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc this function is called when ar_sup is being stopped. If it
%% was correctly configured, this should be the last function to be
%% executed in the supervision tree.
%% @end
%%--------------------------------------------------------------------
terminate(_Reason = shutdown, _State = #{ started_at := StartedAt }) ->
	Now = erlang:system_time(),
	terminate_connections(),
	?LOG_INFO([
		{stop, ?MODULE},
		{started_at, StartedAt},
		{stopped_at, Now},
		{uptime, Now-StartedAt}
	]),
	ok.

%%--------------------------------------------------------------------
%% @hidden
%% @doc terminate all active http connections from ranch/cowboy. This
%% @end
%%--------------------------------------------------------------------
terminate_connections() ->
	% this process should not exit, it will be linked to another
	% process in charge to kill all connections.
	erlang:process_flag(trap_exit, true),

	% list the connections/sockets by target, where target can
	% be gun or cowboy.
	Connections = list_connections(),
	?LOG_DEBUG([{connections, length(Connections)}]),
	case Connections of
		[] ->
			ok;
		Sockets ->
			% this call is blocking until all killer
			% processes are dead. When done, the code
			% will loop to check if some connections
			% are still active.
			_ = killers_connections_init(Sockets),
			terminate_connections()
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
list_connections() ->
	Targets = [cowboy, gun],
	lists:flatten([ list_connections(T) || T <- Targets ]).

list_connections(gun) ->
	lists:flatten([
		begin
			ProcessInfo = process_info(P),
			Links = proplists:get_value(links, ProcessInfo, []),
			[ L || L <- Links, is_port(L) ]
		end ||
		{_, P, _, _} <- supervisor:which_children(gun_sup)
	]);
list_connections(cowboy) ->
	Filters = [{'=:=', peer_port, arweave_config:get(port)}],
	SocketsInfo = connections(#{ filters => Filters }),
	[ S || #{ socket := S } <- SocketsInfo ].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
killers_connections_init([]) -> ok;
killers_connections_init(Sockets) ->
	% start killers processes to stop those sockets.
	Killers = lists:foldr(
		fun(S, Acc) ->
			case start_killer(S) of
				{ok, K} -> [K|Acc];
				_ -> Acc
			end
		end, [], Sockets),
	% wait until all connection killers are done with their job.
	killers_connections_loop(Killers).

%%--------------------------------------------------------------------
%% @hidden
%% @doc main terminate connections loop. This functions waits for all
%% killer process to be stopped.
%% @end
%%--------------------------------------------------------------------
-spec killers_connections_loop([pid()]) -> ok.

killers_connections_loop([]) -> ok;
killers_connections_loop(Killers) ->
	TcpTimeout = 1000*arweave_config:get(shutdown_tcp_connection_timeout),
	receive
		{'EXIT', Pid, _} ->
			Filter = fun
				(P) when P =:= Pid -> false;
				(_) -> true
			end,
			NewKillers = lists:filter(Filter, Killers),
			killers_connections_loop(NewKillers);
		Msg ->
			?LOG_WARNING([{received, Msg}]),
			killers_connections_loop(Killers)
	after
		TcpTimeout ->
			?LOG_WARNING([{error, timeout}]),
			{error, timeout}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @doc start a connection killer process. This function starts a new
%% killer job using a socket. A killer will be spawned and linked to
%% the caller process.
%% @end
%%--------------------------------------------------------------------
-spec start_killer(port()) -> {ok, pid()} | {error, term()}.

start_killer(Socket)
	when is_port(Socket) ->
		Fun = fun() -> killer_init(Socket) end,
		{ok, spawn_link(Fun)};
start_killer(Term) ->
	{error, Term}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc killer process initialization function. A killer connection
%% process requires some information about a socket, but it also
%% need to wait until the socket is terminated. So, the first step
%% is to trap exit then links it to the socket. The extended information
%% about the socket is also collected before entering into the loop.
%% @see killer_loop/1
%% @end
%%--------------------------------------------------------------------
killer_init(Socket) ->
	?LOG_DEBUG([{socket, Socket}, {pid, self()}, {action, started}]),
	erlang:process_flag(trap_exit, true),
	erlang:link(Socket),
	Mode = arweave_config:get(shutdown_tcp_mode),
	State = socket_info(Socket),
	NewState = killer_loop(State#{
		counter => 0,
		mode => Mode
	}),
	?LOG_DEBUG([{state, NewState}, {pid, self()}, {action, done}]).

%%--------------------------------------------------------------------
%% @hidden
%% @doc killer connection loop. this loop is checking the state of
%% an active socket. The goal is to have all socket in closed state.
%% if its not the case, the killer connection will loop over until
%% its done.
%% @end
%%--------------------------------------------------------------------
-spec killer_loop(State) -> Return when
	State :: map(),
	Return :: ok | {error, term()}.

killer_loop(_State = #{ info := #{ states := [closed] }}) -> ok;
killer_loop(State = #{ socket := Socket, counter := Counter }) ->
	Delay = maps:get(delay, State, 1000),
	receive
		{'EXIT', Socket, _} ->
			?LOG_DEBUG([{state, State}, {pid, self()},
				{action, exited}]),
			ok
	after
		Delay ->
			try stop_connection(State) of
				stop -> ok;
				continue ->
					NewState = socket_info(Socket, State),
					killer_loop(NewState#{ counter => Counter + 1})
			catch
				E:R ->
					?LOG_DEBUG([{state, State}, {pid, self()},
						{action, error}, {error, E}, {reason, R}]),
					{E, {R, State}}
			end
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @doc stop an active connection using a socket. This function is
%% mainly used to kill a connection based on socket state.
%%
%% Two modes are currently available: shutdown or close. The shutdown mode
%% shutdowns the socket and follows the safest procedure for the
%% client: shutdown the socket and then close it when the remote
%% side of the connection is okay.
%%
%% The close mode is setting linger and close the socket. This is not
%% a clean way but in some situation, it can be useful.
%% @end
%%--------------------------------------------------------------------
-spec stop_connection(State) -> Return when
	State :: map(),
	Return :: continue | stop.

stop_connection(State = #{ socket := Socket, mode := shutdown }) ->
	?LOG_DEBUG([{state, State}, {pid, self()}]),
	case State of
		#{ info := #{ states := [closed] }} ->
			stop;
		#{ counter := 0 } ->
			ranch_tcp:setopts(Socket, [{delay_send, false}]),
			ranch_tcp:setopts(Socket, [{nodelay, true}]),
			ranch_tcp:setopts(Socket, [{send_timeout_close, true}]),
			ranch_tcp:setopts(Socket, [{send_timeout, 10}]),
			ranch_tcp:setopts(Socket, [{keepalive, false}]),
			ranch_tcp:setopts(Socket, [{exit_on_close, false}]),
			ranch_tcp:shutdown(Socket, write),
			continue;
		#{ counter := Counter } when Counter < 10 ->
			ranch_tcp:shutdown(Socket, write),
			continue;
		#{ counter := Counter } when Counter > 10 ->
			ranch_tcp:shutdown(Socket, read),
			ranch_tcp:close(Socket),
			continue;
		_ ->
			continue
	end;
stop_connection(State = #{ socket := Socket, mode := close }) ->
	?LOG_DEBUG([{state, State}, {pid, self()}]),
	case State of
		#{ info := #{ states := [closed] }} ->
			stop;
		_ ->
			ranch_tcp:setopts(Socket, [{delay_send, false}]),
			ranch_tcp:setopts(Socket, [{nodelay, true}]),
			ranch_tcp:setopts(Socket, [{exit_on_close, false}]),
			ranch_tcp:setopts(Socket, [{send_timeout_close, true}]),
			ranch_tcp:setopts(Socket, [{send_timeout, 10}]),
			ranch_tcp:setopts(Socket, [{keepalive, false}]),
			ranch_tcp:setopts(Socket, [{linger, {true, 0}}]),
			ranch_tcp:shutdown(Socket, read_write),
			ranch_tcp:close(Socket),
			continue
	end.

%%--------------------------------------------------------------------
%% @doc list all active connections.
%% @see connections/1
%% @end
%%--------------------------------------------------------------------
-spec connections() -> Return when
	Return :: [map()].

connections() ->
	connections(#{}).

%%--------------------------------------------------------------------
%% @doc `connections/1' is based on inet module private function.
%% The goal of this function is to return only active http connections.
%% By using this method, we are avoiding checking the links from
%% `ranch:procs/2' and deal directly with the sockets from `inet'.
%% @see connections/1
%% @end
%%--------------------------------------------------------------------
-spec connections(Opts) -> Return when
	Opts :: #{
		filters => [tuple()]
	},
	Return :: [map()].

connections(Opts) ->
	Filters = maps:get(filters, Opts, []),
	Sockets = erlang:ports(),
	Foldr = fun network_connection_foldr/2,
	NetworkSockets = lists:foldr(Foldr, [], Sockets),
	DefaultFilters = [
		{'=:=', name, "tcp_inet"},
		{'=/=', [info, states], [closed]},
		{'=/=', [info, states], [listen, open]},
		{'=/=', [info, states], [acception, listen, open]}
	],
	FinalFilters = DefaultFilters ++ Filters,
	data_filters(FinalFilters, NetworkSockets, #{}).

%%--------------------------------------------------------------------
%% @hidden
%% @doc filters only ports defined as network sockets, ignore all
%% other ports/sockets, and collect extended socket information.
%% @end
%%--------------------------------------------------------------------
-spec network_connection_foldr(Port, Acc) -> Return when
	Port :: port(),
	Acc :: list(),
	Return :: [map()].

network_connection_foldr(Port, Acc) ->
	try erlang:port_info(Port, name) of
		{name, N = "tcp_inet"} ->
			I = socket_info(Port),
			[I#{ name => N }|Acc];
		{name, N = "udp_inet"} ->
			I = socket_info(Port),
			[I#{ name => N }|Acc];
		{name, N = "sctp_inet"} ->
			I = socket_info(Port),
			[I#{ name => N }|Acc];
		_ -> Acc
	catch
		_:_ -> Acc
	end.

%%--------------------------------------------------------------------
%% @doc Returns extended information about an active socket (port).
%% it includes a formatted version of the peer/sock name and all
%% information from `inet:info/1' function.
%% @end
%%--------------------------------------------------------------------
-spec socket_info(Socket) -> Return when
	Socket :: port(),
	Return :: #{
		socket => port(),
		sock_name => {tuple(), pos_integer()},
		sock_address => tuple(),
		sock_port => pos_integer(),
		sock => string(),
		peer_name => {tuple(), pos_integer()},
		peer_address => tuple(),
		peer_port => pos_integer(),
		peer => string(),
		info => map()
	}.

socket_info(Socket)
	when is_port (Socket) ->
		socket_info(Socket, #{}).

socket_info(Socket, State) ->
	{SockAddress, SockPort, Sock} =
		sock_wrapper(Socket, sockname),

	{PeerAddress, PeerPort, Peer} =
		sock_wrapper(Socket, peername),

	% On R24, inet:info/1 can raise an exception
	% because some functions used to generate the
	% final map results are not returning correct
	% data. See inet:port_info/1, line 714
	Info =
		try inet:info(Socket) of
			Result when is_map(Result) -> Result;
			_ -> #{}
		catch
			_:_ -> #{}
		end,

	NewState= #{
		socket => Socket,
		sock_name => {SockAddress, SockPort},
		sock_address => SockAddress,
		sock_port => SockPort,
		sock => Sock,
		peer_name => {PeerAddress, SockPort},
		peer_address => PeerAddress,
		peer_port => PeerPort,
		peer => Peer,
		info => Info
	},
	maps:merge(State, NewState).

%%--------------------------------------------------------------------
%% @hidden
%% @doc wrapper around `inet:peername/1' and `inet:sockname/1'.
%% @end
%%---------------------------------------------------------------------
-spec sock_wrapper(Socket, Info) -> Return when
	Socket :: port(),
	Info :: peername | sockname,
	Return :: {Address, Port, Peer},
	Address :: tuple() | undefined,
	Port :: pos_integer() | undefined,
	Peer :: string().

sock_wrapper(Socket, Info) ->
	try inet:Info(Socket) of
		{ok, {Address, Port}} ->
			AddressList = inet:ntoa(Address),
			PortList = integer_to_list(Port),
			Peer = string:join([AddressList, PortList], ":"),
			{Address, Port, Peer};
		_ ->
			{undefined, undefined, "unknown"}
	catch
		_:_ ->
			{undefined, undefined, "unknown"}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @doc a simple data filter function. Filters Data using a list of
%% filters. Only the data matching all filters are returned.
%% @end
%%--------------------------------------------------------------------
-spec data_filters(Filters, Datas, Opts) -> Return when
	Filters :: [
		{Operator, CheckKey} |
		{Operator, CheckKey, CheckValue}
	],
	Operator :: atom(),
	CheckKey :: [term()] | term(),
	CheckValue :: term(),
	Datas :: [map()],
	Opts :: #{
		reverse => boolean()
	},
	Return :: [map()].

data_filters(_, [], _) -> [];
data_filters([], Datas, _) -> Datas;
data_filters(Filters, Datas, Opts) ->
	data_filters(Filters, Datas, [], Opts).

data_filters_test() ->
	?assertEqual(
		[],
		data_filters([], [], #{})
	),
	?assertEqual(
		[#{}],
		data_filters([], [#{}], #{})
	),
	?assertEqual(
		[],
		data_filters([{'is_integer', a}], [], #{})
	),
	?assertEqual(
		[],
		data_filters([{'=:=', a, 1}], [#{ a => 2 }], #{})
	),
	?assertEqual([
		#{ a => 1 }],
		data_filters([{'=:=', a, 1}], [#{ a => 1 }], #{})
	).

data_filters(_Filters, [], Buffer, _Opts) -> lists:reverse(Buffer);
data_filters(Filters, [H|T], Buffer, Opts) ->
	case filters(Filters, H, Opts) of
		{false, _} -> data_filters(Filters, T, Buffer, Opts);
		{true, _} -> data_filters(Filters, T, [H|Buffer], Opts)
	end.

filters([], Data, _Opts) ->
	{true, Data};
filters([Filter|Rest], Data, Opts) ->
	case filter(Filter, Data, Opts) of
		true ->
			filters(Rest, Data, Opts);
		false ->
			{false, Data}
	end.

filter(Filter, Data, Opts) ->
	CheckKey = erlang:element(2, Filter),
	case get(CheckKey, Data) of
		{ok, Value} ->
			filter2(Filter, [Value], Data, Opts);
		{error, _} ->
			false
	end.

filter2(Filter = {Operator, _CheckKey}, [Value], Data, Opts) ->
	Result = erlang:apply(erlang, Operator, [Value]),
	filter3(Filter, [Result, Value], Data, Opts);
filter2(Filter = {Operator, _CheckKey, CheckValue}, [Value], Data, Opts) ->
	Result = erlang:apply(erlang, Operator, [CheckValue, Value]),
	filter3(Filter, [Result, Value], Data, Opts).

filter3(_Filter, [Result|_], _Data, #{ reverse := true }) ->
	not Result;
filter3(_Filter, [Result|_], _Data, _Opts) ->
	Result.

%%--------------------------------------------------------------------
%% @hidden
%% @doc recursive maps value extractor.
%% @end
%%--------------------------------------------------------------------
-spec get(Key, Map) -> Return when
	Key :: [term()] | term(),
	Map :: map(),
	Return :: {ok, term()} | {error, term()}.

get(Key, Map)
	when not is_list(Key), is_map(Map) ->
		{ok, maps:get(Key, Map)};
get([Key], Map)
	when is_map(Map) ->
		{ok, maps:get(Key, Map)};
get([Key|Rest], Map)
	when is_map(Map) ->
		Value = maps:get(Key, Map),
		get(Rest, Value);
get(_, _) ->
	{error, not_found}.

get_test() ->
	?assertEqual(
		{error, not_found},
		get(1, [])
	),
	?assertEqual(
		{ok, 1},
		get(a, #{ a => 1 })
	),
	?assertEqual(
		{ok, 1},
		get([a,b], #{ a => #{ b => 1 } })
	),
	?assertEqual(
		{error, not_found},
		get([a,b,c], #{ a => #{ b => 1 } })
	).
