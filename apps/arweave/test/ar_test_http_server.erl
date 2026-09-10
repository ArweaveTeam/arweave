%%% @doc A stub HTTP server for tests that exercise the client side of
%%% the HTTP stack.
%%%
%%% Local-only — no peer nodes are involved, so it is safe to use from
%%% modules tagged `-test_category([fast])'. Every server gets its own
%%% listener reference, route table, and port, so several may run at once.
%%%
%%% Booting a node with ar_test_node:start/1 restarts ranch, which kills
%%% every listener in the BEAM. When the port has to be in the node's
%%% configuration, reserve it with reserve_port/0 before the boot and
%%% start the server with start/2 after it.
%%%
%%% Mocks responses using an ETS table.
-module(ar_test_http_server).
-test_category([fast]).

-export([reserve_port/0, start/1, start/2, set_routes/2, set_route/3,
         stop/2]).

%% cowboy handler callback.
-export([init/2]).

-include_lib("eunit/include/eunit.hrl").

%%% Public

%% @doc Reserve a free port for a server started later with start/2 and
%% return {Port, Reservation}; the port stays bound until start/2 takes it.
reserve_port() ->
    {ok, Socket} = gen_tcp:listen(0, []),
    {ok, Port} = inet:port(Socket),
    {Port, Socket}.

%% @doc Start a server answering Routes, a map of {Method, Path} to a
%% {Status, Headers, Body} response or a fun(Req) returning one.
%% Returns the peer to pass to ar_http, and the listener reference and
%% route table to pass to stop/2. Unmapped requests are answered with a
%% 404. The route table belongs to the calling process.
start(Routes) ->
    start(Routes, #{}).

%% @doc start/1 with options: reservation, from reserve_port/0, makes the
%% server take over the reserved port.
start(Routes, Opts) ->
    Table = ets:new(?MODULE, [set, public]),
    ok = set_routes(Table, Routes),
    Ref = {?MODULE, erlang:unique_integer([positive])},
    Dispatch = cowboy_router:compile([{'_', [{"/[...]", ?MODULE, Table}]}]),
    %% Port 0 makes the kernel pick and bind a free port in one step. A
    %% reserved port is released right before the bind, so the window in
    %% which something else could grab it is a few microseconds.
    Port = release_reservation(maps:get(reservation, Opts, none)),
    {ok, _} = cowboy:start_clear(Ref, [{port, Port}],
        #{ env => #{ dispatch => Dispatch } }),
    {ok, {127, 0, 0, 1, ranch:get_port(Ref)}, Ref, Table}.

release_reservation(none) ->
    0;
release_reservation(Socket) ->
    {ok, Port} = inet:port(Socket),
    ok = gen_tcp:close(Socket),
    Port.

%% @doc Replace the whole route map on a running server. The next
%% request served - including one on an already-open connection - uses
%% the new routes.
set_routes(Table, Routes) ->
    true = ets:delete_all_objects(Table),
    true = ets:insert(Table, maps:to_list(Routes)),
    ok.

%% @doc Replace (or add) the response for a single route on a running
%% server, leaving the other routes alone.
set_route(Table, Route, Response) ->
    true = ets:insert(Table, {Route, Response}),
    ok.

%% @doc Stop a server started by start/1.
stop(Ref, Table) ->
    ok = cowboy:stop_listener(Ref),
    true = ets:delete(Table),
    ok.

%%% Cowboy handler
init(Req, Table) ->
    Route = {cowboy_req:method(Req), cowboy_req:path(Req)},
    {Status, Headers, Body} = response(lookup(Table, Route), Req),
    {ok, cowboy_req:reply(Status, Headers, Body, Req), Table}.

lookup(Table, Route) ->
    case ets:lookup(Table, Route) of
        [{_Route, Response}] ->
            Response;
        [] ->
            no_route
    end.

%%% Private
response(no_route, _Req) ->
    {404, #{}, <<>>};
response(Fun, Req) when is_function(Fun, 1) ->
    Fun(Req);
response({_Status, _Headers, _Body} = Response, _Req) ->
    Response.

%%%===================================================================
%%% Tests.
%%%===================================================================

%% @doc A server started on a reserved port listens on that port.
reserved_port_test() ->
    {Port, Reservation} = reserve_port(),
    {ok, Peer, Ref, Table} = start(
        #{ {<<"GET">>, <<"/r">>} => {200, #{}, <<"reserved">>} },
        #{ reservation => Reservation }),
    ?assertEqual({127, 0, 0, 1, Port}, Peer),
    ?assertMatch({ok, {{<<"200">>, _}, _, <<"reserved">>, _, _}},
                 get(Peer, "/r")),
    ok = stop(Ref, Table).

%% @doc Two servers in one BEAM keep separate route tables and ports.
two_servers_test() ->
    Route = {<<"GET">>, <<"/x">>},
    {ok, Peer1, Ref1, Table1} = start(#{ Route => {200, #{}, <<"one">>} }),
    {ok, Peer2, Ref2, Table2} = start(#{ Route => {200, #{}, <<"two">>} }),
    ?assertNotEqual(Peer1, Peer2),
    ?assertMatch({ok, {{<<"200">>, _}, _, <<"one">>, _, _}},
                 get(Peer1, "/x")),
    ?assertMatch({ok, {{<<"200">>, _}, _, <<"two">>, _, _}},
                 get(Peer2, "/x")),
    ok = stop(Ref1, Table1),
    ok = stop(Ref2, Table2).

%% @doc Servers started at the same time get distinct, working ports. Each
%% worker owns its server's route table, so it stays alive until told to
%% stop.
concurrent_servers_test() ->
    Self = self(),
    Route = {<<"GET">>, <<"/n">>},
    Workers = [spawn_monitor(fun() ->
        Body = integer_to_binary(N),
        {ok, Peer, Ref, Table} = start(#{ Route => {200, #{}, Body} }),
        Self ! {started, self(), Peer, Body},
        receive stop -> ok = stop(Ref, Table) end
    end) || N <- lists:seq(1, 16)],
    Servers = [receive
                   {started, W, Peer, Body} -> {W, Peer, Body};
                   {'DOWN', Ref, process, W, Reason} -> error({W, Reason})
               after 10000 -> error({server_not_started, W})
               end || {W, Ref} <- Workers],
    Ports = [Port || {_, {_, _, _, _, Port}, _} <- Servers],
    ?assertEqual(length(Ports), length(lists:usort(Ports))),
    lists:foreach(
        fun({W, Peer, Body}) ->
            ?assertMatch({ok, {{<<"200">>, _}, _, Body, _, _}},
                         get(Peer, "/n")),
            W ! stop
        end,
        Servers).

get(Peer, Path) ->
    ar_http:req(#{ method => get, peer => Peer, path => Path,
                   connect_timeout => 500, timeout => 2000 }).
