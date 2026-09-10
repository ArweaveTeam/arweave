%%% @doc A stub HTTP server for tests that exercise the client side of
%%% the HTTP stack.
%%%
%%% Local-only — no peer nodes are involved, so it is safe to use from
%%% modules tagged `-test_category([fast])'. Every server gets its own
%%% listener reference, route table, and port, so several may run at once.
%%%
%%% Mocks responses using an ETS table.
-module(ar_test_http_server).
-test_category([fast]).

-export([start/1, set_routes/2, set_route/3, stop/2]).

%% cowboy handler callback.
-export([init/2]).

-include_lib("eunit/include/eunit.hrl").

%%% Public

%% @doc Start a server answering Routes, a map of {Method, Path} to a
%% {Status, Headers, Body} response or a fun(Req) returning one.
%% Returns the peer to pass to ar_http, and the listener reference and
%% route table to pass to stop/2. Unmapped requests are answered with a
%% 404. The route table belongs to the calling process.
start(Routes) ->
    Table = ets:new(?MODULE, [set, public]),
    ok = set_routes(Table, Routes),
    Ref = {?MODULE, erlang:unique_integer([positive])},
    Dispatch = cowboy_router:compile([{'_', [{"/[...]", ?MODULE, Table}]}]),
    %% Port 0 makes the kernel pick and bind a free port in one step.
    {ok, _} = cowboy:start_clear(Ref, [{port, 0}],
        #{ env => #{ dispatch => Dispatch } }),
    {ok, {127, 0, 0, 1, ranch:get_port(Ref)}, Ref, Table}.

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
