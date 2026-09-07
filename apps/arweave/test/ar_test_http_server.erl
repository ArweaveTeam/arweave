%%% @doc A stub HTTP server for tests that exercise the client side of
%%% the HTTP stack.
%%%
%%% Local-only — no peer nodes are involved, so it is safe to use from
%%% modules tagged `-test_category([fast])'. Every server gets its own
%%% listener reference and an unused port, so several may run at once.
%%%
%%% Mocks responses using an ETS table.
-module(ar_test_http_server).

-export([start/1, set_routes/2, set_route/3, stop/2]).

%% cowboy handler callback.
-export([init/2]).

-include_lib("eunit/include/eunit.hrl").

%%% Public

%% @doc Start a server answering Routes, a map of {Method, Path} to a
%% {Status, Headers, Body} response or a fun(Req) returning one.
%% Returns the listener reference to pass to stop/1 and the peer to
%% pass to ar_http. Unmapped requests are answered with a 404.
start(Routes) ->
    Table = ets:new(?MODULE, [set, named_table, public]),
    ok = set_routes(Table, Routes),

    Ref = {?MODULE, erlang:unique_integer([positive])},
    Dispatch = cowboy_router:compile([{'_', [{"/[...]", ?MODULE, Table}]}]),

    Port = get_unused_port(),
    {ok, _} = cowboy:start_clear(Ref, [{port, Port}],
        #{ env => #{ dispatch => Dispatch } }),
    {ok, {127, 0, 0, 1, Port}, Ref, Table}.

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
    ets:delete(Table),
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

get_unused_port() ->
    {ok, Socket} = gen_tcp:listen(0, []),
    {ok, Port} = inet:port(Socket),
    ok = gen_tcp:close(Socket),
    Port.
