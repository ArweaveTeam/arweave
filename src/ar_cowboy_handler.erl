-module(ar_cowboy_handler).
-behaviour(cowboy_handler).
-export([init/2]).

init(InitialReq, State) ->
    RepiedRed = cowboy_req:reply(
        200,
        #{<<"content-type">> => <<"text/plain">>},
        <<"Hello, Cowboy!">>,
        InitialReq
    ),
    {ok, RepiedRed, State}.
