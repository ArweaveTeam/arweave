%% @hidden
-module(elli_example_middleware).
-export([handle/2, handle_event/3]).
-behaviour(elli_handler).


%%
%% ELLI
%%

handle(Req, _Args) ->
    do_handle(elli_request:path(Req)).

do_handle([<<"middleware">>, <<"short-circuit">>]) ->
    {200, [], <<"short circuit!">>};
do_handle(_) ->
    ignore.


%%
%% ELLI EVENT CALLBACKS
%%


handle_event(_Event, _Data, _Args) ->
    ok.
