-module(elli_example_middleware).
-export([handle/2, handle_event/3]).
-behaviour(elli_handler).


%%
%% ELLI
%%

handle(Req, _Args) ->
    case elli_request:path(Req) of
        [<<"middleware">>, <<"short-circuit">>] ->
            {200, [], <<"short circuit!">>};
        _ ->
            ignore
    end.



%%
%% ELLI EVENT CALLBACKS
%%


handle_event(_Event, _Data, _Args) ->
    ok.
