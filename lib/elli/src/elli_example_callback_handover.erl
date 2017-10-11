-module(elli_example_callback_handover).
-export([init/2, handle/2, handle_event/3]).

init(Req, _Args) ->
    case elli_request:path(Req) of
        [<<"hello">>, <<"world">>] ->
            {ok, handover};
        _ ->
            ignore
    end.

handle(Req, Args) ->
    handle(elli_request:method(Req), elli_request:path(Req), Req, Args).


handle('GET', [<<"hello">>, <<"world">>], Req, _Args) ->
    Body = <<"Hello World!">>,
    Size = list_to_binary(integer_to_list(size(Body))),
    elli_http:send_response(Req, 200, [{"Connection", "close"},
                                       {"Content-Length", Size}], Body),
    {close, <<>>};


handle('GET', [<<"hello">>], Req, _Args) ->
    %% Fetch a GET argument from the URL.
    Name = elli_request:get_arg(<<"name">>, Req, <<"undefined">>),
    {ok, [], <<"Hello ", Name/binary>>}.


handle_event(_, _, _) ->
    ok.
