-module(elli_example_callback_handover).
-export([init/2, handle/2, handle_event/3]).

-include("elli_util.hrl").
-behaviour(elli_handler).

%% @doc Return `{ok, handover}' if `Req''s path is `/hello/world',
%% otherwise `ignore'.
init(Req, _Args) ->
    case elli_request:path(Req) of
        [<<"hello">>, <<"world">>] ->
            {ok, handover};
        _ ->
            ignore
    end.

%% TODO: write docstring
-spec handle(Req, Args) -> Result when
      Req    :: elli:req(),
      Args   :: elli_handler:callback_args(),
      Result :: elli_handler:result().
handle(Req, Args) ->
    handle(elli_request:method(Req), elli_request:path(Req), Req, Args).


handle('GET', [<<"hello">>, <<"world">>], Req, _Args) ->
    Body    = <<"Hello World!">>,
    Size    = list_to_binary(?I2L(size(Body))),
    Headers = [{"Connection", "close"}, {"Content-Length", Size}],
    elli_http:send_response(Req, 200, Headers, Body),
    {close, <<>>};


handle('GET', [<<"hello">>], Req, _Args) ->
    %% Fetch a GET argument from the URL.
    Name = elli_request:get_arg(<<"name">>, Req, <<"undefined">>),
    {ok, [], <<"Hello ", Name/binary>>}.


%% @hidden
handle_event(_, _, _) ->
    ok.
