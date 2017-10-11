%% @doc: Elli example callback
%%
%% Your callback needs to implement two functions, handle/2 and
%% handle_event/3. For every request, Elli will call your handle
%% function with the request. When an event happens, like Elli
%% completed a request, there was a parsing error or your handler
%% threw an error, handle_event/3 is called.

-module(elli_example_callback).
-export([handle/2, handle_event/3]).
-export([chunk_loop/1]).

-include("elli.hrl").
-behaviour(elli_handler).

-include_lib("kernel/include/file.hrl").

%%
%% ELLI REQUEST CALLBACK
%%

handle(Req, _Args) ->
    %% Delegate to our handler function
    handle(Req#req.method, elli_request:path(Req), Req).



%% Route METHOD & PATH to the appropriate clause
handle('GET',[<<"hello">>, <<"world">>], _Req) ->
    %% Reply with a normal response. 'ok' can be used instead of '200'
    %% to signal success.
    {ok, [], <<"Hello World!">>};

handle('GET', [<<"hello">>], Req) ->
    %% Fetch a GET argument from the URL.
    Name = elli_request:get_arg(<<"name">>, Req, <<"undefined">>),
    {ok, [], <<"Hello ", Name/binary>>};

handle('POST', [<<"hello">>], Req) ->
    %% Fetch a POST argument from the POST body.
    Name = elli_request:post_arg(<<"name">>, Req, <<"undefined">>),
    %% Fetch and decode
    City = elli_request:post_arg_decoded(<<"city">>, Req, <<"undefined">>),
    {ok, [], <<"Hello ", Name/binary, " of ", City/binary>>};

handle('GET', [<<"hello">>, <<"iolist">>], Req) ->
    %% Iolists will be kept as iolists all the way to the socket.
    Name = elli_request:get_arg(<<"name">>, Req),
    {ok, [], [<<"Hello ">>, Name]};

handle('GET', [<<"type">>], Req) ->
    Name = elli_request:get_arg(<<"name">>, Req),
    %% Fetch a header.
    case elli_request:get_header(<<"Accept">>, Req, <<"text/plain">>) of
        <<"text/plain">> ->
            {ok, [{<<"Content-type">>, <<"text/plain; charset=ISO-8859-1">>}],
             <<"name: ", Name/binary>>};
        <<"application/json">> ->
            {ok, [{<<"Content-type">>, <<"application/json; charset=ISO-8859-1">>}],
             <<"{\"name\" : \"", Name/binary, "\"}">>}
    end;

handle('GET',[<<"headers.html">>], _Req) ->
    %% Set custom headers, for example 'Content-Type'
    {ok, [{<<"X-Custom">>, <<"foobar">>}], <<"see headers">>};

handle('GET',[<<"user">>, <<"defined">>, <<"behaviour">>], _Req) ->
    %% If you return any of the following HTTP headers, you can
    %% override the default behaviour of Elli:
    %%
    %%  * Connection: By default Elli will use keep-alive if the
    %%                protocol supports it, setting "close" will close
    %%                the connection immediately after Elli has sent
    %%                the response. If the client has already sent
    %%                pipelined requests, these will be discarded.
    %%
    %%  * Content-Length: By default Elli looks at the size of the
    %%                    body you returned to determine the
    %%                    Content-Length header. Explicitly including
    %%                    your own Content-Length (with the value as
    %%                    int, binary or list) allows you to return an
    %%                    empty body. Useful for implementing the "304
    %%                    Not Modified" response.
    %%
    {304, [{<<"Connection">>, <<"close">>},
           {<<"Content-Length">>, <<"123">>}], <<"ignored">>};

handle('GET', [<<"user">>, <<"content-length">>], _Req) ->
    {200, [{<<"Content-Length">>, 123}], <<"foobar">>};

handle('GET', [<<"crash">>], _Req) ->
    %% Throwing an exception results in a 500 response and
    %% request_throw being called
    throw(foobar);

handle('GET', [<<"decoded-hello">>], Req) ->
    %% Fetch a URI decoded GET argument from the URL.
    Name = elli_request:get_arg_decoded(<<"name">>, Req, <<"undefined">>),
    {ok, [], <<"Hello ", Name/binary>>};

handle('GET', [<<"decoded-list">>], Req) ->
    %% Fetch a URI decoded GET argument from the URL.
    [{<<"name">>, Name}, {<<"foo">>, true}] = elli_request:get_args_decoded(Req),
    {ok, [], <<"Hello ", Name/binary>>};


handle('GET', [<<"sendfile">>], _Req) ->
    %% Returning {file, "/path/to/file"} instead of the body results
    %% in Elli using sendfile.
    %% All required headers should be added by the handler.
    F = "../README.md",
    Size = elli_util:file_size(F),
    {ok, [{<<"Content-Length">>, Size}], {file, F}};

handle('GET', [<<"sendfile">>, <<"range">>], Req) ->
    %% Read the Range header of the request and use the normalized
    %% range with sendfile, otherwise send the entire file when
    %% no range is present, or respond with a 416 if the range is invalid.
    F = "../README.md",
    Size = elli_util:file_size(F),
    Range = elli_util:normalize_range(elli_request:get_range(Req), Size),
    case Range of
        {_Offset, Length} ->
            {206, [{<<"Content-Length">>, Length},
                   {<<"Content-Range">>, elli_util:encode_range(Range, Size)}],
             {file, F, Range}};
        undefined ->
            {200, [{<<"Content-Length">>, Size}], {file, F}};
        invalid_range ->
            {416, [{<<"Content-Length">>, 0},
                   {<<"Content-Range">>, elli_util:encode_range(invalid_range, Size)}],
             []}
    end;

handle('GET', [<<"compressed">>], _Req) ->
    %% Body with a byte size over 1024 are automatically gzipped by
    %% elli_middleware_compress
    {ok, binary:copy(<<"Hello World!">>, 86)};

handle('GET', [<<"compressed-io_list">>], _Req) ->
    %% Body with a iolist size over 1024 are automatically gzipped by
    %% elli_middleware_compress
    {ok, lists:duplicate(86, [<<"Hello World!">>])};


handle('HEAD', [<<"head">>], _Req) ->
    {200, [], <<"body must be ignored">>};

handle('GET', [<<"chunked">>], Req) ->
    %% Start a chunked response for streaming real-time events to the
    %% browser.
    %%
    %% Calling elli_request:send_chunk(ChunkRef, Body) will send that
    %% part to the client. elli_request:close_chunk(ChunkRef) will
    %% close the response.
    %%
    %% Return immediately {chunk, Headers} to signal we want to chunk.
    Ref = elli_request:chunk_ref(Req),
    spawn(fun() -> ?MODULE:chunk_loop(Ref) end),
    {chunk, [{<<"Content-Type">>, <<"text/event-stream">>}]};

handle('GET', [<<"shorthand">>], _Req) ->
    {200, <<"hello">>};

handle('GET', [<<"304">>], _Req) ->
    %% A "Not Modified" response is exactly like a normal response (so
    %% Content-Length is included), but the body will not be sent.
    {304, [{<<"Etag">>, <<"foobar">>}], <<"Ignored">>};

handle('GET', [<<"302">>], _Req) ->
    {302, [{<<"Location">>, <<"/hello/world">>}], <<>>};

handle('GET', [<<"403">>], _Req) ->
    %% Exceptions formatted as return codes can be used to
    %% short-circuit a response, for example in case of
    %% authentication/authorization
    throw({403, [], <<"Forbidden">>});

handle('GET', [<<"invalid_return">>], _Req) ->
    {invalid_return};

handle(_, _, _Req) ->
    {404, [], <<"Not Found">>}.



%% Send 10 separate chunks to the client.
chunk_loop(Ref) ->
    chunk_loop(Ref, 10).

chunk_loop(Ref, 0) ->
    elli_request:close_chunk(Ref);
chunk_loop(Ref, N) ->
    timer:sleep(10),

    %% Send a chunk to the client, check for errors as the user might
    %% have disconnected
    case elli_request:send_chunk(Ref, [<<"chunk">>, integer_to_list(N)]) of
        ok -> ok;
        {error, Reason} ->
            io:format("error in sending chunk: ~p~n", [Reason])
    end,

    chunk_loop(Ref, N-1).


%%
%% ELLI EVENT CALLBACKS
%%


%% elli_startup is sent when Elli is starting up. If you are
%% implementing a middleware, you can use it to spawn processes,
%% create ETS tables or start supervised processes in a supervisor
%% tree.
handle_event(elli_startup, [], _) -> ok;

%% request_complete fires *after* Elli has sent the response to the
%% client. Timings contains timestamps of events like when the
%% connection was accepted, when request parsing finished, when the
%% user callback returns, etc. This allows you to collect performance
%% statistics for monitoring your app.
handle_event(request_complete, [_Request,
                                _ResponseCode, _ResponseHeaders, _ResponseBody,
                                _Timings], _) -> ok;

%% request_throw, request_error and request_exit events are sent if
%% the user callback code throws an exception, has an error or
%% exits. After triggering this event, a generated response is sent to
%% the user.
handle_event(request_throw, [_Request, _Exception, _Stacktrace], _) -> ok;
handle_event(request_error, [_Request, _Exception, _Stacktrace], _) -> ok;
handle_event(request_exit, [_Request, _Exception, _Stacktrace], _) -> ok;

%% invalid_return is sent if the user callback code returns a term not
%% understood by elli, see elli_http:execute_callback/1.
%% After triggering this event, a generated response is sent to the user.
handle_event(invalid_return, [_Request, _ReturnValue], _) -> ok;


%% chunk_complete fires when a chunked response is completely
%% sent. It's identical to the request_complete event, except instead
%% of the response body you get the atom "client" or "server"
%% depending on who closed the connection.
handle_event(chunk_complete, [_Request,
                              _ResponseCode, _ResponseHeaders, _ClosingEnd,
                              _Timings], _) -> ok;

%% request_closed is sent if the client closes the connection when
%% Elli is waiting for the next request on a keep alive connection.
handle_event(request_closed, [], _) -> ok;

%% request_timeout is sent if the client times out when
%% Elli is waiting for the request.
handle_event(request_timeout, [], _) -> ok;

%% request_parse_error fires if the request is invalid and cannot be
%% parsed by erlang:decode_packet/3 or it contains a path Elli cannot
%% parse or does not support.
handle_event(request_parse_error, [_], _) -> ok;

%% client_closed can be sent from multiple parts of the request
%% handling. It's sent when the client closes the connection or if for
%% any reason the socket is closed unexpectedly. The "Where" atom
%% tells you in which part of the request processing the closed socket
%% was detected: receiving_headers, receiving_body, before_response
handle_event(client_closed, [_Where], _) -> ok;

%% client_timeout can as with client_closed be sent from multiple
%% parts of the request handling. If Elli tries to receive data from
%% the client socket and does not receive anything within a timeout,
%% this event fires and the socket is closed.
handle_event(client_timeout, [_Where], _) -> ok;

%% bad_request is sent when Elli detects a request is not well
%% formatted or does not conform to the configured limits. Currently
%% the Reason variable can be any of the following: {too_many_headers,
%% Headers}, {body_size, ContentLength}
handle_event(bad_request, [_Reason], _) -> ok;

%% file_error is sent when the user wants to return a file as a
%% response, but for some reason it cannot be opened.
handle_event(file_error, [_ErrorReason], _) -> ok.
