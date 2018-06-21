%%% @doc: Elli example callback
%%%
%%% Your callback needs to implement two functions, {@link handle/2} and
%%% {@link handle_event/3}. For every request, Elli will call your handle
%%% function with the request. When an event happens, like Elli
%%% completed a request, there was a parsing error or your handler
%%% threw an error, {@link handle_event/3} is called.

-module(elli_example_callback).
-export([handle/2, handle_event/3]).
-export([chunk_loop/1]).

-include("elli.hrl").
-include("elli_util.hrl").
-behaviour(elli_handler).

-include_lib("kernel/include/file.hrl").

%%
%% ELLI REQUEST CALLBACK
%%

%% @doc Handle a `Req'uest.
%% Delegate to our handler function.
%% @see handle/3
-spec handle(Req, _Args) -> Result when
      Req    :: elli:req(),
      _Args  :: elli_handler:callback_args(),
      Result :: elli_handler:result().
handle(Req, _Args) -> handle(Req#req.method, elli_request:path(Req), Req).



%% @doc Route `Method' and `Path' to the appropriate clause.
%%
%% `ok' can be used instead of `200' to signal success.
%%
%% If you return any of the following HTTP headers, you can
%% override the default behaviour of Elli:
%%
%%  * **Connection**:     By default Elli will use `keep-alive' if the protocol
%%                        supports it, setting `<<"close">>' will close the
%%                        connection immediately after Elli has sent the
%%                        response. If the client has already sent pipelined
%%                        requests, these will be discarded.
%%
%%  * **Content-Length**: By default Elli looks at the size of the body you
%%                        returned to determine the `Content-Length' header.
%%                        Explicitly including your own `Content-Length' (with
%%                        the value as `integer()', `binary()' or `list()')
%%                        allows you to return an empty body. Useful for
%%                        implementing the `"304 Not Modified"' response.
%%
%% @see elli_request:get_arg/3
%% @see elli_request:post_arg/3
%% @see elli_request:post_arg_decoded/3
%% @see elli_request:get_header/3
%% @see elli_request:get_arg_decoded/3
%% @see elli_request:get_args_decoded/1
%% @see elli_util:file_size/1
%% @see elli_request:get_range/1
%% @see elli_request:normalize_range/2
%% @see elli_request:encode_range/2
%% @see elli_request:chunk_ref/1
%% @see chunk_loop/1
-spec handle(Method, Path, Req) -> elli_handler:result() when
      Method :: elli:http_method(),
      Path   :: [binary()],
      Req    :: elli:req().
handle('GET', [<<"hello">>, <<"world">>], _Req) ->
    %% Reply with a normal response.
    timer:sleep(1000),
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
            {ok, [{<<"Content-type">>,
                   <<"application/json; charset=ISO-8859-1">>}],
             <<"{\"name\" : \"", Name/binary, "\"}">>}
    end;

handle('GET', [<<"headers.html">>], _Req) ->
    %% Set custom headers, for example 'Content-Type'
    {ok, [{<<"X-Custom">>, <<"foobar">>}], <<"see headers">>};

%% See note in function doc re: overriding Elli's default behaviour
%% via Connection and Content-Length headers.
handle('GET', [<<"user">>, <<"defined">>, <<"behaviour">>], _Req) ->
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
    [{<<"name">>, Name}, {<<"foo">>, true}] =
        elli_request:get_args_decoded(Req),
    {ok, [], <<"Hello ", Name/binary>>};


handle('GET', [<<"sendfile">>], _Req) ->
    %% Returning {file, "/path/to/file"} instead of the body results
    %% in Elli using sendfile.
    F    = "README.md",
    {ok, [], {file, F}};

handle('GET', [<<"send_no_file">>], _Req) ->
    %% Returning {file, "/path/to/file"} instead of the body results
    %% in Elli using sendfile.
    F    = "README",
    {ok, [], {file, F}};

handle('GET', [<<"sendfile">>, <<"error">>], _Req) ->
    F    = "test",
    {ok, [], {file, F}};

handle('GET', [<<"sendfile">>, <<"range">>], Req) ->
    %% Read the Range header of the request and use the normalized
    %% range with sendfile, otherwise send the entire file when
    %% no range is present, or respond with a 416 if the range is invalid.
    F     = "README.md",
    {ok, [], {file, F, elli_request:get_range(Req)}};

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

handle('GET', [<<"ip">>], Req) ->
    {<<"200 OK">>, elli_request:peer(Req)};

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



%% @doc Send 10 separate chunks to the client.
%% @equiv chunk_loop(Ref, 10)
chunk_loop(Ref) ->
    chunk_loop(Ref, 10).

%% @doc If `N > 0', send a chunk to the client, checking for errors,
%% as the user might have disconnected.
%% When `N == 0', call {@link elli_request:close_chunk/1.
%% elli_request:close_chunk(Ref)}.
chunk_loop(Ref, 0) ->
    elli_request:close_chunk(Ref);
chunk_loop(Ref, N) ->
    timer:sleep(10),

    case elli_request:send_chunk(Ref, [<<"chunk">>, ?I2L(N)]) of
        ok              -> ok;
        {error, Reason} -> ?LOG_ERROR("error in sending chunk: ~p~n", [Reason])
    end,

    chunk_loop(Ref, N-1).


%%
%% ELLI EVENT CALLBACKS
%%


%% @doc Handle Elli events, fired throughout processing a request.
%%
%% `elli_startup' is sent when Elli is starting up. If you are
%% implementing a middleware, you can use it to spawn processes,
%% create ETS tables or start supervised processes in a supervisor
%% tree.
%%
%% `request_complete' fires *after* Elli has sent the response to the
%% client. `Timings' contains timestamps (native units) of events like when the
%% connection was accepted, when headers/body parsing finished, when the
%% user callback returns, response sent, etc. `Sizes' contains response sizes
%% like response headers size, response body or file size.
%% This allows you to collect performance statistics for monitoring your app.
%%
%% `request_throw', `request_error' and `request_exit' events are sent if
%% the user callback code throws an exception, has an error or
%% exits. After triggering this event, a generated response is sent to
%% the user.
%%
%% `invalid_return' is sent if the user callback code returns a term not
%% understood by elli, see {@link elli_http:execute_callback/1}.
%% After triggering this event, a generated response is sent to the user.
%%
%% `chunk_complete' fires when a chunked response is completely
%% sent. It's identical to the `request_complete' event, except instead
%% of the response body you get the atom `client' or `server'
%% depending on who closed the connection. `Sizes' will have the key `chunks',
%% which is the total size of all chunks plus encoding overhead.
%%
%% `request_closed' is sent if the client closes the connection when
%% Elli is waiting for the next request on a keep alive connection.
%%
%% `request_timeout' is sent if the client times out when
%% Elli is waiting for the request.
%%
%% `request_parse_error' fires if the request is invalid and cannot be parsed by
%% [`erlang:decode_packet/3`][decode_packet/3] or it contains a path Elli cannot
%% parse or does not support.
%%
%% [decode_packet/3]: http://erlang.org/doc/man/erlang.html#decode_packet-3
%%
%% `client_closed' can be sent from multiple parts of the request
%% handling. It's sent when the client closes the connection or if for
%% any reason the socket is closed unexpectedly. The `Where' atom
%% tells you in which part of the request processing the closed socket
%% was detected: `receiving_headers', `receiving_body' or `before_response'.
%%
%% `client_timeout' can as with `client_closed' be sent from multiple
%% parts of the request handling. If Elli tries to receive data from
%% the client socket and does not receive anything within a timeout,
%% this event fires and the socket is closed.
%%
%% `bad_request' is sent when Elli detects a request is not well
%% formatted or does not conform to the configured limits. Currently
%% the `Reason' variable can be `{too_many_headers, Headers}'
%% or `{body_size, ContentLength}'.
%%
%% `file_error' is sent when the user wants to return a file as a
%% response, but for some reason it cannot be opened.
-spec handle_event(Event, Args, Config) -> ok when
      Event  :: elli:event(),
      Args   :: elli_handler:callback_args(),
      Config :: [tuple()].
handle_event(elli_startup, [], _) -> ok;
handle_event(request_complete, [_Request,
                                _ResponseCode, _ResponseHeaders, _ResponseBody,
                                {_Timings, _Sizes}], _) -> ok;
handle_event(request_throw, [_Request, _Exception, _Stacktrace], _) -> ok;
handle_event(request_error, [_Request, _Exception, _Stacktrace], _) -> ok;
handle_event(request_exit, [_Request, _Exception, _Stacktrace], _) -> ok;

handle_event(invalid_return, [_Request, _ReturnValue], _) -> ok;

handle_event(chunk_complete, [_Request,
                              _ResponseCode, _ResponseHeaders, _ClosingEnd,
                              {_Timings, _Sizes}], _) -> ok;

handle_event(request_closed, [], _) -> ok;

handle_event(request_timeout, [], _) -> ok;

handle_event(request_parse_error, [_], _) -> ok;

handle_event(client_closed, [_Where], _) -> ok;

handle_event(client_timeout, [_Where], _) -> ok;

handle_event(bad_request, [_Reason], _) -> ok;

handle_event(file_error, [_ErrorReason], _) -> ok.
