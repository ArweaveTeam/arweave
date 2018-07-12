%% @doc: Elli HTTP request implementation
%%
%% An elli_http process blocks in elli_tcp:accept/2 until a client
%% connects. It then handles requests on that connection until it's
%% closed either by the client timing out or explicitly by the user.
-module(elli_http).
-include("elli.hrl").
-include("elli_util.hrl").


%% API
-export([start_link/4]).

-export([send_response/4]).

-export([mk_req/7, mk_req/10]). %% useful when testing.

%% Exported for looping with a fully-qualified module name
-export([accept/4, handle_request/4, chunk_loop/1, split_args/1,
         parse_path/1, keepalive_loop/3, keepalive_loop/5]).

-export_type([version/0]).

%% @type version(). HTTP version as a tuple, i.e. `{0, 9} | {1, 0} | {1, 1}'.
-type version() :: {0, 9} | {1, 0} | {1, 1}.

%% TODO: use this
%% -type connection_token() :: keep_alive | close.

-spec start_link(Server, ListenSocket, Options, Callback) -> pid() when
      Server       :: pid(),
      ListenSocket :: elli_tcp:socket(),
      Options      :: proplists:proplist(),
      Callback     :: elli_handler:callback().
start_link(Server, ListenSocket, Options, Callback) ->
    proc_lib:spawn_link(?MODULE, accept,
                        [Server, ListenSocket, Options, Callback]).

%% @doc Accept on the socket until a client connects.
%% Handle the request, then loop if we're using keep alive or chunked transfer.
%% If {@link elli_tcp:accept/3} doesn't return a socket within a configurable
%% timeout, loop to allow code upgrades of this module.
-spec accept(Server, ListenSocket, Options, Callback) -> ok when
      Server       :: pid(),
      ListenSocket :: elli_tcp:socket(),
      Options      :: proplists:proplist(),
      Callback     :: elli_handler:callback().
accept(Server, ListenSocket, Options, Callback) ->
    case catch elli_tcp:accept(ListenSocket, Server, accept_timeout(Options)) of
        {ok, Socket} ->
            t(accepted),
            ?MODULE:keepalive_loop(Socket, Options, Callback);
        {error, timeout} ->
            ?MODULE:accept(Server, ListenSocket, Options, Callback);
        {error, econnaborted} ->
            ?MODULE:accept(Server, ListenSocket, Options, Callback);
        {error, {tls_alert, _}} ->
            ?MODULE:accept(Server, ListenSocket, Options, Callback);
        {error, closed} ->
            ok;
        {error, Other} ->
            exit({error, Other})
    end.


%% @doc Handle multiple requests on the same connection, i.e. `"keep alive"'.
keepalive_loop(Socket, Options, Callback) ->
    keepalive_loop(Socket, 0, <<>>, Options, Callback).

keepalive_loop(Socket, NumRequests, Buffer, Options, Callback) ->
    case ?MODULE:handle_request(Socket, Buffer, Options, Callback) of
        {keep_alive, NewBuffer} ->
            ?MODULE:keepalive_loop(Socket, NumRequests + 1,
                                   NewBuffer, Options, Callback);
        {close, _} ->
            elli_tcp:close(Socket),
            ok
    end.

%% @doc Handle a HTTP request that will possibly come on the socket.
%% Returns the appropriate connection token and any buffer containing (parts of)
%% the next request.
-spec handle_request(Socket, PrevBin, Options, Callback) -> ConnToken when
      Socket    :: elli_tcp:socket(),
      PrevBin   :: binary(),
      Options   :: proplists:proplist(),
      Callback  :: elli_handler:callback(),
      ConnToken :: {'keep_alive' | 'close', binary()}.
handle_request(S, PrevB, Opts, {Mod, Args} = Callback) ->
    {Method, RawPath, V, B0} = get_request(S, PrevB, Opts, Callback),
    t(headers_start),
    {RequestHeaders, B1} = get_headers(S, V, B0, Opts, Callback),
    t(headers_end),
    Req = mk_req(Method, RawPath, RequestHeaders, <<>>, V, S, Callback),

    case init(Req) of
        {ok, standard} ->
            t(body_start),
            {RequestBody, B2} = get_body(S, RequestHeaders, B1, Opts, Callback),
            t(body_end),
            Req1 = Req#req{body = RequestBody},

            t(user_start),
            Response = execute_callback(Req1),
            t(user_end),

            handle_response(Req1, B2, Response);
        {ok, handover} ->
            Req1 = Req#req{body = B1},

            t(user_start),
            Response = Mod:handle(Req1, Args),
            t(user_end),

            t(request_end),
            handle_event(Mod, request_complete,
                         [Req1, handover, [], <<>>, {get_timings(),
                                                     get_sizes()}], Args),
            Response
    end.

handle_response(Req, Buffer, {response, Code, UserHeaders, Body}) ->
    #req{callback = {Mod, Args}} = Req,

    Headers = [connection(Req, UserHeaders),
               content_length(UserHeaders, Body)
               | UserHeaders],
    t(send_start),
    send_response(Req, Code, Headers, Body),
    t(send_end),

    t(request_end),
    handle_event(Mod, request_complete,
                 [Req, Code, Headers, Body, {get_timings(),
                                             get_sizes()}], Args),
    {close_or_keepalive(Req, UserHeaders), Buffer};


handle_response(Req, _Buffer, {chunk, UserHeaders, Initial}) ->
    #req{callback = {Mod, Args}} = Req,

    ResponseHeaders = [{<<"Transfer-Encoding">>, <<"chunked">>},
                       connection(Req, UserHeaders)
                       | UserHeaders],
    send_response(Req, 200, ResponseHeaders, <<"">>),

    t(send_start),
    Initial =:= <<"">> orelse send_chunk(Req#req.socket, Initial),
    ClosingEnd = case start_chunk_loop(Req#req.socket) of
                     {error, client_closed} -> client;
                     ok                     -> server
                 end,
    t(send_end),

    t(request_end),
    handle_event(Mod, chunk_complete,
                 [Req, 200, ResponseHeaders, ClosingEnd, {get_timings(),
                                                          get_sizes()}],
                 Args),
    {close, <<>>};


handle_response(Req, Buffer, {file, ResponseCode, UserHeaders,
                              Filename, Range}) ->
    #req{callback = {Mod, Args}} = Req,

    ResponseHeaders = [connection(Req, UserHeaders) | UserHeaders],

    case elli_util:file_size(Filename) of
        {error, FileError} ->
            handle_event(Mod, file_error, [FileError], Args),
            send_server_error(Req#req.socket),
            elli_tcp:close(Req#req.socket),
            exit(normal);
        Size ->
            t(send_start),
            case elli_util:normalize_range(Range, Size) of
                undefined ->
                    send_file(Req, ResponseCode,
                              [{<<"Content-Length">>, Size} |
                               ResponseHeaders],
                              Filename, {0, 0});
                {Offset, Length} ->
                    ERange = elli_util:encode_range({Offset, Length}, Size),
                    send_file(Req, 206,
                              lists:append(ResponseHeaders,
                                           [{<<"Content-Length">>, Length},
                                            {<<"Content-Range">>, ERange}]),
                              Filename, {Offset, Length});
                invalid_range ->
                    ERange = elli_util:encode_range(invalid_range, Size),
                    send_response(Req, 416,
                                  lists:append(ResponseHeaders,
                                               [{<<"Content-Length">>, 0},
                                                {<<"Content-Range">>, ERange}]),
                                  [])
            end,
            t(send_end),

            t(request_end),
            handle_event(Mod, request_complete,
                         [Req, ResponseCode, ResponseHeaders, <<>>,
                          {get_timings(),
                           get_sizes()}],
                         Args),

            {close_or_keepalive(Req, UserHeaders), Buffer}
    end.


%% @doc Generate a HTTP response and send it to the client.
send_response(Req, Code, Headers, UserBody) ->
    ResponseHeaders = assemble_response_headers(Code, Headers),

    Body     = case {Req#req.method, Code} of
                   {'HEAD', _} -> <<>>;
                   {_, 304}    -> <<>>;
                   {_, 204}    -> <<>>;
                   _           -> UserBody
               end,
    s(resp_body, iolist_size(Body)),

    Response = [ResponseHeaders,
                Body],

    case elli_tcp:send(Req#req.socket, Response) of
        ok -> ok;
        {error, Closed} when Closed =:= closed orelse Closed =:= enotconn ->
            #req{callback = {Mod, Args}} = Req,
            handle_event(Mod, client_closed, [before_response], Args),
            ok
    end.

%% @doc Send a HTTP response to the client where the body is the
%% contents of the given file. Assumes correctly set response code
%% and headers.
-spec send_file(Req, Code, Headers, Filename, Range) -> ok when
      Req      :: elli:req(),
      Code     :: elli:response_code(),
      Headers  :: elli:headers(),
      Filename :: file:filename(),
      Range    :: elli_util:range().
send_file(#req{callback={Mod, Args}} = Req, Code, Headers, Filename, Range) ->
    ResponseHeaders = assemble_response_headers(Code, Headers),

    case file:open(Filename, [read, raw, binary]) of
        {ok,    Fd}        -> do_send_file(Fd, Range, Req, ResponseHeaders);
        {error, FileError} ->
            handle_event(Mod, file_error, [FileError], Args),
            send_server_error(Req#req.socket),
            elli_tcp:close(Req#req.socket),
            exit(normal)
    end,
    ok.

do_send_file(Fd, {Offset, Length}, #req{callback={Mod, Args}} = Req, Headers) ->
    try elli_tcp:send(Req#req.socket, Headers) of
        ok ->
            case elli_tcp:sendfile(Fd, Req#req.socket, Offset, Length, []) of
                {ok, BytesSent} -> s(file, BytesSent), ok;
                {error, Closed} when Closed =:= closed orelse
                                     Closed =:= enotconn ->
                    handle_event(Mod, client_closed, [before_response], Args)
            end;
        {error, Closed} when Closed =:= closed orelse
                             Closed =:= enotconn ->
            handle_event(Mod, client_closed, [before_response], Args)
    after
        file:close(Fd)
    end.

%% @doc To send a response, we must first have received everything the
%% client is sending. If this is not the case, {@link send_bad_request/1}
%% might reset the client connection.
send_bad_request(Socket) ->
    send_rescue_response(Socket, 400, <<"Bad Request">>).

send_server_error(Socket) ->
    send_rescue_response(Socket, 500, <<"Server Error">>).

send_rescue_response(Socket, Code, Body) ->
    Response = http_response(Code, Body),
    elli_tcp:send(Socket, Response).

%% @doc Execute the user callback, translating failure into a proper response.
execute_callback(#req{callback = {Mod, Args}} = Req) ->
    try Mod:handle(Req, Args) of
        %% {ok,...{file,...}}
        {ok, Headers, {file, Filename}} ->
            {file, 200, Headers, Filename, []};
        {ok, Headers, {file, Filename, Range}} ->
            {file, 200, Headers, Filename, Range};
        %% ok simple
        {ok, Headers, Body}       -> {response, 200, Headers, Body};
        {ok, Body}                -> {response, 200, [], Body};
        %% Chunk
        {chunk, Headers}          -> {chunk, Headers, <<"">>};
        {chunk, Headers, Initial} -> {chunk, Headers, Initial};
        %% File
        {HttpCode, Headers, {file, Filename}} ->
            {file, HttpCode, Headers, Filename, {0, 0}};
        {HttpCode, Headers, {file, Filename, Range}} ->
            {file, HttpCode, Headers, Filename, Range};
        %% Simple
        {HttpCode, Headers, Body} -> {response, HttpCode, Headers, Body};
        {HttpCode, Body}          -> {response, HttpCode, [], Body};
        %% Unexpected
        Unexpected ->
            handle_event(Mod, invalid_return, [Req, Unexpected], Args),
            {response, 500, [], <<"Internal server error">>}
    catch
        throw:{ResponseCode, Headers, Body} when is_integer(ResponseCode) ->
            {response, ResponseCode, Headers, Body};
        ?WITH_STACKTRACE(throw, Exc, Stacktrace)
            handle_event(Mod, request_throw,
                         [Req, Exc, Stacktrace],
                         Args),
            {response, 500, [], <<"Internal server error">>};
        ?WITH_STACKTRACE(error, Error, Stacktrace)
            handle_event(Mod, request_error,
                         [Req, Error, Stacktrace],
                         Args),
            {response, 500, [], <<"Internal server error">>};
        ?WITH_STACKTRACE(exit, Exit, Stacktrace)
            handle_event(Mod, request_exit,
                         [Req, Exit, Stacktrace],
                         Args),
            {response, 500, [], <<"Internal server error">>}
    end.

%%
%% CHUNKED-TRANSFER
%%


%% @doc The chunk loop is an intermediary between the socket and the
%% user. We forward anything the user sends until the user sends an
%% empty response, which signals that the connection should be
%% closed. When the client closes the socket, the loop exits.
start_chunk_loop(Socket) ->
    %% Set the socket to active so we receive the tcp_closed message
    %% if the client closes the connection
    elli_tcp:setopts(Socket, [{active, once}]),
    ?MODULE:chunk_loop(Socket).

chunk_loop(Socket) ->
    {_SockType, InnerSocket} = Socket,
    receive
        {tcp_closed, InnerSocket} ->
            {error, client_closed};

        {chunk, close} ->
            case elli_tcp:send(Socket, <<"0\r\n\r\n">>) of
                ok ->
                    elli_tcp:close(Socket),
                    ok;
                {error, Closed} when Closed =:= closed orelse
                                     Closed =:= enotconn ->
                    {error, client_closed}
            end;
        {chunk, close, From} ->
            case elli_tcp:send(Socket, <<"0\r\n\r\n">>) of
                ok ->
                    elli_tcp:close(Socket),
                    From ! {self(), ok},
                    ok;
                {error, Closed} when Closed =:= closed orelse
                                     Closed =:= enotconn ->
                    From ! {self(), {error, closed}},
                    ok
            end;

        {chunk, Data} ->
            send_chunk(Socket, Data),
            ?MODULE:chunk_loop(Socket);
        {chunk, Data, From} ->
            case send_chunk(Socket, Data) of
                ok ->
                    From ! {self(), ok};
                {error, Closed} when Closed =:= closed orelse
                                     Closed =:= enotconn ->
                    From ! {self(), {error, closed}}
            end,
            ?MODULE:chunk_loop(Socket)
    after 10000 ->
            ?MODULE:chunk_loop(Socket)
    end.


send_chunk(Socket, Data) ->
    case iolist_size(Data) of
        0 -> ok;
        Size ->
            Response = [integer_to_list(Size, 16),
                        <<"\r\n">>, Data, <<"\r\n">>],
            s(chunks, iolist_size(Response)),
            elli_tcp:send(Socket, Response)
    end.


%%
%% RECEIVE REQUEST
%%

%% @doc Retrieve the request line.
get_request(Socket, <<>>, Options, Callback) ->
    NewBuffer = recv_request(Socket, <<>>, Options, Callback),
    get_request(Socket, NewBuffer, Options, Callback);
get_request(Socket, Buffer, Options, Callback) ->
    t(request_start),
    get_request_(Socket, Buffer, Options, Callback).

get_request_(Socket, Buffer, Options, {Mod, Args} = Callback) ->
    case erlang:decode_packet(http_bin, Buffer, []) of
        {more, _} ->
            NewBuffer = recv_request(Socket, Buffer, Options, Callback),
            get_request_(Socket, NewBuffer, Options, Callback);
        {ok, {http_request, Method, RawPath, Version}, Rest} ->
            {Method, RawPath, Version, Rest};
        {ok, {http_error, _}, _} ->
            handle_event(Mod, request_parse_error, [Buffer], Args),
            send_bad_request(Socket),
            elli_tcp:close(Socket),
            exit(normal);
        {ok, {http_response, _, _, _}, _} ->
            elli_tcp:close(Socket),
            exit(normal)
    end.

recv_request(Socket, Buffer, Options, {Mod, Args} = _Callback) ->
    case elli_tcp:recv(Socket, 0, request_timeout(Options)) of
        {ok, Data} ->
            <<Buffer/binary, Data/binary>>;
        {error, timeout} ->
            handle_event(Mod, request_timeout, [], Args),
            elli_tcp:close(Socket),
            exit(normal);
        {error, Closed} when Closed =:= closed orelse
                             Closed =:= enotconn ->
            handle_event(Mod, request_closed, [], Args),
            elli_tcp:close(Socket),
            exit(normal)
    end.

-spec get_headers(Socket, V, Buffer, Opts, Callback) -> Headers when
      Socket   :: elli_tcp:socket(),
      V        :: version(),
      Buffer   :: binary(),
      Opts     :: proplists:proplist(),
      Callback :: elli_handler:callback(),
      Headers  :: {elli:headers(), any()}. % TODO: refine
get_headers(_Socket, {0, 9}, _, _, _) ->
    {[], <<>>};
get_headers(Socket, {1, _}, Buffer, Opts, Callback) ->
    get_headers(Socket, Buffer, [], 0, Opts, Callback).

get_headers(Socket, _, Headers, HeadersCount, _Opts, {Mod, Args})
  when HeadersCount >= 100 ->
    handle_event(Mod, bad_request, [{too_many_headers, Headers}], Args),
    send_bad_request(Socket),
    elli_tcp:close(Socket),
    exit(normal);
get_headers(Socket, Buffer, Headers, Count, Opts, {Mod, Args} = Callback) ->
    case erlang:decode_packet(httph_bin, Buffer, []) of
        {ok, {http_header, _, Key, _, Value}, Rest} ->
            NewHeaders = [{ensure_binary(Key), Value} | Headers],
            get_headers(Socket, Rest, NewHeaders, Count + 1, Opts, Callback);
        {ok, http_eoh, Rest} ->
            {Headers, Rest};
        {ok, {http_error, _}, Rest} ->
            get_headers(Socket, Rest, Headers, Count, Opts, Callback);
        {more, _} ->
            case elli_tcp:recv(Socket, 0, header_timeout(Opts)) of
                {ok, Data} ->
                    get_headers(Socket, <<Buffer/binary, Data/binary>>,
                                Headers, Count, Opts, Callback);
                {error, Closed} when Closed =:= closed orelse
                                     Closed =:= enotconn ->
                    handle_event(Mod, client_closed, [receiving_headers], Args),
                    elli_tcp:close(Socket),
                    exit(normal);
                {error, timeout} ->
                    handle_event(Mod, client_timeout,
                                 [receiving_headers], Args),
                    elli_tcp:close(Socket),
                    exit(normal)
            end
    end.

%% @doc Fetch the full body of the request, if any is available.
%%
%% At the moment we don't need to handle large requests, so there is
%% no need for streaming or lazily fetching the body in the user
%% code. Fully receiving the body allows us to avoid the complex
%% request object threading in Cowboy and the caching in Mochiweb.
%%
%% As we are always receiving whatever the client sends, we might have
%% buffered too much and get parts of the next pipelined request. In
%% that case, push it back in the buffer and handle the first request.
-spec get_body(Socket, Headers, Buffer, Opts, Callback) -> FullBody when
      Socket   :: undefined | elli_tcp:socket(),
      Headers  :: elli:headers(),
      Buffer   :: binary(),
      Opts     :: proplists:proplist(),
      Callback :: elli_handler:callback(),
      FullBody :: {elli:body(), binary()}.
get_body(Socket, Headers, Buffer, Opts, Callback) ->
    case proplists:get_value(<<"Content-Length">>, Headers, undefined) of
        undefined ->
            {<<>>, Buffer};
        ContentLengthBin ->
            maybe_send_continue(Socket, Headers),

            ContentLength = ?B2I(binary:replace(ContentLengthBin,
                                                <<" ">>, <<>>, [global])),

            ok = check_max_size(Socket, ContentLength, Buffer, Opts, Callback),

            Result = case ContentLength - byte_size(Buffer) of
                         0 ->
                             {Buffer, <<>>};
                         N when N > 0 ->
                             do_get_body(Socket, Buffer, Opts, N, Callback);
                         _ ->
                             <<Body:ContentLength/binary, R/binary>> = Buffer,
                             {Body, R}
                     end,
            %% set the size here so if do_get_body exits it won't have
            %% req_body in sizes
            s(req_body, ContentLength),
            Result
    end.

do_get_body(Socket, Buffer, Opts, N, {Mod, Args}) ->
    case elli_tcp:recv(Socket, N, body_timeout(Opts)) of
        {ok, Data} ->
            {<<Buffer/binary, Data/binary>>, <<>>};
        {error, Closed} when Closed =:= closed orelse Closed =:= enotconn ->
            handle_event(Mod, client_closed, [receiving_body], Args),
            ok = elli_tcp:close(Socket),
            exit(normal);
        {error, timeout} ->
            handle_event(Mod, client_timeout, [receiving_body], Args),
            ok = elli_tcp:close(Socket),
            exit(normal)
    end.

ensure_binary(Bin) when is_binary(Bin) -> Bin;
ensure_binary(Atom) when is_atom(Atom) -> atom_to_binary(Atom, latin1).

maybe_send_continue(Socket, Headers) ->
    % According to RFC2616 section 8.2.3 an origin server must respond with
    % either a "100 Continue" or a final response code when the client
    % headers contains "Expect:100-continue"
    case proplists:get_value(<<"Expect">>, Headers, undefined) of
        <<"100-continue">> ->
            Response = http_response(100),
            elli_tcp:send(Socket, Response);
        _Other ->
            ok
    end.

%% @doc To send a response, we must first receive anything the client is
%% sending. To avoid allowing clients to use all our bandwidth, if the request
%% size is too big, we simply close the socket.
check_max_size(Socket, ContentLength, Buffer, Opts, {Mod, Args}) ->
    MaxSize = max_body_size(Opts),
    do_check_max_size(Socket, ContentLength, Buffer, MaxSize, {Mod, Args}).

do_check_max_size(Socket, ContentLength, Buffer, MaxSize, {Mod, Args})
  when ContentLength > MaxSize ->
    handle_event(Mod, bad_request, [{body_size, ContentLength}], Args),
    do_check_max_size_x2(Socket, ContentLength, Buffer, MaxSize),
    elli_tcp:close(Socket),
    exit(normal);
do_check_max_size(_, _, _, _, _) -> ok.

do_check_max_size_x2(Socket, ContentLength, Buffer, MaxSize)
  when ContentLength < MaxSize * 2 ->
    OnSocket = ContentLength - size(Buffer),
    elli_tcp:recv(Socket, OnSocket, 60000),
    Response = http_response(413),
    elli_tcp:send(Socket, Response);
do_check_max_size_x2(_, _, _, _) -> ok.

-spec mk_req(Method, PathTuple, Headers, Body, V, Socket, Callback) -> Req when
      Method    :: elli:http_method(),
      PathTuple :: {PathType :: atom(), RawPath :: binary()},
      Headers   :: elli:headers(),
      Body      :: elli:body(),
      V         :: version(),
      Socket    :: elli_tcp:socket() | undefined,
      Callback  :: elli_handler:callback(),
      Req       :: elli:req().
mk_req(Method, PathTuple, Headers, Body, V, Socket, {Mod, Args} = Callback) ->
    case parse_path(PathTuple) of
        {ok, {Scheme, Host, Port}, {Path, URL, URLArgs}} ->
            #req{method   = Method, scheme   = Scheme, host    = Host,
                 port     = Port,   path     = URL,    args    = URLArgs,
                 version  = V,      raw_path = Path,   headers = Headers,
                 body     = Body,   pid      = self(), socket  = Socket,
                 callback = Callback};
        {error, Reason} ->
            handle_event(Mod, request_parse_error,
                         [{Reason, {Method, PathTuple}}], Args),
            send_bad_request(Socket),
            elli_tcp:close(Socket),
            exit(normal)
    end.

mk_req(Method, Scheme, Host, Port, PathTuple, Headers, Body, V, Socket, Callback) ->
    Req = mk_req(Method, PathTuple, Headers, Body, V, Socket, Callback),
    Req#req{scheme = Scheme, host = Host, port = Port}.


%%
%% HEADERS
%%

http_response(Code) ->
    http_response(Code, <<>>).

http_response(Code, Body) ->
    http_response(Code, [{<<"Content-Length">>, size(Body)}], Body).

http_response(Code, Headers, <<>>) ->
    [<<"HTTP/1.1 ">>, status(Code), <<"\r\n">>,
     encode_headers(Headers), <<"\r\n">>];
http_response(Code, Headers, Body) ->
    [http_response(Code, Headers, <<>>), Body].

assemble_response_headers(Code, Headers) ->
    ResponseHeaders = http_response(Code, Headers, <<>>),
    s(resp_headers, iolist_size(ResponseHeaders)),
    ResponseHeaders.

encode_headers([]) ->
    [];

encode_headers([[] | H]) ->
    encode_headers(H);
encode_headers([{K, V} | H]) ->
    [encode_value(K), <<": ">>, encode_value(V), <<"\r\n">>, encode_headers(H)].

encode_value(V) when is_integer(V) -> ?I2L(V);
encode_value(V) when is_binary(V)  -> V;
encode_value(V) when is_list(V)    -> list_to_binary(V).


connection_token(#req{version = {1, 1}, headers = Headers}) ->
    case proplists:get_value(<<"Connection">>, Headers) of
        <<"close">> -> <<"close">>;
        <<"Close">> -> <<"close">>;
        _           -> <<"Keep-Alive">>
    end;
connection_token(#req{version = {1, 0}, headers = Headers}) ->
    case proplists:get_value(<<"Connection">>, Headers) of
        <<"Keep-Alive">> -> <<"Keep-Alive">>;
        _                -> <<"close">>
    end;
connection_token(#req{version = {0, 9}}) ->
    <<"close">>.


close_or_keepalive(Req, UserHeaders) ->
    case proplists:get_value(<<"Connection">>, UserHeaders) of
        undefined ->
            case connection_token(Req) of
                <<"Keep-Alive">> -> keep_alive;
                <<"close">>      -> close
            end;
        <<"close">>      -> close;
        <<"Keep-Alive">> -> keep_alive
    end.


%% @doc Add appropriate connection header if the user did not add one already.
connection(Req, UserHeaders) ->
    case proplists:get_value(<<"Connection">>, UserHeaders) of
        undefined ->
            {<<"Connection">>, connection_token(Req)};
        _ ->
            []
    end.

content_length(Headers, Body)->
    ?IF(proplists:is_defined(<<"Content-Length">>, Headers), [],
        {<<"Content-Length">>, iolist_size(Body)}).

%%
%% PATH HELPERS
%%

parse_path({abs_path, FullPath}) ->
    Parsed = case binary:split(FullPath, [<<"?">>]) of
                 [URL]       -> {FullPath, split_path(URL), []};
                 [URL, Args] -> {FullPath, split_path(URL), split_args(Args)}
             end,
    {ok, {undefined, undefined, undefined}, Parsed};
parse_path({absoluteURI, Scheme, Host, Port, Path}) ->
    setelement(2, parse_path({abs_path, Path}), {Scheme, Host, Port});
parse_path(_) ->
    {error, unsupported_uri}.

split_path(Path) ->
    [P || P <- binary:split(Path, [<<"/">>], [global]),
          P =/= <<>>].

%% @doc Split the URL arguments into a proplist.
%% Lifted from `cowboy_http:x_www_form_urlencoded/2'.
-spec split_args(binary()) -> list({binary(), binary() | true}).
split_args(<<>>) ->
    [];
split_args(Qs) ->
    Tokens = binary:split(Qs, <<"&">>, [global, trim]),
    [case binary:split(Token, <<"=">>) of
         [Token]       -> {Token, true};
         [Name, Value] -> {Name, Value}
     end || Token <- Tokens].


%%
%% CALLBACK HELPERS
%%

init(#req{callback = {Mod, Args}} = Req) ->
    ?IF(erlang:function_exported(Mod, init, 2),
        case Mod:init(Req, Args) of
            ignore          -> {ok, standard};
            {ok, Behaviour} -> {ok, Behaviour}
        end,
        {ok, standard}).

handle_event(Mod, Name, EventArgs, ElliArgs) ->
    try
        Mod:handle_event(Name, EventArgs, ElliArgs)
    catch
        ?WITH_STACKTRACE(EvClass, EvError, Stacktrace)
            ?LOG_ERROR("~p:handle_event/3 crashed ~p:~p~n~p",
                       [Mod, EvClass, EvError, Stacktrace])
    end.

%%
%% TIMING HELPERS
%%

%% @doc Record the current monotonic time in the process dictionary.
%% This allows easily adding time tracing wherever,
%% without passing along any variables.
t(Key) ->
    put({time, Key}, erlang:monotonic_time()).

get_timings() ->
    lists:filtermap(fun get_timings/1, get()).

get_timings({{time, accepted}, Value}) ->
    {true, {accepted, Value}};
get_timings({{time, Key}, Value}) ->
    erase({time, Key}),
    {true, {Key, Value}};
get_timings(_) ->
    false.

%%
%% SIZE HELPERS
%%

%% @doc stores response part size in bytes
s(chunks, Size) ->
    case get({size, chunks}) of
        undefined ->
            put({size, chunks}, Size);
        Sum ->
            put({size, chunks}, Size + Sum)
    end;
s(Key, Size) ->
    put({size, Key}, Size).

get_sizes() ->
    lists:filtermap(fun get_sizes/1, get()).

get_sizes({{size, Key}, Value}) ->
    erase({size, Key}),
    {true, {Key, Value}};
get_sizes(_) ->
    false.


%%
%% OPTIONS
%%

accept_timeout(Opts)  -> proplists:get_value(accept_timeout, Opts).
request_timeout(Opts) -> proplists:get_value(request_timeout, Opts).
header_timeout(Opts)  -> proplists:get_value(header_timeout, Opts).
body_timeout(Opts)    -> proplists:get_value(body_timeout, Opts).
max_body_size(Opts)   -> proplists:get_value(max_body_size, Opts).


%%
%% HTTP STATUS CODES
%%

%% @doc Response code string. Lifted from `cowboy_http_req.erl'.
status(100) -> <<"100 Continue">>;
status(101) -> <<"101 Switching Protocols">>;
status(102) -> <<"102 Processing">>;
status(200) -> <<"200 OK">>;
status(201) -> <<"201 Created">>;
status(202) -> <<"202 Accepted">>;
status(203) -> <<"203 Non-Authoritative Information">>;
status(204) -> <<"204 No Content">>;
status(205) -> <<"205 Reset Content">>;
status(206) -> <<"206 Partial Content">>;
status(207) -> <<"207 Multi-Status">>;
status(208) -> <<"208 Already Reported">>;
status(226) -> <<"226 IM Used">>;
status(300) -> <<"300 Multiple Choices">>;
status(301) -> <<"301 Moved Permanently">>;
status(302) -> <<"302 Found">>;
status(303) -> <<"303 See Other">>;
status(304) -> <<"304 Not Modified">>;
status(305) -> <<"305 Use Proxy">>;
status(306) -> <<"306 Switch Proxy">>;
status(307) -> <<"307 Temporary Redirect">>;
status(400) -> <<"400 Bad Request">>;
status(401) -> <<"401 Unauthorized">>;
status(402) -> <<"402 Payment Required">>;
status(403) -> <<"403 Forbidden">>;
status(404) -> <<"404 Not Found">>;
status(405) -> <<"405 Method Not Allowed">>;
status(406) -> <<"406 Not Acceptable">>;
status(407) -> <<"407 Proxy Authentication Required">>;
status(408) -> <<"408 Request Timeout">>;
status(409) -> <<"409 Conflict">>;
status(410) -> <<"410 Gone">>;
status(411) -> <<"411 Length Required">>;
status(412) -> <<"412 Precondition Failed">>;
status(413) -> <<"413 Request Entity Too Large">>;
status(414) -> <<"414 Request-URI Too Long">>;
status(415) -> <<"415 Unsupported Media Type">>;
status(416) -> <<"416 Requested Range Not Satisfiable">>;
status(417) -> <<"417 Expectation Failed">>;
status(418) -> <<"418 I'm a teapot">>;
status(421) -> <<"421 Misdirected Request">>;
status(422) -> <<"422 Unprocessable Entity">>;
status(423) -> <<"423 Locked">>;
status(424) -> <<"424 Failed Dependency">>;
status(425) -> <<"425 Unordered Collection">>;
status(426) -> <<"426 Upgrade Required">>;
status(428) -> <<"428 Precondition Required">>;
status(429) -> <<"429 Too Many Requests">>;
status(431) -> <<"431 Request Header Fields Too Large">>;
status(500) -> <<"500 Internal Server Error">>;
status(501) -> <<"501 Not Implemented">>;
status(502) -> <<"502 Bad Gateway">>;
status(503) -> <<"503 Service Unavailable">>;
status(504) -> <<"504 Gateway Timeout">>;
status(505) -> <<"505 HTTP Version Not Supported">>;
status(506) -> <<"506 Variant Also Negotiates">>;
status(507) -> <<"507 Insufficient Storage">>;
status(510) -> <<"510 Not Extended">>;
status(511) -> <<"511 Network Authentication Required">>;
status(B) when is_binary(B) -> B.


%%
%% UNIT TESTS
%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

get_body_test() ->
    Socket   = undefined,
    Headers  = [{<<"Content-Length">>, <<" 42 ">>}],
    Buffer   = binary:copy(<<".">>, 42),
    Opts     = [],
    Callback = {no_mod, []},
    ?assertMatch({Buffer, <<>>},
                 get_body(Socket, Headers, Buffer, Opts, Callback)).
-endif.
