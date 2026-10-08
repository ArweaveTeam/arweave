%%% @doc Regressions for gun replies other than a plain response and body,
%%% using loopback sockets and real Gun.
-module(ar_http_response_tests).
-test_category([fast]).

-include_lib("eunit/include/eunit.hrl").

%% Fail a stuck loopback exchange promptly; these are not timing assertions.
-define(TIMEOUT, 3000).

%% The slow servers send a part of the response every ?SLOW_INTERVAL
%% milliseconds, well within ?SLOW_REQUEST_TIMEOUT, ?SLOW_PARTS times, taking
%% far longer than ?SLOW_REQUEST_TIMEOUT in total.
-define(SLOW_REQUEST_TIMEOUT, 500).
-define(SLOW_INTERVAL, 100).
-define(SLOW_PARTS, 20).

%% @doc An HTTP/1.1 interim 1xx response is skipped.
http1_interim_response_test() ->
    with_server(interim, fun(Peer) ->
        ?assertMatch({ok, {{<<"200">>, _}, _, <<"OK">>, _, _}},
            request(Peer, http1))
    end).

%% @doc An HTTP/2 interim 1xx response is skipped.
http2_interim_response_test() ->
    with_server(interim, fun(Peer) ->
        ?assertMatch({ok, {{<<"200">>, _}, _, <<"OK">>, _, _}},
            request(Peer, http2))
    end).

%% @doc An HTTP/1.1 chunked body ending with trailers completes the response.
http1_trailers_test() ->
    with_server(trailers, fun(Peer) ->
        ?assertMatch({ok, {{<<"200">>, _}, _, <<"OK">>, _, _}},
            request(Peer, http1))
    end).

%% @doc An HTTP/2 body ending with trailers completes the response.
http2_trailers_test() ->
    with_server(trailers, fun(Peer) ->
        ?assertMatch({ok, {{<<"200">>, _}, _, <<"OK">>, _, _}},
            request(Peer, http2))
    end).

%% @doc An HTTP/2 pushed stream is cancelled and the response completes.
http2_push_test() ->
    with_server(push, fun(Peer) ->
        ?assertMatch({ok, {{<<"200">>, _}, _, <<"OK">>, _, _}},
            request(Peer, http2))
    end).

%% @doc The timeout bounds the whole request, not each wait for a message, so
%% a body arriving in small parts more slowly than that times out.
http1_slow_body_timeout_test() ->
    with_server(slow_body, fun(Peer) ->
        ?assertEqual({error, timeout},
            request(Peer, http1, ?SLOW_REQUEST_TIMEOUT))
    end).

%% @doc Interim responses repeated beyond the timeout do not keep the request
%% waiting.
http1_repeated_interim_timeout_test() ->
    with_server(repeated_interim, fun(Peer) ->
        ?assertEqual({error, timeout},
            request(Peer, http1, ?SLOW_REQUEST_TIMEOUT))
    end).

%% @doc Send one request; peer requests use HTTP/2, others HTTP/1.1.
request(Peer, Protocol) ->
    request(Peer, Protocol, ?TIMEOUT).

request(Peer, Protocol, Timeout) ->
    ar_http:req(#{
        method => get,
        peer => Peer,
        path => "/probe",
        timeout => Timeout,
        is_peer_request => Protocol == http2
    }).

%% @doc Run a stub replying to every connection in the given mode.
with_server(Mode, Fun) ->
    {ok, Listener} = gen_tcp:listen(0, [
        binary,
        {active, false},
        {ip, {127, 0, 0, 1}}
    ]),
    {ok, Port} = inet:port(Listener),
    Server = spawn_link(fun() -> accept(Listener, Mode) end),
    try
        Fun({127, 0, 0, 1, Port})
    after
        gen_tcp:close(Listener),
        unlink(Server),
        exit(Server, shutdown)
    end.

accept(Listener, Mode) ->
    case gen_tcp:accept(Listener) of
        {ok, Socket} ->
            serve(Socket, Mode),
            accept(Listener, Mode);
        {error, closed} ->
            ok
    end.

%% @doc Distinguish the fixed 24-byte HTTP/2 preface from an HTTP/1.1 request.
serve(Socket, Mode) ->
    try
        case gen_tcp:recv(Socket, 24, ?TIMEOUT) of
            {ok, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>} ->
                serve_http2(Socket, Mode);
            {ok, _HTTP1} ->
                serve_http1(Socket, Mode);
            {error, _} ->
                ok
        end
    after
        gen_tcp:close(Socket)
    end.

serve_http1(Socket, interim) ->
    finish_response(Socket, [
        "HTTP/1.1 100 Continue\r\n\r\n",
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK"
    ]);
serve_http1(Socket, slow_body) ->
    ok = gen_tcp:send(Socket, ["HTTP/1.1 200 OK\r\nContent-Length: ",
        integer_to_list(?SLOW_PARTS), "\r\nConnection: close\r\n\r\n"]),
    send_slowly(Socket, "x"),
    finish_response(Socket, []);
serve_http1(Socket, repeated_interim) ->
    send_slowly(Socket, "HTTP/1.1 103 Early Hints\r\n\r\n"),
    finish_response(Socket, [
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK"
    ]);
serve_http1(Socket, trailers) ->
    finish_response(Socket, [
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n",
        "Trailer: x-checksum\r\nConnection: close\r\n\r\n",
        "2\r\nOK\r\n0\r\nx-checksum: 1\r\n\r\n"
    ]).

%% @doc Send Part ?SLOW_PARTS times, ?SLOW_INTERVAL milliseconds apart. Stop
%% early once the client disconnects.
send_slowly(Socket, Part) ->
    send_slowly(Socket, Part, ?SLOW_PARTS).

send_slowly(_Socket, _Part, 0) ->
    ok;
send_slowly(Socket, Part, N) ->
    timer:sleep(?SLOW_INTERVAL),
    case gen_tcp:send(Socket, Part) of
        ok -> send_slowly(Socket, Part, N - 1);
        {error, _} -> ok
    end.

serve_http2(Socket, Mode) ->
    ok = gen_tcp:send(Socket, cow_http2:settings(#{})),
    StreamID = receive_headers(Socket),
    finish_response(Socket, http2_frames(StreamID, Mode, Socket)).

http2_frames(StreamID, push, Socket) ->
    %% Gun refuses a promise whose authority differs from the request's.
    {ok, {_, Port}} = inet:sockname(Socket),
    Authority = iolist_to_binary(["127.0.0.1:", integer_to_list(Port)]),
    {Promise, State} = cow_hpack:encode([{<<":method">>, <<"GET">>},
        {<<":scheme">>, <<"http">>}, {<<":authority">>, Authority},
        {<<":path">>, <<"/pushed">>}]),
    {Final, _} = cow_hpack:encode([{<<":status">>, <<"200">>}], State),
    [
        cow_http2:push_promise(StreamID, 2, Promise),
        cow_http2:headers(StreamID, nofin, Final),
        cow_http2:data(StreamID, fin, <<"OK">>)
    ];
http2_frames(StreamID, Mode, _Socket) ->
    http2_frames(StreamID, Mode).

http2_frames(StreamID, interim) ->
    {Inform, State} = cow_hpack:encode([{<<":status">>, <<"103">>}]),
    {Final, _} = cow_hpack:encode([{<<":status">>, <<"200">>}], State),
    [
        cow_http2:headers(StreamID, nofin, Inform),
        cow_http2:headers(StreamID, nofin, Final),
        cow_http2:data(StreamID, fin, <<"OK">>)
    ];
http2_frames(StreamID, trailers) ->
    {Final, State} = cow_hpack:encode([{<<":status">>, <<"200">>}]),
    {Trailers, _} = cow_hpack:encode([{<<"x-checksum">>, <<"1">>}], State),
    [
        cow_http2:headers(StreamID, nofin, Final),
        cow_http2:data(StreamID, nofin, <<"OK">>),
        cow_http2:headers(StreamID, fin, Trailers)
    ].

%% @doc Read frames until HEADERS, returning the request's stream ID.
receive_headers(Socket) ->
    {ok, <<Length:24, Type:8, _Flags:8, _:1, StreamID:31>>} =
        gen_tcp:recv(Socket, 9, ?TIMEOUT),
    case Length of
        0 -> ok;
        _ -> {ok, _} = gen_tcp:recv(Socket, Length, ?TIMEOUT)
    end,
    case Type of
        1 -> StreamID;
        _ -> receive_headers(Socket)
    end.

%% @doc Half-close and drain unread bytes to avoid resetting the TCP connection.
finish_response(Socket, Response) ->
    ok = gen_tcp:send(Socket, Response),
    ok = gen_tcp:shutdown(Socket, write),
    drain_request(Socket).

%% @doc Consume remaining request bytes until the client disconnects.
drain_request(Socket) ->
    case gen_tcp:recv(Socket, 0, ?TIMEOUT) of
        {ok, _} -> drain_request(Socket);
        {error, _} -> ok
    end.
