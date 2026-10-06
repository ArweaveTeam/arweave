%%% @doc HTTP/2 fallback regressions using loopback sockets and real Gun.
-module(ar_http_fallback_tests).
-test_category([fast]).

-include_lib("eunit/include/eunit.hrl").

%% Fail a stuck loopback exchange promptly; these are not timing assertions.
-define(TIMEOUT, 3000).

%% @doc A peer closing without a reply causes failure, not HTTP/1 fallback.
silent_close_keeps_http2_test() ->
    with_server(silent_close, fun(Peer, Counts) ->
        ?assertEqual({error, client_error}, request(Peer)),
        ?assert(http2_connection_count(Counts) >= 1),
        ?assertEqual(0, http1_connection_count(Counts))
    end).

%% @doc An HTTP/2 reset requests fallback to HTTP/1.
http1_required_reset_test() ->
    with_server({reset, http_1_1_required}, fun(Peer, Counts) ->
        %% HTTP/2 fails, then the automatic HTTP/1 retry succeeds.
        ?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, request(Peer)),
        %% The next call uses HTTP/1 directly: one HTTP/2 and two HTTP/1 in total.
        ?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, request(Peer)),
        ?assertEqual(1, http2_connection_count(Counts)),
        ?assertEqual(2, http1_connection_count(Counts))
    end).

%% @doc An HTTP/2 GOAWAY also requests fallback to HTTP/1.
http1_required_goaway_test() ->
    with_server({goaway, http_1_1_required}, fun(Peer, Counts) ->
        %% HTTP/2 fails, then the automatic HTTP/1 retry succeeds.
        ?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, request(Peer)),
        %% The next call uses HTTP/1 directly: one HTTP/2 and two HTTP/1 in total.
        ?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, request(Peer)),
        ?assertEqual(1, http2_connection_count(Counts)),
        ?assertEqual(2, http1_connection_count(Counts))
    end).

%% @doc Overload fails the first call; the next succeeds on HTTP/2.
overload_recovery_keeps_http2_test() ->
    with_server(overload_then_recover, fun(Peer, Counts) ->
        ?assertEqual({error, client_error}, request(Peer)),
        %% Overload does not mean HTTP/2 is unsupported, so do not try HTTP/1.
        ?assert(http2_connection_count(Counts) >= 1),
        ?assertMatch({ok, {{<<"200">>, _}, _, <<"OK">>, _, _}}, request(Peer)),
        ?assertEqual(0, http1_connection_count(Counts))
    end).

%% @doc The HTTP/1 retry succeeds before Gun reports the failed HTTP/2 connection.
request_error_before_owner_notification_test() ->
    Parent = self(),
    ar_test_util:run_with_mocked([
        {gun, reply, fun
            (To, {gun_error, PID, {protocol_error, _}} = Message) ->
                Result = meck:passthrough([To, Message]),
                Parent ! {refusal_sent, PID},
                receive
                    release_gun -> Result
                after ?TIMEOUT -> error(gun_not_released)
                end;
            (To, Message) ->
                meck:passthrough([To, Message])
        end}
    ], fun() ->
        with_server(http1_response, fun(Peer, Counts) ->
            Caller = spawn_link(fun() ->
                Parent ! {result, self(), request(Peer)}
            end),
            PID =
                receive
                    {refusal_sent, GunPID} -> GunPID
                after ?TIMEOUT -> error(no_refusal)
                end,
            try
                %% Keep the failed HTTP/2 connection paused; its HTTP/1 retry
                %% must succeed without waiting for it to finish closing.
                ?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, result(Caller)),
                ?assertEqual(1, http2_connection_count(Counts)),
                ?assertEqual(1, http1_connection_count(Counts))
            after
                PID ! release_gun
            end
        end)
    end).

%% @doc Count connections that sent an HTTP/1 request.
http1_connection_count(Counts) ->
    atomics:get(Counts, 2).

%% @doc Count connections that sent the HTTP/2 preface.
http2_connection_count(Counts) ->
    atomics:get(Counts, 1).

%% @doc Send one request with enough time for a local retry.
request(Peer) ->
    ar_http:req(#{
        method => get,
        peer => Peer,
        path => "/probe",
        timeout => ?TIMEOUT
    }).

%% @doc Receive a specific caller's result without accepting unrelated messages.
result(Caller) ->
    receive
        {result, Caller, Reply} -> Reply
    after ?TIMEOUT -> error({no_result, Caller})
    end.

%% @doc Run a stub counting HTTP/2 and HTTP/1.1 connections separately.
with_server(Mode, Fun) ->
    {ok, Listener} = gen_tcp:listen(0, [
        binary,
        {active, false},
        {ip, {127, 0, 0, 1}}
    ]),
    {ok, Port} = inet:port(Listener),
    Counts = atomics:new(2, []),
    Server = spawn_link(fun() -> accept(Listener, Mode, Counts) end),
    try
        Fun({127, 0, 0, 1, Port}, Counts)
    after
        gen_tcp:close(Listener),
        unlink(Server),
        exit(Server, shutdown)
    end.

%% @doc Accept concurrently so a paused connection cannot block its retry.
accept(Listener, Mode, Counts) ->
    case gen_tcp:accept(Listener) of
        {ok, Socket} ->
            Worker = spawn_link(fun() ->
                receive
                    {socket, Socket} -> serve(Socket, Mode, Counts)
                end
            end),
            ok = gen_tcp:controlling_process(Socket, Worker),
            Worker ! {socket, Socket},
            accept(Listener, Mode, Counts);
        {error, closed} ->
            ok
    end.

%% @doc Distinguish the fixed 24-byte HTTP/2 preface from an HTTP/1.1 request.
serve(Socket, Mode, Counts) ->
    try
        case gen_tcp:recv(Socket, 24, ?TIMEOUT) of
            {ok, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>} ->
                Attempt = atomics:add_get(Counts, 1, 1),
                serve_http2(Socket, Mode, Attempt);
            {ok, _HTTP1} ->
                atomics:add(Counts, 2, 1),
                serve_http1(Socket);
            {error, _} ->
                ok
        end
    after
        gen_tcp:close(Socket)
    end.

%% @doc Return a valid HTTP/1.1 response to fallback requests.
serve_http1(Socket) ->
    finish_response(
        Socket,
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK"
    ).

%% @doc Produce the specific wire failure under test after the client's preface.
serve_http2(Socket, overload_then_recover, Attempt) when Attempt =< 2 ->
    %% Refuse the original request and its one retry, then allow recovery.
    serve_http2(Socket, {goaway, enhance_your_calm});
serve_http2(Socket, overload_then_recover, _Attempt) ->
    ok = gen_tcp:send(Socket, cow_http2:settings(#{})),
    StreamID = receive_headers(Socket),
    {Headers, _} = cow_hpack:encode([{<<":status">>, <<"200">>}]),
    finish_response(Socket, [
        cow_http2:headers(StreamID, nofin, Headers),
        cow_http2:data(StreamID, fin, <<"OK">>)
    ]);
serve_http2(Socket, Mode, _Attempt) ->
    serve_http2(Socket, Mode).

serve_http2(Socket, silent_close) ->
    finish_response(Socket, <<>>);
serve_http2(Socket, http1_response) ->
    %% Wait for the request so the refusal reaches its caller before gun_down.
    receive_headers(Socket),
    finish_response(
        Socket,
        "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n"
    );
serve_http2(Socket, {reset, Code}) ->
    ok = gen_tcp:send(Socket, cow_http2:settings(#{})),
    StreamID = receive_headers(Socket),
    finish_response(Socket, cow_http2:rst_stream(StreamID, Code));
serve_http2(Socket, {goaway, Code}) ->
    ok = gen_tcp:send(Socket, cow_http2:settings(#{})),
    receive_headers(Socket),
    finish_response(Socket, cow_http2:goaway(0, Code, <<>>)).

%% @doc Read frames until HEADERS guarantees Gun has a request to notify.
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
