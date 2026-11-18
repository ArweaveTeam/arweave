%%% Regressions for HTTP connection buffering and decoded header limits.
-module(ar_http_security_tests).
-test_category([fast]).

-include_lib("eunit/include/eunit.hrl").

%% Bound stalled loopback exchanges, without asserting elapsed time.
-define(TIMEOUT, 2000).

http1_pipelined_request_backpressure_test() ->
    Parent = self(),
    Hold = fun(Req) ->
        Parent ! {held_request, self(), maps:get(pid, Req)},
        receive release -> {200, #{}, <<"first">>} end
    end,
    Routes = #{
        {<<"GET">>, <<"/hold">>} => Hold,
        {<<"GET">>, <<"/next">>} => {200, #{}, <<"second">>}
    },
    {ok, {127, 0, 0, 1, Port}, Ref, Table} =
        ar_test_http_server:start(Routes, #{protocols => [http]}),
    try
        {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, Port,
            [binary, {active, false}], ?TIMEOUT),
        try
            ok = gen_tcp:send(Socket,
                <<"GET /hold HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
            Held = receive
                {held_request, _, _} = Message -> Message
            after ?TIMEOUT -> timeout
            end,
            ?assertMatch({held_request, _, _}, Held),
            {held_request, Handler, Connection} = Held,
            try
                ok = gen_tcp:send(Socket,
                    <<"GET /next HTTP/1.1\r\nHost: localhost\r\n",
                        "Connection: close\r\n\r\n">>),
                %% Cowboy's private state tuple stores its socket at index 4.
                %% Inspect the socket, not VM memory or OS send-buffer sizes.
                ServerSocket = element(4, sys:get_state(Connection)),
                ?assertEqual(ok, ar_test_await:until(
                    pipelined_socket_passive,
                    fun() ->
                        inet:getopts(ServerSocket, [active]) =:=
                            {ok, [{active, false}]}
                    end,
                    ?TIMEOUT
                )),
                %% The socket also turns passive for an instant whenever
                %% {active, 1} delivers data. Let the connection process
                %% drain its mailbox, then require that it stayed passive.
                _ = sys:get_state(Connection),
                ?assertEqual({ok, [{active, false}]},
                    inet:getopts(ServerSocket, [active])),
                Handler ! release,
                {End, Response} = receive_http1(Socket, <<>>),
                ?assertEqual(closed, End),
                %% Both requests must finish, in order, on this connection.
                ?assertEqual(2,
                    length(binary:matches(Response, <<"HTTP/1.1 200">>))),
                ?assertMatch({_, _}, binary:match(Response,
                    <<"firstHTTP/1.1 200">>)),
                ?assertMatch({_, _}, binary:match(Response,
                    <<"\r\n\r\nsecond">>))
            after
                Handler ! release
            end
        after
            gen_tcp:close(Socket)
        end
    after
        ar_test_http_server:stop(Ref, Table)
    end.

http2_decoded_header_list_limit_test() ->
    {ok, Preface, State0} = cow_http2_machine:init(server, #{}),
    try
        {ok, State1} = cow_http2_machine:frame({settings, #{}}, State0),
        %% Four pseudo-headers and 96 cookies stay within the 100-field cap.
        %% The 4000-byte value fits the 4096-byte HPACK dynamic table, but
        %% 96 references expand past the default 32768-byte header-list cap.
        Prefix = <<16#82, 16#86, 16#84, 1, 9, "localhost">>,
        %% Static indexes 2/6/4: GET/http/"/"; index 1: :authority.
        %% 0x60 inserts cookie (static name 32) into dynamic slot 62.
        %% The 7-bit-prefixed length is 127 + 33 + 30 * 128 = 4000.
        Block = <<Prefix/binary, 16#60, 127, 161, 30,
            (binary:copy(<<"a">>, 4000))/binary,
            (binary:copy(<<16#be>>, 95))/binary>>,
        %% 0xbe is indexed slot 62; one insertion plus 95 copies = 96.
        Rejected = cow_http2_machine:frame(
            {headers, 1, fin, head_fin, Block}, State1),
        ?assertMatch(
            {error, {stream_error, 1, enhance_your_calm, _}, _}, Rejected),
        {error, _, State2} = Rejected,
        %% A valid next stream reuses the table entry from the rejected one.
        %% Client-initiated HTTP/2 streams use consecutive odd IDs: 1, 3.
        Next = <<Prefix/binary, 16#be>>,
        Accepted = cow_http2_machine:frame(
            {headers, 3, fin, head_fin, Next}, State2),
        ?assertMatch({ok, {headers, 3, fin, _, _, _}, _}, Accepted),
        {ok, {headers, 3, fin, Headers, _, _}, _} = Accepted,
        ?assertEqual([{<<"cookie">>, binary:copy(<<"a">>, 4000)}], Headers),
        %% The same finite limit must be advertised to the remote endpoint.
        {ok, {settings, Settings}, _} = cow_http2:parse(
            iolist_to_binary(Preface)),
        ?assertEqual(32768, maps:get(max_header_list_size, Settings))
    after
        cow_http2_machine:terminate(State0)
    end.

%% @doc Read the two finite HTTP/1 responses until the server closes.
receive_http1(Socket, Acc) ->
    case gen_tcp:recv(Socket, 0, ?TIMEOUT) of
        {ok, Data} -> receive_http1(Socket, <<Acc/binary, Data/binary>>);
        {error, Reason} -> {Reason, Acc}
    end.
