%%% @doc Tests that a client sending a POST /data_roots body slowly does not
%%% block other /data_roots requests.
-module(ar_http_data_roots_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

%% The declared body length. The slow client sends one byte a second, so it
%% is still sending the body for longer than the server reads a body.
-define(BODY_SIZE, 100).
-define(BYTE_INTERVAL_MS, 1000).

slow_post_body_does_not_block_data_roots_test_() ->
    {timeout, 120, fun test_slow_post_body_does_not_block_data_roots/0}.

test_slow_post_body_does_not_block_data_roots() ->
    [B0] = ar_weave:init(),
    ar_test_node:start(B0),
    BodyReadStarted = atomics:new(1, []),
    Mocks = [
        %% Accept data roots for the genesis block.
        {ar_disk_pool, get_threshold, fun() -> B0#block.weave_size end},
        {ar_data_roots, are_synced, fun(_, _, _, _) -> false end},
        {ar_http_req, body, fun(Req, SizeLimit) ->
            case cowboy_req:path(Req) of
                <<"/data_roots/0">> ->
                    atomics:put(BodyReadStarted, 1, 1);
                _ ->
                    ok
            end,
            meck:passthrough([Req, SizeLimit])
        end}
    ],
    ar_test_node:run_with_mocked([main], Mocks, fun() ->
        {_, _, _, _, Port} = Peer = ar_test_node:peer_ip(main),
        {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, Port,
            [binary, {active, false}]),
        ok = gen_tcp:send(Socket, [
            "POST /data_roots/0 HTTP/1.1\r\n",
            "host: 127.0.0.1:", integer_to_list(Port), "\r\n",
            "x-network: ", ?NETWORK_NAME, "\r\n",
            "content-length: ", integer_to_list(?BODY_SIZE), "\r\n\r\n"
        ]),
        Sender = spawn_link(fun() -> send_slowly(Socket, ?BODY_SIZE - 1) end),
        try
            ok = ar_test_await:until(data_roots_body_read_started, fun() ->
                atomics:get(BodyReadStarted, 1) == 1
            end),
            Reply = ar_http:req(#{
                method => get,
                peer => Peer,
                path => "/data_roots/0",
                timeout => 30_000
            }),
            ?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, Reply),
            %% The slow body is still being sent, so the POST has no reply.
            ?assertEqual({error, timeout}, gen_tcp:recv(Socket, 0, 0))
        after
            unlink(Sender),
            exit(Sender, kill),
            gen_tcp:close(Socket)
        end
    end).

%% @doc Send Count body bytes, one every ?BYTE_INTERVAL_MS.
send_slowly(_Socket, 0) ->
    ok;
send_slowly(Socket, Count) ->
    ok = gen_tcp:send(Socket, <<0>>),
    timer:sleep(?BYTE_INTERVAL_MS),
    send_slowly(Socket, Count - 1).
