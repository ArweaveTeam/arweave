%%% @doc Tests for the GET and POST /data_roots endpoints.
-module(ar_http_data_roots_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

%% The declared body length. The slow client sends one byte a second, so it
%% is still sending the body for longer than the server reads a body.
-define(BODY_SIZE, 100).
-define(BYTE_INTERVAL_MS, 1000).

%% An offset inside the genesis block that no background process looks up.
-define(OFFSET, 1).

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

post_data_roots_after_reorg_test_() ->
    {timeout, 120, fun test_post_data_roots_after_reorg/0}.

test_post_data_roots_after_reorg() ->
    [B0] = ar_weave:init(),
    ar_test_node:start(B0),
    BlockEnd = B0#block.weave_size,
    %% Before the body read, the offset belongs to a block that a reorg then
    %% replaces with B0.
    {BoundsMock, BoundsCalls} =
        block_bounds_mock([{0, BlockEnd + 1, <<"orphaned">>}]),
    Mocks = [
        {ar_disk_pool, get_threshold, fun() -> BlockEnd end},
        BoundsMock
    ],
    ar_test_node:run_with_mocked([main], Mocks, fun() ->
        {ok, DataRoots} = ar_data_roots:get_block(0),
        {ok, _} = ar_data_roots:remove_range(0, BlockEnd, ?DEFAULT_MODULE),
        Reply = post_data_roots(ar_serialize:data_roots_to_binary(DataRoots)),
        ?assertMatch({ok, {{<<"200">>, _}, _, <<>>, _, _}}, Reply),
        ?assertEqual(2, atomics:get(BoundsCalls, 1)),
        ?assert(ar_data_roots:are_synced(0, BlockEnd, B0#block.tx_root,
            ?DEFAULT_MODULE))
    end).

data_roots_offset_without_block_test_() ->
    {timeout, 120, fun test_data_roots_offset_without_block/0}.

test_data_roots_offset_without_block() ->
    [B0] = ar_weave:init(),
    ar_test_node:start(B0),
    {BoundsMock, _BoundsCalls} = block_bounds_mock([not_found, not_found]),
    Mocks = [
        {ar_disk_pool, get_threshold, fun() -> B0#block.weave_size end},
        BoundsMock
    ],
    ar_test_node:run_with_mocked([main], Mocks, fun() ->
        ?assertMatch({ok, {{<<"404">>, _}, _, _, _, _}},
            post_data_roots(<<>>)),
        ?assertMatch({ok, {{<<"404">>, _}, _, _, _, _}},
            ar_http:req(#{
                method => get,
                peer => ar_test_node:peer_ip(main),
                path => data_roots_path()
            }))
    end).

post_data_roots_block_gone_during_body_read_test_() ->
    {timeout, 120, fun test_post_data_roots_block_gone_during_body_read/0}.

test_post_data_roots_block_gone_during_body_read() ->
    [B0] = ar_weave:init(),
    ar_test_node:start(B0),
    BlockEnd = B0#block.weave_size,
    {BoundsMock, BoundsCalls} = block_bounds_mock([passthrough, not_found]),
    Mocks = [
        {ar_disk_pool, get_threshold, fun() -> BlockEnd end},
        BoundsMock
    ],
    ar_test_node:run_with_mocked([main], Mocks, fun() ->
        {ok, DataRoots} = ar_data_roots:get_block(0),
        {ok, _} = ar_data_roots:remove_range(0, BlockEnd, ?DEFAULT_MODULE),
        Reply = post_data_roots(ar_serialize:data_roots_to_binary(DataRoots)),
        ?assertMatch({ok, {{<<"404">>, _}, _, _, _, _}}, Reply),
        ?assertEqual(2, atomics:get(BoundsCalls, 1))
    end).

%% @doc Mock ar_block_index:get_block_bounds/1 to answer the lookups of
%% ?OFFSET with Responses in order, where passthrough means a real lookup,
%% and with real lookups afterwards. Also return the ?OFFSET lookup counter.
block_bounds_mock(Responses) ->
    Calls = atomics:new(1, []),
    Mock = fun
        (?OFFSET) ->
            N = atomics:add_get(Calls, 1, 1),
            case N > length(Responses)
                    orelse lists:nth(N, Responses) == passthrough of
                true ->
                    meck:passthrough([?OFFSET]);
                false ->
                    lists:nth(N, Responses)
            end;
        (Offset) ->
            meck:passthrough([Offset])
    end,
    {{ar_block_index, get_block_bounds, Mock}, Calls}.

post_data_roots(Body) ->
    ar_http:req(#{
        method => post,
        peer => ar_test_node:peer_ip(main),
        path => data_roots_path(),
        body => Body
    }).

data_roots_path() ->
    "/data_roots/" ++ integer_to_list(?OFFSET).

%% @doc Send Count body bytes, one every ?BYTE_INTERVAL_MS.
send_slowly(_Socket, 0) ->
    ok;
send_slowly(Socket, Count) ->
    ok = gen_tcp:send(Socket, <<0>>),
    timer:sleep(?BYTE_INTERVAL_MS),
    send_slowly(Socket, Count - 1).
