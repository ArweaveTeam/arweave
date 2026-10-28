-module(ar_block_tx_body_tests).
-test_peers([peer1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

tx_body_check_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun test_tx_body_check/0}.

test_tx_body_check() ->
    Key = ar_wallet:new(),
    [B0] = ar_weave:init([{ar_wallet:to_address(Key), ?AR(100), <<>>}]),
    ar_test_node:start(#{ b0 => B0, config => #{
        [gossip, block, throttle_by_ip_interval] => 0,
        [gossip, block, throttle_by_solution_interval] => 0 } }),
    ar_test_node:start_peer(peer1, B0),
    ar_test_node:disconnect_from(peer1),
    TX = ar_test_node:sign_tx(peer1, Key,
                              #{ tags => [{<<"name">>, <<"a">>}] }),
    ar_test_node:assert_post_tx_to_peer(peer1, TX),
    ar_test_node:mine(peer1),
    {ok, [{H, _, _} | _]} = ar_test_await:node_height(peer1, 1),
    B = ar_test_node:remote_call(peer1, ar_block_cache, get, [block_cache, H]),
    TX2 = TX#tx{ tags = [{<<"name">>, <<"b">>}] },
    Sender = {127, 0, 0, 2, 1984},
    OtherSender = {127, 0, 0, 3, 1984},
    ?assertEqual(invalid, ar_block_pre_validator:pre_validate(
                            B#block{ txs = [TX2] }, Sender,
                            erlang:timestamp())),
    ?assertNot(ar_ignore_registry:member(H)),
    ?assertNot(ar_ignore_registry:member({H, Sender})),
    BanKey = {ban, {127, 0, 0, 2}},
    ok = ar_test_await:until(sender_banned, fun() ->
        ets:member(ar_blacklist_middleware, BanKey)
    end),
    [{_, Expires}] = ets:lookup(ar_blacklist_middleware, BanKey),
    ?assert(Expires =< os:system_time(seconds) + ?BAD_TX_BODY_BAN_TIME),
    Sender2 = {127, 0, 0, 4, 1984},
    meck:new(ar_block_cache, [passthrough]),
    meck:expect(ar_block_cache, rejects_resigned_relative_solution,
        fun(_Height) -> true end),
    try
        ?assertEqual(invalid, ar_block_pre_validator:pre_validate(
                                B#block{ txs = [TX2] }, Sender2,
                                erlang:timestamp())),
        BanKey2 = {ban, {127, 0, 0, 4}},
        ok = ar_test_await:until(sender2_banned, fun() ->
            ets:member(ar_blacklist_middleware, BanKey2)
        end),
        [{_, Expires2}] = ets:lookup(ar_blacklist_middleware, BanKey2),
        ?assert(Expires2 > os:system_time(seconds) + ?BAD_BLOCK_BAN_TIME - 60)
    after
        meck:unload(ar_block_cache)
    end,
    ?assertEqual(ok, ar_block_pre_validator:pre_validate(B, OtherSender,
                                                         erlang:timestamp())),
    ?assertMatch({ok, [{H, _, _} | _]}, ar_test_await:node_height(main, 1)).
