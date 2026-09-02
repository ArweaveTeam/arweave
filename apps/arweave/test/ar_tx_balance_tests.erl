-module(ar_tx_balance_tests).
-test_peers([peer1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

returns_error_when_txs_exceed_balance_test_() ->
    PrepareTestFor = fun(BuildTXSetFun) ->
        fun() ->
            returns_error_when_txs_exceed_balance(BuildTXSetFun)
        end
    end,
    [
        {timeout, ?TEST_NODE_TIMEOUT, {
            "Three transactions with block anchor",
            PrepareTestFor(
                fun ar_tx_test_utils:block_anchor_txs_spending_balance_plus_one_more/2)
        }},
        {timeout, ?TEST_NODE_TIMEOUT, {
            "Five transactions with mixed anchors",
            PrepareTestFor(
                fun ar_tx_test_utils:mixed_anchor_txs_spending_balance_plus_one_more/2)
        }}
    ].

does_not_allow_to_spend_mempool_tokens_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun test_does_not_allow_to_spend_mempool_tokens/0}.

does_not_allow_to_replay_empty_wallet_txs_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun test_does_not_allow_to_replay_empty_wallet_txs/0}.

returns_error_when_txs_exceed_balance(BuildTXSetFun) ->
    Key = {_, Pub} = ar_wallet:new(),
    [B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),

    _ = ar_test_node:start(B0),
    _ = ar_test_node:start_peer(peer1, B0),

    TXs = BuildTXSetFun(Key, B0),

    ar_test_node:connect_to_peer(peer1),

    %% All posts succeed, but each mempool insert re-checks balances and ejects
    %% overspending TXs in {Utility, TXID} order, so among equal-utility TXs the
    %% lower TXID is ejected first.
    SortedTXs = lists:sort(
        fun(TX1, TX2) ->
            %% Sort in reverse order - "biggest" first.
            {ar_tx:utility(TX1), TX1#tx.id} > {ar_tx:utility(TX2), TX2#tx.id}
        end,
        TXs
    ),
    ExceedBalanceTX = lists:last(SortedTXs),
    BelowBalanceTXs = lists:droplast(SortedTXs),
    lists:foreach(
        fun(TX) ->
            ar_test_node:assert_post_tx_to_peer(peer1, TX, false)
        end,
        TXs
    ),

    ?assertEqual(ok, ar_test_await:txs_ready_for_mining(main, BelowBalanceTXs)),
    ar_test_node:mine(peer1),
    {ok, PeerBI} = ar_test_await:node_height(peer1, 1),
    TXIDs = lists:map(fun(TX) -> TX#tx.id end, BelowBalanceTXs),
    ?assertEqual(
        lists:sort(TXIDs),
        lists:sort((ar_test_node:remote_call(peer1, ar_test_await, block_stored, [hd(PeerBI)]))#block.txs)
    ),
    {ok, BI} = ar_test_await:node_height(main, 1),
    ?assertEqual(
        lists:sort(TXIDs),
        lists:sort((ar_test_await:block_stored(hd(BI)))#block.txs)
    ),
    %% Reposting the overspending TX fails with an overspend error.
    ar_test_node:remote_call(peer1, ets, delete, [ignored_ids, ExceedBalanceTX#tx.id]),
    {ok, {{<<"400">>, _}, _, _Body, _, _}} =
        ar_http:req(#{
            method => post,
            peer => ar_test_node:peer_ip(peer1),
            path => "/tx",
            body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(ExceedBalanceTX))
        }),
    ?assertEqual({ok, ["overspend"]}, ar_test_node:remote_call(peer1, ar_tx_db, get_error_codes,
            [ExceedBalanceTX#tx.id])).

test_does_not_allow_to_spend_mempool_tokens() ->
    %% A TX spending tokens that are still only in a mempool TX is rejected;
    %% once that funding TX is mined, spending the tokens is accepted.
    Key1 = {_, Pub1} = ar_wallet:new(),
    Key2 = {_, Pub2} = ar_wallet:new(),
    [B0] = ar_weave:init([
        {ar_wallet:to_address(Pub1), ?AR(20), <<>>},
        {ar_wallet:to_address(Pub2), ?AR(0), <<>>}
    ]),
    _ = ar_test_node:start_peer(peer1, B0),
    _ = ar_test_node:connect_to_peer(peer1),
    TX1 = ar_test_node:sign_tx(Key1, #{ target => ar_wallet:to_address(Pub2), reward => ?AR(1),
            quantity => ?AR(2) }),
    ar_test_node:assert_post_tx_to_peer(peer1, TX1),
    TX2 = ar_test_node:sign_tx(
        Key2,
        #{
            target => ar_wallet:to_address(Pub1),
            reward => ?AR(1),
            quantity => ?AR(1),
            last_tx => B0#block.indep_hash,
            tags => [{<<"nonce">>, <<"1">>}]
        }
    ),
    {ok, {{<<"400">>, _}, _, _, _, _}} = ar_test_node:post_tx_to_peer(peer1, TX2),
    ?assertEqual({ok, ["overspend"]}, ar_test_node:remote_call(peer1, ar_tx_db, get_error_codes, [TX2#tx.id])),
    ar_test_node:mine(peer1),
    {ok, PeerBI} = ar_test_await:node_height(peer1, 1),
    B1 = ar_test_node:remote_call(peer1, ar_test_await, block_stored, [hd(PeerBI)]),
    ?assertEqual([TX1#tx.id], B1#block.txs),
    TX3 = ar_test_node:sign_tx(
        Key2,
        #{
            target => ar_wallet:to_address(Pub1),
            reward => ?AR(1),
            quantity => ?AR(1),
            last_tx => B1#block.indep_hash,
            tags => [{<<"nonce">>, <<"3">>}]
        }
    ),
    ar_test_node:assert_post_tx_to_peer(peer1, TX3),
    ar_test_node:mine(peer1),
    {ok, PeerBI2} = ar_test_await:node_height(peer1, 2),
    B2 = ar_test_node:remote_call(peer1, ar_test_await, block_stored, [hd(PeerBI2)]),
    ?assertEqual([TX3#tx.id], B2#block.txs).

test_does_not_allow_to_replay_empty_wallet_txs() ->
    %% Fund a wallet, drain it back to zero, then re-fund it. Replaying the
    %% drain TX is rejected even though the balance would now cover it.
    Key1 = {_, Pub1} = ar_wallet:new(),
    Key2 = {_, Pub2} = ar_wallet:new(),
    [B0] = ar_weave:init([
        {ar_wallet:to_address(Pub1), ?AR(50), <<>>}
    ]),
    _ = ar_test_node:start_peer(peer1, B0),
    TX1 = ar_test_node:sign_tx(Key1, #{ target => ar_wallet:to_address(Pub2), reward => ?AR(6),
            quantity => ?AR(2), last_tx => <<>> }),
    ar_test_node:assert_post_tx_to_peer(peer1, TX1),
    ar_test_node:mine(peer1),
    ?assertMatch({ok, _}, ar_test_await:node_height(peer1, 1)),
    GetBalancePath = binary_to_list(arweave_util:encode(ar_wallet:to_address(Pub2))),
    {ok, {{<<"200">>, _}, _, Body, _, _}} =
        ar_http:req(#{
            method => get,
            peer => ar_test_node:peer_ip(peer1),
            path => "/wallet/" ++ GetBalancePath ++ "/balance"
        }),
    Balance = binary_to_integer(Body),
    TX2 = ar_test_node:sign_tx(Key2, #{ target => ar_wallet:to_address(Pub1), reward => Balance - ?AR(1),
            quantity => ?AR(1), last_tx => <<>> }),
    ar_test_node:assert_post_tx_to_peer(peer1, TX2),
    ar_test_node:mine(peer1),
    ?assertMatch({ok, _}, ar_test_await:node_height(peer1, 2)),
    {ok, {{<<"200">>, _}, _, Body2, _, _}} =
        ar_http:req(#{
            method => get,
            peer => ar_test_node:peer_ip(peer1),
            path => "/wallet/" ++ GetBalancePath ++ "/balance"
        }),
    ?assertEqual(0, binary_to_integer(Body2)),
    TX3 = ar_test_node:sign_tx(Key1, #{ target => ar_wallet:to_address(Pub2), reward => ?AR(6),
            quantity => ?AR(2), last_tx => TX1#tx.id }),
    ar_test_node:assert_post_tx_to_peer(peer1, TX3),
    ar_test_node:mine(peer1),
    ?assertMatch({ok, _}, ar_test_await:node_height(peer1, 3)),
    %% Remove the replay TX from the ignore list (to simulate e.g. a node restart).
    ar_test_node:remote_call(peer1, ets, delete, [ignored_ids, TX2#tx.id]),
    {ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx).">>, _, _}} =
        ar_test_node:post_tx_to_peer(peer1, TX2).
