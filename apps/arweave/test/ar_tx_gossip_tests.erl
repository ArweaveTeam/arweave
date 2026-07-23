-module(ar_tx_gossip_tests).
-test_peers([peer1]).

-include("ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("eunit/include/eunit.hrl").

accepts_gossips_and_mines_test_() ->
    PrepareTestFor = fun(BuildTXSetFun, KeyType) ->
        fun() ->
            %% Init the weave under the fork so price estimates match the
            %% current pricing model.
            Key = {_, Pub} = ar_wallet:new(KeyType),
            Wallets = [{ar_wallet:to_address(Pub), ?AR(5), <<>>}],
            [B0] = ar_weave:init(Wallets),
            accepts_gossips_and_mines(B0, BuildTXSetFun(Key, B0))
        end
    end,
    [
        {timeout, ?TEST_NODE_TIMEOUT, {
            "One RSA transaction with wallet list anchor followed by one with block anchor",
            PrepareTestFor(fun ar_tx_test_utils:one_wallet_list_one_block_anchored_txs/2,
                ?RSA_KEY_TYPE)
        }},
        {timeout, ?TEST_NODE_TIMEOUT, {
            "One ECDSA transaction with wallet list anchor followed by one with block anchor",
            PrepareTestFor(fun ar_tx_test_utils:one_wallet_list_one_block_anchored_txs/2,
                ?ECDSA_KEY_TYPE)
        }},
        {timeout, ?TEST_NODE_TIMEOUT, {
            "Two RSA transactions with block anchor",
            PrepareTestFor(fun ar_tx_test_utils:two_block_anchored_txs/2, ?RSA_KEY_TYPE)
        }},
        {timeout, ?TEST_NODE_TIMEOUT, {
            "Two ECDSA transactions with block anchor",
            PrepareTestFor(fun ar_tx_test_utils:two_block_anchored_txs/2, ?ECDSA_KEY_TYPE)
        }}
    ].

polls_for_transactions_and_gossips_and_mines_test_() ->
    PrepareTestFor = fun(BuildTXSetFun, KeyType) ->
        fun() ->
            %% Init the weave under the fork so price estimates match the
            %% current pricing model.
            Key = {_, Pub} = ar_wallet:new(KeyType),
            Wallets = [{ar_wallet:to_address(Pub), ?AR(5), <<>>}],
            [B0] = ar_weave:init(Wallets),
            polls_for_transactions_and_gossips_and_mines(B0, BuildTXSetFun(Key, B0))
        end
    end,
    [
        {timeout, ?TEST_NODE_TIMEOUT, {
            "Two RSA transactions with block anchor",
            PrepareTestFor(fun ar_tx_test_utils:two_block_anchored_txs/2, ?RSA_KEY_TYPE)
        }},
        {timeout, ?TEST_NODE_TIMEOUT, {
            "Two ECDSA transactions with block anchor",
            PrepareTestFor(fun ar_tx_test_utils:two_block_anchored_txs/2, ?ECDSA_KEY_TYPE)
        }}
    ].

keeps_txs_after_new_block_test_() ->
    PrepareTestFor = fun(BuildFirstTXSetFun, BuildSecondTXSetFun) ->
        fun() ->
            Key = {_, Pub} = ar_wallet:new(),
            Key2 = {_, Pub2} = ar_test_node:new_custom_size_rsa_wallet(66),
            Wallets = [{ar_wallet:to_address(Pub), ?AR(5), <<>>},
                    {ar_wallet:to_address(Pub2), ?AR(5), <<>>}],
            [B0] = ar_weave:init(Wallets),
            keeps_txs_after_new_block(
                B0,
                BuildFirstTXSetFun(Key, B0),
                BuildSecondTXSetFun(Key2, B0)
            )
        end
    end,
    [
        %% Main node receives the second set then the first set. Peer node only
        %% receives the second set.
        {timeout, ?TEST_NODE_TIMEOUT, {
            "First set: two block anchored txs, second set: empty",
            PrepareTestFor(
                fun ar_tx_test_utils:two_block_anchored_txs/2,
                fun ar_tx_test_utils:empty_tx_set/2)
        }},
        {timeout, ?TEST_NODE_TIMEOUT, {
            "First set: empty, second set: two block anchored txs",
            PrepareTestFor(
                fun ar_tx_test_utils:empty_tx_set/2,
                fun ar_tx_test_utils:two_block_anchored_txs/2)
        }},
        {timeout, ?TEST_NODE_TIMEOUT, {
            "First set: two block anchored txs, second set: two block anchored txs",
            PrepareTestFor(
                fun ar_tx_test_utils:two_block_anchored_txs/2,
                fun ar_tx_test_utils:two_block_anchored_txs/2)
        }}
    ].

accepts_gossips_and_mines(B0, TXFuns) ->
    %% TXs posted to peer1 are gossiped to main, mined into a block, and that
    %% block is accepted by both nodes.
    _ = ar_test_node:start(B0),
    _ = ar_test_node:start_peer(peer1, B0),
    %% Sign after the node starts so the price estimate comes from it.
    TXs = lists:map(fun(TXFun) -> TXFun() end, TXFuns),
    ar_test_node:connect_to_peer(peer1),
    lists:foreach(
        fun(TX) ->
            ar_test_node:assert_post_tx_to_peer(peer1, TX),
            %% Gossiped to main.
            ?assertEqual(ok, ar_test_await:txs_ready_for_mining(main, [TX]))
        end,
        TXs
    ),
    ar_test_node:mine(peer1),
    {ok, PeerBI} = ar_test_await:node_height(peer1, 1),
    TXIDs = lists:map(fun(TX) -> TX#tx.id end, TXs),
    ?assertEqual(
        lists:sort(TXIDs),
        lists:sort((ar_test_node:remote_call(peer1, ar_test_await, block_stored, [hd(PeerBI)]))#block.txs)
    ),
    lists:foreach(
        fun(TX) ->
            ?assertEqual(TX, ar_test_node:remote_call(peer1, ar_storage, read_tx, [TX#tx.id]))
        end,
        TXs
    ),
    {ok, BI} = ar_test_await:node_height(main, 1),
    ?assertEqual(
        lists:sort(TXIDs),
        lists:sort((ar_test_await:block_stored(hd(BI)))#block.txs)
    ),
    lists:foreach(
        fun(TX) ->
            ?assertEqual(TX, ar_storage:read_tx(TX#tx.id))
        end,
        TXs
    ).

polls_for_transactions_and_gossips_and_mines(B0, TXFuns) ->
    %% Like accepts_gossips_and_mines, but gossip is disabled so main must
    %% poll peer1 for the TXs before they are mined and the block accepted.
    MainConfig = arweave_config:snapshot(),
    PeerConfig = ar_test_node:remote_call(peer1, arweave_config, snapshot, []),
    try
        _ = ar_test_node:start(#{ b0 => B0,
                config => #{ [gossip, tx, max_peers] => 0 } }),
        _ = ar_test_node:start_peer(peer1, #{ b0 => B0,
                config => #{ [gossip, tx, max_peers] => 0 } }),
        %% Sign after the node starts so the price estimate comes from it.
        TXs = lists:map(fun(TXFun) -> TXFun() end, TXFuns),
        ar_test_node:connect_to_peer(peer1),
        lists:foreach(
            fun(TX) ->
                ar_test_node:assert_post_tx_to_peer(peer1, TX),
                %% Fetched by main via polling.
                ?assertEqual(ok, ar_test_await:txs_ready_for_mining(main, [TX]))
            end,
            TXs
        ),
        ar_test_node:mine(peer1),
        {ok, PeerBI} = ar_test_await:node_height(peer1, 1),
        TXIDs = lists:map(fun(TX) -> TX#tx.id end, TXs),
        ?assertEqual(
            lists:sort(TXIDs),
            lists:sort((ar_test_node:remote_call(peer1, ar_test_await, block_stored, [hd(PeerBI)]))#block.txs)
        ),
        lists:foreach(
            fun(TX) ->
                ?assertEqual(TX, ar_test_node:remote_call(peer1, ar_storage, read_tx, [TX#tx.id]))
            end,
            TXs
        ),
        {ok, BI} = ar_test_await:node_height(main, 1),
        ?assertEqual(
            lists:sort(TXIDs),
            lists:sort((ar_test_await:block_stored(hd(BI)))#block.txs)
        ),
        lists:foreach(
            fun(TX) ->
                ?assertEqual(TX, ar_storage:read_tx(TX#tx.id))
            end,
            TXs
        )
    after
        arweave_config:restore(MainConfig),
        ar_test_node:remote_call(peer1, arweave_config, restore, [PeerConfig])
    end.

keeps_txs_after_new_block(B0, FirstTXSetFuns, SecondTXSetFuns) ->
    %% main holds the first set (ungossiped) plus the second set; peer1 mines the
    %% second set into a block. After main accepts that block, the set difference
    %% stays in main's mempool and is mined into main's next block.
    MainConfig = arweave_config:snapshot(),
    PeerConfig = ar_test_node:remote_call(peer1, arweave_config, snapshot, []),

    try
        _ = ar_test_node:start(#{ b0 => B0,
                config => #{ [gossip, tx, polling_enabled] => false } }),
        _ = ar_test_node:start_peer(peer1, #{ b0 => B0,
                config => #{ [gossip, tx, polling_enabled] => false } }),
        %% Sign after the node starts so the price estimate comes from it.
        FirstTXSet = lists:map(fun(TXFun) -> TXFun() end, FirstTXSetFuns),
        SecondTXSet = lists:map(fun(TXFun) -> TXFun() end, SecondTXSetFuns),
        %% Disconnect so peer1 does not receive main's TXs.
        ar_test_node:disconnect_from(peer1),
        lists:foreach(
            fun(TX) ->
                ar_test_node:post_tx_to_peer(main, TX)
            end,
            SecondTXSet ++ FirstTXSet
        ),
        ?assertEqual([], ar_test_node:remote_call(peer1, ar_mempool, get_all_txids, [])),
        lists:foreach(
            fun(TX) ->
                ar_test_node:assert_post_tx_to_peer(peer1, TX)
            end,
            SecondTXSet
        ),
        %% Wait long enough that the TXs won't be gossiped on reconnect.
        timer:sleep(2000), % == 2 * ?CHECK_MEMPOOL_FREQUENCY
        ar_test_node:connect_to_peer(peer1),
        ar_test_node:mine(peer1),
        {ok, BI} = ar_test_await:node_height(main, 1),
        SecondSetTXIDs = lists:map(fun(TX) -> TX#tx.id end, SecondTXSet),
        ?assertEqual(lists:sort(SecondSetTXIDs),
                lists:sort((ar_test_await:block_stored(hd(BI)))#block.txs)),
        %% The set difference remains in main's mempool.
        ?assertEqual(ok, ar_test_await:txs_ready_for_mining(main, FirstTXSet -- SecondTXSet)),
        ar_test_node:mine(),
        {ok, BI2} = ar_test_await:node_height(main, 2),
        SetDifferenceTXIDs = lists:map(fun(TX) -> TX#tx.id end, FirstTXSet -- SecondTXSet),
        ?assertEqual(
            lists:sort(SetDifferenceTXIDs),
            lists:sort((ar_test_await:block_stored(hd(BI2)))#block.txs)
        )
    after
        arweave_config:restore(MainConfig),
        ar_test_node:remote_call(peer1, arweave_config, restore, [PeerConfig])
    end.
