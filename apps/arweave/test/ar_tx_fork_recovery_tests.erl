-module(ar_tx_fork_recovery_tests).
-test_peers([peer1]).


-include("ar.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include_lib("eunit/include/eunit.hrl").


recovers_from_forks_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
            [{ar_fork, height_2_9_6, fun() -> infinity end}],
            fun() -> recovers_from_forks(7) end, ?TEST_NODE_TIMEOUT).

re_admits_orphaned_tx_after_fork_recovery_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun re_admits_orphaned_tx_after_fork_recovery/0}.

anchor_depth_boundary_after_fork_recovery_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun anchor_depth_boundary_after_fork_recovery/0}.

recovers_from_forks(ForkHeight) ->
    %% peer1 and main mine in sync, then diverge; an extra block on peer1 makes
    %% main fork-recover onto it. Afterwards, replaying any past TX on main is
    %% rejected, while the orphaned fork's TXs are accepted and mined again.
    Key = {_, Pub} = ar_wallet:new(),
    [B0] = ar_weave:init([
        {ar_wallet:to_address(Pub), ?AR(20), <<>>}
    ]),
    _ = ar_test_node:start(B0),
    _ = ar_test_node:start_peer(peer1, B0),
    ar_test_node:connect_to_peer(peer1),
    MainPort = arweave_config:get([port]),
    PreForkTXs = lists:foldl(
        fun(Height, TXs) ->
            TX = ar_test_node:sign_v1_tx(Key, #{ last_tx => ar_test_node:get_tx_anchor(peer1),
                    tags => [{<<"nonce">>, random_nonce()}] }),
            ar_test_node:assert_post_tx_to_peer(peer1, TX),
            ?assertEqual(ok, ar_test_await:txs_ready_for_mining(main, [TX])),
            ar_test_node:mine(peer1),
            {ok, BI} = ar_test_await:node_height(peer1, Height),
            {ok, BI} = ar_test_await:node_height(main, Height),
            assert_block_txs(peer1, [TX], BI),
            assert_block_txs(main, [TX], BI),
            TXs ++ [TX]
        end,
        [],
        lists:seq(1, ForkHeight)
    ),
    PostTXToMain =
        fun() ->
            UnsignedTX = #{ last_tx => ar_test_node:get_tx_anchor(main),
                    tags => [{<<"nonce">>, random_nonce()}], reward => ?AR(1) },
            TX = case rand:uniform(2) of
                1 ->
                    ar_test_node:sign_tx(main, Key, UnsignedTX);
                2 ->
                    ar_test_node:sign_v1_tx(main, Key, UnsignedTX)
            end,
            ar_test_node:assert_post_tx_to_peer(main, TX),
            [TX]
        end,
    PostTXToPeer =
        fun() ->
            UnsignedTX = #{ last_tx => ar_test_node:get_tx_anchor(peer1),
                    tags => [{<<"nonce">>, random_nonce()}] },
            TX = case rand:uniform(2) of
                1 ->
                    ar_test_node:sign_tx(Key, UnsignedTX);
                2 ->
                    ar_test_node:sign_v1_tx(Key, UnsignedTX)
            end,
            ar_test_node:assert_post_tx_to_peer(peer1, TX),
            [TX]
        end,
    ar_test_node:disconnect_from(peer1),
    {MainPostForkTXs, PeerPostForkTXs} = lists:foldl(
        fun(Height, {MainTXs, PeerTXs}) ->
            UpdatedMainTXs = MainTXs ++ ([NewMainTX] = PostTXToMain()),
            ar_test_node:mine(),
            {ok, BI} = ar_test_await:node_height(main, Height),
            assert_block_txs(main, [NewMainTX], BI),
            UpdatedPeerTXs = PeerTXs ++ ([NewPeerTX] = PostTXToPeer()),
            ar_test_node:mine(peer1),
            {ok, PeerBI} = ar_test_await:node_height(peer1, Height),
            assert_block_txs(peer1, [NewPeerTX], PeerBI),
            {UpdatedMainTXs, UpdatedPeerTXs}
        end,
        {[], []},
        lists:seq(ForkHeight + 1, 9)
    ),
    ar_test_node:connect_to_peer(peer1),
    TX2 = ar_test_node:sign_tx(Key, #{ last_tx => ar_test_node:get_tx_anchor(peer1),
            tags => [{<<"nonce">>, random_nonce()}] }),
    ar_test_node:assert_post_tx_to_peer(peer1, TX2),
    ?assertEqual(ok, ar_test_await:txs_ready_for_mining(main, [TX2])),
    ar_test_node:mine(peer1),
    ?assertMatch({ok, _}, ar_test_await:node_height(peer1, 10)),
    ?assertMatch({ok, _}, ar_test_await:node_height(main, 10)),
    forget_txs(
        PreForkTXs ++
        MainPostForkTXs ++
        PeerPostForkTXs ++
        [TX2]
    ),
    %% The pre-fork, fork-recovery, and fresh TXs are all on the weave.
    lists:foreach(
        fun(TX) ->
            ok = ar_test_await:tx_confirmed(main, TX#tx.id),
            {ok, {{<<"400">>, _}, _, _, _, _}} =
                ar_test_node:post_tx_to_peer(main, TX)
        end,
        PreForkTXs ++ PeerPostForkTXs ++ [TX2]
    ),
    %% The abandoned fork's block-anchored TXs are back in the mempool.
    lists:foreach(
        fun(TX) ->
            {ok, {{<<"208">>, _}, _, <<"Transaction already processed.">>, _, _}} =
                ar_http:req(#{
                    method => post,
                    peer => {127, 0, 0, 1, MainPort},
                    path => "/tx",
                    headers => [{<<"x-p2p-port">>, integer_to_binary(MainPort, 10)}],
                    body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
                })
        end,
        MainPostForkTXs
    ).

re_admits_orphaned_tx_after_fork_recovery() ->
    %% recovers_from_forks_test_ asserts that orphaned TXs come back as 208
    %% on resubmission (i.e. they are back in the mempool / ignored set), but
    %% never mines a block to prove RecentTXMap and BlockAnchors were actually
    %% updated. This test pushes one step further: it orphans a block
    %% carrying a TX whose anchor is on the shared prefix (so the anchor
    %% survives recovery), then mines on the winning chain and asserts the
    %% TX makes it back in. That requires update_block_txs_pairs2 in
    %% ar_node_worker.erl to have dropped the orphaned block's pair, so
    %% verify_block_txs no longer rejects the resubmission as
    %% tx_already_in_weave.
    Key = {_, Pub} = ar_wallet:new(),
    [B0] = ar_weave:init([
        {ar_wallet:to_address(Pub), ?AR(20), <<>>}
    ]),
    _ = ar_test_node:start(B0),
    _ = ar_test_node:start_peer(peer1, B0),
    ar_test_node:connect_to_peer(peer1),
    %% Build a one-block shared prefix so both chains agree on the anchor.
    ar_test_node:mine(peer1),
    {ok, [{SharedBH, _, _} | _]} = ar_test_await:node_height(main, 1),
    ar_test_node:disconnect_from(peer1),
    %% On main, mine a block containing OrphanedTX. This block becomes the
    %% orphan when peer1's chain wins.
    OrphanedTX = ar_test_node:sign_tx(main, Key, #{
        last_tx => SharedBH,
        tags => [{<<"nonce">>, <<"orphaned">>}],
        reward => ?AR(1)
    }),
    ar_test_node:assert_post_tx_to_peer(main, OrphanedTX),
    ar_test_node:mine(),
    {ok, MainBI} = ar_test_await:node_height(main, 2),
    [{OrphanedBH, _, _} | _] = MainBI,
    assert_block_txs(main, [OrphanedTX], MainBI),
    %% Build a longer chain on peer1 so reconnect triggers fork recovery
    %% on main.
    ar_test_node:mine(peer1),
    ?assertMatch({ok, _}, ar_test_await:node_height(peer1, 2)),
    ar_test_node:mine(peer1),
    ?assertMatch({ok, _}, ar_test_await:node_height(peer1, 3)),
    ar_test_node:connect_to_peer(peer1),
    ?assertMatch({ok, _}, ar_test_await:node_height(main, 3)),
    %% A new TX anchored to the orphaned block must not be accepted after
    %% recovery. This covers the BlockAnchors side of the same cache rebuild.
    OrphanedBlockAnchorTX = ar_test_node:sign_tx(main, Key, #{
        last_tx => OrphanedBH,
        tags => [{<<"nonce">>, <<"orphaned_block_anchor">>}],
        reward => ?AR(1)
    }),
    {ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx).">>, _, _}} =
        ar_tx_test_utils:post_tx_to_peer_once(main, OrphanedBlockAnchorTX),
    %% After fork recovery, the orphaned TX should be back in main's
    %% mempool. Wait for it to be ready for mining, mine, and assert the
    %% TX lands in the new chain - if RecentTXMap still held the TX's id
    %% from the orphaned block, verify_block_txs would reject it as
    %% tx_already_in_weave and the block would mine empty.
    ?assertEqual(ok, ar_test_await:txs_ready_for_mining(main, [OrphanedTX])),
    ar_test_node:mine(),
    {ok, NewBI} = ar_test_await:node_height(main, 4),
    assert_block_txs(main, [OrphanedTX], NewBI).

anchor_depth_boundary_after_fork_recovery() ->
    %% rejects_txs_with_outdated_anchors_test_ (ar_tx_anchor_tests) pins the
    %% anchor-depth boundary on a linearly built chain. This test pins the same
    %% boundary after a fork recovery, where ar_node_worker:update_block_txs_pairs2/3
    %% rebuilds block_txs_pairs (and thus BlockAnchors) by splicing the winning
    %% fork onto the shared prefix instead of extending it block by block. An
    %% off-by-one in that rebuild would shift the anchor window only after a reorg
    %% and slip past the linear-path test. After main recovers onto peer1's longer
    %% chain, a TX anchoring the deepest still-valid block (get_max_tx_anchor_depth()
    %% from the tip) must be accepted, while a TX anchoring one block deeper must be
    %% rejected as tx_bad_anchor.
    Key = {_, Pub} = ar_wallet:new(),
    [B0] = ar_weave:init([
        {ar_wallet:to_address(Pub), ?AR(20), <<>>}
    ]),
    _ = ar_test_node:start(B0),
    _ = ar_test_node:start_peer(peer1, B0),
    ar_test_node:connect_to_peer(peer1),
    Depth = ar_block:get_max_tx_anchor_depth(),
    %% Build a shared chain tall enough to have a full anchor window: both nodes
    %% stay in sync up to height Depth.
    lists:foreach(
        fun(H) ->
            ar_test_node:mine(peer1),
            ?assertMatch({ok, _}, ar_test_await:node_height(peer1, H)),
            ?assertMatch({ok, _}, ar_test_await:node_height(main, H))
        end,
        lists:seq(1, Depth)
    ),
    ar_test_node:disconnect_from(peer1),
    %% main mines a block (height Depth + 1) that will be orphaned.
    ar_test_node:mine(),
    ?assertMatch({ok, _}, ar_test_await:node_height(main, Depth + 1)),
    %% peer1 mines two blocks so its chain wins; on reconnect main fork-recovers
    %% onto it, replacing its own height Depth + 1 block via update_block_txs_pairs2.
    ar_test_node:mine(peer1),
    ?assertMatch({ok, _}, ar_test_await:node_height(peer1, Depth + 1)),
    ar_test_node:mine(peer1),
    ?assertMatch({ok, _}, ar_test_await:node_height(peer1, Depth + 2)),
    ar_test_node:connect_to_peer(peer1),
    {ok, BI} = ar_test_await:node_height(main, Depth + 2),
    %% Pin both sides of the anchor-depth boundary on the recovered chain. With
    %% the tip at height Depth + 2, the window covers the top Depth blocks, so the
    %% Depth-th block from the tip (lists:nth(Depth, BI)) is the deepest valid
    %% anchor and the next one down is one block too deep.
    DeepestValidBH = element(1, lists:nth(Depth, BI)),
    OneTooDeepBH = element(1, lists:nth(Depth + 1, BI)),
    ValidTX = ar_test_node:sign_tx(main, Key, #{ last_tx => DeepestValidBH,
            reward => ?AR(1), tags => [{<<"nonce">>, <<"deepest_valid">>}] }),
    ?assertMatch({valid, _},
            ar_test_node:remote_call(main, ar_tx_validator, validate, [ValidTX])),
    ar_test_node:assert_post_tx_to_peer(main, ValidTX),
    TooDeepTX = ar_test_node:sign_tx(main, Key, #{ last_tx => OneTooDeepBH,
            reward => ?AR(1), tags => [{<<"nonce">>, <<"too_deep">>}] }),
    ?assertEqual({invalid, tx_bad_anchor},
            ar_test_node:remote_call(main, ar_tx_validator, validate, [TooDeepTX])),
    {ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx).">>, _, _}} =
        ar_test_node:post_tx_to_peer(main, TooDeepTX).

forget_txs(TXs) ->
    lists:foreach(
        fun(TX) ->
            ets:delete(ignored_ids, TX#tx.id)
        end,
        TXs
    ).

assert_block_txs(Node, TXs, BI) ->
    TXIDs = lists:map(fun(TX) -> TX#tx.id end, TXs),
    B = ar_test_node:remote_call(Node, ar_test_await, block_stored, [hd(BI)]),
    ?assertEqual(lists:sort(TXIDs), lists:sort(B#block.txs)).

random_nonce() ->
    integer_to_binary(rand:uniform(1000000)).
