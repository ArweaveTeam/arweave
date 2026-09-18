-module(ar_fork_recovery_tests).
-test_category([vdf]).
-test_peers([peer1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

height_plus_one_fork_recovery_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun test_height_plus_one_fork_recovery/0}.

test_height_plus_one_fork_recovery() ->
    %% Mine on two nodes until they fork. Mine an extra block on one of them.
    %% Expect the other one to recover.
    {_, Pub} = ar_wallet:new(),
    [B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
    ar_test_node:start(B0),
    ar_test_node:start_peer(peer1, B0),
    ar_test_node:disconnect_from(peer1),
    ar_test_node:mine(peer1),
    ?assertMatch({ok, _}, ar_test_await:node_height(peer1, 1)),
    ar_test_node:mine(),
    ?assertMatch({ok, _}, ar_test_await:node_height(main, 1)),
    ar_test_node:mine(),
    {ok, MainBI} = ar_test_await:node_height(main, 2),
    ar_test_node:connect_to_peer(peer1),
    ?assertEqual({ok, MainBI}, ar_test_await:node_height(peer1, 2)),
    ar_test_node:disconnect_from(peer1),
    ar_test_node:mine(),
    ?assertMatch({ok, _}, ar_test_await:node_height(main, 3)),
    ar_test_node:mine(peer1),
    ?assertMatch({ok, _}, ar_test_await:node_height(peer1, 3)),
    ar_test_node:rejoin_on(#{ node => main, join_on => peer1 }),
    ar_test_node:mine(peer1),
    {ok, PeerBI} = ar_test_await:node_height(peer1, 4),
    ?assertEqual({ok, PeerBI}, ar_test_await:node_height(main, 4)).

height_plus_three_fork_recovery_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun test_height_plus_three_fork_recovery/0}.

test_height_plus_three_fork_recovery() ->
    %% Mine on two nodes until they fork. Mine three extra blocks on one of them.
    %% Expect the other one to recover.
    {_, Pub} = ar_wallet:new(),
    [B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
    ar_test_node:start(B0),
    ar_test_node:start_peer(peer1, B0),
    ar_test_node:disconnect_from(peer1),
    ar_test_node:mine(peer1),
    ?assertMatch({ok, _}, ar_test_await:node_height(peer1, 1)),
    ar_test_node:mine(),
    ?assertMatch({ok, _}, ar_test_await:node_height(main, 1)),
    ar_test_node:mine(),
    ?assertMatch({ok, _}, ar_test_await:node_height(main, 2)),
    ar_test_node:mine(peer1),
    ?assertMatch({ok, _}, ar_test_await:node_height(peer1, 2)),
    ar_test_node:mine(),
    ?assertMatch({ok, _}, ar_test_await:node_height(main, 3)),
    ar_test_node:mine(peer1),
    ?assertMatch({ok, _}, ar_test_await:node_height(peer1, 3)),
    ar_test_node:connect_to_peer(peer1),
    ar_test_node:mine(),
    {ok, MainBI} = ar_test_await:node_height(main, 4),
    ?assertEqual({ok, MainBI}, ar_test_await:node_height(peer1, 4)).

missing_txs_fork_recovery_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun test_missing_txs_fork_recovery/0}.

test_missing_txs_fork_recovery() ->
    %% Mine a block with a transaction on the peer1 node
    %% but do not gossip the transaction. The main node
    %% is expected fetch the missing transaction and apply the block.
    Key = {_, Pub} = ar_wallet:new(),
    [B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
    ar_test_node:start(B0),
    ar_test_node:start_peer(peer1, B0),
    ar_test_node:disconnect_from(peer1),
    TX1 = ar_test_node:sign_tx(Key, #{}),
    ar_test_node:assert_post_tx_to_peer(peer1, TX1),
    %% Wait to make sure the tx will not be gossiped upon reconnect.
    timer:sleep(2000), % == 2 * ?CHECK_MEMPOOL_FREQUENCY
    ar_test_node:rejoin_on(#{ node => main, join_on => peer1 }),
    ?assertEqual([], ar_mempool:get_all_txids()),
    ar_test_node:mine(peer1),
    {ok, [{H1, _, _} | _]} = ar_test_await:node_height(main, 1),
    ?assertEqual(1, length((ar_test_await:block_stored(H1))#block.txs)).

orphaned_txs_are_remined_after_fork_recovery_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun test_orphaned_txs_are_remined_after_fork_recovery/0}.

test_orphaned_txs_are_remined_after_fork_recovery() ->
    %% Mine a transaction on peer1, mine two blocks on main to
    %% make the transaction orphaned. Mine a block on peer1 and
    %% assert the transaction is re-mined.
    Key = {_, Pub} = ar_wallet:new(),
    [B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
    ar_test_node:start(B0),
    ar_test_node:start_peer(peer1, B0),
    ar_test_node:disconnect_from(peer1),
    TX = #tx{ id = TXID } = ar_test_node:sign_tx(Key, #{ denomination => 1, reward => ?AR(1) }),
    ar_test_node:assert_post_tx_to_peer(peer1, TX),
    ar_test_node:mine(peer1),
    {ok, [{H1, _, _} | _]} = ar_test_await:node_height(peer1, 1),
    H1TXIDs = (ar_test_node:remote_call(peer1, ar_test_await, block_stored, [H1]))#block.txs,
    ?assertEqual([TXID], H1TXIDs),
    ar_test_node:mine(),
    {ok, [{H2, _, _} | _]} = ar_test_await:node_height(main, 1),
    ar_test_node:mine(),
    {ok, [{H3, _, _}, {H2, _, _}, {_, _, _}]} = ar_test_await:node_height(main, 2),
    ar_test_node:connect_to_peer(peer1),
    ?assertMatch({ok, [{H3, _, _}, {H2, _, _}, {_, _, _}]}, ar_test_await:node_height(peer1, 2)),
    ar_test_node:mine(peer1),
    {ok, [{H4, _, _} | _]} = ar_test_await:node_height(peer1, 3),
    H4TXIDs = (ar_test_node:remote_call(peer1, ar_test_await, block_stored, [H4]))#block.txs,
    ?debugFmt("Expecting ~s to be re-mined.~n", [arweave_util:encode(TXID)]),
    ?assertEqual([TXID], H4TXIDs).

orphaned_high_value_tx_is_remined_after_fork_recovery_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT,
     fun test_orphaned_high_value_tx_is_remined_after_fork_recovery/0}.

test_orphaned_high_value_tx_is_remined_after_fork_recovery() ->
    %% Like the test above, but the orphaned transaction spends more than half
    %% of the sender's balance. The mempool overspend check must evaluate the
    %% returned transaction against the balances at the new tip, not the
    %% orphaned one, where the transaction is already applied.
    Key = {_, Pub} = ar_wallet:new(),
    {_, TargetPub} = ar_wallet:new(),
    %% Fund the target so that the fee does not include the new account fee.
    [B0] = ar_weave:init([
        {ar_wallet:to_address(Pub), ?AR(20), <<>>},
        {ar_wallet:to_address(TargetPub), ?AR(1), <<>>}
    ]),
    ar_test_node:start(B0),
    ar_test_node:start_peer(peer1, B0),
    ar_test_node:disconnect_from(peer1),
    TX = #tx{ id = TXID } = ar_test_node:sign_tx(Key, #{
        denomination => 1,
        reward => ?AR(1),
        target => ar_wallet:to_address(TargetPub),
        quantity => ?AR(15)
    }),
    ar_test_node:assert_post_tx_to_peer(peer1, TX),
    ar_test_node:mine(peer1),
    {ok, [{H1, _, _} | _]} = ar_test_await:node_height(peer1, 1),
    H1B = ar_test_node:remote_call(peer1, ar_test_await, block_stored, [H1]),
    ?assertEqual([TXID], H1B#block.txs),
    ar_test_node:mine(),
    {ok, [{H2, _, _} | _]} = ar_test_await:node_height(main, 1),
    ar_test_node:mine(),
    {ok, [{H3, _, _}, {H2, _, _}, {_, _, _}]} =
        ar_test_await:node_height(main, 2),
    ar_test_node:connect_to_peer(peer1),
    ?assertMatch({ok, [{H3, _, _}, {H2, _, _}, {_, _, _}]},
        ar_test_await:node_height(peer1, 2)),
    ?assertEqual([TXID],
        ar_test_node:remote_call(peer1, ar_mempool, get_all_txids, [])),
    ar_test_node:mine(peer1),
    {ok, [{H4, _, _} | _]} = ar_test_await:node_height(peer1, 3),
    H4B = ar_test_node:remote_call(peer1, ar_test_await, block_stored, [H4]),
    ?assertEqual([TXID], H4B#block.txs).

orphaned_tx_is_restored_across_redenomination_test_() ->
    ar_test_node:test_with_all_nodes_mocked([
        {ar_pricing, may_be_redenominate, fun forced_redenomination/1}
    ], fun test_orphaned_tx_is_restored_across_redenomination/0).

test_orphaned_tx_is_restored_across_redenomination() ->
    %% Mine a transaction on peer1 and a second block on top, both at
    %% denomination 1. Let main alone redenominate at height 2 and pay the
    %% sender at height 3, so main's tip stores the sender's account at
    %% denomination 2. Let peer1 switch to main's fork. Restoring the orphan
    %% converts the sender's balance at the new tip to the node's current
    %% denomination, which must already be the new tip's: converting down
    %% has no clause and would kill the node worker.

    %% Fund the sender and a second wallet that pays the sender later.
    SenderKey = {_, SenderPub} = ar_wallet:new(),
    FundingKey = {_, FundingPub} = ar_wallet:new(),
    Sender = ar_wallet:to_address(SenderPub),
    [B0] = ar_weave:init([
        {Sender, ?AR(20), <<>>},
        {ar_wallet:to_address(FundingPub), ?AR(20), <<>>}
    ]),
    ar_test_node:start(B0),
    ar_test_node:start_peer(peer1, B0),
    ar_test_node:disconnect_from(peer1),
    %% peer1: mine the sender's transaction at height 1 and an empty block
    %% at height 2, both at denomination 1.
    OrphanedTX = #tx{ id = OrphanedTXID } = ar_test_node:sign_tx(peer1,
        SenderKey, #{ denomination => 1, reward => ?AR(1) }),
    ar_test_node:assert_post_tx_to_peer(peer1, OrphanedTX),
    ar_test_node:mine(peer1),
    {ok, [{PeerH1, _, _} | _]} = ar_test_await:node_height(peer1, 1),
    PeerB1 = ar_test_node:remote_call(peer1, ar_test_await, block_stored,
        [PeerH1]),
    ?assertEqual(1, PeerB1#block.denomination),
    ?assertEqual([OrphanedTXID], PeerB1#block.txs),
    ar_test_node:mine(peer1),
    {ok, [{PeerH2, _, _} | _]} = ar_test_await:node_height(peer1, 2),
    PeerB2 = ar_test_node:remote_call(peer1, ar_test_await, block_stored,
        [PeerH2]),
    ?assertEqual(1, PeerB2#block.denomination),
    %% main: mine an empty block at height 1, then the mock redenominates
    %% and the empty block at height 2 is at denomination 2.
    ar_test_node:mine(),
    ?assertMatch({ok, _}, ar_test_await:node_height(main, 1)),
    ar_test_node:mine(),
    {ok, [{MainH2, _, _} | _]} = ar_test_await:node_height(main, 2),
    ?assertEqual(2, (ar_test_await:block_stored(MainH2))#block.denomination),
    %% main: pay the sender at height 3. Accounts are stored in the
    %% denomination of the previous block, so the payment has to sit below
    %% a denomination 2 block for the account to be stored at denomination 2.
    FundingTX = #tx{ id = FundingTXID } = ar_test_node:sign_tx(main,
        FundingKey, #{ denomination => 1, reward => ?AR(1), target => Sender,
            quantity => ?AR(1) }),
    ar_test_node:assert_post_tx_to_peer(main, FundingTX),
    ar_test_node:mine(),
    {ok, [{MainH3, _, _} | _]} = ar_test_await:node_height(main, 3),
    ?assertEqual([FundingTXID],
        (ar_test_await:block_stored(MainH3))#block.txs),
    ?assertMatch(#{ Sender := {_, _, 2, _} }, ar_account_tree:get(Sender)),
    %% Reconnect. peer1 switches to main's heavier fork, which orphans its
    %% two blocks. The node worker must survive the switch, the tip must be
    %% at denomination 2, and the orphaned transaction must be back in the
    %% mempool.
    Worker = ar_test_node:remote_call(peer1, erlang, whereis,
        [ar_node_worker]),
    ar_test_node:connect_to_peer(peer1),
    ?assertMatch({ok, [{MainH3, _, _} | _]},
        ar_test_await:node_height(peer1, 3)),
    ?assertEqual(Worker,
        ar_test_node:remote_call(peer1, erlang, whereis, [ar_node_worker])),
    PeerB3 = ar_test_node:remote_call(peer1, ar_node, get_current_block, []),
    ?assertEqual(2, PeerB3#block.denomination),
    ?assertEqual([OrphanedTXID],
        ar_test_node:remote_call(peer1, ar_mempool, get_all_txids, [])),
    %% The restored transaction is mined on the new fork.
    ar_test_node:mine(peer1),
    {ok, [{PeerH4, _, _} | _]} = ar_test_await:node_height(peer1, 4),
    PeerB4 = ar_test_node:remote_call(peer1, ar_test_await, block_stored,
        [PeerH4]),
    ?assertEqual([OrphanedTXID], PeerB4#block.txs).

%% @doc Redenominate at height 2 on main's fork only: main's block 1 is
%% empty, peer1's block 1 holds the orphaned transaction.
forced_redenomination(#block{ height = 1, txs = [] }) ->
    {2, 1};
forced_redenomination(#block{ denomination = Denomination,
        redenomination_height = RedenominationHeight }) ->
    {Denomination, RedenominationHeight}.

orphaned_tx_survives_sibling_mined_in_both_forks_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT,
     fun test_orphaned_tx_survives_sibling_mined_in_both_forks/0}.

test_orphaned_tx_survives_sibling_mined_in_both_forks() ->
    %% Mine TX1 and TX2 from the same wallet on peer1 and TX1 alone on main.
    %% Make main's fork longer and let peer1 switch to it. TX1 is in the new
    %% fork so it must not return to the mempool. Were it returned, it would
    %% count towards the wallet's spent total on top of the new tip's balance,
    %% which already reflects it, and TX2, the lower fee one, would be dropped
    %% as overspending instead of being re-mined.
    Key = {_, Pub} = ar_wallet:new(),
    {_, TargetPub} = ar_wallet:new(),
    Target = ar_wallet:to_address(TargetPub),
    [B0] = ar_weave:init([
        {ar_wallet:to_address(Pub), ?AR(20), <<>>},
        {Target, ?AR(1), <<>>}
    ]),
    ar_test_node:start(B0),
    ar_test_node:start_peer(peer1, B0),
    ar_test_node:disconnect_from(peer1),
    TX1 = #tx{ id = TXID1 } = ar_test_node:sign_tx(Key, #{
        denomination => 1,
        reward => ?AR(2),
        target => Target,
        quantity => ?AR(7)
    }),
    TX2 = #tx{ id = TXID2 } = ar_test_node:sign_tx(Key, #{
        denomination => 1,
        reward => ?AR(1),
        target => Target,
        quantity => ?AR(8)
    }),
    ar_test_node:assert_post_tx_to_peer(main, TX1),
    ar_test_node:assert_post_tx_to_peer(peer1, TX1),
    ar_test_node:assert_post_tx_to_peer(peer1, TX2),
    ar_test_node:mine(peer1),
    {ok, [{H1, _, _} | _]} = ar_test_await:node_height(peer1, 1),
    H1B = ar_test_node:remote_call(peer1, ar_test_await, block_stored, [H1]),
    ?assertEqual(lists:sort([TXID1, TXID2]), lists:sort(H1B#block.txs)),
    ar_test_node:mine(),
    {ok, [{H2, _, _} | _]} = ar_test_await:node_height(main, 1),
    ?assertEqual([TXID1], (ar_test_await:block_stored(H2))#block.txs),
    ar_test_node:mine(),
    {ok, [{H3, _, _}, {H2, _, _}, {_, _, _}]} =
        ar_test_await:node_height(main, 2),
    ar_test_node:connect_to_peer(peer1),
    ?assertMatch({ok, [{H3, _, _}, {H2, _, _}, {_, _, _}]},
        ar_test_await:node_height(peer1, 2)),
    ?assertEqual([TXID2],
        ar_test_node:remote_call(peer1, ar_mempool, get_all_txids, [])),
    ar_test_node:mine(peer1),
    {ok, [{H4, _, _} | _]} = ar_test_await:node_height(peer1, 3),
    H4B = ar_test_node:remote_call(peer1, ar_test_await, block_stored, [H4]),
    ?assertEqual([TXID2], H4B#block.txs).

orphaned_tx_survives_pending_tx_mined_in_new_fork_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT,
     fun test_orphaned_tx_survives_pending_tx_mined_in_new_fork/0}.

test_orphaned_tx_survives_pending_tx_mined_in_new_fork() ->
    %% Mine TX1 on peer1. Then post TX2, from the same wallet and with a higher
    %% fee, to both nodes and mine it on main, below the block that makes
    %% main's fork longer. At the switch TX2 is still pending on peer1, and
    %% it must leave the mempool before TX1 returns. Otherwise it would count
    %% towards the wallet's spent total on top of the new tip's balance, which
    %% already reflects it, and TX1, the lower fee one, would be dropped as
    %% overspending instead of being re-mined.
    Key = {_, Pub} = ar_wallet:new(),
    {_, TargetPub} = ar_wallet:new(),
    Target = ar_wallet:to_address(TargetPub),
    [B0] = ar_weave:init([
        {ar_wallet:to_address(Pub), ?AR(20), <<>>},
        {Target, ?AR(1), <<>>}
    ]),
    ar_test_node:start(B0),
    ar_test_node:start_peer(peer1, B0),
    ar_test_node:disconnect_from(peer1),
    TX1 = #tx{ id = TXID1 } = ar_test_node:sign_tx(Key, #{
        denomination => 1,
        reward => ?AR(1),
        target => Target,
        quantity => ?AR(8)
    }),
    ar_test_node:assert_post_tx_to_peer(peer1, TX1),
    ar_test_node:mine(peer1),
    {ok, [{H1, _, _} | _]} = ar_test_await:node_height(peer1, 1),
    H1B = ar_test_node:remote_call(peer1, ar_test_await, block_stored, [H1]),
    ?assertEqual([TXID1], H1B#block.txs),
    TX2 = #tx{ id = TXID2 } = ar_test_node:sign_tx(Key, #{
        denomination => 1,
        reward => ?AR(2),
        target => Target,
        quantity => ?AR(7)
    }),
    ar_test_node:assert_post_tx_to_peer(main, TX2),
    ar_test_node:assert_post_tx_to_peer(peer1, TX2),
    ar_test_node:mine(),
    {ok, [{H2, _, _} | _]} = ar_test_await:node_height(main, 1),
    ?assertEqual([TXID2], (ar_test_await:block_stored(H2))#block.txs),
    ar_test_node:mine(),
    {ok, [{H3, _, _}, {H2, _, _}, {_, _, _}]} =
        ar_test_await:node_height(main, 2),
    ar_test_node:connect_to_peer(peer1),
    ?assertMatch({ok, [{H3, _, _}, {H2, _, _}, {_, _, _}]},
        ar_test_await:node_height(peer1, 2)),
    ?assertEqual([TXID1],
        ar_test_node:remote_call(peer1, ar_mempool, get_all_txids, [])),
    ar_test_node:mine(peer1),
    {ok, [{H4, _, _} | _]} = ar_test_await:node_height(peer1, 3),
    H4B = ar_test_node:remote_call(peer1, ar_test_await, block_stored, [H4]),
    ?assertEqual([TXID1], H4B#block.txs).

invalid_block_with_high_cumulative_difficulty_test_() ->
    ar_test_node:test_with_all_nodes_mocked([{arweave_constants, height_2_6, fun() -> 0 end}],
        fun() -> test_invalid_block_with_high_cumulative_difficulty() end).

test_invalid_block_with_high_cumulative_difficulty() ->
    %% Submit an alternative fork with valid blocks weaker than the tip and
    %% an invalid block on top, much stronger than the tip. Make sure the node
    %% ignores the invalid block and continues to build on top of the valid fork.
    RewardKey = ar_wallet:new_keyfile(),
    RewardAddr = ar_wallet:to_address(RewardKey),
    WalletName = arweave_util:encode(RewardAddr),
    Path = ar_wallet:wallet_filepath(WalletName),
    PeerPath = ar_test_node:remote_call(peer1, ar_wallet, wallet_filepath, [WalletName]),
    %% Copy the key because we mine blocks on both nodes using the same key in this test.
    {ok, _} = file:copy(Path, PeerPath),
    [B0] = ar_weave:init(),
    ar_test_node:start(B0, RewardAddr),
    ar_test_node:start_peer(peer1, B0, RewardAddr),
    ar_test_node:disconnect_from(peer1),
    ar_test_node:mine(peer1),
    {ok, [{H1, _, _} | _]} = ar_test_await:node_height(peer1, 1),
    ar_test_node:mine(),
    {ok, [{H2, _, _} | _]} = ar_test_await:node_height(main, 1),
    ar_test_node:connect_to_peer(peer1),
    ?assertNotEqual(H2, H1),
    B1 = ar_test_await:block_stored(H2),
    B2 = fake_block_with_strong_cumulative_difficulty(B1, B0, 10000000000000000),
    B2H = B2#block.indep_hash,
    ?debugFmt("Fake block: ~s.", [arweave_util:encode(B2H)]),
    ok = ar_events:subscribe(block),
    ?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
            ar_http_iface_client:send_block_binary(ar_test_node:peer_ip(main), B2#block.indep_hash,
                    ar_serialize:block_to_binary(B2))),
    %% The block event stream is global, so unrelated peer gossip can arrive
    %% before the fake block's verdict; wait specifically for that verdict.
    wait_until_fake_block_rejected(B2H, erlang:monotonic_time(millisecond) + 60_000),
    {ok, [{H1, _, _} | _]} = ar_test_await:node_height(peer1, 1),
    ar_test_node:mine(),
    %% Assert the nodes have continued building on the original fork.
    {ok, [{H3, _, _} | _]} = ar_test_await:node_height(peer1, 2),
    ?assertNotEqual(B2#block.indep_hash, H3),
    {_Peer, B3, _Time, _Size} =
    ar_http_iface_client:get_block_shadow(1,
        ar_test_node:peer_ip(peer1),
        binary, #{}),
    ?assertEqual(H2, B3#block.indep_hash).

wait_until_fake_block_rejected(B2H, Deadline) ->
    Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {event, block, {rejected, invalid_cumulative_difficulty, B2H, _Peer2}} ->
            ok;
        {event, block, {rejected, _Reason, B2H, _Peer2}} ->
            ?assert(false, "Unexpected fake block rejection");
        {event, block, {new, #block{ indep_hash = B2H }, _Peer3}} ->
            ?assert(false, "Unexpected block acceptance");
        {event, block, _Other} ->
            wait_until_fake_block_rejected(B2H, Deadline)
    after Timeout ->
        ?assert(false, "Timed out waiting for the node to pre-validate the fake "
                "block.")
    end.

fake_block_with_strong_cumulative_difficulty(B, PrevB, CDiff) ->
    #block{
        height = Height,
        partition_number = PartitionNumber,
        previous_solution_hash = PrevSolutionH,
        nonce_limiter_info = #nonce_limiter_info{
                partition_upper_bound = PartitionUpperBound },
        diff = Diff
    } = B,
    B2 = B#block{ cumulative_diff = CDiff },
    Wallet = ar_wallet:new(),
    RewardAddr2 = ar_wallet:to_address(Wallet),
    H0 = ar_block:compute_h0(B, PrevB),
    {RecallByte, _RecallRange2Start} = ar_block:get_recall_range(H0, PartitionNumber,
            PartitionUpperBound),
    {ok, #{ data_path := DataPath, tx_path := TXPath,
            chunk := Chunk } } = ar_data_sync:get_chunk(RecallByte + 1,
                    #{ pack => true, packing => {spora_2_6, RewardAddr2},
                    origin => test }),
    {H1, Preimage} = ar_block:compute_h1(H0, 0, Chunk),
    case binary:decode_unsigned(H1) > Diff of
        true ->
            PoA = #poa{ chunk = Chunk, data_path = DataPath, tx_path = TXPath },
            B3 = B2#block{ hash = H1, hash_preimage = Preimage, reward_addr = RewardAddr2,
                    reward_key = element(2, Wallet), recall_byte = RecallByte, nonce = 0,
                    recall_byte2 = undefined, poa2 = #poa{},
                    unpacked_chunk2_hash = undefined,
                    poa = #poa{ chunk = Chunk, data_path = DataPath,
                            tx_path = TXPath },
                    chunk_hash = crypto:hash(sha256, Chunk) },
            B4 =
                case arweave_constants:height_2_8() of
                    0 ->
                        {ok, #{ chunk := UnpackedChunk } } = ar_data_sync:get_chunk(
                                RecallByte + 1, #{ pack => true, packing => unpacked,
                                origin => test }),
                        B3#block{ packing_difficulty = 1,
                                poa = PoA#poa{ unpacked_chunk = UnpackedChunk },
                                unpacked_chunk_hash = crypto:hash(sha256, UnpackedChunk) };
                    _ ->
                        B3
                end,
            PrevCDiff = PrevB#block.cumulative_diff,
            SignedH = ar_block:generate_signed_hash(B4),
            SignaturePreimage = ar_block:get_block_signature_preimage(CDiff, PrevCDiff,
                << PrevSolutionH/binary, SignedH/binary >>, Height),
            Signature = ar_wallet:sign(element(1, Wallet), SignaturePreimage),
            B4#block{ indep_hash = ar_block:indep_hash2(SignedH, Signature),
                    signature = Signature };
        false ->
            fake_block_with_strong_cumulative_difficulty(B, PrevB, CDiff)
    end.
