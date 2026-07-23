-module(ar_header_sync_tests).
-test_peers([peer1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [sign_v1_tx/3,
    random_v1_data/1
]).

syncs_headers_test_() ->
    ar_test_node:test_with_all_nodes_mocked([
            {ar_fork, height_2_8, fun() -> 0 end},
            {ar_fork, height_2_9, fun() -> 0 end},
            {ar_retarget, is_retarget_height, fun(_Height) -> false end},
            {ar_retarget, is_retarget_block, fun(_Block) -> false end}],
            fun test_syncs_headers/0).

test_syncs_headers() ->
    Wallet = {_, Pub} = ar_wallet:new(),
    [B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(2000), <<>>}]),
    MainAddr = ar_test_node:generate_address(main),
    ar_test_node:start(#{
        b0 => B0,
        addr => MainAddr,
        config => wide_replica_config(MainAddr)
    }),
    post_random_blocks(Wallet, ar_block:get_max_tx_anchor_depth() + 5, B0),
    PeerAddr = ar_test_node:generate_address(peer1),
    ar_test_node:join_on(#{
        node => peer1,
        join_on => main,
        addr => PeerAddr,
        config => wide_replica_config(PeerAddr)
    }),
    {ok, BI} = ar_test_await:node_height(peer1, ar_block:get_max_tx_anchor_depth() + 5),
    lists:foreach(
        fun(Height) ->
            ok = ar_test_await:until(peer1_block_available,
                fun() ->
                    ar_test_node:remote_call(peer1, ar_storage, read_block, [Height, BI])
                            /= unavailable
                end,
                30000
            ),
            B = ar_test_node:remote_call(peer1, ar_storage, read_block, [Height, BI]),
            MainB = ar_storage:read_block(Height, ar_node:get_block_index()),
            ?assertEqual(B, MainB),
            TXs = ar_test_node:remote_call(peer1, ar_storage, read_tx, [B#block.txs]),
            MainTXs = ar_storage:read_tx(B#block.txs),
            ?assertEqual(TXs, MainTXs)
        end,
        lists:reverse(lists:seq(0, ar_block:get_max_tx_anchor_depth() + 5))
    ),
    %% Throw the event to simulate running out of disk space.
    ar_disksup:pause(),
    ar_events:send(disksup, {remaining_disk_space, ?DEFAULT_MODULE, true, 0, 0}),
    ok = ar_test_await:until(header_sync_paused_for_disk_space,
        fun() ->
            ar_header_sync:is_disk_space_sufficient() =:= false
        end,
        5000
    ),
    try
        NoSpaceHeight = ar_block:get_max_tx_anchor_depth() + 6,
        NoSpaceTX = sign_v1_tx(main, Wallet,
            #{ data => random_v1_data(10 * 1024), last_tx => ar_test_node:get_tx_anchor(peer1) }),
        ar_test_node:assert_post_tx_to_peer(main, NoSpaceTX),
        ar_test_node:mine(),
        {ok, [{NoSpaceH, _, _} | _]} = ar_test_await:node_height(main, NoSpaceHeight),
        %% The cleanup is not expected to kick in yet.
        NoSpaceB = ar_test_await:block_stored(NoSpaceH),
        ?assertMatch(#block{}, NoSpaceB),
        ?assertMatch(#tx{}, ar_storage:read_tx(NoSpaceTX#tx.id)),
        ?assertMatch({ok, _}, ar_storage:read_wallet_list(NoSpaceB#block.wallet_list)),
        ets:new(test_syncs_header, [set, named_table]),
        ets:insert(test_syncs_header, {height, NoSpaceHeight + 1}),
        %% Keep mining blocks. At some point the cleanup procedure will
        %% kick in and remove the oldest files.
        ok = ar_test_await:until(header_sync_cleanup_kicked_in,
            fun() ->
                _ = mine_next_block(Wallet),
                unavailable == ar_storage:read_block(NoSpaceH)
                    andalso ar_storage:read_tx(NoSpaceTX#tx.id) == unavailable
            end,
            20000
        ),
        %% The latest block must not be cleaned up. The tiny test header
        %% cache can evict a just-written tip header during a cleanup pass,
        %% so keep mining and check the live tip rather than pinning one
        %% hash that may have been evicted (and never rewritten once idle).
        ok = ar_test_await:until(latest_block_retained,
            fun() -> latest_block_fully_stored(mine_next_block(Wallet)) end,
            20000
        )
    after
        ar_disksup:resume(),
        ar_events:send(disksup, {remaining_disk_space, ?DEFAULT_MODULE, true, 100, 20_000_000_000})
    end.

%% @doc Mine one block carrying a fresh 200 KiB v1 tx, advance the
%% `test_syncs_header' height counter, and return the new tip's hash.
mine_next_block(Wallet) ->
    TX = sign_v1_tx(main, Wallet, #{
        data => random_v1_data(200 * 1024),
        last_tx => ar_test_node:get_tx_anchor(peer1)
    }),
    ar_test_node:assert_post_tx_to_peer(main, TX),
    ar_test_node:mine(),
    [{_, Height}] = ets:lookup(test_syncs_header, height),
    {ok, [{LatestH, _, _} | _]} = ar_test_await:node_height(main, Height),
    ets:insert(test_syncs_header, {height, Height + 1}),
    LatestH.

%% @doc True when block `H' and its first tx and wallet list are all
%% readable (i.e. the block was not evicted from the disk cache).
latest_block_fully_stored(H) ->
    case ar_storage:read_block(H) of
        #block{ txs = [TXID | _] } = B ->
            ar_storage:read_tx(TXID) /= unavailable
                andalso case ar_storage:read_wallet_list(B#block.wallet_list) of
                    {ok, _} -> true;
                    _ -> false
                end;
        _ ->
            false
    end.

post_random_blocks(Wallet, TargetHeight, B0) ->
    lists:foldl(
        fun(Height, Anchor) ->
            ?LOG_INFO([{event, post_random_blocks}, {height, Height}]),
            TXs =
                lists:foldl(
                    fun(_, Acc) ->
                        case rand:uniform(2) == 1 of
                            true ->
                                TX = ar_test_node:sign_tx(main, Wallet,
                                    #{
                                        last_tx => Anchor,
                                        data => crypto:strong_rand_bytes(10 * ?MiB)
                                    }),
                                ar_test_node:assert_post_tx_to_peer(main, TX),
                                [TX | Acc];
                            false ->
                                Acc
                        end
                    end,
                    [],
                    lists:seq(1, 2)
                ),
            ?LOG_INFO([{event, post_random_blocks}, {transactions_posted, length(TXs)}, {height, Height}]),
            ar_test_node:mine(),
            {ok, [{H, _, _} | _]} = ar_test_await:node_height(main, Height),
            ?LOG_INFO([{event, post_random_blocks}, {block_mined, ar_util:encode(H)}, {height, Height}]),
            ?assertEqual(length(TXs), length((ar_test_await:block_stored(H))#block.txs)),
            H
        end,
        B0#block.indep_hash,
        lists:seq(1, TargetHeight)
    ).

wide_replica_config(Addr) ->
    ar_test_node:storage_module_config(
        Addr, lists:seq(0, 8), #{ packing => replica_2_9 }).
