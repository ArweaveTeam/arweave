-module(ar_tx_replay_pool_tests).
-test_category([fast]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").

-include_lib("eunit/include/eunit.hrl").

verify_block_txs_test_() ->
    {setup, fun ar_tx_db:setup_ets/0, fun(Cleanup) -> Cleanup() end,
        {timeout, 30, fun test_verify_block_txs/0}}.

test_verify_block_txs() ->
    Key1 = ar_wallet:new(),
    Key2 = ar_wallet:new(),
    CurrentHeight = 0,
    RandomBlockAnchors =
        [crypto:strong_rand_bytes(32) || _ <- lists:seq(1, ar_block:get_max_tx_anchor_depth())],
    BlockAnchorTX = tx(Key1, fee(CurrentHeight), <<"hash">>),
    Timestamp = os:system_time(second),
    TestCases = [
        #{
            title => "Accepts block anchors",
            txs => [tx(Key1, fee(CurrentHeight), <<"hash">>)],
            height => CurrentHeight,
            block_anchors => [<<"hash">>],
            recent_txs_map => #{},
            wallet_list => [wallet(Key1, fee(CurrentHeight))],
            expected_result => valid
        },
        #{
            title => "Rejects outdated block anchors",
            txs => [
                tx(
                    Key1,
                    fee(CurrentHeight),
                    crypto:strong_rand_bytes(32)
                )
            ],
            block_anchors => RandomBlockAnchors,
            recent_txs_map => #{},
            height => CurrentHeight,
            wallet_list => [wallet(Key1, fee(CurrentHeight))],
            expected_result => invalid
        },
        #{
            title => "Accepts wallet list anchors",
            txs => [
                tx(Key1, fee(CurrentHeight), <<>>),
                tx(Key2, fee(CurrentHeight), <<>>)
            ],
            height => CurrentHeight,
            wallet_list => [
                wallet(Key1, fee(CurrentHeight)),
                wallet(Key2, fee(CurrentHeight))
            ],
            block_anchors => [],
            recent_txs_map => #{},
            expected_result => valid
        },
        #{
            title => "Rejects conflicting wallet list anchors",
            txs => [
                tx(Key1, fee(CurrentHeight), <<>>),
                tx(Key1, fee(CurrentHeight), <<>>)
            ],
            height => CurrentHeight,
            block_anchors => [],
            recent_txs_map => #{},
            wallet_list => [wallet(Key1, 2 * fee(CurrentHeight))],
            expected_result => invalid
        },
        #{
            title => "Rejects chained wallet list anchors",
            txs => make_tx_chain(Key1, CurrentHeight),
            height => CurrentHeight,
            block_anchors => [],
            recent_txs_map => #{},
            wallet_list => [wallet(Key1, 2 * fee(CurrentHeight))],
            expected_result => invalid
        },
        #{
            title => "Rejects conflicting balances",
            txs => [
                tx(Key1, fee(CurrentHeight), <<>>),
                tx(Key1, fee(CurrentHeight), <<>>)
            ],
            height => CurrentHeight,
            wallet_list =>
                [wallet(Key1, erlang:trunc(1.5 * fee(CurrentHeight)))],
            block_anchors => [],
            recent_txs_map => #{},
            expected_result => invalid
        },
        #{
            title => "Rejects duplicates",
            txs => [BlockAnchorTX, BlockAnchorTX],
            height => CurrentHeight,
            block_anchors => [],
            recent_txs_map => #{},
            wallet_list => [wallet(Key1, 2 * fee(CurrentHeight))],
            expected_result => invalid
        },
        %% The duplicate case above sets block_anchors=[], so both copies fail
        %% at tx_bad_anchor. The case below makes the first copy genuinely
        %% valid (matching block anchor) so the second copy is the one that
        %% trips verify_tx_in_mempool → tx_already_in_mempool.
        #{
            title => "Rejects duplicate already in mempool",
            txs => [BlockAnchorTX, BlockAnchorTX],
            height => CurrentHeight,
            block_anchors => [<<"hash">>],
            recent_txs_map => #{},
            wallet_list => [wallet(Key1, 2 * fee(CurrentHeight))],
            expected_result => invalid
        },
        #{
            title => "Rejects txs from the weave",
            txs => [BlockAnchorTX],
            height => CurrentHeight,
            block_anchors => [<<"hash">>, <<"otherhash">>],
            recent_txs_map => #{
                <<"txid">> => ok,
                <<"txid2">> => ok,
                BlockAnchorTX#tx.id => ok
            },
            wallet_list => [wallet(Key1, fee(CurrentHeight))],
            expected_result => invalid
        }
    ],
    lists:foreach(
        fun(#{
            title := Title,
            txs := TXs,
            height := Height,
            wallet_list := WL,
            block_anchors := BlockAnchors,
            recent_txs_map := RecentTXMap,
            expected_result := ExpectedResult
        }) ->
            Rate = {1, 4},
            PricePerGiBMinute = 2000,
            KryderPlusRateMultiplier = 1,
            Denomination = 1,
            RedenominationHeight = 0,
            Wallets = maps:from_list([{A, {B, LTX}} || {A, B, LTX} <- WL]),
            ?debugFmt("~s:~n", [Title]),
            ?assertEqual(
                ExpectedResult,
                ar_tx_replay_pool:verify_block_txs({TXs, Rate, PricePerGiBMinute,
                        KryderPlusRateMultiplier, Denomination, Height, RedenominationHeight,
                        Timestamp, Wallets, BlockAnchors, RecentTXMap}),
                Title),
            PickedTXs = ar_tx_replay_pool:pick_txs_to_mine({BlockAnchors, RecentTXMap, Height,
                    RedenominationHeight, Rate, PricePerGiBMinute, KryderPlusRateMultiplier,
                    Denomination, Timestamp, Wallets, TXs}),
            ?assertEqual(
                valid,
                ar_tx_replay_pool:verify_block_txs({PickedTXs, Rate, PricePerGiBMinute,
                        KryderPlusRateMultiplier, Denomination, Height, RedenominationHeight,
                        Timestamp, Wallets, BlockAnchors, RecentTXMap}),
                lists:flatten(
                    io_lib:format("Verifying after picking_txs_to_mine: ~s:", [Title])
                )
            )
        end,
        TestCases
    ).

verify_tx_reasons_test_() ->
    {setup, fun ar_tx_db:setup_ets/0, fun(Cleanup) -> Cleanup() end,
        {timeout, 30, fun test_verify_tx_reasons/0}}.

%% verify_block_txs/1 collapses every rejection to the bare atom `invalid`, so
%% the cases in verify_block_txs_test_ can only tell apart the replay-protection
%% failures by their setup, not by what the code reports. verify_tx/2 keeps the
%% {invalid, Reason} shape, so we pin each distinct reason here. A regression
%% that, say, returned tx_bad_anchor where tx_already_in_weave is expected would
%% still leave verify_block_txs/1 returning `invalid` and slip by otherwise.
test_verify_tx_reasons() ->
    Key = ar_wallet:new(),
    Height = 0,
    Wallets = wallets([wallet(Key, fee(Height))]),
    TX = tx(Key, fee(Height), <<"hash">>),
    Verify = fun(VerifiedTX, BlockAnchors, RecentTXMap, Mempool) ->
        ar_tx_replay_pool:verify_tx({VerifiedTX, {1, 4}, 2000, 1, 1, Height, 0,
                BlockAnchors, RecentTXMap, Mempool, Wallets}, verify_signature)
    end,
    %% The anchor is not among the recent block anchors.
    ?assertEqual({invalid, tx_bad_anchor},
            Verify(TX, [], #{}, #{})),
    %% The anchor is valid, but the id is already on the weave.
    ?assertEqual({invalid, tx_already_in_weave},
            Verify(TX, [<<"hash">>], #{ TX#tx.id => ok }, #{})),
    %% The anchor is valid and the id is not on the weave, but the same id is
    %% already in the mempool.
    ?assertEqual({invalid, tx_already_in_mempool},
            Verify(TX, [<<"hash">>], #{}, #{ TX#tx.id => no_tx })),
    %% The anchor references a transaction that is itself still in the mempool
    %% (a last_tx chain). Only checked at and after fork 1.8, which is height 0
    %% under FORKS_RESET in the test profile.
    MempoolAnchorTX = tx(Key, fee(Height), <<"mempool_anchor">>),
    ?assertEqual({invalid, last_tx_in_mempool},
            Verify(MempoolAnchorTX, [<<"hash">>], #{}, #{ <<"mempool_anchor">> => no_tx })),
    %% The anchor is valid and the id is neither on the weave nor in the mempool.
    ?assertEqual(valid,
            Verify(TX, [<<"hash">>], #{}, #{})).

format_1_fork_2_9_6_test_() ->
    {setup, fun ar_tx_db:setup_ets/0, fun(Cleanup) -> Cleanup() end,
        ar_test_util:with_mocked([{ar_fork, height_2_9_6, fun() -> 5 end}],
                fun test_format_1_fork_2_9_6/0)}.

%% verify_tx/2, verify_block_txs/1, and pick_txs_to_mine/1 receive the
%% previous block's height, so with the activation height mocked to 5 the
%% highest accepted height is 3: the block built on top of it, at height 4,
%% is the last one that may carry format-1 transactions.
test_format_1_fork_2_9_6() ->
    Key = ar_wallet:new(),
    PrevHeightBeforeFork = 3,
    PrevHeightAtFork = 4,
    Timestamp = os:system_time(second),
    Reward = max(fee(PrevHeightBeforeFork), fee(PrevHeightAtFork)),
    TX = v1_tx(Key, Reward, <<"hash">>),
    Wallets = wallets([wallet(Key, Reward)]),
    VerifyTX = fun(Height) ->
        ar_tx_replay_pool:verify_tx({TX, {1, 4}, 2000, 1, 1, Height, 0,
                [<<"hash">>], #{}, #{}, Wallets}, verify_signature)
    end,
    VerifyBlockTXs = fun(Height) ->
        ar_tx_replay_pool:verify_block_txs({[TX], {1, 4}, 2000, 1, 1, Height,
                0, Timestamp, Wallets, [<<"hash">>], #{}})
    end,
    PickTXs = fun(Height) ->
        ar_tx_replay_pool:pick_txs_to_mine({[<<"hash">>], #{}, Height, 0,
                {1, 4}, 2000, 1, 1, Timestamp, Wallets, [TX]})
    end,
    ?assertEqual(valid, VerifyTX(PrevHeightBeforeFork)),
    ?assertEqual(valid, VerifyBlockTXs(PrevHeightBeforeFork)),
    ?assertEqual([TX], PickTXs(PrevHeightBeforeFork)),
    ?assertEqual({invalid, tx_verification_failed}, VerifyTX(PrevHeightAtFork)),
    ?assertEqual({ok, ["tx_format_1_not_supported"]},
            ar_tx_db:get_error_codes(TX#tx.id)),
    ?assertEqual(invalid, VerifyBlockTXs(PrevHeightAtFork)),
    ?assertEqual([], PickTXs(PrevHeightAtFork)).

make_tx_chain(Key, Height) ->
    TX1 = tx(Key, fee(Height), <<>>),
    TX2 = tx(Key, fee(Height), TX1#tx.id),
    [TX1, TX2].

tx(Key = {_, {_, Owner}}, Reward, Anchor) ->
    ar_tx:sign(
        #tx{
            format = 2,
            owner = Owner,
            reward = Reward,
            last_tx = Anchor
        },
        Key
    ).

v1_tx(Key = {_, {_, Owner}}, Reward, Anchor) ->
    ar_tx:sign_v1(
        #tx{
            format = 1,
            owner = Owner,
            reward = Reward,
            last_tx = Anchor,
            %% An explicit denomination keeps the transaction out of the
            %% deprecated denomination-0 class and short-circuits the
            %% malleability check, which would otherwise constrain the fee.
            denomination = 1
        },
        Key
    ).

wallet({_, Pub}, Balance) ->
    {ar_wallet:to_address(Pub), Balance, <<>>}.

wallets(WL) ->
    maps:from_list([{Addr, {Balance, LastTX}} || {Addr, Balance, LastTX} <- WL]).

fee(Height) ->
    ar_tx:get_tx_fee({0, 2000, 1, <<>>, #{}, Height + 1}).
