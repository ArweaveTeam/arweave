-module(ar_tx_replay_pool_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").

-include_lib("eunit/include/eunit.hrl").

verify_block_txs_test_() ->
	{timeout, 30, fun test_verify_block_txs/0}.

test_verify_block_txs() ->
	Key1 = ar_wallet:new(),
	Key2 = ar_wallet:new(),
	RandomBlockAnchors =
		[crypto:strong_rand_bytes(32) || _ <- lists:seq(1, ?MAX_TX_ANCHOR_DEPTH)],
	BlockAnchorTXAtForkHeight = tx(Key1, fee(ar_fork:height_2_0()), <<"hash">>),
	BlockAnchorTXAfterForkHeight =
		tx(Key1, fee(ar_fork:height_2_0() + 1), <<"hash">>),
	Timestamp = os:system_time(second),
	TestCases = [
		#{
			title => "Fork height 2.0 accepts block anchors",
			txs => [tx(Key1, fee(ar_fork:height_2_0()), <<"hash">>)],
			height => ar_fork:height_2_0(),
			block_anchors => [<<"hash">>],
			recent_txs_map => #{},
			wallet_list => [wallet(Key1, fee(ar_fork:height_2_0()))],
			expected_result => valid
		},
		#{
			title => "After fork height 2.0 accepts block anchors",
			txs => [tx(Key1, fee(ar_fork:height_2_0() + 1), <<"hash">>)],
			height => ar_fork:height_2_0() + 1,
			block_anchors => [<<"hash">>],
			recent_txs_map => #{},
			wallet_list => [wallet(Key1, fee(ar_fork:height_2_0() + 1))],
			expected_result => valid
		},
		#{
			title => "Fork height 2.0 rejects outdated block anchors",
			txs => [
				tx(
					Key1,
					fee(ar_fork:height_2_0()),
					crypto:strong_rand_bytes(32)
				)
			],
			block_anchors => RandomBlockAnchors,
			recent_txs_map => #{},
			height => ar_fork:height_2_0(),
			wallet_list => [wallet(Key1, fee(ar_fork:height_2_0()))],
			expected_result => invalid
		},
		#{
			title => "Fork height 2.0 accepts wallet list anchors",
			txs => [
				tx(Key1, fee(ar_fork:height_2_0()), <<>>),
				tx(Key2, fee(ar_fork:height_2_0()), <<>>)
			],
			height => ar_fork:height_2_0(),
			wallet_list => [
				wallet(Key1, fee(ar_fork:height_2_0())),
				wallet(Key2, fee(ar_fork:height_2_0()))
			],
			block_anchors => [],
			recent_txs_map => #{},
			expected_result => valid
		},
		#{
			title => "After fork height 2.0 accepts wallet list anchors",
			txs => [
				tx(Key1, fee(ar_fork:height_2_0() + 1), <<>>),
				tx(Key2, fee(ar_fork:height_2_0() + 1), <<>>)
			],
			height => ar_fork:height_2_0() + 1,
			wallet_list => [
				wallet(Key1, fee(ar_fork:height_2_0() + 1)),
				wallet(Key2, fee(ar_fork:height_2_0() + 1))
			],
			block_anchors => [],
			recent_txs_map => #{},
			expected_result => valid
		},
		#{
			title => "Fork height 2.0 rejects conflicting wallet list anchors",
			txs => [
				tx(Key1, fee(ar_fork:height_2_0()), <<>>),
				tx(Key1, fee(ar_fork:height_2_0()), <<>>)
			],
			height => ar_fork:height_2_0(),
			block_anchors => [],
			recent_txs_map => #{},
			wallet_list => [wallet(Key1, 2 * fee(ar_fork:height_2_0()))],
			expected_result => invalid
		},
		#{
			title => "Fork height 2.0 rejects chained wallet list anchors",
			txs => make_tx_chain(Key1, ar_fork:height_2_0()),
			height => ar_fork:height_2_0(),
			block_anchors => [],
			recent_txs_map => #{},
			wallet_list => [wallet(Key1, 2 * fee(ar_fork:height_2_0()))],
			expected_result => invalid
		},
		#{
			title => "Fork height 2.0 rejects conflicting balances",
			txs => [
				tx(Key1, fee(ar_fork:height_2_0()), <<>>),
				tx(Key1, fee(ar_fork:height_2_0()), <<>>)
			],
			height => ar_fork:height_2_0(),
			wallet_list =>
				[wallet(Key1, erlang:trunc(1.5 * fee(ar_fork:height_2_0())))],
			block_anchors => [],
			recent_txs_map => #{},
			expected_result => invalid
		},
		#{
			title => "Fork height 2.0 rejects duplicates",
			txs => [BlockAnchorTXAtForkHeight, BlockAnchorTXAtForkHeight],
			height => ar_fork:height_2_0(),
			block_anchors => [],
			recent_txs_map => #{},
			wallet_list => [wallet(Key1, 2 * fee(ar_fork:height_2_0()))],
			expected_result => invalid
		},
		#{
			title => "After fork height 2.0 rejects duplicates",
			txs => [BlockAnchorTXAfterForkHeight, BlockAnchorTXAfterForkHeight],
			height => ar_fork:height_2_0() + 1,
			block_anchors => [],
			recent_txs_map => #{},
			wallet_list => [wallet(Key1, 2 * fee(ar_fork:height_2_0() + 1))],
			expected_result => invalid
		},
		#{
			title => "Fork height 2.0 rejects txs from the weave",
			txs => [BlockAnchorTXAtForkHeight],
			height => ar_fork:height_2_0(),
			block_anchors => [<<"hash">>, <<"otherhash">>],
			recent_txs_map => #{
				<<"txid">> => ok,
				<<"txid2">> => ok,
				BlockAnchorTXAtForkHeight#tx.id => ok
			},
			wallet_list => [wallet(Key1, fee(ar_fork:height_2_0()))],
			expected_result => invalid
		},
		#{
			title => "After fork height 2.0 rejects txs from the weave",
			txs => [BlockAnchorTXAfterForkHeight],
			height => ar_fork:height_2_0() + 1,
			block_anchors => [<<"hash">>, <<"otherhash">>],
			recent_txs_map => #{
				<<"txid">> => ok,
				<<"txid2">> => ok,
				BlockAnchorTXAfterForkHeight#tx.id => ok
			},
			wallet_list => [wallet(Key1, fee(ar_fork:height_2_0() + 1))],
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

format_1_fork_2_9_6_test_() ->
	ar_test_node:test_with_mocked_functions(
			[{ar_fork, height_2_9_6, fun() -> 5 end}],
			fun test_format_1_fork_2_9_6/0).

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
