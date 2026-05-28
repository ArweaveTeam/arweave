-module(ar_tx_replay_pool_tests).
-test_category([fast]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").

-include_lib("eunit/include/eunit.hrl").

verify_block_txs_test_() ->
	{timeout, 30, fun test_verify_block_txs/0}.

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

wallet({_, Pub}, Balance) ->
	{ar_wallet:to_address(Pub), Balance, <<>>}.

fee(Height) ->
	ar_tx:get_tx_fee({0, 2000, 1, <<>>, #{}, Height + 1}).
