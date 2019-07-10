-module(ar_tx_replay_pool_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

verify_block_txs_test() ->
	Key1 = ar_wallet:new(),
	Key2 = ar_wallet:new(),
	Hashes = generate_hashes(),
	EmptyBlockTXPairs = block_txs_pairs(Hashes),
	TX1 = tx(Key1, 5, <<>>),
	TX2 = tx(Key1, 5, TX1#tx.id),
	BlockAnchorTX = tx(Key1, 1, <<"hash">>),
	TestCases = [
		#{
			title => "Fork height 1.8 accepts block anchors",
			txs => [tx(Key1, 10, <<"hash">>)],
			diff => 1,
			height => ar_fork:height_1_8(),
			wallet_list => [wallet(Key1, 10)],
			block_txs_pairs => [{<<"hash">>, []}],
			expected_result => valid
		},
		#{
			title => "After fork height 1.8 accepts block anchors",
			txs => [tx(Key1, 10, <<"hash">>)],
			diff => 1,
			height => ar_fork:height_1_8() + 1,
			wallet_list => [wallet(Key1, 10)],
			block_txs_pairs => [{<<"hash">>, []}],
			expected_result => valid
		},
		#{
			title => "Before fork height 1.8 rejects block anchors",
			txs => [tx(Key1, 10, <<"hash">>)],
			diff => 1,
			height => ar_fork:height_1_8() - 1,
			wallet_list => [wallet(Key1, 10)],
			block_txs_pairs => [{<<"hash">>, []}],
			expected_result => invalid
		},
		#{
			title => "Fork height 1.8 rejects outdated block anchors",
			txs => [tx(Key1, 10, lists:nth(?MAX_TX_ANCHOR_DEPTH + 1, Hashes))],
			diff => 1,
			height => ar_fork:height_1_8(),
			wallet_list => [wallet(Key1, 10)],
			block_txs_pairs => EmptyBlockTXPairs,
			expected_result => invalid
		},
		#{
			title => "Fork height 1.8 accepts wallet list anchors",
			txs => [tx(Key1, 10, <<>>), tx(Key2, 10, <<>>)],
			diff => 1,
			height => ar_fork:height_1_8(),
			wallet_list => [wallet(Key1, 10), wallet(Key2, 10)],
			block_txs_pairs => [],
			expected_result => valid
		},
		#{
			title => "After fork height 1.8 accepts wallet list anchors",
			txs => [tx(Key1, 10, <<>>), tx(Key2, 10, <<>>)],
			diff => 1,
			height => ar_fork:height_1_8() + 1,
			wallet_list => [wallet(Key1, 10), wallet(Key2, 10)],
			block_txs_pairs => [],
			expected_result => valid
		},
		#{
			title => "Before fork height 1.8 rejects conflicting wallet list anchors",
			txs => [tx(Key1, 5, <<>>), tx(Key1, 5, <<>>)],
			diff => 1,
			height => ar_fork:height_1_8() - 1,
			wallet_list => [wallet(Key1, 10)],
			block_txs_pairs => [],
			expected_result => invalid
		},
		#{
			title => "Before fork height 1.8 accepts chained wallet list anchors",
			txs => [TX1, TX2],
			diff => 1,
			height => ar_fork:height_1_8() - 1,
			wallet_list => [wallet(Key1, 10)],
			block_txs_pairs => [],
			expected_result => valid
		},
		#{
			title => "Fork height 1.8 rejects conflicting wallet list anchors",
			txs => [tx(Key1, 5, <<>>), tx(Key1, 5, <<>>)],
			diff => 1,
			height => ar_fork:height_1_8(),
			wallet_list => [wallet(Key1, 10)],
			block_txs_pairs => [],
			expected_result => invalid
		},
		#{
			title => "Fork height 1.8 rejects chained wallet list anchors",
			txs => [TX1, TX2],
			diff => 1,
			height => ar_fork:height_1_8(),
			wallet_list => [wallet(Key1, 10)],
			block_txs_pairs => [],
			expected_result => invalid
		},
		#{
			title => "Before fork height 1.8 rejects conflicting balances",
			txs => [tx(Key1, 6, <<>>), tx(Key1, 6, <<>>)],
			diff => 1,
			height => ar_fork:height_1_8() - 1,
			wallet_list => [wallet(Key1, 10)],
			block_txs_pairs => [],
			expected_result => invalid
		},
		#{
			title => "Fork height 1.8 rejects conflicting balances",
			txs => [tx(Key1, 6, <<>>), tx(Key1, 6, <<>>)],
			diff => 1,
			height => ar_fork:height_1_8(),
			wallet_list => [wallet(Key1, 10)],
			block_txs_pairs => [],
			expected_result => invalid
		},
		#{
			title => "Fork height 1.8 rejects duplicates",
			txs => [BlockAnchorTX, BlockAnchorTX],
			diff => 1,
			height => ar_fork:height_1_8(),
			wallet_list => [wallet(Key1, 10)],
			block_txs_pairs => [],
			expected_result => invalid
		},
		#{
			title => "After fork height 1.8 rejects duplicates",
			txs => [BlockAnchorTX, BlockAnchorTX],
			diff => 1,
			height => ar_fork:height_1_8() + 1,
			wallet_list => [wallet(Key1, 10)],
			block_txs_pairs => [],
			expected_result => invalid
		},
		#{
			title => "Fork height 1.8 rejects txs from the weave",
			txs => [BlockAnchorTX],
			diff => 1,
			height => ar_fork:height_1_8(),
			wallet_list => [wallet(Key1, 10)],
			block_txs_pairs =>
				[{<<"hash">>, []}, {<<"otherhash">>, [<<"txid">>, <<"txid2">>, BlockAnchorTX#tx.id]}],
			expected_result => invalid
		},
		#{
			title => "After fork height 1.8 rejects txs from the weave",
			txs => [BlockAnchorTX],
			diff => 1,
			height => ar_fork:height_1_8(),
			wallet_list => [wallet(Key1, 10)],
			block_txs_pairs =>
				[{<<"hash">>, []}, {<<"otherhash">>, [<<"txid">>, <<"txid2">>, BlockAnchorTX#tx.id]}],
			expected_result => invalid
		}
	],
	lists:foreach(
		fun(#{
			title := Title,
			txs := TXs,
			diff := Diff,
			height := Height,
			wallet_list := WalletList,
			block_txs_pairs := BlockTXPairs,
			expected_result := ExpectedResult
		}) ->
			?assertEqual(
				ExpectedResult,
				ar_tx_replay_pool:verify_block_txs(TXs, Diff, Height, WalletList, BlockTXPairs),
				Title
			),
			PickedTXs = ar_tx_replay_pool:pick_txs_to_mine(BlockTXPairs, Height, Diff, WalletList, TXs),
			?assertEqual(
				valid,
				ar_tx_replay_pool:verify_block_txs(PickedTXs, Diff, Height, WalletList, BlockTXPairs),
				lists:flatten(io_lib:format("Verifyng after picking_txs_to_mine: ~s:", [Title]))
			)
		end,
		TestCases
	).

tx(Key = {_, Pub}, Reward, Anchor) ->
	ar_tx:sign(
		(ar_tx:new())#tx{
			owner = Pub,
			reward = ?AR(Reward),
			last_tx = Anchor
		},
		Key
	).

wallet({_, Pub}, Balance) ->
	{ar_wallet:to_address(Pub), ?AR(Balance), <<>>}.

generate_hashes() ->
	lists:map(
		fun(_) ->
			integer_to_binary(rand:uniform(1000))
		end,
		lists:seq(1, (?MAX_TX_ANCHOR_DEPTH) * 2)
	).

block_txs_pairs(Hashes) ->
	lists:map(
		fun(H) ->
			{H, []}
		end,
		Hashes
	).
