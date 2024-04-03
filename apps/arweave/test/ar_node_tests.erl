-module(ar_node_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [sign_v1_tx/3, read_block_when_stored/1]).

ar_node_interface_test_() ->
	{timeout, 300, fun test_ar_node_interface/0}.

test_ar_node_interface() ->
	[B0] = ar_weave:init(),
	ar_test_node:start(B0),
	?assertEqual(0, ar_node:get_height()),
	?assertEqual(B0#block.indep_hash, ar_node:get_current_block_hash()),
	ar_test_node:mine(),
	B0H = B0#block.indep_hash,
	[{H, _, _}, {B0H, _, _}] = ar_test_node:wait_until_height(1),
	?assertEqual(1, ar_node:get_height()),
	?assertEqual(H, ar_node:get_current_block_hash()).

mining_reward_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
		fun test_mining_reward/0, 120).

test_mining_reward() ->
	{_Priv1, Pub1} = ar_wallet:new_keyfile(),
	[B0] = ar_weave:init(),
	ar_test_node:start(B0, MiningAddr = ar_wallet:to_address(Pub1)),
	ar_test_node:mine(),
	ar_test_node:wait_until_height(1),
	B1 = ar_node:get_current_block(),
	[{MiningAddr, _, Reward, 1}, _] = B1#block.reward_history,
	{_, TotalLocked} = lists:foldl(
		fun(Height, {PrevB, TotalLocked}) ->
			?assertEqual(0, ar_node:get_balance(Pub1)),
			?assertEqual(TotalLocked, ar_rewards:get_total_reward_for_address(MiningAddr, PrevB)),
			ar_test_node:mine(),
			ar_test_node:wait_until_height(Height + 1),
			B = ar_node:get_current_block(),
			{B, TotalLocked + B#block.reward}
		end,
		{B1, Reward},
		lists:seq(1, ?REWARD_HISTORY_BLOCKS)
	),
	?assertEqual(Reward, ar_node:get_balance(Pub1)),

	%% Unlock one more reward.
	ar_test_node:mine(),
	ar_test_node:wait_until_height(?REWARD_HISTORY_BLOCKS + 2),
	FinalB = ar_node:get_current_block(),
	?assertEqual(Reward + 10, ar_node:get_balance(Pub1)),
	?assertEqual(
		TotalLocked - Reward - 10 + FinalB#block.reward,
		ar_rewards:get_total_reward_for_address(MiningAddr, FinalB)).

% @doc Check that other nodes accept a new block and associated mining reward.
multi_node_mining_reward_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
		fun test_multi_node_mining_reward/0, 120).

test_multi_node_mining_reward() ->
	{_Priv1, Pub1} = ar_test_node:remote_call(peer1, ar_wallet, new_keyfile, []),
	[B0] = ar_weave:init(),
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0, MiningAddr = ar_wallet:to_address(Pub1)),
	ar_test_node:connect_to_peer(peer1),
	ar_test_node:mine(peer1),
	ar_test_node:wait_until_height(1),
	B1 = ar_node:get_current_block(),
	[{MiningAddr, _, Reward, 1}, _] = B1#block.reward_history,
	?assertEqual(0, ar_node:get_balance(Pub1)),
	lists:foreach(
		fun(Height) ->
			?assertEqual(0, ar_node:get_balance(Pub1)),
			ar_test_node:mine(),
			ar_test_node:wait_until_height(Height + 1)
		end,
		lists:seq(1, ?REWARD_HISTORY_BLOCKS)
	),
	?assertEqual(Reward, ar_node:get_balance(Pub1)).

%% @doc Ensure that TX replay attack mitigation works.
replay_attack_test_() ->
	{timeout, 120, fun() ->
		Key1 = {_Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		ar_test_node:start(B0),
		ar_test_node:start_peer(peer1, B0),
		ar_test_node:connect_to_peer(peer1),
		SignedTX = sign_v1_tx(main, Key1, #{ target => ar_wallet:to_address(Pub2),
				quantity => ?AR(1000), reward => ?AR(1), last_tx => <<>> }),
		ar_test_node:assert_post_tx_to_peer(main, SignedTX),
		ar_test_node:mine(),
		ar_test_node:assert_wait_until_height(peer1, 1),
		?assertEqual(?AR(8999), ar_test_node:remote_call(peer1, ar_node, get_balance, [Pub1])),
		?assertEqual(?AR(1000), ar_test_node:remote_call(peer1, ar_node, get_balance, [Pub2])),
		ar_events:send(tx, {ready_for_mining, SignedTX}),
		ar_test_node:wait_until_receives_txs([SignedTX]),
		ar_test_node:mine(),
		ar_test_node:assert_wait_until_height(peer1, 2),
		?assertEqual(?AR(8999), ar_test_node:remote_call(peer1, ar_node, get_balance, [Pub1])),
		?assertEqual(?AR(1000), ar_test_node:remote_call(peer1, ar_node, get_balance, [Pub2]))
	end}.

%% @doc Create two new wallets and a blockweave with a wallet balance.
%% Create and verify execution of a signed exchange of value tx.
wallet_transaction_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
		fun test_wallet_transaction/0, 120).

test_wallet_transaction() ->
	TestWalletTransaction = fun(KeyType) ->
		fun() ->
			{Priv1, Pub1} = ar_wallet:new_keyfile(KeyType),
			{_Priv2, Pub2} = ar_wallet:new(),
			TX = ar_tx:new(ar_wallet:to_address(Pub2), ?AR(1), ?AR(9000), <<>>),
			SignedTX = ar_tx:sign(TX#tx{ format = 2 }, Priv1, Pub1),
			[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
			ar_test_node:start(B0, ar_wallet:to_address(ar_wallet:new_keyfile({eddsa, ed25519}))),
			ar_test_node:start_peer(peer1, B0),
			ar_test_node:connect_to_peer(peer1),
			ar_test_node:assert_post_tx_to_peer(main, SignedTX),
			ar_test_node:mine(),
			ar_test_node:wait_until_height(1),
			ar_test_node:assert_wait_until_height(peer1, 1),
			?assertEqual(?AR(999), ar_test_node:remote_call(peer1, ar_node, get_balance, [Pub1])),
			?assertEqual(?AR(9000), ar_test_node:remote_call(peer1, ar_node, get_balance, [Pub2]))
		end
	end,
	[
		{"PS256_65537", timeout, 60, TestWalletTransaction({?RSA_SIGN_ALG, 65537})},
		{"ES256K", timeout, 60, TestWalletTransaction({?ECDSA_SIGN_ALG, secp256k1})},
		{"Ed25519", timeout, 60, TestWalletTransaction({?EDDSA_SIGN_ALG, ed25519})}
	].

%% @doc Ensure that TX Id threading functions correctly (in the positive case).
tx_threading_test_() ->
	{timeout, 120, fun() ->
		Key1 = {_Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		ar_test_node:start(B0),
		ar_test_node:start_peer(peer1, B0),
		ar_test_node:connect_to_peer(peer1),
		SignedTX = sign_v1_tx(main, Key1, #{ target => ar_wallet:to_address(Pub2),
				quantity => ?AR(1000), reward => ?AR(1), last_tx => <<>> }),
		SignedTX2 = sign_v1_tx(main, Key1, #{ target => ar_wallet:to_address(Pub2),
				quantity => ?AR(1000), reward => ?AR(1), last_tx => SignedTX#tx.id }),
		ar_test_node:assert_post_tx_to_peer(main, SignedTX),
		ar_test_node:mine(),
		ar_test_node:wait_until_height(1),
		ar_test_node:assert_post_tx_to_peer(main, SignedTX2),
		ar_test_node:mine(),
		ar_test_node:assert_wait_until_height(peer1, 2),
		?assertEqual(?AR(7998), ar_test_node:remote_call(peer1, ar_node, get_balance, [Pub1])),
		?assertEqual(?AR(2000), ar_test_node:remote_call(peer1, ar_node, get_balance, [Pub2]))
	end}.

persisted_mempool_test_() ->
	%% Make the propagation delay noticeable so that the submitted transactions do not
	%% become ready for mining before the node is restarted and we assert that waiting
	%% transactions found in the persisted mempool are (re-)submitted to peers.
	ar_test_node:test_with_mocked_functions([{ar_node_worker, calculate_delay,
			fun(_Size) -> 5000 end}], fun test_persisted_mempool/0).

test_persisted_mempool() ->
	{_, Pub} = Wallet = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:disconnect_from(peer1),
	SignedTX = ar_test_node:sign_tx(Wallet, #{ last_tx => ar_test_node:get_tx_anchor(main) }),
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = ar_test_node:post_tx_to_peer(main, SignedTX, false),
	Mempool = ar_mempool:get_map(),
	true = ar_util:do_until(
		fun() ->
			maps:is_key(SignedTX#tx.id, Mempool)
		end,
		100,
		10000
	),
	Config = ar_test_node:stop(),
	%% Rejoin the network.
	%% Expect the pending transactions to be picked up and distributed.
	ok = application:set_env(arweave, config, Config#config{
		start_from_latest_state = false,
		peers = [ar_test_node:peer_ip(peer1)]
	}),
	ar:start_dependencies(),
	ar_test_node:wait_until_joined(),
	ar_test_node:connect_to_peer(peer1),
	ar_test_node:assert_wait_until_receives_txs(peer1, [SignedTX]),
	ar_test_node:mine(),
	[{H, _, _} | _] = ar_test_node:assert_wait_until_height(peer1, 1),
	B = read_block_when_stored(H),
	?assertEqual([SignedTX#tx.id], B#block.txs),
	ok = application:set_env(arweave, config, Config).
