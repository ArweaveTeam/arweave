-module(ar_poller_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [
	start/1, slave_start/1,
	disconnect_from_slave/0,
	slave_call/3,
	get_tx_anchor/0,
	sign_tx/2,
	assert_post_tx_to_slave/1,
	slave_mine/0,
	slave_wait_until_height/1, wait_until_height/1,
	read_block_when_stored/1
]).

polling_test_() ->
	{timeout, 30, fun test_polling/0}.

test_polling() ->
	{_, Pub} = Wallet = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),
	start(B0),
	slave_start(B0),
	disconnect_from_slave(),
	{ok, Config} = application:get_env(arweave, config),
	Peer = {127, 0, 0, 1, slave_call(ar_meta_db, get, [port])},
	Config2 = Config#config{ peers = [Peer] },
	application:set_env(arweave, config, Config2),
	ar_bridge:add_remote_peer(Peer),
	TXs =
		lists:map(
			fun(Height) ->
				SignedTX = sign_tx(Wallet, #{ last_tx => get_tx_anchor() }),
				assert_post_tx_to_slave(SignedTX),
				slave_mine(),
				slave_wait_until_height(Height),
				SignedTX
			end,
			lists:seq(1, 10)
		),
	wait_until_height(10),
	lists:foreach(
		fun(Height) ->
			{H, _, _} = ar_node:get_block_index_entry(Height),
			B = read_block_when_stored(H),
			TX = lists:nth(Height, TXs),
			?assertEqual([TX#tx.id], B#block.txs)
		end,
		lists:seq(1, 10)
	).
