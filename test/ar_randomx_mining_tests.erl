-module(ar_randomx_mining_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_fork, [test_on_fork/3]).
-import(ar_test_node, [slave_start/1]).
-import(ar_test_node, [slave_call/3, connect_to_slave/0, slave_mine/1]).
-import(ar_test_node, [wait_until_height/2]).

two_nodes_successfully_fork_from_sha384_to_randomx_test_() ->
	%% Set retarget height so that RandomX difficuly is small enough after fork.
	ForkHeight = ?RETARGET_BLOCKS,
	{
		timeout,
		240,
		test_on_fork(
			height_1_7,
			ForkHeight,
			fun() -> two_nodes_successfully_fork_from_sha384_to_randomx(ForkHeight) end
		)
	}.

two_nodes_successfully_fork_from_sha384_to_randomx(ForkHeight) ->
	%% Start a remote node.
	{Slave, B0} = slave_start(no_block),
	%% Start a local node and connect to slave.
	{Master, _} = ar_test_node:start(B0, {127, 0, 0, 1, slave_call(ar_meta_db, get, [port])}),
	connect_to_slave(),
	ar_randomx_state:reset(),
	slave_call(ar_randomx_state, reset, []),
	%% Mine on slave and accept on master until they go past the fork height and
	%% switches to the next RandomX key plus a couple of more blocks.
	?assertEqual(ok, mine(Slave, Master, 0, ForkHeight + ?RANDOMX_KEY_SWAP_FREQ + 2)).

mine(_, _, StopHeight, StopHeight) ->
	ok;
mine(Slave, Master, CurrentHeight, StopHeight) ->
	slave_mine(Slave),
	wait_until_height(Master, CurrentHeight + 1),
	mine(Slave, Master, CurrentHeight + 1, StopHeight).
