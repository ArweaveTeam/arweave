-module(ar_vdf_block_validation_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar_config.hrl").

-define(TEST_RESET_FREQUENCY, 400).

fork_at_entropy_reset_point_test_() ->
	[
		ar_test_node:test_with_mocked_functions([mock_reset_frequency(), mock_block_propagation_parallelization()],
			fun test_fork_checkpoints_not_found/0, ?TEST_NODE_TIMEOUT),
		ar_test_node:test_with_mocked_functions([mock_reset_frequency(), mock_block_propagation_parallelization()],
			fun test_fork_refuse_validation/0, ?TEST_NODE_TIMEOUT)
	].

%% Scenario:
%% 1. VDF server applies a block that opens a new VDF session
%% 2. VDF client mines a solution at that same height
%%    (i.e. it mines a fork before receiving the other block)
%% 3. That solution fails because it is mined off VDF steps from the
%%    server which are in the new session, but the block being mined
%%    is an entropy reset block. 
%% 
%% The failure in this case (`step_checkpoints_not_found' error) is
%% unavoidable in this specific scenario. So this test will just assert
%% that the block is rejected and that the VDF client can later get on
%% the correct chain and then mine a solution there.
test_fork_checkpoints_not_found() ->
	[B0] = ar_weave:init([], 0), %% Set difficulty to 0 to speed up tests

	%% Start nodes in such way that they will not gossip blocks to
	%% each other. This lets us control when blocks are shared.
	%% Note: also relies on `mock_block_propagation_parallelization()`.
	{ok, Config} = arweave_config:get_env(),
	ar_test_node:start(#{
		b0 => B0,
		config => Config#config{
			nonce_limiter_client_peers = [
				ar_util:format_peer(ar_test_node:peer_ip(peer1))
			],
			block_pollers = 0
		}
	}),

	{ok, PeerConfig} = ar_test_node:get_config(peer1),
	ar_test_node:start_peer(peer1, #{ 
		b0 => B0,
		config => PeerConfig#config{
			nonce_limiter_server_trusted_peers = [
				ar_util:format_peer(ar_test_node:peer_ip(main))
			],
			block_pollers = 0
		}
	}),

	%% Still need to connect to make sure VDF is shared
	ar_test_node:connect_to_peer(peer1),

	ar_test_node:mine(main),
	[H1 | _] = ar_test_node:wait_until_height(main, 1),
	send_block(H1, main, peer1),
	ar_test_node:wait_until_height(peer1, 1),

	ar_test_node:disconnect_from(peer1),
	%% Make sure that we are deep into the new session before we try to mine
	wait_until_step_number(main, ?TEST_RESET_FREQUENCY + 101),
	ar_test_node:mine(main),
	[H2 | _] = ar_test_node:wait_until_height(main, 2),

	ar_test_node:connect_to_peer(peer1),
	ar_test_node:mine(peer1),
	%% Assert that peer1 is unable to mine a block
	timer:sleep(10000),
	BI = ar_test_node:remote_call(peer1, ar_node, get_blocks, []),
	?assertEqual(2, length(BI)), %% block 0 and 1

	%% Get peer1 on the main chain
	send_block(H2, main, peer1),
	ar_test_node:wait_until_height(peer1, 2),

	%% Now that we're on the main chain and still mining, we should eventually mine a block.
	ar_test_node:mine(peer1),
	ar_test_node:wait_until_height(peer1, 3).

%% Scenario:
%% 1. There's a chain fork on a block that opens a new VDF session.
%%    The "winning" block has a higher VDF step than the "losing" block. 
%%    Both blocks need to be validated using the current VDF session
%%    (not the new one)
%% 2. VDF server applies the "winning" block, validates with the current
%%    VDF session, and opens a new VDF session.
%% 3. VDF client applies the "losing" block, is able to get the VDF steps
%%    it needs to validate because the steps are before the new session that
%%    was opened on the VDF server so they still belong to the "old" session
%%    (or perhaps it just validates the "losing" block before the VDF server
%%    opens a new session)
%% 4. Later the VDF client tries to apply the winning block. However when it
%%    queries the steps it needs to validate the block, the VDF server which is
%%    now on the new session returns the steps for that new session - which won't
%%    validate.
%% 5. VDF client is stuck trying to validate the winning block and can't proceed.
%% 
%% We built in a fix for this scenario before 2.9.5-alpha1, but it relied on
%% VDF Pull being enabled (in which case the VDF client would explicitly ask
%% the server for the full current and previous sessions). In 2.9.5-alpha1 we
%% broke this fix for nodes using `disable vdf_server_pull`. We've now
%% re-applied the fix and added this test.
test_fork_refuse_validation() ->
	[B0] = ar_weave:init([], 0), %% Set difficulty to 0 to speed up tests

	%% Start nodes in such way that they will not gossip blocks to
	%% each other. This lets us control when blocks are shared.
	%% Note: also relies on `mock_block_propagation_parallelization()`.
	{ok, Config} = arweave_config:get_env(),
	ar_test_node:start(#{
		b0 => B0,
		config => Config#config{
			nonce_limiter_client_peers = [
				ar_util:format_peer(ar_test_node:peer_ip(peer1))
			],
			block_pollers = 0
		}
	}),

	{ok, PeerConfig} = ar_test_node:get_config(peer1),
	ar_test_node:start_peer(peer1, #{ 
		b0 => B0,
		config => PeerConfig#config{
			nonce_limiter_server_trusted_peers = [
				ar_util:format_peer(ar_test_node:peer_ip(main))
			],
			block_pollers = 0,
			disable = [vdf_server_pull | PeerConfig#config.disable]
		}
	}),

	%% Still need to connect to make sure VDF is shared
	ar_test_node:connect_to_peer(peer1),

	ar_test_node:mine(main),
	[H1 | _] = ar_test_node:wait_until_height(main, 1),
	send_block(H1, main, peer1),
	ar_test_node:wait_until_height(peer1, 1),
	wait_until_step_number(peer1, ?TEST_RESET_FREQUENCY + 1),

	ar_test_node:mine(peer1),
	ar_test_node:wait_until_height(peer1, 2),
	ar_test_node:disconnect_from(peer1),
	wait_until_step_number(main, ?TEST_RESET_FREQUENCY + 100),

	ar_test_node:mine(main),
	[H2 | _] = ar_test_node:wait_until_height(main, 2),
	ar_test_node:mine(main),
	[H3 | _] = ar_test_node:wait_until_height(main, 3),
	%% Just avoids some errors if the test finishes before the mining server is paused.
	ar_test_node:wait_until_mining_paused(main),

	ar_test_node:connect_to_peer(peer1),
	send_block(H2, main, peer1),
	send_block(H3, main, peer1),
	ar_test_node:wait_until_height(peer1, 3).

mock_reset_frequency() ->
	{
		ar_nonce_limiter, get_reset_frequency,
		fun() ->
			?TEST_RESET_FREQUENCY
		end
	}.

mock_block_propagation_parallelization() ->
	{
		ar_bridge, block_propagation_parallelization,
		fun() ->
			0
		end
	}.

send_block(H, FromNode, ToNode) ->
	Block = ar_test_node:remote_call(FromNode, ar_storage, read_block, [H]),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, ar_test_node:send_new_block(ar_test_node:peer_ip(ToNode), Block)).

wait_until_step_number(Node, StepNumber) ->
	true = ar_util:do_until(
		fun() -> 
			CurrentStepNumber = ar_test_node:remote_call(Node, ar_nonce_limiter, get_current_step_number, []),
			CurrentStepNumber >= StepNumber
		end,
		500, 
	60000).
