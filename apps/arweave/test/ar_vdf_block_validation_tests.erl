-module(ar_vdf_block_validation_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-define(TEST_RESET_FREQUENCY, 400).
-define(BLOCK_DELIVERY_TIMEOUT, 120000).

fork_at_entropy_reset_point_test_() ->
	[
		{timeout, ?TEST_NODE_TIMEOUT, fun test_fork_checkpoints_not_found/0},
		{timeout, ?TEST_NODE_TIMEOUT, fun test_fork_refuse_validation/0}
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
	mock_reset_frequency_and_block_propagation_parallelization(),
	try
		[B0] = ar_weave:init(),

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
		mock_reset_frequency_and_block_propagation_parallelization(main),

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
		mock_reset_frequency_and_block_propagation_parallelization(peer1),

		H2 = ar_test_node:with_gossip_paused(main, fun() ->
			%% Still need to connect to make sure VDF is shared
			ar_test_node:connect_to_peer(peer1),

			ar_test_node:mine(main),
			[H1 | _] = ar_test_node:wait_until_height(main, 1),
			send_block(H1, main, peer1),
			ar_test_node:wait_until_height(peer1, 1),

			ar_test_node:disconnect_from(peer1),
			%% Make sure that we are deep into the new session before we try to mine.
			%% Suspend peer1's nonce limiter so it cannot advance to the new session while isolated.
			[H2Local | _] = with_nonce_limiter_paused(peer1, fun() ->
				wait_until_step_number(main, ?TEST_RESET_FREQUENCY + 101),
				ar_test_node:mine(main),
				ar_test_node:wait_until_height(main, 2)
			end),

			ar_test_node:connect_to_peer(peer1),
			%% Wait until peer1 has transitioned to the new VDF session.
			wait_until_step_number(peer1, ?TEST_RESET_FREQUENCY + 1),
			with_vdf_pull_and_push_disabled(peer1, fun() ->
				ar_test_node:mine(peer1),
				%% Assert that peer1 is unable to mine a block.
				timer:sleep(10000),
				BI = ar_test_node:remote_call(peer1, ar_node, get_blocks, []),
				?assertEqual(2, length(BI))
			end),
			H2Local
		end),

		%% Get peer1 on the main chain
		send_block(H2, main, peer1),
		ar_test_node:wait_until_height(peer1, 2),

		%% Now that we're on the main chain and still mining, we should eventually mine a block.
		ar_test_node:mine(peer1),
		ar_test_node:wait_until_height(peer1, 3)
	after
		disable_mocks(main),
		disable_mocks(peer1),
		disable_mocks()
	end.

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
	mock_reset_frequency_and_block_propagation_parallelization(),
	try
		[B0] = ar_weave:init(),

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
		mock_reset_frequency_and_block_propagation_parallelization(main),

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
		mock_reset_frequency_and_block_propagation_parallelization(peer1),

		ar_test_node:with_gossip_paused(main, fun() ->
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
			ensure_block_applied(H2, main, peer1, 2),
			ensure_block_applied(H3, main, peer1, 3)
		end),
		ar_test_node:wait_until_height(peer1, 3)
	after
		disable_mocks(main),
		disable_mocks(peer1),
		disable_mocks()
	end.

mock_reset_frequency_and_block_propagation_parallelization() ->
	ar_test_node:new_mock(ar_nonce_limiter, [passthrough]),
	ar_test_node:new_mock(ar_bridge, [passthrough]),
	ar_test_node:mock_function(ar_nonce_limiter, get_reset_frequency, fun() -> ?TEST_RESET_FREQUENCY end),
	ar_test_node:mock_function(ar_bridge, block_propagation_parallelization, fun() -> 0 end).

disable_mocks() ->
	ar_test_node:unmock_module(ar_bridge),
	ar_test_node:unmock_module(ar_nonce_limiter).

mock_reset_frequency_and_block_propagation_parallelization(Node) ->
	ar_test_node:remote_call(Node, ar_test_node, new_mock, [ar_nonce_limiter, [passthrough]]),
	ar_test_node:remote_call(Node, ar_test_node, new_mock, [ar_bridge, [passthrough]]),
	ar_test_node:remote_call(Node, ar_test_node, mock_function, [ar_nonce_limiter, get_reset_frequency, fun() -> ?TEST_RESET_FREQUENCY end]),
	ar_test_node:remote_call(Node, ar_test_node, mock_function, [ar_bridge, block_propagation_parallelization, fun() -> 0 end]).

disable_mocks(Node) ->
	ok = ar_test_node:remote_call(Node, ar_test_node, unmock_module, [ar_bridge]),
	ok = ar_test_node:remote_call(Node, ar_test_node, unmock_module, [ar_nonce_limiter]).

send_block(H, FromNode, ToNode) ->
	Block = ar_test_node:remote_call(FromNode, ar_storage, read_block, [H]),
	case ar_test_node:send_new_block(ar_test_node:peer_ip(ToNode), Block) of
		{ok, {{<<"200">>, _}, _, _, _, _}} ->
			ok;
		{ok, {{<<"208">>, _}, _, _, _, _}} ->
			ok;
		Error ->
			?assert(false, io_lib:format("Got unexpected error: ~p", [Error]))
	end.

ensure_block_applied(H, FromNode, ToNode, TargetHeight) ->
	ar_util:do_until(
		fun() ->
			send_block(H, FromNode, ToNode),
			Height = ar_test_node:remote_call(ToNode, ar_node, get_height, []),
			Height >= TargetHeight
		end,
		1000,
		?BLOCK_DELIVERY_TIMEOUT).

wait_until_step_number(Node, StepNumber) ->
	true = ar_util:do_until(
		fun() -> 
			CurrentStepNumber = ar_test_node:remote_call(Node, ar_nonce_limiter, get_current_step_number, []),
			CurrentStepNumber >= StepNumber
		end,
		500, 
	60000).

with_nonce_limiter_paused(Node, Fun) when is_function(Fun, 0) ->
	Pid = suspend_nonce_limiter(Node),
	try
		Fun()
	after
		resume_nonce_limiter(Node, Pid)
	end.

with_vdf_pull_and_push_disabled(Node, Fun) when is_function(Fun, 0) ->
	{ok, Config} = ar_test_node:remote_call(Node, arweave_config, get_env, []),
	DisableFlags = Config#config.disable,
	%% Update config so that ar_http_iface_middleware
	%% responds to POST /vdf with #nonce_limiter_update_response {postpone = 120 }.
	ok = ar_test_node:remote_call(
		Node,
		arweave_config,
		set_env,
		[Config#config{ disable = lists:delete(vdf_server_pull, DisableFlags) }]
	),
	%% Also suspend the pull loop so peer1 cannot fetch full sessions.
	Pid = suspend_nonce_limiter_client(Node),
	try
		Fun()
	after
		ok = ar_test_node:remote_call(Node, arweave_config, set_env, [Config]),
		resume_nonce_limiter_client(Node, Pid)
	end.

suspend_nonce_limiter(Node) ->
	Pid = ar_test_node:remote_call(Node, erlang, whereis, [ar_nonce_limiter]),
	?assert(is_pid(Pid)),
	ok = ar_test_node:remote_call(Node, sys, suspend, [Pid]),
	Pid.

suspend_nonce_limiter_client(Node) ->
	Pid = ar_test_node:remote_call(Node, erlang, whereis, [ar_nonce_limiter_client]),
	?assert(is_pid(Pid)),
	ok = ar_test_node:remote_call(Node, sys, suspend, [Pid]),
	Pid.

resume_nonce_limiter(_Node, undefined) ->
	ok;
resume_nonce_limiter(Node, Pid) ->
	case ar_test_node:remote_call(Node, erlang, is_process_alive, [Pid]) of
		true ->
			ok = ar_test_node:remote_call(Node, sys, resume, [Pid]);
		false ->
			ok
	end.

resume_nonce_limiter_client(_Node, undefined) ->
	ok;
resume_nonce_limiter_client(Node, Pid) ->
	case ar_test_node:remote_call(Node, erlang, is_process_alive, [Pid]) of
		true ->
			ok = ar_test_node:remote_call(Node, sys, resume, [Pid]);
		false ->
			ok
	end.
