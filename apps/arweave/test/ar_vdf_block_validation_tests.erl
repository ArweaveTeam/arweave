-module(ar_vdf_block_validation_tests).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

%% Low enough that the +100 step waits stay in the first post-reset session
%% without burning the test budget on VDF steps.
-define(TEST_RESET_FREQUENCY, 150).
-define(TEST_VDF_DIFFICULTY, 100_000).
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
	mock_reset_frequency(),
	try
		[B0] = test_weave(),

		%% Start nodes that won't gossip blocks to each other, so the test
		%% controls when blocks are shared while mining forks.
		ar_test_node:start(#{
			b0 => B0,
			config => #{
				[peers, vdf_client] => [ar_util:format_peer(ar_test_node:peer_ip(peer1))],
				[gossip, block, pollers] => 0
			}
		}),
		mock_reset_frequency(main),

		ar_test_node:start_peer(peer1, #{
			b0 => B0,
			config => #{
				[peers, vdf_server] => [ar_util:format_peer(ar_test_node:peer_ip(main))],
				[gossip, block, pollers] => 0
			}
		}),
		mock_reset_frequency(peer1),

		ar_test_node:with_gossip_paused(main, fun() ->
			ar_test_node:with_gossip_paused(peer1, fun() ->
				%% Still need to connect to make sure VDF is shared.
				ar_test_node:connect_to_peer(peer1),

				ar_test_node:mine(main),
				{ok, [H1 | _]} = ar_test_await:node_height(main, 1),
				ok = ar_test_await:block_applied_from(peer1, main, H1, 1),

				ar_test_node:disconnect_from(peer1),
				%% Suspend peer1's nonce limiter so it stays in the old session while
				%% main advances deep into the new one before mining.
				[H2 | _] = with_nonce_limiter_paused(peer1, fun() ->
					ok = ar_test_await:vdf_step(main, ?TEST_RESET_FREQUENCY + 101),
					ar_test_node:mine(main),
					{ok, BI2} = ar_test_await:node_height(main, 2),
					BI2
				end),

				ar_test_node:connect_to_peer(peer1),
				%% Wait until peer1 has transitioned to the new VDF session.
				ok = ar_test_await:vdf_step(peer1, ?TEST_RESET_FREQUENCY + 1),
				with_vdf_pull_and_push_disabled(peer1, fun() ->
					ar_test_node:mine(peer1),
					%% Assert that peer1 is unable to mine a block.
					timer:sleep(10000),
					BI = ar_test_node:remote_call(peer1, ar_node, get_blocks, []),
					?assertEqual(2, length(BI))
				end),

				%% Get peer1 on the main chain.
				ok = ar_test_await:block_applied_from(peer1, main, H2, 2),

				%% On the main chain, peer1 should now be able to mine a block.
				ar_test_node:mine(peer1),
				?assertMatch({ok, _}, ar_test_await:node_height(peer1, 3))
			end)
		end)
	after
		disable_mocks(main),
		disable_mocks(peer1)
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
	mock_reset_frequency(),
	try
		[B0] = test_weave(),

		%% Start nodes that won't gossip blocks to each other, so the test
		%% controls when blocks are shared while mining forks.
		ar_test_node:start(#{
			b0 => B0,
			config => #{
				[peers, vdf_client] => [ar_util:format_peer(ar_test_node:peer_ip(peer1))],
				[gossip, block, pollers] => 0
			}
		}),
		mock_reset_frequency(main),

		ar_test_node:start_peer(peer1, #{
			b0 => B0,
			config => #{
				[peers, vdf_server] => [ar_util:format_peer(ar_test_node:peer_ip(main))],
				[gossip, block, pollers] => 0,
				[vdf, pull] => false
			}
		}),
		mock_reset_frequency(peer1),

		ar_test_node:with_gossip_paused(main, fun() ->
			ar_test_node:with_gossip_paused(peer1, fun() ->
				%% Still need to connect to make sure VDF is shared.
				ar_test_node:connect_to_peer(peer1),

				ar_test_node:mine(main),
				{ok, [H1 | _]} = ar_test_await:node_height(main, 1),
				ok = ar_test_await:block_applied_from(peer1, main, H1, 1),
				ok = ar_test_await:vdf_step(peer1, ?TEST_RESET_FREQUENCY + 1),

				ar_test_node:mine(peer1),
				?assertMatch({ok, _}, ar_test_await:node_height(peer1, 2)),
				%% Peer1 must keep its losing fork fixed while main mines the winning branch.
				ok = ar_test_node:remote_call(peer1, ar_node_worker, pause, []),
				ar_test_await:mining_paused(peer1),
				ar_test_node:disconnect_from(peer1),
				ok = ar_test_await:vdf_step(main, ?TEST_RESET_FREQUENCY + 100),

				ar_test_node:mine(main),
				{ok, [H2 | _]} = ar_test_await:node_height(main, 2),
				ar_test_node:mine(main),
				{ok, [H3 | _]} = ar_test_await:node_height(main, 3),
				%% Avoids noise if the test finishes before the mining server is paused.
				ar_test_await:mining_paused(main),

				ar_test_node:connect_to_peer(peer1),
				ok = ar_test_await:block_applied_from(peer1, main, H2, 2),
				ok = ar_test_await:block_applied_from(peer1, main, H3, 3),
				?assertMatch({ok, _}, ar_test_await:node_height(peer1, 3))
			end)
		end)
	after
		disable_mocks(main),
		disable_mocks(peer1)
	end.

mock_reset_frequency() ->
	ar_test_util:new_mock(ar_nonce_limiter, [passthrough]),
	ok = meck:expect(ar_nonce_limiter, get_reset_frequency, 0, ?TEST_RESET_FREQUENCY).

mock_reset_frequency(Node) ->
	ok = ar_test_node:remote_call(Node, ar_test_util, new_mock,
		[ar_nonce_limiter, [passthrough]]),
	ok = ar_test_node:remote_call(Node, meck, expect,
		[ar_nonce_limiter, get_reset_frequency, 0, ?TEST_RESET_FREQUENCY]).

disable_mocks(Node) ->
	ok = ar_test_node:remote_call(Node, ar_test_util, unmock_module, [ar_nonce_limiter]).

test_weave() ->
	[B0] = ar_weave:init(),
	NonceLimiterInfo = B0#block.nonce_limiter_info,
	B1 = B0#block{
		nonce_limiter_info = NonceLimiterInfo#nonce_limiter_info{
			vdf_difficulty = ?TEST_VDF_DIFFICULTY,
			next_vdf_difficulty = ?TEST_VDF_DIFFICULTY
		}
	},
	[B1#block{ indep_hash = ar_block:indep_hash(B1) }].

with_nonce_limiter_paused(Node, Fun) when is_function(Fun, 0) ->
	Pid = suspend_nonce_limiter(Node),
	try
		Fun()
	after
		resume_nonce_limiter(Node, Pid)
	end.

with_vdf_pull_and_push_disabled(Node, Fun) when is_function(Fun, 0) ->
	%% Disable `[vdf, pull]' so `ar_http_iface_middleware' responds to
	%% POST /vdf with `#nonce_limiter_update_response{postpone = 120}'.
	%% (In the legacy config this was the `vdf_server_pull' bit of
	%% `disable'; in the per-leaf store it's the dedicated boolean.)
	Prior = ar_test_node:remote_call(Node, arweave_config, get, [[vdf, pull]]),
	ok = ar_test_node:remote_call(Node, arweave_config, force_config,
		[#{[vdf, pull] => false}]),
	%% Also suspend the pull loop so peer1 cannot fetch full sessions.
	Pid = suspend_nonce_limiter_client(Node),
	try
		Fun()
	after
		ok = ar_test_node:remote_call(Node, arweave_config, force_config,
			[#{[vdf, pull] => Prior}]),
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
