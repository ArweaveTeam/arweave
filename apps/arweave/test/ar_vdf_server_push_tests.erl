-module(ar_vdf_server_push_tests).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").
-include("ar_config.hrl").
-include("ar_consensus.hrl").
-include("ar_mining.hrl").
-include("ar_vdf.hrl").

-import(ar_test_node, [assert_wait_until_height/2, post_block/2, send_new_block/2]).

%% -------------------------------------------------------------------------------------------------
%% Test Registration
%% -------------------------------------------------------------------------------------------------

%% @doc All vdf_server_push_test_ tests test a few things
%% 1. VDF server posts regular VDF updates to the client
%% 2. For partial updates (session doesn't change), each step number posted is 1 greater than
%%    the one before
%% 3. When the client responds that it doesn't have the session in a partial update, server
%%    should post the full session
%%
%% test_vdf_server_push_fast_block tests that the VDF server can handle receiving
%% a block that is ahead in the VDF chain: specifically:
%%    When a block comes in that starts a new VDF session, the server should first post the
%%    full previous session which should include all steps up to and including the
%%    global_step_number of the block. The server should not post the new session until it has
%%    computed a step in that session - which means the new session's first step will be 1
%%    greater than the last step of the previous session and also 1 greater than the block's
%%    global_step_number
%%
%% test_vdf_server_push_slow_block tests that the VDF server can handle receiving
%% a block that is behind in the VDF chain: specifically:
%%
vdf_server_push_test_() ->
	{foreach,
		fun ar_vdf_test_utils:setup/0,
		fun ar_vdf_test_utils:cleanup/1,
		[
			ar_test_node:test_with_mocked_functions([ ar_vdf_test_utils:mock_reset_frequency()],
				fun test_vdf_server_push_fast_block/0, ?TEST_SUITE_TIMEOUT),
			ar_test_node:test_with_mocked_functions([ ar_vdf_test_utils:mock_reset_frequency()],
				fun test_vdf_server_push_slow_block/0, ?TEST_SUITE_TIMEOUT)
		]
	}.

%% -------------------------------------------------------------------------------------------------
%% Tests
%% -------------------------------------------------------------------------------------------------

test_vdf_server_push_fast_block() ->
	VDFPort = ar_test_node:get_unused_port(),
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),

	%% Let peer1 get ahead of main in the VDF chain
	_ = ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),
	timer:sleep(3000),

	{ok, Config} = application:get_env(arweave, config),
	_ = ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ 
			nonce_limiter_client_peers = [ "127.0.0.1:" ++ integer_to_list(VDFPort) ]
		}
	),
	%% Setup a server to listen for VDF pushes
	Routes = [{"/[...]", ar_vdf_test_utils, []}],
	{ok, _} = cowboy:start_clear(
		ar_vdf_server_test_listener,
		[{port, VDFPort}],
		#{ env => #{ dispatch => cowboy_router:compile([{'_', Routes}]) } }
	),
	%% Mine a block that will be ahead of main in the VDF chain
	ar_test_node:mine(peer1),
	BI = assert_wait_until_height(peer1, 1),
	B1 = ar_test_node:remote_call(peer1, ar_storage, read_block, [hd(BI)]),
	%% Post the block to main which will cause it to validate VDF for the block under
	%% the B0 session and then begin using the B1 VDF session going forward
	ok = ar_events:subscribe(block),
	post_block(B1, valid),
	timer:sleep(3000),

	Seed0 = B0#block.nonce_limiter_info#nonce_limiter_info.next_seed,
	Seed1 = B1#block.nonce_limiter_info#nonce_limiter_info.next_seed,
	StepNumber1 = ar_block:vdf_step_number(B1),

	[{Seed0, _, LatestStepNumber0}] = ets:lookup(computed_output, Seed0),
	[{Seed1, _FirstStepNumber1, _}] = ets:lookup(computed_output, Seed1),
	?assertEqual(2, ets:info(computed_output, size), "VDF server did not post 2 sessions"),
	?assertEqual(StepNumber1, LatestStepNumber0,
		"VDF server did not post the full Session0 when starting Session1"),

	cowboy:stop_listener(ar_vdf_server_test_listener).

test_vdf_server_push_slow_block() ->
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),

	{ok, Config} = application:get_env(arweave, config),
	_ = ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ 
			nonce_limiter_client_peers = [ "127.0.0.1:1986" ]
		}
	),
	timer:sleep(3000),

	%% Let peer1 get ahead of main in the VDF chain
	_ = ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),

	%% Setup a server to listen for VDF pushes
	Routes = [{"/[...]", ar_vdf_test_utils, []}],
	{ok, _} = cowboy:start_clear(
		ar_vdf_server_test_listener,
		[{port, 1986}],
		#{ env => #{ dispatch => cowboy_router:compile([{'_', Routes}]) } }
	),

	%% Mine a block that will be ahead of main in the VDF chain
	ar_test_node:mine(peer1),
	BI = assert_wait_until_height(peer1, 1),
	B1 = ar_test_node:remote_call(peer1, ar_storage, read_block, [hd(BI)]),

	%% Post the block to main which will cause it to validate VDF for the block under
	%% the B0 session and then begin using the B1 VDF session going forward
	ok = ar_events:subscribe(block),
	post_block(B1, valid),
	timer:sleep(3000),

	Seed0 = B0#block.nonce_limiter_info#nonce_limiter_info.next_seed,
	Seed1 = B1#block.nonce_limiter_info#nonce_limiter_info.next_seed,
	_StepNumber1 = ar_block:vdf_step_number(B1),

	[{Seed0, _, LatestStepNumber0}] = ets:lookup(computed_output, Seed0),
	[{Seed1, FirstStepNumber1, LatestStepNumber1}] = ets:lookup(computed_output, Seed1),
	?assert(LatestStepNumber0 > FirstStepNumber1, "Session0 should have started later than Session1"),

	timer:sleep(3000),
	[{Seed0, _, NewLatestStepNumber0}] = ets:lookup(computed_output, Seed0),
	[{Seed1, _, NewLatestStepNumber1}] = ets:lookup(computed_output, Seed1),
	?assertEqual(LatestStepNumber0, NewLatestStepNumber0,
		"Session0 should not have progressed"),
	?assert(NewLatestStepNumber1 > LatestStepNumber1, "Session1 should have progressed"),

	cowboy:stop_listener(ar_vdf_server_test_listener). 