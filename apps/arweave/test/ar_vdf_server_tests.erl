-module(ar_vdf_server_tests).

-export([init/2]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_vdf.hrl").

-import(ar_test_node, [stop/0, assert_wait_until_height/2, post_block/2, send_new_block/2]).

%% -------------------------------------------------------------------------------------------------
%% Test Fixtures
%% -------------------------------------------------------------------------------------------------

setup() ->
	ets:new(?MODULE, [named_table, set, public]),
	{ok, Config} = application:get_env(arweave, config),
	{ok, PeerConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
    {Config, PeerConfig}.

cleanup({Config, PeerConfig}) ->
	application:set_env(arweave, config, Config),
	ar_test_node:remote_call(peer1, application, set_env, [arweave, config, PeerConfig]),
	ets:delete(?MODULE).

setup_external_update() ->
	{ok, Config} = application:get_env(arweave, config),
	[B0] = ar_weave:init(),
	%% Start the testnode with a configured VDF server so that it doesn't compute its own VDF -
	%% this is necessary so that we can test the behavior of apply_external_update without any
	%% auto-computed VDF steps getting in the way.
	ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ nonce_limiter_server_trusted_peers = [ 
			ar_util:format_peer(vdf_server_1()),
			ar_util:format_peer(vdf_server_2()) ]}),
	ets:new(?MODULE, [named_table, ordered_set, public]),
	Pid = spawn(
		fun() ->
			ok = ar_events:subscribe(nonce_limiter),
			computed_output()
		end
	),

	?assertEqual(5, ?NONCE_LIMITER_RESET_FREQUENCY, "If this fails, the test needs to be updated"),
	{Pid, Config}.

cleanup_external_update({Pid, Config}) ->
	exit(Pid, kill),
	ok = application:set_env(arweave, config, Config),
	ets:delete(?MODULE).

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
		fun setup/0,
     	fun cleanup/1,
		[
			{timeout, 120, fun test_vdf_server_push_fast_block/0},
			{timeout, 120, fun test_vdf_server_push_slow_block/0}
		]
    }.

%% @doc Similar to the vdf_server_push_test_ tests except we test the full end-to-end
%% flow where a VDF client has to validate a block with VDF information provided by
%% the VDF server.
vdf_client_test_() ->
	{foreach,
		fun setup/0,
		fun cleanup/1,
		[
			{timeout, 180, fun test_vdf_client_fast_block/0},
			{timeout, 180, fun test_vdf_client_fast_block_pull_interface/0},
			{timeout, 180, fun test_vdf_client_slow_block/0},
			{timeout, 180, fun test_vdf_client_slow_block_pull_interface/0}
		]
    }.

external_update_test_() ->
    {foreach,
		fun setup_external_update/0,
     	fun cleanup_external_update/1,
		[
			{timeout, 120, fun test_session_overlap/0},
			{timeout, 120, fun test_client_ahead/0},
			{timeout, 120, fun test_skip_ahead/0},
			{timeout, 120, fun test_2_servers_switching/0},
			{timeout, 120, fun test_backtrack/0},
			{timeout, 120, fun test_2_servers_backtrack/0}
		]
    }.

serialize_test_() ->
    [
		{timeout, 120, fun test_serialize_update_format_1/0},
		{timeout, 120, fun test_serialize_update_format_2/0},
		{timeout, 120, fun test_serialize_response/0},
		{timeout, 120, fun test_serialize_response_compatibility/0}
	].

%% -------------------------------------------------------------------------------------------------
%% Tests
%% -------------------------------------------------------------------------------------------------

%%
%% vdf_server_push_test_
%%
test_vdf_server_push_fast_block() ->
	VDFPort = ar_test_node:get_unused_port(),
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),

	%% Let peer1 get ahead of main in the VDF chain
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),
	timer:sleep(3000),

	{ok, Config} = application:get_env(arweave, config),
	ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ nonce_limiter_client_peers = [ "127.0.0.1:" ++ integer_to_list(VDFPort) ]}),

	%% Setup a server to listen for VDF pushes
	Routes = [{"/[...]", ar_vdf_server_tests, []}],
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
	StepNumber1 = B1#block.nonce_limiter_info#nonce_limiter_info.global_step_number,

	[{Seed0, _, LatestStepNumber0}] = ets:lookup(?MODULE, Seed0),
	[{Seed1, FirstStepNumber1, _}] = ets:lookup(?MODULE, Seed1),
	?assertEqual(2, ets:info(?MODULE, size), "VDF server did not post 2 sessions"),
	?assertEqual(FirstStepNumber1, LatestStepNumber0+1),
	?assertEqual(StepNumber1, LatestStepNumber0,
		"VDF server did not post the full Session0 when starting Session1"),

	cowboy:stop_listener(ar_vdf_server_test_listener).

test_vdf_server_push_slow_block() ->
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),

	{ok, Config} = application:get_env(arweave, config),
	ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ nonce_limiter_client_peers = [ "127.0.0.1:1986" ]}),
	timer:sleep(3000),

	%% Let peer1 get ahead of main in the VDF chain
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),

	%% Setup a server to listen for VDF pushes
	Routes = [{"/[...]", ar_vdf_server_tests, []}],
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
	StepNumber1 = B1#block.nonce_limiter_info#nonce_limiter_info.global_step_number,

	[{Seed0, _, LatestStepNumber0}] = ets:lookup(?MODULE, Seed0),
	[{Seed1, FirstStepNumber1, LatestStepNumber1}] = ets:lookup(?MODULE, Seed1),
	?assertEqual(2, ets:info(?MODULE, size), "VDF server did not post 2 sessions"),
	?assert(LatestStepNumber0 > FirstStepNumber1, "Session0 should be ahead of Session1"),
	?assert(LatestStepNumber0 > LatestStepNumber1, "Session0 should be ahead of Session1"),
	%% The new session begins at the reset line in case there is a block
	%% mined strictly after the previous reset line.
	case (StepNumber1 + 1) rem 10 == 0 of
		true ->
			?assertEqual(StepNumber1 + 1, FirstStepNumber1);
		false ->
			?assert(FirstStepNumber1 >= StepNumber1 + 1)
	end,

	timer:sleep(3000),
	[{Seed0, _, NewLatestStepNumber0}] = ets:lookup(?MODULE, Seed0),
	[{Seed1, _, NewLatestStepNumber1}] = ets:lookup(?MODULE, Seed1),
	?assertEqual(LatestStepNumber0, NewLatestStepNumber0,
		"Session0 should not have progressed"),
	?assert(NewLatestStepNumber1 > LatestStepNumber1, "Session1 should have progressed"),

	cowboy:stop_listener(ar_vdf_server_test_listener).

%%
%% vdf_client_test_
%%
test_vdf_client_fast_block() ->
	{ok, Config} = application:get_env(arweave, config),
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),

	PeerAddress = ar_wallet:to_address(ar_test_node:remote_call(peer1, ar_wallet, new_keyfile, [])),

	%% Let peer1 get ahead of main in the VDF chain
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),
	timer:sleep(20000),

	%% Mine a block that will be ahead of main in the VDF chain
	ar_test_node:mine(peer1),
	BI = assert_wait_until_height(peer1, 1),
	B1 = ar_test_node:remote_call(peer1, ar_storage, read_block, [hd(BI)]),
	ar_test_node:stop(peer1),

	%% Restart peer1 as a VDF client
	{ok, PeerConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
	ar_test_node:start_peer(peer1, 
		B0, PeerAddress,
		PeerConfig#config{ nonce_limiter_server_trusted_peers = [ 
			ar_util:format_peer(ar_test_node:peer_ip(main)) ] }),
	%% Start main as a VDF server
	ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ nonce_limiter_client_peers = [ 
			ar_util:format_peer(ar_test_node:peer_ip(peer1)) ]}),
	ar_test_node:connect_to_peer(peer1),

	%% Post the block to the VDF client. It won't be able to validate it since the VDF server
	%% isn't aware of the new VDF session yet.
	send_new_block(ar_test_node:peer_ip(peer1), B1),
	timer:sleep(10000),
	?assertEqual(1,
		length(ar_test_node:remote_call(peer1, ar_node, get_blocks, [])),
		"VDF client shouldn't be able to validate the block until the VDF server posts a "
		"new VDF session"),

	%% After the VDF server receives the block, it should push the old and new VDF sessions
	%% to the VDF client allowing it to validate teh block.
	send_new_block(ar_test_node:peer_ip(main), B1),
	%% If all is right, the VDF server should push the old and new VDF sessions allowing
	%% the VDF clietn to finally validate the block.
	BI = assert_wait_until_height(peer1, 1).

test_vdf_client_fast_block_pull_interface() ->
  {ok, Config} = application:get_env(arweave, config),
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),

	PeerAddress = ar_wallet:to_address(ar_test_node:remote_call(peer1, ar_wallet, new_keyfile, [])),

	%% Let peer1 get ahead of main in the VDF chain
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),
	timer:sleep(20000),

	%% Mine a block that will be ahead of main in the VDF chain
	ar_test_node:mine(peer1),
	BI = assert_wait_until_height(peer1, 1),
	B1 = ar_test_node:remote_call(peer1, ar_storage, read_block, [hd(BI)]),
	ar_test_node:stop(peer1),

	%% Restart peer1 as a VDF client
	{ok, PeerConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
	ar_test_node:start_peer(peer1, 
		B0, PeerAddress,
		PeerConfig#config{ nonce_limiter_server_trusted_peers = [ "127.0.0.1:" ++ integer_to_list(Config#config.port) ],
				enable = [vdf_server_pull | PeerConfig#config.enable] }),
	%% Start the main as a VDF server
	ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ nonce_limiter_client_peers = [ "127.0.0.1:" ++ integer_to_list(ar_test_node:peer_port(peer1)) ]}),
	ar_test_node:connect_to_peer(peer1),

	%% Post the block to the VDF client. It won't be able to validate it since the VDF server
	%% isn't aware of the new VDF session yet.
	send_new_block(ar_test_node:peer_ip(peer1), B1),
	timer:sleep(10000),
	?assertEqual(1,
		length(ar_test_node:remote_call(peer1, ar_node, get_blocks, [])),
		"VDF client shouldn't be able to validate the block until the VDF server posts a "
		"new VDF session"),

	%% After the VDF server receives the block, it should push the old and new VDF sessions
	%% to the VDF client allowing it to validate teh block.
	send_new_block(ar_test_node:peer_ip(main), B1),
	%% If all is right, the VDF server should push the old and new VDF sessions allowing
	%% the VDF clietn to finally validate the block.
	BI = assert_wait_until_height(peer1, 1).

test_vdf_client_slow_block() ->
	{ok, Config} = application:get_env(arweave, config),
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),

	PeerAddress = ar_wallet:to_address(ar_test_node:remote_call(peer1, ar_wallet, new_keyfile, [])),

	%% Let peer1 get ahead of main in the VDF chain
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),

	%% Mine a block that will be ahead of main in the VDF chain
	ar_test_node:mine(peer1),
	BI = assert_wait_until_height(peer1, 1),
	B1 = ar_test_node:remote_call(peer1, ar_storage, read_block, [hd(BI)]),
	ar_test_node:stop(peer1),

	%% Restart peer1 as a VDF client
	{ok, PeerConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
	ar_test_node:start_peer(peer1, 
		B0, PeerAddress,
		PeerConfig#config{ nonce_limiter_server_trusted_peers = [
			"127.0.0.1:" ++ integer_to_list(Config#config.port)
		] }),
	%% Start the main as a VDF server
	ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ nonce_limiter_client_peers = [
			"127.0.0.1:" ++ integer_to_list(ar_test_node:peer_port(peer1))
		]}),
	ar_test_node:connect_to_peer(peer1),
	timer:sleep(10000),

	%% Post the block to the VDF client, it should validate it "immediately" since the
	%% VDF server is ahead of the block in the VDF chain.
	send_new_block(ar_test_node:peer_ip(peer1), B1),
	BI = assert_wait_until_height(peer1, 1).

test_vdf_client_slow_block_pull_interface() ->
  {ok, Config} = application:get_env(arweave, config),
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),

	PeerAddress = ar_wallet:to_address(ar_test_node:remote_call(peer1, ar_wallet, new_keyfile, [])),

	%% Let peer1 get ahead of main in the VDF chain
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),

	%% Mine a block that will be ahead of main in the VDF chain
	ar_test_node:mine(peer1),
	BI = assert_wait_until_height(peer1, 1),
	B1 = ar_test_node:remote_call(peer1, ar_storage, read_block, [hd(BI)]),
	ar_test_node:stop(peer1),

	%% Restart peer1 as a VDF client
	{ok, PeerConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
	ar_test_node:start_peer(peer1, 
		B0, PeerAddress,
		PeerConfig#config{ nonce_limiter_server_trusted_peers = [
			"127.0.0.1:" ++ integer_to_list(Config#config.port) ],
				enable = [vdf_server_pull | PeerConfig#config.enable] }),
	%% Start the main as a VDF server
	{ok, Config} = application:get_env(arweave, config),
	ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ nonce_limiter_client_peers = [
			"127.0.0.1:" ++ integer_to_list(ar_test_node:peer_port(peer1))
		]}),
	ar_test_node:connect_to_peer(peer1),
	timer:sleep(10000),

	%% Post the block to the VDF client, it should validate it "immediately" since the
	%% VDF server is ahead of the block in the VDF chain.
	send_new_block(ar_test_node:peer_ip(peer1), B1),
	BI = assert_wait_until_height(peer1, 1).

%%
%% external_update_test_
%%

%% @doc The VDF session key is only updated when a block is procesed by the VDF server. Until that
%% happens the serve will push all VDF steps under the same session key - even if those steps
%% cross an entropy reset line. When a block comes in the server will update the session key
%% *and* move all appropriate steps to that session. Prior to 2.7 this caused VDF clients to
%% process some steps twice - once under the old session key, and once under the new session key.
%% This test asserts that this behavior has been fixed and that VDF clients only process each
%% step once.
test_session_overlap() ->
	SessionKey0 = {<<"session0">>, 0, 1},
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(SessionKey1, [], 8, true, SessionKey0),
		"Partial session1, session not found"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [7, 6, 5], 8, false, SessionKey0),
		"Full session1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 9, true, SessionKey0),
		"Partial session1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 10, true, SessionKey0),
		"Partial session1, interval2"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 11, true, SessionKey0),
		"Partial session1, interval2"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(SessionKey2, [], 12, true, SessionKey1),
		"Partial session2, interval2"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = true, step_number = 11 },
		apply_external_update(SessionKey1, [8, 7, 6, 5], 9, false, SessionKey1),
		"Full session1, all steps already seen"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey2, [11, 10], 12, false, SessionKey1),
		"Full session2, some steps already seen"),
	timer:sleep(2000),
	?assertEqual(
		[<<"8">>, <<"7">>, <<"6">>, <<"5">>, <<"9">>, <<"10">>, <<"11">>, <<"12">>],
	computed_steps()),
	?assertEqual(
		[10, 10, 10, 10, 10, 20, 20, 20],
		computed_upper_bounds()).


%% @doc This test asserts that the client responds correctly when it is ahead of the VDF server.
test_client_ahead() ->
	SessionKey0 = {<<"session0">>, 0, 1},
	SessionKey1 = {<<"session1">>, 1, 1},
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [7, 6, 5], 8, false, SessionKey0),
		"Full session"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 8 },
		apply_external_update(SessionKey1, [], 7, true, SessionKey0),
		"Partial session, client ahead"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 8 },
		apply_external_update(SessionKey1, [6, 5], 7, false, SessionKey0),
		"Full session, client ahead"),
	timer:sleep(2000),
	?assertEqual(
		[<<"8">>, <<"7">>, <<"6">>, <<"5">>],
		computed_steps()),
	?assertEqual(
		[10, 10, 10, 10],
		computed_upper_bounds()).

%% @doc
%% Test case:
%% 1. VDF server pushes a partial update that skips too far ahead of the client
%% 2. Simulate the updates that the server would then push (i.e. full session updates of the current
%%    session and maybe previous session)
%%
%% Assert that the client responds correctly and only processes each step once (even though it may
%% see the same step several times as part of the full session updates).
test_skip_ahead() ->
	SessionKey0 = {<<"session0">>, 0, 1},
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [5], 6, false, SessionKey0),
		"Full session1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = true, step_number = 6 },
		apply_external_update(SessionKey1, [], 8, true, SessionKey0),
		"Partial session1, server ahead"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [7, 6, 5], 8, false, SessionKey0),
		"Full session1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(SessionKey2, [], 12, true, SessionKey1),
		"Partial session2, server ahead"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [8, 7, 6, 5], 9, false, SessionKey0),
		"Full session1, all steps already seen"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey2, [11, 10], 12, false, SessionKey1),
		"Full session2, some steps already seen"),
	timer:sleep(2000),
	?assertEqual(
		[<<"6">>, <<"5">>, <<"8">>, <<"7">>, <<"9">>, <<"12">>, <<"11">>, <<"10">>],
		computed_steps()),
	?assertEqual(
		[10, 10, 10, 10, 10, 20, 20, 20],
		computed_upper_bounds()).

test_2_servers_switching() ->
	SessionKey0 = {<<"session0">>, 0, 1},
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [6, 5], 7, false, SessionKey0, vdf_server_1()),
		"Full session1 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 8, true, SessionKey0, vdf_server_2()),
		"Partial session1 from vdf_server_2"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 9, true, SessionKey0, vdf_server_2()),
		"Partial session1 from vdf_server_2"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(SessionKey2, [], 11, true, SessionKey1, vdf_server_1()),
		"Full session2 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey2, [10], 11, false, SessionKey1, vdf_server_1()),
		"Full session2 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(
			SessionKey2, [11, 10], 12, false, SessionKey1, vdf_server_2()),
		"Full session2 from vdf_server_2"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 12 },
		apply_external_update(SessionKey2, [], 12, true, SessionKey1, vdf_server_1()),
		"Partial (repeat) session2 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey2, [], 13, true, SessionKey1, vdf_server_1()),
		"Partial (new) session2 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey2, [], 14, true, SessionKey1, vdf_server_2()),
		"Partial (new) session2 from vdf_server_2"),
	timer:sleep(2000),
	?assertEqual([
		<<"7">>, <<"6">>, <<"5">>, <<"8">>, <<"9">>,
		<<"11">>, <<"10">>, <<"12">>, <<"13">>, <<"14">>
	], computed_steps()),
	?assertEqual(
		[10, 10, 10, 10, 10, 20, 20, 20, 20, 20],
		computed_upper_bounds()).

test_backtrack() ->
	SessionKey0 = {<<"session0">>, 0, 1},
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [
			16, 15,				%% interval 3
			14, 13, 12, 11, 10, %% interval 2
			9, 8, 7, 6, 5		%% interval 1
		], 17, false, SessionKey0),
		"Full session1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 18, true, SessionKey0),
		"Partial session1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(SessionKey2, [], 15, true, SessionKey1),
		"Partial session2"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 18 },
		apply_external_update(
			SessionKey1, [8, 7, 6, 5], 9, false, SessionKey0),
		"Backtrack. Send full session1."),
	?assertEqual(
		ok,
		apply_external_update(
			SessionKey2, [14, 13, 12, 11, 10], 15, false, SessionKey1),
		"Backtrack. Send full session2"),
	timer:sleep(2000),
	?assertEqual([
		<<"17">>,<<"16">>,<<"15">>,<<"14">>,<<"13">>,<<"12">>,
        <<"11">>,<<"10">>,<<"9">>,<<"8">>,<<"7">>,<<"6">>,
        <<"5">>,<<"18">>,<<"15">>
	], computed_steps()),
	?assertEqual(
		[20, 20, 20, 20, 20, 20, 20, 20, 10, 10, 10, 10, 10, 20, 30],
		computed_upper_bounds()).

test_2_servers_backtrack() ->
	SessionKey0 = {<<"session0">>, 0, 1},
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [
			16, 15,				%% interval 3
			14, 13, 12, 11, 10, %% interval 2
			9, 8, 7, 6, 5		%% interval 1
		], 17, false, SessionKey0, vdf_server_1()),
		"Full session1 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 18, true, SessionKey0, vdf_server_1()),
		"Partial session1 from vdf_server_1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(SessionKey2, [], 15, true, SessionKey1, vdf_server_2()),
		"Partial session2 from vdf_server_2"),
	?assertEqual(
		ok,
		apply_external_update(
			SessionKey2, [14, 13, 12, 11, 10], 15, false, SessionKey1, vdf_server_2()),
		"Backtrack in session2 from vdf_server_2"),
	timer:sleep(2000),
	?assertEqual([
		<<"17">>,<<"16">>,<<"15">>,<<"14">>,<<"13">>,<<"12">>,
        <<"11">>,<<"10">>,<<"9">>,<<"8">>,<<"7">>,<<"6">>,
        <<"5">>,<<"18">>,<<"15">>
	], computed_steps()),
	?assertEqual(
		[20, 20, 20, 20, 20, 20, 20, 20, 10, 10, 10, 10, 10, 20, 30],
		computed_upper_bounds()).

%%
%% serialize_test_
%%

test_serialize_update_format_1() ->
	SessionKey0 = {crypto:strong_rand_bytes(48), 0, 1},
	SessionKey1 = {crypto:strong_rand_bytes(48), 1, 1},
	Update = #nonce_limiter_update{
		session_key = SessionKey1,
		is_partial = true,
		checkpoints = [crypto:strong_rand_bytes(32) || _ <- lists:seq(1, 25)],
		session = #vdf_session{
			upper_bound = 1,
			next_upper_bound = 1,
			prev_session_key = SessionKey0,
			step_number = 1,
			seed = element(1, SessionKey1),
			steps = [crypto:strong_rand_bytes(32)]
		}
	},
	Binary = ar_serialize:nonce_limiter_update_to_binary(1, Update),

	%% Deserialize function copied from ar_serialize:binary_to_nonce_limiter_update/1 as of
	%% commit 4bc555e1fa8b764c329e9e41d53489ca8cbfbfc5
	%% This test confirms that an updtaed VDF server can build a payload for a unupdated VDF
	%% client. i.e. that the Server can generate payload in Format 1
	Format1Deserialize =
		fun
			(<< NextSeed:48/binary, Interval:64, IsPartial:8,
				CheckpointLen:16, Checkpoints:(CheckpointLen * 32)/binary,
				StepNumber:64, Seed:48/binary, UpperBoundSize:8, UpperBound:(UpperBoundSize * 8),
				NextUpperBoundSize:8, NextUpperBound:(NextUpperBoundSize * 8),
				StepsLen:16, Steps:(StepsLen * 32)/binary, PrevSessionKeyBin/binary >>)
			when UpperBoundSize > 0, StepsLen > 0, CheckpointLen == ?VDF_CHECKPOINT_COUNT_IN_STEP ->
				NextUpperBound2 = case NextUpperBoundSize of 0 -> undefined; _ -> NextUpperBound end,
				NonceLimiterUpdate = #nonce_limiter_update{ session_key = {NextSeed, Interval},
						checkpoints = ar_serialize:parse_32b_list(Checkpoints),
						is_partial = case IsPartial of 0 -> false; _ -> true end,
						session = Session = #vdf_session{ step_number = StepNumber, seed = Seed,
								upper_bound = UpperBound, next_upper_bound = NextUpperBound2,
								steps = ar_serialize:parse_32b_list(Steps) } },
				case PrevSessionKeyBin of
					<<>> ->
						{ok, NonceLimiterUpdate};
					<< PrevNextSeed:48/binary, PrevInterval:64 >> ->
						Session2 = Session#vdf_session{ prev_session_key = {PrevNextSeed, PrevInterval} },
						{ok, NonceLimiterUpdate#nonce_limiter_update{ session = Session2 }};
					_ ->
						{error, invalid1}
				end;
			(_Bin) ->
				{error, invalid2}
		end,

	ExpectedSession = (Update#nonce_limiter_update.session)#vdf_session{
		prev_session_key = {element(1, SessionKey0), element(2, SessionKey0)}
	},
	ExpectedUpdate = Update#nonce_limiter_update{
		session_key = {element(1, SessionKey1), element(2, SessionKey1)},
		session = ExpectedSession
	},
	?assertEqual({ok, ExpectedUpdate}, Format1Deserialize(Binary)).

test_serialize_update_format_2() ->
	SessionKey0 = {crypto:strong_rand_bytes(48), 0, 1},
	SessionKey1 = {crypto:strong_rand_bytes(48), 1, 1},
	Checkpoints = [crypto:strong_rand_bytes(32) || _ <- lists:seq(1, 25)],
	Update = #nonce_limiter_update{
		session_key = SessionKey1,
		is_partial = true,
		checkpoints = Checkpoints,
		session = #vdf_session{
			upper_bound = 1,
			next_upper_bound = 1,
			prev_session_key = SessionKey0,
			step_number = 1,
			seed = element(1, SessionKey1),
			steps = [crypto:strong_rand_bytes(32)]
		}
	},
	Binary = ar_serialize:nonce_limiter_update_to_binary(2, Update),
	?assertEqual({ok, Update}, ar_serialize:binary_to_nonce_limiter_update(Binary)).

%% @doc test serializing and deserializing a #nonce_limiter_update_response when the client
%% is running the same node version as the server.
test_serialize_response() ->
	ResponseA = #nonce_limiter_update_response{},
	BinaryA = ar_serialize:nonce_limiter_update_response_to_binary(ResponseA),
	?assertEqual({ok, ResponseA}, ar_serialize:binary_to_nonce_limiter_update_response(BinaryA)),

	ResponseB = #nonce_limiter_update_response{
		session_found = false,
		step_number = 8589934593,
		postpone = 255,
		format = 2
	},
	BinaryB = ar_serialize:nonce_limiter_update_response_to_binary(ResponseB),
	?assertEqual({ok, ResponseB}, ar_serialize:binary_to_nonce_limiter_update_response(BinaryB)).

%% @doc test serializing and deserializing a #nonce_limiter_update_response when the client
%% is running an older node version than the server.
test_serialize_response_compatibility() ->
	BinaryA = << 0:8, 1:8, 5:8 >>,
	ResponseA = #nonce_limiter_update_response{
		session_found = false,
		step_number = 5,
		postpone = 0,
		format = 1
	},
	?assertEqual({ok, ResponseA}, ar_serialize:binary_to_nonce_limiter_update_response(BinaryA)),

	BinaryB = << 1:8, 2:8, 511:16, 120:8 >>,
	ResponseB = #nonce_limiter_update_response{
		session_found = true,
		step_number = 511,
		postpone = 120,
		format = 1
	},
	?assertEqual({ok, ResponseB}, ar_serialize:binary_to_nonce_limiter_update_response(BinaryB)).

%% -------------------------------------------------------------------------------------------------
%% Helper Functions
%% -------------------------------------------------------------------------------------------------

init(Req, State) ->
	SplitPath = ar_http_iface_server:split_path(cowboy_req:path(Req)),
	handle(SplitPath, Req, State).

handle([<<"vdf">>], Req, State) ->
	{ok, Body, _} = ar_http_req:body(Req, ?MAX_BODY_SIZE),
	case ar_serialize:binary_to_nonce_limiter_update(Body) of
		{ok, Update} ->
			handle_update(Update, Req, State);
		{error, _} ->
			Response = #nonce_limiter_update_response{ format = 2 },
			Bin = ar_serialize:nonce_limiter_update_response_to_binary(Response),
			{ok, cowboy_req:reply(202, #{}, Bin, Req), State}
	end.

handle_update(Update, Req, State) ->
	{Seed, _, _} = Update#nonce_limiter_update.session_key,
	IsPartial  = Update#nonce_limiter_update.is_partial,
	UpdateOutput = hd(Update#nonce_limiter_update.checkpoints),

	Session = Update#nonce_limiter_update.session,
	StepNumber = Session#vdf_session.step_number,
	SessionOutput = hd(Session#vdf_session.steps),

	?assertNotEqual(Update#nonce_limiter_update.checkpoints, Session#vdf_session.steps),
	%% #nonce_limiter_update.checkpoints should be the checkpoints of the last step so
	%% the head of checkpoints should match the head of the session's steps
	?assertEqual(UpdateOutput, SessionOutput),

	case ets:lookup(?MODULE, Seed) of
		[{Seed, FirstStepNumber, LatestStepNumber}] ->
			?assert(not IsPartial orelse StepNumber == LatestStepNumber + 1,
					"Partial VDF update did not increase by 1"),

			ets:insert(?MODULE, {Seed, FirstStepNumber, StepNumber}),
			{ok, cowboy_req:reply(200, #{}, <<>>, Req), State};
		_ ->
			case IsPartial of
				true ->
					Response = #nonce_limiter_update_response{ session_found = false },
					Bin = ar_serialize:nonce_limiter_update_response_to_binary(Response),
					{ok, cowboy_req:reply(202, #{}, Bin, Req), State};
				false ->
					ets:insert(?MODULE, {Seed, StepNumber, StepNumber}),
					{ok, cowboy_req:reply(200, #{}, <<>>, Req), State}
			end
	end.

vdf_server_1() ->
	{127,0,0,1,2001}.

vdf_server_2() ->
	{127,0,0,1,2002}.

computed_steps() ->
    lists:reverse(ets:foldl(fun({_, Step, _}, Acc) -> [Step | Acc] end, [], ?MODULE)).

computed_upper_bounds() ->
    lists:reverse(ets:foldl(fun({_, _, UpperBound}, Acc) -> [UpperBound | Acc] end, [], ?MODULE)).

computed_output() ->
	receive
		{event, nonce_limiter, {computed_output, Args}} ->
			{_SessionKey, _StepNumber, Output, UpperBound} = Args,
			Key = ets:info(?MODULE, size) + 1, % Unique key based on current size, ensures ordering
    		ets:insert(?MODULE, {Key, Output, UpperBound}),
			computed_output()
	end.

apply_external_update(SessionKey, ExistingSteps, StepNumber, IsPartial, PrevSessionKey) ->
	apply_external_update(SessionKey, ExistingSteps, StepNumber, IsPartial, PrevSessionKey,
		vdf_server_1()).
apply_external_update(SessionKey, ExistingSteps, StepNumber, IsPartial, PrevSessionKey, Peer) ->
	{Seed, Interval, _Difficulty} = SessionKey,
	Steps = [list_to_binary(integer_to_list(Step)) || Step <- [StepNumber | ExistingSteps]],
	Session = #vdf_session{
		upper_bound = Interval * 10,
		next_upper_bound = (Interval+1) * 10,
		prev_session_key = PrevSessionKey,
		step_number = StepNumber,
		seed = Seed,
		steps = Steps
	},

	Update = #nonce_limiter_update{
		session_key = SessionKey,
		is_partial = IsPartial,
		checkpoints = [],
		session = Session
	},
	ar_nonce_limiter:apply_external_update(Update, Peer).
