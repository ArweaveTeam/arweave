-module(ar_vdf_server_tests).

-export([init/2]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-import(ar_test_node, [assert_wait_until_height/2, post_block/2, send_new_block/2]).

%% -------------------------------------------------------------------------------------------------
%% Test Fixtures
%% -------------------------------------------------------------------------------------------------

setup() ->
	ets:new(computed_output, [named_table, set, public]),
	{ok, Config} = application:get_env(arweave, config),
	{ok, PeerConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
    {Config, PeerConfig}.

cleanup({Config, PeerConfig}) ->
	arweave_config_legacy:import(Config),
	ar_test_node:remote_call(peer1, arweave_config_legacy, import, [PeerConfig]),
	ets:delete(computed_output).

setup_external_update() ->
	{ok, Config} = application:get_env(arweave, config),
	[B0] = ar_weave:init(),
	%% Start the testnode with a configured VDF server so that it doesn't compute its own VDF -
	%% this is necessary so that we can test the behavior of apply_external_update without any
	%% auto-computed VDF steps getting in the way.
	_ = ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ 
			nonce_limiter_server_trusted_peers = [
				ar_util:format_peer(vdf_server_1()),
				ar_util:format_peer(vdf_server_2()) 
			],
			mine = true
		}
	),
	ets:new(computed_output, [named_table, ordered_set, public]),
	ets:new(add_task, [named_table, bag, public]),
	Pid = spawn(
		fun() ->
			ok = ar_events:subscribe(nonce_limiter),
			computed_output()
		end
	),
	{Pid, Config}.

cleanup_external_update({Pid, Config}) ->
	exit(Pid, kill),
	arweave_config_legacy:import(Config),
	ets:delete(add_task),
	ets:delete(computed_output).

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
			ar_test_node:test_with_mocked_functions([mock_reset_frequency()],
				fun test_vdf_server_push_fast_block/0, ?TEST_NODE_TIMEOUT),
			ar_test_node:test_with_mocked_functions([mock_reset_frequency()],
				fun test_vdf_server_push_slow_block/0, ?TEST_NODE_TIMEOUT)
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
			ar_test_node:test_with_mocked_functions([mock_reset_frequency()],
				fun test_vdf_client_fast_block/0, ?TEST_NODE_TIMEOUT),
			ar_test_node:test_with_mocked_functions([mock_reset_frequency()],
				fun test_vdf_client_fast_block_pull_interface/0, ?TEST_NODE_TIMEOUT),
			ar_test_node:test_with_mocked_functions([mock_reset_frequency()],
				fun test_vdf_client_slow_block/0, ?TEST_NODE_TIMEOUT),
			ar_test_node:test_with_mocked_functions([mock_reset_frequency()],
				fun test_vdf_client_slow_block_pull_interface/0, ?TEST_NODE_TIMEOUT)
		]
    }.

serialize_test_() ->
    [
		{timeout, 120, fun test_serialize_update_format_2/0},
		{timeout, 120, fun test_serialize_update_format_3/0},
		{timeout, 120, fun test_serialize_update_format_4/0},
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
	%% the B0 session and then begin using the (later) B1 VDF session going forward
	ok = ar_events:subscribe(block),
	post_block(B1, valid),
	timer:sleep(3000),

	Seed0 = B0#block.nonce_limiter_info#nonce_limiter_info.next_seed,
	Seed1 = B1#block.nonce_limiter_info#nonce_limiter_info.next_seed,
	StepNumber1 = ar_block:vdf_step_number(B1),

	[{Seed0, _, LatestStepNumber0}] = get_computed_output(Seed0),
	[{Seed1, _FirstStepNumber1, _}] = get_computed_output(Seed1),
	?assertEqual(2, ets:info(computed_output, size), "VDF server did not post 2 sessions"),
	?assertEqual(StepNumber1, LatestStepNumber0,
		"VDF server did not post the full Session0 when starting Session1"),

	cowboy:stop_listener(ar_vdf_server_test_listener).

test_vdf_server_push_slow_block() ->
	VDFPort = ar_test_node:get_unused_port(),
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),

	{ok, Config} = application:get_env(arweave, config),
	_ = ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ 
			nonce_limiter_client_peers = [ "127.0.0.1:" ++ integer_to_list(VDFPort) ]
		}
	),
	%% Let main get ahead of peer1 in the VDF chain
	timer:sleep(3000),

	
	_ = ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),

	%% Setup a server to listen for VDF pushes
	Routes = [{"/[...]", ar_vdf_server_tests, []}],
	{ok, _} = cowboy:start_clear(
		ar_vdf_server_test_listener,
		[{port, VDFPort}],
		#{ env => #{ dispatch => cowboy_router:compile([{'_', Routes}]) } }
	),

	%% Mine a block that will be behind main in the VDF chain
	ar_test_node:mine(peer1),
	BI = assert_wait_until_height(peer1, 1),
	B1 = ar_test_node:remote_call(peer1, ar_storage, read_block, [hd(BI)]),

	%% Post the block to main which will cause it to validate VDF for the block under
	%% the B0 session and then begin using the (earlier) B1 VDF session going forward
	ok = ar_events:subscribe(block),
	post_block(B1, valid),
	timer:sleep(3000),

	Seed0 = B0#block.nonce_limiter_info#nonce_limiter_info.next_seed,
	Seed1 = B1#block.nonce_limiter_info#nonce_limiter_info.next_seed,
	StepNumber1 = ar_block:vdf_step_number(B1),

	[{Seed0, _, LatestStepNumber0}] = get_computed_output(Seed0),
	[{Seed1, FirstStepNumber1, LatestStepNumber1}] = get_computed_output(Seed1),
	?assert(LatestStepNumber0 > FirstStepNumber1, "Session0 should have started later than Session1"),

	timer:sleep(3000),
	[{Seed0, _, NewLatestStepNumber0}] = get_computed_output(Seed0),
	[{Seed1, _, NewLatestStepNumber1}] = get_computed_output(Seed1),
	?assertEqual(LatestStepNumber0, NewLatestStepNumber0,
		"Session0 should not have progressed"),
	?assert(NewLatestStepNumber1 > LatestStepNumber1, "Session1 should have progressed"),

	cowboy:stop_listener(ar_vdf_server_test_listener).

%%
%% vdf_client_test_
%%
test_vdf_client_fast_block() ->
	ar_test_node:stop(),
	{ok, Config} = application:get_env(arweave, config),
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),

	PeerAddress = ar_wallet:to_address(ar_test_node:remote_call(peer1, ar_wallet, new_keyfile, [])),

	%% Let peer1 get ahead of main in the VDF chain
	_ = ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),
	timer:sleep(5_000),

	%% Mine a block that will be ahead of main in the VDF chain
	ar_test_node:mine(peer1),
	BI = assert_wait_until_height(peer1, 1),
	B1 = ar_test_node:remote_call(peer1, ar_storage, read_block, [hd(BI)]),
	ar_test_node:stop(peer1),

	%% Restart peer1 as a VDF client
	{ok, PeerConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
	_ = ar_test_node:start_peer(peer1,
		B0, PeerAddress,
		PeerConfig#config{ 
			nonce_limiter_server_trusted_peers = [
				ar_util:format_peer(ar_test_node:peer_ip(main))
			]
		}),
	%% Start main as a VDF server
	ar_test_node:stop(),
	_ = ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ 
			nonce_limiter_client_peers = [
				ar_util:format_peer(ar_test_node:peer_ip(peer1))
			]
		}),

	%% Post the block to the VDF client. It won't be able to validate it since the VDF server
	%% isn't aware of the new VDF session yet.
	send_new_block(ar_test_node:peer_ip(peer1), B1),
	timer:sleep(5_000),
	?assertEqual(1,
		length(ar_test_node:remote_call(peer1, ar_node, get_blocks, [])),
		"VDF client shouldn't be able to validate the block until the VDF server posts a "
		"new VDF session"),

	%% After the VDF server receives the block, it should push the old and new VDF sessions
	%% to the VDF client allowing it to validate teh block.
	send_new_block(ar_test_node:peer_ip(main), B1),
	%% If all is right, the VDF server should push the old and new VDF sessions allowing
	%% the VDF client to finally validate the block.
	BI = assert_wait_until_height(peer1, 1).

test_vdf_client_fast_block_pull_interface() ->
  	{ok, Config} = application:get_env(arweave, config),
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),

	PeerAddress = ar_wallet:to_address(ar_test_node:remote_call(peer1, ar_wallet, new_keyfile, [])),

	%% Let peer1 get ahead of main in the VDF chain
	_ = ar_test_node:start_peer(peer1, B0),
	_ = ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),
	timer:sleep(20000),

	%% Mine a block that will be ahead of main in the VDF chain
	ar_test_node:mine(peer1),
	BI = assert_wait_until_height(peer1, 1),
	B1 = ar_test_node:remote_call(peer1, ar_storage, read_block, [hd(BI)]),
	ar_test_node:stop(peer1),

	%% Restart peer1 as a VDF client
	{ok, PeerConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
	_ = ar_test_node:start_peer(peer1,
		B0, PeerAddress,
		PeerConfig#config{ 
			nonce_limiter_server_trusted_peers = [
				ar_util:format_peer(ar_test_node:peer_ip(main))
			],
			enable = [vdf_server_pull | PeerConfig#config.enable] 
		}
	),
	%% Start the main as a VDF server
	_ = ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ 
			nonce_limiter_client_peers = [
				ar_util:format_peer(ar_test_node:peer_ip(peer1))
			]
		}
	),
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
	_ = ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),

	%% Mine a block that will be ahead of main in the VDF chain
	ar_test_node:mine(peer1),
	BI = assert_wait_until_height(peer1, 1),
	B1 = ar_test_node:remote_call(peer1, ar_storage, read_block, [hd(BI)]),
	ar_test_node:stop(peer1),

	%% Restart peer1 as a VDF client
	{ok, PeerConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
	_ = ar_test_node:start_peer(peer1,
		B0, PeerAddress,
		PeerConfig#config{ 
			nonce_limiter_server_trusted_peers = [
				"127.0.0.1:" ++ integer_to_list(Config#config.port)
			] 
		}
	),
	%% Start the main as a VDF server
	_ = ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ 
			nonce_limiter_client_peers = [
				"127.0.0.1:" ++ integer_to_list(ar_test_node:peer_port(peer1))
			]
		}
	),
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
	_ = ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),

	%% Mine a block that will be ahead of main in the VDF chain
	ar_test_node:mine(peer1),
	BI = assert_wait_until_height(peer1, 1),
	B1 = ar_test_node:remote_call(peer1, ar_storage, read_block, [hd(BI)]),
	ar_test_node:stop(peer1),

	%% Restart peer1 as a VDF client
	{ok, PeerConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
	_ = ar_test_node:start_peer(peer1,
		B0, PeerAddress,
		PeerConfig#config{ 
			nonce_limiter_server_trusted_peers = [
				"127.0.0.1:" ++ integer_to_list(Config#config.port) 
			],
			enable = [vdf_server_pull | PeerConfig#config.enable] 
		}
	),
	%% Start the main as a VDF server
	{ok, Config} = application:get_env(arweave, config),
	_ = ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ 
			nonce_limiter_client_peers = [
				"127.0.0.1:" ++ integer_to_list(ar_test_node:peer_port(peer1))
			]
		}
	),
	ar_test_node:connect_to_peer(peer1),
	timer:sleep(10000),

	%% Post the block to the VDF client, it should validate it "immediately" since the
	%% VDF server is ahead of the block in the VDF chain.
	send_new_block(ar_test_node:peer_ip(peer1), B1),
	BI = assert_wait_until_height(peer1, 1).

%%
%% serialize_test_
%%

test_serialize_update_format_2() ->
	SessionKey0 = {crypto:strong_rand_bytes(48), 0, 1},
	SessionKey1 = {crypto:strong_rand_bytes(48), 1, 1},
	Checkpoints = [crypto:strong_rand_bytes(32) || _ <- lists:seq(1, 25)],
	Update = #nonce_limiter_update{
		session_key = SessionKey1,
		is_partial = true,
		session = #vdf_session{
			step_checkpoints_map = #{ 1 => Checkpoints },
			upper_bound = 1,
			next_upper_bound = 1,
			prev_session_key = SessionKey0,
			step_number = 1,
			seed = element(1, SessionKey1),
			steps = [crypto:strong_rand_bytes(32)]
		}
	},
	Binary = ar_serialize:nonce_limiter_update_to_binary(2, Update),
	?assertEqual({ok, Update}, ar_serialize:binary_to_nonce_limiter_update(2, Binary)).

test_serialize_update_format_3() ->
	SessionKey0 = {crypto:strong_rand_bytes(48), 0, 1},
	SessionKey1 = {crypto:strong_rand_bytes(48), 1, 1},
	Checkpoints = [crypto:strong_rand_bytes(32) || _ <- lists:seq(1, 25)],
	Update = #nonce_limiter_update{
		session_key = SessionKey1,
		is_partial = true,
		session = #vdf_session{
			step_checkpoints_map = #{ 1 => Checkpoints },
			upper_bound = 1,
			next_upper_bound = 1,
			prev_session_key = SessionKey0,
			step_number = 1,
			seed = element(1, SessionKey1),
			steps = [crypto:strong_rand_bytes(32)]
		}
	},
	Binary = ar_serialize:nonce_limiter_update_to_binary(3, Update),
	?assertEqual({ok, Update}, ar_serialize:binary_to_nonce_limiter_update(3, Binary)).

test_serialize_update_format_4() ->
	SessionKey0 = {crypto:strong_rand_bytes(48), 0, 1},
	SessionKey1 = {crypto:strong_rand_bytes(48), 1, 1},
	Checkpoints = [crypto:strong_rand_bytes(32) || _ <- lists:seq(1, 25)],
	Update = #nonce_limiter_update{
		session_key = SessionKey1,
		is_partial = true,
		session = #vdf_session{
			step_checkpoints_map = #{ 1 => Checkpoints },
			upper_bound = 1,
			next_upper_bound = 1,
			prev_session_key = SessionKey0,
			vdf_difficulty = 10000,
			next_vdf_difficulty = 1,
			step_number = 1,
			seed = element(1, SessionKey1),
			steps = [crypto:strong_rand_bytes(32)]
		}
	},
	Binary = ar_serialize:nonce_limiter_update_to_binary(4, Update),
	?assertEqual({ok, Update}, ar_serialize:binary_to_nonce_limiter_update(4, Binary)).

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
	case ar_serialize:binary_to_nonce_limiter_update(2, Body) of
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
	Session = Update#nonce_limiter_update.session,
	StepNumber = Session#vdf_session.step_number,
	NSteps = length(Session#vdf_session.steps),
	Checkpoints = maps:get(StepNumber, Session#vdf_session.step_checkpoints_map),

	UpdateOutput = hd(Checkpoints),

	SessionOutput = hd(Session#vdf_session.steps),

	?assertNotEqual(Checkpoints, Session#vdf_session.steps),
	%% #nonce_limiter_update.checkpoints should be the checkpoints of the last step so
	%% the head of checkpoints should match the head of the session's steps
	?assertEqual(UpdateOutput, SessionOutput),

	case ets:lookup(computed_output, Seed) of
		[{Seed, FirstStepNumber, LatestStepNumber}] ->
			%% Normally a partial VDF update should always increase by 1, but the VDF_DIFFICULTY
			%% is so low in tests that there can be a race condition which causes a partial
			%% update to repeat a VDF step. This assertion allows for that scenario in order
			%% to improve test reliability.
			?assert(
					not IsPartial orelse
					StepNumber == LatestStepNumber + 1 orelse
					StepNumber == LatestStepNumber,
				lists:flatten(io_lib:format(
					"Partial VDF update has step gap, "
					"StepNumber: ~p, LatestStepNumber: ~p",
					[StepNumber, LatestStepNumber]))),

			ets:insert(computed_output, {Seed, FirstStepNumber, StepNumber}),
			{ok, cowboy_req:reply(200, #{}, <<>>, Req), State};
		_ ->
			case IsPartial of
				true ->
					Response = #nonce_limiter_update_response{ session_found = false },
					Bin = ar_serialize:nonce_limiter_update_response_to_binary(Response),
					{ok, cowboy_req:reply(202, #{}, Bin, Req), State};
				false ->
					ets:insert(computed_output, {Seed, StepNumber - NSteps + 1, StepNumber}),
					{ok, cowboy_req:reply(200, #{}, <<>>, Req), State}
			end
	end.

get_computed_output(Seed) ->
	ar_util:do_until(
		fun() ->
			case ets:lookup(computed_output, Seed) of
				[] -> false;
				_ -> true
			end
		end,
		1000,
		10_000
	),
	ets:lookup(computed_output, Seed).

mock_reset_frequency() ->
	{
		ar_nonce_limiter, get_reset_frequency,
		fun() ->
			5
		end
	}.
