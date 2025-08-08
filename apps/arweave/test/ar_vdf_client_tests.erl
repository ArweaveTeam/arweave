-module(ar_vdf_client_tests).

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

%% @doc Similar to the vdf_server_push_test_ tests except we test the full end-to-end
%% flow where a VDF client has to validate a block with VDF information provided by
%% the VDF server.
vdf_client_test_() ->
	{foreach,
		fun ar_vdf_test_utils:setup/0,
		fun ar_vdf_test_utils:cleanup/1,
		[
			ar_test_node:test_with_mocked_functions([ ar_vdf_test_utils:mock_reset_frequency()],
				fun test_vdf_client_fast_block/0, 600),
			ar_test_node:test_with_mocked_functions([ ar_vdf_test_utils:mock_reset_frequency()],
				fun test_vdf_client_fast_block_pull_interface/0, 600),
			ar_test_node:test_with_mocked_functions([ ar_vdf_test_utils:mock_reset_frequency()],
				fun test_vdf_client_slow_block/0, 600),
			ar_test_node:test_with_mocked_functions([ ar_vdf_test_utils:mock_reset_frequency()],
				fun test_vdf_client_slow_block_pull_interface/0, 600)
		]
	}.

%% -------------------------------------------------------------------------------------------------
%% Tests
%% -------------------------------------------------------------------------------------------------

test_vdf_client_fast_block() ->
	{ok, Config} = application:get_env(arweave, config),
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),

	PeerAddress = ar_wallet:to_address(ar_test_node:remote_call(peer1, ar_wallet, new_keyfile, [])),

	%% Let peer1 get ahead of main in the VDF chain
	_ = ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),
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
			]
		}),
	%% Start main as a VDF server
	_ = ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ 
			nonce_limiter_client_peers = [
				ar_util:format_peer(ar_test_node:peer_ip(peer1))
			]
		}),
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