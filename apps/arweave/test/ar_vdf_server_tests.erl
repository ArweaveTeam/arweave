-module(ar_vdf_server_tests).

-export([init/2]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-import(ar_test_node, [stop/0, assert_wait_until_height/2, post_block/2, send_new_block/2]).

setup() ->
	ets:new(?MODULE, [named_table, set, public]),
	{ok, Config} = application:get_env(arweave, config),
	{ok, SlaveConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
    {Config, SlaveConfig}.

cleanup({Config, SlaveConfig}) ->
	application:set_env(arweave, config, Config),
	ar_test_node:remote_call(peer1, application, set_env, [arweave, config, SlaveConfig]),
	ets:delete(?MODULE).

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

init(Req, State) ->
	SplitPath = ar_http_iface_server:split_path(cowboy_req:path(Req)),
	handle(SplitPath, Req, State).

handle([<<"vdf">>], Req, State) ->
	{ok, Body, _} = ar_http_req:body(Req, ?MAX_BODY_SIZE),
	{ok, Update} = ar_serialize:binary_to_nonce_limiter_update(Body),

	{SessionKey, _} = Update#nonce_limiter_update.session_key,
	IsPartial  = Update#nonce_limiter_update.is_partial,
	UpdateOutput = hd(Update#nonce_limiter_update.checkpoints),

	Session = Update#nonce_limiter_update.session,
	StepNumber = Session#vdf_session.step_number,
	SessionOutput = hd(Session#vdf_session.steps),

	?assertNotEqual(Update#nonce_limiter_update.checkpoints, Session#vdf_session.steps),
	%% #nonce_limiter_update.checkpoints should be the checkpoints of the last step so
	%% the head of checkpoints should match the head of the session's steps
	?assertEqual(UpdateOutput, SessionOutput),

	case ets:lookup(?MODULE, SessionKey) of
		[{SessionKey, FirstStepNumber, LatestStepNumber}] ->
			?assert(not IsPartial orelse StepNumber == LatestStepNumber + 1,
					"Partial VDF update did not increase by 1"),
			ets:insert(?MODULE, {SessionKey, FirstStepNumber, StepNumber}),
			{ok, cowboy_req:reply(200, #{}, <<>>, Req), State};
		_ ->
			case IsPartial of
				true ->
					Bin = ar_serialize:nonce_limiter_update_response_to_binary(
						#nonce_limiter_update_response{ session_found = false }),
					{ok, cowboy_req:reply(202, #{}, Bin, Req), State};
				false ->
					ets:insert(?MODULE, {SessionKey, StepNumber, StepNumber}),
					{ok, cowboy_req:reply(200, #{}, <<>>, Req), State}
			end
	end.

test_vdf_server_push_fast_block() ->
	VdfPort = ar_test_node:get_unused_port(),
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),

	%% Let peer1 get ahead of main in the VDF chain
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),
	timer:sleep(3000),

	{ok, Config} = application:get_env(arweave, config),
	ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ nonce_limiter_client_peers = [ "127.0.0.1:" ++ integer_to_list(VdfPort) ]}),

	%% Setup a server to listen for VDF pushes
	Routes = [{"/[...]", ar_vdf_server_tests, []}],
	{ok, _} = cowboy:start_clear(
		ar_vdf_server_test_listener,
		[{port, VdfPort}],
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

	SessionKey0 = B0#block.nonce_limiter_info#nonce_limiter_info.next_seed,
	SessionKey1 = B1#block.nonce_limiter_info#nonce_limiter_info.next_seed,
	StepNumber1 = B1#block.nonce_limiter_info#nonce_limiter_info.global_step_number,

	[{SessionKey0, _, LatestStepNumber0}] = ets:lookup(?MODULE, SessionKey0),
	[{SessionKey1, FirstStepNumber1, _}] = ets:lookup(?MODULE, SessionKey1),
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

	SessionKey0 = B0#block.nonce_limiter_info#nonce_limiter_info.next_seed,
	SessionKey1 = B1#block.nonce_limiter_info#nonce_limiter_info.next_seed,
	StepNumber1 = B1#block.nonce_limiter_info#nonce_limiter_info.global_step_number,

	[{SessionKey0, _, LatestStepNumber0}] = ets:lookup(?MODULE, SessionKey0),
	[{SessionKey1, FirstStepNumber1, LatestStepNumber1}] = ets:lookup(?MODULE, SessionKey1),
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
	[{SessionKey0, _, NewLatestStepNumber0}] = ets:lookup(?MODULE, SessionKey0),
	[{SessionKey1, _, NewLatestStepNumber1}] = ets:lookup(?MODULE, SessionKey1),
	?assertEqual(LatestStepNumber0, NewLatestStepNumber0,
		"Session0 should not have progressed"),
	?assert(NewLatestStepNumber1 > LatestStepNumber1, "Session1 should have progressed"),

	cowboy:stop_listener(ar_vdf_server_test_listener).

test_vdf_client_fast_block() ->
	?LOG_ERROR("***** STARTING test_vdf_client_fast_block"),
	{ok, Config} = application:get_env(arweave, config),
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),

	SlaveAddress = ar_wallet:to_address(ar_test_node:remote_call(peer1, ar_wallet, new_keyfile, [])),

	%% Let peer1 get ahead of main in the VDF chain
	?LOG_ERROR([{event, start_peer}]),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),
	timer:sleep(20000),

	%% Mine a block that will be ahead of main in the VDF chain
	ar_test_node:mine(peer1),
	BI = assert_wait_until_height(peer1, 1),
	B1 = ar_test_node:remote_call(peer1, ar_storage, read_block, [hd(BI)]),
	ar_test_node:stop(peer1),

	%% Restart peer1 as a VDF client
	{ok, SlaveConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
	ar_test_node:start_peer(peer1, 
		B0, SlaveAddress,
		SlaveConfig#config{ nonce_limiter_server_trusted_peers = [ 
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

	SlaveAddress = ar_wallet:to_address(ar_test_node:remote_call(peer1, ar_wallet, new_keyfile, [])),

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
	{ok, SlaveConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
	ar_test_node:start_peer(peer1, 
		B0, SlaveAddress,
		SlaveConfig#config{ nonce_limiter_server_trusted_peers = [ "127.0.0.1:" ++ integer_to_list(Config#config.port) ],
				enable = [vdf_server_pull | SlaveConfig#config.enable] }),
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

	SlaveAddress = ar_wallet:to_address(ar_test_node:remote_call(peer1, ar_wallet, new_keyfile, [])),

	%% Let peer1 get ahead of main in the VDF chain
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),

	%% Mine a block that will be ahead of main in the VDF chain
	ar_test_node:mine(peer1),
	BI = assert_wait_until_height(peer1, 1),
	B1 = ar_test_node:remote_call(peer1, ar_storage, read_block, [hd(BI)]),
	ar_test_node:stop(peer1),

	%% Restart peer1 as a VDF client
	{ok, SlaveConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
	ar_test_node:start_peer(peer1, 
		B0, SlaveAddress,
		SlaveConfig#config{ nonce_limiter_server_trusted_peers = [
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

	SlaveAddress = ar_wallet:to_address(ar_test_node:remote_call(peer1, ar_wallet, new_keyfile, [])),

	%% Let peer1 get ahead of main in the VDF chain
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:remote_call(peer1, ar_http, block_peer_connections, []),

	%% Mine a block that will be ahead of main in the VDF chain
	ar_test_node:mine(peer1),
	BI = assert_wait_until_height(peer1, 1),
	B1 = ar_test_node:remote_call(peer1, ar_storage, read_block, [hd(BI)]),
	ar_test_node:stop(peer1),

	%% Restart peer1 as a VDF client
	{ok, SlaveConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
	ar_test_node:start_peer(peer1, 
		B0, SlaveAddress,
		SlaveConfig#config{ nonce_limiter_server_trusted_peers = [
			"127.0.0.1:" ++ integer_to_list(Config#config.port) ],
				enable = [vdf_server_pull | SlaveConfig#config.enable] }),
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

vdf_server_1() ->
	{127,0,0,1,2001}.

vdf_server_2() ->
	{127,0,0,1,2002}.

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

computed_steps() ->
    lists:reverse(ets:foldl(fun({_, Int}, Acc) -> [Int | Acc] end, [], ?MODULE)).

computed_output() ->
	receive
		{event, nonce_limiter, {computed_output, Args}} ->
			{_SessionKey, _Session, _PrevSessionKey, _PrevSession, Output, _UpperBound} = Args,
			Key = ets:info(?MODULE, size) + 1, % Unique key based on current size, ensures ordering
    		ets:insert(?MODULE, {Key, Output}),
			computed_output()
	end.

apply_external_update(Seed, Interval, ExistingSteps, StepNumber, IsPartial,
		PrevSeed, PrevInterval) ->
	apply_external_update(Seed, Interval, ExistingSteps, StepNumber, IsPartial,
		PrevSeed, PrevInterval, vdf_server_1()).
apply_external_update(Seed, Interval, ExistingSteps, StepNumber, IsPartial,
		PrevSeed, PrevInterval, Peer) ->
	PrevSessionKey = {PrevSeed, PrevInterval},

	SessionKey = {Seed, Interval},
	Steps = [list_to_binary(integer_to_list(Step)) || Step <- [StepNumber | ExistingSteps]],
	Session = #vdf_session{
		upper_bound = 0,
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

%% @doc The VDF session key is only updated when a block is procesed by the VDF server. Until that
%% happens the serve will push all VDF steps under the same session key - even if those steps
%% cross an entropy reset line. When a block comes in the server will update the session key
%% *and* move all appropriate steps to that session. Prior to 2.7 this caused VDF clients to
%% process some steps twice - once under the old session key, and once under the new session key.
%% This test asserts that this behavior has been fixed and that VDF clients only process each
%% step once.
test_session_overlap() ->
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(<<"session1">>, 1, [], 8, true, <<"session0">>, 0),
		"Partial session1, session not found"),
	?assertEqual(
		ok,
		apply_external_update(<<"session1">>, 1, [7, 6, 5], 8, false, <<"session0">>, 0),
		"Full session1"),
	?assertEqual(
		ok,
		apply_external_update(<<"session1">>, 1, [], 9, true, <<"session0">>, 0),
		"Partial session1"),
	?assertEqual(
		ok,
		apply_external_update(<<"session1">>, 1, [], 10, true, <<"session0">>, 0),
		"Partial session1, interval2"),
	?assertEqual(
		ok,
		apply_external_update(<<"session1">>, 1, [], 11, true, <<"session0">>, 0),
		"Partial session1, interval2"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(<<"session2">>, 2, [], 12, true, <<"session1">>, 1),
		"Partial session2, interval2"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = true, step_number = 11 },
		apply_external_update(<<"session1">>, 1, [8, 7, 6, 5], 9, false, <<"session1">>, 1),
		"Full session1, all steps already seen"),
	?assertEqual(
		ok,
		apply_external_update(<<"session2">>, 2, [11, 10], 12, false, <<"session1">>, 1),
		"Full session2, some steps already seen"),
	timer:sleep(2000),
	?assertEqual(
		[<<"8">>, <<"7">>, <<"6">>, <<"5">>, <<"9">>, <<"10">>, <<"11">>, <<"12">>],
		computed_steps()).

%% @doc This test asserts that the client responds correctly when it is ahead of the VDF server.
test_client_ahead() ->
	?assertEqual(
		ok,
		apply_external_update(<<"session1">>, 1, [7, 6, 5], 8, false, <<"session0">>, 0),
		"Full session"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 8 },
		apply_external_update(<<"session1">>, 1, [], 7, true, <<"session0">>, 0),
		"Partial session, client ahead"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 8 },
		apply_external_update(<<"session1">>, 1, [6, 5], 7, false, <<"session0">>, 0),
		"Full session, client ahead"),
	timer:sleep(2000),
	?assertEqual(
		[<<"8">>, <<"7">>, <<"6">>, <<"5">>],
		computed_steps()).

%% @doc
%% Test case:
%% 1. VDF server pushes a partial update that skips too far ahead of the client
%% 2. Simulate the updates that the server would then push (i.e. full session updates of the current
%%    session and maybe previous session)
%%
%% Assert that the client responds correctly and only processes each step once (even though it may
%% see the same step several times as part of the full session updates).
test_skip_ahead() ->
	?assertEqual(
		ok,
		apply_external_update(<<"session1">>, 1, [5], 6, false, <<"session0">>, 0),
		"Full session1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = true, step_number = 6 },
		apply_external_update(<<"session1">>, 1, [], 8, true, <<"session0">>, 0),
		"Partial session1, server ahead"),
	?assertEqual(
		ok,
		apply_external_update(<<"session1">>, 1, [7, 6, 5], 8, false, <<"session0">>, 0),
		"Full session1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(<<"session2">>, 2, [], 12, true, <<"session1">>, 1),
		"Partial session2, server ahead"),
	?assertEqual(
		ok,
		apply_external_update(<<"session1">>, 1, [8, 7, 6, 5], 9, false, <<"session0">>, 0),
		"Full session1, all steps already seen"),
	?assertEqual(
		ok,
		apply_external_update(<<"session2">>, 2, [11, 10], 12, false, <<"session1">>, 1),
		"Full session2, some steps already seen"),
	timer:sleep(2000),
	?assertEqual(
		[<<"6">>, <<"5">>, <<"8">>, <<"7">>, <<"9">>, <<"12">>, <<"11">>, <<"10">>],
		computed_steps()).

test_2_servers_switching() ->
	?assertEqual(
		ok,
		apply_external_update(<<"session1">>, 1, [6, 5], 7, false, <<"session0">>, 0, vdf_server_1()),
		"Full session1 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(<<"session1">>, 1, [], 8, true, <<"session0">>, 0, vdf_server_2()),
		"Partial session1 from vdf_server_2"),
	?assertEqual(
		ok,
		apply_external_update(<<"session1">>, 1, [], 9, true, <<"session0">>, 0, vdf_server_2()),
		"Partial session1 from vdf_server_2"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(<<"session2">>, 2, [], 11, true, <<"session1">>, 1, vdf_server_1()),
		"Full session2 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(<<"session2">>, 2, [10], 11, false, <<"session1">>, 1, vdf_server_1()),
		"Full session2 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(
			<<"session2">>, 2, [11, 10], 12, false, <<"session1">>, 1, vdf_server_2()),
		"Full session2 from vdf_server_2"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 12 },
		apply_external_update(<<"session2">>, 2, [], 12, true, <<"session1">>, 1, vdf_server_1()),
		"Partial (repeat) session2 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(<<"session2">>, 2, [], 13, true, <<"session1">>, 1, vdf_server_1()),
		"Partial (new) session2 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(<<"session2">>, 2, [], 14, true, <<"session1">>, 1, vdf_server_2()),
		"Partial (new) session2 from vdf_server_2"),
	timer:sleep(2000),
	?assertEqual([
		<<"7">>, <<"6">>, <<"5">>, <<"8">>, <<"9">>,
		<<"11">>, <<"10">>, <<"12">>, <<"13">>, <<"14">>
	], computed_steps()).

test_backtrack() ->
	?assertEqual(
		ok,
		apply_external_update(<<"session1">>, 1, [
			16, 15,				%% interval 3
			14, 13, 12, 11, 10, %% interval 2
			9, 8, 7, 6, 5		%% interval 1
		], 17, false, <<"session0">>, 0),
		"Full session1"),
	?assertEqual(
		ok,
		apply_external_update(<<"session1">>, 1, [], 18, true, <<"session0">>, 0),
		"Partial session1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(<<"session2">>, 2, [], 15, true, <<"session1">>, 1),
		"Partial session2"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 18 },
		apply_external_update(
			<<"session1">>, 1, [8, 7, 6, 5], 9, false, <<"session0">>, 0),
		"Backtrack. Send full session1."),
	?assertEqual(
		ok,
		apply_external_update(
			<<"session2">>, 2, [14, 13, 12, 11, 10], 15, false, <<"session1">>, 1),
		"Backtrack. Send full session2"),
	timer:sleep(2000),
	?assertEqual([
		<<"17">>,<<"16">>,<<"15">>,<<"14">>,<<"13">>,<<"12">>,
        <<"11">>,<<"10">>,<<"9">>,<<"8">>,<<"7">>,<<"6">>,
        <<"5">>,<<"18">>,<<"15">>
	], computed_steps()).

test_2_servers_backtrack() ->
	?assertEqual(
		ok,
		apply_external_update(<<"session1">>, 1, [
			16, 15,				%% interval 3
			14, 13, 12, 11, 10, %% interval 2
			9, 8, 7, 6, 5		%% interval 1
		], 17, false, <<"session0">>, 0, vdf_server_1()),
		"Full session1 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(<<"session1">>, 1, [], 18, true, <<"session0">>, 0, vdf_server_1()),
		"Partial session1 from vdf_server_1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(<<"session2">>, 2, [], 15, true, <<"session1">>, 1, vdf_server_2()),
		"Partial session2 from vdf_server_2"),
	?assertEqual(
		ok,
		apply_external_update(
			<<"session2">>, 2, [14, 13, 12, 11, 10], 15, false, <<"session1">>, 1, vdf_server_2()),
		"Backtrack in session2 from vdf_server_2"),
	timer:sleep(2000),
	?assertEqual([
		<<"17">>,<<"16">>,<<"15">>,<<"14">>,<<"13">>,<<"12">>,
        <<"11">>,<<"10">>,<<"9">>,<<"8">>,<<"7">>,<<"6">>,
        <<"5">>,<<"18">>,<<"15">>
	], computed_steps()).