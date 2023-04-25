-module(ar_vdf_server_tests).

-export([init/2]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-import(ar_test_node, [start/3, slave_start/1, disconnect_from_slave/0,
		sign_tx/3, assert_post_tx_to_master/1, slave_mine/0,
		assert_slave_wait_until_height/1, slave_call/3,
		wait_until_height/1, read_block_when_stored/1,
		create_block/2, sign_block/3, post_block/2]).

init(Req, State) ->
	SplitPath = ar_http_iface_server:split_path(cowboy_req:path(Req)),
	handle(SplitPath, Req, State).

handle([<<"vdf">>], Req, State) ->
	{ok, Body, _} = ar_http_req:body(Req, ?MAX_BODY_SIZE),
	{ok, Update} = ar_serialize:binary_to_nonce_limiter_update(Body),
	?LOG_ERROR("***VDF!!!!***"),
	{SessionKey, _} = Update#nonce_limiter_update.session_key,
	StepNumber = Update#nonce_limiter_update.session#vdf_session.step_number,
	IsPartial  = Update#nonce_limiter_update.is_partial,

	case ets:lookup(?MODULE, SessionKey) of
		[{SessionKey, StartStepNumber, LastStepNumber}] ->
			?LOG_ERROR("***FOUND*** ~p / ~p", [ar_util:encode(SessionKey), StepNumber]),
			?assertEqual(StepNumber, LastStepNumber+1, "VDF updates did not increase by 1"),
			ets:insert(?MODULE, {SessionKey, StartStepNumber, StepNumber}),
			{ok, cowboy_req:reply(200, #{}, <<>>, Req), State};
		_ ->
			case IsPartial of
				true ->
					?LOG_ERROR("***NOT FOUND*** ~p / ~p", [ar_util:encode(SessionKey), StepNumber]),
					Bin = ar_serialize:nonce_limiter_update_response_to_binary(
						#nonce_limiter_update_response{ session_found = false }),
					{ok, cowboy_req:reply(202, #{}, Bin, Req), State};
				false ->
					?LOG_ERROR("***INITIALIZING*** ~p / ~p", [ar_util:encode(SessionKey), StepNumber]),
					ets:insert(?MODULE, {SessionKey, StepNumber, StepNumber}),
					{ok, cowboy_req:reply(200, #{}, <<>>, Req), State}
			end
	end.


vdf_server_test_() ->
	{timeout, 120, fun test_vdf_server/0}.

%% @doc tests a few things
%% 1. VDF server posts regular VDF updates to the client
%% 2. For partial updates (session doesn't change), each step number posted is 1 greater than
%%    the one before
%% 3. When the client responds that it doesn't have the session in a partial update, server
%%    should post the full session
%% 4. When posting a new full session, the server should also post the full previous session -
%%    which should include all steps up to (but not included) the first step in the new session
test_vdf_server() ->
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),

	%% Let the slave get ahead of master in the VDF chain
	slave_start(B0),
	slave_call(ar_http, block_peer_connections, []),
	timer:sleep(3000),

	{ok, Config} = application:get_env(arweave, config),
	Config2 = Config#config{ nonce_limiter_client_peers = [ "127.0.0.1:1986" ]},
	start(B0, ar_wallet:to_address(ar_wallet:new_keyfile()), Config2),

	%% Setup a server to listen for VDF pushes
	ets:new(?MODULE, [named_table, set, public]),
	Routes = [{"/[...]", ar_vdf_server_tests, []}],
	{ok, _} = cowboy:start_clear(
		ar_vdf_server_test_listener,
		[{port, 1986}],
		#{ env => #{ dispatch => cowboy_router:compile([{'_', Routes}]) } }
	),

	%% Mine a block that will be ahead of master in the VDF chain
	slave_mine(),
	BI = assert_slave_wait_until_height(1),
	B1 = slave_call(ar_storage, read_block, [hd(BI)]),

	%% Post the block to master which will cause it to validate VDF for the block under
	%% the B0 session and then begin using the B1 VDF session going forward
	ok = ar_events:subscribe(block),
	post_block(B1, valid),
	timer:sleep(3000),

	SessionKey0 = B0#block.nonce_limiter_info#nonce_limiter_info.next_seed,
	SessionKey1 = B1#block.nonce_limiter_info#nonce_limiter_info.next_seed,
	Steps1 = B1#block.nonce_limiter_info#nonce_limiter_info.global_step_number,

	[{SessionKey0, _, LastStepNumber0}] = ets:lookup(?MODULE, SessionKey0),
	[{SessionKey1, StartStepNumber1, _}] = ets:lookup(?MODULE, SessionKey1),
	?assertEqual(2, ets:info(?MODULE, size), "VDF server did not post 2 sessions"),
	?assert(StartStepNumber1 > LastStepNumber0),
	?assertEqual(Steps1, LastStepNumber0+1,
		"VDF server did not post the full Session0 when starting Session1"),

	cowboy:stop_listener(ar_vdf_server_test_listener),
	application:set_env(arweave, config, Config#config{ nonce_limiter_client_peers = [] }).
