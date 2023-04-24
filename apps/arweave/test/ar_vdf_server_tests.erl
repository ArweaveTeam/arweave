-module(ar_vdf_server_tests).

-export([init/2]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-import(ar_test_node, [start/3, sign_tx/3, assert_post_tx_to_master/1,
		wait_until_height/1, read_block_when_stored/1, sign_block/3, post_block/2]).

init(Req, State) ->
	SplitPath = ar_http_iface_server:split_path(cowboy_req:path(Req)),
	handle(SplitPath, Req, State).

handle([<<"vdf">>], Req, State) ->
	?LOG_ERROR("***VDF!!!!*** ~p", [State]),
	{ok, Body, _} = ar_http_req:body(Req, ?MAX_BODY_SIZE),
	{ok, Update} = ar_serialize:binary_to_nonce_limiter_update(Body),
	{NextSeed, IntervalNumber} = Update#nonce_limiter_update.session_key,
	StepNumber = Update#nonce_limiter_update.session#vdf_session.step_number,
	?LOG_ERROR("SessionKey: ~p, ~p", [ar_util:encode(NextSeed), IntervalNumber]),
	?LOG_ERROR("StepNumber: ~p", [StepNumber]),
	% TX = maps:get(<<"transaction">>, JSON),
	% ets:insert(?MODULE, {{tx, maps:get(<<"id">>, TX)}, TX}),
	{ok, cowboy_req:reply(200, #{}, <<>>, Req), State}.

vdf_server_test_() ->
	{timeout, 120, fun test_vdf_server/0}.

test_vdf_server() ->
	{Priv, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),
	?LOG_ERROR("B0: ~p", [B0#block.nonce_limiter_info]),
	?LOG_ERROR("BO SessionKey: ~p, ~p", [ar_util:encode(B0#block.nonce_limiter_info#nonce_limiter_info.next_seed), B0#block.nonce_limiter_info#nonce_limiter_info.global_step_number]),
	{ok, Config} = application:get_env(arweave, config),
	Config2 = Config#config{ nonce_limiter_client_peers = [ "127.0.0.1:1986" ]},
	start(B0, ar_wallet:to_address(ar_wallet:new_keyfile()), Config2),
	%% Setup a server that would be listening for the webhooks and registering
	%% them in the ETS table.
	ets:new(?MODULE, [named_table, set, public]),
	Routes = [{"/[...]", ar_vdf_server_tests, []}],
	cowboy:start_clear(
		ar_vdf_server_test_listener,
		[{port, 1986}],
		#{ env => #{ dispatch => cowboy_router:compile([{'_', Routes}]) } }
	),
	timer:sleep(5000),
	ok = ar_events:subscribe(block),
	Info0 = B0#block.nonce_limiter_info,
	B1 = sign_block(B0#block{
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			nonce_limiter_info = Info0#nonce_limiter_info{
				global_step_number = 200 } }, B0, Priv),
	?LOG_ERROR("POSTING BLOCK"),
	post_block(B1, valid),
	% ar_node:mine(),
	% timer:sleep(5000),
	% ar_node:mine(),
	% timer:sleep(5000),
	% ar_node:mine(),
	% timer:sleep(5000),
	% TXs =
	% 	lists:map(
	% 		fun(Height) ->
	% 			SignedTX = sign_tx(master, Wallet, #{}),
	% 			assert_post_tx_to_master(SignedTX),
	% 			ar_node:mine(),
	% 			wait_until_height(Height),
	% 			SignedTX
	% 		end,
	% 		lists:seq(1, 10)
	% 	),
	% UnconfirmedTX = sign_tx(master, Wallet, #{}),
	% assert_post_tx_to_master(UnconfirmedTX),
	% lists:foreach(
	% 	fun(Height) ->
	% 		TX = lists:nth(Height, TXs),
	% 		true = ar_util:do_until(
	% 			fun() ->
	% 				case ets:lookup(?MODULE, {block, Height}) of
	% 					[{_, B}] ->
	% 						{H, _, _} = ar_node:get_block_index_entry(Height),
	% 						B2 = read_block_when_stored(H),
	% 						Struct = ar_serialize:block_to_json_struct(B2),
	% 						Expected =
	% 							maps:remove(
	% 								<<"wallet_list">>,
	% 								jiffy:decode(ar_serialize:jsonify(Struct), [return_maps])
	% 							),
	% 						?assertEqual(Expected, B),
	% 						true;	
	% 					_ ->
	% 						false
	% 				end
	% 			end,
	% 			100,
	% 			1000
	% 		),
	% 		true = ar_util:do_until(
	% 			fun() ->
	% 				case ets:lookup(?MODULE, {tx, ar_util:encode(TX#tx.id)}) of
	% 					[{_, TX2}] ->
	% 						Struct = ar_serialize:tx_to_json_struct(TX),
	% 						Expected =
	% 							maps:remove(
	% 								<<"data">>,
	% 								jiffy:decode(ar_serialize:jsonify(Struct), [return_maps])
	% 							),
	% 						?assertEqual(Expected, TX2),
	% 						true;
	% 					_ ->
	% 						false
	% 				end
	% 			end,
	% 			100,
	% 			1000
	% 		)
	% 	end,
	% 	lists:seq(1, 10)
	% ),
	% true = ar_util:do_until(
	% 	fun() ->
	% 		case ets:lookup(?MODULE, {tx, ar_util:encode(UnconfirmedTX#tx.id)}) of
	% 			[{_, TX}] ->
	% 				Struct = ar_serialize:tx_to_json_struct(UnconfirmedTX),
	% 				Expected =
	% 					maps:remove(
	% 						<<"data">>,
	% 						jiffy:decode(ar_serialize:jsonify(Struct), [return_maps])
	% 					),
	% 				?assertEqual(Expected, TX),
	% 				true;
	% 			_ ->
	% 				false
	% 		end
	% 	end,
	% 	100,
	% 	1000
	% ),
	cowboy:stop_listener(ar_vdf_server_test_listener),
	application:set_env(arweave, config, Config#config{ nonce_limiter_client_peers = [] }).
