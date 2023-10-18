-module(ar_webhook_tests).

-export([init/2]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-import(ar_test_node, [sign_tx/3, assert_post_tx_to_master/1,
		wait_until_height/1, read_block_when_stored/1]).

init(Req, State) ->
	SplitPath = ar_http_iface_server:split_path(cowboy_req:path(Req)),
	handle(SplitPath, Req, State).

handle([<<"tx">>], Req, State) ->
	{ok, Reply, _} = cowboy_req:read_body(Req),
	JSON = jiffy:decode(Reply, [return_maps]),
	TX = maps:get(<<"transaction">>, JSON),
	ets:insert(?MODULE, {{tx, maps:get(<<"id">>, TX)}, TX}),
	{ok, cowboy_req:reply(200, #{}, <<>>, Req), State};

handle([<<"block">>], Req, State) ->
	{ok, Reply, _} = cowboy_req:read_body(Req),
	JSON = jiffy:decode(Reply, [return_maps]),
	B = maps:get(<<"block">>, JSON),
	ets:insert(?MODULE, {{block, maps:get(<<"height">>, B)}, B}),
	{ok, cowboy_req:reply(200, #{}, <<>>, Req), State}.

webhooks_test_() ->
	{timeout, 120, fun test_webhooks/0}.

test_webhooks() ->
	{_, Pub} = Wallet = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),
	{ok, Config} = application:get_env(arweave, config),
	Config2 = Config#config{ webhooks = [
		#config_webhook{
			url = <<"http://127.0.0.1:1986/tx">>,
			events = [transaction]
		},
		#config_webhook{
			url = <<"http://127.0.0.1:1986/block">>,
			events = [block]
		}
	]},
	ar_test_node:start(B0, ar_wallet:to_address(ar_wallet:new_keyfile()), Config2),
	%% Setup a server that would be listening for the webhooks and registering
	%% them in the ETS table.
	ets:new(?MODULE, [named_table, set, public]),
	Routes = [{"/[...]", ar_webhook_tests, []}],
	cowboy:start_clear(
		ar_webhook_test_listener,
		[{port, 1986}],
		#{ env => #{ dispatch => cowboy_router:compile([{'_', Routes}]) } }
	),
	TXs =
		lists:map(
			fun(Height) ->
				SignedTX = sign_tx(master, Wallet, #{}),
				assert_post_tx_to_master(SignedTX),
				ar_test_node:mine(),
				wait_until_height(Height),
				SignedTX
			end,
			lists:seq(1, 10)
		),
	UnconfirmedTX = sign_tx(master, Wallet, #{}),
	assert_post_tx_to_master(UnconfirmedTX),
	lists:foreach(
		fun(Height) ->
			TX = lists:nth(Height, TXs),
			true = ar_util:do_until(
				fun() ->
					case ets:lookup(?MODULE, {block, Height}) of
						[{_, B}] ->
							{H, _, _} = ar_node:get_block_index_entry(Height),
							B2 = read_block_when_stored(H),
							Struct = ar_serialize:block_to_json_struct(B2),
							Expected =
								maps:remove(
									<<"wallet_list">>,
									jiffy:decode(ar_serialize:jsonify(Struct), [return_maps])
								),
							?assertEqual(Expected, B),
							true;	
						_ ->
							false
					end
				end,
				100,
				1000
			),
			true = ar_util:do_until(
				fun() ->
					case ets:lookup(?MODULE, {tx, ar_util:encode(TX#tx.id)}) of
						[{_, TX2}] ->
							Struct = ar_serialize:tx_to_json_struct(TX),
							Expected =
								maps:remove(
									<<"data">>,
									jiffy:decode(ar_serialize:jsonify(Struct), [return_maps])
								),
							?assertEqual(Expected, TX2),
							true;
						_ ->
							false
					end
				end,
				100,
				1000
			)
		end,
		lists:seq(1, 10)
	),
	true = ar_util:do_until(
		fun() ->
			case ets:lookup(?MODULE, {tx, ar_util:encode(UnconfirmedTX#tx.id)}) of
				[{_, TX}] ->
					Struct = ar_serialize:tx_to_json_struct(UnconfirmedTX),
					Expected =
						maps:remove(
							<<"data">>,
							jiffy:decode(ar_serialize:jsonify(Struct), [return_maps])
						),
					?assertEqual(Expected, TX),
					true;
				_ ->
					false
			end
		end,
		100,
		1000
	),
	cowboy:stop_listener(ar_webhook_test_listener),
	application:set_env(arweave, config, Config#config{ webhooks = [] }).
