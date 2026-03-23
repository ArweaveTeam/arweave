-module(ar_forced_validation_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-import(ar_test_node, [wait_until_height/2,
                       post_block/2,
                       sign_block/3,
                       send_new_block/2]).

avde3_post_2_8_test_() ->
    	{setup, fun setup_all_post_2_8/0, fun cleanup_all_post_fork/1,
		{foreach, fun reset_node/0,
                 [
                  instantiator(fun inject_undersized_rsa/1)
                 ]
                }
        }.

avde3_post_2_9_height_test_() ->
        {setup, fun setup_all_post_2_9_height/0, fun cleanup_all_post_fork/1,
		{foreach, fun reset_node/0,
                 [
                  instantiator(fun inject_undersized_rsa/1)
                 ]
                }
        }.

start_node() ->
	[B0] = ar_weave:init([], 0), %% Set difficulty to 0 to speed up tests
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:connect_to_peer(peer1).

reset_node() ->
	ar_blacklist_middleware:reset(),
	ar_test_node:remote_call(peer1, ar_blacklist_middleware, reset, []),
	ar_test_node:connect_to_peer(peer1),

	Height = height(peer1),
	[{PrevH, _, _} | _] = wait_until_height(main, Height),
	ar_test_node:disconnect_from(peer1),
	ar_test_node:mine(peer1),
	[{H, _, _} | _] = ar_test_node:assert_wait_until_height(peer1, Height + 1),
	B = ar_test_node:remote_call(peer1, ar_block_cache, get, [block_cache, H]),
	PrevB = ar_test_node:remote_call(peer1, ar_block_cache, get, [block_cache, PrevH]),
	{ok, Config} = ar_test_node:remote_call(peer1, arweave_config, get_env, []),
	Key = ar_test_node:remote_call(peer1, ar_wallet, load_key, [Config#config.mining_addr]),
	{Key, B, PrevB}.

setup_all_post_2_8() ->
	{Setup, Cleanup} = ar_test_node:mock_functions([
		{ar_fork, height_2_8, fun() -> 0 end}
		]),
	Functions = Setup(),
	start_node(),
	{Cleanup, Functions}.

setup_all_post_2_9_height() ->
        {Setup, Cleanup} = ar_test_node:mock_functions([
		{ar_fork, height_2_9, fun() -> 0 end}
		]),
	Functions = Setup(),
	start_node(),
	{Cleanup, Functions}.


cleanup_all_post_fork({Cleanup, Functions}) ->
	Cleanup(Functions).

instantiator(TestFun) ->
	fun (Fixture) -> {timeout, 180, {with, Fixture, [TestFun]}} end.

debug_decode_block(B) ->
    %% TEST ENCODE/DECODE - This is to help to determine where
    %%                      encode/decode errors occur, as the HTTP API can be
    %%                      a bit opaque.
    BinaryB = ar_serialize:block_to_binary(B),
    try
	ar_serialize:binary_to_block(BinaryB),
        ?debugFmt("============== DECODE PASSED ================== ~n", [])
    catch
        Exception:Reason:Stacktrace ->
            ?debugFmt("==>>>decode error: ~p:~p -->>>>>stack:~p~n",
                      [Exception, Reason, Stacktrace]),
            {error, {Exception, Reason}}
    end.

inject_undersized_rsa({Key, BIn, PrevB}) ->
    Victim = main,
    Peer = ar_test_node:peer_ip(Victim),

    ReqRes = ar_http:req(#{
                           method => get,
                           peer => Peer,
                           path => "/info",
                           headers => p2p_headers()
                          }),
    ?assertMatch({ok, {{<<"200">>, _}, _, _Body, _Start, _End}}, ReqRes),
    TX = poc07_tx(),
    B = block_with_undersized_rsa(Key, PrevB, BIn, TX),

    debug_decode_block(B),

    post_block(B, valid),

    timer:sleep(1000),
    ReqRes2 = ar_http:req(#{
                           method => get,
                           peer => Peer,
                           path => "/info",
                           headers => p2p_headers()
                          }),
    ?assertMatch({ok, {{<<"200">>, _}, _, _Body, _Start, _End}}, ReqRes2),
    ok.


block_with_undersized_rsa(Key, PrevB, BIn, TX) ->
    ok = ar_events:subscribe(block),

    Height = BIn#block.height,
    BlockSize = ar_tx:get_weave_size_increase(TX, Height),
    WeaveSize = PrevB#block.weave_size + BlockSize,
    TxRoot = ar_block:generate_tx_root_for_block([TX], Height),
    SizeTagged = ar_block:generate_size_tagged_list_from_txs([TX], Height),

    B1 = BIn#block{
           txs = [TX],
           block_size = BlockSize,
           weave_size = WeaveSize,
           tx_root = TxRoot,
           size_tagged_txs = SizeTagged
          },

    B2 = sign_block(B1, PrevB, Key),
    B2.

poc07_tx() ->
    Sig = <<71>>,
    #tx{
        format = 1,
        id = crypto:hash(sha256, << Sig/binary >>),
        last_tx = <<>>,
        owner = <<191>>,
        owner_address = not_set,
        tags = [],
        target = <<>>,
        quantity = 0,
        data = <<>>,
        data_size = 0,
        data_root = <<>>,
        signature = Sig,
        reward = 0,
        denomination = 0,
        signature_type = {rsa, 65537}
    }.


p2p_headers() ->
    {ok, Config} = arweave_config:get_env(),
    [{<<"x-p2p-port">>, integer_to_binary(Config#config.port)},
     {<<"x-release">>, integer_to_binary(?RELEASE_NUMBER)}].


%% ------------------------------------------------------------------------------------------
%% Helper functions
%% ------------------------------------------------------------------------------------------

height(Node) ->
	ar_test_node:remote_call(Node, ar_node, get_height, []).
