-module(app_ipfs_daemon_server_tests).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

put_get_key_test() ->
	{K, W} = init_kqw(),
	{ok, _Q, W} = app_ipfs_daemon_server:get_key_q_wallet(K),
	app_ipfs_daemon_server:del_key(K),
	ok.

all_ok_test() ->
	{K, _W} = init_kqw(),
	H = make_new_ipfs_hash(),
	{ok, {{Status, _}, _, Body, _, _}} = send_request(getsend, {K, H}),
	app_ipfs_daemon_server:del_key(K),
	?assertEqual(<<"200">>, Status),
	?assertEqual(<<"Request sent to queue">>, Body).

ignore_second_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		[B0] = ar_weave:init([]),
		Node = ar_node:start([], [B0]),
		ar_http_iface_server:reregister(Node),
		Bridge = ar_bridge:start([], Node, ?DEFAULT_HTTP_IFACE_PORT),
		ar_http_iface_server:reregister(http_bridge_node, Bridge),
		ar_node:add_peers(Node, Bridge),

		{K, _W} = init_kqw(),
		H = make_new_ipfs_hash(),
		{ok, {{Status1, _}, _, Body1, _, _}} = send_request(getsend, {K, H}),

		timer:sleep(1000),
		ar_node:mine(Node),
		timer:sleep(1000),

		{ok, {{Status2, _}, _, Body2, _, _}} = send_request(getsend, {K, H}),
		app_ipfs_daemon_server:del_key(K),

		?assertEqual(<<"200">>, Status1),
		?assertEqual(<<"Request sent to queue">>, Body1),
		?assertEqual(<<"208">>, Status2),
		?assertEqual(<<"Hash already reported by this user">>, Body2)
	end}.

%%% Private

init_kqw() ->
	app_ipfs_daemon_server:start(),
	W = ar_wallet:new(),
	K = <<"api_key_for_test">>,
	app_ipfs_daemon_server:put_key_wallet(K, W),
	{K, W}.

make_new_ipfs_hash() ->
	Filename = <<"testdata4ipfs.txt">>,
	DataToHash = app_ipfs_tests:timestamp_data(Filename),
	{ok, Hash} = ar_ipfs:add_data(DataToHash, Filename),
	Hash.

send_request(getsend, {APIKey, IPFSHash}) ->
	JStruct = {[
		{<<"api_key">>, APIKey},
		{<<"ipfs_hash">>, IPFSHash}]},
	ar_httpc:request(
		<<"POST">>,
		{127,0,0,1,1984},
		"/api/ipfs/getsend",
		[],
		ar_serialize:jsonify(JStruct)).
