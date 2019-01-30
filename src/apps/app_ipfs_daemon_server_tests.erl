-module(app_ipfs_daemon_server_tests).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

put_get_key_test() ->
	app_ipfs_daemon_server:start(),
	W = ar_wallet:new(),
	Q = app_queue:start(W),
	K = <<"api_key_for_test">>,
	app_ipfs_daemon_server:put_key(K, Q, W),
	{ok, Q, W} = app_ipfs_daemon_server:get_key(K),
	ok.

all_ok_test() ->
	K = init_kqw(),
	H = <<"QmYNRMvMEYhVqWrUzn5D228CBxghLUu9WKGoxgBxJS3vG9">>,
	{ok, {{Status, _}, _, Body, _, _}} = send_request(getsend, {K, H}),
	?assertEqual(<<"200">>, Status),
	?assertEqual(<<"Data queued for distribution">>, Body).

ignore_second_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	Bridge = ar_bridge:start([], Node, ?DEFAULT_HTTP_IFACE_PORT),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),

	K = init_kqw(),
	H = <<"QmYNRMvMEYhVqWrUzn5D228CBxghLUu9WKGoxgBxJS3vG9">>,
	{ok, {{Status1, _}, _, Body1, _, _}} = send_request(getsend, {K, H}),
	?assertEqual(<<"200">>, Status1),
	?assertEqual(<<"Data queued for distribution">>, Body1),

	timer:sleep(1000),
	ar_node:mine(Node),
	timer:sleep(1000),

	{ok, {{Status2, _}, _, Body2, _, _}} = send_request(getsend, {K, H}),
	?assertEqual(<<"208">>, Status2),
	?assertEqual(<<"Hash already reported by this user">>, Body2).

%%% Private

init_kqw() ->
	app_ipfs_daemon_server:start(),
	W = ar_wallet:new(),
	Q = app_queue:start(W),
	K = <<"api_key_for_test">>,
	app_ipfs_daemon_server:put_key(K, Q, W),
	K.

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
