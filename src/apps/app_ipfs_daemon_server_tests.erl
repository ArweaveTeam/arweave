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
	{ok, {{Status, _}, Body, _, _, _}} = send_request(getsend, {K, H}),
	?assertEqual(<<"200">>, Status),
	?assertEqual(<<"Data queued for distribution">>, Body),
	ok.

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
