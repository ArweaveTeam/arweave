-module(app_ipfs_tests).
-include("../ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([timestamp_data/1]).

get_everipedia_hashes_test_() ->
	{timeout, 60, fun() ->
		N = 6,
		From = 240,
		{Hashes, _More} = ar_ipfs:ep_get_ipfs_hashes(N, From),
		lists:foreach(fun(H) -> io:format("Hash: ~p~n", [H]) end, Hashes),
		?assertEqual(N, length(Hashes))
	end}.

% not_sending_already_got_test_() ->
% 	{timeout, 60, fun() ->
% 		{_, IPFSPid} = setup(),
% 		{HashTups, _} = ar_ipfs:ep_get_ipfs_hashes(3, 123),
% 		Hashes = ar_ipfs:hashes_only(HashTups),
% 		ar:d({here, Hashes}),
% 		app_ipfs:get_and_send(app_ipfs, Hashes),
% 		timer:sleep(3000),
% 		app_ipfs:get_and_send(app_ipfs, Hashes),
% 		closedown(IPFSPid)
% 	end}.

add_local_and_get_test() ->
	Filename = "known_local.txt",
	DataDir = "src/apps/app_ipfs_test_data/",
	Path = DataDir ++ Filename,
	{ok, Data} = file:read_file(Path),
	DataToHash = timestamp_data(Data),
	{ok, Hash} = ar_ipfs:add_data(DataToHash, Filename),
	{ok, DataToHash} = ar_ipfs:cat_data_by_hash(Hash).

%%% private

% setup() ->
% 	Node = ar_node_init(),
% 	timer:sleep(1000),
% 	Wallet = ar_wallet:new(),
% 	case whereis(app_ipfs) of
% 		undefined -> ok;
% 		AlreadyRunningPid -> app_ipfs:stop(AlreadyRunningPid)
% 	end,
% 	{ok, Pid} = app_ipfs:start([Node], Wallet, []),
% 	timer:sleep(1000),
% 	{Node, Pid}.

% closedown(IPFSPid) ->
% 	app_ipfs:stop(IPFSPid).

% ar_node_init() ->
% 	ar_storage:clear(),
% 	B0 = ar_weave:init([]),
% 	Pid = ar_node:start([], B0),
% 	Pid.

% mine_n_blocks_on_node(N, Node) ->
% 	lists:foreach(fun(_) ->
% 			ar_node:mine(Node),
% 			timer:sleep(1000)
% 		end, lists:seq(1,N)),
% 	timer:sleep(1000),
% 	ar_node:get_blocks(Node).

% add_n_txs_to_node(N, Node) ->
% 	% cribbed from ar_http_iface:add_external_tx_with_tags_test/0.
% 	prepare_tx_adder(Node),
% 	lists:map(fun(_) ->
% 			Tags = [
% 				{<<"TEST_TAG1">>, <<"TEST_VAL1">>},
% 				{<<"TEST_TAG2">>, <<"TEST_VAL2">>}
% 			],
% 			TX = tag_tx(ar_tx:new(<<"DATA">>), Tags),
% 			ar_http_iface:send_new_tx({127, 0, 0, 1, 1984}, TX),
% 			receive after 1000 -> ok end,
% 			ar_node:mine(Node),
% 			receive after 1000 -> ok end,
% 			TX#tx.id
% 		end,
% 		lists:seq(1,N)).

% add_n_tx_pairs_to_node(N, Node) ->
% 	prepare_tx_adder(Node),
% 	BoringTags = [
% 		{<<"TEST_TAG1">>, <<"TEST_VAL1">>},
% 		{<<"TEST_TAG2">>, <<"TEST_VAL2">>}
% 	],
% 	lists:map(fun(X) ->
% 			TX1 = tag_tx(ar_tx:new(timestamp_data(<<"DATA">>)), BoringTags),
% 			send_tx_mine_block(Node, TX1),
% 			TS = ar_ipfs:rfc3339_timestamp(),
% 			Filename = numbered_fn(X),
% 			Data = timestamp_data(TS, <<"Data">>),
% 			Tags = [{<<"IPFS-Add">>, Filename}],
% 			TX2 = tag_tx(ar_tx:new(Data), Tags),
% 			send_tx_mine_block(Node, TX2),
% 			TS
% 		end,
% 		lists:seq(1,N)).

% ipfs_hashes_to_data(Pid) ->
% 	lists:map(fun(Hash) ->
% 				{ok, Data} = ar_ipfs:cat_data_by_hash(Hash),
% 				Data
% 		end,
% 		lists:reverse(app_ipfs:get_ipfs_hashes(Pid))).

% ipfs_hashes_to_TSs(Pid) ->
% 	lists:map(fun(Bin) ->
% 				<<TS:25/binary,_/binary>> = Bin,
% 				TS
% 		end,
% 		ipfs_hashes_to_data(Pid)).

% numbered_fn(N) ->
% 	NB = integer_to_binary(N),
% 	<<"testdata-", NB/binary, ".txt">>.

% prepare_tx_adder(Node) ->
% 	ar_http_iface_server:reregister(Node),
% 	Bridge = ar_bridge:start([], [], Node),
% 	ar_http_iface_server:reregister(http_bridge_node, Bridge),
% 	ar_node:add_peers(Node, Bridge).

% send_tx_mine_block(Node, TX) ->
% 	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX),
% 	receive after 1000 -> ok end,
% 	ar_node:mine(Node),
% 	receive after 1000 -> ok end.

% tag_tx(TX, Tags) ->
% 	TX#tx{tags=Tags}.

timestamp_data(Data) ->
	timestamp_data(ar_ipfs:rfc3339_timestamp(), Data).
timestamp_data(TS, Data) ->
	<<TS/binary, "  *  ", Data/binary>>.
