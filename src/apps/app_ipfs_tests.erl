-module(app_ipfs_tests).
-include("../ar.hrl").
-include_lib("eunit/include/eunit.hrl").

add_local_and_get_test() ->
	Filename = "known_local.txt",
	DataDir = "src/apps/app_ipfs_test_data/",
	Path = DataDir ++ Filename,
	{ok, Data} = file:read_file(Path),
	DataToHash = timestamp_data(Data),
	{ok, Hash} = ar_ipfs:add_data(DataToHash, Filename),
	{ok, DataToHash} = ar_ipfs:cat_data_by_hash(Hash).

adt_simple_callback_gets_blocks_test_() ->
	{timeout, 30, fun() ->
		{ARNode, IPFSPid} = setup(),
		ExpectedIndeps = lists:droplast(mine_n_blocks_on_node(3, ARNode)),
		Actual = app_ipfs:get_block_hashes(IPFSPid),
		?assertEqual(ExpectedIndeps, Actual)
	end}.

adt_simple_callback_gets_txs_test_() ->
	{timeout, 30, fun() ->
		{ARNode, IPFSPid} = setup(),
		ExpectedTXIDs = add_n_txs_to_node(3, ARNode),
		Actual = lists:reverse([TX#tx.id || TX <- app_ipfs:get_txs(IPFSPid)]),
		?assertEqual(ExpectedTXIDs, Actual)
	end}.

adt_simple_callback_ipfs_add_txs_test_() ->
	{timeout, 30, fun() ->
		{ARNode, IPFSPid} = setup(),
		ExpectedTSs = add_n_tx_pairs_to_node(3, ARNode, add),
		Actual = ipfs_hashes_to_TSs(IPFSPid),
		?assertEqual(ExpectedTSs, Actual)
	end}.

adt_simple_callback_ipfs_hash_txs_test_() ->
	{timeout, 30, fun() ->
		{ARNode, IPFSPid} = setup(),
		ExpectedHashes = add_n_tx_pairs_to_node(3, ARNode, hash),
		Actual = lists:reverse(app_ipfs:get_ipfs_hashes(IPFSPid)),
		?assertEqual(ExpectedHashes, Actual)
	end}.

%%% private

setup() ->
	Node = ar_node_init(),
	timer:sleep(1000),
	{ok, Pid} = app_ipfs:start([Node]),
	timer:sleep(1000),
	{Node, Pid}.

ar_node_init() ->
	ar_storage:clear(),
	B0 = ar_weave:init([]),
	Pid = ar_node:start([], B0),
	Pid.

mine_n_blocks_on_node(N, Node) ->
	lists:foreach(fun(_) ->
			ar_node:mine(Node),
			timer:sleep(1000)
		end, lists:seq(1,N)),
	timer:sleep(1000),
	ar_node:get_blocks(Node).

add_n_txs_to_node(N, Node) ->
	% cribbed from ar_http_iface:add_external_tx_with_tags_test/0.
	prepare_tx_adder(Node),
	lists:map(fun(_) ->
			Tags = [
				{<<"TEST_TAG1">>, <<"TEST_VAL1">>},
				{<<"TEST_TAG2">>, <<"TEST_VAL2">>}
			],
			TX = tag_tx(ar_tx:new(<<"DATA">>), Tags),
			ar_http_iface:send_new_tx({127, 0, 0, 1, 1984}, TX),
			receive after 1000 -> ok end,
			ar_node:mine(Node),
			receive after 1000 -> ok end,
			TX#tx.id
		end,
		lists:seq(1,N)).

add_n_tx_pairs_to_node(N, Node, add) ->
	prepare_tx_adder(Node),
	BoringTags = [
		{<<"TEST_TAG1">>, <<"TEST_VAL1">>},
		{<<"TEST_TAG2">>, <<"TEST_VAL2">>}
	],
	lists:map(fun(X) ->
			TX1 = tag_tx(ar_tx:new(timestamp_data(<<"DATA">>)), BoringTags),
			send_tx_mine_block(Node, TX1),
			TS = ts_bin(),
			Filename = numbered_fn(X),
			IPFSTags = [{<<"IPFS-Add">>, Filename}],
			TX2 = tag_tx(ar_tx:new(timestamp_data(TS, <<"DATA">>)), IPFSTags),
			send_tx_mine_block(Node, TX2),
			TS
		end,
		lists:seq(1,N));

add_n_tx_pairs_to_node(N, Node, hash) ->
	prepare_tx_adder(Node),
	BoringTags = [
		{<<"TEST_TAG1">>, <<"TEST_VAL1">>},
		{<<"TEST_TAG2">>, <<"TEST_VAL2">>}
	],
	lists:map(fun(X) ->
			TX1 = tag_tx(ar_tx:new(timestamp_data(<<"DATA">>)), BoringTags),
			send_tx_mine_block(Node, TX1),
			TS = ts_bin(),
			Filename = numbered_fn(X),
			Data = timestamp_data(TS, <<"Data">>),
			{ok, Hash} = ar_ipfs:add_data(Data, Filename),
			IPFSTags = [{<<"IPFS-Hash">>, Hash}],
			TX2 = tag_tx(ar_tx:new(Data), IPFSTags),
			send_tx_mine_block(Node, TX2),
			Hash
		end,
		lists:seq(1,N)).

ipfs_hashes_to_TSs(Pid) ->
	lists:map(fun(Hash) ->
				{ok, Data} = ar_ipfs:cat_data_by_hash(Hash),
				<<TS:25/binary,_/binary>> = Data,
				TS
		end,
		lists:reverse(app_ipfs:get_ipfs_hashes(Pid))).

numbered_fn(N) ->
	NB = integer_to_binary(N),
	<<"testdata-", NB/binary, ".txt">>.

prepare_tx_adder(Node) ->
	ar_http_iface:reregister(Node),
	Bridge = ar_bridge:start([], Node),
	ar_http_iface:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge).

send_tx_mine_block(Node, TX) ->
	ar_http_iface:send_new_tx({127, 0, 0, 1, 1984}, TX),
	receive after 1000 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end.

tag_tx(TX, Tags) ->
	TX#tx{tags=Tags}.

timestamp_data(Data) ->
	timestamp_data(ts_bin(), Data).
timestamp_data(TS, Data) ->
	<<TS/binary, "  *  ", Data/binary>>.

ts_bin() ->
	list_to_binary(calendar:system_time_to_rfc3339(erlang:system_time(second))).

