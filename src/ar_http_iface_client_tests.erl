-module(ar_http_iface_client_tests).
-export([get_txs_by_send_recv_test_slow/0, get_full_block_by_hash_test_slow/0]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Tests for ar_http_iface_client

%% @doc Ensure that server info can be retreived via the HTTP interface.
get_info_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node1),
	BridgeNode = ar_bridge:start([]),
	ar_http_iface_server:reregister(http_bridge_node, BridgeNode),
	SRV = {127, 0, 0, 1, 1984},
	?assertEqual(<<?NETWORK_NAME>>, ar_http_iface_client:get_info(SRV, name)),
	?assertEqual({<<"release">>, ?RELEASE_NUMBER}, ar_http_iface_client:get_info(SRV, release)),
	?assertEqual(?CLIENT_VERSION, ar_http_iface_client:get_info(SRV, version)),
	?assertEqual(1, ar_http_iface_client:get_info(SRV, peers)),
	?assertEqual(1, ar_http_iface_client:get_info(SRV, blocks)),
	?assertEqual(0, ar_http_iface_client:get_info(SRV, height)).

%% @doc Ensure transaction reward can be retrieved via http iface.
get_tx_reward_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node1),
	% Hand calculated result for 1000 bytes.
	ExpectedPrice = ar:d(ar_tx:calculate_min_tx_cost(1000, B0#block.diff)),
	ExpectedPrice = ar:d(ar_http_iface_client:get_tx_reward({127, 0, 0, 1, 1984}, 1000)).

%% @doc Ensure that server info can be retreived via the HTTP interface.
get_unjoined_info_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start([]),
	ar_http_iface_server:reregister(Node1),
	BridgeNode = ar_bridge:start([]),
	ar_http_iface_server:reregister(http_bridge_node, BridgeNode),
	?assertEqual(<<?NETWORK_NAME>>, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, name)),
	?assertEqual(?CLIENT_VERSION, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, version)),
	?assertEqual(0, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, peers)),
	?assertEqual(0, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, blocks)),
	?assertEqual(0, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, height)).

%% @doc Check that balances can be retreived over the network.
get_balance_test() ->
	ar_storage:clear(),
	{_Priv1, Pub1} = ar_wallet:new(),
	Bs = ar_weave:init([{ar_wallet:to_address(Pub1), 10000, <<>>}]),
	Node1 = ar_node:start([], Bs),
	ar_http_iface_server:reregister(Node1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, 1984},
			"/wallet/"++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/balance",
			[]
		),
	?assertEqual(10000, list_to_integer(binary_to_list(Body))).

%% @doc Test that wallets issued in the pre-sale can be viewed.
get_presale_balance_test() ->
	ar_storage:clear(),
	{_Priv1, Pub1} = ar_wallet:new(),
	Bs = ar_weave:init([{ar_wallet:to_address(Pub1), 10000, <<>>}]),
	Node1 = ar_node:start([], Bs),
	ar_http_iface_server:reregister(Node1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, 1984},
			"/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/balance",
			[]
		),
	?assertEqual(10000, list_to_integer(binary_to_list(Body))).

%% @doc Test that last tx associated with a wallet can be fetched.
get_last_tx_single_test() ->
	ar_storage:clear(),
	{_Priv1, Pub1} = ar_wallet:new(),
	Bs = ar_weave:init([{ar_wallet:to_address(Pub1), 10000, <<"TEST_ID">>}]),
	Node1 = ar_node:start([], Bs),
	ar_http_iface_server:reregister(Node1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, 1984},
			"/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/last_tx",
			[]
		),
	?assertEqual(<<"TEST_ID">>, ar_util:decode(Body)).

%% @doc Ensure that blocks can be received via a hash.
get_block_by_hash_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node1),
	receive after 200 -> ok end,
	?assertEqual(B0, ar_http_iface_client:get_block({127, 0, 0, 1, 1984}, B0#block.indep_hash)).

% get_recall_block_by_hash_test() ->
% 	ar_storage:clear(),
%     [B0] = ar_weave:init([]),
%     ar_storage:write_block(B0),
%     [B1|_] = ar_weave:add([B0], []),
% 	ar_storage:write_block(B1),
% 	Node1 = ar_node:start([], [B1, B0]),
% 	ar_http_iface_server:reregister(Node1),
% 	receive after 200 -> ok end,
% 	not_found = ar_http_iface_client:get_block({127, 0, 0, 1, 1984}, B0#block.indep_hash).

%% @doc Ensure that full blocks can be received via a hash.
get_full_block_by_hash_test_slow() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{Priv2, Pub2} = ar_wallet:new(),
	{_Priv3, Pub3} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(9000), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(500), <<>>),
	SignedTX2 = ar_tx:sign(TX2, Priv2, Pub2),
	[B0] = ar_weave:init([]),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	Bridge = ar_bridge:start([], Node),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	receive after 200 -> ok end,
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, SignedTX),
	receive after 200 -> ok end,
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, SignedTX2),
	receive after 200 -> ok end,
	ar_node:mine(Node),
	receive after 200 -> ok end,
	[B1|_] = ar_node:get_blocks(Node),
	B2 = ar_http_iface_client:get_block({127, 0, 0, 1, 1984}, B1),
	B3 = ar_http_iface_client:get_full_block({127, 0, 0, 1, 1984}, B1),
	?assertEqual(B3, B2#block {txs = [SignedTX, SignedTX2]}).

%% @doc Ensure that blocks can be received via a height.
get_block_by_height_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node1),
	?assertEqual(B0, ar_http_iface_client:get_block({127, 0, 0, 1, 1984}, 0)).

get_current_block_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node1),
	?assertEqual(B0, ar_http_iface_client:get_current_block({127, 0, 0, 1, 1984})).

%% @doc Test adding transactions to a block.
add_external_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	Bridge = ar_bridge:start([], Node),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA">>)),
	receive after 1000 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	[B1|_] = ar_node:get_blocks(Node),
	TXID = TX#tx.id,
	?assertEqual([TXID], (ar_storage:read_block(B1))#block.txs).

%% @doc Test getting transactions
find_external_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	SearchNode = app_search:start(Node),
	ar_node:add_peers(Node, SearchNode),
	ar_http_iface_server:reregister(http_search_node, SearchNode),
	Bridge = ar_bridge:start([], Node),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA">>)),
	receive after 1000 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	%write a get_tx function like get_block
	FoundTXID = (ar_http_iface_client:get_tx({127, 0, 0, 1, 1984}, TX#tx.id))#tx.id,
	?assertEqual(FoundTXID, TX#tx.id).

fail_external_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	Bridge = ar_bridge:start([], Node),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	SearchNode = app_search:start(Node),
	ar_node:add_peers(Node, SearchNode),
	ar_http_iface_server:reregister(http_search_node, SearchNode),
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, ar_tx:new(<<"DATA">>)),
	receive after 1000 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	BadTX = ar_tx:new(<<"BADDATA">>),
	?assertEqual(not_found, ar_http_iface_client:get_tx({127, 0, 0, 1, 1984}, BadTX#tx.id)).

%% @doc Ensure that blocks can be added to a network from outside
%% a single node.
add_external_block_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node1),
	Bridge = ar_bridge:start([], Node1),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node1, Bridge),
	Node2 = ar_node:start([], [B0]),
	ar_node:mine(Node2),
	receive after 1000 -> ok end,
	[B1|_] = ar_node:get_blocks(Node2),
	ar_http_iface_server:reregister(Node1),
	ar_http_iface_client:send_new_block({127, 0, 0, 1, 1984}, ?DEFAULT_HTTP_IFACE_PORT, ar_storage:read_block(B1), B0),
	receive after 500 -> ok end,
	[B1, XB0] = ar_node:get_blocks(Node1),
	?assertEqual(B0, ar_storage:read_block(XB0)).

%% @doc Post a tx to the network and ensure that last_tx call returns the ID of last tx.
add_tx_and_get_last_test() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	Bridge = ar_bridge:start([], Node),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	{_Priv2, Pub2} = ar_wallet:new(),
	TX = ar_tx:new(ar_wallet:to_address(Pub2), ?AR(1), ?AR(9000), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	ID = SignedTX#tx.id,
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, SignedTX),
	receive after 500 -> ok end,
	ar_node:mine(Node),
	receive after 500 -> ok end,
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, 1984},
			"/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/last_tx",
			[]
		),
	?assertEqual(ID, ar_util:decode(Body)).

%% @doc Post a tx to the network and ensure that its subfields can be gathered
get_subfields_of_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	Bridge = ar_bridge:start([], Node),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	SearchNode = app_search:start(Node),
	ar_node:add_peers(Node, SearchNode),
	ar_http_iface_server:reregister(http_search_node, SearchNode),
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA">>)),
	receive after 1000 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	%write a get_tx function like get_block
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, 1984},
			"/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)) ++ "/data",
			[]
		),
	Orig = TX#tx.data,
	?assertEqual(Orig, ar_util:decode(Body)).

%% @doc Correctly check the status of pending is returned for a pending transaction
get_pending_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	Bridge = ar_bridge:start([], Node),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	SearchNode = app_search:start(Node),
	ar_node:add_peers(Node, SearchNode),
	ar_http_iface_server:reregister(http_search_node, SearchNode),
	io:format("~p\n",[
		ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA1">>))
		]),
	receive after 1000 -> ok end,
	%write a get_tx function like get_block
	{ok, {{<<"202">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, 1984},
			"/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)),
			[]
		),
	?assertEqual(<<"Pending">>, Body).

%% @doc Find all pending transactions in the network
%% TODO: Fix test to send txs from different wallets
get_multiple_pending_txs_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	Bridge = ar_bridge:start([], Node),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	SearchNode = app_search:start(Node),
	ar_node:add_peers(Node, SearchNode),
	ar_http_iface_server:reregister(http_search_node, SearchNode),
	ar_http_iface_client:send_new_tx({127, 0, 0, 1,1984}, TX1 = ar_tx:new(<<"DATA1">>)),
	receive after 1000 -> ok end,
	ar_http_iface_client:send_new_tx({127, 0, 0, 1,1984}, TX2 = ar_tx:new(<<"DATA2">>)),
	receive after 1000 -> ok end,
	%write a get_tx function like get_block
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, 1984},
			"/tx/pending",
			[]
		),
	PendingTXs = ar_serialize:dejsonify(Body),
	?assertEqual(
		[
			ar_util:encode(TX1#tx.id),
			ar_util:encode(TX2#tx.id)
		],
		PendingTXs
	).

get_tx_by_tag_test() ->
	% Spawn a network with two nodes and a chirper server
	ar_storage:clear(),
	SearchServer = app_search:start(),
	Peers = ar_network:start(10, 10),
	ar_node:add_peers(hd(Peers), SearchServer),
	% Generate the transaction.
	TX = (ar_tx:new())#tx {tags = [{<<"TestName">>, <<"TestVal">>}]},
	% Add tx to network
	ar_node:add_tx(hd(Peers), TX),
	% Begin mining
	receive after 250 -> ok end,
	ar_node:mine(hd(Peers)),
	receive after 1000 -> ok end,
	QueryJSON = ar_serialize:jsonify(
		ar_serialize:query_to_json_struct(
			{'equals', <<"TestName">>, <<"TestVal">>}
			)
		),
	{ok, {_, _, Body, _, _}} = 
		ar_httpc:request(
			<<"POST">>,
			{127, 0, 0, 1, 1984},
			"/arql",
			QueryJSON
		),
	TXs = ar_serialize:dejsonify(Body),
	?assertEqual(true, lists:member(
			TX#tx.id,
			lists:map(
				fun ar_util:decode/1,
				TXs
			)
	)).

get_txs_by_send_recv_test_slow() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{Priv2, Pub2} = ar_wallet:new(),
	{_Priv3, Pub3} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(9000), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(500), <<>>),
	SignedTX2 = ar_tx:sign(TX2, Priv2, Pub2),
	B0 = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}], 8),
	Node1 = ar_node:start([], B0),
	Node2 = ar_node:start([Node1], B0),
	ar_node:add_peers(Node1, Node2),
	ar_node:add_tx(Node1, SignedTX),
	ar_storage:write_tx([SignedTX]),
	receive after 300 -> ok end,
	ar_node:mine(Node1), % Mine B1
	receive after 1000 -> ok end,
    ar_node:add_tx(Node2, SignedTX2),
    ar_storage:write_tx([SignedTX2]),
	receive after 1000 -> ok end,
	ar_node:mine(Node2), % Mine B2
    receive after 1000 -> ok end,
	QueryJSON = ar_serialize:jsonify(
		ar_serialize:query_to_json_struct(
				{'or', {'equals', "to", TX#tx.target}, {'equals', "from", TX#tx.target}}
			)
		),
	{ok, {_, _, Res, _, _}} = 
		ar_httpc:request(
			<<"POST">>,
			{127, 0, 0, 1, 1984},
			"/arql",
			QueryJSON
		),
	TXs = ar_serialize:dejsonify(Res),
	?assertEqual(true,
		lists:member(
			SignedTX#tx.id,
			lists:map(
				fun ar_util:decode/1,
				TXs
			)
		)),
	?assertEqual(true,
		lists:member(
			SignedTX2#tx.id,
			lists:map(
				fun ar_util:decode/1,
				TXs
			)
		)).

% get_encrypted_block_test() ->
% 	ar_storage:clear(),
% 	[B0] = ar_weave:init([]),
% 	Node1 = ar_node:start([], [B0]),
% 	ar_http_iface_server:reregister(Node1),
% 	receive after 200 -> ok end,
% 	Enc0 = ar_http_iface_client:get_encrypted_block({127, 0, 0, 1, 1984}, B0#block.indep_hash),
% 	ar_storage:write_encrypted_block(B0#block.indep_hash, Enc0),
% 	ar_cleanup:remove_invalid_blocks([]),
% 	ar_http_iface_client:send_new_block(
% 		{127, 0, 0, 1, 1984},
% 		B0,
% 		B0
% 	),
% 	receive after 500 -> ok end,
% 	B0 = ar_node:get_current_block(whereis(http_entrypoint_node)),
% 	ar_node:mine(Node1).

% get_encrypted_full_block_test() ->
% 	ar_storage:clear(),
%     B0 = ar_weave:init([]),
%     ar_storage:write_block(B0),
% 	TX = ar_tx:new(<<"DATA1">>),
% 	TX1 = ar_tx:new(<<"DATA2">>),
% 	ar_storage:write_tx([TX, TX1]),
% 	Node = ar_node:start([], B0),
% 	ar_http_iface_server:reregister(Node),
% 	ar_node:mine(Node),
% 	receive after 500 -> ok end,
% 	[B1|_] = ar_node:get_blocks(Node),
% 	Enc0 = ar_http_iface_client:get_encrypted_full_block({127, 0, 0, 1, 1984}, (hd(B0))#block.indep_hash),
% 	ar_storage:write_encrypted_block((hd(B0))#block.indep_hash, Enc0),
% 	ar_cleanup:remove_invalid_blocks([B1]),
% 	ar_http_iface_client:send_new_block(
% 		{127, 0, 0, 1, 1984},
% 		hd(B0),
% 		hd(B0)
% 	),
% 	receive after 1000 -> ok end,
% 	ar_node:mine(Node).
	% ar_node:add_peers(Node, Bridge),
	% receive after 200 -> ok end,
	% ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA1">>)),
	% receive after 200 -> ok end,
	% ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX1 = ar_tx:new(<<"DATA2">>)),
	% receive after 200 -> ok end,
	% ar_node:mine(Node),
	% receive after 200 -> ok end,
	% [B1|_] = ar_node:get_blocks(Node),
	% B2 = get_block({127, 0, 0, 1, 1984}, B1),
	% ar:d(ar_http_iface_client:get_encrypted_full_block({127, 0, 0, 1, 1984}, B2#block.indep_hash)),
	% B2 = ar_http_iface_client:get_block({127, 0, 0, 1, 1984}, B1),
	% B3 = gar_http_iface_client:get_full_block({127, 0, 0, 1, 1984}, B1),
	% B3 = B2#block {txs = [TX, TX1]},
