-module(ar_firewall_distributed_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

node_validates_blocks_with_rejected_tx_test() ->
	%% Start a remote node.
	ar_rpc:ping(slave),
	Peer = {127, 0, 0, 1, ar_meta_db:get(port)},
	{SlaveNode, B0} = ar_rpc:call(slave, ar_test_node, start, [no_block, Peer], 5000),
	%% Post the first tx to the remote node. This should also make the second node peer with the first one.
	{_, Pub} = ar_wallet:new(),
	TX1 = (ar_tx:new())#tx{ data = <<"BADCONTENT1">>, owner = Pub },
	SlavePort = ar_rpc:call(slave, ar_meta_db, get, [port], 5000),
	{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			{127, 0, 0, 1, SlavePort},
			"/tx",
			[{<<"X-P2p-Port">>, integer_to_binary(ar_meta_db:get(port))}],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX1))
		),
	timer:sleep(200),
	%% Start a local node.
	{Node, _} = ar_test_node:start(B0, {127, 0, 0, 1, ar_rpc:call(slave, ar_meta_db, get, [port], 5000)}),
	%% Configure the firewall to reject one of the txs submitted to the remote node.
	ar_meta_db:put(content_policy_files, ["test/test_sig.txt"]),
	ar_firewall:reload(),
	%% Mine the tx into blocks on the remote node.
	ar_rpc:call(slave, ar_node, mine, [SlaveNode], 5000),
	timer:sleep(1000),
	%% Expect the local node to reject the block.
	?assertEqual(1, length(ar_node:get_hash_list(Node))),
	%% Post the second tx to the remote node.
	TX2 = (ar_tx:new())#tx{ data = <<"GOOD CONTENT">>, owner = Pub },
	{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			{127, 0, 0, 1, SlavePort},
			"/tx",
			[{<<"X-P2p-Port">>, integer_to_binary(ar_meta_db:get(port))}],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX2))
		),
	timer:sleep(200),
	%% Mine the second tx into a block.
	ar_rpc:call(slave, ar_node, mine, [SlaveNode], 5000),
	%% Expect the local node to fork recover to the block.
	{ok, HL} = ar_util:do_until(
		fun() ->
			HL = ar_node:get_hash_list(Node),
			case HL of
				[_H1, _H2, _H3|_Rest] ->
					{ok, HL};
				_ ->
					false
			end
		end,
		10,
		5000
	),
	%% Expect the local node to store the last block and the valid tx.
	[H2, H1, H0] = HL,
	B2 = ar_storage:read_block(H2, [H2, H1, H0]),
	[B2TX] = B2#block.txs,
	?assertEqual(TX2#tx.id, (ar_storage:read_tx(B2TX))#tx.id),
	%% Expect the local node to store the previous block without the invalid tx.
	B1 = ar_storage:read_block(H1, [H1, H0]),
	[B1TX] = B1#block.txs,
	?assertEqual(unavailable, ar_storage:read_tx(B1TX)),
	%% Expect the remote node to store both transactions.
	RemoteB2 = ar_rpc:call(slave, ar_storage, read_block, [H2, [H2, H1, H0]], 5000),
	[RemoteTX2] = RemoteB2#block.txs,
	?assertEqual(RemoteTX2, (ar_rpc:call(slave, ar_storage, read_tx, [RemoteTX2], 5000))#tx.id),
	RemoteB1 = ar_rpc:call(slave, ar_storage, read_block, [H1, [H1, H0]], 5000),
	[RemoteTX1] = RemoteB1#block.txs,
	?assertEqual(RemoteTX1, (ar_rpc:call(slave, ar_storage, read_tx, [RemoteTX1], 5000))#tx.id).
