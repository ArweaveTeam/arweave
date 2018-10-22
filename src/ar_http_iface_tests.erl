%%%
%%% @doc tests for ar_http_iface client and server
%%%

-module(ar_http_iface_tests).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc Ensure that server info can be retreived via the HTTP interface.
get_info_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node1),
	BridgeNode = ar_bridge:start([]),
	ar_http_iface_server:reregister(http_bridge_node, BridgeNode),
	?assertEqual(<<?NETWORK_NAME>>, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, name)),
	?assertEqual({<<"release">>, ?RELEASE_NUMBER}, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, release)),
	?assertEqual(?CLIENT_VERSION, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, version)),
	?assertEqual(0, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, peers)),
	?assertEqual(1, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, blocks)),
	?assertEqual(0, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, height)).

%% @doc Ensure transaction reward can be retrieved via http iface.
get_tx_reward_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node1),
	% Hand calculated result for 1000 bytes.
	ExpectedPrice = ar_tx:calculate_min_tx_cost(1000, B0#block.diff),
	ExpectedPrice = get_tx_reward({127, 0, 0, 1, 1984}, 1000).

%% @doc Get the minimum cost that a remote peer would charge for
%% a transaction of the given data size in bytes.
get_tx_reward(Peer, Size) ->
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/price/" ++ integer_to_list(Size),
			[]
		),
	binary_to_integer(Body).

%% @doc Ensure that objects are only re-gossiped once.
single_regossip_test_() ->
	{ timeout, 60, fun() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node1),
	TX = ar_tx:new(<<"TEST DATA">>),
	Responses =
		ar_util:pmap(
			fun(_) ->
				ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX)
			end,
			lists:seq(1, 100)
		),
	1 = length([ processed || {ok, {{<<"200">>, _}, _, _, _, _}} <- Responses ])
	end}.

%% @doc Unjoined nodes should not accept blocks
post_block_to_unjoined_node_test() ->
	JB = ar_serialize:jsonify({[{foo, [<<"bing">>, 2.3, true]}]}),
	{ok, {RespTup, _, Body, _, _}} =
		ar_httpc:request(<<"POST">>, {127, 0, 0, 1, 1984}, "/block/", JB),
	case ar_node:is_joined(whereis(http_entrypoint_node)) of
		false ->
			?assertEqual({<<"503">>, <<"Service Unavailable">>}, RespTup),
			?assertEqual(<<"Not joined.">>, Body);
		true ->
			?assertEqual({<<"400">>,<<"Bad Request">>}, RespTup),
			?assertEqual(<<"Invalid block.">>, Body)
	end.

%% @doc Test that nodes sending too many requests are temporarily blocked: (a) GET.
-spec node_blacklisting_get_spammer_test() -> ok.
node_blacklisting_get_spammer_test() ->
	{RequestFun, ErrorResponse} = get_fun_msg_pair(get_info),
	node_blacklisting_test_frame(RequestFun, ErrorResponse, ?MAX_REQUESTS + 1, 1).

%% @doc Test that nodes sending too many requests are temporarily blocked: (b) POST.
-spec node_blacklisting_post_spammer_test() -> ok.
node_blacklisting_post_spammer_test() ->
	{RequestFun, ErrorResponse} = get_fun_msg_pair(send_new_tx),
	node_blacklisting_test_frame(RequestFun, ErrorResponse, ?MAX_REQUESTS + 1, 1).

%% @doc Given a label, return a fun and a message.
-spec get_fun_msg_pair(atom()) -> {fun(), any()}.
get_fun_msg_pair(get_info) ->
	{ fun(_) ->
			ar_http_iface_client:get_info({127, 0, 0, 1, 1984})
		end
	, info_unavailable};
get_fun_msg_pair(send_new_tx) ->
	{ fun(_) ->
			case ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, ar_tx:new(<<"DATA">>)) of
				{ok,
					{{<<"429">>, <<"Too Many Requests">>}, _,
						<<"Too Many Requests">>, _, _}} ->
					too_many_requests;
				_ -> ok
			end
		end
	, too_many_requests}.

%% @doc Frame to test spamming an endpoint.
-spec node_blacklisting_test_frame(fun(), any(), non_neg_integer(), non_neg_integer()) -> ok.
node_blacklisting_test_frame(RequestFun, ErrorResponse, NRequests, ExpectedErrors) ->
	ar_blacklist:reset_counters(),
	Responses =	ar_util:pmap(RequestFun, lists:seq(1, NRequests)),
	ar_blacklist:reset_counters(),
	ActualErrors = length(lists:filter(fun(X) -> X == ErrorResponse end, Responses)),
	?assertEqual(NRequests, length(Responses)),
	?assertEqual(ExpectedErrors, ActualErrors).

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
	?assertEqual(10000, binary_to_integer(Body)).

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
	?assertEqual(10000, binary_to_integer(Body)).

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
	ar_storage:write_block(B0),
	Node1 = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node1),
	receive after 200 -> ok end,
	?assertEqual(B0, ar_http_iface_client:get_block({127, 0, 0, 1, 1984}, B0#block.indep_hash, B0#block.hash_list)).

% get_recall_block_by_hash_test() ->
%	ar_storage:clear(),
%	  [B0] = ar_weave:init([]),
%	  ar_storage:write_block(B0),
%	  [B1|_] = ar_weave:add([B0], []),
%	ar_storage:write_block(B1),
%	Node1 = ar_node:start([], [B1, B0]),
%	ar_http_iface_server:reregister(Node1),
%	receive after 200 -> ok end,
%	not_found = ar_http_iface_client:get_block({127, 0, 0, 1, 1984}, B0#block.indep_hash).

%% @doc Ensure that blocks can be received via a height.
get_block_by_height_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	Node1 = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node1),
	?assertEqual(B0, ar_http_iface_client:get_block({127, 0, 0, 1, 1984}, 0, B0#block.hash_list)).

get_current_block_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	Node1 = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node1),
	ar_util:do_until(
		fun() -> B0 == ar_node:get_current_block(Node1) end,
		100,
		2000
	),
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
	?assertEqual([TXID], (ar_storage:read_block(B1, ar_node:get_hash_list(Node)))#block.txs).

%% @doc Test adding transactions to a block.
add_external_tx_with_tags_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	Bridge = ar_bridge:start([], Node),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	TX = ar_tx:new(<<"DATA">>),
	TaggedTX =
		TX#tx {
			tags =
				[
					{<<"TEST_TAG1">>, <<"TEST_VAL1">>},
					{<<"TEST_TAG2">>, <<"TEST_VAL2">>}
				]
		},
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TaggedTX),
	receive after 1000 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	[B1Hash|_] = ar_node:get_blocks(Node),
	B1 = ar_storage:read_block(B1Hash, ar_node:get_hash_list(Node)),
	TXID = TaggedTX#tx.id,
	?assertEqual([TXID], B1#block.txs),
	?assertEqual(TaggedTX, ar_storage:read_tx(hd(B1#block.txs))).

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
	timer:sleep(1000),
	ar_node:mine(Node),
	timer:sleep(1000),
	% write a get_tx function like get_block
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
	timer:sleep(1000),
	ar_node:mine(Node),
	timer:sleep(1000),
	BadTX = ar_tx:new(<<"BADDATA">>),
	?assertEqual(not_found, ar_http_iface_client:get_tx({127, 0, 0, 1, 1984}, BadTX#tx.id)).

%% @doc Ensure that blocks can be added to a network from outside
%% a single node.
add_external_block_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		[BGen] = ar_weave:init([]),
		Node1 = ar_node:start([], [BGen]),
		ar_http_iface_server:reregister(http_entrypoint_node, Node1),
		timer:sleep(500),
		Bridge = ar_bridge:start([], Node1),
		ar_http_iface_server:reregister(http_bridge_node, Bridge),
		ar_node:add_peers(Node1, Bridge),
		Node2 = ar_node:start([], [BGen]),
		ar_node:mine(Node2),
		ar_util:do_until(
			fun() ->
				length(ar_node:get_blocks(Node2)) == 2
			end,
			100,
			10 * 1000
		),
		[BH2 | _] = ar_node:get_blocks(Node2),
		ar_http_iface_server:reregister(Node1),
		ar_http_iface_client:send_new_block(
			{127, 0, 0, 1, 1984},
			?DEFAULT_HTTP_IFACE_PORT,
			ar_storage:read_block(BH2, ar_node:get_hash_list(Node2)),
			BGen
		),
		% Wait for test block and assert.
		?assert(ar_util:do_until(
			fun() ->
				length(ar_node:get_blocks(Node1)) > 1
			end,
			1000,
			10 * 1000
		)),
		[BH1 | _] = ar_node:get_blocks(Node1),
		?assertEqual(BH1, BH2)
	end}.

%% @doc POST block with bad "block_data_segment" field in json
add_external_block_with_bad_bds_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		[BGen] = ar_weave:init([]),
		Node1 = ar_node:start([], [BGen]),
		ar_http_iface_server:reregister(http_entrypoint_node, Node1),
		timer:sleep(500),
		Bridge = ar_bridge:start([], Node1),
		ar_http_iface_server:reregister(http_bridge_node, Bridge),
		ar_node:add_peers(Node1, Bridge),
		Node2 = ar_node:start([], [BGen]),
		ar_node:mine(Node2),
		ar_util:do_until(
			fun() ->
				length(ar_node:get_blocks(Node2)) == 2
			end,
			100,
			10 * 1000
		),
		[BH2 | _] = ar_node:get_blocks(Node2),
		ar_http_iface_server:reregister(Node1),
		NewB = ar_storage:read_block(BH2, ar_node:get_hash_list(Node2)),
		BlockDataSegment = <<"badbadbad">>,
		{ok,{{<<"400">>,_},_,<<"Invalid Block Work">>,_,_}} =
			ar_http_iface_client:send_new_block_with_bds(
				{127, 0, 0, 1, 1984},
				?DEFAULT_HTTP_IFACE_PORT,
				NewB,
				BGen,
				BlockDataSegment
			)
	end}.

%% @doc POST block with "block_data_segment" field in json
add_external_block_with_bds_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		[BGen] = ar_weave:init([]),
		Node1 = ar_node:start([], [BGen]),
		ar_http_iface_server:reregister(http_entrypoint_node, Node1),
		timer:sleep(500),
		Bridge = ar_bridge:start([], Node1),
		ar_http_iface_server:reregister(http_bridge_node, Bridge),
		ar_node:add_peers(Node1, Bridge),
		Node2 = ar_node:start([], [BGen]),
		ar_node:mine(Node2),
		ar_util:do_until(
			fun() ->
				length(ar_node:get_blocks(Node2)) == 2
			end,
			100,
			10 * 1000
		),
		[BH2 | _] = ar_node:get_blocks(Node2),
		ar_http_iface_server:reregister(Node1),
		NewB = ar_storage:read_block(BH2, ar_node:get_hash_list(Node2)),
		{ok,{{<<"200">>,_},_,_,_,_}} =
			ar_http_iface_client:send_new_block(
				{127, 0, 0, 1, 1984},
				?DEFAULT_HTTP_IFACE_PORT,
				NewB,
				BGen
			),
		% Wait for test block and assert.
		?assert(ar_util:do_until(
			fun() ->
				length(ar_node:get_blocks(Node1)) > 1
			end,
			1000,
			10 * 1000
		)),
		[BH1 | _] = ar_node:get_blocks(Node1),
		?assertEqual(BH1, BH2)
	end}.
%% @doc Ensure that blocks with tx can be added to a network from outside
%% a single node.
add_external_block_with_good_tx_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		[BGen] = ar_weave:init([]),
		Node1 = ar_node:start([], [BGen]),
		ar_http_iface_server:reregister(http_entrypoint_node, Node1),
		timer:sleep(500),
		Bridge = ar_bridge:start([], Node1),
		ar_http_iface_server:reregister(http_bridge_node, Bridge),
		ar_node:add_peers(Node1, Bridge),
		% Start node 2, add transaction, and wait until mined.
		Node2 = ar_node:start([], [BGen]),
		TX = ar_tx:new(<<"TEST DATA">>),
		ar_node:add_tx(Node2, TX),
		timer:sleep(500),
		ar_node:mine(Node2),
		ar_util:do_until(
			fun() ->
				[BH | _] = ar_node:get_blocks(Node2),
				B = ar_storage:read_block(BH, ar_node:get_hash_list(Node2)),
				lists:member(TX#tx.id, B#block.txs)
			end,
			500,
			10 * 1000
		),
		[BTest|_] = ar_node:get_blocks(Node2),
		ar_http_iface_server:reregister(Node1),

		ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX),
		timer:sleep(1500),

		ar_http_iface_client:send_new_block(
			{127, 0, 0, 1, 1984},
			?DEFAULT_HTTP_IFACE_PORT,
			ar_storage:read_block(BTest, ar_node:get_hash_list(Node2)),
			BGen
		),
		% Wait for test block and assert that it contains transaction.
		?assert(ar_util:do_until(
			fun() ->
				length(ar_node:get_blocks(Node1)) > 1
			end,
			500,
			10 * 1000
		)),
		[BH | _] = ar_node:get_blocks(Node1),
		B = ar_storage:read_block(BH, ar_node:get_hash_list(Node1)),
		?assert(lists:member(TX#tx.id, B#block.txs))
	end}.

%% @doc If a node doesn't have a submitted tx, it should reject the block.
add_external_block_with_bad_tx_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		[BGen] = ar_weave:init([]),
		Node1 = ar_node:start([], [BGen]),
		ar_http_iface_server:reregister(http_entrypoint_node, Node1),
		timer:sleep(500),
		Bridge = ar_bridge:start([], Node1),
		ar_http_iface_server:reregister(http_bridge_node, Bridge),
		ar_node:add_peers(Node1, Bridge),
		% Start node 2, add transaction, and wait until mined.
		Node2 = ar_node:start([], [BGen]),
		TX = ar_tx:new(<<"TEST DATA">>),
		ar_node:add_tx(Node2, TX),
		timer:sleep(500),
		ar_node:mine(Node2),
		ar_util:do_until(
			fun() ->
				[BH | _] = ar_node:get_blocks(Node2),
				B = ar_storage:read_block(BH, ar_node:get_hash_list(Node2)),
				lists:member(TX#tx.id, B#block.txs)
			end,
			500,
			10 * 1000
		),
		[BTest|_] = ar_node:get_blocks(Node2),
		ar_http_iface_server:reregister(Node1),
		{ok,{{<<"400">>,<<"Bad Request">>},
		_, <<"Cannot verify transactions.">>,
		_,_}} = ar_http_iface_client:send_new_block(
			{127, 0, 0, 1, 1984},
			?DEFAULT_HTTP_IFACE_PORT,
			ar_storage:read_block(BTest, ar_node:get_hash_list(Node2)),
			BGen
		)
	end}.

%% @doc Ensure that blocks can be added to a network from outside
%% a single node.
fork_recover_by_http_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		[BGen] = ar_weave:init([]),
		Node1 = ar_node:start([], [BGen]),
		ar_http_iface_server:reregister(Node1),
		Bridge = ar_bridge:start([], Node1),
		ar_http_iface_server:reregister(http_bridge_node, Bridge),
		ar_node:add_peers(Node1, Bridge),
		Node2 = ar_node:start([], [BGen]),
		ar_node:mine(Node2),
		timer:sleep(500),
		ar_node:mine(Node2),
		timer:sleep(500),
		ar_node:mine(Node2),
		timer:sleep(500),
		ar_util:do_until(
			fun() ->
				length(ar_node:get_blocks(Node2)) > 5
			end,
			1000,
			10 * 1000
		),
		[BTest|_] = ar_node:get_blocks(Node2),
		ar_http_iface_server:reregister(Node1),
		NewB = ar_storage:read_block(BTest, ar_node:get_hash_list(Node2)),
		BDS = ar_block:generate_block_data_segment(
			BGen,
			BGen,
			lists:map(fun ar_storage:read_tx/1, NewB#block.txs),
			NewB#block.reward_addr,
			NewB#block.timestamp,
			NewB#block.tags
		),
		{ok,{{<<"200">>,_},_,_,_,_}} = ar_http_iface_client:send_new_block_with_bds(
			{127, 0, 0, 1, 1984},
			?DEFAULT_HTTP_IFACE_PORT,
			ar_storage:read_block(BTest, ar_node:get_hash_list(Node2)),
			BGen,
			BDS
		),
		% Wait for test block and assert.
		?assert(ar_util:do_until(
			fun() ->
				length(ar_node:get_blocks(Node1)) > 1
			end,
			1000,
			10 * 1000
		)),
		[HB | TBs] = ar_node:get_blocks(Node1),
		?assertEqual(HB, BTest),
		LB = lists:last(TBs),
		?assertEqual(BGen, ar_storage:read_block(LB, ar_node:get_hash_list(Node1)))
	end}.

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
	timer:sleep(500),
	ar_node:mine(Node),
	timer:sleep(500),
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
	timer:sleep(1000),
	ar_node:mine(Node),
	timer:sleep(1000),
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

%% @doc Post a tx to the network and ensure that its subfields can be gathered
get_bad_subfield_of_tx_404_test() ->
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
	timer:sleep(1000),
	ar_node:mine(Node),
	timer:sleep(1000),
	%write a get_tx function like get_block
	{ok, {{<<"404">>, _}, _, <<"Not Found.">>, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, 1984},
			"/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)) ++ "/qweasd.css",
			[]
		).

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
	timer:sleep(1000),
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
get_multiple_pending_txs_test_() ->
	%% TODO: faulty test: having multiple txs against a single wallet
	%% in a single block is problematic.
	{timeout, 60, fun() ->
		ar_storage:clear(),
		W1 = ar_wallet:new(),
		W2 = ar_wallet:new(),
		TX1 = ar_tx:new(<<"DATA1">>, ?AR(999)),
		TX2 = ar_tx:new(<<"DATA2">>, ?AR(999)),
		SignedTX1 = ar_tx:sign(TX1, W1),
		SignedTX2 = ar_tx:sign(TX2, W2),
		[B0] =
			ar_weave:init(
				[
					{ar_wallet:to_address(W1), ?AR(1000), <<>>},
					{ar_wallet:to_address(W2), ?AR(1000), <<>>}
				]
			),
		Node = ar_node:start([], [B0]),
		ar_http_iface_server:reregister(Node),
		Bridge = ar_bridge:start([], [Node]),
		ar_http_iface_server:reregister(http_bridge_node, Bridge),
		ar_node:add_peers(Node, Bridge),
		ar_http_iface_client:send_new_tx({127, 0, 0, 1,1984}, SignedTX1),
		ar_http_iface_client:send_new_tx({127, 0, 0, 1,1984}, SignedTX2),
		% Wait for pending blocks.
		{ok, PendingTXs} = ar_util:do_until(
			fun() ->
				{ok, {{<<"200">>, _}, _, Body, _, _}} =
					ar_httpc:request(
						<<"GET">>,
						{127, 0, 0, 1, 1984},
						"/tx/pending",
						[]
					),
				PendingTXs = ar_serialize:dejsonify(Body),
				case length(PendingTXs) of
					2 -> {ok, PendingTXs};
					_ -> false
				end
			end,
			1000,
			45000
		),
		2 = length(PendingTXs)
	end}.

%% @doc Spawn a network with two nodes and a chirper server.
get_tx_by_tag_test() ->
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

get_txs_by_send_recv_test_() ->
	{timeout, 60, fun() ->
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
			))
	end}.

%	Node = ar_node:start([], B0),
%	ar_http_iface_server:reregister(Node),
%	ar_node:mine(Node),
%	receive after 500 -> ok end,
%	[B1|_] = ar_node:get_blocks(Node),
%	Enc0 = ar_http_iface_client:get_encrypted_full_block({127, 0, 0, 1, 1984}, (hd(B0))#block.indep_hash),
%	ar_storage:write_encrypted_block((hd(B0))#block.indep_hash, Enc0),
%	ar_cleanup:remove_invalid_blocks([B1]),
%	send_new_block(
%		{127, 0, 0, 1, 1984},
%		hd(B0),
%		hd(B0)
%	),
%	receive after 1000 -> ok end,
%	ar_node:mine(Node).
	% ar_node:add_peers(Node, Bridge),
	% receive after 200 -> ok end,
	% ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA1">>)),
	% receive after 200 -> ok end,
	% ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX1 = ar_tx:new(<<"DATA2">>)),
	% receive after 200 -> ok end,
	% ar_node:mine(Node),
	% receive after 200 -> ok end,
	% [B1|_] = ar_node:get_blocks(Node),
	% B2 = ar_http_iface_client:get_block({127, 0, 0, 1, 1984}, B1),
	% ar:d(get_encrypted_full_block({127, 0, 0, 1, 1984}, B2#block.indep_hash)),
	% B2 = ar_http_iface_client:get_block({127, 0, 0, 1, 1984}, B1),
	% B3 = ar_http_iface_client:get_full_block({127, 0, 0, 1, 1984}, B1),
	% B3 = B2#block {txs = [TX, TX1]},


% get_encrypted_block_test() ->
%	ar_storage:clear(),
%	[B0] = ar_weave:init([]),
%	Node1 = ar_node:start([], [B0]),
%	ar_http_iface_server:reregister(Node1),
%	receive after 200 -> ok end,
%	Enc0 = ar_http_iface_client:get_encrypted_block({127, 0, 0, 1, 1984}, B0#block.indep_hash),
%	ar_storage:write_encrypted_block(B0#block.indep_hash, Enc0),
%	ar_cleanup:remove_invalid_blocks([]),
%	ar_http_iface_client:send_new_block(
%		{127, 0, 0, 1, 1984},
%		B0,
%		B0
%	),
%	receive after 500 -> ok end,
%	B0 = ar_node:get_current_block(whereis(http_entrypoint_node)),
%	ar_node:mine(Node1).

% get_encrypted_full_block_test() ->
%	ar_storage:clear(),
%	  B0 = ar_weave:init([]),
%	  ar_storage:write_block(B0),
%	TX = ar_tx:new(<<"DATA1">>),
%	TX1 = ar_tx:new(<<"DATA2">>),
%	ar_storage:write_tx([TX, TX1]),
%	Node = ar_node:start([], B0),
%	ar_http_iface_server:reregister(Node),
%	ar_node:mine(Node),
%	receive after 500 -> ok end,
%	[B1|_] = ar_node:get_blocks(Node),
%	Enc0 = ar_http_iface_client:get_encrypted_full_block({127, 0, 0, 1, 1984}, (hd(B0))#block.indep_hash),
%	ar_storage:write_encrypted_block((hd(B0))#block.indep_hash, Enc0),
%	ar_cleanup:remove_invalid_blocks([B1]),
%	ar_http_iface_client:send_new_block(
%		{127, 0, 0, 1, 1984},
%		hd(B0),
%		hd(B0)
%	),
%	receive after 1000 -> ok end,
%	ar_node:mine(Node).
	% ar_node:add_peers(Node, Bridge),
	% receive after 200 -> ok end,
	% ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA1">>)),
	% receive after 200 -> ok end,
	% ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX1 = ar_tx:new(<<"DATA2">>)),
	% receive after 200 -> ok end,
	% ar_node:mine(Node),
	% receive after 200 -> ok end,
	% [B1|_] = ar_node:get_blocks(Node),
	% B2 = ar_http_iface_client:get_block({127, 0, 0, 1, 1984}, B1),
	% ar:d(get_encrypted_full_block({127, 0, 0, 1, 1984}, B2#block.indep_hash)),
	% B2 = ar_http_iface_client:get_block({127, 0, 0, 1, 1984}, B1),
	% B3 = ar_http_iface_client:get_full_block({127, 0, 0, 1, 1984}, B1),
	% B3 = B2#block {txs = [TX, TX1]},

%%%
%%% EOF
%%%
