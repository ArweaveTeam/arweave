%%%
%%% @doc Tests for ar_http_iface_* modules.
%%%

-module(ar_http_iface_tests).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

fork_1_8_test_() ->
	{
		foreach,
		fun() ->
			meck:new(ar_fork, [passthrough]),
			meck:expect(ar_fork, height_1_8, fun() -> 0 end),
			ok
		end,
		fun(ok) ->
			meck:unload(ar_fork)
		end,
		[
			%% Tests that run for both before and after fork 1.7
			{"get_last_tx_single_test", fun get_last_tx_single_test/0},
			{"add_external_tx_test", fun add_external_tx_test/0},
			{"add_external_tx_with_tags_test", fun add_external_tx_with_tags_test/0},
			{"find_external_tx_test", fun find_external_tx_test/0},
			{"fail_external_tx_test", fun fail_external_tx_test/0},
			{"add_tx_and_get_last_test", fun add_tx_and_get_last_test/0},
			{"get_subfields_of_tx_test", fun get_subfields_of_tx_test/0},
			{"get_pending_tx_test", fun get_pending_tx_test/0},
			{"get_multiple_pending_txs_test_", get_multiple_pending_txs_test_()},
			{"get_tx_by_tag_test", fun get_tx_by_tag_test/0},
			{"get_tx_body_test", fun get_tx_body_test/0},
			{"get_txs_by_send_recv_test_", get_txs_by_send_recv_test_()}
		]
	}.

%% @doc Ensure that server info can be retreived via the HTTP interface.
get_info_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node1),
	BridgeNode = ar_bridge:start([], [], ?DEFAULT_HTTP_IFACE_PORT),
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
	Height = 0,
	ExpectedPrice = ar_tx:calculate_min_tx_cost(1000, B0#block.diff - 1, Height),
	ExpectedPrice = ar_http_iface_client:get_tx_reward({127, 0, 0, 1, 1984}, 1000).

%% @doc Ensure that objects are only re-gossiped once.
single_regossip_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		[B0] = ar_weave:init([]),
		Node1 = ar_node:start([], [B0]),
		ar_http_iface_server:reregister(Node1),
		TX = ar_tx:new(),
		Responses =
			lists:map(
				fun(_) ->
					timer:sleep(rand:uniform(100)),
					ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX)
				end,
				lists:seq(1, 10)
			),
		?assertEqual(1, length([ processed || {ok, {{<<"200">>, _}, _, _, _, _}} <- Responses ]))
	end}.

%% @doc Unjoined nodes should not accept blocks
post_block_to_unjoined_node_test() ->
	JB = ar_serialize:jsonify({[{foo, [<<"bing">>, 2.3, true]}]}),
	{ok, {RespTup, _, Body, _, _}} =
		ar_httpc:request(<<"POST">>, {127, 0, 0, 1, 1984}, "/block/", [], JB),
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
	node_blacklisting_test_frame(
		RequestFun,
		ErrorResponse,
		(ar_meta_db:get(requests_per_minute_limit) div 2)+ 1,
		1
	).

%% @doc Test that nodes sending too many requests are temporarily blocked: (b) POST.
-spec node_blacklisting_post_spammer_test() -> ok.
node_blacklisting_post_spammer_test() ->
	{RequestFun, ErrorResponse} = get_fun_msg_pair(send_new_tx),
	NErrors = 11,
	NRequests = (ar_meta_db:get(requests_per_minute_limit) div 2) + NErrors,
	node_blacklisting_test_frame(RequestFun, ErrorResponse, NRequests, NErrors).

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
%% TODO: Perform the requests in parallel. Just changing the lists:map/2 call
%% to an ar_util:pmap/2 call fails the tests currently.
-spec node_blacklisting_test_frame(fun(), any(), non_neg_integer(), non_neg_integer()) -> ok.
node_blacklisting_test_frame(RequestFun, ErrorResponse, NRequests, ExpectedErrors) ->
	ar_blacklist_middleware:reset(),
	Responses = lists:map(RequestFun, lists:seq(1, NRequests)),
	?assertEqual(length(Responses), NRequests),
	ar_blacklist_middleware:reset(),
	ByResponseType = count_by_response_type(ErrorResponse, Responses),
	Expected = #{
		error_responses => ExpectedErrors,
		ok_responses => NRequests - ExpectedErrors
	},
	?assertEqual(Expected, ByResponseType).

%% @doc Count the number of successful and error responses.
count_by_response_type(ErrorResponse, Responses) ->
	count_by(
		fun
			(Response) when Response == ErrorResponse -> error_responses;
			(_) -> ok_responses
		end,
		Responses
	).

%% @doc Count the occurances in the list based on the predicate.
count_by(Pred, List) ->
	maps:map(fun (_, Value) -> length(Value) end, group(Pred, List)).

%% @doc Group the list based on the key generated by Grouper.
group(Grouper, Values) ->
	group(Grouper, Values, maps:new()).

group(_, [], Acc) ->
	Acc;
group(Grouper, [Item | List], Acc) ->
	Key = Grouper(Item),
	Updater = fun (Old) -> [Item | Old] end,
	NewAcc = maps:update_with(Key, Updater, [Item], Acc),
	group(Grouper, List, NewAcc).

%% @doc Ensure that server info can be retreived via the HTTP interface.
get_unjoined_info_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start([]),
	ar_http_iface_server:reregister(Node1),
	BridgeNode = ar_bridge:start([], [], ?DEFAULT_HTTP_IFACE_PORT),
	ar_http_iface_server:reregister(http_bridge_node, BridgeNode),
	?assertEqual(<<?NETWORK_NAME>>, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, name)),
	?assertEqual(?CLIENT_VERSION, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, version)),
	?assertEqual(0, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, peers)),
	?assertEqual(0, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, blocks)),
	?assertEqual(-1, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, height)).

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
			"/wallet/"++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/balance"
		),
	?assertEqual(10000, binary_to_integer(Body)).

%% @doc Test that heights are returned correctly.
get_height_test() ->
	ar_storage:clear(),
	B0 = ar_weave:init([], ?DEFAULT_DIFF, ?AR(1)),
	Node1 = ar_node:start([self()], B0),
	ar_http_iface_server:reregister(Node1),
	0 = ar_http_iface_client:get_height({127,0,0,1,1984}),
	ar_node:mine(Node1),
	timer:sleep(1000),
	1 = ar_http_iface_client:get_height({127,0,0,1,1984}).

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
			"/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/balance"
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
			"/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/last_tx"
		),
	?assertEqual(<<"TEST_ID">>, ar_util:decode(Body)).

%% @doc Check that we can qickly get the local time from the peer.
get_time_test() ->
	Now = os:system_time(second),
	{ok, {Min, Max}} = ar_http_iface_client:get_time({127, 0, 0, 1, 1984}, 10 * 1000),
	?assert(Min < Now),
	?assert(Now < Max).

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

%% @doc Test that the various different methods of GETing a block all perform
%% correctly if the block cannot be found.
get_non_existent_block_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	Node1 = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node1),
	{ok, {{<<"404">>, _}, _, _, _, _}}
		= ar_httpc:request(<<"GET">>, {127, 0, 0, 1, 1984}, "/block/height/100"),
	{ok, {{<<"404">>, _}, _, _, _, _}}
		= ar_httpc:request(<<"GET">>, {127, 0, 0, 1, 1984}, "/block/hash/abcd"),
	{ok, {{<<"404">>, _}, _, _, _, _}}
		= ar_httpc:request(<<"GET">>, {127, 0, 0, 1, 1984}, "/block/height/101/wallet_list"),
	{ok, {{<<"404">>, _}, _, _, _, _}}
		= ar_httpc:request(<<"GET">>, {127, 0, 0, 1, 1984}, "/block/hash/abcd/wallet_list"),
	{ok, {{<<"404">>, _}, _, _, _, _}}
		= ar_httpc:request(<<"GET">>, {127, 0, 0, 1, 1984}, "/block/height/101/hash_list"),
	{ok, {{<<"404">>, _}, _, _, _, _}}
		= ar_httpc:request(<<"GET">>, {127, 0, 0, 1, 1984}, "/block/hash/abcd/hash_list").

%% @doc Test adding transactions to a block.
add_external_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	Bridge = ar_bridge:start([], Node, ?DEFAULT_HTTP_IFACE_PORT),
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
	Bridge = ar_bridge:start([], Node, ?DEFAULT_HTTP_IFACE_PORT),
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
	Bridge = ar_bridge:start([], Node, ?DEFAULT_HTTP_IFACE_PORT),
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
	Bridge = ar_bridge:start([], Node, ?DEFAULT_HTTP_IFACE_PORT),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
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
		Bridge = ar_bridge:start([], Node1, ?DEFAULT_HTTP_IFACE_PORT),
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
		send_new_block(
			{127, 0, 0, 1, 1984},
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
	{timeout, 20, fun() ->
		Setup = fun() ->
			ar_storage:clear(),
			ar_blacklist_middleware:reset(),
			[B0] = ar_weave:init([]),
			BHL0 = [B0#block.indep_hash],
			NodeWithBridge = ar_node:start([], [B0]),
			Bridge = ar_bridge:start([], NodeWithBridge, ?DEFAULT_HTTP_IFACE_PORT),
			OtherNode = ar_node:start([], [B0]),
			timer:sleep(500),
			ar_http_iface_server:reregister(http_bridge_node, Bridge),
			ar_http_iface_server:reregister(http_entrypoint_node, NodeWithBridge),
			{BHL0, {NodeWithBridge, {127, 0, 0, 1, 1984}}, OtherNode}
		end,
		BlocksFromStorage = fun(BHL) ->
			B = ar_storage:read_block(hd(BHL), BHL),
			RecallB = ar_node_utils:find_recall_block(BHL),
			{B, RecallB}
		end,
		{BHL0, {RemoteNode, RemotePeer}, LocalNode} = Setup(),
		BHL1 = mine_one_block(LocalNode, BHL0),
		?assertMatch(BHL0, ar_node:get_blocks(RemoteNode)),
		{_, RecallB0} = BlocksFromStorage(BHL0),
		{B1, _} = BlocksFromStorage(BHL1),
		?assertMatch(
			{ok, {{<<"200">>, _}, _, _, _, _}},
			send_new_block(
				RemotePeer,
				B1,
				RecallB0
			)
		),
		%% Try to post the same block again
		?assertMatch(
			{ok, {{<<"208">>, _}, _, <<"Block Data Segment already processed.">>, _, _}},
			send_new_block(RemotePeer, B1, RecallB0)
		),
		%% Try to post the same block again, but with a different data segment
		?assertMatch(
			{ok, {{<<"208">>, _}, _, <<"Block already processed.">>, _, _}},
			send_new_block(
				RemotePeer,
				B1,
				RecallB0,
				add_rand_suffix(<<"other-block-data-segment">>)
			)
		),
		%% Try to post an invalid data segment. This triggers a ban in ar_blacklist_middleware.
		?assertMatch(
			{ok, {{<<"400">>, _}, _, <<"Invalid Block Proof of Work">>, _, _}},
			send_new_block(
				RemotePeer,
				B1#block{indep_hash = add_rand_suffix(<<"new-hash">>)},
				RecallB0,
				add_rand_suffix(<<"bad-block-data-segment">>)
			)
		),
		%% Verify the IP address of self is banned in ar_blacklist_middleware.
		?assertMatch(
			{ok, {{<<"403">>, _}, _, <<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(
				RemotePeer,
				B1#block{indep_hash = add_rand_suffix(<<"new-hash-again">>)},
				RecallB0,
				add_rand_suffix(<<"bad-block-data-segment">>)
			)
		),
		ar_blacklist_middleware:reset()
	end}.

add_external_block_with_invalid_timestamp_test() ->
	Setup = fun() ->
		ar_storage:clear(),
		ar_blacklist_middleware:reset(),
		[B0] = ar_weave:init([]),
		BHL0 = [B0#block.indep_hash],
		NodeWithBridge = ar_node:start([], [B0]),
		Bridge = ar_bridge:start([], NodeWithBridge, ?DEFAULT_HTTP_IFACE_PORT),
		OtherNode = ar_node:start([], [B0]),
		timer:sleep(500),
		ar_http_iface_server:reregister(http_bridge_node, Bridge),
		ar_http_iface_server:reregister(http_entrypoint_node, NodeWithBridge),
		{BHL0, {127, 0, 0, 1, 1984}, OtherNode}
	end,
	{BHL0, RemotePeer, LocalNode} = Setup(),
	BHL1 = mine_one_block(LocalNode, BHL0),
	B1 = ar_storage:read_block(hd(BHL1), BHL1),
	RecallB0 = ar_node_utils:find_recall_block(BHL0),
	%% Expect the timestamp too far from the future to be rejected
	FutureTimestampTolerance = ?JOIN_CLOCK_TOLERANCE * 2 + ?CLOCK_DRIFT_MAX,
	TooFarFutureTimestamp = os:system_time(second) + FutureTimestampTolerance + 3,
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"Invalid timestamp.">>, _, _}},
		send_new_block(
			RemotePeer,
			update_block(B1#block {
				indep_hash = add_rand_suffix(<<"random-hash">>),
				timestamp = TooFarFutureTimestamp
			}, RecallB0),
			RecallB0
		)
	),
	%% Expect the timestamp from the future within the tolerance interval to be accepted
	OkFutureTimestamp = os:system_time(second) + FutureTimestampTolerance - 3,
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		send_new_block(
			RemotePeer,
			update_block(B1#block {
				indep_hash = add_rand_suffix(<<"random-hash">>),
				timestamp = OkFutureTimestamp
			}, RecallB0),
			RecallB0
		)
	),
	%% Expect the timestamp far from the past to be rejected
	PastTimestampTolerance = lists:sum([
		?JOIN_CLOCK_TOLERANCE * 2,
		?CLOCK_DRIFT_MAX,
		?MINING_TIMESTAMP_REFRESH_INTERVAL,
		?MAX_BLOCK_PROPAGATION_TIME
	]),
	TooFarPastTimestamp = os:system_time(second) - PastTimestampTolerance - 3,
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"Invalid timestamp.">>, _, _}},
		send_new_block(
			RemotePeer,
			update_block(B1#block {
				indep_hash = add_rand_suffix(<<"random-hash">>),
				timestamp = TooFarPastTimestamp
			}, RecallB0),
			RecallB0
		)
	),
	%% Expect the block with a timestamp from the past within the tolerance interval to be accepted
	OkPastTimestamp = os:system_time(second) - PastTimestampTolerance + 3,
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		send_new_block(
			RemotePeer,
			update_block(B1#block { timestamp = OkPastTimestamp}, RecallB0),
			RecallB0
		)
	).

add_rand_suffix(Bin) ->
	Suffix = ar_util:encode(crypto:strong_rand_bytes(6)),
	iolist_to_binary([Bin, " - ", Suffix]).

%% @doc Ensure that blocks with tx can be added to a network from outside
%% a single node.
add_external_block_with_tx_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		ar_blacklist_middleware:reset(),
		[BGen] = ar_weave:init([]),
		Node1 = ar_node:start([], [BGen]),
		timer:sleep(500),
		Bridge = ar_bridge:start([], Node1, ?DEFAULT_HTTP_IFACE_PORT),
		ar_node:add_peers(Node1, Bridge),
		% Start node 2, add transaction, and wait until mined.
		Node2 = ar_node:start([], [BGen]),
		ar_http_iface_server:reregister(http_entrypoint_node, Node1),
		ar_http_iface_server:reregister(http_bridge_node, Bridge),
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
		?assertMatch(
			{ok, {{<<"200">>, _}, _, _, _, _}},
			send_new_block(
				{127, 0, 0, 1, 1984},
				ar_storage:read_block(BTest, ar_node:get_hash_list(Node2)),
				BGen
			)
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

mine_illicit_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node = ar_node:start([], [B0]),
	TX = ar_tx:new(<<"BADCONTENT1">>),
	ar_node:add_tx(Node, TX),
	timer:sleep(500),
	ar_meta_db:put(content_policy_files, []),
	ar_firewall:reload(),
	ar_node:mine(Node),
	timer:sleep(500),
	?assertEqual(<<"BADCONTENT1">>, (ar_storage:read_tx(TX#tx.id))#tx.data),
	FilteredTX = ar_tx:new(<<"BADCONTENT1">>),
	ar_node:add_tx(Node, FilteredTX),
	timer:sleep(500),
	ar_meta_db:put(content_policy_files, ["test/test_sig.txt"]),
	ar_firewall:reload(),
	ar_node:mine(Node),
	timer:sleep(500),
	?assertEqual(unavailable, ar_storage:read_tx(FilteredTX#tx.id)).

%% @doc Ensure that blocks can be added to a network from outside
%% a single node.
fork_recover_by_http_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	Bridge = ar_bridge:start([], Node1, ?DEFAULT_HTTP_IFACE_PORT),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node1, Bridge),
	Node2 = ar_node:start([], [B0]),
	timer:sleep(500),
	ar_http_iface_server:reregister(Node1),
	BHL0 = [B0#block.indep_hash],
	FullBHL = mine_n_blocks(Node2, BHL0, 10),
	%% Send only the latest block to Node1 and let it fork recover up to it.
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		send_new_block(
			{127, 0, 0, 1, 1984},
			ar_storage:read_block(hd(FullBHL), FullBHL)
		)
	),
	ar_test_node:wait_until_block_hash_list(Node1, FullBHL).

%% @doc Post a tx to the network and ensure that last_tx call returns the ID of last tx.
add_tx_and_get_last_test() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	Bridge = ar_bridge:start([], Node, ?DEFAULT_HTTP_IFACE_PORT),
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
			"/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/last_tx"
		),
	?assertEqual(ID, ar_util:decode(Body)).

%% @doc Post a tx to the network and ensure that its subfields can be gathered
get_subfields_of_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	Bridge = ar_bridge:start([], Node, ?DEFAULT_HTTP_IFACE_PORT),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA">>)),
	timer:sleep(1000),
	ar_node:mine(Node),
	timer:sleep(1000),
	%write a get_tx function like get_block
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, 1984},
			"/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)) ++ "/data"
		),
	Orig = TX#tx.data,
	?assertEqual(Orig, ar_util:decode(Body)).

%% @doc Correctly check the status of pending is returned for a pending transaction
get_pending_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	Bridge = ar_bridge:start([], Node, ?DEFAULT_HTTP_IFACE_PORT),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	io:format("~p\n",[
		ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA1">>))
		]),
	timer:sleep(1000),
	%write a get_tx function like get_block
	{ok, {{<<"202">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, 1984},
			"/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id))
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
		Bridge = ar_bridge:start([], [Node], ?DEFAULT_HTTP_IFACE_PORT),
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
						"/tx/pending"
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
	Peers = ar_network:start(10, 10),
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
			[],
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

%% @doc Mine a transaction into a block and retrieve it's binary body via HTTP.
get_tx_body_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	Bridge = ar_bridge:start([], Node, ?DEFAULT_HTTP_IFACE_PORT),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	TX = ar_tx:new(<<"TEST DATA">>),
	ar:d(byte_size(ar_util:encode(TX#tx.id))),
	% Add tx to network
	ar_node:add_tx(Node, TX),
	% Begin mining
	receive after 250 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	?assertEqual(<<"TEST DATA">>, ar_http_iface_client:get_tx_data({127,0,0,1,1984}, TX#tx.id)).

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
					{'or',
						{'equals',
							<<"to">>,
							ar_util:encode(TX#tx.target)},
						{'equals',
							<<"from">>,
							ar_util:encode(TX#tx.target)}
					}
				)
			),
		{ok, {_, _, Res, _, _}} =
			ar_httpc:request(
				<<"POST">>,
				{127, 0, 0, 1, 1984},
				"/arql",
				[],
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

get_tx_status_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node = ar_node:start([], [B0]),
	TX = (ar_tx:new())#tx {tags = [{<<"TestName">>, <<"TestVal">>}]},
	ar_node:add_tx(Node, TX),
	receive after 250 -> ok end,
	FetchStatus = fun() ->
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, 1984},
			"/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)) ++ "/status"
		)
	end,
	?assertMatch({ok, {{<<"202">>, _}, _, <<"Pending">>, _, _}}, FetchStatus()),
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	{ok, {{<<"200">>, _}, _, Body, _, _}} = FetchStatus(),
	{Res} = ar_serialize:dejsonify(Body),
	HashList = ar_node:get_hash_list(Node),
	?assertEqual(
		#{
			<<"block_height">> => length(HashList) - 1,
			<<"block_indep_hash">> => ar_util:encode(hd(HashList)),
			<<"number_of_confirmations">> => 1
		},
		maps:from_list(Res)
	),
	% mine yet another block and assert the increment
	receive after 250 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	{ok, {{<<"200">>, _}, _, Body2, _, _}} = FetchStatus(),
	{Res2} = ar_serialize:dejsonify(Body2),
	?assertEqual(
		#{
			<<"block_height">> => length(HashList) - 1,
			<<"block_indep_hash">> => ar_util:encode(hd(HashList)),
			<<"number_of_confirmations">> => 2
		},
		maps:from_list(Res2)
	),
	%% Emulate fork recovering to a fork where the TX doesn't exist
	[ForkB0] = ar_weave:init([]),
	Node ! {replace_block_list, [ForkB0]},
	receive after 50 -> ok end,
	?assertMatch({ok, {{<<"404">>, _}, _, _, _, _}}, FetchStatus()).

post_unsigned_tx_test_() ->
	{timeout, 20, fun post_unsigned_tx/0}.

post_unsigned_tx() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(Node),
	Bridge = ar_bridge:start([], Node, ?DEFAULT_HTTP_IFACE_PORT),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	% generate a wallet and receive a wallet access code
	{ok, {{<<"421">>, _}, _, _, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			{127, 0, 0, 1, 1984},
			"/wallet",
			[],
			<<>>
		),
	ar_meta_db:put(internal_api_secret, <<"correct_secret">>),
	{ok, {{<<"421">>, _}, _, _, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			{127, 0, 0, 1, 1984},
			"/wallet",
			[{<<"X-Internal-Api-Secret">>, <<"incorrect_secret">>}],
			<<>>
		),
	{ok, {{<<"200">>, <<"OK">>}, _, CreateWalletBody, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			{127, 0, 0, 1, 1984},
			"/wallet",
			[{<<"X-Internal-Api-Secret">>, <<"correct_secret">>}],
			<<>>
		),
	ar_meta_db:put(internal_api_secret, not_set),
	{CreateWalletRes} = ar_serialize:dejsonify(CreateWalletBody),
	[WalletAccessCode] = proplists:get_all_values(<<"wallet_access_code">>, CreateWalletRes),
	?assertMatch([_], proplists:get_all_values(<<"wallet_address">>, CreateWalletRes)),
	% send an unsigned transaction to be signed with the generated key
	TX = (ar_tx:new())#tx{reward = ?AR(1)},
	UnsignedTXProps = [
		{<<"last_tx">>, TX#tx.last_tx},
		{<<"target">>, TX#tx.target},
		{<<"quantity">>, integer_to_binary(TX#tx.quantity)},
		{<<"data">>, TX#tx.data},
		{<<"reward">>, integer_to_binary(TX#tx.reward)},
		{<<"wallet_access_code">>, WalletAccessCode}
	],
	{ok, {{<<"421">>, _}, _, _, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			{127, 0, 0, 1, 1984},
			"/unsigned_tx",
			[],
			ar_serialize:jsonify({UnsignedTXProps})
		),
	ar_meta_db:put(internal_api_secret, <<"correct_secret">>),
	{ok, {{<<"421">>, _}, _, _, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			{127, 0, 0, 1, 1984},
			"/unsigned_tx",
			[{<<"X-Internal-Api-Secret">>, <<"incorrect_secret">>}],
			ar_serialize:jsonify({UnsignedTXProps})
		),
	{ok, {{<<"200">>, <<"OK">>}, _, Body, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			{127, 0, 0, 1, 1984},
			"/unsigned_tx",
			[{<<"X-Internal-Api-Secret">>, <<"correct_secret">>}],
			ar_serialize:jsonify({UnsignedTXProps})
		),
	ar_meta_db:put(internal_api_secret, not_set),
	{Res} = ar_serialize:dejsonify(Body),
	TXID = proplists:get_value(<<"id">>, Res),
	% mine it into a block
	receive after 250 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	% expect the transaction to be successfully mined
	{ok, {_, _, GetTXBody, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, 1984},
			"/tx/" ++ binary_to_list(TXID) ++ "/status"
		),
	{GetTXRes} = ar_serialize:dejsonify(GetTXBody),
	?assertMatch(
		#{
			<<"number_of_confirmations">> := 1
		},
		maps:from_list(GetTXRes)
	).

get_wallet_txs_test_() ->
	{timeout, 10, fun() ->
		ar_storage:clear(),
		%% Create a wallet
		{_, Pub} = ar_wallet:new(),
		WalletAddress = binary_to_list(ar_util:encode(ar_wallet:to_address(Pub))),
		[B0] = ar_weave:init([{ar_wallet:to_address(Pub), 10000, <<>>}]),
		Node = ar_node:start([], [B0]),
		ar_http_iface_server:reregister(Node),
		Bridge = ar_bridge:start([], Node, ?DEFAULT_HTTP_IFACE_PORT),
		ar_http_iface_server:reregister(http_bridge_node, Bridge),
		ar_node:add_peers(Node, Bridge),
		{ok, {{<<"200">>, <<"OK">>}, _, Body, _, _}} =
			ar_httpc:request(
				<<"GET">>,
				{127, 0, 0, 1, 1984},
				"/wallet/" ++ WalletAddress ++ "/txs"
			),
		TXs = ar_serialize:dejsonify(Body),
		%% Expect the wallet to have no transactions
		?assertEqual([], TXs),
		%% Sign and post a transaction and expect it to appear in the wallet list
		TX = (ar_tx:new())#tx{ owner = Pub },
		{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
			ar_httpc:request(
				<<"POST">>,
				{127, 0, 0, 1, 1984},
				"/tx",
				[],
				ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
			),
		receive after 250 -> ok end,
		ar_node:mine(Node),
		receive after 1000 -> ok end,
		{ok, {{<<"200">>, <<"OK">>}, _, GetOneTXBody, _, _}} =
			ar_httpc:request(
				<<"GET">>,
				{127, 0, 0, 1, 1984},
				"/wallet/" ++ WalletAddress ++ "/txs"
			),
		OneTX = ar_serialize:dejsonify(GetOneTXBody),
		?assertEqual([ar_util:encode(TX#tx.id)], OneTX),
		%% Expect the same when the TX is specified as the earliest TX
		{ok, {{<<"200">>, <<"OK">>}, _, GetOneTXAgainBody, _, _}} =
			ar_httpc:request(
				<<"GET">>,
				{127, 0, 0, 1, 1984},
				"/wallet/" ++ WalletAddress ++ "/txs/" ++ binary_to_list(ar_util:encode(TX#tx.id))
			),
		OneTXAgain = ar_serialize:dejsonify(GetOneTXAgainBody),
		?assertEqual([ar_util:encode(TX#tx.id)], OneTXAgain),
		%% Add one more TX and expect it to be appended to the wallet list
		SecondTX = (ar_tx:new())#tx{ owner = Pub, last_tx = TX#tx.id },
		{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
			ar_httpc:request(
				<<"POST">>,
				{127, 0, 0, 1, 1984},
				"/tx",
				[],
				ar_serialize:jsonify(ar_serialize:tx_to_json_struct(SecondTX))
			),
		receive after 250 -> ok end,
		ar_node:mine(Node),
		receive after 1000 -> ok end,
		{ok, {{<<"200">>, <<"OK">>}, _, GetTwoTXsBody, _, _}} =
			ar_httpc:request(
				<<"GET">>,
				{127, 0, 0, 1, 1984},
				"/wallet/" ++ WalletAddress ++ "/txs"
			),
		Expected = [ar_util:encode(SecondTX#tx.id), ar_util:encode(TX#tx.id)],
		?assertEqual(Expected, ar_serialize:dejsonify(GetTwoTXsBody)),
		%% Specify the second TX as the earliest and expect the first one to be excluded
		{ok, {{<<"200">>, <<"OK">>}, _, GetSecondTXBody, _, _}} =
			ar_httpc:request(
				<<"GET">>,
				{127, 0, 0, 1, 1984},
				"/wallet/" ++ WalletAddress ++ "/txs/" ++ binary_to_list(ar_util:encode(SecondTX#tx.id))
			),
		OneSecondTX = ar_serialize:dejsonify(GetSecondTXBody),
		?assertEqual([ar_util:encode(SecondTX#tx.id)], OneSecondTX)
	end}.

get_wallet_deposits_test_() ->
	{timeout, 10, fun() ->
		ar_storage:clear(),
		%% Create a wallet to transfer tokens to
		{_, PubTo} = ar_wallet:new(),
		WalletAddressTo = binary_to_list(ar_util:encode(ar_wallet:to_address(PubTo))),
		%% Create a wallet to transfer tokens from
		{_, PubFrom} = ar_wallet:new(),
		[B0] = ar_weave:init([
			{ar_wallet:to_address(PubTo), 0, <<>>},
			{ar_wallet:to_address(PubFrom), 200, <<>>}
		]),
		Node = ar_node:start([], [B0]),
		ar_http_iface_server:reregister(Node),
		Bridge = ar_bridge:start([], Node, ?DEFAULT_HTTP_IFACE_PORT),
		ar_http_iface_server:reregister(http_bridge_node, Bridge),
		ar_node:add_peers(Node, Bridge),
		GetTXs = fun(EarliestDeposit) ->
			BasePath = "/wallet/" ++ WalletAddressTo ++ "/deposits",
			Path = 	BasePath ++ "/" ++ EarliestDeposit,
			{ok, {{<<"200">>, <<"OK">>}, _, Body, _, _}} =
				ar_httpc:request(
					<<"GET">>,
					{127, 0, 0, 1, 1984},
					Path
				),
			ar_serialize:dejsonify(Body)
		end,
		%% Expect the wallet to have no incoming transfers
		?assertEqual([], GetTXs("")),
		%% Send some Winston to WalletAddressTo
		FirstTX = (ar_tx:new())#tx{
			owner = PubFrom,
			target = ar_wallet:to_address(PubTo),
			quantity = 100
		},
		PostTX = fun(T) ->
			{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
				ar_httpc:request(
					<<"POST">>,
					{127, 0, 0, 1, 1984},
					"/tx",
					[],
					ar_serialize:jsonify(ar_serialize:tx_to_json_struct(T))
				)
		end,
		PostTX(FirstTX),
		receive after 250 -> ok end,
		ar_node:mine(Node),
		receive after 1000 -> ok end,
		%% Expect the endpoint to report the received transfer
		?assertEqual([ar_util:encode(FirstTX#tx.id)], GetTXs("")),
		%% Send some more Winston to WalletAddressTo
		SecondTX = (ar_tx:new())#tx{
			owner = PubFrom,
			target = ar_wallet:to_address(PubTo),
			last_tx = FirstTX#tx.id,
			quantity = 100
		},
		PostTX(SecondTX),
		receive after 250 -> ok end,
		ar_node:mine(Node),
		receive after 1000 -> ok end,
		%% Expect the endpoint to report the received transfer
		?assertEqual(
			[ar_util:encode(SecondTX#tx.id), ar_util:encode(FirstTX#tx.id)],
			GetTXs("")
		),
		%% Specify the first tx as the earliest, still expect to get both txs
		?assertEqual(
			[ar_util:encode(SecondTX#tx.id), ar_util:encode(FirstTX#tx.id)],
			GetTXs(ar_util:encode(FirstTX#tx.id))
		),
		%% Specify the second tx as the earliest, expect to get only it
		?assertEqual(
			[ar_util:encode(SecondTX#tx.id)],
			GetTXs(ar_util:encode(SecondTX#tx.id))
		)
	end}.

%% The test ensures that if a node has multiple txs per wallet in the mempool and
%% receives a new block, it preserves those txs in the mempool and later mines them
%% into another block successfully. Runs after fork 1.7.
accepts_blocks_with_multiple_txs_per_wallet() ->
	ar_storage:clear(),
	{Priv, Pub} = ar_wallet:new(),
	%% Create a node and fill its mempool with 2 txs from the same wallet
	B0 = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(100), <<>>}]),
	NodeA = ar_node:start([], B0),
	TX = (ar_tx:new())#tx{ owner = Pub, reward = ?AR(1) },
	SignedTX = ar_tx:sign(TX, {Priv, Pub}),
	ar_node:add_tx(NodeA, SignedTX),
	TX2 = (ar_tx:new())#tx{ owner = Pub, reward = ?AR(1), last_tx = SignedTX#tx.id },
	SignedTX2 = ar_tx:sign(TX2, {Priv, Pub}),
	ar_node:add_tx(NodeA, SignedTX2),
	%% Wait a little bit so that when we connect the nodes, NodeB does not receive the txs
	receive after 1000 -> ok end,
	%% Create another node, mine an empty block on it, connect it with NodeA to receive it
	NodeB = ar_node:start([], B0),
	ar_node:add_peers(NodeB, NodeA),
	ar_node:mine(NodeB),
	receive after 1000 -> ok end,
	%% Mine a new block on NodeA - expect the original transactions to be mined into it
	ar_node:mine(NodeA),
	receive after 1000 -> ok end,
	ReadTX = ar_storage:read_tx(SignedTX#tx.id),
	?assertEqual(<<>>, ReadTX#tx.last_tx),
	ReadTX2 = ar_storage:read_tx(SignedTX2#tx.id),
	?assertEqual(SignedTX#tx.id, ReadTX2#tx.last_tx).

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
%	B0 = ar_weave:init([]),
%	ar_storage:write_block(B0),
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
	% ar:d(ar_http_iface_client:get_encrypted_full_block({127, 0, 0, 1, 1984}, B2#block.indep_hash)),
	% B2 = ar_http_iface_client:get_block({127, 0, 0, 1, 1984}, B1),
	% B3 = ar_http_iface_client:get_full_block({127, 0, 0, 1, 1984}, B1),
	% B3 = B2#block {txs = [TX, TX1]},


%% Utility functions

mine_n_blocks(_, BHL, 0) ->
	BHL;
mine_n_blocks(Node, PreMineBHL, N) ->
	PostMineBHL = mine_one_block(Node, PreMineBHL),
	mine_n_blocks(Node, PostMineBHL, N - 1).

mine_one_block(Node, PreMineBHL) ->
	ar_node:mine(Node),
	PostMineBHL = ar_test_node:wait_until_height(Node, length(PreMineBHL)),
	?assertMatch([_ | PreMineBHL], PostMineBHL),
	PostMineBHL.

send_new_block(Peer, B) ->
	PreviousRecallB = ar_node_utils:find_recall_block(B#block.hash_list),
	?assert(is_record(PreviousRecallB, block)),
	send_new_block(Peer, B, PreviousRecallB).

send_new_block(Peer, B, PreviousRecallB) ->
	send_new_block(
		Peer,
		B,
		PreviousRecallB,
		generate_block_data_segment(B, PreviousRecallB)
	).

send_new_block(Peer, B, PreviousRecallB, BDS) ->
	ar_http_iface_client:send_new_block(
		Peer,
		B,
		BDS,
		{
			PreviousRecallB#block.indep_hash,
			PreviousRecallB#block.block_size,
			<<>>,
			<<>>
		}
	).

generate_block_data_segment(B, PreviousRecallB) ->
	ar_block:generate_block_data_segment(
		ar_storage:read_block(B#block.previous_block, B#block.hash_list),
		PreviousRecallB,
		lists:map(fun ar_storage:read_tx/1, B#block.txs),
		B#block.reward_addr,
		B#block.timestamp,
		B#block.tags
	).

%% Update the nonce, dependent hash and the independen hash.
update_block(B, PreviousRecallB) ->
	update_block(B, PreviousRecallB, 0).

update_block(B, PreviousRecallB, Nonce) ->
	NonceBinary = integer_to_binary(Nonce),
	BDS = generate_block_data_segment(B#block { nonce = NonceBinary }, PreviousRecallB),
	MinDiff = ar_mine:min_difficulty(B#block.height),
	case ar_weave:hash(BDS, NonceBinary, B#block.height) of
		<< 0:MinDiff, _/bitstring >> = DepHash ->
			UpdatedB = B#block {
				hash = DepHash,
				nonce = NonceBinary
			},
			UpdatedB#block { indep_hash = ar_weave:indep_hash(UpdatedB) };
		_ ->
			update_block(B, PreviousRecallB, Nonce + 1)
	end.
