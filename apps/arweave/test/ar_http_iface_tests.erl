-module(ar_http_iface_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start/1, slave_start/1, connect_to_slave/0, get_tx_anchor/0,
		sign_tx/2, post_tx_json_to_master/1, assert_slave_wait_until_receives_txs/1,
		slave_wait_until_height/1, read_block_when_stored/1, master_peer/0]).

addresses_with_checksums_test_() ->
	{timeout, 60, fun test_addresses_with_checksum/0}.

test_addresses_with_checksum() ->
	{_, Pub} = Wallet = ar_wallet:new(),
	{_, Pub2} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(100), <<>>},
			{ar_wallet:to_address(Pub2), ?AR(100), <<>>}]),
	start(B0),
	slave_start(B0),
	connect_to_slave(),
	Address19 = crypto:strong_rand_bytes(19),
	Address65 = crypto:strong_rand_bytes(65),
	Address20 = crypto:strong_rand_bytes(20),
	Address32 = ar_wallet:to_address(Pub2),
	TX = sign_tx(Wallet, #{ last_tx => get_tx_anchor() }),
	{JSON} = ar_serialize:tx_to_json_struct(TX),
	JSON2 = proplists:delete(<<"target">>, JSON),
	TX2 = sign_tx(Wallet, #{ last_tx => get_tx_anchor(), target => Address32 }),
	{JSON3} = ar_serialize:tx_to_json_struct(TX2),
	InvalidPayloads = [
		[{<<"target">>, <<":">>} | JSON2],
		[{<<"target">>, << <<":">>/binary, (ar_util:encode(<< 0:32 >>))/binary >>} | JSON2],
		[{<<"target">>, << (ar_util:encode(Address19))/binary, <<":">>/binary,
				(ar_util:encode(<< (erlang:crc32(Address19)):32 >> ))/binary >>} | JSON2],
		[{<<"target">>, << (ar_util:encode(Address65))/binary, <<":">>/binary,
				(ar_util:encode(<< (erlang:crc32(Address65)):32 >>))/binary >>} | JSON2],
		[{<<"target">>, << (ar_util:encode(Address32))/binary, <<":">>/binary,
				(ar_util:encode(<< 0:32 >>))/binary >>} | JSON2],
		[{<<"target">>, << (ar_util:encode(Address20))/binary, <<":">>/binary,
				(ar_util:encode(<< 1:32 >>))/binary >>} | JSON2],
		[{<<"target">>, << (ar_util:encode(Address32))/binary, <<":">>/binary,
				(ar_util:encode(<< (erlang:crc32(Address32)):32 >>))/binary,
				<<":">>/binary >>} | JSON2],
		[{<<"target">>, << (ar_util:encode(Address32))/binary, <<":">>/binary >>} | JSON3]
	],
	lists:foreach(
		fun(Struct) ->
			Payload = ar_serialize:jsonify({Struct}),
			?assertMatch({ok, {{<<"400">>, _}, _, <<"Invalid JSON.">>, _, _}},
					post_tx_json_to_master(Payload))
		end,
		InvalidPayloads
	),
	ValidPayloads = [
		[{<<"target">>, << (ar_util:encode(Address32))/binary, <<":">>/binary,
				(ar_util:encode(<< (erlang:crc32(Address32)):32 >>))/binary >>} | JSON3],
		JSON
	],
	lists:foreach(
		fun(Struct) ->
			Payload = ar_serialize:jsonify({Struct}),
			?assertMatch({ok, {{<<"200">>, _}, _, <<"OK">>, _, _}},
					post_tx_json_to_master(Payload))
		end,
		ValidPayloads
	),
	assert_slave_wait_until_receives_txs([TX, TX2]),
	ar_node:mine(),
	[{H, _, _} | _] = slave_wait_until_height(1),
	B = read_block_when_stored(H),
	ChecksumAddr = << (ar_util:encode(Address32))/binary, <<":">>/binary,
			(ar_util:encode(<< (erlang:crc32(Address32)):32 >>))/binary >>,
	?assertEqual(2, length(B#block.txs)),
	Balance = get_balance(ar_util:encode(Address32)),
	?assertEqual(Balance, get_balance(ChecksumAddr)),
	LastTX = get_last_tx(ar_util:encode(Address32)),
	?assertEqual(LastTX, get_last_tx(ChecksumAddr)),
	Price = get_price(ar_util:encode(Address32)),
	?assertEqual(Price, get_price(ChecksumAddr)),
	ServeTXTarget = maps:get(<<"target">>, jiffy:decode(get_tx(TX2#tx.id), [return_maps])),
	?assertEqual(ar_util:encode(TX2#tx.target), ServeTXTarget).

get_balance(EncodedAddr) ->
	Peer = master_peer(),
	{_, _, _, _, Port} = Peer,
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/wallet/" ++ binary_to_list(EncodedAddr) ++ "/balance",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	binary_to_integer(Reply).

get_last_tx(EncodedAddr) ->
	Peer = master_peer(),
	{_, _, _, _, Port} = Peer,
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/wallet/" ++ binary_to_list(EncodedAddr) ++ "/last_tx",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	Reply.

get_price(EncodedAddr) ->
	Peer = master_peer(),
	{_, _, _, _, Port} = Peer,
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/price/0/" ++ binary_to_list(EncodedAddr),
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	binary_to_integer(Reply).

get_tx(ID) ->
	Peer = master_peer(),
	{_, _, _, _, Port} = Peer,
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/tx/" ++ binary_to_list(ar_util:encode(ID)),
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	Reply.

%% @doc Ensure that server info can be retreived via the HTTP interface.
get_info_test() ->
	ar_test_node:disconnect_from_slave(),
	ar_test_node:start(no_block),
	?assertEqual(<<?NETWORK_NAME>>, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, name)),
	?assertEqual({<<"release">>, ?RELEASE_NUMBER},
			ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, release)),
	?assertEqual(?CLIENT_VERSION, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, version)),
	?assertEqual(0, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, peers)),
	ar_util:do_until(
		fun() ->
			1 == ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, blocks)
		end,
		100,
		2000
	),
	?assertEqual(0, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, height)).

%% @doc Ensure that transactions are only accepted once.
single_regossip_test() ->
	ar_test_node:start(no_block),
	ar_test_node:slave_start(no_block),
	TX = ar_tx:new(),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX)
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1983}, TX)
	),
	?assertMatch(
		{ok, {{<<"208">>, _}, _, _, _, _}},
		ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1983}, TX)
	),
	?assertMatch(
		{ok, {{<<"208">>, _}, _, _, _, _}},
		ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1983}, TX)
	).

%% @doc Unjoined nodes should not accept blocks
post_block_to_unjoined_node_test() ->
	JB = ar_serialize:jsonify({[{foo, [<<"bing">>, 2.3, true]}]}),
	{ok, {RespTup, _, Body, _, _}} =
		ar_http:req(#{ method => post, peer => {127, 0, 0, 1, 1984}, path => "/block/",
				body => JB }),
	case ar_node:is_joined() of
		false ->
			?assertEqual({<<"503">>, <<"Service Unavailable">>}, RespTup),
			?assertEqual(<<"Not joined.">>, Body);
		true ->
			?assertEqual({<<"400">>,<<"Bad Request">>}, RespTup),
			?assertEqual(<<"Invalid block.">>, Body)
	end.

%% @doc Test that nodes sending too many requests are temporarily blocked: (a) GET.
node_blacklisting_get_spammer_test() ->
	{RequestFun, ErrorResponse} = get_fun_msg_pair(get_info),
	node_blacklisting_test_frame(
		RequestFun,
		ErrorResponse,
		(ar_meta_db:get(requests_per_minute_limit) div 2)+ 1,
		1
	).

%% @doc Test that nodes sending too many requests are temporarily blocked: (b) POST.
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
			InvalidTX = (ar_tx:new())#tx { owner = <<"key">>, signature = <<"invalid">> },
			case ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, InvalidTX) of
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
	ar_test_node:disconnect_from_slave(),
	ar_blacklist_middleware:reset(),
	ar_rate_limiter:off(),
	Responses = lists:map(RequestFun, lists:seq(1, NRequests)),
	?assertEqual(length(Responses), NRequests),
	ar_blacklist_middleware:reset(),
	ByResponseType = count_by_response_type(ErrorResponse, Responses),
	Expected = #{
		error_responses => ExpectedErrors,
		ok_responses => NRequests - ExpectedErrors
	},
	?assertEqual(Expected, ByResponseType),
	ar_rate_limiter:on().

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

%% @doc Check that balances can be retreived over the network.
get_balance_test() ->
	{_Priv1, Pub1} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), 10000, <<>>}]),
	{_Node, _} = ar_test_node:start(B0),
	Addr = binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet/" ++ Addr ++ "/balance"
		}),
	?assertEqual(10000, binary_to_integer(Body)),
	RootHash = binary_to_list(ar_util:encode(B0#block.wallet_list)),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet_list/" ++ RootHash ++ "/" ++ Addr ++ "/balance"
		}),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet_list/" ++ RootHash ++ "/" ++ Addr ++ "/balance"
		}).

get_wallet_list_in_chunks_test() ->
	{_Priv1, Pub1} = ar_wallet:new(),
	[B0] = ar_weave:init([{Addr = ar_wallet:to_address(Pub1), 10000, <<>>}]),
	{_Node, _} = ar_test_node:start(B0),
	NonExistentRootHash = binary_to_list(ar_util:encode(crypto:strong_rand_bytes(32))),
	{ok, {{<<"404">>, _}, _, <<"Root hash not found.">>, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet_list/" ++ NonExistentRootHash
		}),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet_list/" ++ binary_to_list(ar_util:encode(B0#block.wallet_list))
		}),
	?assertEqual(
		#{ next_cursor => last, wallets => [{Addr, {10000, <<>>}}] },
		binary_to_term(Body)
	).

%% @doc Test that heights are returned correctly.
get_height_test() ->
	[B0] = ar_weave:init([], ?DEFAULT_DIFF, ?AR(1)),
	{_Node, _} = ar_test_node:start(B0),
	0 = ar_http_iface_client:get_height({127, 0, 0, 1, 1984}),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	1 = ar_http_iface_client:get_height({127, 0, 0, 1, 1984}).

%% @doc Test that last tx associated with a wallet can be fetched.
get_last_tx_single_test() ->
	{_Priv1, Pub1} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), 10000, <<"TEST_ID">>}]),
	ar_test_node:start(B0),
	Addr = binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet/" ++ Addr ++ "/last_tx"
		}),
	?assertEqual(<<"TEST_ID">>, ar_util:decode(Body)).

%% @doc Check that we can qickly get the local time from the peer.
get_time_test() ->
	Now = os:system_time(second),
	{ok, {Min, Max}} = ar_http_iface_client:get_time({127, 0, 0, 1, 1984}, 10 * 1000),
	?assert(Min < Now),
	?assert(Now < Max).

%% @doc Ensure that blocks can be received via a hash.
get_block_by_hash_test() ->
	[B0] = ar_weave:init([]),
	ar_test_node:start(B0),
	B1 = ar_http_iface_client:get_block({127, 0, 0, 1, 1984}, B0#block.indep_hash),
	?assertEqual(B0#block{ hash_list = unset, size_tagged_txs = unset }, B1).

%% @doc Ensure that blocks can be received via a height.
get_block_by_height_test() ->
	[B0] = ar_weave:init(),
	{_Node, _} = ar_test_node:start(B0),
	ar_test_node:wait_until_height(0),
	{_, B1} = ar_http_iface_client:get_block_shadow([{127, 0, 0, 1, 1984}], 0),
	?assertEqual(
		B0#block{ hash_list = unset, wallet_list = not_set, size_tagged_txs = unset },
		B1#block{ wallet_list = not_set }
	).

get_current_block_test_() ->
	{timeout, 10, fun test_get_current_block/0}.

test_get_current_block() ->
	[B0] = ar_weave:init([]),
	{_Node, _} = ar_test_node:start(B0),
	ar_util:do_until(
		fun() -> B0#block.indep_hash == ar_node:get_current_block_hash() end,
		100,
		2000
	),
	Peer = {127, 0, 0, 1, 1984},
	BI = ar_http_iface_client:get_block_index([Peer]),
	B1 = ar_http_iface_client:get_block([Peer], hd(BI)),
	?assertEqual(B0#block{ hash_list = unset, size_tagged_txs = unset }, B1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{method => get, peer => {127, 0, 0, 1, 1984}, path => "/block/current"}),
	?assertEqual(
		B0#block.indep_hash,
		(ar_serialize:json_struct_to_block(Body))#block.indep_hash
	).

%% @doc Test that the various different methods of GETing a block all perform
%% correctly if the block cannot be found.
get_non_existent_block_test() ->
	[B0] = ar_weave:init([]),
	ar_test_node:start(B0),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{method => get, peer => {127, 0, 0, 1, 1984}, path => "/block/height/100"}),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{method => get, peer => {127, 0, 0, 1, 1984}, path => "/block/hash/abcd"}),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/block/height/101/wallet_list"
		}),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/block/hash/abcd/wallet_list"
		}),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/block/height/101/hash_list"
		}),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/block/hash/abcd/hash_list"
		}).

%% @doc A test for retrieving format=2 transactions from HTTP API.
get_format_2_tx_test() ->
	[B0] = ar_weave:init(),
	{_Node, _} = ar_test_node:start(B0),
	DataRoot = (ar_tx:generate_chunk_tree(#tx{ data = <<"DATA">> }))#tx.data_root,
	ValidTX = #tx{ id = TXID } = (ar_tx:new(<<"DATA">>))#tx{ format = 2, data_root = DataRoot },
	InvalidDataRootTX = #tx{ id = InvalidTXID } = (ar_tx:new(<<"DATA">>))#tx{ format = 2 },
	EmptyTX = #tx{ id = EmptyTXID } = (ar_tx:new())#tx{ format = 2 },
	EncodedTXID = binary_to_list(ar_util:encode(TXID)),
	EncodedInvalidTXID = binary_to_list(ar_util:encode(InvalidTXID)),
	EncodedEmptyTXID = binary_to_list(ar_util:encode(EmptyTXID)),
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, ValidTX),
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, InvalidDataRootTX),
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, EmptyTX),
	ar_test_node:wait_until_receives_txs([ValidTX, EmptyTX, InvalidDataRootTX]),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	%% Ensure format=2 transactions can be retrieved over the HTTP
	%% interface with no populated data, while retaining info on all other fields.
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ EncodedTXID
		}),
	?assertEqual(ValidTX#tx{ data = <<>>, data_size = 4 }, ar_serialize:json_struct_to_tx(Body)),
	%% Ensure data can be fetched for format=2 transactions via /tx/[ID]/data.
	{ok, Data} = wait_until_syncs_tx_data(TXID),
	?assertEqual(ar_util:encode(<<"DATA">>), Data),
	%% Ensure no data is stored when it does not match the data root.
	{ok, {{<<"200">>, _}, _, InvalidData, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ EncodedInvalidTXID ++ "/data"
		}),
	?assertEqual(<<>>, InvalidData),
	%% Ensure /tx/[ID]/data works for format=2 transactions when the data is empty.
	{ok, {{<<"200">>, _}, _, <<>>, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ EncodedEmptyTXID ++ "/data"
		}),
	%% Ensure data can be fetched for format=2 transactions via /tx/[ID]/data.html.
	{ok, {{<<"200">>, _}, Headers, HTMLData, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ EncodedTXID ++ "/data.html"
		}),
	?assertEqual(<<"DATA">>, HTMLData),
	?assertEqual(
		[{<<"content-type">>, <<"text/html">>}],
		proplists:lookup_all(<<"content-type">>, Headers)
	).

get_format_1_tx_test() ->
	[B0] = ar_weave:init(),
	{_Node, _} = ar_test_node:start(B0),
	TX = #tx{ id = TXID } = ar_tx:new(<<"DATA">>),
	EncodedTXID = binary_to_list(ar_util:encode(TXID)),
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX),
	ar_test_node:wait_until_receives_txs([TX]),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	{ok, Body} =
		ar_util:do_until(
			fun() ->
				case ar_http:req(#{
					method => get,
					peer => {127, 0, 0, 1, 1984},
					path => "/tx/" ++ EncodedTXID
				}) of
					{ok, {{<<"404">>, _}, _, _, _, _}} ->
						false;
					{ok, {{<<"200">>, _}, _, Payload, _, _}} ->
						{ok, Payload}
				end
			end,
			100,
			2000
		),
	?assertEqual(TX, ar_serialize:json_struct_to_tx(Body)).

%% @doc Test adding transactions to a block.
add_external_tx_with_tags_test() ->
	[B0] = ar_weave:init([]),
	{_Node, _} = ar_test_node:start(B0),
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
	ar_test_node:wait_until_receives_txs([TaggedTX]),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	[B1Hash | _] = ar_node:get_blocks(),
	B1 = ar_test_node:read_block_when_stored(B1Hash),
	TXID = TaggedTX#tx.id,
	?assertEqual([TXID], B1#block.txs),
	?assertEqual(TaggedTX, ar_storage:read_tx(hd(B1#block.txs))).

%% @doc Test getting transactions
find_external_tx_test() ->
	[B0] = ar_weave:init(),
	{_Node, _} = ar_test_node:start(B0),
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA">>)),
	ar_test_node:wait_until_receives_txs([TX]),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	{ok, FoundTXID} =
		ar_util:do_until(
			fun() ->
				case ar_http_iface_client:get_tx([{127, 0, 0, 1, 1984}], TX#tx.id, maps:new()) of
					not_found ->
						false;
					TX ->
						{ok, TX#tx.id}
				end
			end,
			100,
			1000
		),
	?assertEqual(FoundTXID, TX#tx.id).

fail_external_tx_test() ->
	[B0] = ar_weave:init(),
	{_Node, _} = ar_test_node:start(B0),
	TX = ar_tx:new(<<"DATA">>),
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX),
	ar_test_node:wait_until_receives_txs([TX]),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	BadTX = ar_tx:new(<<"BADDATA">>),
	?assertEqual(not_found, ar_http_iface_client:get_tx([{127, 0, 0, 1, 1984}], BadTX#tx.id, maps:new())).

add_block_with_invalid_hash_test_() ->
	{timeout, 20, fun test_add_block_with_invalid_hash/0}.

test_add_block_with_invalid_hash() ->
	[B0] = ar_weave:init([], ar_retarget:switch_to_linear_diff(10)),
	ar_test_node:start(B0),
	{_Slave, _} = ar_test_node:slave_start(B0),
	ar_test_node:slave_mine(),
	BI = ar_test_node:assert_slave_wait_until_height(1),
	Peer = {127, 0, 0, 1, 1984},
	B1Shadow =
		(ar_test_node:slave_call(ar_storage, read_block, [hd(BI)]))#block{
			hash_list = [B0#block.indep_hash]
		},
	%% Try to post an invalid block. This triggers a ban in ar_blacklist_middleware.
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"Invalid Block Hash">>, _, _}},
		send_new_block(
			Peer,
			B1Shadow#block{ indep_hash = add_rand_suffix(<<"new-hash">>), nonce = <<>> }
		)
	),
	%% Verify the IP address of self is banned in ar_blacklist_middleware.
	?assertMatch(
		{ok, {{<<"403">>, _}, _, <<"IP address blocked due to previous request.">>, _, _}},
		send_new_block(
			Peer,
			B1Shadow#block{ indep_hash = add_rand_suffix(<<"new-hash-again">>) }
		)
	),
	ar_blacklist_middleware:reset(),
	%% The valid block with the ID from the failed attempt can still go through.
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		send_new_block(Peer, B1Shadow)
	),
	%% Try to post the same block again.
	?assertMatch(
		{ok, {{<<"208">>, _}, _, <<"Block already processed.">>, _, _}},
		send_new_block(Peer, B1Shadow)
	),
	%% Correct hash, but invalid PoW.
	B2Shadow = B1Shadow#block{ reward_addr = crypto:strong_rand_bytes(32) },
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"Invalid Block Proof of Work">>, _, _}},
		send_new_block(
			Peer,
			B2Shadow#block{ indep_hash = ar_weave:indep_hash(B2Shadow) }
		)
	),
	?assertMatch(
		{ok, {{<<"403">>, _}, _, <<"IP address blocked due to previous request.">>, _, _}},
		send_new_block(
			Peer,
			B1Shadow#block{indep_hash = add_rand_suffix(<<"new-hash-again">>)}
		)
	),
	ar_blacklist_middleware:reset().

add_external_block_with_invalid_timestamp_test() ->
	ar_blacklist_middleware:reset(),
	[B0] = ar_weave:init([]),
	ar_test_node:start(B0),
	{_Slave, _} = ar_test_node:slave_start(B0),
	ar_test_node:slave_mine(),
	BI = ar_test_node:assert_slave_wait_until_height(1),
	Peer = {127, 0, 0, 1, 1984},
	B1Shadow =
		(ar_test_node:slave_call(ar_storage, read_block, [hd(BI)]))#block{
			hash_list = [B0#block.indep_hash]
		},
	%% Expect the timestamp too far from the future to be rejected.
	FutureTimestampTolerance = ?JOIN_CLOCK_TOLERANCE * 2 + ?CLOCK_DRIFT_MAX,
	TooFarFutureTimestamp = os:system_time(second) + FutureTimestampTolerance + 3,
	B2Shadow = update_block_timestamp(B1Shadow, TooFarFutureTimestamp),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"Invalid timestamp.">>, _, _}},
		send_new_block(Peer, B2Shadow)
	),
	%% Expect the timestamp from the future within the tolerance interval to be accepted.
	OkFutureTimestamp = os:system_time(second) + FutureTimestampTolerance - 3,
	B3Shadow = update_block_timestamp(B1Shadow, OkFutureTimestamp),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		send_new_block(Peer, B3Shadow)
	),
	%% Expect the timestamp far from the past to be rejected.
	PastTimestampTolerance = lists:sum([
		?JOIN_CLOCK_TOLERANCE * 2,
		?CLOCK_DRIFT_MAX,
		?MINING_TIMESTAMP_REFRESH_INTERVAL,
		?MAX_BLOCK_PROPAGATION_TIME
	]),
	TooFarPastTimestamp = os:system_time(second) - PastTimestampTolerance - 3,
	B4Shadow = update_block_timestamp(B1Shadow, TooFarPastTimestamp),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"Invalid timestamp.">>, _, _}},
		send_new_block(Peer, B4Shadow)
	),
	%% Expect the block with a timestamp from the past within the tolerance interval
	%% to be accepted.
	OkPastTimestamp = os:system_time(second) - PastTimestampTolerance + 3,
	B5Shadow = update_block_timestamp(B1Shadow, OkPastTimestamp),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		send_new_block(Peer, B5Shadow)
	).

add_rand_suffix(Bin) ->
	Suffix = ar_util:encode(crypto:strong_rand_bytes(6)),
	iolist_to_binary([Bin, " - ", Suffix]).

update_block_timestamp(B, Timestamp) ->
	#block{
		height = Height,
		nonce = Nonce,
		previous_block = PrevH,
		poa = #poa{ chunk = Chunk }
	} = B,
	B2 = B#block{ timestamp = Timestamp },
	BDS = ar_block:generate_block_data_segment(B2),
	{H0, _Entropy} = ar_mine:spora_h0_with_entropy(BDS, Nonce, Height),
	B3 = B2#block{ hash = ar_mine:spora_solution_hash(PrevH, Timestamp, H0, Chunk, Height) },
	B3#block{ indep_hash = ar_weave:indep_hash(B3) }.

%% @doc Post a tx to the network and ensure that last_tx call returns the ID of last tx.
add_tx_and_get_last_test() ->
	{Priv1, Pub1} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
	{_Node, _} = ar_test_node:start(B0),
	{_Priv2, Pub2} = ar_wallet:new(),
	TX = ar_tx:new(ar_wallet:to_address(Pub2), ?AR(1), ?AR(9000), <<>>),
	SignedTX = ar_tx:sign_v1(TX, Priv1, Pub1),
	ID = SignedTX#tx.id,
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, SignedTX),
	ar_test_node:wait_until_receives_txs([SignedTX]),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/last_tx"
		}),
	?assertEqual(ID, ar_util:decode(Body)).

%% @doc Post a tx to the network and ensure that its subfields can be gathered
get_subfields_of_tx_test() ->
	[B0] = ar_weave:init(),
	{_Node, _} = ar_test_node:start(B0),
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA">>)),
	ar_test_node:wait_until_receives_txs([TX]),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	{ok, Body} = wait_until_syncs_tx_data(TX#tx.id),
	Orig = TX#tx.data,
	?assertEqual(Orig, ar_util:decode(Body)).

%% @doc Correctly check the status of pending is returned for a pending transaction
get_pending_tx_test() ->
	[B0] = ar_weave:init(),
	{_Node, _} = ar_test_node:start(B0),
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA1">>)),
	ar_test_node:wait_until_receives_txs([TX]),
	%write a get_tx function like get_block
	{ok, {{<<"202">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id))
		}),
	?assertEqual(<<"Pending">>, Body).

%% @doc Mine a transaction into a block and retrieve it's binary body via HTTP.
get_tx_body_test() ->
	[B0] = ar_weave:init(random_wallets()),
	{_Node, _} = ar_test_node:start(B0),
	TX = ar_tx:new(<<"TEST DATA">>),
	% Add tx to network
	ar_node:add_tx(TX),
	ar_test_node:wait_until_receives_txs([TX]),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	{ok, Data} = wait_until_syncs_tx_data(TX#tx.id),
	?assertEqual(<<"TEST DATA">>, ar_util:decode(Data)).

random_wallets() ->
	{_, Pub} = ar_wallet:new(),
	[{ar_wallet:to_address(Pub), ?AR(10000), <<>>}].

get_txs_by_send_recv_test_() ->
	{timeout, 60, fun() ->
		{Priv1, Pub1} = ar_wallet:new(),
		{Priv2, Pub2} = ar_wallet:new(),
		{_Priv3, Pub3} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(9000), <<>>),
		SignedTX = ar_tx:sign_v1(TX, Priv1, Pub1),
		TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(500), <<>>),
		SignedTX2 = ar_tx:sign_v1(TX2, Priv2, Pub2),
		[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		{_Node, _} = ar_test_node:start(B0),
		ar_node:add_tx(SignedTX),
		ar_test_node:wait_until_receives_txs([SignedTX]),
		ar_node:mine(),
		ar_test_node:wait_until_height(1),
		ar_node:add_tx(SignedTX2),
		ar_test_node:wait_until_receives_txs([SignedTX2]),
		ar_node:mine(),
		ar_test_node:wait_until_height(2),
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
			ar_http:req(#{
				method => post,
				peer => {127, 0, 0, 1, 1984},
				path => "/arql",
				body => QueryJSON
			}),
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
	[B0] = ar_weave:init([]),
	{_Node, _} = ar_test_node:start(B0),
	TX = (ar_tx:new())#tx{ tags = [{<<"TestName">>, <<"TestVal">>}] },
	ar_node:add_tx(TX),
	ar_test_node:wait_until_receives_txs([TX]),
	FetchStatus = fun() ->
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)) ++ "/status"
		})
	end,
	?assertMatch({ok, {{<<"202">>, _}, _, <<"Pending">>, _, _}}, FetchStatus()),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} = FetchStatus(),
	{Res} = ar_serialize:dejsonify(Body),
	BI = ar_node:get_block_index(),
	?assertEqual(
		#{
			<<"block_height">> => length(BI) - 1,
			<<"block_indep_hash">> => ar_util:encode(element(1, hd(BI))),
			<<"number_of_confirmations">> => 1
		},
		maps:from_list(Res)
	),
	ar_node:mine(),
	ar_test_node:wait_until_height(2),
	ar_util:do_until(
		fun() ->
			{ok, {{<<"200">>, _}, _, Body2, _, _}} = FetchStatus(),
			{Res2} = ar_serialize:dejsonify(Body2),
			#{
				<<"block_height">> => length(BI) - 1,
				<<"block_indep_hash">> => ar_util:encode(element(1, hd(BI))),
				<<"number_of_confirmations">> => 2
			} == maps:from_list(Res2)
		end,
		200,
		5000
	),
	%% Create a fork which returns the TX to mempool.
	{_Slave, _} = ar_test_node:slave_start(B0),
	ar_test_node:connect_to_slave(),
	ar_test_node:slave_mine(),
	ar_test_node:assert_slave_wait_until_height(1),
	ar_test_node:slave_mine(),
	ar_test_node:assert_slave_wait_until_height(2),
	ar_test_node:slave_mine(),
	ar_test_node:wait_until_height(3),
	?assertMatch({ok, {{<<"202">>, _}, _, _, _, _}}, FetchStatus()).

post_unsigned_tx_test_() ->
	{timeout, 20, fun post_unsigned_tx/0}.

post_unsigned_tx() ->
	{_, Pub} = Wallet = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(5000), <<>>}]),
	{_Node, _} = ar_test_node:start(B0),
	%% Generate a wallet and receive a wallet access code.
	{ok, {{<<"421">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet"
		}),
	ar_meta_db:put(internal_api_secret, <<"correct_secret">>),
	{ok, {{<<"421">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet",
			headers => [{<<"X-Internal-Api-Secret">>, <<"incorrect_secret">>}]
		}),
	{ok, {{<<"200">>, <<"OK">>}, _, CreateWalletBody, _, _}} =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet",
			headers => [{<<"X-Internal-Api-Secret">>, <<"correct_secret">>}]
		}),
	ar_meta_db:put(internal_api_secret, not_set),
	{CreateWalletRes} = ar_serialize:dejsonify(CreateWalletBody),
	[WalletAccessCode] = proplists:get_all_values(<<"wallet_access_code">>, CreateWalletRes),
	[Address] = proplists:get_all_values(<<"wallet_address">>, CreateWalletRes),
	%% Top up the new wallet.
	TopUpTX = ar_tx:sign_v1((ar_tx:new())#tx {
		owner = Pub,
		target = ar_util:decode(Address),
		quantity = ?AR(1),
		reward = ?AR(1)
	}, Wallet),
	{ok, {{<<"200">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx",
			body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TopUpTX))
		}),
	ar_test_node:wait_until_receives_txs([TopUpTX]),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	%% Send an unsigned transaction to be signed with the generated key.
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
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, 1984},
			path => "/unsigned_tx",
			body => ar_serialize:jsonify({UnsignedTXProps})
		}),
	ar_meta_db:put(internal_api_secret, <<"correct_secret">>),
	{ok, {{<<"421">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, 1984},
			path => "/unsigned_tx",
			headers => [{<<"X-Internal-Api-Secret">>, <<"incorrect_secret">>}],
			body => ar_serialize:jsonify({UnsignedTXProps})
		}),
	{ok, {{<<"200">>, <<"OK">>}, _, Body, _, _}} =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, 1984},
			path => "/unsigned_tx",
			headers => [{<<"X-Internal-Api-Secret">>, <<"correct_secret">>}],
			body => ar_serialize:jsonify({UnsignedTXProps})
		}),
	ar_meta_db:put(internal_api_secret, not_set),
	{Res} = ar_serialize:dejsonify(Body),
	TXID = proplists:get_value(<<"id">>, Res),
	timer:sleep(200),
	ar_node:mine(),
	ar_test_node:wait_until_height(2),
	{ok, {_, _, GetTXBody, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ binary_to_list(TXID) ++ "/status"
		}),
	{GetTXRes} = ar_serialize:dejsonify(GetTXBody),
	?assertMatch(
		#{
			<<"number_of_confirmations">> := 1
		},
		maps:from_list(GetTXRes)
	).

get_wallet_txs_test_() ->
	{timeout, 10, fun() ->
		{_, Pub} = ar_wallet:new(),
		WalletAddress = binary_to_list(ar_util:encode(ar_wallet:to_address(Pub))),
		[B0] = ar_weave:init([{ar_wallet:to_address(Pub), 10000, <<>>}]),
		{_Node, _} = ar_test_node:start(B0),
		{ok, {{<<"200">>, <<"OK">>}, _, Body, _, _}} =
			ar_http:req(#{
				method => get,
				peer => {127, 0, 0, 1, 1984},
				path => "/wallet/" ++ WalletAddress ++ "/txs"
			}),
		TXs = ar_serialize:dejsonify(Body),
		%% Expect the wallet to have no transactions
		?assertEqual([], TXs),
		%% Sign and post a transaction and expect it to appear in the wallet list
		TX = (ar_tx:new())#tx{ owner = Pub },
		{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
			ar_http:req(#{
				method => post,
				peer => {127, 0, 0, 1, 1984},
				path => "/tx",
				body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
			}),
		ar_test_node:wait_until_receives_txs([TX]),
		ar_node:mine(),
		[{H, _, _} | _] = ar_test_node:wait_until_height(1),
		%% Wait until the storage is updated before querying for wallet's transactions.
		ar_test_node:read_block_when_stored(H),
		{ok, {{<<"200">>, <<"OK">>}, _, GetOneTXBody, _, _}} =
			ar_http:req(#{
				method => get,
				peer => {127, 0, 0, 1, 1984},
				path => "/wallet/" ++ WalletAddress ++ "/txs"
			}),
		OneTX = ar_serialize:dejsonify(GetOneTXBody),
		?assertEqual([ar_util:encode(TX#tx.id)], OneTX),
		%% Expect the same when the TX is specified as the earliest TX
		{ok, {{<<"200">>, <<"OK">>}, _, GetOneTXAgainBody, _, _}} =
			ar_http:req(#{
				method => get,
				peer => {127, 0, 0, 1, 1984},
				path => "/wallet/" ++ WalletAddress ++ "/txs/" ++ binary_to_list(ar_util:encode(TX#tx.id))
			}),
		OneTXAgain = ar_serialize:dejsonify(GetOneTXAgainBody),
		?assertEqual([ar_util:encode(TX#tx.id)], OneTXAgain),
		%% Add one more TX and expect it to be appended to the wallet list
		SecondTX = (ar_tx:new())#tx{ owner = Pub, last_tx = TX#tx.id },
		{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
			ar_http:req(#{
				method => post,
				peer => {127, 0, 0, 1, 1984},
				path => "/tx",
				body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(SecondTX))
			}),
		ar_test_node:wait_until_receives_txs([SecondTX]),
		ar_node:mine(),
		ar_test_node:wait_until_height(2),
		{ok, {{<<"200">>, <<"OK">>}, _, GetTwoTXsBody, _, _}} =
			ar_http:req(#{
				method => get,
				peer => {127, 0, 0, 1, 1984},
				path => "/wallet/" ++ WalletAddress ++ "/txs"
			}),
		Expected = [ar_util:encode(SecondTX#tx.id), ar_util:encode(TX#tx.id)],
		?assertEqual(Expected, ar_serialize:dejsonify(GetTwoTXsBody)),
		%% Specify the second TX as the earliest and expect the first one to be excluded
		{ok, {{<<"200">>, <<"OK">>}, _, GetSecondTXBody, _, _}} =
			ar_http:req(#{
				method => get,
				peer => {127, 0, 0, 1, 1984},
				path => "/wallet/" ++ WalletAddress ++ "/txs/" ++ binary_to_list(ar_util:encode(SecondTX#tx.id))
			}),
		OneSecondTX = ar_serialize:dejsonify(GetSecondTXBody),
		?assertEqual([ar_util:encode(SecondTX#tx.id)], OneSecondTX)
	end}.

get_wallet_deposits_test_() ->
	{timeout, 10, fun() ->
		%% Create a wallet to transfer tokens to
		{_, PubTo} = ar_wallet:new(),
		WalletAddressTo = binary_to_list(ar_util:encode(ar_wallet:to_address(PubTo))),
		%% Create a wallet to transfer tokens from
		{_, PubFrom} = ar_wallet:new(),
		[B0] = ar_weave:init([
			{ar_wallet:to_address(PubTo), 0, <<>>},
			{ar_wallet:to_address(PubFrom), 200, <<>>}
		]),
		{_Node, _} = ar_test_node:start(B0),
		GetTXs = fun(EarliestDeposit) ->
			BasePath = "/wallet/" ++ WalletAddressTo ++ "/deposits",
			Path = 	BasePath ++ "/" ++ EarliestDeposit,
			{ok, {{<<"200">>, <<"OK">>}, _, Body, _, _}} =
				ar_http:req(#{
					method => get,
					peer => {127, 0, 0, 1, 1984},
					path => Path
				}),
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
				ar_http:req(#{
					method => post,
					peer => {127, 0, 0, 1, 1984},
					path => "/tx",
					body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(T))
				})
		end,
		PostTX(FirstTX),
		ar_test_node:wait_until_receives_txs([FirstTX]),
		ar_node:mine(),
		ar_test_node:wait_until_height(1),
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
		ar_test_node:wait_until_receives_txs([SecondTX]),
		ar_node:mine(),
		ar_test_node:wait_until_height(2),
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

%% @doc Ensure the HTTP client stops fetching data from an endpoint when its data size
%% limit is exceeded.
get_error_of_data_limit_test() ->
	[B0] = ar_weave:init(),
	{_Node, _} = ar_test_node:start(B0),
	Limit = 1460,
	ar_http_iface_client:send_new_tx(
		{127, 0, 0, 1, 1984},
		TX = ar_tx:new(<< <<0>> || _ <- lists:seq(1, Limit * 2) >>)
	),
	ar_test_node:wait_until_receives_txs([TX]),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	{ok, _} = wait_until_syncs_tx_data(TX#tx.id),
	Resp =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)) ++ "/data",
			limit => Limit
		}),
	?assertEqual({error, too_much_data}, Resp).

send_new_block(Peer, B) ->
	BDS = ar_block:generate_block_data_segment(B),
	ar_http_iface_client:send_new_block(Peer, B, BDS).

wait_until_syncs_tx_data(TXID) ->
	ar_util:do_until(
		fun() ->
			case ar_http:req(#{
				method => get,
				peer => {127, 0, 0, 1, 1984},
				path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/data"
			}) of
				{ok, {{<<"404">>, _}, _, _, _, _}} ->
					false;
				{ok, {{<<"200">>, _}, _, <<>>, _, _}} ->
					false;
				{ok, {{<<"200">>, _}, _, Payload, _, _}} ->
					{ok, Payload}
			end
		end,
		100,
		2000
	).
