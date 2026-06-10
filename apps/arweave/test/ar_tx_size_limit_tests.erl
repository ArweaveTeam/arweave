-module(ar_tx_size_limit_tests).
-test_peers([peer1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [random_v1_data/1]).

mines_blocks_under_the_size_limit_test_() ->
	PrepareTestFor = fun(BuildTXSetFun) ->
		fun() ->
			{B0, TXGroups} = BuildTXSetFun(),
			mines_blocks_under_the_size_limit(B0, TXGroups)
		end
	end,
	[
		{
			"Five transactions with block anchors",
			{timeout, ?TEST_NODE_TIMEOUT, PrepareTestFor(fun ar_tx_test_utils:grouped_txs/0)}
		}
	].

rejects_transactions_above_the_size_limit_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun test_rejects_transactions_above_the_size_limit/0}.

drops_v1_txs_exceeding_mempool_limit_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun test_drops_v1_txs_exceeding_mempool_limit/0}.

drops_v2_txs_exceeding_mempool_limit_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun drops_v2_txs_exceeding_mempool_limit/0}.

mines_format_2_txs_without_size_limit_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun mines_format_2_txs_without_size_limit/0}.

mines_blocks_under_the_size_limit(B0, TXGroups) ->
	%% TXs grouped by block size are mined one group per block, each block
	%% staying under the size limit.
	_ = ar_test_node:start(B0),
	_ = ar_test_node:start_peer(peer1, B0),
	ar_test_node:connect_to_peer(peer1),
	lists:foreach(
		fun(TX) ->
			ar_test_node:assert_post_tx_to_peer(peer1, TX),
			?assertEqual(ok, ar_test_await:txs_ready_for_mining(main, [TX]))
		end,
		lists:flatten(TXGroups)
	),
	lists:foldl(
		fun(Group, Height) ->
			ar_test_node:mine(peer1),
			{ok, PeerBI} = ar_test_await:node_height(peer1, Height),
			GroupTXIDs = lists:map(fun(TX) -> TX#tx.id end, Group),
			?assertEqual(
				lists:sort(GroupTXIDs),
				lists:sort(
					(ar_test_node:remote_call(peer1, ar_test_await, block_stored, [hd(PeerBI)]))#block.txs
				),
				io_lib:format("Height ~B", [Height])
			),
			ok = ar_test_await:txs_stored(GroupTXIDs),
			Height + 1
		end,
		1,
		TXGroups
	).

test_rejects_transactions_above_the_size_limit() ->
	Key1 = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(20), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(20), <<>>}
	]),
	_ = ar_test_node:start_peer(peer1, B0),
	_ = ar_test_node:connect_to_peer(peer1),
	SmallData = random_v1_data(?TX_DATA_SIZE_LIMIT),
	BigData = random_v1_data(?TX_DATA_SIZE_LIMIT + 1),
	GoodTX = ar_test_node:sign_v1_tx(Key1, #{ data => SmallData }),
	ar_test_node:assert_post_tx_to_peer(peer1, GoodTX),
	BadTX = ar_test_node:sign_v1_tx(Key2, #{ data => BigData }),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"Transaction verification failed.">>, _, _}},
		ar_test_node:post_tx_to_peer(peer1, BadTX)
	),
	?assertMatch(
		{ok, ["tx_fields_too_large"]},
		ar_test_node:remote_call(peer1, ar_tx_db, get_error_codes, [BadTX#tx.id])
	).

test_drops_v1_txs_exceeding_mempool_limit() ->
	%% A v1 TX that overflows the mempool size limit is dropped.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(20), <<>>}
	]),
	_ = ar_test_node:start_peer(peer1, B0),
	BigChunk = random_v1_data(?TX_DATA_SIZE_LIMIT - ?TX_SIZE_BASE),
	TXs = lists:map(
		fun(N) ->
			ar_test_node:sign_v1_tx(Key, #{ last_tx => B0#block.indep_hash,
					data => BigChunk, tags => [{<<"nonce">>, integer_to_binary(N)}] })
		end,
		lists:seq(1, 6)
	),
	lists:foreach(
		fun(TX) ->
			ar_test_node:assert_post_tx_to_peer(peer1, TX)
		end,
		lists:sublist(TXs, 5)
	),
	Peer1 = ar_test_node:peer_ip(peer1),
	{{ok, Mempool1}, Peer1} = ar_http_iface_client:get_mempool(Peer1),
	%% Equal-utility TXs are ordered by submission.
	?assertEqual([TX#tx.id || TX <- lists:sublist(TXs, 5)], Mempool1),
	Last = lists:last(TXs),
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = ar_test_node:post_tx_to_peer(peer1, Last, false),
	{{ok, Mempool2}, Peer1} = ar_http_iface_client:get_mempool(Peer1),
	%% The mempool is full, so the last TX is dropped.
	?assertEqual([TX#tx.id || TX <- lists:sublist(TXs, 5)], Mempool2).

drops_v2_txs_exceeding_mempool_limit() ->
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(20), <<>>}
	]),
	_ = ar_test_node:start_peer(peer1, B0),
	BigChunk = crypto:strong_rand_bytes(?TX_DATA_SIZE_LIMIT div 2),
	TXs = lists:map(
		fun(N) ->
			ar_test_node:sign_tx(Key, #{ last_tx => B0#block.indep_hash,
					data => case N of 11 -> << BigChunk/binary, BigChunk/binary >>;
							_ -> BigChunk end,
					tags => [{<<"nonce">>, integer_to_binary(N)}] })
		end,
		lists:seq(1, 11)
	),
	lists:foreach(
		fun(TX) ->
			ar_test_node:assert_post_tx_to_peer(peer1, TX)
		end,
		lists:sublist(TXs, 10)
	),
	Peer1 = ar_test_node:peer_ip(peer1),
	{{ok, Mempool1}, Peer1} = ar_http_iface_client:get_mempool(Peer1),
	%% Equal-utility TXs are ordered by submission.
	?assertEqual([TX#tx.id || TX <- lists:sublist(TXs, 10)], Mempool1),
	Last = lists:last(TXs),
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = ar_test_node:post_tx_to_peer(peer1, Last, false),
	{{ok, Mempool2}, Peer1} = ar_http_iface_client:get_mempool(Peer1),
	%% The last TX is twice as big and twice as valuable, so it evicts two TXs.
	?assertEqual([Last#tx.id | [TX#tx.id || TX <- lists:sublist(TXs, 8)]], Mempool2),
	%% Posting the same TX with data stripped: the header alone is accepted.
	StrippedTX = ar_test_node:sign_tx(Key, #{ last_tx => B0#block.indep_hash,
			data => BigChunk, tags => [{<<"nonce">>, integer_to_binary(12)}] }),
	ar_test_node:assert_post_tx_to_peer(peer1, StrippedTX#tx{ data = <<>> }),
	{{ok, Mempool3}, Peer1} = ar_http_iface_client:get_mempool(Peer1),
	?assertEqual([Last#tx.id] ++ [TX#tx.id || TX <- lists:sublist(TXs, 8)]
			++ [StrippedTX#tx.id], Mempool3).

mines_format_2_txs_without_size_limit() ->
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(20), <<>>}
	]),
	MainAddr = ar_test_node:generate_address(main),
	PeerAddr = ar_test_node:generate_address(peer1),
	_ = ar_test_node:start(#{
		b0 => B0,
		addr => MainAddr,
		config => ar_test_node:storage_module_config(MainAddr, lists:seq(0, 8))
	}),
	_ = ar_test_node:start_peer(peer1, #{
		b0 => B0,
		addr => PeerAddr,
		config => ar_test_node:storage_module_config(PeerAddr, lists:seq(0, 8))
	}),
	ar_test_node:connect_to_peer(peer1),
	ChunkSize = ?MEMPOOL_DATA_SIZE_LIMIT div (?BLOCK_TX_COUNT_LIMIT + 1),
	lists:foreach(
		fun(N) ->
			TX = ar_test_node:sign_tx(
				Key,
				#{
					last_tx => B0#block.indep_hash,
					data => << <<1>> || _ <- lists:seq(1, ChunkSize) >>,
					tags => [{<<"nonce">>, integer_to_binary(N)}]
				}
			),
			ar_test_node:assert_post_tx_to_peer(peer1, TX),
			?assertEqual(ok, ar_test_await:txs_ready_for_mining(main, [TX]))
		end,
		lists:seq(1, ?BLOCK_TX_COUNT_LIMIT + 1)
	),
	ar_test_node:mine(),
	{ok, [{H, _, _} | _]} = ar_test_await:node_height(main, 1),
	B = ar_test_await:block_stored(H),
	?assertEqual(?BLOCK_TX_COUNT_LIMIT, length(B#block.txs)),
	TotalSize = lists:sum([(ar_storage:read_tx(TXID))#tx.data_size || TXID <- B#block.txs]),
	?assert(TotalSize > ?BLOCK_TX_DATA_SIZE_LIMIT).
