-module(ar_data_sync_tests).

-include_lib("eunit/include/eunit.hrl").

-include("src/ar.hrl").
-include("src/ar_data_sync.hrl").

-import(ar_test_node, [start/1, slave_start/1, connect_to_slave/0, sign_tx/2, sign_v1_tx/2]).
-import(ar_test_node, [assert_post_tx_to_master/2, assert_post_tx_to_slave/2]).
-import(ar_test_node, [wait_until_height/2, assert_slave_wait_until_height/2]).
-import(ar_test_node, [get_tx_anchor/0, slave_mine/1]).

rejects_invalid_chunks_test() ->
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"chunk_too_big\"}">>, _, _}},
		post_chunk(jiffy:encode(#{ chunk => ar_util:encode(crypto:strong_rand_bytes(?DATA_CHUNK_SIZE + 1)) }))
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"tx_path_too_big\"}">>, _, _}},
		post_chunk(jiffy:encode(#{ tx_path => ar_util:encode(crypto:strong_rand_bytes(?MAX_PATH_SIZE)) }))
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"data_path_too_big\"}">>, _, _}},
		post_chunk(jiffy:encode(#{ data_path => ar_util:encode(crypto:strong_rand_bytes(?MAX_PATH_SIZE)) }))
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"offset_too_big\"}">>, _, _}},
		post_chunk(jiffy:encode(#{ offset => integer_to_binary(trunc(math:pow(2, 256))) }))
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"chunk_proof_ratio_not_attractive\"}">>, _, _}},
		post_chunk(jiffy:encode(#{ chunk => <<"a">>, proof => <<"bb">> }))
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"invalid_proof\"}">>, _, _}},
		post_chunk(jiffy:encode(#{
			chunk => <<"a">>,
			proof => <<"b">>,
			tx_path => <<"a">>,
			data_path => <<"b">>
		}))
	),
	?assertMatch(
		{ok, {{<<"413">>, _}, _, <<"Payload too large">>, _, _}},
		post_chunk(<< <<0>> || _ <- lists:seq(1, ?MAX_SERIALIZED_CHUNK_PROOF_SIZE + 1) >>)
	).

accepts_confirmed_chunk_test_() ->
	{timeout, 60, fun test_accepts_confirmed_chunk/0}.

test_accepts_confirmed_chunk() ->
	{Master, Slave, Wallet} = setup_nodes(),
	%% TODO turn syncing off on master, we are going to submit chunks through API here
	{TX, Chunks} = tx(Wallet, #{ chunks => 2 }),
	B = post_and_wait_until_confirmed(Master, Slave, TX, ?TRACK_CONFIRMATIONS),
	[FirstProof, SecondProof] = build_proofs(B, TX, Chunks),
	EndOffset = B#block.weave_size - B#block.block_size + binary_to_integer(maps:get(offset, FirstProof)),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(EndOffset)
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		post_chunk(jiffy:encode(FirstProof))
	),
	%% Expect the chunk to be retrieved by any offset within
	%% (EndOffset - ChunkSize, EndOffset], but not outside of it.
	FirstChunk = maps:get(chunk, FirstProof),
	FirstChunkSize = byte_size(FirstChunk),
	ExpectedProof = jiiffy:encode(#{
		data_path => maps:get(data_path, FirstProof),
		tx_path => maps:get(tx_path, FirstProof),
		chunk => FirstChunk
	}),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, ExpectedProof, _, _}},
		get_chunk(EndOffset)
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, ExpectedProof, _, _}},
		get_chunk(EndOffset - FirstChunkSize + 2 + rand:uniform(FirstChunkSize - 1))
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, ExpectedProof, _, _}},
		get_chunk(EndOffset - FirstChunkSize + 1)
	),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(EndOffset - FirstChunkSize)
	),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(EndOffset + 1)
	),
	%% Expect the transaction data to be empty because the second chunk
	%% is not synced yet.
	?assertMatch(
		{ok, {{<<"200">>, _}, _, <<>>, _, _}},
		get_tx_data(TX#tx.id)
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		post_chunk(jiffy:encode(SecondProof))
	),
	ExpectedSecondProof = jiiffy:encode(#{
		data_path => maps:get(data_path, SecondProof),
		tx_path => maps:get(tx_path, SecondProof),
		chunk => maps:get(chunk, SecondProof)
	}),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, ExpectedSecondProof, _, _}},
		get_chunk(B#block.weave_size)
	),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(B#block.weave_size + 1)
	),
	{ok, {{<<"200">>, _}, _, Data, _, _}} = get_tx_data(TX#tx.id),
	?assertEqual(ar_util:encode(TX#tx.data), Data).

accepts_unconfirmed_chunks_test_() ->
	{timeout, 60, fun test_accepts_unconfirmed_chunks/0}.

test_accepts_unconfirmed_chunks() ->
	{Master, Slave, Wallet} = setup_nodes(),
	%% TODO turn syncing off on master, we are going to submit chunks through API here
	{TX, Chunks} = tx(Wallet, #{ chunks => 3 }),
	B = post_and_wait_until_confirmed(Master, Slave, TX, ?TRACK_CONFIRMATIONS - 1),
	[FirstProof, SecondProof, ThirdProof] = build_proofs(B, TX, Chunks),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		post_chunk(jiffy:encode(FirstProof))
	),
	EndOffset = B#block.weave_size - B#block.block_size + binary_to_integer(maps:get(offset, FirstProof)),
	%% Unconfirmed chunks are not served by GET /chunk/<offset>.
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(EndOffset)
	),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(EndOffset - 1)
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, <<>>, _, _}},
		get_tx_data(TX#tx.id)
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		post_chunk(jiffy:encode(SecondProof))
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		post_chunk(jiffy:encode(ThirdProof))
	),
	{ok, {{<<"200">>, _}, _, Data, _, _}} = get_tx_data(TX#tx.id),
	?assertEqual(ar_util:encode(TX#tx.data), Data).

syncs_data_test_() ->
	{timeout, 180, fun test_syncs_data/0}.

test_syncs_data() ->
	{Master, Slave, Wallet} = setup_nodes(),
	Records = post_random_blocks(Master, Slave, Wallet),
	RecordsWithProofs = lists:flatmap(
		fun({B, TX, Chunks}) ->
			[{B, TX, Proof} || Proof <- build_proofs(B, TX, Chunks)]
		end,
		Records
	),
	lists:foreach(
		fun({_, _, Proof}) ->
			?assertMatch(
				{ok, {{<<"200">>, _}, _, _, _, _}},
				post_chunk(jiffy:encode(Proof))
			)
		end,
		RecordsWithProofs
	),
	lists:foreach(
		fun({B, _, Proof}) ->
			EndOffset = B#block.weave_size - B#block.block_size + binary_to_integer(maps:get(offset, Proof)),
			ExpectedProof = jiffy:encode(#{
				data_path => maps:get(data_path, Proof),
				tx_path => maps:get(tx_path, Proof),
				chunk => maps:get(chunk, Proof)
			}),
			ar_util:do_until(
				fun() ->
					case get_chunk_from_slave(EndOffset) of
						{ok, {{<<"200">>, _}, _, ExpectedProof, _, _}} ->
							true;
						_ ->
							false
					end
				end,
				100,
				60 * 1000
			)
		end,
		RecordsWithProofs
	),
	lists:foreach(
		fun({#tx{ id = TXID, data = Data }, _, _}) ->
			ExpectedData = ar_util:encode(Data),
			ar_util:do_until(
				fun() ->
					get_tx_data_from_slave(TXID) == ExpectedData
				end,
				100,
				60 * 1000
			)
		end,
		RecordsWithProofs
	).

post_chunk(Body) ->
	ar_http:req(#{
		method => post,
		peer => {127, 0, 0, 1, 1984},
		path => "/chunk",
		body => Body
	}).

setup_nodes() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	{Master, _} = start(B0),
	{Slave, _} = slave_start(B0),
	connect_to_slave(),
	{Master, Slave, Wallet}.

tx(Wallet, Params) ->
	tx(Wallet, Params, v2).

v1_tx(Wallet, Params) ->
	tx(Wallet, Params, v1).

tx(Wallet, #{ chunks := ChunkCount }, Format) ->
	{Data, Chunks} = lists:foldl(
		fun(_, {Data, Chunks}) ->
			OneThird = ?DATA_CHUNK_SIZE div 3,
			RandomSize = OneThird + rand:uniform(?DATA_CHUNK_SIZE - OneThird) - 1,
			Chunk = crypto:strong_rand_bytes(RandomSize),
			{<< Data/binary, Chunk/binary >>, [Chunk | Chunks]}
		end,
		{<<>>, []},
		lists:seq(1, ChunkCount)
	),
	TX = case Format of
		v1 ->
			sign_v1_tx(Wallet, #{ data => Data, last_tx => get_tx_anchor() });
		v2 ->
			sign_tx(Wallet, #{ data => Data, last_tx => get_tx_anchor() })
	end,
	{TX, Chunks}.

post_and_wait_until_confirmed(Master, Slave, TX, Confirmations) ->
	assert_post_tx_to_slave(Slave, TX),
	lists:foreach(
		fun(Height) ->
			slave_mine(Slave),
			wait_until_height(Master, Height)
		end,
		lists:seq(1, Confirmations)
	),
	BI = ar_node:get_block_index(Master),
	H = element(1, lists:nth(length(BI) - 1, BI)),
	BShadow = ar_storage:read_block(H),
	BShadow#block{ txs = ar_storage:read_tx(BShadow#block.txs) }.

build_proofs(B, TX, Chunks) ->
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(B#block.txs),
	{_, TXOffset} = lists:nth(index(TX#tx.id, lists:sort(B#block.txs)), SizeTaggedTXs),
	{TXRoot, TXTree} = ar_merkle:generate_tree(SizeTaggedTXs),
	TXPath = ar_merkle:generate_path(TXRoot, TXOffset, TXTree),
	SizeTaggedChunks = ar_tx:chunks_to_size_tagged_chunks(Chunks),
	{DataRoot, DataTree} = ar_merkle:generate_tree(SizeTaggedChunks),
	lists:foldl(
		fun({Chunk, ChunkOffset}, Proofs) ->
			Proof = #{
				tx_path => ar_util:encode(TXPath),
				data_path =>
					ar_util:encode(ar_merkle:generate_path(DataRoot, ChunkOffset, DataTree)),
				chunk => ar_util:encode(Chunk),
				offset => integer_to_binary(TXOffset + ChunkOffset)
			},
			Proofs ++ [Proof]
		end,
		[],
		SizeTaggedChunks
	).

index(TXID, TXs) ->
	index(TXID, TXs, 1).

index(TXID, [#tx{ id = TXID } | _], Index) ->
	Index;
index(TXID, [_ | TXs], Index) ->
	index(TXID, TXs, Index + 1).

get_chunk(Offset) ->
	ar_http:req(#{
		method => get,
		peer => {127, 0, 0, 1, 1984},
		path => "/chunk/" ++ integer_to_list(Offset)
	}).

get_chunk_from_slave(Offset) ->
	ar_http:req(#{
		method => get,
		peer => {127, 0, 0, 1, 1983},
		path => "/chunk/" ++ integer_to_list(Offset)
	}).

get_tx_data(TXID) ->
	ar_http:req(#{
		method => get,
		peer => {127, 0, 0, 1, 1984},
		path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/data"
	}).

get_tx_data_from_slave(TXID) ->
	ar_http:req(#{
		method => get,
		peer => {127, 0, 0, 1, 1983},
		path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/data"
	}).

post_random_blocks(Master, Slave, Wallet) ->
	BlockMap = [
		[v1],
		empty,
		[v2, v1],
		[v2, v2, v2],
		empty,
		[v1, v2, v2, v2],
		[v2],
		empty,
		empty,
		[v1, v2, v1, v2],
		empty,
		empty,
		empty,
		empty
	],
	lists:foldl(
		fun
			({empty, Height}, Acc) ->
				ar_node:mine(Master),
				assert_slave_wait_until_height(Slave, Height),
				Acc;
			({TXMap, Height}, Acc) ->
				TXsWithChunks = lists:map(
					fun
						(v1) ->
							v1_tx(Wallet, #{ chunks => rand:uniform(10) });
						(v2) ->
							tx(Wallet, #{ chunks => rand:uniform(10) })
					end,
					TXMap
				),
				lists:foreach(
					fun
						({#tx{ format = 2 } = TX, _}) ->
							assert_post_tx_to_master(Master, TX);
						({TX, _}) ->
							assert_post_tx_to_master(Master, TX)
					end,
					TXsWithChunks
				),
				ar_node:mine(Master),
				assert_slave_wait_until_height(Slave, Height),
				[{H, _, _} | _] = ar_node:get_block_index(Master),
				BShadow = ar_storage:read_block(H),
				B = BShadow#block{ txs = ar_storage:read_tx(BShadow#block.txs) },
				[{B, TX, Chunks} || {TX, Chunks} <- TXsWithChunks] ++ Acc
		end,
		[],
		lists:zip(BlockMap, lists:seq(1, length(BlockMap)))
	).
