-module(ar_tx_blacklist_tests).

-export([init/2]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-import(ar_test_node, [
	slave_start/1, start/3, connect_to_slave/0,
	get_tx_anchor/1,
	sign_tx/2,
	slave_call/3,
	assert_post_tx_to_slave/1, assert_post_tx_to_master/1,
	slave_mine/0,
	wait_until_height/1, assert_slave_wait_until_height/1,
	get_chunk/1, get_chunk/2, post_chunk/1, post_chunk/2,
	disconnect_from_slave/0
]).

init(Req, State) ->
	SplitPath = ar_http_iface_server:split_path(cowboy_req:path(Req)),
	handle(SplitPath, Req, State).

handle([<<"empty">>], Req, State) ->
	{ok, cowboy_req:reply(200, #{}, <<>>, Req), State};

handle([<<"good">>], Req, State) ->
	{ok, cowboy_req:reply(200, #{}, ar_util:encode(hd(State)), Req), State};

handle([<<"bad">>, <<"and">>, <<"good">>], Req, State) ->
	Reply =
		list_to_binary(
			io_lib:format(
				"~s\nbad base64url \n~s\n",
				lists:map(fun ar_util:encode/1, State)
			)
		),
	{ok, cowboy_req:reply(200, #{}, Reply, Req), State}.

uses_blacklists_test_() ->
	{timeout, 240, fun test_uses_blacklists/0}.

test_uses_blacklists() ->
	{
		BlacklistFiles,
		B0,
		Wallet,
		TXs,
		GoodTXIDs,
		BadTXIDs,
		GoodOffsets,
		BadOffsets,
		DataTrees
	} = setup(),
	WhitelistFile = random_filename(),
	ok = file:write_file(WhitelistFile, <<>>),
	{_, _} =
		start(B0, unclaimed, (element(2, application:get_env(arweave, config)))#config{
			transaction_blacklist_files = BlacklistFiles,
			transaction_whitelist_files = [WhitelistFile],
			transaction_blacklist_urls = [
				%% Serves empty body.
				"http://localhost:1985/empty",
				%% Serves a valid TX ID (one from the BadTXIDs list).
				"http://localhost:1985/good",
				%% Serves some valid TX IDs (from the BadTXIDs list) and a line
				%% with invalid Base64URL.
				"http://localhost:1985/bad/and/good"
			]
		}),
	connect_to_slave(),
	lists:foreach(
		fun({TX, Height}) ->
			assert_post_tx_to_slave(TX),
			slave_mine(),
			upload_data([TX], GoodTXIDs, DataTrees),
			wait_until_height(Height)
		end,
		lists:zip(TXs, lists:seq(1, length(TXs)))
	),
	assert_present_txs(GoodTXIDs),
	assert_removed_txs(BadTXIDs),
	assert_present_offsets(GoodOffsets),
	assert_removed_offsets(BadOffsets),
	assert_does_not_accept_offsets(BadOffsets),
	%% Add a new transaction to the blacklist, add a blacklisted transaction to whitelist.
	ok = file:write_file(WhitelistFile, ar_util:encode(lists:nth(2, BadTXIDs))),
	%% Reseting the contents of the files does not remove the corresponding txs from blacklist.
	ok = file:write_file(lists:nth(2, BlacklistFiles), <<>>),
	ok = file:write_file(lists:nth(3, BlacklistFiles), <<>>),
	ok = file:write_file(lists:nth(4, BlacklistFiles), ar_util:encode(hd(GoodTXIDs))),
	[UnblacklistedOffsets, WhitelistOffsets | BadOffsets2] = BadOffsets,
	RestoredOffsets = [UnblacklistedOffsets, WhitelistOffsets],
	[_UnblacklistedTXID, _WhitelistTXID | BadTXIDs2] = BadTXIDs,
	%% Expect the transaction data to be resynced.
	assert_present_offsets(RestoredOffsets),
	%% Expect the freshly blacklisted transaction to be erased.
	assert_removed_txs([hd(GoodTXIDs)]),
	assert_removed_offsets([hd(GoodOffsets)]),
	assert_does_not_accept_offsets([hd(GoodOffsets)]),
	%% Expect the previously blacklisted transactions to stay blacklisted.
	assert_removed_txs(BadTXIDs2),
	assert_removed_offsets(BadOffsets2),
	assert_does_not_accept_offsets(BadOffsets2),
	%% Blacklist the last transaction. Fork the weave. Assert the blacklisted offsets are moved.
	disconnect_from_slave(),
	TX = sign_tx(
		Wallet,
		#{
			format => 1,
			data => crypto:strong_rand_bytes(102400),
			last_tx => get_tx_anchor(slave)
		}
	),
	assert_post_tx_to_master(TX),
	ar_node:mine(),
	[{_, WeaveSize, _} | _] = wait_until_height(length(TXs) + 1),
	assert_present_offsets([[WeaveSize]]),
	ok = file:write_file(lists:nth(3, BlacklistFiles), ar_util:encode(TX#tx.id)),
	assert_removed_offsets([[WeaveSize]]),
	connect_to_slave(),
	TX2 = sign_tx(
		Wallet,
		#{
			format => 1,
			data => crypto:strong_rand_bytes(204800),
			last_tx => get_tx_anchor(slave)
		}
	),
	assert_post_tx_to_slave(TX2),
	slave_mine(),
	assert_slave_wait_until_height(length(TXs) + 1),
	assert_post_tx_to_slave(TX),
	slave_mine(),
	assert_slave_wait_until_height(length(TXs) + 2),
	[{_, WeaveSize2, _} | _] = wait_until_height(length(TXs) + 2),
	assert_removed_offsets([[WeaveSize2]]),
	assert_present_offsets([[WeaveSize]]),
	teardown().

setup() ->
	{B0, Wallet} = setup_slave(),
	{TXs, DataTrees} = create_txs(Wallet),
	TXIDs = [TX#tx.id || TX <- TXs],
	BadTXIDs = [lists:nth(1, TXIDs), lists:nth(3, TXIDs)],
	BlacklistFiles = create_files(BadTXIDs),
	BadTXIDs2 = [lists:nth(5, TXIDs), lists:nth(7, TXIDs)],
	Routes = [{"/[...]", ar_tx_blacklist_tests, BadTXIDs2}],
	{ok, _PID} =
		slave_call(cowboy, start_clear, [
			ar_tx_blacklist_test_listener,
			[{port, 1985}],
			#{ env => #{ dispatch => cowboy_router:compile([{'_', Routes}]) } }
		]),
	GoodTXIDs = TXIDs -- (BadTXIDs ++ BadTXIDs2),
	DataSizes = [TX#tx.data_size || TX <- TXs],
	[S1, S2, S3, S4, S5, S6, S7, S8 | _] = DataSizes,
	BadOffsets = [S1, S1 + S2 + S3, S1 + S2 + S3 + S4 + S5, S1 + S2 + S3 + S4 + S5 + S6 + S7],
	BadOffsets2 =
		lists:map(
			fun(TXOffset) ->
				%% Every TX in this test consists of 10 chunks.
				%% Only every second chunk is uploaded in this test
				%% for (originally) blacklisted transactions.
				[TXOffset - 102400 * I || I <- lists:seq(0, 9, 2)]
			end,
			BadOffsets
		),
	GoodOffsets = [
		S1 + S2, S1 + S2 + S3 + S4, S1 + S2 + S3 + S4 + S5 + S6,
		S1 + S2 + S3 + S4 + S5 + S6 + S7 + S8
	],
	GoodOffsets2 =
		lists:map(
			fun(TXOffset) ->
				%% Every TX in this test consists of 10 chunks.
				[TXOffset - 102400 * I || I <- lists:seq(0, 9)]
			end,
			GoodOffsets
		),
	{
		BlacklistFiles,
		B0,
		Wallet,
		TXs,
		GoodTXIDs,
		BadTXIDs ++ BadTXIDs2,
		GoodOffsets2,
		BadOffsets2,
		DataTrees
	}.

setup_slave() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(200), <<>>}]),
	slave_start(B0),
	{B0, Wallet}.

create_txs(Wallet) ->
	lists:foldl(
		fun
			(_, {TXs, DataTrees}) ->
				Chunks =
					lists:sublist(
						ar_tx:chunk_binary(102400, crypto:strong_rand_bytes(1024000)),
						10
					), % Exclude empty chunk created by chunk_to_binary.
				SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(
					ar_tx:chunks_to_size_tagged_chunks(Chunks)
				),
				{DataRoot, DataTree} = ar_merkle:generate_tree(SizedChunkIDs),
				TX = sign_tx(
					Wallet,
					#{
						format => 2,
						data_root => DataRoot,
						data_size => 1024000,
						last_tx => get_tx_anchor(slave)
					}
				),
				{[TX | TXs], maps:put(TX#tx.id, {DataTree, Chunks}, DataTrees)}
		end,
		{[], #{}},
		lists:seq(1, 10)
	).

create_files(BadTXIDs) ->
	Files = [
		{random_filename(), <<>>},
		{random_filename(), <<"bad base64url ">>},
		{random_filename(), ar_util:encode(hd(BadTXIDs))},
		{random_filename(),
			list_to_binary(
				io_lib:format(
					"~s\nbad base64url \n~s\n",
					lists:map(fun ar_util:encode/1, BadTXIDs)
				)
			)}
	],
	lists:foreach(
		fun
			({Filename, Binary}) ->
				ok = file:write_file(Filename, Binary)
		end,
		Files
	),
	[Filename || {Filename, _} <- Files].

random_filename() ->
	filename:join(
		slave_call(ar_meta_db, get, [data_dir]),
		"ar-tx-blacklist-tests-transaction-blacklist-"
		++
		binary_to_list(ar_util:encode(crypto:strong_rand_bytes(32)))
	).

encode_chunk(Proof) ->
	ar_serialize:jsonify(#{
		chunk => ar_util:encode(maps:get(chunk, Proof)),
		data_path => ar_util:encode(maps:get(data_path, Proof)),
		data_root => ar_util:encode(maps:get(data_root, Proof)),
		data_size => integer_to_binary(maps:get(data_size, Proof)),
		offset => integer_to_binary(maps:get(offset, Proof))
	}).

upload_data(TXs, GoodTXIDs, DataTrees) ->
	lists:foreach(
		fun(TX) ->
			#tx{
				id = TXID,
				data_root = DataRoot,
				data_size = DataSize
			} = TX,
			{DataTree, Chunks} = maps:get(TXID, DataTrees),
			ChunkOffsets = lists:zip(Chunks, lists:seq(102400, 1024000, 102400)),
			UploadChunks =
				case lists:member(TXID, GoodTXIDs) of
					true ->
						ChunkOffsets;
					false ->
						[{Chunk, O} || {Chunk, O} <- ChunkOffsets, O rem 204800 == 0]
				end,
			lists:foreach(
				fun({Chunk, Offset}) ->
					DataPath = ar_merkle:generate_path(DataRoot, Offset - 1, DataTree),
					{ok, {{<<"200">>, _}, _, _, _, _}} =
						post_chunk(slave, encode_chunk(#{
							data_root => DataRoot,
							chunk => Chunk,
							data_path => DataPath,
							offset => Offset - 1,
							data_size => DataSize
						}))
				end,
				UploadChunks
			)
		end,
		TXs
	).

assert_present_txs(GoodTXIDs) ->
	true = ar_util:do_until(
		fun() ->
			lists:all(
				fun(TXID) ->
					is_record(ar_storage:read_tx(TXID), tx)
				end,
				GoodTXIDs
			)
		end,
		500,
		10000
	).

assert_removed_txs(BadTXIDs) ->
	true = ar_util:do_until(
		fun() ->
			lists:all(
				fun(TXID) ->
					unavailable == ar_storage:read_tx(TXID)
				end,
				BadTXIDs
			)
		end,
		500,
		10000
	).

assert_present_offsets(GoodOffsets) ->
	true = ar_util:do_until(
		fun() ->
			lists:all(
				fun(Offset) ->
					case get_chunk(Offset) of
						{ok, {{<<"200">>, _}, _, _, _, _}} ->
							true;
						_ ->
							false
					end
				end,
				lists:flatten(GoodOffsets)
			)
		end,
		500,
		60000
	).

assert_removed_offsets(BadOffsets) ->
	true = ar_util:do_until(
		fun() ->
			lists:all(
				fun(Offset) ->
					case get_chunk(Offset) of
						{ok, {{<<"404">>, _}, _, _, _, _}} ->
							true;
						_ ->
							false
					end
				end,
				lists:flatten(BadOffsets)
			)
		end,
		500,
		60000
	).

assert_does_not_accept_offsets(BadOffsets) ->
	true = ar_util:do_until(
		fun() ->
			lists:all(
				fun(Offset) ->
					case get_chunk(Offset) of
						{ok, {{<<"404">>, _}, _, _, _, _}} ->
							{ok, {{<<"200">>, _}, _, EncodedProof, _, _}} =
								get_chunk(slave, Offset),
							Proof = decode_chunk(EncodedProof),
							DataPath = maps:get(data_path, Proof),
							{ok, DataRoot} = ar_merkle:extract_root(DataPath),
							RelativeOffset = ar_merkle:extract_note(DataPath),
							Proof2 = Proof#{
								offset => RelativeOffset - 1,
								data_root => DataRoot,
								data_size => 1024000
							},
							EncodedProof2 = encode_chunk(Proof2),
							%% The node returns 200 but does not store the chunk.
							case post_chunk(EncodedProof2) of
								{ok, {{<<"200">>, _}, _, _, _, _}} ->
									case get_chunk(Offset) of
										{ok, {{<<"404">>, _}, _, _, _, _}} ->
											true;
										_ ->
											false
									end;
								_ ->
									false
							end;
						_ ->
							false
					end
				end,
				lists:flatten(BadOffsets)
			)
		end,
		500,
		60000
	).

decode_chunk(EncodedProof) ->
	ar_serialize:json_map_to_chunk_proof(
		jiffy:decode(EncodedProof, [return_maps])
	).

teardown() ->
	{ok, Config} = application:get_env(arweave, config),
	ok = slave_call(cowboy, stop_listener, [ar_tx_blacklist_test_listener]),
	application:set_env(arweave, config, Config#config{
		transaction_blacklist_files = [],
		transaction_blacklist_urls = [],
		transaction_whitelist_files = []
	}).
