-module(ar_tx_blacklist_tests).

-export([init/2]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-import(ar_test_node, [slave_start/1,
		sign_v1_tx/2, random_v1_data/1, assert_post_tx_to_slave/1,
		assert_post_tx_to_master/1, wait_until_height/1,
		assert_wait_until_height/2, get_chunk/1, get_chunk/2, post_chunk/1, post_chunk/2]).

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
	{timeout, 300, fun test_uses_blacklists/0}.

test_uses_blacklists() ->
	{
		BlacklistFiles,
		B0,
		Wallet,
		TXs,
		GoodTXIDs,
		BadTXIDs,
		V1TX,
		GoodOffsets,
		BadOffsets,
		DataTrees
	} = setup(),
	WhitelistFile = random_filename(),
	ok = file:write_file(WhitelistFile, <<>>),
	RewardAddr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	{_, _} =
		ar_test_node:start(B0, RewardAddr,
				(element(2, application:get_env(arweave, config)))#config{
			transaction_blacklist_files = BlacklistFiles,
			transaction_whitelist_files = [WhitelistFile],
			sync_jobs = 10,
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
	ar_test_node:connect_to_peer(peer1),
	BadV1TXIDs = [V1TX#tx.id],
	lists:foreach(
		fun({TX, Height}) ->
			assert_post_tx_to_slave(TX),
			ar_test_node:assert_wait_until_receives_txs([TX]),
			case Height == length(TXs) of
				true ->
					assert_post_tx_to_slave(V1TX),
					ar_test_node:assert_wait_until_receives_txs([V1TX]);
				_ ->
					ok
			end,
			ar_test_node:mine(peer1),
			upload_data([TX], DataTrees),
			wait_until_height(Height)
		end,
		lists:zip(TXs, lists:seq(1, length(TXs)))
	),
	assert_present_txs(GoodTXIDs),
	assert_present_txs(BadTXIDs), % V2 headers must not be removed.
	assert_removed_txs(BadV1TXIDs),
	assert_present_offsets(GoodOffsets),
	assert_removed_offsets(BadOffsets),
	assert_does_not_accept_offsets(BadOffsets),
	%% Add a new transaction to the blacklist, add a blacklisted transaction to whitelist.
	ok = file:write_file(lists:nth(3, BlacklistFiles), <<>>),
	ok = file:write_file(WhitelistFile, ar_util:encode(lists:nth(2, BadTXIDs))),
	ok = file:write_file(lists:nth(4, BlacklistFiles), io_lib:format("~s~n~s",
			[ar_util:encode(hd(GoodTXIDs)), ar_util:encode(V1TX#tx.id)])),
	[UnblacklistedOffsets, WhitelistOffsets | BadOffsets2] = BadOffsets,
	RestoredOffsets = [UnblacklistedOffsets, WhitelistOffsets] ++
			[lists:nth(6, lists:reverse(BadOffsets))],
	BadOffsets3 = BadOffsets2 -- [lists:nth(6, lists:reverse(BadOffsets))],
	[_UnblacklistedTXID, _WhitelistTXID | BadTXIDs2] = BadTXIDs,
	%% Expect the transaction data to be resynced.
	assert_present_offsets(RestoredOffsets),
	%% Expect the freshly blacklisted transaction to be erased.
	assert_present_txs([hd(GoodTXIDs)]), % V2 headers must not be removed.
	assert_removed_offsets([hd(GoodOffsets)]),
	assert_does_not_accept_offsets([hd(GoodOffsets)]),
	%% Expect the previously blacklisted transactions to stay blacklisted.
	assert_present_txs(BadTXIDs2), % V2 headers must not be removed.
	assert_removed_txs(BadV1TXIDs),
	assert_removed_offsets(BadOffsets3),
	assert_does_not_accept_offsets(BadOffsets3),
	%% Blacklist the last transaction. Fork the weave. Assert the blacklisted offsets are moved.
	ar_test_node:disconnect_from(peer1),
	TX = ar_test_node:sign_tx(Wallet, #{ data => crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
			last_tx => ar_test_node:get_tx_anchor(slave) }),
	assert_post_tx_to_master(TX),
	ar_test_node:mine(),
	[{_, WeaveSize, _} | _] = wait_until_height(length(TXs) + 1),
	assert_present_offsets([[WeaveSize]]),
	ok = file:write_file(lists:nth(3, BlacklistFiles), ar_util:encode(TX#tx.id)),
	assert_removed_offsets([[WeaveSize]]),
	TX2 = sign_v1_tx(Wallet, #{ data => random_v1_data(2 * ?DATA_CHUNK_SIZE),
			last_tx => ar_test_node:get_tx_anchor(slave) }),
	assert_post_tx_to_slave(TX2),
	ar_test_node:mine(peer1),
	assert_wait_until_height(peer1, length(TXs) + 1),
	assert_post_tx_to_slave(TX),
	ar_test_node:mine(peer1),
	assert_wait_until_height(peer1, length(TXs) + 2),
	ar_test_node:connect_to_peer(peer1),
	[{_, WeaveSize2, _} | _] = wait_until_height(length(TXs) + 2),
	assert_removed_offsets([[WeaveSize2]]),
	assert_present_offsets([[WeaveSize]]),
	teardown().

setup() ->
	{B0, Wallet} = setup_slave(),
	{TXs, DataTrees} = create_txs(Wallet),
	TXIDs = [TX#tx.id || TX <- TXs],
	BadTXIDs = [lists:nth(1, TXIDs), lists:nth(3, TXIDs)],
	V1TX = sign_v1_tx(Wallet, #{ data => random_v1_data(3 * ?DATA_CHUNK_SIZE),
			last_tx => ar_test_node:get_tx_anchor(slave), reward => ?AR(10000) }),
	DataSizes = [TX#tx.data_size || TX <- TXs],
	S0 = B0#block.block_size,
	[S1, S2, S3, S4, S5, S6, S7, S8 | _] = DataSizes,
	BadOffsets = [S0 + O || O <- [S1, S1 + S2 + S3, % Blacklisted in the file.
			S1 + S2 + S3 + S4 + S5,
			S1 + S2 + S3 + S4 + S5 + S6 + S7]], % Blacklisted in the endpoint.
	BlacklistFiles = create_files([V1TX#tx.id | BadTXIDs],
			[{S0 + S1 + S2 + S3 + ?DATA_CHUNK_SIZE, S0 + S1 + S2 + S3 + ?DATA_CHUNK_SIZE * 2},
				{S0 + S1 + S2 + S3 + S4 + S5,
						S0 + S1 + S2 + S3 + S4 + S5 + ?DATA_CHUNK_SIZE * 5},
				% This one just repeats the range of a blacklisted tx:
				{S0 + S1 + S2 + S3 + S4 + S5 + S6, S0 + S1 + S2 + S3 + S4 + S5 + S6 + S7}
			]),
	BadTXIDs2 = [lists:nth(5, TXIDs), lists:nth(7, TXIDs)], % The endpoint.
	BadTXIDs3 = [lists:nth(4, TXIDs), lists:nth(6, TXIDs)], % Ranges.
	Routes = [{"/[...]", ar_tx_blacklist_tests, BadTXIDs2}],
	{ok, _PID} =
		ar_test_node:remote_call(peer1, cowboy, start_clear, [
			ar_tx_blacklist_test_listener,
			[{port, 1985}],
			#{ env => #{ dispatch => cowboy_router:compile([{'_', Routes}]) } }
		]),
	GoodTXIDs = TXIDs -- (BadTXIDs ++ BadTXIDs2 ++ BadTXIDs3),
	BadOffsets2 =
		lists:map(
			fun(TXOffset) ->
				%% Every TX in this test consists of 10 chunks.
				%% Only every second chunk is uploaded in this test
				%% for (originally) blacklisted transactions.
				[TXOffset - ?DATA_CHUNK_SIZE * I || I <- lists:seq(0, 9, 2)]
			end,
			BadOffsets
		),
	BadOffsets3 = BadOffsets2 ++ [S0 + O || O <- [S1 + S2 + S3 + ?DATA_CHUNK_SIZE * 2,
			S1 + S2 + S3 + S4 + S5 + ?DATA_CHUNK_SIZE,
			S1 + S2 + S3 + S4 + S5 + ?DATA_CHUNK_SIZE * 2,
			S1 + S2 + S3 + S4 + S5 + ?DATA_CHUNK_SIZE * 3,
			S1 + S2 + S3 + S4 + S5 + ?DATA_CHUNK_SIZE * 4,
			S1 + S2 + S3 + S4 + S5 + ?DATA_CHUNK_SIZE * 5]], % Blacklisted as a range.
	GoodOffsets = [S0 + O || O <- [S1 + S2, S1 + S2 + S3 + S4, S1 + S2 + S3 + S4 + S5 + S6,
			S1 + S2 + S3 + S4 + S5 + S6 + S7 + S8]],
	GoodOffsets2 =
		lists:map(
			fun(TXOffset) ->
				%% Every TX in this test consists of 10 chunks.
				[TXOffset - ?DATA_CHUNK_SIZE * I || I <- lists:seq(0, 9)] -- BadOffsets3
			end,
			GoodOffsets
		),
	{
		BlacklistFiles,
		B0,
		Wallet,
		TXs,
		GoodTXIDs,
		BadTXIDs ++ BadTXIDs2 ++ BadTXIDs3,
		V1TX,
		GoodOffsets2,
		BadOffsets3,
		DataTrees
	}.

setup_slave() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(100000000), <<>>}]),
	ar_test_node:start_peer(peer1, B0),
	{B0, Wallet}.

create_txs(Wallet) ->
	lists:foldl(
		fun
			(_, {TXs, DataTrees}) ->
				Chunks =
					lists:sublist(
						ar_tx:chunk_binary(?DATA_CHUNK_SIZE,
								crypto:strong_rand_bytes(10 * ?DATA_CHUNK_SIZE)),
						10
					), % Exclude empty chunk created by chunk_to_binary.
				SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(
					ar_tx:chunks_to_size_tagged_chunks(Chunks)
				),
				{DataRoot, DataTree} = ar_merkle:generate_tree(SizedChunkIDs),
				TX = ar_test_node:sign_tx(Wallet, #{ format => 2, data_root => DataRoot,
						data_size => 10 * ?DATA_CHUNK_SIZE, last_tx => ar_test_node:get_tx_anchor(slave),
						reward => ?AR(10000), denomination => 1 }),
				{[TX | TXs], maps:put(TX#tx.id, {DataTree, Chunks}, DataTrees)}
		end,
		{[], #{}},
		lists:seq(1, 10)
	).

create_files(BadTXIDs, [{Start1, End1}, {Start2, End2}, {Start3, End3}]) ->
	Files = [
		{random_filename(), <<>>},
		{random_filename(), <<"bad base64url ">>},
		{random_filename(), ar_util:encode(lists:nth(2, BadTXIDs))},
		{random_filename(),
			list_to_binary(
				io_lib:format(
					"~s\nbad base64url \n~s\n~s\n~B,~B\n",
					lists:map(fun ar_util:encode/1, BadTXIDs) ++ [Start1, End1]
				)
			)},
		{random_filename(), list_to_binary(io_lib:format("~B,~B\n~B,~B",
				[Start2, End2, Start3, End3]))}
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
	{ok, Config} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
	filename:join(Config#config.data_dir,
		"ar-tx-blacklist-tests-transaction-blacklist-"
		++
		binary_to_list(ar_util:encode(crypto:strong_rand_bytes(32)))).

encode_chunk(Proof) ->
	ar_serialize:jsonify(#{
		chunk => ar_util:encode(maps:get(chunk, Proof)),
		data_path => ar_util:encode(maps:get(data_path, Proof)),
		data_root => ar_util:encode(maps:get(data_root, Proof)),
		data_size => integer_to_binary(maps:get(data_size, Proof)),
		offset => integer_to_binary(maps:get(offset, Proof))
	}).

upload_data(TXs, DataTrees) ->
	lists:foreach(
		fun(TX) ->
			#tx{
				id = TXID,
				data_root = DataRoot,
				data_size = DataSize
			} = TX,
			{DataTree, Chunks} = maps:get(TXID, DataTrees),
			ChunkOffsets = lists:zip(Chunks,
					lists:seq(?DATA_CHUNK_SIZE, 10 * ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE)),
			UploadChunks = ChunkOffsets,
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
	?debugFmt("Waiting until these txids are stored: ~p.",
			[[ar_util:encode(TXID) || TXID <- GoodTXIDs]]),
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
	),
	lists:foreach(
		fun(TXID) ->
			?assertMatch({ok, {_, _}}, ar_storage:get_tx_confirmation_data(TXID))
		end,
		GoodTXIDs
	).

assert_removed_txs(BadTXIDs) ->
	?debugFmt("Waiting until these txids are removed: ~p.",
			[[ar_util:encode(TXID) || TXID <- BadTXIDs]]),
	true = ar_util:do_until(
		fun() ->
			lists:all(
				fun(TXID) ->
					{error, not_found} == ar_data_sync:get_tx_data(TXID)
							%% Do not use ar_storage:read_tx because the
							%% transaction is temporarily kept in the disk cache,
							%% even when blacklisted.
							andalso ar_kv:get(tx_db, TXID) == not_found
				end,
				BadTXIDs
			)
		end,
		500,
		30000
	),
	%% We have to keep the confirmation data even for blacklisted transactions.
	lists:foreach(
		fun(TXID) ->
			?assertMatch({ok, {_, _}}, ar_storage:get_tx_confirmation_data(TXID))
		end,
		BadTXIDs
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
							?debugFmt("Waiting until the end offset ~B is stored.", [Offset]),
							false
					end
				end,
				lists:flatten(GoodOffsets)
			)
		end,
		500,
		120000
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
							?debugFmt("Waiting until the end offset ~B is removed.", [Offset]),
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
								data_size => 10 * ?DATA_CHUNK_SIZE
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
	ok = ar_test_node:remote_call(peer1, cowboy, stop_listener, [ar_tx_blacklist_test_listener]),
	application:set_env(arweave, config, Config#config{
		transaction_blacklist_files = [],
		transaction_blacklist_urls = [],
		transaction_whitelist_files = []
	}).
