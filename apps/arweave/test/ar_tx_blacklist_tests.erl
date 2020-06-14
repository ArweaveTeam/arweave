-module(ar_tx_blacklist_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blacklist/ar_tx_blacklist.hrl").

-import(ar_test_node, [
	start/1,
	sign_tx/2,
	wait_until_height/2,
	post_and_mine/2,
	get_tx_anchor/1,
	assert_post_tx_to_master/2
]).

removes_blacklisted_txs_test_() ->
	{timeout, 60, fun removes_blacklisted_txs/0}.

removes_blacklisted_txs() ->
	AR = ?AR(20),
	Key1 = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	Key3 = {_, Pub3} = ar_wallet:new(),
	GoodTX = sign_tx(Key1, #{data => <<>>}),
	ToDelTX1 = sign_tx(Key2, #{data => <<>>}),
	ToDelTX2 = sign_tx(Key3, #{data => <<>>}),
	Data = <<(ar_util:encode(ToDelTX1#tx.id))/binary, "\n", (ar_util:encode(ToDelTX2#tx.id))/binary>>,
	FullFilePath = write_file(Data),
	UrlPath = "/mock/policy_content/" ++ FullFilePath,
	[Block] = ar_weave:init([{ar_wallet:to_address(Pub1), AR, <<>>}, {ar_wallet:to_address(Pub2), AR, <<>>}, {ar_wallet:to_address(Pub3), AR, <<>>}]),
	{Master, Block} = start(Block),
	assert_post_tx_to_master(Master, GoodTX),
	assert_post_tx_to_master(Master, ToDelTX1),
	assert_post_tx_to_master(Master, ToDelTX2),
	ar_node:mine(Master),
	wait_until_height(Master, 1),
	?assertEqual(ToDelTX1, ar_storage:read_tx(ToDelTX1#tx.id)),
	?assertEqual(ToDelTX2, ar_storage:read_tx(ToDelTX2#tx.id)),
	ar_meta_db:put(content_policy_provider_urls, ["http://localhost:1984" ++ UrlPath]),
	timer:sleep(5000),
	ok = file:delete(FullFilePath),
	?assertEqual(GoodTX, ar_storage:read_tx(GoodTX#tx.id)),
	?assertEqual(unavailable, ar_storage:read_tx(ToDelTX1#tx.id)),
	?assertEqual(unavailable, ar_storage:read_tx(ToDelTX2#tx.id)).

delete_tx_data_basic_aut_test_() ->
	{timeout, 60, fun test_delete_tx_data_with_basic_auth/0}.

test_delete_tx_data_with_basic_auth() ->
	{Master, _, Wallet} = ar_data_sync_tests:setup_nodes(),
	DataSize = 10000,
	OutOfBoundsOffsetChunk = crypto:strong_rand_bytes(DataSize),
	ChunkID = ar_tx:generate_chunk_id(OutOfBoundsOffsetChunk),
	{DataRoot, DataTree} = ar_merkle:generate_tree([{ChunkID, DataSize + 1}]),
	TX = sign_tx(
		Wallet,
		#{ last_tx => get_tx_anchor(master), data_size => DataSize, data_root => DataRoot }
	),
	post_and_mine(#{ miner => {master, Master}, await_on => {master, Master} }, [TX]),
	DataPath = ar_merkle:generate_path(DataRoot, 0, DataTree),
	Proof = #{
		data_root => ar_util:encode(DataRoot),
		data_path => ar_util:encode(DataPath),
		chunk => ar_util:encode(OutOfBoundsOffsetChunk),
		offset => <<"0">>,
		data_size => integer_to_binary(DataSize)
	},
	Data = <<(ar_util:encode(TX#tx.id))/binary, "\n">>,
	FullFilePath = write_file(Data),
	UrlPath = "/mock/policy_content/auth/" ++ FullFilePath,
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_data_sync_tests:post_chunk(jiffy:encode(Proof))
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_data_sync_tests:get_chunk(DataSize)
	),
	ar_meta_db:put(content_policy_provider_urls, ["http://user:pass@localhost:1984" ++ UrlPath]),
	timer:sleep(5000),
	ok = file:delete(FullFilePath),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		ar_data_sync_tests:get_chunk(DataSize)
	).

scan_transactions_test_() ->
	{timeout, 60, fun test_scan_transactions/0}.

test_scan_transactions() ->
	TX = ar_tx:new(),
	GoodTXs = [
		TX#tx{ data = <<"goodstuff">> },
		TX#tx{ tags = [{<<"goodstuff">>, <<"goodstuff">>}] },
		TX#tx{ target = <<"goodstuff">> }
	],
	BadID_0 = <<"badtxid_0">>,
	BadID_1 = <<"badtxid_1">>,
	BadID_2 = <<"badtxid_2">>,
	BadID_3 = <<"badtxid_3">>,
	BadID_4 = <<"badtxid_4">>,
	BadTXs = [
		TX#tx{ id = BadID_0, data = <<"badstuff">> },
		TX#tx{ id = BadID_1, tags = [{<<"badstuff">>, <<"goodstuff">>}] },
		TX#tx{ id = BadID_2, tags = [{<<"goodstuff">>, <<"badstuff">>}] },
		TX#tx{ id = BadID_3, target = <<"badstuff">> },
		TX#tx{ id = BadID_4, data = <<"badstuff">> }
	],
	Data = <<(ar_util:encode(BadID_0))/binary, "\n", (ar_util:encode(BadID_1))/binary, "\n",
	          (ar_util:encode(BadID_2))/binary, "\n", (ar_util:encode(BadID_3))/binary, "\n",
	          (ar_util:encode(BadID_4))/binary, "\n">>,
	FullFilePath = write_file(Data),
	ar_meta_db:put(content_policy_provider_urls, ["http://localhost:1984/mock/policy_content/" ++ FullFilePath]),
	timer:sleep(1500),
	lists:foreach(
		fun(BadTX) ->
			?assertEqual(reject, ar_tx_blacklist:scan_tx(BadTX))
		end,
		BadTXs
	),
	ar_meta_db:put(content_policy_provider_urls, []),
	lists:foreach(
		fun(GoodTX) ->
			?assertEqual(accept, ar_tx_blacklist:scan_tx(GoodTX))
		end,
		GoodTXs
	).

scan_signatures_test() ->
	Sig = #sig{
		name = "Test",
		type = binary,
		data = #binary_sig{
			target_type = "0",
			offset = any,
			binary = <<"badstuff">>
		}
	},
	ContentSigs = {[Sig], binary:compile_pattern([(Sig#sig.data)#binary_sig.binary])},
	TX = ar_tx:new(),
	GoodTXs = [
		TX#tx{ data = <<"goodstuff">> },
		TX#tx{ tags = [{<<"goodstuff">>, <<"goodstuff">>}] },
		TX#tx{ target = <<"goodstuff">> }
	],
	BadTXs = [
		TX#tx{ data = <<"badstuff">> },
		TX#tx{ tags = [{<<"badstuff">>, <<"goodstuff">>}] },
		TX#tx{ tags = [{<<"goodstuff">>, <<"badstuff">>}] },
		TX#tx{ target = <<"badstuff">> },
		TX#tx{ id = <<"badtxid">>, data = <<"badstuff">> }
	],
	lists:foreach(
		fun(BadTX) ->
			?assertEqual(reject, ar_tx_blacklist:verify_content_sigs(BadTX, ContentSigs))
		end,
		BadTXs
	),
	lists:foreach(
		fun(GoodTX) ->
			?assertEqual(accept, ar_tx_blacklist:verify_content_sigs(GoodTX, ContentSigs))
		end,
		GoodTXs
	).

scan_and_clean_disk_test_() ->
	{timeout, 60, fun test_scan_and_clean_disk/0}.

test_scan_and_clean_disk() ->
	GoodTXID = <<"goodtxid">>,
	BadTXID = <<"badtxid1">>,
	BadDataTXID = <<"badtxid">>,
	TagName = <<"name">>,
	TagValue = <<"value">>,
	Tags = [{TagName, TagValue}],
	%% Blacklist a transaction, write it to disk and add it to the index.
	BadTX = (ar_tx:new())#tx{ id = BadTXID, tags = Tags },
	ar_storage:write_tx(BadTX),
	?assertEqual(BadTXID, (ar_storage:read_tx(BadTXID))#tx.id),
	ar_meta_db:put(transaction_blacklist_files, [fixture("test_transaction_blacklist.txt")]),
	%% Setup a content policy, write a bad tx to disk and add it to the index.
	BadTX2 = (ar_tx:new())#tx{ id = BadDataTXID, data = <<"BADCONTENT1">>, tags = Tags },
	ar_storage:write_tx(BadTX2),
	?assertEqual(BadDataTXID, (ar_storage:read_tx(BadDataTXID))#tx.id),
	ar_meta_db:put(content_policy_files, [fixture("test_sig.txt")]),
	%% Write a good tx to disk and add it to the index.
	GoodTX = (ar_tx:new())#tx{ id = GoodTXID, tags = Tags },
	ar_storage:write_tx(GoodTX),
	?assertEqual(GoodTXID, (ar_storage:read_tx(GoodTXID))#tx.id),
	%% Write a file that is not a tx to the transaction directory.
	NotTXFile = filename:join([ar_meta_db:get(data_dir), ?TX_DIR, "not_a_tx"]),
	ok = file:write_file(NotTXFile, <<"not a tx">>),
	%% Assert illicit txs and only they are removed by the cleanup procedure.
	?assertEqual(GoodTXID, (ar_storage:read_tx(GoodTXID))#tx.id),
	{ok, <<"not a tx">>} = file:read_file(NotTXFile),
	timer:sleep(1500),
	ar_tx_blacklist:scan_and_clean_disk(),
	?assertEqual(unavailable, ar_storage:read_tx(BadTXID)),
	?assertEqual(unavailable, ar_storage:read_tx(BadDataTXID)).

%%% ==================================================================
%%% Internal functions
%%% ==================================================================

gen_filename() ->
	DirPath = filename:join(ar_meta_db:get(data_dir), "content_policy_provider"),
	ok = filelib:ensure_dir(DirPath ++ "/"),
	FileName = binary_to_list(<<(base64:encode(crypto:strong_rand_bytes(16)))/binary, ".txt">>),
	filename:join(DirPath, FileName).

fixture(Filename) ->
	filename:dirname(?FILE) ++ "/../test/" ++ Filename.

write_file(Data) ->
	FullFilePath = gen_filename(),
	case file:write_file(FullFilePath, Data) of
		ok ->
			FullFilePath;
		_ ->
			write_file(Data)
	end.
