-module(ar_data_sync_disk_pool_rotation_test).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").
-include("ar_consensus.hrl").
-include("ar_config.hrl").

-import(ar_test_node, [assert_wait_until_height/2]).

disk_pool_rotation_test_() ->
	{timeout, 120, fun test_disk_pool_rotation/0}.

test_disk_pool_rotation() ->
	?LOG_DEBUG([{event, test_disk_pool_rotation_start}]),
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	%% Will store the three genesis chunks.
	%% The third one falls inside the "overlap" (see ar_storage_module.erl)
	StorageModules = [{2 * ?DATA_CHUNK_SIZE, 0,
			ar_test_node:get_default_storage_module_packing(Addr, 0)}],
	Wallet = ar_test_data_sync:setup_nodes(
			#{ addr => Addr, storage_modules => StorageModules }),
	Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)],
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	{TX, Chunks} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	Offset = ?DATA_CHUNK_SIZE,
	DataSize = ?DATA_CHUNK_SIZE,
	DataPath = ar_merkle:generate_path(DataRoot, Offset, DataTree),
	Proof = #{ data_root => ar_util:encode(DataRoot),
			data_path => ar_util:encode(DataPath),
			chunk => ar_util:encode(hd(Chunks)),
			offset => integer_to_binary(Offset),
			data_size => integer_to_binary(DataSize) },
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
			ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 1),
	timer:sleep(2_000),
	Options = #{ format => etf, random_subset => false },
	{ok, Binary1} = ar_global_sync_record:get_serialized_sync_record(Options),
	{ok, Global1} = ar_intervals:safe_from_etf(Binary1),
	%% 3 genesis chunks are packed with the replica 2.9 format and therefore stored
	%% in the footprint record and not here.
	?assertEqual([{1048576, 786432}], ar_intervals:to_list(Global1)),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 2),
	{ok, Binary2} = ar_global_sync_record:get_serialized_sync_record(Options),
	{ok, Global2} = ar_intervals:safe_from_etf(Binary2),
	?assertEqual([{1048576, 786432}], ar_intervals:to_list(Global2)),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 3),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 4),
	%% The new chunk has been confirmed but there is not storage module to take it.
	?assertEqual(3, ?SEARCH_SPACE_UPPER_BOUND_DEPTH),
	true = ar_util:do_until(
		fun() ->
			{ok, Binary3} = ar_global_sync_record:get_serialized_sync_record(Options),
			{ok, Global3} = ar_intervals:safe_from_etf(Binary3),
			[] == ar_intervals:to_list(Global3)
		end,
		200,
		5000
	). 