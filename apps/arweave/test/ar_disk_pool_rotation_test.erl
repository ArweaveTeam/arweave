%% @ar_test: isolated
-module(ar_disk_pool_rotation_test).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include("ar.hrl").
-include("ar_consensus.hrl").

disk_pool_rotation_test_() ->
	{timeout, 240, fun test_disk_pool_rotation/0}.

test_disk_pool_rotation() ->
	?LOG_DEBUG([{event, test_disk_pool_rotation_start}]),
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	%% Store the three genesis chunks + an extra chunk to cover the
	%% long-term storage vicinity around the weave size at the time of posting.
	StorageModules = [{4 * ?DATA_CHUNK_SIZE, 0,
			ar_test_node:get_default_storage_module_packing(Addr, 0)}],
	Wallet = ar_test_data_sync:setup_nodes(
			#{ addr => Addr, [storage_modules] => [arweave_config:storage_module_to_config(ConfigModule) || ConfigModule <- StorageModules] }),
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
	?assertMatch({ok, _}, ar_test_await:node_height(main, 1)),
	timer:sleep(2_000),
	Options = #{ format => etf, random_subset => false },
	Global1 = get_global_sync_record(Options),
	?assertEqual(Expected, ar_intervals:intersection(Global1, Expected)),
	ar_test_node:mine(main),
	?assertMatch({ok, _}, ar_test_await:node_height(main, 2)),
	Global2 = get_global_sync_record(Options),
	?assertEqual(Expected, ar_intervals:intersection(Global2, Expected)),
	ar_test_node:mine(main),
	?assertMatch({ok, _}, ar_test_await:node_height(main, 3)),
	ar_test_node:mine(main),
	?assertMatch({ok, _}, ar_test_await:node_height(main, 4)),
	?assertEqual(3, ?SEARCH_SPACE_UPPER_BOUND_DEPTH),
	ok = ar_test_await:global_sync_record_excludes(Options, Expected).

get_global_sync_record(Options) ->
	{ok, Binary} = ar_global_sync_record:get_serialized_sync_record(Options),
	{ok, Global} = ar_intervals:safe_from_etf(Binary),
	Global.
