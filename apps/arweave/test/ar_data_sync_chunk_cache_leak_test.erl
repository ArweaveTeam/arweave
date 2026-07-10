-module(ar_data_sync_chunk_cache_leak_test).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").
-include("ar_consensus.hrl").

%%% Fetched chunks that fail unpacking must not leak chunk cache counts:
%%% ar_data_sync's unpack_error handler has to decrement the cache size like
%%% every other terminal path, or the leaked counts exceed
%%% [sync, cache_size_limit] and ar_sync_dispatcher stops dispatching
%%% forever. The test corrupts a packed chunk on peer1 and requires that a
%%% valid chunk appearing afterwards still syncs.

chunk_cache_leak_on_unpack_error_test_() ->
	{timeout, 600, fun test_chunk_cache_leak_on_unpack_error/0}.

test_chunk_cache_leak_on_unpack_error() ->
	Addr = ar_test_node:generate_address(main),
	PeerAddr = ar_test_node:generate_address(peer1),
	%% Main stores unpacked, so everything fetched from peer1 (packed) goes
	%% through the unpack request path. With the limit of 1 a single leaking
	%% retry stalls the node.
	Wallet = ar_test_data_sync:setup_nodes(#{
		addr => Addr,
		peer_addr => PeerAddr,
		config => #{
			[sync, cache_size_limit] => 1,
			[storage_modules] => [arweave_config:storage_module_to_config(
					{10 * ar_block:partition_size(), 0, unpacked})]
		},
		peer_config => ar_test_node:storage_module_config(PeerAddr, [0])
	}),
	%% Fill the weave up to the strict data split threshold so both target
	%% chunks land bucket-padded in peer1's ar_chunk_storage.
	StrictThreshold = ar_block:strict_data_split_threshold(),
	?assertEqual(0, StrictThreshold rem ?DATA_CHUNK_SIZE),
	FillerChunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)
			|| _ <- lists:seq(1, StrictThreshold div ?DATA_CHUNK_SIZE)],
	mine_block_with_chunks(Wallet, FillerChunks),
	%% Smaller than ?DATA_CHUNK_SIZE: the packed form is zero-padded, so
	%% garbage packed bytes fail unpacking with invalid_padding.
	{CorruptedB, CorruptedTX, CorruptedChunks} =
		mine_block_with_chunks(Wallet, [crypto:strong_rand_bytes(100 * 1024)]),
	{ValidB, ValidTX, ValidChunks} =
		mine_block_with_chunks(Wallet, [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)]),
	[{CorruptedEndOffset, CorruptedProof}] =
		ar_test_data_sync:build_proofs(CorruptedB, CorruptedTX, CorruptedChunks),
	[{ValidEndOffset, ValidProof}] =
		ar_test_data_sync:build_proofs(ValidB, ValidTX, ValidChunks),
	%% Push the disk pool threshold past both chunks so peer1 stores posted
	%% proofs packed and main network-syncs the range.
	mine_block_with_chunks(Wallet, [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)]),
	mine_until_below_disk_pool_threshold(ValidEndOffset, 10),
	%% Keep peer1's data out of main's reach while we set the scene.
	ar_test_node:disconnect_from(peer1),
	PeerPacking = ar_test_node:storage_module_packing(PeerAddr, 0),
	post_chunk_to_peer1(CorruptedProof),
	ok = ar_test_await:chunk_recorded(peer1, CorruptedEndOffset,
			#{ packing => PeerPacking }),
	corrupt_stored_chunk(CorruptedEndOffset),
	ar_test_node:connect_to_peer(peer1),
	%% Ordering gate, not an assertion: with the leak present the cache size
	%% settles over the limit here, guaranteeing the cache is exhausted
	%% before the valid chunk appears. With correct accounting it drains
	%% after every retry and the gate just times out.
	_ = ar_test_await:until(chunk_cache_over_limit,
			fun() -> chunk_cache_size(main) >= 2 end, 60_000),
	%% Make the valid chunk available on peer1 and require main to sync it:
	%% the unpack failures must not exhaust the chunk cache budget.
	post_chunk_to_peer1(ValidProof),
	ok = ar_test_await:chunk_recorded(peer1, ValidEndOffset,
			#{ packing => PeerPacking }),
	ok = ar_test_await:until(valid_chunk_synced,
			fun() ->
				ar_sync_record:is_recorded(ValidEndOffset, ar_data_sync) =/= false
			end, 60_000),
	%% At most the corrupted chunk retry stays in flight.
	ok = ar_test_await:until(chunk_cache_drained,
			fun() -> chunk_cache_size(main) =< 1 end),
	?assertNot(ar_test_node:remote_call(main, ar_data_sync, is_chunk_cache_full, [])).

%% @doc Mine one block on peer1 with a single fixed-data v2 tx carrying Chunks.
mine_block_with_chunks(Wallet, Chunks) ->
	{DataRoot, _DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks))),
	{TX, _} = ar_test_data_sync:tx(#{
		wallet => Wallet,
		split_type => {fixed_data, DataRoot, Chunks},
		format => v2,
		reward => fetch,
		tx_anchor_peer => peer1,
		get_fee_peer => peer1 }),
	B = ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 }, [TX]),
	?assertMatch({ok, _}, ar_test_await:node_height(main, B#block.height)),
	{B, TX, Chunks}.

%% @doc Mine empty blocks on peer1 until the disk pool threshold (the weave
%% size a few blocks back) covers Offset on both nodes.
mine_until_below_disk_pool_threshold(_Offset, 0) ->
	?assert(false, "The disk pool threshold did not reach the target offset.");
mine_until_below_disk_pool_threshold(Offset, RetryCount) ->
	MainThreshold = ar_test_node:remote_call(main, ar_disk_pool, get_threshold, []),
	PeerThreshold = ar_test_node:remote_call(peer1, ar_disk_pool, get_threshold, []),
	case MainThreshold >= Offset andalso PeerThreshold >= Offset of
		true ->
			ok;
		false ->
			Height = ar_test_node:remote_call(peer1, ar_node, get_height, []),
			ar_test_node:mine(peer1),
			?assertMatch({ok, _}, ar_test_await:node_height(peer1, Height + 1)),
			?assertMatch({ok, _}, ar_test_await:node_height(main, Height + 1)),
			mine_until_below_disk_pool_threshold(Offset, RetryCount - 1)
	end.

post_chunk_to_peer1(Proof) ->
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(peer1, ar_serialize:jsonify(Proof))).

%% @doc Overwrite the packed chunk bytes in peer1's chunk storage. Metadata
%% and sync records stay intact, so peer1 keeps serving the chunk.
corrupt_stored_chunk(EndOffset) ->
	PaddedEndOffset = ar_block:get_chunk_padded_offset(EndOffset),
	[StorageModule | _] = ar_test_node:remote_call(peer1, ar_storage_module,
			get_all, [PaddedEndOffset]),
	StoreID = ar_storage_module:id(StorageModule),
	Garbage = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	?assertMatch({ok, _}, ar_test_node:remote_call(peer1, ar_chunk_storage,
			write_chunk, [PaddedEndOffset, Garbage, #{}, StoreID])).

chunk_cache_size(Node) ->
	case ar_test_node:remote_call(Node, ets, lookup,
			[ar_data_sync_state, chunk_cache_size]) of
		[{chunk_cache_size, Size}] -> Size;
		_ -> 0
	end.
