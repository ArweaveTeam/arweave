-module(ar_data_roots_sync_tests).

-include("ar.hrl").
-include("ar_data_sync.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include_lib("eunit/include/eunit.hrl").

%%% Group: data roots metadata only (GET/POST /data_roots, sync from chain — no /chunk roundtrip).
%%% ---------------------------------------------------------------------------

%% Data roots sync from a peer via ar_data_root_sync when main joins with partial storage (not header sync).
data_roots_sync_from_peer_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_block, get_consensus_window_size, fun() -> 5 end},
			{ar_block, get_max_tx_anchor_depth, fun() -> 5 end},
			{ar_storage_module, get_overlap, fun(_Packing) -> 0 end}],
		fun test_data_roots_sync_from_peer/0).

%% Data roots pushed with HTTP: GET /data_roots from miner, POST /data_roots to peer; assert metadata via GET.
data_roots_http_post_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_block, get_consensus_window_size, fun() -> 5 end},
			{ar_block, get_max_tx_anchor_depth, fun() -> 5 end},
			{ar_storage_module, get_overlap, fun(_Packing) -> 0 end}],
		fun test_data_roots_http_post/0).

%%% Group: chunk POST/GET after data roots are available on the peer.
%%% ---------------------------------------------------------------------------

%% HTTP share of roots then chunk roundtrip: per block, GET roots from miner, POST to main, POST/GET /chunk.
chunk_after_data_roots_http_post_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_block, get_consensus_window_size, fun() -> 5 end},
			{ar_block, get_max_tx_anchor_depth, fun() -> 5 end},
			{ar_storage_module, get_overlap, fun(_Packing) -> 0 end}],
		fun test_chunk_after_data_roots_http_post/0).

%% Background ar_data_root_sync + header_sync_jobs > 0; then POST/GET /chunk (regression: POST 200, GET 404).
chunk_after_data_roots_background_sync_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_block, get_consensus_window_size, fun() -> 5 end},
			{ar_block, get_max_tx_anchor_depth, fun() -> 5 end},
			{ar_storage_module, get_overlap, fun(_Packing) -> 0 end}],
		fun test_chunk_after_data_roots_background_sync/0).

%% Background sync completes, but a block in an unconfigured partition still requires a
%% manual POST /data_roots before POST /chunk can be accepted temporarily into the disk pool.
chunk_in_unconfigured_partition_requires_manual_data_roots_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_block, get_consensus_window_size, fun() -> 5 end},
			{ar_block, get_max_tx_anchor_depth, fun() -> 5 end},
			{ar_storage_module, get_overlap, fun(_Packing) -> 0 end}],
		fun test_chunk_in_unconfigured_partition_requires_manual_data_roots/0).

chunk_skipped_with_duplicate_data_root_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_block, get_consensus_window_size, fun() -> 5 end},
			{ar_block, get_max_tx_anchor_depth, fun() -> 5 end},
			{ar_storage_module, get_overlap, fun(_Packing) -> 0 end}],
		fun test_chunk_skipped_with_duplicate_data_root/0).

chunk_skipped_with_depth_exhaustion_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_block, get_consensus_window_size, fun() -> 5 end},
			{ar_block, get_max_tx_anchor_depth, fun() -> 5 end},
			{ar_storage_module, get_overlap, fun(_Packing) -> 0 end}],
		fun test_chunk_skipped_with_depth_exhaustion/0).

test_data_roots_sync_from_peer() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(2_000_000_000_000_000), <<>>}]),

	%% Peer1 will mine while main stays disconnected until join_on later.
	start_peers_then_disconnect(main, peer1, B0),

	%% Mine blocks with transactions with data on peer1 BEFORE main joins.
	BlocksBeforeJoin = lists:map(
		fun(_) ->
			Data = generate_random_txs(Wallet),
			TXs = [TX || {TX, _} <- Data],
			ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 }, TXs)
		end,
		lists:seq(1, 3)
	),
	%% The node fetches this many latest blocks after joining the network.
	%% We want all our data blocks be older so that the node has to use
	%% the data root syncing mechanism to fetch data roots (we explicitly
	%% assert the unexpected data roots are not synced further down here).
	?assertEqual(10, 2 * ar_block:get_max_tx_anchor_depth()),
	Blocks = BlocksBeforeJoin ++ lists:map(
			fun(_) ->
				Data = generate_random_txs(Wallet),
				TXs = [TX || {TX, _} <- Data],
				ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 }, TXs)
			end,
			lists:seq(1, 11)
		),

	%% Now start main (node A) with header syncing disabled and storage modules covering PART of the range.
	{ok, BaseConfig} = arweave_config:get_env(),
	MainRewardAddr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	%% Cover only the first partition and half of the second one to ensure partial coverage.
	MainConfig = BaseConfig#config{
		mine = false,
		header_sync_jobs = 0,
		storage_modules = [
			%% The first MB of the weave.
			{?MiB, 0, {replica_2_9, MainRewardAddr}},
			%% The second 3 MB of the weave (skipping 1-2 MB).
			{3 * ?MiB, 1, {replica_2_9, MainRewardAddr}}
		]
	},
    ConfiguredRanges = ar_intervals:from_list([{?MiB, 0}, {6 * ?MiB, 3 * ?MiB}]),

	ar_test_node:join_on(#{ node => main, join_on => peer1, config => MainConfig }, true),
	ar_test_node:connect_to_peer(peer1),
	ar_test_node:wait_until_joined(main),

	LastConsensusWindowHeight = 4,
	lists:foreach(
		fun	(#block{ block_size = 0 }) ->
				ok;
			(B) ->
				BlockStart = block_start(B),
				case BlockStart >= ar_data_sync:get_disk_pool_threshold() of
					true ->
						ok;
					false ->
						BlockEnd = B#block.weave_size,
						BlockRange = ar_intervals:from_list([{BlockEnd, BlockStart}]),
						Intersection = ar_intervals:intersection(BlockRange, ConfiguredRanges),
						case B#block.height >= LastConsensusWindowHeight of
							true ->
								?debugFmt("Asserting data roots synced during consensus "
									"are stored, even outside the configured storage modules, "
									"height: ~B, configured ranges: ~0p, intersection: ~0p",
									[B#block.height, ConfiguredRanges, Intersection]),
								wait_for_data_roots(main, B);
							false ->
								case ar_intervals:is_empty(Intersection) of
									false ->
										?debugFmt("Asserting data roots synced for partitions "
											"we configured, range intersection: ~0p", [Intersection]),
										wait_for_data_roots(main, B);
									true ->
										?debugFmt("Asserting no data roots for partitions "
											"we did not configure, block range: ~0p", [BlockRange]),
										assert_no_data_roots(main, B)
								end
						end
				end
		end,
		Blocks
	).

test_data_roots_http_post() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(2_000_000_000_000_000), <<>>}]),
	start_peers_then_disconnect(peer1, main, B0),
	{B, _} = mine_block_with_fixed_data_tx(peer1, Wallet, 4096),
	%% Mine some empty blocks to push the data block out of the recent window.
	mine_empty_blocks_on_peer_after(peer1, B, 11),
	join_main_on_peer1(B#block.height + 11, false),
	ar_test_node:disconnect_from(peer1),
	assert_no_data_roots(main, B),
	{ok, Body} = get_data_roots(peer1, B),
	post_data_roots(main, B, Body),
	wait_for_data_roots(main, B),
	ok.

test_chunk_after_data_roots_http_post() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(2_000_000_000_000_000), <<>>}]),

	%% Main must not sync while peer1 builds the chain we will POST later.
	start_peers_then_disconnect(main, peer1, B0),

	%% Mine blocks with data on peer1. "Guaranteed" is a block with a fixed small data tx that
	%% is guaranteed to trigger a POST /data_roots in the test loop. The remaining random data
	%% provides some good additional coverage, but aren't guaranteed to always
	%% trigger a POST /data_roots.
	Guaranteed = mine_block_with_fixed_data_tx(peer1, Wallet, 4096),
	Random = lists:map(
		fun(_) ->
			TXData0 = generate_random_txs(Wallet),
			TXs = [TX || {TX, _} <- TXData0],
			B = ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 }, TXs),
			{B, filter_mined_tx_data(B, TXData0)}
		end,
		lists:seq(1, 4)
	),
	Data = [Guaranteed | Random],

	%% Mine some empty blocks to push the data blocks out of the recent window.
	{LastB, _} = lists:last(Data),
	mine_empty_blocks_on_peer_after(peer1, LastB, 11),

	join_main_on_peer1(LastB#block.height + 11, false),
	ar_test_node:disconnect_from(peer1),

	%% GET/POST /data_roots only apply below each peer's disk pool bound (see
	%% ar_data_roots:get_for_offset/1 and POST handler in ar_http_iface_middleware).
	Peer1PoolBound = ar_test_node:remote_call(peer1, ar_data_sync, get_disk_pool_threshold, []),
	MainPoolBound = ar_test_node:remote_call(main, ar_data_sync, get_disk_pool_threshold, []),

	%% Verify POST /data_roots and POST /chunk for each non-empty block in range on both peers.
	lists:foreach(
		fun({B, TXData}) ->
			BlockStart = block_start(B),
			case
				B#block.block_size > 0
				andalso BlockStart < Peer1PoolBound
				andalso BlockStart < MainPoolBound
			of
				true ->
					assert_no_data_roots(main, B),

					%% Fetch data roots from peer1 and POST to main.
					{ok, Body} = get_data_roots(peer1, B),
					post_data_roots(main, B, Body),

					%% For each transaction with data, POST its chunks to main and verify GET /chunk.
					lists:foreach(
						fun({TX, Chunks}) ->
							case TX#tx.data_size > 0 of
								true ->
									post_then_get_chunks(main, B, TX, Chunks);
								false ->
									ok
							end
						end,
						TXData
					);
				false ->
					ok
			end
		end,
		Data
	),
	ok.

test_chunk_after_data_roots_background_sync() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(2_000_000_000_000_000), <<>>}]),
	start_peers_then_disconnect(peer1, main, B0),
	{B, [{TX, Chunks}]} = mine_block_with_fixed_data_tx(peer1, Wallet, 4096),
	mine_empty_blocks_on_peer_after(peer1, B, 11),
	join_main_on_peer1(B#block.height + 11, true),
	true = B#block.block_size > 0,
	wait_until_data_roots_synced(main, B),
	ar_test_node:disconnect_from(peer1),
	post_then_get_chunks(main, B, TX, Chunks),
	ok.

test_chunk_in_unconfigured_partition_requires_manual_data_roots() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(2_000_000_000_000_000), <<>>}]),
	start_peers_then_disconnect(peer1, main, B0),
	BlocksData = [
		%% Use slightly different sizes so each block gets a distinct data root while still
		%% advancing the weave quickly into the uncovered partition.
		mine_block_with_fixed_data_tx(peer1, Wallet, 262140 + Tag)
		|| Tag <- lists:seq(1, 10)
	],
	{LastB, _} = lists:last(BlocksData),
	mine_empty_blocks_on_peer_after(peer1, LastB, 11),

	{ok, BaseConfig} = arweave_config:get_env(),
	MainConfig = BaseConfig#config{
		mine = false,
		sync_jobs = 0,
		header_sync_jobs = 0,
		enable_data_roots_syncing = true,
		storage_modules = [
			{?MiB, 0, unpacked},
			{3 * ?MiB, 1, unpacked}
		]
	},
	ar_test_node:join_on(#{ node => main, join_on => peer1, config => MainConfig }, true),
	ar_test_node:connect_to_peer(peer1),
	ar_test_node:wait_until_joined(main),
	ar_test_node:assert_wait_until_height(main, LastB#block.height + 11),

	ExpectedBackgroundSync = lists:takewhile(
		fun({B, _TXData}) -> block_start(B) < ?MiB end,
		BlocksData),
	%% Background sync scans by block start offset within configured module ranges.
	lists:foreach(
		fun({B, _TXData}) ->
			wait_for_data_roots(main, B)
		end,
		ExpectedBackgroundSync
	),
	%% Give the background sync pass time to complete before selecting a block that still
	%% remains outside the synced coverage.
	timer:sleep(5_000),
	%% The configured modules cover [0, 1 MiB) and [3 MiB, 6 MiB), so blocks fully inside
	%% [1 MiB, 3 MiB] are the ones this test expects to remain unsynced.
	GapCandidates = [
		BlockData
		|| {B, _} = BlockData <- BlocksData,
			block_start(B) >= ?MiB,
			B#block.weave_size =< 3 * ?MiB
	],
	%% Pick the first block in the uncovered range that still has no local data-roots entry.
	%% This keeps the test aligned with the actual background-sync coverage instead of relying
	%% on a fixed block offset that may shift as the mined block sizes change.
	{TargetB, [{TargetTX, TargetChunks}]} =
		case lists:dropwhile(
			fun({B, _}) ->
				get_data_roots(main, B) =/= not_found
			end,
			GapCandidates
		) of
			[Target | _] ->
				Target;
			[] ->
				?assert(false, lists:flatten(io_lib:format(
					"Expected at least one unsynced block in the uncovered gap; candidates: ~p",
					[[{block_start(B), B#block.weave_size} || {B, _} <- GapCandidates]]
				)))
		end,
	TargetStart = block_start(TargetB),
	?assert(TargetStart >= ?MiB),
	?assert(TargetB#block.weave_size =< 3 * ?MiB),
	assert_no_data_roots(main, TargetB),

	ar_test_node:disconnect_from(peer1),
	[{_AbsEnd, Proof}] = ar_test_data_sync:build_proofs(TargetB, TargetTX, TargetChunks),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"data_root_not_found\"}">>, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
	),

	{ok, Body} = get_data_roots(peer1, TargetB),
	post_data_roots(main, TargetB, Body),
	wait_for_data_roots(main, TargetB),
	?assertMatch(
		{ok, {{<<"303">>, _}, _, <<>>, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
	),
	ok.

test_chunk_skipped_with_duplicate_data_root() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(2_000_000_000_000_000), <<>>}]),
	start_peers_then_disconnect(peer1, main, B0),
	%% Mine two consecutive blocks on peer1 with the SAME data root (identical chunk data).
	{B1, [{TX1, Chunks1}]} = mine_block_with_fixed_data_tx(peer1, Wallet, 4096),
	{B2, [{TX2, Chunks2}]} = mine_block_with_fixed_data_tx(peer1, Wallet, 4096),
	?assertEqual(TX1#tx.data_root, TX2#tx.data_root),
	%% Mine empty blocks to push the data blocks out of the recent window.
	mine_empty_blocks_on_peer_after(peer1, B2, 11),
	%% Join main with background data_roots syncing turned off.
	%% We manually sync only B2's data roots (simulating incomplete background sync where
	%% B2 was processed but B1 was not).
	join_main_on_peer1(B2#block.height + 11, false),
	%% Pre-fetch B1's data roots while still connected to peer1. We'll post them to main
	%% immediately after posting B1's chunk to beat the disk pool scan.
	{ok, Body1} = get_data_roots(peer1, B1),
	{ok, Body2} = get_data_roots(peer1, B2),
	post_data_roots(main, B2, Body2),
	wait_for_data_roots(main, B2),
	ar_test_node:disconnect_from(peer1),
	[{AbsEnd2, Proof2}] = ar_test_data_sync:build_proofs(B2, TX2, Chunks2),
	post_chunk(main, Proof2),
	wait_for_sync_record_update(main, AbsEnd2),
	%% POST TX1's chunk. Since we've only synced one copy of the data_roots, and we've already
	%% postd one chunk matching that data_root (TX2's chunk), we should get a 200 when
	%% posting Chunk1 - *but* we will not see Chunk1 persisted. This is not ideal but it is
	%% by design. 
	%% For more context, and to track the state of any improvements to the process, see:
	%% https://github.com/ArweaveTeam/arweave-dev/issues/1112
	[{AbsEnd1, Proof1}] = ar_test_data_sync:build_proofs(B1, TX1, Chunks1),
	post_chunk(main, Proof1),
	%% Allow time for the chunk to be persisted (it shouldn't be)
	timer:sleep(10_000),
	?assertEqual(not_found, get_chunk(main, AbsEnd1)),
	%% Now we post B1's data roots, which should allow Chunk1 to be persisted
	post_data_roots(main, B1, Body1),
	wait_for_data_roots(main, B1),
	%% Re-POST the chunk — now B1's index entry exists so the chunk can be promoted.
	post_chunk(main, Proof1),
	ok = wait_for_chunk_to_persist(main, AbsEnd1).

test_chunk_skipped_with_depth_exhaustion() ->
	MaxDuplicateDataRoots = 3,
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(2_000_000_000_000_000), <<>>}]),
	start_peers_then_disconnect(peer1, main, B0),
	%% Mine one more block than the configured duplicate-depth limit. This ensures the oldest
	%% chunk falls outside the configured duplicate data-root window.
	Blocks = lists:map(
		fun(_) -> mine_block_with_fixed_data_tx(peer1, Wallet, 4096) end,
		lists:seq(1, MaxDuplicateDataRoots + 1)
	),
	{LastB, _} = lists:last(Blocks),
	%% Mine enough empty blocks to push the chunks out of the disk pool. The later duplicates
	%% should still be persistable, but the oldest should now be outside the configured window.
	mine_empty_blocks_on_peer_after(peer1, LastB, 10),
	join_main_on_peer1(LastB#block.height + 10, false, MaxDuplicateDataRoots),
	ar_test_node:disconnect_from(peer1),
	[{B1, [{TX1, Chunks1}]} | HigherBlocks] = Blocks,
	lists:foreach(
		fun({B, _}) ->
			{ok, Body} = get_data_roots(peer1, B),
			post_data_roots(main, B, Body),
			wait_for_data_roots(main, B)
		end,
		HigherBlocks
	),
	%% POST chunks for the later duplicate blocks and wait for promotion. The oldest block B1
	%% is outside the configured duplicate-offset window, so it should not become queryable via
	%% duplicate fanout.
	HigherAbsEnds = lists:map(
		fun({B, [{TX, Chunks}]}) ->
			[{AbsEnd, Proof}] = ar_test_data_sync:build_proofs(B, TX, Chunks),
			post_chunk(main, Proof),
			AbsEnd
		end,
		HigherBlocks
	),
	lists:foreach(
		fun(AbsEnd) ->
			wait_for_chunk_to_persist(main, AbsEnd)
		end,
		HigherAbsEnds
	),
	timer:sleep(10_000),
	%% POST chunk for block 1 (lowest offset). chunk_offsets_synced/5 only checks the configured
	%% number of synced duplicate offsets, so B1 is treated as already synced and skipped.
	[{AbsEnd1, Proof1}] = ar_test_data_sync:build_proofs(B1, TX1, Chunks1),
	post_chunk(main, Proof1),
	timer:sleep(10_000),
	?assertEqual(not_found, get_chunk(main, AbsEnd1)),
	{ok, Body1} = get_data_roots(peer1, B1),
	%% This test confirms that doign a POST /data_roots before posting a chunk is not a reliable
	%% workaround to force the chunk to be persisted. We will either need to change the
	%% duplicate data_roots logic, or implement a new endpoint. 
	%% For more context, and to track the state of any improvements to the process, see:
	%% https://github.com/ArweaveTeam/arweave-dev/issues/1112
	post_data_roots(main, B1, Body1),
	wait_for_data_roots(main, B1),
	post_chunk(main, Proof1),
	timer:sleep(10_000),
	?assertEqual(not_found, get_chunk(main, AbsEnd1)),
	% ok = wait_for_chunk_to_persist(main, AbsEnd1),
	ok.

%% Start PeerA and PeerB from the same genesis, wait until both joined, then have PeerA
%% disconnect from PeerB so they stop syncing while tests extend the chain on one side.
start_peers_then_disconnect(PeerA, PeerB, B0) ->
	ar_test_node:start_peer(PeerA, B0),
	ar_test_node:start_peer(PeerB, B0),
	ar_test_node:wait_until_joined(PeerA),
	ar_test_node:wait_until_joined(PeerB),
	ar_test_node:remote_call(PeerA, ar_test_node, disconnect_from, [PeerB]),
	ok.

%% Mine Count empty blocks on Peer immediately after Block (height advances from Block#block.height).
mine_empty_blocks_on_peer_after(Peer, Block, Count) ->
	lists:foldl(
		fun(_, Height) ->
			ar_test_node:mine(Peer),
			ar_test_node:assert_wait_until_height(Peer, Height + 1),
			Height + 1
		end,
		Block#block.height,
		lists:seq(1, Count)
	).

%% Rejoin main onto peer1 with mine disabled. When EnableBackgroundSync is true, enable the
%% normal background sync setup; when false, disable both header syncing and background
%% data_roots syncing. Does not disconnect — caller may call
%% ar_test_node:remote_call(main, ar_test_node, disconnect_from, [peer1]) when needed.
join_main_on_peer1(ExpectedHeight, EnableBackgroundSync) ->
	join_main_on_peer1(ExpectedHeight, EnableBackgroundSync, undefined).

join_main_on_peer1(ExpectedHeight, EnableBackgroundSync, MaxDuplicateDataRoots) ->
	{ok, BaseConfig} = arweave_config:get_env(),
	Config = BaseConfig#config{
		mine = false,
		sync_jobs = 0,
		header_sync_jobs =
			case EnableBackgroundSync of
				true -> 2;
				false -> 0
			end,
		enable_data_roots_syncing = EnableBackgroundSync
	},
	MainConfig = case MaxDuplicateDataRoots of
		undefined ->
			Config;
		Value ->
			Config#config{ max_duplicate_data_roots = Value }
	end,
	ar_test_node:join_on(#{ node => main, join_on => peer1, config => MainConfig }, true),
	ar_test_node:connect_to_peer(peer1),
	ar_test_node:wait_until_joined(main),
	ar_test_node:assert_wait_until_height(main, ExpectedHeight),
	ok.

wait_until_data_roots_synced(Peer, B) ->
	Start = block_start(B),
	End = B#block.weave_size,
	true = ar_util:do_until(
		fun() ->
			ar_test_node:remote_call(Peer, ar_data_roots, are_synced,
				[Start, End, B#block.tx_root, ?DEFAULT_MODULE])
		end,
		500,
		120_000),
	ok.

%% Mine one block on peer1 with a single fixed-size v2 data tx. TXData matches
%% generate_random_txs/1 entries.
mine_block_with_fixed_data_tx(Peer, Wallet, Size) ->
	Chunks = [<< 0:(Size * 8) >>],
	{DataRoot, _DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	{TX, _} = ar_test_data_sync:tx(
		#{ wallet => Wallet,
			split_type => {fixed_data, DataRoot, Chunks},
			format => v2,
			reward => ?AR(10_000_000_000),
			tx_anchor_peer => Peer,
			get_fee_peer => Peer }),
	B = ar_test_node:post_and_mine(#{ miner => Peer, await_on => Peer }, [TX]),
	{B, [{TX, Chunks}]}.

generate_random_txs(Wallet) ->
	Coin = rand:uniform(12),
	case Coin of
		Val when Val =< 3 ->
			%% Add three data txs with different data roots.
			[ar_test_data_sync:tx(
				#{ wallet => Wallet,
					split_type => original_split,
					format => v2,
					reward => ?AR(10_000_000_000),
					tx_anchor_peer => peer1,
					get_fee_peer => peer1 }),
				ar_test_data_sync:tx(
					#{ wallet => Wallet,
						split_type => original_split,
						format => v2,
						reward => ?AR(10_000_000_000),
						tx_anchor_peer => peer1,
						get_fee_peer => peer1 }),
				ar_test_data_sync:tx(
					#{ wallet => Wallet,
						split_type => original_split,
						format => v2,
						reward => ?AR(10_000_000_000),
						tx_anchor_peer => peer1,
						get_fee_peer => peer1 })
					| generate_random_txs(Wallet)];
		Val when Val =< 6 ->
			%% A bit smaller than 256 KiB to provoke padding.
			Chunks = [<< 0:(262140 * 8) >>],
			{DataRoot, _DataTree} = ar_merkle:generate_tree(
				ar_tx:sized_chunks_to_sized_chunk_ids(
					ar_tx:chunks_to_size_tagged_chunks(Chunks)
				)
			),
			%% Two transactions with the same data root.
			[ar_test_data_sync:tx(
				#{ wallet => Wallet,
					split_type => {fixed_data, DataRoot, Chunks},
					format => v2,
					reward => ?AR(10_000_000_000),
					tx_anchor_peer => peer1,
					get_fee_peer => peer1 }),
			ar_test_data_sync:tx(
				#{ wallet => Wallet,
					split_type => {fixed_data, DataRoot, Chunks},
					format => v2,
					reward => ?AR(10_000_000_000),
					tx_anchor_peer => peer1,
					get_fee_peer => peer1 }) | generate_random_txs(Wallet)];
		Val when Val =< 9 ->
			%% Add empty tx and two transactions with different data roots.
			[ar_test_data_sync:tx(
				#{ wallet => Wallet,
					split_type => {fixed_data, <<>>, []},
					format => v2,
					reward => ?AR(10_000_000_000),
					tx_anchor_peer => peer1,
					get_fee_peer => peer1 }),
				ar_test_data_sync:tx(
				#{ wallet => Wallet,
					split_type => original_split,
					format => v2,
					reward => ?AR(10_000_000_000),
					tx_anchor_peer => peer1,
					get_fee_peer => peer1 }),
				ar_test_data_sync:tx(
					#{ wallet => Wallet,
						split_type => original_split,
						format => v2,
						reward => ?AR(10_000_000_000),
						tx_anchor_peer => peer1,
						get_fee_peer => peer1 })
			| generate_random_txs(Wallet)];
		_ ->
			[]
	end.

block_start(B) ->
	B#block.weave_size - B#block.block_size.

filter_mined_tx_data(B, TXData) ->
	MinedTXIDs = sets:from_list(B#block.txs),
	[{TX, Chunks} || {TX, Chunks} <- TXData, sets:is_element(TX#tx.id, MinedTXIDs)].

data_roots_path(BlockStart) ->
	"/data_roots/" ++ integer_to_list(BlockStart).

%% GET /data_roots for B on Peer. Returns {ok, Body} | not_found.
get_data_roots(Peer, B) ->
	Start = block_start(B),
	case ar_http:req(#{
		method => get,
		peer => ar_test_node:peer_ip(Peer),
		path => data_roots_path(Start)
	}) of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			{ok, Body};
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			not_found;
		Other ->
			?assert(false, lists:flatten(io_lib:format(
				"GET /data_roots/~B: unexpected reply ~p", [Start, Other])))
	end.

%% POST /data_roots for B to Peer. Asserts 200.
post_data_roots(Peer, B, Body) ->
	Start = block_start(B),
	case ar_http:req(#{
		method => post,
		peer => ar_test_node:peer_ip(Peer),
		path => data_roots_path(Start),
		body => Body
	}) of
		{ok, {{<<"200">>, _}, _, <<>>, _, _}} ->
			ok;
		Other ->
			?assert(false, lists:flatten(io_lib:format(
				"POST /data_roots/~B: expected 200, got ~p", [Start, Other])))
	end.

%% POST /chunk with a proof map. Asserts 200.
post_chunk(Node, Proof) ->
	case ar_test_node:post_chunk(Node, ar_serialize:jsonify(Proof)) of
		{ok, {{<<"200">>, _}, _, <<>>, _, _}} ->
			ok;
		Other ->
			?assert(false, lists:flatten(io_lib:format(
				"POST /chunk: expected 200, got ~p", [Other])))
	end.

%% Poll until AbsoluteEndOffset appears in the sync record for the given node.
wait_for_sync_record_update(Node, AbsoluteEndOffset) ->
	true = ar_util:do_until(
		fun() ->
			case ar_test_node:remote_call(Node, ar_sync_record, is_recorded,
					[AbsoluteEndOffset, ar_data_sync]) of
				{{true, _}, _} -> true;
				_ -> false
			end
		end,
		500,
		60_000).

%% GET /chunk at the given offset with any packing. Returns ok | not_found.
get_chunk(Node, GlobalEndOffset) ->
	case ar_test_node:get_chunk(Node, GlobalEndOffset, any) of
		{ok, {{<<"200">>, _}, _, _, _, _}} ->
			ok;
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			not_found;
		Other ->
			?assert(false, lists:flatten(io_lib:format(
				"GET /chunk/~B: unexpected reply ~p", [GlobalEndOffset, Other])))
	end.

%% POST /chunk for each proof from get_records_with_proofs/3, then poll GET /chunk until
%% queryable (disk pool → sync_record promotion is asynchronous).
post_then_get_chunks(Node, B, TX, Chunks) ->
	Records = ar_test_data_sync:get_records_with_proofs(B, TX, Chunks),
	lists:foreach(
		fun({_, _, _, {GlobalChunkEndOffset, Proof}}) ->
			post_chunk(Node, Proof),
			ok = wait_for_chunk_to_persist(Node, GlobalChunkEndOffset)
		end,
		Records
	).

%% Poll GET /chunk until HTTP 200 (disk-pool → sync_record promotion is asynchronous).
wait_for_chunk_to_persist(Node, GlobalEndOffset) ->
	wait_for_chunk_to_persist(Node, GlobalEndOffset, 15_000).

wait_for_chunk_to_persist(Node, GlobalEndOffset, TimeoutMs) ->
	case ar_util:do_until(
		fun() ->
			case get_chunk(Node, GlobalEndOffset) of
				ok -> true;
				not_found -> false
			end
		end,
		100,
		TimeoutMs
	) of
		true ->
			ok;
		{error, timeout} ->
			?assert(false, 
			lists:flatten(io_lib:format("Timeout waiting for chunk to persist: ~p",
				[GlobalEndOffset])))
	end.

wait_for_data_roots(Peer, B) ->
	Start = block_start(B),
	End = B#block.weave_size,
	Height = B#block.height,
	true = ar_util:do_until(
		fun() ->
			case get_data_roots(Peer, B) of
				{ok, Body} ->
					case ar_serialize:binary_to_data_roots(Body) of
						{ok, {_TXRoot, BlockSize, _Entries}}
								when Start + BlockSize == End ->
							true;
						{ok, {_TXRoot, BlockSize2, _Entries}} ->
							?debugFmt("Unexpected block size: ~B, expected: ~B, height: ~B",
								[BlockSize2, End - Start, Height]),
							?assert(false);
						{error, Error} ->
							?debugFmt("Unexpected error: ~p, height: ~B", [Error, Height]),
							?assert(false)
					end;
				not_found ->
					false
			end
		end,
		200,
		120_000
	),
	ok.

assert_no_data_roots(Peer, B) ->
	case get_data_roots(Peer, B) of
		not_found ->
			ok;
		{ok, Body} ->
			?debugFmt("Expected 404 but got body: ~p ~p",
				[Body, ar_serialize:binary_to_data_roots(Body)]),
			?assert(false)
	end.
