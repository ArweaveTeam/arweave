-module(ar_data_roots_sync_tests).

-include("ar.hrl").
-include("ar_data_sync.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include_lib("eunit/include/eunit.hrl").

%% Test syncs missing data roots from a peer (NOT through header syncing)".
data_roots_syncs_from_peer_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_block, get_consensus_window_size, fun() -> 5 end},
			{ar_block, get_max_tx_anchor_depth, fun() -> 5 end},
			{ar_storage_module, get_overlap, fun(_Packing) -> 0 end}],
		fun test_data_roots_syncs_from_peer/0).

post_data_roots_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_block, get_consensus_window_size, fun() -> 5 end},
			{ar_block, get_max_tx_anchor_depth, fun() -> 5 end},
			{ar_storage_module, get_overlap, fun(_Packing) -> 0 end}],
		fun test_post_data_roots/0).

post_data_roots_store_sync_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_block, get_consensus_window_size, fun() -> 5 end},
			{ar_block, get_max_tx_anchor_depth, fun() -> 5 end},
			{ar_storage_module, get_overlap, fun(_Packing) -> 0 end}],
		fun test_post_data_roots_store_sync/0).

test_data_roots_syncs_from_peer() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(2_000_000_000_000_000), <<>>}]),

	%% Start peer1 (node B) that will mine blocks with data.
	ar_test_node:start_peer(peer1, B0),
	%% Start main from the same block.
	ar_test_node:start_peer(main, B0),
	%% Ensure peer1 has fully joined before submitting transactions.
	ar_test_node:wait_until_joined(peer1),
	ar_test_node:wait_until_joined(main),
	%% Disconnect the peers to create a block gap on main.
	ar_test_node:disconnect_from(peer1),

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
				Height = B#block.height,
				BlockStart = B#block.weave_size - B#block.block_size,
				case BlockStart >= ar_data_sync:get_disk_pool_threshold() of
					true ->
						ok;
					false ->
						BlockEnd = B#block.weave_size,
						BlockRange = ar_intervals:from_list([{BlockEnd, BlockStart}]),
						Intersection = ar_intervals:intersection(BlockRange, ConfiguredRanges),
						case Height >= LastConsensusWindowHeight of
							true ->
								?debugFmt("Asserting data roots synced during consensus "
									"are stored, even outside the configured storage modules, "
									"height: ~B, configured ranges: ~p, intersection: ~p",
									[Height, ConfiguredRanges, Intersection]),
								wait_until_data_roots_range(main, BlockStart, BlockEnd, Height);
							false ->
								case ar_intervals:is_empty(Intersection) of
									false ->
										?debugFmt("Asserting data roots synced for partitions "
											"we configured, range intersection: ~p", [Intersection]),
										wait_until_data_roots_range(main, BlockStart, BlockEnd, Height);
									true ->
										?debugFmt("Asserting no data roots for partitions "
											"we did not configure, block range: ~p", [BlockRange]),
										assert_no_data_roots(main, BlockStart)
								end
						end
				end
		end,
		Blocks
	).

test_post_data_roots() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(2_000_000_000_000_000), <<>>}]),

	ar_test_node:start_peer(peer1, B0),
	ar_test_node:start_peer(main, B0),
	ar_test_node:wait_until_joined(peer1),
	ar_test_node:wait_until_joined(main),
	%% Disconnect main from peer1 so that main doesn't sync data roots, chunks, or blocks
	%% while we're setting peer1 up.
	ar_test_node:disconnect_from(peer1),

	%% Mine blocks with data on peer1. "Guaranteed" is a block with a fixed small data tx that
	%% is guaranteed to trigger a POST /data_roots in the test loop. The remaining random data
	%% provides some good additional coverage, but aren't guaranteed to always
	%% trigger a POST /data_roots.
	Guaranteed = mine_peer_block_with_small_fixed_data_tx(peer1, Wallet),
	Random = lists:map(
		fun(_) ->
			TXData = generate_random_txs(Wallet),
			TXs = [TX || {TX, _} <- TXData],
			B = ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 }, TXs),
			{B, TXData}
		end,
		lists:seq(1, 4)
	),
	Data = [Guaranteed | Random],

	%% Mine some empty blocks to push the data blocks out of the recent window.
	{LastB, _} = lists:last(Data),
	lists:foldl(
		fun(_, Height) ->
			ar_test_node:mine(peer1),
			ar_test_node:assert_wait_until_height(peer1, Height + 1),
			Height + 1
		end,
		LastB#block.height,
		lists:seq(1, 11)
	),

	join_main_on_peer1_without_header_sync(LastB#block.height + 11),

	%% GET/POST /data_roots only apply below each peer's disk pool bound (see
	%% ar_data_sync:get_data_roots_for_offset/1 and POST handler in ar_http_iface_middleware).
	Peer1PoolBound = ar_test_node:remote_call(peer1, ar_data_sync, get_disk_pool_threshold, []),
	MainPoolBound = ar_test_node:remote_call(main, ar_data_sync, get_disk_pool_threshold, []),

	%% Verify POST /data_roots and POST /chunk for each non-empty block in range on both peers.
	lists:foreach(
		fun({B, TXData}) ->
			BlockStart = B#block.weave_size - B#block.block_size,
			case
				B#block.block_size > 0
				andalso BlockStart < Peer1PoolBound
				andalso BlockStart < MainPoolBound
			of
				true ->
					assert_no_data_roots(main, BlockStart),

					%% Fetch data roots from peer1 and POST to main.
					Peer1 = ar_test_node:peer_ip(peer1),
					Main = ar_test_node:peer_ip(main),
					Path = "/data_roots/" ++ integer_to_list(BlockStart),
					{ok, {{<<"200">>, _}, _, Body, _, _}} = ar_http:req(#{ method => get, peer => Peer1, path => Path }),
					{ok, {{<<"200">>, _}, _, <<>>, _, _}} = ar_http:req(#{ method => post, peer => Main, path => Path, body => Body }),

					%% For each transaction with data, POST its chunks to main.
					lists:foreach(
						fun({TX, Chunks}) ->
							case TX#tx.data_size > 0 of
								true ->
									Proofs = ar_test_data_sync:get_records_with_proofs(B, TX, Chunks),
									lists:foreach(
										fun({_, _, _, {_, Proof}}) ->
											{ok, {{<<"200">>, _}, _, <<>>, _, _}} = ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
										end,
										Proofs
									);
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

test_post_data_roots_store_sync() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(2_000_000_000_000_000), <<>>}]),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:start_peer(main, B0),
	ar_test_node:wait_until_joined(peer1),
	ar_test_node:wait_until_joined(main),
	ar_test_node:disconnect_from(peer1),
	{B, _} = mine_peer_block_with_small_fixed_data_tx(peer1, Wallet),
	%% Mine some empty blocks to push the data block out of the recent window.
	lists:foldl(
		fun(_, Height) ->
			ar_test_node:mine(peer1),
			ar_test_node:assert_wait_until_height(peer1, Height + 1),
			Height + 1
		end,
		B#block.height,
		lists:seq(1, 11)
	),
	join_main_on_peer1_without_header_sync(B#block.height + 11),
	BlockStart = B#block.weave_size - B#block.block_size,
	BlockEnd = B#block.weave_size,
	true = B#block.block_size > 0,
	Peer1PoolBound = ar_test_node:remote_call(peer1, ar_data_sync, get_disk_pool_threshold, []),
	MainPoolBound = ar_test_node:remote_call(main, ar_data_sync, get_disk_pool_threshold, []),
	true = BlockStart < Peer1PoolBound andalso BlockStart < MainPoolBound,
	assert_no_data_roots(main, BlockStart),
	Peer1 = ar_test_node:peer_ip(peer1),
	Main = ar_test_node:peer_ip(main),
	Path = "/data_roots/" ++ integer_to_list(BlockStart),
	{ok, {{<<"200">>, _}, _, Body, _, _}} = ar_http:req(#{ method => get, peer => Peer1, path => Path }),
	{ok, {{<<"200">>, _}, _, <<>>, _, _}} = ar_http:req(#{ method => post, peer => Main, path => Path, body => Body }),
	wait_until_data_roots_range(main, BlockStart, BlockEnd, B#block.height),
	ok.

%% Join main to peer1 with header syncing disabled (mine = false, header_sync_jobs = 0).
%% Main advances to ExpectedHeight on the chain without syncing tx headers, isolating
%% data_roots behaviour for the assertions that follow.
join_main_on_peer1_without_header_sync(ExpectedHeight) ->
	{ok, BaseConfig} = arweave_config:get_env(),
	MainConfig = BaseConfig#config{ mine = false, header_sync_jobs = 0 },
	ar_test_node:join_on(#{ node => main, join_on => peer1, config => MainConfig }, true),
	ar_test_node:connect_to_peer(peer1),
	ar_test_node:wait_until_joined(main),
	ar_test_node:assert_wait_until_height(main, ExpectedHeight),
	ar_test_node:disconnect_from(peer1),
	ok.

%% Mine one block on peer1 with a single small v2 data tx. TXData matches generate_random_txs/1 entries.
mine_peer_block_with_small_fixed_data_tx(Peer, Wallet) ->
	Chunks = [<< 0:(4096 * 8) >>],
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

wait_until_data_roots_range(Peer, BlockStart, BlockEnd, Height) ->
	true = ar_util:do_until(
		fun() ->
			PeerIP = ar_test_node:peer_ip(Peer),
			Path = "/data_roots/" ++ integer_to_list(BlockStart),
			case ar_http:req(#{ method => get, peer => PeerIP, path => Path }) of
				{ok, {{<<"200">>, _}, _, Body, _, _}} ->
					case ar_serialize:binary_to_data_roots(Body) of
						{ok, {_TXRoot, BlockSize, _Entries}} when BlockStart + BlockSize == BlockEnd ->
                            true;
						{ok, {_TXRoot, BlockSize2, _Entries}} ->
                            ?debugFmt("Received get data roots reply "
								"with unexpected block size: ~B, expected: ~B, height: ~B",
								[BlockSize2, BlockEnd - BlockStart, Height]),
							?assert(false);
						{error, Error} ->
							?debugFmt("Unexpected error: ~p, height: ~B", [Error, Height]),
							?assert(false)
					end;
				{ok, {{<<"404">>, _}, _, _, _, _}} ->
                    false;
				Reply ->
					?debugFmt("Unexpected reply: ~p (expected 200 or 404)", [Reply]),
					?assert(false)
			end
		end,
		200,
		120_000
	),
	ok.

assert_no_data_roots(Peer, BlockStart) ->
	PeerIP = ar_test_node:peer_ip(Peer),
	Path = "/data_roots/" ++ integer_to_list(BlockStart),
	case ar_http:req(#{ method => get, peer => PeerIP, path => Path }) of
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
            ok;
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			?debugFmt("Body: ~p ~p", [Body, ar_serialize:binary_to_data_roots(Body)]),
            ?assert(false, "Unexpected get data roots reply, expected 404");
		Reply ->
            ?debugFmt("Unexpected reply: ~p (expected 404)", [Reply]),
			?assert(false)
	end.
