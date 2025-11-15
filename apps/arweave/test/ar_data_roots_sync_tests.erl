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
	DataTXs = [element(1, ar_test_data_sync:tx(
				#{ wallet => Wallet,
					split_type => original_split,
					format => v2,
					reward => ?AR(1),
					tx_anchor_peer => peer1,
					get_fee_peer => peer1 }))
				|| _ <- lists:seq(1, 3)],
	EmptyTXs = [element(1, ar_test_data_sync:tx(
				#{ wallet => Wallet,
					split_type => {fixed_data, <<>>, []},
					format => v2,
					reward => ?AR(1),
					tx_anchor_peer => peer1,
					get_fee_peer => peer1 })) || _ <- lists:seq(1, 3)],
	B1 = ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 },
			[lists:nth(1, DataTXs)]),
	%% Empty transactions should be ignored.
	B2 = ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 },
			[lists:nth(1, EmptyTXs), lists:nth(2, DataTXs), lists:nth(2, EmptyTXs)]),
	B3 = ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 },
			[lists:nth(3, EmptyTXs), lists:nth(3, DataTXs)]),
	%% The node fetches this many latest blocks after joining the network.
	%% We want all our data blocks be older so that the node has to use
	%% the data root syncing mechanism to fetch data roots (we explicitly
	%% assert the unexpected data roots are not synced further down here).
	?assertEqual(10, 2 * ar_block:get_max_tx_anchor_depth()),
	Blocks = [B1, B2, B3
		| lists:map(
			fun(_) ->
				TXs = generate_random_txs(Wallet),
				ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 }, TXs)
			end,
			lists:seq(1, 11)
		)],

	%% Now start main (node A) with header syncing disabled and storage modules covering PART of the range.
	{ok, BaseConfig} = arweave_config:get_env(),
	MainRewardAddr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	%% Cover only the first partition and half of the second one to ensure partial coverage.
	MB = 1024 * 1024,
	MainConfig = BaseConfig#config{
		mine = false,
		header_sync_jobs = 0,
		storage_modules = [
			%% The first MB of the weave.
			{MB, 0, {replica_2_9, MainRewardAddr}},
			%% The second 3 MB of the weave (skipping 1-2 MB).
			{3 * MB, 1, {replica_2_9, MainRewardAddr}}
		]
	},
    ConfiguredRanges = ar_intervals:from_list([{MB, 0}, {6 * MB, 3 * MB}]),

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
				BlockEnd = B#block.weave_size,
				BlockRange = ar_intervals:from_list([{BlockEnd, BlockStart}]),
				Intersection = ar_intervals:intersection(BlockRange, ConfiguredRanges),
				case Height >= LastConsensusWindowHeight of
					true ->
						?debugFmt("Asserting data roots synced during consensus "
							"are stored, even outside the configured storage modules, "
							"height: ~B, configured ranges: ~p, intersection: ~p",
							[Height, ConfiguredRanges, Intersection]),
						wait_until_data_roots_range(BlockStart, BlockEnd, Height);
					false ->
						case ar_intervals:is_empty(Intersection) of
							false ->
								?debugFmt("Asserting data roots synced for partitions "
									"we configured, range intersection: ~p", [Intersection]),
								wait_until_data_roots_range(BlockStart, BlockEnd, Height);
							true ->
								?debugFmt("Asserting no data roots for partitions "
									"we did not configure, block range: ~p", [BlockRange]),
								assert_no_data_roots(BlockStart)
						end
				end
		end,
		Blocks
	).

generate_random_txs(Wallet) ->
	Coin = random:uniform(12),
	case Coin of
		Val when Val =< 3 ->
			%% Add data tx.
			[element(1, ar_test_data_sync:tx(
				#{ wallet => Wallet,
					split_type => original_split,
					format => v2,
					reward => ?AR(10_000_000_000),
					tx_anchor_peer => peer1,
					get_fee_peer => peer1 })) | generate_random_txs(Wallet)];
		Val when Val =< 6 ->
			%% A bit smaller than 256 KiB to provoke padding.
			Chunks = [<< 0:(262140 * 8) >>],
			{DataRoot, _DataTree} = ar_merkle:generate_tree(
				ar_tx:sized_chunks_to_sized_chunk_ids(
					ar_tx:chunks_to_size_tagged_chunks(Chunks)
				)
			),
			%% Insert two transactions with the same data root.
			[element(1, ar_test_data_sync:tx(
				#{ wallet => Wallet,
					split_type => {fixed_data, DataRoot, Chunks},
					format => v2,
					reward => ?AR(10_000_000_000),
					tx_anchor_peer => peer1,
					get_fee_peer => peer1 })),
			element(1, ar_test_data_sync:tx(
				#{ wallet => Wallet,
					split_type => {fixed_data, DataRoot, Chunks},
					format => v2,
					reward => ?AR(10_000_000_000),
					tx_anchor_peer => peer1,
					get_fee_peer => peer1 })) | generate_random_txs(Wallet)];
		Val when Val =< 9 ->
			%% Add empty tx.
			[element(1, ar_test_data_sync:tx(
				#{ wallet => Wallet,
					split_type => {fixed_data, <<>>, []},
					format => v2,
					reward => ?AR(10_000_000_000),
					tx_anchor_peer => peer1,
					get_fee_peer => peer1 })) | generate_random_txs(Wallet)];
		_ ->
			[]
	end.

wait_until_data_roots_range(BlockStart, BlockEnd, Height) ->
	true = ar_util:do_until(
		fun() ->
			Peer = ar_test_node:peer_ip(main),
			Path = "/data_roots/" ++ integer_to_list(BlockStart),
			case ar_http:req(#{ method => get, peer => Peer, path => Path }) of
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

assert_no_data_roots(BlockStart) ->
	Peer = ar_test_node:peer_ip(main),
	Path = "/data_roots/" ++ integer_to_list(BlockStart),
	case ar_http:req(#{ method => get, peer => Peer, path => Path }) of
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
            ok;
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			?debugFmt("Body: ~p ~p", [Body, ar_serialize:binary_to_data_roots(Body)]),
            ?assert(false, "Unexpected get data roots reply, expected 404");
		Reply ->
            ?debugFmt("Unexpected reply: ~p (expected 404)", [Reply]),
			?assert(false)
	end.