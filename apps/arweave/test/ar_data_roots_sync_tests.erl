-module(ar_data_roots_sync_tests).

-include("ar.hrl").
-include("ar_config.hrl").
-include("ar_data_sync.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Test syncs missing data roots from a peer (NOT through header syncing)".
data_roots_syncs_from_peer_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_storage_module, get_overlap, fun(_Packing) -> 0 end}],
		fun test_data_roots_syncs_from_peer/0).

test_data_roots_syncs_from_peer() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(200000), <<>>}]),

	%% Start peer1 (node B) that will mine blocks with data.
	ar_test_node:start_peer(peer1, B0),
	%% Start main from the same block.
	ar_test_node:start_peer(main, B0),
	%% Ensure peer1 has fully joined before submitting transactions.
	ar_test_node:wait_until_joined(peer1),
	ar_test_node:wait_until_joined(main),
	%% Disconnect the peers to create a block gap on main.
	ar_test_node:disconnect_from(peer1),

	%% Mine ~5 blocks with transactions with data on peer1 BEFORE main joins.
	TXsAndBlocks = lists:map(
		fun(_I) ->
			TX = element(1, ar_test_data_sync:tx(
				#{ wallet => Wallet,
					split_type => original_split,
					format => v2,
					reward => ?AR(1),
					tx_anchor_peer => peer1,
					get_fee_peer => peer1 })),
			B = ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 }, [TX]),
			{TX, B}
		end,
		lists:seq(1, 3)
	),
	%% The node fetches this many latest blocks after joining the network.
	%% We want all our data blocks be older so that the node has to use
	%% the data root syncing mechanism to fetch data roots (we explicitly
	%% assert the unexpected data roots are not synced further down here).
	?assertEqual(10, 2 * ?MAX_TX_ANCHOR_DEPTH),
	[ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 }, [])
		|| _ <- lists:seq(1, 10)],

	%% Now start main (node A) with header syncing disabled and storage modules covering PART of the range.
	{ok, BaseConfig} = application:get_env(arweave, config),
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

	%% For each block, assert that only blocks intersecting configured ranges
    %% get data roots synced.
	lists:foreach(
		fun({_TX, B}) ->
			BlockStart = B#block.weave_size - B#block.block_size,
			BlockEnd = B#block.weave_size,
            BlockRange = ar_intervals:from_list([{BlockEnd, BlockStart}]),
			Intersection = ar_intervals:intersection(BlockRange, ConfiguredRanges),
            case ar_intervals:is_empty(Intersection) of
				false ->
                    ?debugFmt("Asserting data roots synced for partitions we configured, range intersection: ~p", [Intersection]),
                    wait_until_data_roots_range(BlockStart, BlockEnd);
				true ->
                    ?debugFmt("Asserting no data roots for partitions we did not configure, block range: ~p", [BlockRange]),
                    assert_no_data_roots(BlockStart)
			end
		end,
		TXsAndBlocks
	).

wait_until_data_roots_range(BlockStart, BlockEnd) ->
	true = ar_util:do_until(
		fun() ->
			Peer = ar_test_node:peer_ip(main),
			Path = "/data_roots/" ++ integer_to_list(BlockStart),
			case ar_http:req(#{ method => get, peer => Peer, path => Path }) of
				{ok, {{<<"200">>, _}, _, Body, _, _}} ->
					case ar_serialize:binary_to_data_roots(Body) of
						{ok, {_TXRoot, BlockSize, _Entries}} when BlockStart + BlockSize == BlockEnd ->
                            true;
						_ ->
                            ?debugFmt("Received get data roots reply with unexpected block size", []),
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
		30_000
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