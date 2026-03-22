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

test_data_roots_http_post() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(2_000_000_000_000_000), <<>>}]),
	start_peers_then_disconnect(peer1, main, B0),
	{B, _} = mine_block_with_small_fixed_data_tx(peer1, Wallet),
	%% Mine some empty blocks to push the data block out of the recent window.
	mine_empty_blocks_on_peer_after(peer1, B, 11),
	join_main_on_peer1(B#block.height + 11, 0),
	ar_test_node:disconnect_from(peer1),
	BlockStart = B#block.weave_size - B#block.block_size,
	BlockEnd = B#block.weave_size,
	true = B#block.block_size > 0,
	Peer1PoolBound = ar_test_node:remote_call(peer1, ar_data_sync, get_disk_pool_threshold, []),
	MainPoolBound = ar_test_node:remote_call(main, ar_data_sync, get_disk_pool_threshold, []),
	true = BlockStart < Peer1PoolBound andalso BlockStart < MainPoolBound,
	assert_no_data_roots(main, BlockStart),
	{ok, {{<<"200">>, _}, _, Body, _, _}} = get_data_roots(peer1, BlockStart),
	{ok, {{<<"200">>, _}, _, <<>>, _, _}} = post_data_roots(main, BlockStart, Body),
	wait_until_data_roots_range(main, BlockStart, BlockEnd, B#block.height),
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
	Guaranteed = mine_block_with_small_fixed_data_tx(peer1, Wallet),
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
	mine_empty_blocks_on_peer_after(peer1, LastB, 11),

	join_main_on_peer1(LastB#block.height + 11, 0),
	ar_test_node:disconnect_from(peer1),

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
					{ok, {{<<"200">>, _}, _, Body, _, _}} = get_data_roots(peer1, BlockStart),
					{ok, {{<<"200">>, _}, _, <<>>, _, _}} = post_data_roots(main, BlockStart, Body),

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
	{B, [{TX, Chunks}]} = mine_block_with_small_fixed_data_tx(peer1, Wallet),
	mine_empty_blocks_on_peer_after(peer1, B, 11),
	join_main_on_peer1(B#block.height + 11, 2),
	BlockStart = B#block.weave_size - B#block.block_size,
	BlockEnd = B#block.weave_size,
	true = B#block.block_size > 0,
	wait_until_data_roots_synced(main, BlockStart, BlockEnd, B#block.tx_root),
	%% Allow header_sync jobs to apply blocks after roots exist (ordering under investigation).
	timer:sleep(8000),
	ar_test_node:disconnect_from(peer1),
	%% @TEMPORARY (remove after debugging): explicit POST /data_roots to main — same as
	%% test_data_roots_http_post, to compare ordering vs background ar_data_root_sync.
	_TEMP_Peer1PoolBound = ar_test_node:remote_call(peer1, ar_data_sync, get_disk_pool_threshold, []),
	_TEMP_MainPoolBound = ar_test_node:remote_call(main, ar_data_sync, get_disk_pool_threshold, []),
	case BlockStart < _TEMP_Peer1PoolBound andalso BlockStart < _TEMP_MainPoolBound of
		true ->
			{ok, {{<<"200">>, _}, _, _TEMP_Body, _, _}} = get_data_roots(peer1, BlockStart),
			{ok, {{<<"200">>, _}, _, <<>>, _, _}} = post_data_roots(main, BlockStart, _TEMP_Body),
			wait_until_data_roots_range(main, BlockStart, BlockEnd, B#block.height);
		false ->
			ok
	end,
	post_then_get_chunks(main, B, TX, Chunks),
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

%% Rejoin main onto peer1 with mine disabled and the given header_sync_jobs (e.g. 0 to isolate
%% data_roots tests, or >0 so ar_header_sync may call ar_data_sync:add_block/2 while
%% ar_data_root_sync is also writing roots). Does not disconnect — caller may call
%% ar_test_node:remote_call(main, ar_test_node, disconnect_from, [peer1]) when needed.
join_main_on_peer1(ExpectedHeight, HeaderSyncJobs) ->
	{ok, BaseConfig} = arweave_config:get_env(),
	MainConfig = BaseConfig#config{ mine = false, header_sync_jobs = HeaderSyncJobs },
	ar_test_node:join_on(#{ node => main, join_on => peer1, config => MainConfig }, true),
	ar_test_node:connect_to_peer(peer1),
	ar_test_node:wait_until_joined(main),
	ar_test_node:assert_wait_until_height(main, ExpectedHeight),
	ok.

wait_until_data_roots_synced(Peer, BlockStart, BlockEnd, TXRoot) ->
	true = ar_util:do_until(
		fun() ->
			ar_test_node:remote_call(Peer, ar_data_sync, are_data_roots_synced,
				[BlockStart, BlockEnd, TXRoot])
		end,
		500,
		120_000),
	ok.

%% Mine one block on peer1 with a single small v2 data tx. TXData matches generate_random_txs/1 entries.
mine_block_with_small_fixed_data_tx(Peer, Wallet) ->
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

data_roots_path(BlockStart) ->
	"/data_roots/" ++ integer_to_list(BlockStart).

%% GET /data_roots/{BlockStart} on Peer (returns ar_http:req/1 result).
get_data_roots(Peer, BlockStart) ->
	ar_http:req(#{
		method => get,
		peer => ar_test_node:peer_ip(Peer),
		path => data_roots_path(BlockStart)
	}).

%% POST /data_roots/{BlockStart} to Peer (returns ar_http:req/1 result).
post_data_roots(Peer, BlockStart, Body) ->
	ar_http:req(#{
		method => post,
		peer => ar_test_node:peer_ip(Peer),
		path => data_roots_path(BlockStart),
		body => Body
	}).

%% POST /chunk for each proof from get_records_with_proofs/3, then GET /chunk at the same offset.
post_then_get_chunks(Node, B, TX, Chunks) ->
	Records = ar_test_data_sync:get_records_with_proofs(B, TX, Chunks),
	lists:foreach(
		fun({_, _, _, {GlobalChunkEndOffset, Proof}}) ->
			{ok, {{<<"200">>, _}, _, <<>>, _, _}} =
				ar_test_node:post_chunk(Node, ar_serialize:jsonify(Proof)),
			{ok, GetReply} = ar_test_node:get_chunk(Node, GlobalChunkEndOffset),
			?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, GetReply,
				"POST /chunk returned 200 but GET /chunk for the same global offset must not 404 "
				"(regression: post_chunk_skipped_as_already_synced / background data_roots vs header_sync).")
		end,
		Records
	).

wait_until_data_roots_range(Peer, BlockStart, BlockEnd, Height) ->
	true = ar_util:do_until(
		fun() ->
			case get_data_roots(Peer, BlockStart) of
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
	case get_data_roots(Peer, BlockStart) of
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
            ok;
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			?debugFmt("Body: ~p ~p", [Body, ar_serialize:binary_to_data_roots(Body)]),
            ?assert(false, "Unexpected get data roots reply, expected 404");
		Reply ->
            ?debugFmt("Unexpected reply: ~p (expected 404)", [Reply]),
			?assert(false)
	end.
