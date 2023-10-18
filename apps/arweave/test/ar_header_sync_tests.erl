-module(ar_header_sync_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_header_sync.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

-import(ar_test_node, [
	
	
	sign_v1_tx/3, assert_post_tx_to_master/1,
	wait_until_height/1, assert_wait_until_height/2,
	read_block_when_stored/1,
	random_v1_data/1
]).

syncs_headers_test_() ->
	{timeout, 360, fun test_syncs_headers/0}.

test_syncs_headers() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(2000), <<>>}]),
	ar_test_node:start(B0),
	post_random_blocks(Wallet, ?MAX_TX_ANCHOR_DEPTH + 5, B0),
	ar_test_node:join_on(peer1, main),
	BI = assert_wait_until_height(peer1, ?MAX_TX_ANCHOR_DEPTH + 5),
	lists:foreach(
		fun(Height) ->
			{ok, B} = ar_util:do_until(
				fun() ->
					case ar_test_node:remote_call(peer1, ar_storage, read_block, [Height, BI]) of
						unavailable ->
							unavailable;
						B2 ->
							{ok, B2}
					end
				end,
				200,
				30000
			),
			MasterB = ar_storage:read_block(Height, ar_node:get_block_index()),
			?assertEqual(B, MasterB),
			TXs = ar_test_node:remote_call(peer1, ar_storage, read_tx, [B#block.txs]),
			MasterTXs = ar_storage:read_tx(B#block.txs),
			?assertEqual(TXs, MasterTXs)
		end,
		lists:reverse(lists:seq(0, ?MAX_TX_ANCHOR_DEPTH + 5))
	),
	%% Throw the event to simulate running out of disk space.
	ar_disksup:pause(),
	ar_events:send(disksup, {remaining_disk_space, "default", true, 0, 0}),
	NoSpaceHeight = ?MAX_TX_ANCHOR_DEPTH + 6,
	NoSpaceTX = sign_v1_tx(master, Wallet,
		#{ data => random_v1_data(10 * 1024), last_tx => ar_test_node:get_tx_anchor(peer1) }),
	assert_post_tx_to_master(NoSpaceTX),
	ar_test_node:mine(),
	[{NoSpaceH, _, _} | _] = wait_until_height(NoSpaceHeight),
	timer:sleep(1000),
	%% The cleanup is not expected to kick in yet.
	NoSpaceB = read_block_when_stored(NoSpaceH),
	?assertMatch(#block{}, NoSpaceB),
	?assertMatch(#tx{}, ar_storage:read_tx(NoSpaceTX#tx.id)),
	?assertMatch({ok, _}, ar_storage:read_wallet_list(NoSpaceB#block.wallet_list)),
	ets:new(test_syncs_header, [set, named_table]),
	ets:insert(test_syncs_header, {height, NoSpaceHeight + 1}),
	true = ar_util:do_until(
		fun() ->
			%% Keep mining blocks. At some point the cleanup procedure will
			%% kick in and remove the oldest files.
			TX = sign_v1_tx(master, Wallet, #{
				data => random_v1_data(200 * 1024), last_tx => ar_test_node:get_tx_anchor(peer1) }),
			assert_post_tx_to_master(TX),
			ar_test_node:mine(),
			[{_, Height}] = ets:lookup(test_syncs_header, height),
			[_ | _] = wait_until_height(Height),
			ets:insert(test_syncs_header, {height, Height + 1}),
			unavailable == ar_storage:read_block(NoSpaceH)
				andalso ar_storage:read_tx(NoSpaceTX#tx.id) == unavailable
		end,
		100,
		10000
	),
	timer:sleep(1000),
	[{LatestH, _, _} | _] = ar_node:get_block_index(),
	%% The latest block must not be cleaned up.
	LatestB = read_block_when_stored(LatestH),
	?assertMatch(#block{}, LatestB),
	?assertMatch(#tx{}, ar_storage:read_tx(lists:nth(1, LatestB#block.txs))),
	?assertMatch({ok, _}, ar_storage:read_wallet_list(LatestB#block.wallet_list)),
	ar_disksup:resume().

post_random_blocks(Wallet, TargetHeight, B0) ->
	lists:foldl(
		fun(Height, Anchor) ->
			TXs =
				lists:foldl(
					fun(_, Acc) ->
						case rand:uniform(2) == 1 of
							true ->
								TX = ar_test_node:sign_tx(main, Wallet,
									#{
										last_tx => Anchor,
										data => crypto:strong_rand_bytes(10 * 1024 * 1024)
									}),
								assert_post_tx_to_master(TX),
								[TX | Acc];
							false ->
								Acc
						end
					end,
					[],
					lists:seq(1, 2)
				),
			ar_test_node:mine(),
			[{H, _, _} | _] = wait_until_height(Height),
			?assertEqual(length(TXs), length((read_block_when_stored(H))#block.txs)),
			H
		end,
		B0#block.indep_hash,
		lists:seq(1, TargetHeight)
	).
