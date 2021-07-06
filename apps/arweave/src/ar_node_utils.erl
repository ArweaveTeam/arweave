%%% @doc Different utility functions for node and node worker.
-module(ar_node_utils).

-export([apply_mining_reward/3, apply_tx/3, apply_txs/3, update_wallets/5, validate/6]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Calculate and apply mining reward quantities to a wallet list.
apply_mining_reward(WalletList, unclaimed, _Quantity) ->
	WalletList;
apply_mining_reward(WalletList, RewardAddr, Quantity) ->
	alter_wallet(WalletList, RewardAddr, Quantity).

%% @doc Apply a transaction to a wallet list, updating it.
apply_tx(WalletList, unavailable, _) ->
	WalletList;
apply_tx(WalletList, TX, Height) ->
	do_apply_tx(WalletList, TX, Height).

%% @doc Update a wallet list with a set of new transactions.
apply_txs(WalletList, TXs, Height) ->
	lists:foldl(
		fun(TX, Acc) ->
			apply_tx(Acc, TX, Height)
		end,
		WalletList,
		TXs
	).

%% @doc Update the wallets (accounts) by applying the new transactions and the mining reward.
%% Return the new endowment pool, the miner reward, and updated accounts. It is sufficient
%% to provide the source and the destination accounts of the transactions and the miner's
%% account.
update_wallets(NewB, Wallets, RewardPool, Rate, Height) ->
	TXs = NewB#block.txs,
	{FinderReward, NewRewardPool} =
		get_miner_reward_and_endowment_pool({
			RewardPool,
			TXs,
			NewB#block.reward_addr,
			NewB#block.weave_size,
			NewB#block.height,
			NewB#block.diff,
			NewB#block.timestamp,
			Rate
		}),
	UpdatedWallets =
		apply_mining_reward(
			apply_txs(Wallets, TXs, Height),
			NewB#block.reward_addr,
			FinderReward
		),
	{NewRewardPool, FinderReward, UpdatedWallets}.

%% @doc Validate a block. The block has been already partially validated before gossip so
%% we only validate here what we did not validate then. Also, we do not validate the
%% nonce limiter chain here. Finally, the 'wallet_list', 'reward_pool', and 'reward' fields
%% are validated in ar_node_worker:validate_wallet_list/5.
validate(NewB, B, Wallets, BlockAnchors, RecentTXMap, PartitionUpperBound) ->
	?LOG_INFO([{event, validating_block}, {hash, ar_util:encode(NewB#block.indep_hash)}]),
	case timer:tc(
		fun() ->
			do_validate(NewB, B, Wallets, BlockAnchors, RecentTXMap, PartitionUpperBound)
		end
	) of
		{TimeTaken, valid} ->
			?LOG_INFO([{event, block_validation_successful},
					{hash, ar_util:encode(NewB#block.indep_hash)},
					{time_taken_us, TimeTaken}]),
			valid;
		{TimeTaken, {invalid, Reason}} ->
			?LOG_INFO([{event, block_validation_failed}, {reason, Reason},
					{hash, ar_util:encode(NewB#block.indep_hash)},
					{time_taken_us, TimeTaken}]),
			{invalid, Reason}
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

alter_wallet(Wallets, Target, Adjustment) ->
	case maps:get(Target, Wallets, not_found) of
		not_found ->
			maps:put(Target, {Adjustment, <<>>}, Wallets);
		{Balance, LastTX} ->
			maps:put(Target, {Balance + Adjustment, LastTX}, Wallets)
	end.

do_apply_tx(
		Wallets,
		TX = #tx{
			last_tx = Last,
			owner = From,
			signature_type = SigType
		},
		Height) ->
	Addr = ar_wallet:to_address(From, SigType),
	Fork_1_8 = ar_fork:height_1_8(),
	case {Height, maps:get(Addr, Wallets, not_found)} of
		{H, {_Balance, _LastTX}} when H >= Fork_1_8 ->
			do_apply_tx(Wallets, TX);
		{_, {_Balance, Last}} ->
			do_apply_tx(Wallets, TX);
		_ ->
			Wallets
	end.

do_apply_tx(WalletList, TX) ->
	update_recipient_balance(
		update_sender_balance(WalletList, TX),
		TX
	).

update_sender_balance(
		Wallets,
		#tx {
			id = ID,
			owner = From,
			signature_type = SigType,
			quantity = Qty,
			reward = Reward
		}) ->
	Addr = ar_wallet:to_address(From, SigType),
	case maps:get(Addr, Wallets, not_found) of
		{Balance, _LastTX} ->
			maps:put(Addr, {Balance - (Qty + Reward), ID}, Wallets);
		_ ->
			Wallets
	end.

update_recipient_balance(Wallets, #tx { quantity = 0 }) ->
	Wallets;
update_recipient_balance(
		Wallets,
		#tx {
			target = To,
			quantity = Qty
		}) ->
	case maps:get(To, Wallets, not_found) of
		not_found ->
			maps:put(To, {Qty, <<>>}, Wallets);
		{OldBalance, LastTX} ->
			maps:put(To, {OldBalance + Qty, LastTX}, Wallets)
	end.

do_validate(NewB, OldB, Wallets, BlockAnchors, RecentTXMap, PartitionUpperBound) ->
	validate_block(weave_size, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap,
			PartitionUpperBound}).

%% @doc Return the miner reward and the new endowment pool.
get_miner_reward_and_endowment_pool(Args) ->
	{RewardPool, TXs, RewardAddr, WeaveSize, Height, _Diff, Timestamp, Rate} = Args,
	true = Height >= ar_fork:height_2_4(),
	ar_pricing:get_miner_reward_and_endowment_pool({RewardPool, TXs, RewardAddr, WeaveSize,
			Height, Timestamp, Rate}).

validate_block(weave_size, {#block{ txs = TXs } = NewB, OldB, Wallets, BlockAnchors,
		RecentTXMap, PartitionUpperBound}) ->
	case ar_block:verify_weave_size(NewB, OldB, TXs) of
		false ->
			{invalid, invalid_weave_size};
		true ->
			validate_block(previous_block, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap,
					PartitionUpperBound})
	end;
validate_block(previous_block, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap,
		PartitionUpperBound}) ->
	case OldB#block.indep_hash == NewB#block.previous_block of
		false ->
			{invalid, invalid_previous_block};
		true ->
			validate_block(previous_solution_hash,
				{NewB, OldB, Wallets, BlockAnchors, RecentTXMap, PartitionUpperBound})
	end;
validate_block(previous_solution_hash, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap,
		PartitionUpperBound}) ->
	case NewB#block.height >= ar_fork:height_2_7() of
		true ->
			case NewB#block.previous_solution_hash == OldB#block.hash of
				false ->
					{invalid, invalid_previous_solution_hash};
				true ->
					validate_block(packing_2_5_threshold, {NewB, OldB, Wallets, BlockAnchors,
							RecentTXMap, PartitionUpperBound})
			end;
		false ->
			validate_block(packing_2_5_threshold, {NewB, OldB, Wallets, BlockAnchors,
					RecentTXMap, PartitionUpperBound})
	end;
validate_block(packing_2_5_threshold, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap,
		PartitionUpperBound}) ->
	ExpectedPackingThreshold = ar_block:get_packing_threshold(OldB, PartitionUpperBound),
	Valid =
		case ExpectedPackingThreshold of
			undefined ->
				true;
			_ ->
				NewB#block.packing_2_5_threshold == ExpectedPackingThreshold
		end,
	case Valid of
		true ->
			validate_block(packing_2_6_threshold,
					{NewB, OldB, Wallets, BlockAnchors, RecentTXMap, PartitionUpperBound});
		false ->
			{error, invalid_packing_2_5_threshold}
	end;
validate_block(packing_2_6_threshold, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap,
		PartitionUpperBound}) ->
	ExpectedPackingThreshold = ar_block:get_packing_2_6_threshold(OldB),
	Valid =
		case ExpectedPackingThreshold of
			undefined ->
				true;
			_ ->
				NewB#block.packing_2_6_threshold == ExpectedPackingThreshold
		end,
	case Valid of
		true ->
			validate_block(strict_data_split_threshold,
					{NewB, OldB, Wallets, BlockAnchors, RecentTXMap, PartitionUpperBound});
		false ->
			{error, invalid_packing_2_6_threshold}
	end;
validate_block(strict_data_split_threshold,
		{NewB, OldB, Wallets, BlockAnchors, RecentTXMap, PartitionUpperBound}) ->
	Height = NewB#block.height,
	Fork_2_5 = ar_fork:height_2_5(),
	Valid =
		case Height == Fork_2_5 of
			true ->
				NewB#block.strict_data_split_threshold == OldB#block.weave_size;
			false ->
				case Height > Fork_2_5 of
					true ->
						NewB#block.strict_data_split_threshold ==
								OldB#block.strict_data_split_threshold;
					false ->
						true
				end
		end,
	case Valid of
		true ->
			case NewB#block.height >= ar_fork:height_2_7() of
				false ->
					validate_block(spora, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap,
							PartitionUpperBound});
				true ->
					validate_block(usd_to_ar_rate, {NewB, OldB, Wallets, BlockAnchors,
							RecentTXMap})
			end;
		false ->
			{error, invalid_strict_data_split_threshold}
	end;
validate_block(spora, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap,
		PartitionUpperBound}) ->
	BDS = ar_block:generate_block_data_segment(NewB),
	#block{ nonce = Nonce, height = Height, timestamp = Timestamp, diff = Diff,
			previous_block = PrevH, poa = POA, hash = Hash, reward_addr = RewardAddr } = NewB,
	Packing_2_6_Threshold = NewB#block.packing_2_6_threshold,
	StrictDataSplitThreshold = NewB#block.strict_data_split_threshold,
	case ar_mine:validate_spora({BDS, Nonce, Timestamp, Height, Diff, PrevH,
			PartitionUpperBound, Packing_2_6_Threshold, StrictDataSplitThreshold, RewardAddr,
			POA}) of
		{false, Hash} ->
			{invalid, invalid_spora};
		{false, _} ->
			{invalid, invalid_spora_hash};
		{true, Hash, _} ->
			validate_block(difficulty, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap});
		{true, _, _} ->
			{invalid, invalid_spora_hash}
	end;
validate_block(difficulty, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	case ar_retarget:validate_difficulty(NewB, OldB) of
		false ->
			{invalid, invalid_difficulty};
		true ->
			validate_block(may_be_usd_to_ar_rate, {NewB, OldB, Wallets, BlockAnchors,
					RecentTXMap})
	end;
validate_block(may_be_usd_to_ar_rate, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	#block{ height = Height } = NewB,
	case Height >= ar_fork:height_2_5() of
		true ->
			validate_block(usd_to_ar_rate, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap});
		false ->
			validate_block(wallet_list, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
	end;
validate_block(usd_to_ar_rate, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	{USDToARRate, ScheduledUSDToARRate} = ar_pricing:recalculate_usd_to_ar_rate(OldB),
	case NewB#block.usd_to_ar_rate == USDToARRate
			andalso NewB#block.scheduled_usd_to_ar_rate == ScheduledUSDToARRate of
		false ->
			{invalid, invalid_usd_to_ar_rate};
		true ->
			validate_block(wallet_list, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
	end;
validate_block(wallet_list, {#block{ txs = TXs } = NewB, OldB, Wallets, BlockAnchors,
		RecentTXMap}) ->
	RewardPool = OldB#block.reward_pool,
	Height = OldB#block.height,
	Rate = ar_pricing:usd_to_ar_rate(OldB),
	{_, _, UpdatedWallets} = update_wallets(NewB, Wallets, RewardPool, Rate, Height),
	case lists:any(fun(TX) -> is_wallet_invalid(TX, UpdatedWallets) end, TXs) of
		true ->
			{invalid, invalid_wallet_list};
		false ->
			case Height + 1 >= ar_fork:height_2_6() of
				true ->
					%% The field size limits in 2.6 are naturally asserted in
					%% ar_serialize:binary_to_block/1.
					validate_block(txs, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap});
				false ->
					validate_block(block_field_sizes, {NewB, OldB, Wallets, BlockAnchors,
							RecentTXMap})
			end
	end;
validate_block(block_field_sizes, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	case ar_block:block_field_size_limit(NewB) of
		false ->
			{invalid, invalid_field_size};
		true ->
			validate_block(txs, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
	end;
validate_block(txs, {NewB = #block{ timestamp = Timestamp, height = Height, txs = TXs },
		OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	Rate = ar_pricing:usd_to_ar_rate(OldB),
	Args = {TXs, Rate, Height - 1, Timestamp, Wallets, BlockAnchors, RecentTXMap},
	case ar_tx_replay_pool:verify_block_txs(Args) of
		invalid ->
			{invalid, invalid_txs};
		valid ->
			validate_block(tx_root, {NewB, OldB})
	end;
validate_block(tx_root, {NewB, OldB}) ->
	case ar_block:verify_tx_root(NewB) of
		false ->
			{invalid, invalid_tx_root};
		true ->
			validate_block(block_index_root, {NewB, OldB})
	end;
validate_block(block_index_root, {NewB, OldB}) ->
	case ar_block:verify_block_hash_list_merkle(NewB, OldB) of
		false ->
			{invalid, invalid_block_index_root};
		true ->
			validate_block(last_retarget, {NewB, OldB})
	end;
validate_block(last_retarget, {NewB, OldB}) ->
	case ar_block:verify_last_retarget(NewB, OldB) of
		false ->
			{invalid, invalid_last_retarget};
		true ->
			validate_block(cumulative_diff, {NewB, OldB})
	end;
validate_block(cumulative_diff, {NewB, OldB}) ->
	case ar_block:verify_cumulative_diff(NewB, OldB) of
		false ->
			{invalid, invalid_cumulative_difficulty};
		true ->
			valid
	end.

-ifdef(DEBUG).
is_wallet_invalid(#tx{ signature = <<>> }, _Wallets) ->
	false;
is_wallet_invalid(#tx{ owner = Owner, signature_type = SigType }, Wallets) ->
	Address = ar_wallet:to_address(Owner, SigType),
	case maps:get(Address, Wallets, not_found) of
		{Balance, LastTX} when Balance >= 0 ->
			case Balance of
				0 ->
					byte_size(LastTX) == 0;
				_ ->
					false
			end;
		_ ->
			true
	end.
-else.
is_wallet_invalid(#tx{ owner = Owner, signature_type = SigType }, Wallets) ->
	Address = ar_wallet:to_address(Owner, SigType),
	case maps:get(Address, Wallets, not_found) of
		{Balance, LastTX} when Balance >= 0 ->
			case Balance of
				0 ->
					byte_size(LastTX) == 0;
				_ ->
					false
			end;
		_ ->
			true
	end.
-endif.

%%%===================================================================
%%% Tests.
%%%===================================================================

block_validation_test_pre_fork_2_6_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_fork, height_2_6, fun() -> infinity end},
			{ar_fork, height_2_7, fun() -> infinity end}],
		fun() -> test_block_validation(fork_2_5) end).

block_validation_test_pre_fork_2_7_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_fork, height_2_6, fun() -> 0 end},
			{ar_fork, height_2_7, fun() -> infinity end}],
		fun() -> test_block_validation(fork_2_6) end).

block_validation_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_fork, height_2_6, fun() -> 0 end},
			{ar_fork, height_2_7, fun() -> 0 end}],
		fun() -> test_block_validation(fork_2_7) end).

test_block_validation(Fork) ->
	?debugFmt("Testing on fork: ~p", [Fork]),
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(200), <<>>}]),
	ar_test_node:start(B0),
	%% Add at least 10 KiB of data to the weave and mine a block on top,
	%% to make sure SPoRA mining activates.
	PrevTX = ar_test_node:sign_tx(Wallet, #{ reward => ?AR(1),
			data => crypto:strong_rand_bytes(10 * 1024 * 1024) }),
	ar_test_node:assert_post_tx_to_master(PrevTX),
	ar_node:mine(),
	[_ | _] = ar_test_node:wait_until_height(1),
	ar_node:mine(),
	[{PrevH, _, _} | _ ] = ar_test_node:wait_until_height(2),
	PrevB = ar_node:get_block_shadow_from_cache(PrevH),
	BI = ar_node:get_block_index(),
	PartitionUpperBound = ar_node:get_partition_upper_bound(BI),
	BlockAnchors = ar_node:get_block_anchors(),
	RecentTXMap = ar_node:get_recent_txs_map(),
	TX = ar_test_node:sign_tx(Wallet, #{ reward => ?AR(1),
			data => crypto:strong_rand_bytes(7 * 1024 * 1024), last_tx => PrevH }),
	ar_test_node:assert_post_tx_to_master(TX),
	ar_node:mine(),
	[{H, _, _} | _] = ar_test_node:wait_until_height(3),
	B = ar_node:get_block_shadow_from_cache(H),
	Wallets = #{ ar_wallet:to_address(Pub) => {?AR(200), <<>>} },
	?assertEqual(valid, validate(B, PrevB, Wallets, BlockAnchors, RecentTXMap,
			PartitionUpperBound)),
	?assertEqual({invalid, invalid_weave_size},
			validate(B#block{ weave_size = PrevB#block.weave_size + 1 }, PrevB, Wallets,
					BlockAnchors, RecentTXMap, PartitionUpperBound)),
	?assertEqual({invalid, invalid_previous_block},
			validate(B#block{ previous_block = B#block.indep_hash }, PrevB, Wallets,
					BlockAnchors, RecentTXMap, PartitionUpperBound)),
	InvLastRetargetB = B#block{ last_retarget = B#block.timestamp },
	InvDataRootB = B#block{ tx_root = crypto:strong_rand_bytes(32) },
	InvBlockIndexRootB = B#block{ hash_list_merkle = crypto:strong_rand_bytes(32) },
	InvCDiffB = B#block{ cumulative_diff = PrevB#block.cumulative_diff * 1000 },
	case Fork of
		fork_2_7 ->
			ok;
		_ ->
			?assertEqual({invalid, invalid_spora_hash},
					validate(B#block{ diff = PrevB#block.diff - 1 }, PrevB, Wallets,
							BlockAnchors, RecentTXMap, PartitionUpperBound)),
			?assertEqual({invalid, invalid_spora_hash},
					validate(B#block{ tags = [<<"N">>, <<"V">>] }, PrevB, Wallets,
							BlockAnchors, RecentTXMap, PartitionUpperBound)),
			?assertEqual({invalid, invalid_spora_hash},
					validate(InvDataRootB#block{
							indep_hash = ar_block:indep_hash(InvDataRootB) },
						PrevB, Wallets, BlockAnchors, RecentTXMap, PartitionUpperBound)),
			?assertEqual({invalid, invalid_spora_hash},
					validate(InvLastRetargetB#block{
							indep_hash = ar_block:indep_hash(InvLastRetargetB) },
					PrevB, Wallets, BlockAnchors, RecentTXMap, PartitionUpperBound)),
			?assertEqual({invalid, invalid_spora_hash},
					validate(
						InvBlockIndexRootB#block{
							indep_hash = ar_block:indep_hash(InvBlockIndexRootB) },
						PrevB, Wallets, BlockAnchors, RecentTXMap, PartitionUpperBound)),
			?assertEqual({invalid, invalid_spora_hash},
					validate(InvCDiffB#block{ indep_hash = ar_block:indep_hash(InvCDiffB) },
							PrevB, Wallets, BlockAnchors, RecentTXMap, PartitionUpperBound))
	end,
	?assertEqual({invalid, invalid_difficulty},
			validate_block(difficulty, {
					B#block{ diff = PrevB#block.diff - 1 }, PrevB, Wallets, BlockAnchors,
					RecentTXMap})),
	?assertEqual({invalid, invalid_usd_to_ar_rate},
			validate_block(may_be_usd_to_ar_rate, {
					B#block{ usd_to_ar_rate = {0, 0} }, PrevB, Wallets, BlockAnchors,
					RecentTXMap})),
	?assertEqual({invalid, invalid_usd_to_ar_rate},
			validate_block(may_be_usd_to_ar_rate, {
					B#block{ scheduled_usd_to_ar_rate = {0, 0} }, PrevB, Wallets, BlockAnchors,
					RecentTXMap})),
	?assertEqual({invalid, invalid_txs},
			validate_block(txs, {B#block{ txs = [#tx{ signature = <<1>> }] }, PrevB, Wallets,
					BlockAnchors, RecentTXMap})),
	?assertEqual({invalid, invalid_wallet_list},
			validate(B#block{ txs = [TX#tx{ reward = ?AR(201) }] }, PrevB, Wallets,
					BlockAnchors, RecentTXMap, PartitionUpperBound)),
	?assertEqual({invalid, invalid_tx_root},
			validate_block(tx_root, {
				InvDataRootB#block{ indep_hash = ar_block:indep_hash(InvDataRootB) }, PrevB})),
	?assertEqual({invalid, invalid_difficulty},
			validate_block(difficulty, {
					InvLastRetargetB#block{
						indep_hash = ar_block:indep_hash(InvLastRetargetB) },
					PrevB, Wallets, BlockAnchors, RecentTXMap})),
	?assertEqual({invalid, invalid_block_index_root},
			validate_block(block_index_root, {
				InvBlockIndexRootB#block{
					indep_hash = ar_block:indep_hash(InvBlockIndexRootB) }, PrevB})),
	?assertEqual({invalid, invalid_last_retarget},
			validate_block(last_retarget, {B#block{ last_retarget = 0 }, PrevB})),
	?assertEqual(
		{invalid, invalid_cumulative_difficulty},
		validate_block(cumulative_diff, {
				InvCDiffB#block{ indep_hash = ar_block:indep_hash(InvCDiffB) }, PrevB})),
	BI2 = ar_node:get_block_index(),
	PartitionUpperBound2 = ar_node:get_partition_upper_bound(BI2),
	BlockAnchors2 = ar_node:get_block_anchors(),
	RecentTXMap2 = ar_node:get_recent_txs_map(),
	ar_node:mine(),
	[{H2, _, _} | _ ] = ar_test_node:wait_until_height(4),
	B2 = ar_node:get_block_shadow_from_cache(H2),
	?assertEqual(valid, validate(B2, B, Wallets, BlockAnchors2, RecentTXMap2,
			PartitionUpperBound2)),
	case Fork of
		fork_2_7 ->
			ok;
		_ ->
			?assertEqual({invalid, invalid_spora_hash},
					validate(B2#block{ poa = #poa{} }, B, Wallets, BlockAnchors2, RecentTXMap2,
							PartitionUpperBound2)),
			?assertEqual({invalid, invalid_spora_hash},
					validate(B2#block{ hash = <<>> }, B, Wallets, BlockAnchors2, RecentTXMap2,
							PartitionUpperBound2)),
			?assertEqual({invalid, invalid_spora_hash},
					validate(B2#block{ hash = B#block.hash }, B, Wallets, BlockAnchors2,
							RecentTXMap2, PartitionUpperBound2)),
			PoA = B2#block.poa,
			FakePoA =
				case get_chunk(1) of
					P when P#poa.chunk == PoA#poa.chunk ->
						get_chunk(1 + 256 * 1024);
					P ->
						P
				end,
			{FakeH0, FakeEntropy} = ar_mine:spora_h0_with_entropy(
					ar_block:generate_block_data_segment(B2#block{ poa = FakePoA }),
					B2#block.nonce, B2#block.height),
			{FakeSolutionHash, _} = ar_mine:spora_solution_hash_with_entropy(
					B#block.indep_hash, B2#block.timestamp, FakeH0, FakePoA#poa.chunk,
					FakeEntropy, B2#block.height),
			{FakeSolutionHashNoEntropy, _} = ar_mine:spora_solution_hash(B#block.indep_hash,
					B2#block.timestamp, FakeH0, FakePoA#poa.chunk, B2#block.height),
			?assertEqual({invalid, invalid_spora},
					validate(B2#block{ poa = FakePoA, hash = FakeSolutionHash }, B, Wallets,
							BlockAnchors2, RecentTXMap2, PartitionUpperBound2)),
			?assertEqual({invalid, invalid_spora_hash},
					validate(B2#block{ poa = FakePoA, hash = FakeSolutionHashNoEntropy }, B,
							Wallets, BlockAnchors2, RecentTXMap2, PartitionUpperBound2))
	end.

get_chunk(Byte) ->
	{ok, {{<<"200">>, _}, _, JSON, _, _}} = ar_test_node:get_chunk(Byte),
	#{
		chunk := Chunk,
		data_path := DataPath,
		tx_path := TXPath
	} = ar_serialize:json_map_to_chunk_proof(jiffy:decode(JSON, [return_maps])),
	#poa{ chunk = Chunk, data_path = DataPath, tx_path = TXPath, option = 1 }.
