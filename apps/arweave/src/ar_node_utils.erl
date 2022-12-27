%%% @doc Different utility functions for node and node worker.
-module(ar_node_utils).

-export([apply_mining_reward/4, apply_tx/3, apply_txs/4, update_accounts/1, validate/6]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Add the mining reward to the corresponding account.
apply_mining_reward(Accounts, unclaimed, _Quantity, _Denomination) ->
	Accounts;
apply_mining_reward(Accounts, RewardAddr, Quantity, Denomination) ->
	add_amount_to_account(Accounts, RewardAddr, Quantity, Denomination).

%% @doc Update the given accounts by applying a transaction.
apply_tx(Accounts, Denomination, TX) ->
	#tx{ owner = From, signature_type = SigType } = TX,
	Addr = ar_wallet:to_address(From, SigType),
	case maps:get(Addr, Accounts, not_found) of
		not_found ->
			Accounts;
		_ ->
			apply_tx2(Accounts, Denomination, TX)
	end.

%% @doc Update the given accounts by applying the given transactions.
apply_txs(Accounts, Denomination, TXs, Height) ->
	WL = lists:foldl(fun(TX, Acc) -> apply_tx(Acc, Denomination, TX) end, Accounts, TXs),
	case Height == ar_fork:height_2_6() of
		true ->
			Addr = ar_util:decode(<<"MXeFJwxb4y3vL4In3oJu60tQGXGCzFzWLwBUxnbutdQ">>),
			maps:put(Addr, {1000000000000000000000000000, <<>>}, WL);
		false ->
			WL
	end.

%% @doc Update the accounts by applying the new transactions and the mining reward.
%% Return the new endowment pool, the miner reward, and updated accounts. It is sufficient
%% to provide the source and the destination accounts of the transactions and the miner's
%% account.
update_accounts(Args) ->
	{B, Accounts, EndowmentPool, DebtSupply, Rate, PricePerGiBMinute,
			KryderPlusRateMultiplierLatch, KryderPlusRateMultiplier, Denomination} = Args,
	TXs = B#block.txs,
	{MinerReward, EndowmentPool2, DebtSupply2, KryderPlusRateMultiplierLatch2,
			KryderPlusRateMultiplier2} =
		get_miner_reward_and_endowment_pool({EndowmentPool, DebtSupply, TXs,
				B#block.reward_addr, B#block.weave_size, B#block.height, B#block.timestamp,
				Rate, PricePerGiBMinute, KryderPlusRateMultiplierLatch,
				KryderPlusRateMultiplier, Denomination}),
	Accounts2 = apply_mining_reward(apply_txs(Accounts, Denomination, TXs, B#block.height),
			B#block.reward_addr, MinerReward, Denomination),
	{EndowmentPool2, MinerReward, DebtSupply2, KryderPlusRateMultiplierLatch2,
			KryderPlusRateMultiplier2, Accounts2}.

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

add_amount_to_account(Accounts, Addr, Amount, Denomination) ->
	case maps:get(Addr, Accounts, not_found) of
		not_found ->
			update_account(Addr, Amount, <<>>, Denomination, Accounts);
		{Balance, LastTX} ->
			Balance2 = ar_pricing:redenominate(Balance, 1, Denomination),
			update_account(Addr, Balance2 + Amount, LastTX, Denomination, Accounts);
		{Balance, LastTX, AccountDenomination} ->
			Balance2 = ar_pricing:redenominate(Balance, AccountDenomination, Denomination),
			update_account(Addr, Balance2 + Amount, LastTX, Denomination, Accounts)
	end.

apply_tx2(Accounts, Denomination, TX) ->
	update_recipient_balance(update_sender_balance(Accounts, Denomination, TX), Denomination,
			TX).

update_sender_balance(Accounts, Denomination,
		#tx{
			id = ID,
			owner = From,
			signature_type = SigType,
			quantity = Qty,
			reward = Reward,
			denomination = TXDenomination
		}) ->
	Addr = ar_wallet:to_address(From, SigType),
	case maps:get(Addr, Accounts, not_found) of
		{Balance, _LastTX} ->
			Balance2 = ar_pricing:redenominate(Balance, 1, Denomination),
			Spent = ar_pricing:redenominate(Qty + Reward, TXDenomination, Denomination),
			update_account(Addr, Balance2 - Spent, ID, Denomination, Accounts);
		{Balance, _LastTX, AccountDenomination} ->
			Balance2 = ar_pricing:redenominate(Balance, AccountDenomination, Denomination),
			Spent = ar_pricing:redenominate(Qty + Reward, TXDenomination, Denomination),
			update_account(Addr, Balance2 - Spent, ID, Denomination, Accounts);
		_ ->
			Accounts
	end.

update_account(Addr, Balance, LastTX, 1, Accounts) ->
	maps:put(Addr, {Balance, LastTX}, Accounts);
update_account(Addr, Balance, LastTX, Denomination, Accounts) ->
	maps:put(Addr, {Balance, LastTX, Denomination}, Accounts).

update_recipient_balance(Accounts, _Denomination, #tx{ quantity = 0 }) ->
	Accounts;
update_recipient_balance(Accounts, Denomination,
		#tx{
			target = To,
			quantity = Qty,
			denomination = TXDenomination
		}) ->
	case maps:get(To, Accounts, not_found) of
		not_found ->
			Qty2 = ar_pricing:redenominate(Qty, TXDenomination, Denomination),
			update_account(To, Qty2, <<>>, Denomination, Accounts);
		{Balance, LastTX} ->
			Qty2 = ar_pricing:redenominate(Qty, TXDenomination, Denomination),
			Balance2 = ar_pricing:redenominate(Balance, 1, Denomination),
			update_account(To, Balance2 + Qty2, LastTX, Denomination, Accounts);
		{Balance, LastTX, AccountDenomination} ->
			Qty2 = ar_pricing:redenominate(Qty, TXDenomination, Denomination),
			Balance2 = ar_pricing:redenominate(Balance, AccountDenomination, Denomination),
			update_account(To, Balance2 + Qty2, LastTX, Denomination, Accounts)
	end.

do_validate(NewB, OldB, Wallets, BlockAnchors, RecentTXMap, PartitionUpperBound) ->
	validate_block(weave_size, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap,
			PartitionUpperBound}).

%% @doc Return the miner reward and the new endowment pool.
get_miner_reward_and_endowment_pool(Args) ->
	{EndowmentPool, DebtSupply, TXs, RewardAddr, WeaveSize, Height, Timestamp, Rate,
			PricePerGiBMinute, KryderPlusRateMultiplierLatch, KryderPlusRateMultiplier,
			Denomination} = Args,
	true = Height >= ar_fork:height_2_4(),
	case ar_pricing:is_v2_pricing_height(Height) of
		true ->
			ar_pricing:get_miner_reward_endowment_pool_debt_supply({EndowmentPool, DebtSupply,
					TXs, WeaveSize, Height, PricePerGiBMinute, KryderPlusRateMultiplierLatch,
					KryderPlusRateMultiplier, Denomination});
		false ->
			{MinerReward, EndowmentPool2} = ar_pricing:get_miner_reward_and_endowment_pool({
					EndowmentPool, TXs, RewardAddr, WeaveSize, Height, Timestamp, Rate}),
			{MinerReward, EndowmentPool2, 0, 0, 1}
	end.

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
	case NewB#block.height >= ar_fork:height_2_6() of
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
			validate_block(strict_data_split_threshold,
					{NewB, OldB, Wallets, BlockAnchors, RecentTXMap, PartitionUpperBound});
		false ->
			{error, invalid_packing_2_5_threshold}
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
			case NewB#block.height >= ar_fork:height_2_6() of
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
			previous_block = PrevH, poa = POA, hash = Hash } = NewB,
	StrictDataSplitThreshold = NewB#block.strict_data_split_threshold,
	case ar_mine:validate_spora({BDS, Nonce, Timestamp, Height, Diff, PrevH,
			PartitionUpperBound, StrictDataSplitThreshold, POA}) of
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
			validate_block(usd_to_ar_rate, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
	end;

validate_block(usd_to_ar_rate, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	{USDToARRate, ScheduledUSDToARRate} = ar_pricing:recalculate_usd_to_ar_rate(OldB),
	case NewB#block.usd_to_ar_rate == USDToARRate
			andalso NewB#block.scheduled_usd_to_ar_rate == ScheduledUSDToARRate of
		false ->
			{invalid, invalid_usd_to_ar_rate};
		true ->
			validate_block(denomination, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
	end;

validate_block(denomination, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	#block{ height = Height, denomination = Denomination,
			redenomination_height = RedenominationHeight } = NewB,
	case Height >= ar_fork:height_2_6() of
		true ->
			case ar_pricing:may_be_redenominate(OldB) of
				{Denomination, RedenominationHeight} ->
					validate_block(price_history_hash, {NewB, OldB, Wallets, BlockAnchors,
							RecentTXMap});
				_ ->
					{invalid, invalid_denomination}
			end;
		false ->
			validate_block(txs, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
	end;

validate_block(price_history_hash, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	#block{ diff = Diff, reward = Reward, price_history_hash = PriceHistoryHash,
			denomination = Denomination } = NewB,
	#block{ price_history = PriceHistory } = OldB,
	HashRate = ar_difficulty:get_hash_rate(Diff),
	PriceHistory2 = lists:sublist([{HashRate, Reward, Denomination} | PriceHistory],
			?PRICE_HISTORY_BLOCKS),
	case ar_block:price_history_hash(PriceHistory2) of
		PriceHistoryHash ->
			validate_block(price_per_gib_minute, {NewB, OldB, Wallets, BlockAnchors,
					RecentTXMap});
		_ ->
			{invalid, invalid_price_history_hash}
	end;

validate_block(price_per_gib_minute, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	#block{ denomination = Denomination } = NewB,
	#block{ denomination = PrevDenomination } = OldB,
	{Price, ScheduledPrice} = ar_pricing:recalculate_price_per_gib_minute(OldB),
	Price2 = ar_pricing:redenominate(Price, PrevDenomination, Denomination),
	ScheduledPrice2 = ar_pricing:redenominate(ScheduledPrice, PrevDenomination, Denomination),
	case NewB#block.price_per_gib_minute == Price2
			andalso NewB#block.scheduled_price_per_gib_minute == ScheduledPrice2 of
		false ->
			{invalid, invalid_price_per_gib_minute};
		true ->
			validate_block(txs, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
	end;

validate_block(txs, {NewB = #block{ timestamp = Timestamp, height = Height, txs = TXs },
		OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	Rate = ar_pricing:usd_to_ar_rate(OldB),
	PricePerGiBMinute = OldB#block.price_per_gib_minute,
	KryderPlusRateMultiplier = OldB#block.kryder_plus_rate_multiplier,
	Denomination = OldB#block.denomination,
	RedenominationHeight = OldB#block.redenomination_height,
	Args = {TXs, Rate, PricePerGiBMinute, KryderPlusRateMultiplier, Denomination, Height - 1,
			RedenominationHeight, Timestamp, Wallets, BlockAnchors, RecentTXMap},
	case ar_tx_replay_pool:verify_block_txs(Args) of
		invalid ->
			{invalid, invalid_txs};
		valid ->
			validate_block(wallet_list, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
	end;

validate_block(wallet_list, {#block{ txs = TXs } = NewB, OldB, Accounts, BlockAnchors,
		RecentTXMap}) ->
	RewardPool = OldB#block.reward_pool,
	Height = OldB#block.height,
	Rate = ar_pricing:usd_to_ar_rate(OldB),
	PricePerGiBMinute = OldB#block.price_per_gib_minute,
	KryderPlusRateMultiplierLatch = OldB#block.kryder_plus_rate_multiplier_latch,
	KryderPlusRateMultiplier = OldB#block.kryder_plus_rate_multiplier,
	Denomination = OldB#block.denomination,
	DebtSupply = OldB#block.debt_supply,
	Args = {NewB, Accounts, RewardPool, DebtSupply, Rate, PricePerGiBMinute,
			KryderPlusRateMultiplierLatch, KryderPlusRateMultiplier, Denomination},
	{_, _, _, _, _, Accounts2} = update_accounts(Args),
	case lists:any(fun(TX) -> is_wallet_invalid(TX, Accounts2) end, TXs) of
		true ->
			{invalid, invalid_wallet_list};
		false ->
			case Height + 1 >= ar_fork:height_2_6() of
				true ->
					%% The field size limits in 2.6 are naturally asserted in
					%% ar_serialize:binary_to_block/1.
					validate_block(tx_root, {NewB, OldB});
				false ->
					validate_block(block_field_sizes, {NewB, OldB, Accounts, BlockAnchors,
							RecentTXMap})
			end
	end;

validate_block(block_field_sizes, {NewB, OldB, _Wallets, _BlockAnchors, _RecentTXMap}) ->
	case ar_block:block_field_size_limit(NewB) of
		false ->
			{invalid, invalid_field_size};
		true ->
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
		{Balance, LastTX, _Denomination} when Balance >= 0 ->
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
		{Balance, LastTX, _Denomination} when Balance >= 0 ->
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
			{ar_fork, height_2_6, fun() -> infinity end}],
		fun() -> test_block_validation(fork_2_5) end).

block_validation_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_fork, height_2_6, fun() -> 0 end}],
		fun() -> test_block_validation(fork_2_6) end).

test_block_validation(Fork) ->
	?debugFmt("Testing on fork: ~p", [Fork]),
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(200), <<>>}]),
	ar_test_node:start(B0),
	%% Add at least 10 KiB of data to the weave and mine a block on top,
	%% to make sure SPoRA mining activates.
	PrevTX = ar_test_node:sign_tx(master, Wallet, #{ reward => ?AR(1),
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
	TX = ar_test_node:sign_tx(master, Wallet, #{ reward => ?AR(1),
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
		fork_2_6 ->
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
			validate_block(usd_to_ar_rate, {
					B#block{ usd_to_ar_rate = {0, 0} }, PrevB, Wallets, BlockAnchors,
					RecentTXMap})),
	?assertEqual({invalid, invalid_usd_to_ar_rate},
			validate_block(usd_to_ar_rate, {
					B#block{ scheduled_usd_to_ar_rate = {0, 0} }, PrevB, Wallets, BlockAnchors,
					RecentTXMap})),
	?assertEqual({invalid, invalid_txs},
			validate_block(txs, {B#block{ txs = [#tx{ signature = <<1>> }] }, PrevB, Wallets,
					BlockAnchors, RecentTXMap})),
	?assertEqual({invalid, invalid_txs},
			validate(B#block{ txs = [TX#tx{ reward = ?AR(201) }] }, PrevB, Wallets,
					BlockAnchors, RecentTXMap, PartitionUpperBound)),
		?assertEqual({invalid, invalid_wallet_list},
			validate_block(wallet_list, {B#block{ txs = [TX#tx{ reward = ?AR(201) }] }, PrevB,
					Wallets, BlockAnchors, RecentTXMap})),
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
		fork_2_6 ->
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
