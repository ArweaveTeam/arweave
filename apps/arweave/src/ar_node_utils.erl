%%% @doc Different utility functions for node and node worker.
-module(ar_node_utils).

-export([apply_mining_reward/4, apply_tx/3, apply_txs/3, update_accounts/3, validate/6]).

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
apply_txs(Accounts, Denomination, TXs) ->
	lists:foldl(fun(TX, Acc) -> apply_tx(Acc, Denomination, TX) end, Accounts, TXs).

%% @doc Distribute transaction fees across accounts and the endowment pool,
%% reserve a reward for the current miner, release the reserved reward to the corresponding
%% miner. If a double-signing proof is provided, ban the account and assign a
%% reward to the prover.
update_accounts(B, PrevB, Accounts) ->
	EndowmentPool = PrevB#block.reward_pool,
	Rate = ar_pricing:usd_to_ar_rate(PrevB),
	PricePerGiBMinute = PrevB#block.price_per_gib_minute,
	KryderPlusRateMultiplierLatch = PrevB#block.kryder_plus_rate_multiplier_latch,
	KryderPlusRateMultiplier = PrevB#block.kryder_plus_rate_multiplier,
	Denomination = PrevB#block.denomination,
	DebtSupply = PrevB#block.debt_supply,
	TXs = B#block.txs,
	{MinerReward, EndowmentPool2, DebtSupply2, KryderPlusRateMultiplierLatch2,
			KryderPlusRateMultiplier2} = Args =
		get_miner_reward_and_endowment_pool({EndowmentPool, DebtSupply, TXs,
				B#block.reward_addr, B#block.weave_size, B#block.height, B#block.timestamp,
				Rate, PricePerGiBMinute, KryderPlusRateMultiplierLatch,
				KryderPlusRateMultiplier, Denomination}),
	Accounts2 = apply_txs(Accounts, Denomination, TXs),
	case B#block.height >= ar_fork:height_2_6() of
		false ->
			Accounts3 = apply_mining_reward(Accounts2, B#block.reward_addr, MinerReward,
					Denomination),
			case validate_account_anchors(Accounts3, B#block.txs) of
				true ->
					{ok, {EndowmentPool2, MinerReward, DebtSupply2,
							KryderPlusRateMultiplierLatch2, KryderPlusRateMultiplier2,
							Accounts3}};
				false ->
					{error, invalid_account_anchors}
			end;
		true ->
			update_accounts2(B, PrevB, Accounts2, Args)
	end.

%% @doc Perform the last stage of block validation. The majority of the checks
%% are made in ar_block_pre_validator.erl, ar_nonce_limiter.erl, and
%% ar_node_utils:update_accounts/3.
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
			update_account(Addr, Amount, <<>>, Denomination, true, Accounts);
		{Balance, LastTX} ->
			Balance2 = ar_pricing:redenominate(Balance, 1, Denomination),
			update_account(Addr, Balance2 + Amount, LastTX, Denomination, true, Accounts);
		{Balance, LastTX, AccountDenomination, MiningPermission} ->
			Balance2 = ar_pricing:redenominate(Balance, AccountDenomination, Denomination),
			update_account(Addr, Balance2 + Amount, LastTX, Denomination, MiningPermission,
					Accounts)
	end.

update_account(Addr, Balance, LastTX, 1, true, Accounts) ->
	maps:put(Addr, {Balance, LastTX}, Accounts);
update_account(Addr, Balance, LastTX, Denomination, MiningPermission, Accounts) ->
	maps:put(Addr, {Balance, LastTX, Denomination, MiningPermission}, Accounts).

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
			update_account(Addr, Balance2 - Spent, ID, Denomination, true, Accounts);
		{Balance, _LastTX, AccountDenomination, MiningPermission} ->
			Balance2 = ar_pricing:redenominate(Balance, AccountDenomination, Denomination),
			Spent = ar_pricing:redenominate(Qty + Reward, TXDenomination, Denomination),
			update_account(Addr, Balance2 - Spent, ID, Denomination, MiningPermission,
					Accounts);
		_ ->
			Accounts
	end.

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
			update_account(To, Qty2, <<>>, Denomination, true, Accounts);
		{Balance, LastTX} ->
			Qty2 = ar_pricing:redenominate(Qty, TXDenomination, Denomination),
			Balance2 = ar_pricing:redenominate(Balance, 1, Denomination),
			update_account(To, Balance2 + Qty2, LastTX, Denomination, true, Accounts);
		{Balance, LastTX, AccountDenomination, MiningPermission} ->
			Qty2 = ar_pricing:redenominate(Qty, TXDenomination, Denomination),
			Balance2 = ar_pricing:redenominate(Balance, AccountDenomination, Denomination),
			update_account(To, Balance2 + Qty2, LastTX, Denomination, MiningPermission,
					Accounts)
	end.

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

validate_account_anchors(Accounts, TXs) ->
	not lists:any(fun(TX) -> is_wallet_invalid(TX, Accounts) end, TXs).

update_accounts2(B, PrevB, Accounts, Args) ->
	case is_account_banned(B#block.reward_addr, Accounts) of
		true ->
			{error, mining_address_banned};
		false ->
			update_accounts3(B, PrevB, Accounts, Args)
	end.

is_account_banned(Addr, Accounts) ->
	case maps:get(Addr, Accounts, not_found) of
		not_found ->
			false;
		{_, _} ->
			false;
		{_, _, _, MiningPermission} ->
			not MiningPermission
	end.

update_accounts3(B, PrevB, Accounts, Args) ->
	RewardHistory = lists:sublist(PrevB#block.reward_history, ?REWARD_HISTORY_BLOCKS),
	case may_be_apply_double_signing_proof(B, Accounts, PrevB#block.denomination,
			RewardHistory) of
		{ok, Accounts2} ->
			update_accounts4(B, PrevB, Accounts2, Args, RewardHistory);
		Error ->
			Error
	end.

may_be_apply_double_signing_proof(#block{ double_signing_proof = undefined }, Accounts,
		_Denomination, _RewardHistory) ->
	{ok, Accounts};
may_be_apply_double_signing_proof(#block{
		double_signing_proof = {_Pub, Sig, _, _, _, Sig, _, _, _} }, _Accounts,
		_Denomination, _RewardHistory) ->
	{error, invalid_double_signing_proof_same_signature};
may_be_apply_double_signing_proof(B, Accounts, Denomination, RewardHistory) ->
	{_Pub, _Signature1, CDiff1, PrevCDiff1, _Preimage1, _Signature2, CDiff2, PrevCDiff2,
			_Preimage2} = B#block.double_signing_proof,
	case CDiff1 == CDiff2 orelse (CDiff1 > PrevCDiff2 andalso CDiff2 > PrevCDiff1) of
		false ->
			{error, invalid_double_signing_proof_cdiff};
		true ->
			may_be_apply_double_signing_proof2(B, Accounts, Denomination, RewardHistory)
	end.

may_be_apply_double_signing_proof2(B, Accounts, Denomination, RewardHistory) ->
	{Pub, _Signature1, _CDiff1, _PrevCDiff1, _Preimage1, _Signature2, _CDiff2, _PrevCDiff2,
			_Preimage2} = B#block.double_signing_proof,
	Key = {?DEFAULT_KEY_TYPE, Pub},
	case B#block.reward_key == Key of
		true ->
			{error, invalid_double_signing_proof_same_address};
		false ->
			Addr = ar_wallet:to_address(Key),
			case is_account_banned(Addr, Accounts) of
				true ->
					{error, invalid_double_signing_proof_already_banned};
				false ->
					case is_in_reward_history(Addr, RewardHistory) of
						false ->
							{error, invalid_double_signing_proof_not_in_reward_history};
						true ->
							may_be_apply_double_signing_proof3(B, Accounts, Denomination)
					end
			end
	end.

is_in_reward_history(_Addr, []) ->
	false;
is_in_reward_history(Addr, [{Addr, _, _, _} | _]) ->
	true;
is_in_reward_history(Addr, [_ | RewardHistory]) ->
	is_in_reward_history(Addr, RewardHistory).

may_be_apply_double_signing_proof3(B, Accounts, Denomination) ->
	{Pub, Signature1, CDiff1, PrevCDiff1, Preimage1, Signature2, CDiff2, PrevCDiff2,
			Preimage2} = B#block.double_signing_proof,
	EncodedCDiff1 = ar_serialize:encode_int(CDiff1, 16),
	EncodedPrevCDiff1 = ar_serialize:encode_int(PrevCDiff1, 16),
	SignaturePreimage1 = << EncodedCDiff1/binary, EncodedPrevCDiff1/binary,
			Preimage1/binary >>,
	Key = {?DEFAULT_KEY_TYPE, Pub},
	Addr = ar_wallet:to_address(Key),
	case ar_wallet:verify(Key, SignaturePreimage1, Signature1) of
		false ->
			{error, invalid_double_signing_proof_invalid_signature};
		true ->
			EncodedCDiff2 = ar_serialize:encode_int(CDiff2, 16),
			EncodedPrevCDiff2 = ar_serialize:encode_int(PrevCDiff2, 16),
			SignaturePreimage2 = << EncodedCDiff2/binary,
					EncodedPrevCDiff2/binary, Preimage2/binary >>,
			case ar_wallet:verify(Key, SignaturePreimage2, Signature2) of
				false ->
					{error, invalid_double_signing_proof_invalid_signature};
				true ->
					?LOG_INFO([{event, banning_account},
							{address, ar_util:encode(Addr)},
							{previous_block, ar_util:encode(B#block.previous_block)},
							{height, B#block.height}]),
					{ok, ban_account(Addr, Accounts, Denomination)}
			end
	end.

ban_account(Addr, Accounts, Denomination) ->
	case maps:get(Addr, Accounts, not_found) of
		not_found ->
			maps:put(Addr, {1, <<>>, Denomination, false}, Accounts);
		{Balance, LastTX} ->
			Balance2 = ar_pricing:redenominate(Balance, 1, Denomination),
			maps:put(Addr, {Balance2 + 1, LastTX, Denomination, false}, Accounts);
		{Balance, LastTX, AccountDenomination, _MiningPermission} ->
			Balance2 = ar_pricing:redenominate(Balance, AccountDenomination, Denomination),
			maps:put(Addr, {Balance2 + 1, LastTX, Denomination, false}, Accounts)
	end.

update_accounts4(B, PrevB, Accounts, Args, RewardHistory) ->
	Denomination = PrevB#block.denomination,
	case length(RewardHistory) >= ?REWARD_HISTORY_BLOCKS of
		false ->
			update_accounts5(B, PrevB, Accounts, Args, RewardHistory);
		true ->
			{Addr, _HashRate, Reward, RewardDenomination} = lists:nth(?REWARD_HISTORY_BLOCKS,
					RewardHistory),
			case is_account_banned(Addr, Accounts) of
				true ->
					update_accounts5(B, PrevB, Accounts, Args, RewardHistory);
				false ->
					Reward2 = ar_pricing:redenominate(Reward, RewardDenomination,
							Denomination),
					Accounts2 = apply_mining_reward(Accounts, Addr, Reward2, Denomination),
					update_accounts5(B, PrevB, Accounts2, Args, RewardHistory)
			end
	end.

update_accounts5(B, PrevB, Accounts, Args, RewardHistory) ->
	{MinerReward, EndowmentPool, DebtSupply, KryderPlusRateMultiplierLatch,
			KryderPlusRateMultiplier} = Args,
	case B#block.double_signing_proof of
		undefined ->
			update_accounts6(B, Accounts, Args);
		Proof ->
			Denomination = PrevB#block.denomination,
			BannedAddr = ar_wallet:to_address({?DEFAULT_KEY_TYPE, element(1, Proof)}),
			{Sum, SumDenomination} = get_reward_sum(BannedAddr, RewardHistory),
			Sum2 = ar_pricing:redenominate(Sum, SumDenomination, Denomination) - 1,
			{Dividend, Divisor} = ?DOUBLE_SIGNING_PROVER_REWARD_SHARE,
			Sample = lists:sublist(RewardHistory, ?DOUBLE_SIGNING_REWARD_SAMPLE_SIZE),
			{Min, MinDenomination} = get_minimal_reward(Sample),
			Min2 = ar_pricing:redenominate(Min, MinDenomination, Denomination),
			ProverReward = min(Min2 * Dividend div Divisor, Sum2),
			{MinerReward, EndowmentPool, DebtSupply, KryderPlusRateMultiplierLatch,
					KryderPlusRateMultiplier} = Args,
			EndowmentPool2 = EndowmentPool + Sum2 - ProverReward,
			Accounts2 = apply_mining_reward(Accounts, B#block.reward_addr, ProverReward,
					Denomination),
			Args2 = {MinerReward, EndowmentPool2, DebtSupply, KryderPlusRateMultiplierLatch,
					KryderPlusRateMultiplier},
			update_accounts6(B, Accounts2, Args2)
	end.

get_minimal_reward(RewardHistory) ->
	get_minimal_reward(
			%% Make sure to traverse in the order of not decreasing denomination.
			lists:reverse(RewardHistory), infinity, 1).

get_minimal_reward([], Min, Denomination) ->
	{Min, Denomination};
get_minimal_reward([{_Addr, _HashRate, Reward, RewardDenomination} | RewardHistory], Min,
		Denomination) ->
	Min2 =
		case Min of
			infinity ->
				infinity;
			_ ->
				ar_pricing:redenominate(Min, Denomination, RewardDenomination)
		end,
	case Reward < Min2 of
		true ->
			get_minimal_reward(RewardHistory, Reward, RewardDenomination);
		false ->
			get_minimal_reward(RewardHistory, Min, Denomination)
	end.

get_reward_sum(Addr, RewardHistory) ->
	get_reward_sum(Addr,
			%% Make sure to traverse in the order of not decreasing denomination.
			lists:reverse(RewardHistory), 0, 1).

get_reward_sum(_Addr, [], Sum, Denomination) ->
	{Sum, Denomination};
get_reward_sum(Addr, [{Addr, _HashRate, Reward, RewardDenomination} | RewardHistory],
		Sum, Denomination) ->
	Sum2 = ar_pricing:redenominate(Sum, Denomination, RewardDenomination),
	Sum3 = Sum2 + Reward,
	get_reward_sum(Addr, RewardHistory, Sum3, RewardDenomination);
get_reward_sum(Addr, [_ | RewardHistory], Sum, Denomination) ->
	get_reward_sum(Addr, RewardHistory, Sum, Denomination).

update_accounts6(B, Accounts, Args) ->
	{MinerReward, EndowmentPool, DebtSupply, KryderPlusRateMultiplierLatch,
			KryderPlusRateMultiplier} = Args,
	case validate_account_anchors(Accounts, B#block.txs) of
		true ->
			{ok, {EndowmentPool, MinerReward, DebtSupply, KryderPlusRateMultiplierLatch,
					KryderPlusRateMultiplier, Accounts}};
		false ->
			{error, invalid_account_anchors}
	end.

do_validate(NewB, OldB, Wallets, BlockAnchors, RecentTXMap, PartitionUpperBound) ->
	validate_block(weave_size, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap,
			PartitionUpperBound}).

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
		error ->
			error;
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
					validate_block(reward_history_hash, {NewB, OldB, Wallets, BlockAnchors,
							RecentTXMap});
				_ ->
					{invalid, invalid_denomination}
			end;
		false ->
			validate_block(txs, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
	end;

validate_block(reward_history_hash, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	#block{ diff = Diff, reward = Reward, reward_history_hash = RewardHistoryHash,
			denomination = Denomination } = NewB,
	#block{ reward_history = RewardHistory } = OldB,
	HashRate = ar_difficulty:get_hash_rate(Diff),
	RewardAddr = NewB#block.reward_addr,
	RewardHistory2 = lists:sublist([{RewardAddr, HashRate, Reward, Denomination}
			| RewardHistory], ?REWARD_HISTORY_BLOCKS),
	case ar_block:reward_history_hash(RewardHistory2) of
		RewardHistoryHash ->
			validate_block(price_per_gib_minute, {NewB, OldB, Wallets, BlockAnchors,
					RecentTXMap});
		_ ->
			{invalid, invalid_reward_history_hash}
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
		OldB, Accounts, BlockAnchors, RecentTXMap}) ->
	Rate = ar_pricing:usd_to_ar_rate(OldB),
	PricePerGiBMinute = OldB#block.price_per_gib_minute,
	KryderPlusRateMultiplier = OldB#block.kryder_plus_rate_multiplier,
	Denomination = OldB#block.denomination,
	RedenominationHeight = OldB#block.redenomination_height,
	Args = {TXs, Rate, PricePerGiBMinute, KryderPlusRateMultiplier, Denomination, Height - 1,
			RedenominationHeight, Timestamp, Accounts, BlockAnchors, RecentTXMap},
	case ar_tx_replay_pool:verify_block_txs(Args) of
		invalid ->
			{invalid, invalid_txs};
		valid ->
			case Height >= ar_fork:height_2_6() of
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
		{Balance, LastTX, _Denomination, _MiningPermission} when Balance >= 0 ->
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
		{Balance, LastTX, _Denomination, _MiningPermission} when Balance >= 0 ->
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
	PrevTX = ar_test_node:sign_tx(master, Wallet, #{ reward => ?AR(10),
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
	TX = ar_test_node:sign_tx(master, Wallet, #{ reward => ?AR(10),
			data => crypto:strong_rand_bytes(7 * 1024 * 1024), last_tx => PrevH }),
	ar_test_node:assert_post_tx_to_master(TX),
	ar_node:mine(),
	[{H, _, _} | _] = ar_test_node:wait_until_height(3),
	B = ar_node:get_block_shadow_from_cache(H),
	Wallets = #{ ar_wallet:to_address(Pub) => {?AR(200), <<>>} },
	?assertEqual(valid, validate(B, PrevB, Wallets, BlockAnchors, RecentTXMap,
			PartitionUpperBound)),
	?assertMatch({ok, _}, update_accounts(B, PrevB, Wallets)),
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
	?assertEqual({error, invalid_account_anchors},
			update_accounts(B#block{ txs = [TX#tx{ reward = ?AR(201) }] }, PrevB, Wallets)),
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

update_accounts_rejects_same_signature_in_double_signing_proof_test_() ->
	{timeout, 10, fun test_update_accounts_rejects_same_signature_in_double_signing_proof/0}.

test_update_accounts_rejects_same_signature_in_double_signing_proof() ->
	Accounts = #{},
	Key = ar_wallet:new(),
	Pub = element(2, element(2, Key)),
	Random = crypto:strong_rand_bytes(64),
	Preimage = << (ar_serialize:encode_int(1, 16))/binary,
			(ar_serialize:encode_int(1, 16))/binary, Random/binary >>,
	Sig1 = ar_wallet:sign(element(1, Key), Preimage),
	DoubleSigningProof = {Pub, Sig1, 1, 1, Random, Sig1, 1, 1, Random},
	BannedAddr = ar_wallet:to_address(Key),
	ProverKey = ar_wallet:new(),
	RewardAddr = ar_wallet:to_address(ProverKey),
	B = #block{ timestamp = os:system_time(second), reward_addr = RewardAddr, weave_size = 1,
			double_signing_proof = DoubleSigningProof },
	Reward = 12,
	PrevB = #block{ reward_history = [{RewardAddr, 0, Reward, 1}, {BannedAddr, 0, 10, 1}],
			usd_to_ar_rate = {1, 5}, reward_pool = 0 },
	?assertEqual({error, invalid_double_signing_proof_same_signature},
			update_accounts(B, PrevB, Accounts)).

update_accounts_receives_released_reward_and_prover_reward_test_() ->
	{timeout, 10, fun test_update_accounts_receives_released_reward_and_prover_reward/0}.

test_update_accounts_receives_released_reward_and_prover_reward() ->
	?assert(?DOUBLE_SIGNING_REWARD_SAMPLE_SIZE == 2),
	?assert(?REWARD_HISTORY_BLOCKS == 3),
	?assert(?DOUBLE_SIGNING_PROVER_REWARD_SHARE == {1, 2}),
	Accounts = #{},
	Key = ar_wallet:new(),
	Pub = element(2, element(2, Key)),
	Random = crypto:strong_rand_bytes(64),
	Preimage = << (ar_serialize:encode_int(1, 16))/binary,
			(ar_serialize:encode_int(1, 16))/binary, Random/binary >>,
	Sig1 = ar_wallet:sign(element(1, Key), Preimage),
	Sig2 = ar_wallet:sign(element(1, Key), Preimage),
	DoubleSigningProof = {Pub, Sig1, 1, 1, Random, Sig2, 1, 1, Random},
	BannedAddr = ar_wallet:to_address(Key),
	ProverKey = ar_wallet:new(),
	RewardAddr = ar_wallet:to_address(ProverKey),
	B = #block{ timestamp = os:system_time(second), reward_addr = RewardAddr, weave_size = 1,
			double_signing_proof = DoubleSigningProof },
	Reward = 13,
	ProverReward = 5, % 1/2 of min(10, 12)
	PrevB = #block{ reward_history = [{stub, stub, 12, 1}, {BannedAddr, 0, 10, 1},
			{RewardAddr, 0, Reward, 1}], usd_to_ar_rate = {1, 5}, reward_pool = 0 },
	{ok, {_EndowmentPool2, _MinerReward, _DebtSupply2,
			_KryderPlusRateMultiplierLatch2, _KryderPlusRateMultiplier2, Accounts2}} =
			update_accounts(B, PrevB, Accounts),
	?assertEqual({ProverReward + Reward, <<>>}, maps:get(RewardAddr, Accounts2)),
	?assertEqual({1, <<>>, 1, false}, maps:get(BannedAddr, Accounts2)).

update_accounts_does_not_let_banned_account_take_reward_test_() ->
	{timeout, 10, fun test_update_accounts_does_not_let_banned_account_take_reward/0}.

test_update_accounts_does_not_let_banned_account_take_reward() ->
	?assert(?DOUBLE_SIGNING_REWARD_SAMPLE_SIZE == 2),
	?assert(?REWARD_HISTORY_BLOCKS == 3),
	?assert(?DOUBLE_SIGNING_PROVER_REWARD_SHARE == {1, 2}),
	Accounts = #{},
	Key = ar_wallet:new(),
	Pub = element(2, element(2, Key)),
	Random = crypto:strong_rand_bytes(64),
	Preimage = << (ar_serialize:encode_int(1, 16))/binary,
			(ar_serialize:encode_int(1, 16))/binary, Random/binary >>,
	Sig1 = ar_wallet:sign(element(1, Key), Preimage),
	Sig2 = ar_wallet:sign(element(1, Key), Preimage),
	DoubleSigningProof = {Pub, Sig1, 1, 1, Random, Sig2, 1, 1, Random},
	BannedAddr = ar_wallet:to_address(Key),
	ProverKey = ar_wallet:new(),
	RewardAddr = ar_wallet:to_address(ProverKey),
	B = #block{ timestamp = os:system_time(second), reward_addr = RewardAddr, weave_size = 1,
			double_signing_proof = DoubleSigningProof },
	Reward = 12,
	ProverReward = 3, % 1/2 of min(7, 8)
	PrevB = #block{ reward_history = [{stub, stub, 7, 1}, {stub, stub, 8, 1},
			{BannedAddr, 0, Reward, 1}, {BannedAddr, 0, 10, 1}],
					usd_to_ar_rate = {1, 5}, reward_pool = 0 },
	{ok, {_EndowmentPool2, _MinerReward, _DebtSupply2,
			_KryderPlusRateMultiplierLatch2, _KryderPlusRateMultiplier2, Accounts2}} =
			update_accounts(B, PrevB, Accounts),
	?assertEqual({ProverReward, <<>>}, maps:get(RewardAddr, Accounts2)),
	?assertEqual({1, <<>>, 1, false}, maps:get(BannedAddr, Accounts2)).
