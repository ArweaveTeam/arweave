%%% @doc Different utility functions for node and node worker.
-module(ar_node_utils).

-export([apply_tx/3, apply_txs/3, update_accounts/3, validate/6,
	h1_passes_diff_check/3, h2_passes_diff_check/3, solution_passes_diff_check/2,
	block_passes_diff_check/1, block_passes_diff_check/2, passes_diff_check/4,
	update_account/6, is_account_banned/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================


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
	BlockInterval = ar_block_time_history:compute_block_interval(PrevB),
	Args =
		get_miner_reward_and_endowment_pool({EndowmentPool, DebtSupply, TXs,
				B#block.reward_addr, B#block.weave_size, B#block.height, B#block.timestamp,
				Rate, PricePerGiBMinute, KryderPlusRateMultiplierLatch,
				KryderPlusRateMultiplier, Denomination, BlockInterval}),
	Accounts2 = apply_txs(Accounts, Denomination, TXs),
	true = B#block.height >= ar_fork:height_2_6(),
	update_accounts2(B, PrevB, Accounts2, Args).

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

h1_passes_diff_check(H1, DiffPair, PackingDifficulty) ->
	passes_diff_check(H1, true, DiffPair, PackingDifficulty).

h2_passes_diff_check(H2, DiffPair, PackingDifficulty) ->
	passes_diff_check(H2, false, DiffPair, PackingDifficulty).

solution_passes_diff_check(Solution, DiffPair) ->
	SolutionHash = Solution#mining_solution.solution_hash,
	PackingDifficulty = Solution#mining_solution.packing_difficulty,
	IsPoA1 = ar_mining_server:is_one_chunk_solution(Solution),
	passes_diff_check(SolutionHash, IsPoA1, DiffPair, PackingDifficulty).

block_passes_diff_check(Block) ->
	SolutionHash = Block#block.hash,
	block_passes_diff_check(SolutionHash, Block).

block_passes_diff_check(SolutionHash, Block) ->
	IsPoA1 = (Block#block.recall_byte2 == undefined),
	PackingDifficulty = Block#block.packing_difficulty,
	DiffPair = ar_difficulty:diff_pair(Block),
	passes_diff_check(SolutionHash, IsPoA1, DiffPair, PackingDifficulty).

passes_diff_check(_SolutionHash, _IsPoA1, not_set, _PackingDifficulty) ->
	false;
passes_diff_check(SolutionHash, IsPoA1, {PoA1Diff, Diff}, PackingDifficulty) ->
	Diff2 =
		case IsPoA1 of
			true ->
				PoA1Diff;
			false ->
				Diff
		end,
	Diff3 =
		case PackingDifficulty of
			0 ->
				Diff2;
			_ ->
				SubDiff = ar_difficulty:sub_diff(Diff2,
						?PACKING_DIFFICULTY_ONE_SUB_CHUNK_COUNT),
				ar_difficulty:scale_diff(SubDiff, {1, PackingDifficulty},
						%% The minimal difficulty height. It does not change at the
						%% packing difficulty fork.
						ar_fork:height_2_8())
		end,
	binary:decode_unsigned(SolutionHash) > Diff3.

update_account(Addr, Balance, LastTX, 1, true, Accounts) ->
	maps:put(Addr, {Balance, LastTX}, Accounts);
update_account(Addr, Balance, LastTX, Denomination, MiningPermission, Accounts) ->
	maps:put(Addr, {Balance, LastTX, Denomination, MiningPermission}, Accounts).

is_account_banned(Addr, Accounts) ->
	case maps:get(Addr, Accounts, not_found) of
		not_found ->
			false;
		{_, _} ->
			false;
		{_, _, _, MiningPermission} ->
			not MiningPermission
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

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
			Denomination, BlockInterval} = Args,
	true = Height >= ar_fork:height_2_4(),
	case ar_pricing_transition:is_v2_pricing_height(Height) of
		true ->
			ar_pricing:get_miner_reward_endowment_pool_debt_supply({EndowmentPool, DebtSupply,
					TXs, WeaveSize, Height, PricePerGiBMinute, KryderPlusRateMultiplierLatch,
					KryderPlusRateMultiplier, Denomination, BlockInterval});
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

update_accounts3(B, PrevB, Accounts, Args) ->
	case may_be_apply_double_signing_proof(B, PrevB, Accounts) of
		{ok, Accounts2} ->
			Accounts3 = ar_rewards:apply_rewards(PrevB, Accounts2),
			update_accounts4(B, PrevB, Accounts3, Args);
		Error ->
			Error
	end.

may_be_apply_double_signing_proof(#block{ double_signing_proof = undefined }, _PrevB, Accounts) ->
	{ok, Accounts};
may_be_apply_double_signing_proof(#block{
		double_signing_proof = {_Pub, Sig, _, _, _, Sig, _, _, _} }, _PrevB, _Accounts) ->
	{error, invalid_double_signing_proof_same_signature};
may_be_apply_double_signing_proof(B, PrevB, Accounts) ->
	{_Pub, _Signature1, CDiff1, PrevCDiff1, _Preimage1, _Signature2, CDiff2, PrevCDiff2,
			_Preimage2} = B#block.double_signing_proof,
	case CDiff1 == CDiff2 orelse (CDiff1 > PrevCDiff2 andalso CDiff2 > PrevCDiff1) of
		false ->
			{error, invalid_double_signing_proof_cdiff};
		true ->
			may_be_apply_double_signing_proof2(B, PrevB, Accounts)
	end.

may_be_apply_double_signing_proof2(B, PrevB, Accounts) ->
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
					LockedRewards = ar_rewards:get_locked_rewards(PrevB),
					case ar_rewards:has_locked_reward(Addr, LockedRewards) of
						false ->
							{error, invalid_double_signing_proof_not_in_reward_history};
						true ->
							may_be_apply_double_signing_proof3(B, PrevB, Accounts)
					end
			end
	end.

may_be_apply_double_signing_proof3(B, PrevB, Accounts) ->
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
					{ok, ban_account(Addr, Accounts, PrevB#block.denomination)}
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

update_accounts4(B, PrevB, Accounts, Args) ->
	{MinerReward, EndowmentPool, DebtSupply, KryderPlusRateMultiplierLatch,
			KryderPlusRateMultiplier} = Args,
	case B#block.double_signing_proof of
		undefined ->
			update_accounts5(B, Accounts, Args);
		Proof ->
			Denomination = PrevB#block.denomination,
			BannedAddr = ar_wallet:to_address({?DEFAULT_KEY_TYPE, element(1, Proof)}),
			Sum = ar_rewards:get_total_reward_for_address(BannedAddr, PrevB) - 1,
			{Dividend, Divisor} = ?DOUBLE_SIGNING_PROVER_REWARD_SHARE,
			LockedRewards = ar_rewards:get_locked_rewards(PrevB),
			Sample = lists:sublist(LockedRewards, ?DOUBLE_SIGNING_REWARD_SAMPLE_SIZE),
			{Min, MinDenomination} = get_minimal_reward(Sample),
			Min2 = ar_pricing:redenominate(Min, MinDenomination, Denomination),
			ProverReward = min(Min2 * Dividend div Divisor, Sum),
			{MinerReward, EndowmentPool, DebtSupply, KryderPlusRateMultiplierLatch,
					KryderPlusRateMultiplier} = Args,
			EndowmentPool2 = EndowmentPool + Sum - ProverReward,
			Accounts2 = ar_rewards:apply_reward(Accounts, B#block.reward_addr, ProverReward,
					Denomination),
			Args2 = {MinerReward, EndowmentPool2, DebtSupply, KryderPlusRateMultiplierLatch,
					KryderPlusRateMultiplier},
			update_accounts5(B, Accounts2, Args2)
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

update_accounts5(B, Accounts, Args) ->
	{MinerReward, EndowmentPool, DebtSupply, KryderPlusRateMultiplierLatch,
			KryderPlusRateMultiplier} = Args,
	case validate_account_anchors(Accounts, B#block.txs) of
		true ->
			Accounts2 = ar_testnet:top_up_test_wallet(Accounts, B#block.height),
			{ok, {EndowmentPool, MinerReward, DebtSupply, KryderPlusRateMultiplierLatch,
					KryderPlusRateMultiplier, Accounts2}};
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
	true = NewB#block.height >= ar_fork:height_2_6(),
	case NewB#block.previous_solution_hash == OldB#block.hash of
		false ->
			{invalid, invalid_previous_solution_hash};
		true ->
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
					{NewB, OldB, Wallets, BlockAnchors, RecentTXMap});
		false ->
			{error, invalid_packing_2_5_threshold}
	end;

validate_block(strict_data_split_threshold,
		{NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
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
			true = NewB#block.height >= ar_fork:height_2_6(),
			validate_block(usd_to_ar_rate, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap});
		false ->
			{error, invalid_strict_data_split_threshold}
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
	true = Height >= ar_fork:height_2_6(),
	case ar_pricing:may_be_redenominate(OldB) of
		{Denomination, RedenominationHeight} ->
			validate_block(reward_history_hash, {NewB, OldB, Wallets, BlockAnchors,
					RecentTXMap});
		_ ->
			{invalid, invalid_denomination}
	end;

validate_block(reward_history_hash, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	#block{ reward = Reward, reward_history_hash = RewardHistoryHash,
			denomination = Denomination, height = Height } = NewB,
	#block{ reward_history = RewardHistory } = OldB,
	HashRate = ar_difficulty:get_hash_rate_fixed_ratio(NewB),
	RewardAddr = NewB#block.reward_addr,
	LockedRewards = ar_rewards:trim_locked_rewards(Height,
		[{RewardAddr, HashRate, Reward, Denomination} | RewardHistory]),
	case ar_rewards:reward_history_hash(LockedRewards) of
		RewardHistoryHash ->
			validate_block(block_time_history_hash, {NewB, OldB, Wallets, BlockAnchors,
					RecentTXMap});
		_ ->
			{invalid, invalid_reward_history_hash}
	end;

validate_block(block_time_history_hash, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	case NewB#block.height >= ar_fork:height_2_7() of
		false ->
			validate_block(next_vdf_difficulty, {NewB, OldB, Wallets, BlockAnchors,
					RecentTXMap});
		true ->
			#block{ block_time_history_hash = HistoryHash } = NewB,
			History = ar_block_time_history:update_history(NewB, OldB),
			case ar_block_time_history:hash(History) of
				HistoryHash ->
					validate_block(next_vdf_difficulty, {NewB, OldB, Wallets, BlockAnchors,
							RecentTXMap});
				_ ->
					{invalid, invalid_block_time_history_hash}
			end
	end;

validate_block(next_vdf_difficulty, {NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	case NewB#block.height >= ar_fork:height_2_7() of
		false ->
			validate_block(price_per_gib_minute, {NewB, OldB, Wallets, BlockAnchors,
					RecentTXMap});
		true ->
			ExpectedNextVDFDifficulty = ar_block:compute_next_vdf_difficulty(OldB),
			#nonce_limiter_info{ next_vdf_difficulty = NextVDFDifficulty } = 
				NewB#block.nonce_limiter_info,
			case ExpectedNextVDFDifficulty == NextVDFDifficulty of
				false ->
					{invalid, invalid_next_vdf_difficulty};
				true ->
					validate_block(price_per_gib_minute, {NewB, OldB, Wallets, BlockAnchors,
							RecentTXMap})
			end
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
			true = Height >= ar_fork:height_2_6(),
			%% The field size limits in 2.6 are naturally asserted in
			%% ar_serialize:binary_to_block/1.
			validate_block(tx_root, {NewB, OldB})
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
			validate_block(merkle_rebase_support_threshold, {NewB, OldB})
	end;

validate_block(merkle_rebase_support_threshold, {NewB, OldB}) ->
	#block{ height = Height } = NewB,
	case Height > ar_fork:height_2_7() of
		true ->
			case NewB#block.merkle_rebase_support_threshold
					== OldB#block.merkle_rebase_support_threshold of
				false ->
					{error, invalid_merkle_rebase_support_threshold};
				true ->
					valid
			end;
		false ->
			case Height == ar_fork:height_2_7() of
				true ->
					case NewB#block.merkle_rebase_support_threshold
							== OldB#block.weave_size of
						true ->
							valid;
						false ->
							{error, invalid_merkle_rebase_support_threshold}
					end;
				false ->
					valid
			end
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

block_validation_test_() ->
	{timeout, 90, fun test_block_validation/0}.

test_block_validation() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(200), <<>>}]),
	ar_test_node:start(B0),
	%% Add at least 10 KiB of data to the weave and mine a block on top,
	%% to make sure SPoRA mining activates.
	PrevTX = ar_test_node:sign_tx(main, Wallet, #{ reward => ?AR(10),
			data => crypto:strong_rand_bytes(10 * 1024 * 1024) }),
	ar_test_node:assert_post_tx_to_peer(main, PrevTX),
	ar_test_node:mine(),
	[_ | _] = ar_test_node:wait_until_height(1),
	ar_test_node:mine(),
	[{PrevH, _, _} | _ ] = ar_test_node:wait_until_height(2),
	PrevB = ar_node:get_block_shadow_from_cache(PrevH),
	BI = ar_node:get_block_index(),
	PartitionUpperBound = ar_node:get_partition_upper_bound(BI),
	BlockAnchors = ar_node:get_block_anchors(),
	RecentTXMap = ar_node:get_recent_txs_map(),
	TX = ar_test_node:sign_tx(main, Wallet, #{ reward => ?AR(10),
			data => crypto:strong_rand_bytes(7 * 1024 * 1024), last_tx => PrevH }),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	ar_test_node:mine(),
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
	ar_test_node:mine(),
	[{H2, _, _} | _ ] = ar_test_node:wait_until_height(4),
	B2 = ar_node:get_block_shadow_from_cache(H2),
	?assertEqual(valid, validate(B2, B, Wallets, BlockAnchors2, RecentTXMap2,
			PartitionUpperBound2)).

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

% this function will prepend reward_history up to ?REWARD_HISTORY_BLOCKS
% elements will keep pattern of changed values
augment_reward_history(PrevB = #block{ reward_history = RewardHistory }) ->
	[First, Second | _] = RewardHistory,
	NewRewardHistory = case length(RewardHistory) >= ?REWARD_HISTORY_BLOCKS of
		true -> RewardHistory;
		_ ->
			PairsToAdd = (?REWARD_HISTORY_BLOCKS - length(RewardHistory)) div 2,
			AdditionalElements = lists:flatten(lists:duplicate(PairsToAdd, [First, Second])),
			case (?REWARD_HISTORY_BLOCKS - length(RewardHistory)) rem 2 of
				1 -> [Second | AdditionalElements];
				0 -> AdditionalElements
			end ++ RewardHistory
	end,
	PrevB#block{ reward_history = NewRewardHistory }.

test_update_accounts_receives_released_reward_and_prover_reward() ->
	?assert(?DOUBLE_SIGNING_REWARD_SAMPLE_SIZE == 2),
	?assert(?REWARD_HISTORY_BLOCKS >= 3),
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
	PrevB1 = augment_reward_history(PrevB),
	{ok, {_EndowmentPool2, _MinerReward, _DebtSupply2,
			_KryderPlusRateMultiplierLatch2, _KryderPlusRateMultiplier2, Accounts2}} =
			update_accounts(B, PrevB1, Accounts),
	?assertEqual({ProverReward + Reward, <<>>}, maps:get(RewardAddr, Accounts2)),
	?assertEqual({1, <<>>, 1, false}, maps:get(BannedAddr, Accounts2)).

update_accounts_does_not_let_banned_account_take_reward_test_() ->
	{timeout, 10, fun test_update_accounts_does_not_let_banned_account_take_reward/0}.

test_update_accounts_does_not_let_banned_account_take_reward() ->
	?assert(?DOUBLE_SIGNING_REWARD_SAMPLE_SIZE == 2),
	?assert(?REWARD_HISTORY_BLOCKS >= 3),
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
	PrevB1 = augment_reward_history(PrevB),
	{ok, {_EndowmentPool2, _MinerReward, _DebtSupply2,
			_KryderPlusRateMultiplierLatch2, _KryderPlusRateMultiplier2, Accounts2}} =
			update_accounts(B, PrevB1, Accounts),
	?assertEqual({ProverReward, <<>>}, maps:get(RewardAddr, Accounts2)),
	?assertEqual({1, <<>>, 1, false}, maps:get(BannedAddr, Accounts2)).
