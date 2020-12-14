%%% @doc Different utility functions for node and node worker.
-module(ar_node_utils).

-export([
	find_recall_block/1,
	find_block/1,
	calculate_reward/2,
	calculate_reward_pool/8,
	apply_mining_reward/4,
	apply_tx/3,
	apply_txs/3,
	update_wallets/4,
	validate/5,
	calculate_delay/1
]).

-include("ar.hrl").
-include("ar_data_sync.hrl").
-include("perpetual_storage.hrl").

%%%
%%% Public API.
%%%

%% @doc Search a block index for the next recall block.
find_recall_block([{Hash, _, _}]) ->
	ar_storage:read_block(Hash);
find_recall_block(BI) ->
	Block = ar_storage:read_block(element(1, hd(BI))),
	RecallHash = ar_util:get_recall_hash(Block, BI),
	ar_storage:read_block(RecallHash).

%% @doc Find a block from an ordered block list.
find_block(Hash) when is_binary(Hash) ->
	ar_storage:read_block(Hash).

calculate_reward_pool(
		OldPool,
		TXs,
		RewardAddr,
		POA,
		WeaveSize,
		Height,
		Diff,
		Timestamp) ->
	case ar_fork:height_1_8() of
		H when Height >= H ->
			calculate_reward_pool_perpetual(
				OldPool, TXs, RewardAddr, POA, WeaveSize, Height, Diff, Timestamp);
		_ ->
			Proportion = calculate_proportion(POA, WeaveSize, Height),
			calculate_reward_pool_original(OldPool, TXs, RewardAddr, Proportion)
	end.

%% @doc Split current reward pool into {FinderReward, NewPool}.
calculate_reward_pool_perpetual(OldPool, TXs, unclaimed, _, _, _, _, _) ->
	NewPool = OldPool + lists:sum([TX#tx.reward || TX <- TXs]),
	{0, NewPool};
calculate_reward_pool_perpetual(OldPool, TXs, _, POA, WeaveSize, Height, Diff, Timestamp) ->
	Inflation = erlang:trunc(ar_inflation:calculate(Height)),
	{TXsCost, TXsReward} = lists:foldl(
		fun(TX, {TXCostAcc, TXRewardAcc}) ->
			TXFee = TX#tx.reward,
			TXReward = erlang:trunc((?MINING_REWARD_MULTIPLIER) * TXFee / ((?MINING_REWARD_MULTIPLIER) + 1)),
			{TXCostAcc + TXFee - TXReward, TXRewardAcc + TXReward}
		end,
		{0, 0},
		TXs
	),
	BaseReward = Inflation + TXsReward,
	CostPerGBPerBlock = case ar_fork:height_1_9() of
		H when Height >= H ->
			ar_tx_perpetual_storage:usd_to_ar(
				ar_tx_perpetual_storage:get_cost_per_block_at_timestamp(Timestamp),
				Diff,
				Height
			);
		_ ->
			ar_tx_perpetual_storage:usd_to_ar(
				ar_tx_perpetual_storage:perpetual_cost_at_timestamp_pre_fork_1_9(Timestamp),
				Diff,
				Height
			)
	end,
	Burden = erlang:trunc(WeaveSize * CostPerGBPerBlock / (1024 * 1024 * 1024)),
	AR = Burden - BaseReward,
	NewPool = OldPool + TXsCost,
	case Height >= ar_fork:height_2_0() of
		true ->
			case AR =< 0 of
				true ->
					{BaseReward, NewPool};
				false ->
					Take = min(NewPool, AR),
					{BaseReward + Take, NewPool - Take}
			end;
		false ->
			case AR =< 0 of
				true  -> % BaseReward >= Burden
					{BaseReward, NewPool};
				false -> % Burden > BaseReward
					X = erlang:trunc(AR * max(1, POA#block.block_size) * Height / WeaveSize),
					Take = min(NewPool, X),
					{BaseReward + Take, NewPool - Take}
			end
	end.

%% @doc Calculate the reward.
calculate_reward_pool_original(OldPool, TXs, unclaimed, _Proportion) ->
	Pool = OldPool + lists:sum(
		lists:map(
			fun calculate_tx_reward/1,
			TXs
		)
	),
	{0, Pool};
calculate_reward_pool_original(OldPool, TXs, _RewardAddr, Proportion) ->
	Pool = OldPool + lists:sum(
		lists:map(
			fun calculate_tx_reward/1,
			TXs
		)
	),
	FinderReward = erlang:trunc(Pool * Proportion),
	{FinderReward, Pool - FinderReward}.

%% @doc Calculates the portion of the rewardpool that the miner is entitled
%% to for mining a block with a given recall. The proportion is based on the
%% size of the recall block and the average data stored within the weave.
calculate_proportion(RecallSize, WeaveSize, Height) when (Height == 0)->
	% Genesis block.
	calculate_proportion(
		RecallSize,
		WeaveSize,
		1
	);
calculate_proportion(RecallB, WeaveSize, Height) when is_record(RecallB, block) ->
	calculate_proportion(RecallB#block.block_size, WeaveSize, Height);
calculate_proportion(RecallSize, WeaveSize, Height) when (WeaveSize == 0)->
	% No data stored in the weave.
	calculate_proportion(
		RecallSize,
		1,
		Height
	);
calculate_proportion(RecallSize, WeaveSize, Height) when RecallSize >= (WeaveSize/Height) ->
	% Recall size is larger than the average data stored per block.
	XRaw = ((Height * RecallSize) / WeaveSize) - 1,
	X = min(XRaw, 1023),
	max(
		0.1,
		(math:pow(2, X) / (math:pow(2, X) + 2))
	);
calculate_proportion(RecallSize, WeaveSize, Height) when RecallSize == 0 ->
	% Recall block has no data txs, hence size of zero.
	calculate_proportion(
		1,
		WeaveSize,
		Height
	);
calculate_proportion(RecallSize, WeaveSize, Height) ->
	% Standard recall block, 0 < Recall size < Average block.
	XRaw = -(((Height * WeaveSize) / RecallSize) -1),
	X = min(XRaw, 1023),
	max(
		0.1,
		(math:pow(2, X)/(math:pow(2, X) + 2))
	).

%% @doc Calculate and apply mining reward quantities to a wallet list.
apply_mining_reward(WalletList, unclaimed, _Quantity, _Height) ->
	WalletList;
apply_mining_reward(WalletList, RewardAddr, Quantity, Height) ->
	alter_wallet(WalletList, RewardAddr, calculate_reward(Height, Quantity)).

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

%% @doc Validate a new block, given the previous block, the block index, the wallets of
%% the source and destination addresses and the reward wallet from the previous block,
%% and the mapping between block hashes and transaction identifiers of the recent blocks.
validate(BI, NewB, B, Wallets, BlockTXPairs) ->
	ar:info(
		[
			{event, validating_block},
			{hash, ar_util:encode(NewB#block.indep_hash)}
		]
	),
	case timer:tc(
		fun() ->
			do_validate(BI, NewB, B, Wallets, BlockTXPairs)
		end
	) of
		{TimeTaken, valid} ->
			ar:info(
				[
					{event, block_validation_successful},
					{hash, ar_util:encode(NewB#block.indep_hash)},
					{time_taken_us, TimeTaken}
				]
			),
			valid;
		{TimeTaken, {invalid, Reason}} ->
			ar:info(
				[
					{event, block_validation_failed},
					{reason, Reason},
					{hash, ar_util:encode(NewB#block.indep_hash)},
					{time_taken_us, TimeTaken}
				]
			),
			{invalid, Reason}
	end.

do_validate(BI, NewB, OldB, Wallets, BlockTXPairs) ->
	validate_block(height, {BI, NewB, OldB, Wallets, BlockTXPairs}).

validate_block(height, {BI, NewB, OldB, Wallets, BlockTXPairs}) ->
	case ar_block:verify_height(NewB, OldB) of
		false ->
			{invalid, invalid_height};
		true ->
			validate_block(previous_block, {BI, NewB, OldB, Wallets, BlockTXPairs})
	end;
validate_block(previous_block, {BI, NewB, OldB, Wallets, BlockTXPairs}) ->
	case ar_block:verify_previous_block(NewB, OldB) of
		false ->
			{invalid, invalid_previous_block};
		true ->
			validate_block(poa, {BI, NewB, OldB, Wallets, BlockTXPairs})
	end;
validate_block(poa, {BI, NewB = #block{ poa = POA }, OldB, Wallets, BlockTXPairs}) ->
	case ar_poa:validate(OldB#block.indep_hash, OldB#block.weave_size, BI, POA) of
		false ->
			{invalid, invalid_poa};
		true ->
			validate_block(difficulty, {BI, NewB, OldB, Wallets, BlockTXPairs})
	end;
validate_block(difficulty, {BI, NewB, OldB, Wallets, BlockTXPairs}) ->
	case ar_retarget:validate_difficulty(NewB, OldB) of
		false ->
			{invalid, invalid_difficulty};
		true ->
			validate_block(pow, {BI, NewB, OldB, Wallets, BlockTXPairs})
	end;
validate_block(
	pow,
	{
		BI,
		NewB = #block{ nonce = Nonce, height = Height, diff = Diff, poa = POA },
		OldB,
		Wallets,
		BlockTXPairs
	}
) ->
	POW = ar_weave:hash(
		ar_block:generate_block_data_segment(NewB),
		Nonce,
		Height
	),
	case ar_mine:validate(POW, ar_poa:modify_diff(Diff, POA#poa.option, Height), Height) of
		false ->
			{invalid, invalid_pow};
		true ->
			case ar_block:verify_dep_hash(NewB, POW) of
				false ->
					{invalid, invalid_pow_hash};
				true ->
					validate_block(independent_hash, {BI,  NewB, OldB, Wallets, BlockTXPairs})
			end
	end;
validate_block(independent_hash, {BI, NewB, OldB, Wallets, BlockTXPairs}) ->
	case ar_weave:indep_hash_post_fork_2_0(NewB) == NewB#block.indep_hash of
		false ->
			{invalid, invalid_independent_hash};
		true ->
			validate_block(wallet_list, {BI, NewB, OldB, Wallets, BlockTXPairs})
	end;
validate_block(wallet_list, {BI, #block{ txs = TXs } = NewB, OldB, Wallets, BlockTXPairs}) ->
	RewardPool = OldB#block.reward_pool,
	Height = OldB#block.height,
	{_, UpdatedWallets} = update_wallets(NewB, Wallets, RewardPool, Height),
	case lists:any(fun(TX) -> is_wallet_invalid(TX, UpdatedWallets) end, TXs) of
		true ->
			{invalid, invalid_wallet_list};
		false ->
			validate_block(block_field_sizes, {BI, NewB, OldB, Wallets, BlockTXPairs})
	end;
validate_block(block_field_sizes, {BI, NewB, OldB, Wallets, BlockTXPairs}) ->
	case ar_block:block_field_size_limit(NewB) of
		false ->
			{invalid, invalid_field_size};
		true ->
			validate_block(txs, {BI, NewB, OldB, Wallets, BlockTXPairs})
	end;
validate_block(
	txs,
	{
		BI,
		NewB = #block{ timestamp = Timestamp, height = Height, diff = Diff, txs = TXs },
		OldB,
		Wallets,
		BlockTXPairs
	}
) ->
	case ar_tx_replay_pool:verify_block_txs(
		TXs, Diff, Height - 1, Timestamp, Wallets, BlockTXPairs
	) of
		invalid ->
			{invalid, invalid_txs};
		valid ->
			validate_block(tx_root, {BI, NewB, OldB})
	end;
validate_block(tx_root, {BI, NewB, OldB}) ->
	case ar_block:verify_tx_root(NewB) of
		false ->
			{invalid, invalid_tx_root};
		true ->
			validate_block(weave_size, {BI, NewB, OldB})
	end;
validate_block(weave_size, {BI, #block{ txs = TXs } = NewB, OldB}) ->
	case ar_block:verify_weave_size(NewB, OldB, TXs) of
		false ->
			{invalid, invalid_weave_size};
		true ->
			validate_block(block_index_root, {BI, NewB, OldB})
	end;
validate_block(block_index_root, {BI, NewB, OldB}) ->
	case ar_block:verify_block_hash_list_merkle(NewB, OldB, BI) of
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
			valid
	end.

%%%
%%% Private functions.
%%%

do_apply_tx(
		Wallets,
		TX = #tx {
			last_tx = Last,
			owner = From
		},
		Height) ->
	Addr = ar_wallet:to_address(From),
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
			quantity = Qty,
			reward = Reward
		}) ->
	Addr = ar_wallet:to_address(From),
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

%% @doc Alter a wallet in a wallet list.
alter_wallet(Wallets, Target, Adjustment) ->
	case maps:get(Target, Wallets, not_found) of
		not_found ->
			maps:put(Target, {Adjustment, <<>>}, Wallets);
		{Balance, LastTX} ->
			maps:put(Target, {Balance + Adjustment, LastTX}, Wallets)
	end.

-ifdef(DEBUG).
is_wallet_invalid(#tx{ signature = <<>> }, _Wallets) ->
	false;
is_wallet_invalid(#tx{ owner = Owner }, Wallets) ->
	Address = ar_wallet:to_address(Owner),
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
is_wallet_invalid(#tx{ owner = Owner }, Wallets) ->
	Address = ar_wallet:to_address(Owner),
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

%% @doc Update the wallets by applying the new transactions and the mining reward.
%% Return the new reward pool and wallets. It is sufficient to provide the source
%% and the destination wallets of the transactions and the reward wallet.
update_wallets(NewB, Wallets, RewardPool, Height) ->
	TXs = NewB#block.txs,
	{FinderReward, NewRewardPool} =
		ar_node_utils:calculate_reward_pool( % NOTE: the exported function is used here
			RewardPool,						 % because it is mocked in
			TXs,							 % ar_tx_perpetual_storage_tests
			NewB#block.reward_addr,
			no_recall,
			NewB#block.weave_size,
			NewB#block.height,
			NewB#block.diff,
			NewB#block.timestamp
		),
	UpdatedWallets =
		ar_node_utils:apply_mining_reward(
			ar_node_utils:apply_txs(Wallets, TXs, Height),
			NewB#block.reward_addr,
			FinderReward,
			NewB#block.height
		),
	{NewRewardPool, UpdatedWallets}.

%% @doc Calculate the total mining reward for a block and its associated TXs.
calculate_reward(Height, Quantity) ->
	case ar_fork:height_1_8() of
		H when Height >= H ->
			Quantity;
		_ ->
			erlang:trunc(ar_inflation:calculate(Height) + Quantity)
	end.

%% @doc Given a TX, calculate an appropriate reward.
calculate_tx_reward(#tx { reward = Reward }) ->
	% TDOD mue: Calculation is not calculated, only returned.
	Reward.

-ifdef(FIXED_DELAY).
calculate_delay(_Bytes) ->
	?FIXED_DELAY.
-else.
%% Returns a delay in milliseconds to wait before including a transaction into a block.
%% The delay is computed as base delay + a function of data size with a conservative
%% estimation of the network speed.
calculate_delay(Bytes) ->
	BaseDelay = (?BASE_TX_PROPAGATION_DELAY) * 1000,
	NetworkDelay = Bytes * 8 div (?TX_PROPAGATION_BITS_PER_SECOND) * 1000,
	BaseDelay + NetworkDelay.
-endif.
