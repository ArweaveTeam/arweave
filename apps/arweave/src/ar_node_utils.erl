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
	start_mining/1,
	reset_miner/1,
	integrate_new_block/3,
	validate/5,
	calculate_delay/1,
	update_block_txs_pairs/3,
	update_block_index/2,
	tx_mempool_size/1,
	increase_mempool_size/2,
	calculate_mempool_size/1,
	drop_invalid_txs/1
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

%% @doc Force a node to start mining, update state.
start_mining(#{block_index := not_joined} = StateIn) ->
	%% We don't have a block index. Wait until we have one before
	%% starting to mine.
	StateIn;
start_mining(StateIn) ->
	#{
		node := Node,
		block_index := BI,
		txs := TXs,
		reward_addr := RewardAddr,
		tags := Tags,
		block_txs_pairs := BlockTXPairs,
		block_index := BI
	} = StateIn,
	case ar_poa:generate(BI) of
		unavailable ->
			ar:info(
				[
					{event, could_not_start_mining},
					{reason, data_unavailable_to_generate_poa},
					{generated_options_to_depth, ar_meta_db:get(max_poa_option_depth)}
				]
			),
			StateIn;
		POA ->
			ar_miner_log:started_hashing(),
			B = ar_storage:read_block(element(1, hd(BI))),
			Miner = ar_mine:start(
				B,
				POA,
				maps:fold(
					fun
						(_, {TX, ready_for_mining}, Acc) ->
							[TX | Acc];
						(_, _, Acc) ->
							Acc
					end,
					[],
					TXs
				),
				RewardAddr,
				Tags,
				Node,
				BlockTXPairs,
				BI
			),
			ar:info([{event, started_mining}]),
			StateIn#{ miner => Miner }
	end.

%% @doc Kill the old miner, optionally start a new miner, depending on the automine setting.
reset_miner(#{ miner := undefined, automine := false } = StateIn) ->
	StateIn;
reset_miner(#{ miner := undefined, automine := true } = StateIn) ->
	start_mining(StateIn);
reset_miner(#{ miner := Pid, automine := false } = StateIn) ->
	ar_mine:stop(Pid),
	StateIn#{ miner => undefined };
reset_miner(#{ miner := Pid, automine := true } = StateIn) ->
	ar_mine:stop(Pid),
	start_mining(StateIn#{ miner => undefined }).

%% @doc We have received a new valid block. Update the node state accordingly.
integrate_new_block(
		#{
			txs := TXs,
			block_index := BI,
			block_txs_pairs := BlockTXPairs,
			weave_size := WeaveSize,
			wallet_list := WalletList
		} = StateIn,
		NewB,
		BlockTXs) ->
	NewBI = update_block_index(NewB#block{ txs = BlockTXs }, BI),
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(BlockTXs),
	ar_data_sync:add_block(SizeTaggedTXs, lists:sublist(NewBI, ?TRACK_CONFIRMATIONS), WeaveSize),
	ar_storage:write_full_block(NewB, BlockTXs),
	BH = NewB#block.indep_hash,
	NewBlockTXPairs = update_block_txs_pairs(BH, SizeTaggedTXs, BlockTXPairs),
	{ValidTXs, InvalidTXs} = ar_tx_replay_pool:pick_txs_to_keep_in_mempool(
		NewBlockTXPairs,
		lists:foldl(
			fun(TX, Acc) ->
				maps:remove(TX#tx.id, Acc)
			end,
			TXs,
			BlockTXs
		),
		NewB#block.diff,
		NewB#block.height,
		ar_wallets:get(
			NewB#block.wallet_list,
			ar_tx:get_addresses([TX || {TX, _} <- maps:values(TXs)])
		)
	),
	drop_invalid_txs(InvalidTXs),
	ar_miner_log:foreign_block(NewB#block.indep_hash),
	ar:info(
		[
			{event, accepted_foreign_block},
			{indep_hash, ar_util:encode(NewB#block.indep_hash)},
			{height, NewB#block.height}
		]
	),
	case whereis(fork_recovery_server) of
		undefined -> do_nothing;
		PID ->
			PID ! {parent_accepted_block, NewB}
	end,
	lists:foreach(
		fun(TX) ->
			ar_downloader:enqueue_random({tx_data, TX}),
			ar_tx_queue:drop_tx(TX)
		end,
		BlockTXs
	),
	BH = element(1, hd(NewBI)),
	RewardAddr = NewB#block.reward_addr,
	ar_wallets:set_current(WalletList, NewB#block.wallet_list, RewardAddr, NewB#block.height),
	reset_miner(StateIn#{
		block_index      => NewBI,
		current          => BH,
		txs              => ValidTXs,
		height           => NewB#block.height,
		reward_pool      => NewB#block.reward_pool,
		diff             => NewB#block.diff,
		last_retarget    => NewB#block.last_retarget,
		weave_size       => NewB#block.weave_size,
		block_txs_pairs  => NewBlockTXPairs,
		mempool_size     => calculate_mempool_size(ValidTXs),
		wallet_list      => NewB#block.wallet_list
	}).

update_block_index(B, BI) ->
	maybe_report_n_confirmations(B, BI),
	NewBI = [{B#block.indep_hash, B#block.weave_size, B#block.tx_root} | BI],
	case B#block.height rem ?STORE_BLOCKS_BEHIND_CURRENT of
		0 ->
			spawn(fun() -> ar_storage:write_block_index(NewBI) end);
		_ ->
			do_nothing
	end,
	NewBI.

maybe_report_n_confirmations(B, BI) ->
	N = 10,
	LastNBlocks = lists:sublist(BI, N),
	case length(LastNBlocks) == N of
		true ->
			{H, _, _} = lists:last(LastNBlocks),
			ar_miner_log:block_received_n_confirmations(H, B#block.height - N);
		false ->
			do_nothing
	end.

update_block_txs_pairs(BH, SizeTaggedTXs, BlockTXPairs) ->
	lists:sublist([{BH, SizeTaggedTXs} | BlockTXPairs], 2 * ?MAX_TX_ANCHOR_DEPTH).

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
	case ar_mine:validate(POW, ar_poa:modify_diff(Diff, POA#poa.option), Height) of
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

tx_mempool_size(#tx{ format = 1, data = Data }) ->
	{?TX_SIZE_BASE + byte_size(Data), 0};
tx_mempool_size(#tx{ format = 2, data = Data }) ->
	{?TX_SIZE_BASE, byte_size(Data)}.

increase_mempool_size({MempoolHeaderSize, MempoolDataSize}, TX) ->
	{HeaderSize, DataSize} = tx_mempool_size(TX),
	{MempoolHeaderSize + HeaderSize, MempoolDataSize + DataSize}.

calculate_mempool_size(TXs) ->
	maps:fold(
		fun(_TXID, {TX, _}, {HeaderAcc, DataAcc}) ->
			{HeaderSize, DataSize} = tx_mempool_size(TX),
			{HeaderSize + HeaderAcc, DataSize + DataAcc}
		end,
		{0, 0},
		TXs
	).

drop_invalid_txs(TXs) ->
	lists:foreach(
		fun ({_, tx_already_in_weave}) ->
				ok;
			({TX, Reason}) ->
				ar:info([
					{event, dropped_tx},
					{id, ar_util:encode(TX#tx.id)},
					{reason, Reason}
				]),
				case TX#tx.format == 2 of
					true ->
						ar_data_sync:maybe_drop_data_root_from_disk_pool(
							TX#tx.data_root,
							TX#tx.data_size,
							TX#tx.id
						);
					false ->
						nothing_to_drop_from_disk_pool
				end
		end,
		TXs
	).
