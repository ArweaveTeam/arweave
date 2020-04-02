%%%
%%% @doc Different utility functions for node and node worker.
%%%

-module(ar_node_utils).

-export([find_recall_block/1, find_block/1]).
-export([calculate_reward/2]).
-export([calculate_reward_pool/8]).
-export([apply_mining_reward/4, apply_tx/3, apply_txs/3]).
-export([start_mining/1, reset_miner/1]).
-export([integrate_new_block/3]).
-export([validate/5, validate_wallet_list/1]).
-export([calculate_delay/1]).
-export([update_block_txs_pairs/2, update_block_index/2]).
-export([get_wallet_by_address/2, wallet_map_from_wallet_list/1]).
-export([tx_mempool_size/1, increase_mempool_size/2, calculate_mempool_size/1]).

%% NOT used. Exported for the historical record.
-export([validate_pre_fork_2_0/8]).

-include("ar.hrl").
-include("perpetual_storage.hrl").

%%%
%%% Public API.
%%%

%% @doc Search a block index for the next recall block.
find_recall_block(BI = [{Hash, _, _}]) ->
	ar_storage:read_block(Hash, BI);
find_recall_block(BI) ->
	Block = ar_storage:read_block(element(1, hd(BI)), BI),
	RecallHash = ar_util:get_recall_hash(Block, BI),
	ar_storage:read_block(RecallHash, BI).

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
apply_tx(Wallets, unavailable, _) ->
	Wallets;
apply_tx(Wallets, TX, Height) ->
	do_apply_tx(Wallets, TX, Height).

%% @doc Update a wallet list with a set of new transactions.
apply_txs(WalletList, TXs, Height) ->
	WalletMap = wallet_map_from_wallet_list(WalletList),
	NewWalletMap = lists:foldl(
		fun(TX, CurrWalletMap) ->
			apply_tx(CurrWalletMap, TX, Height)
		end,
		WalletMap,
		TXs
	),
	lists:sort(
		wallet_list_from_wallet_map(NewWalletMap)
	).

wallet_map_from_wallet_list(WalletList) ->
	lists:foldl(
		fun(Wallet = {Addr, _, _}, Map) ->
			maps:put(Addr, Wallet, Map)
		end,
		maps:new(),
		WalletList
	).

wallet_list_from_wallet_map(WalletMap) ->
	maps:fold(
		fun(_Addr, Wallet, List) ->
			[Wallet | List]
		end,
		[],
		WalletMap
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
			);
		POA ->
			ar_miner_log:started_hashing(),
			B = ar_storage:read_block(element(1, hd(BI)), BI),
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
			block_txs_pairs := BlockTXPairs
		} = StateIn,
		NewB,
		BlockTXs) ->
	%% Filter completed TXs from the pending list. The mining reward for TXs is
	%% supposed to be pessimistic (see the /price/[bytes] endpoint) by taking
	%% into account the difficulty may change 1 step before the TX is mined into
	%% a block. Therefore, we re-use the difficulty from NewB when verifying TXs
	%% for the next block because even if the next difficulty makes the price go
	%% up, it should be fine.
	%% Write new block and included TXs to local storage.
	ar_storage:write_full_block(NewB, BlockTXs),
	NewBI = update_block_index(NewB#block{ txs = BlockTXs }, BI),
	NewBlockTXPairs = update_block_txs_pairs(NewB, BlockTXPairs),
	ValidTXs = ar_tx_replay_pool:pick_txs_to_keep_in_mempool(
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
		NewB#block.wallet_list
	),
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
			ar_downloader:enqueue_random({tx_data, TX})
		end,
		BlockTXs
	),
	reset_miner(StateIn#{
		block_index      => NewBI,
		current          => element(1, hd(NewBI)),
		txs              => ValidTXs,
		height           => NewB#block.height,
		reward_pool      => NewB#block.reward_pool,
		diff             => NewB#block.diff,
		last_retarget    => NewB#block.last_retarget,
		weave_size       => NewB#block.weave_size,
		block_txs_pairs  => NewBlockTXPairs,
		mempool_size     => calculate_mempool_size(ValidTXs)
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

update_block_txs_pairs(B, BlockTXPairs) ->
	TXIDs = [case TX of Record when is_record(Record, tx) -> Record#tx.id; ID -> ID end || TX <- B#block.txs],
	lists:sublist([{B#block.indep_hash, TXIDs} | BlockTXPairs], 2 * ?MAX_TX_ANCHOR_DEPTH).

%% @doc Validate a new block, given a server state, a claimed new block and the last block.
validate(BI, WalletList, NewB, TXs, B) ->
	ar:info(
		[
			{event, validating_block},
			{hash, ar_util:encode(NewB#block.indep_hash)}
		]
	),
	case timer:tc(
		fun() ->
			do_validate(BI, WalletList, NewB, TXs, B)
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

do_validate(BI, WalletList, NewB = #block{ wallet_list = WalletList }, TXs, OldB) ->
	validate_block(height, {BI, WalletList, NewB, TXs, OldB});
do_validate(_BI, _WL, NewB, _TXs, _OldB) ->
	ar:info([
		{event, block_not_accepted},
		{hash, ar_util:encode(NewB#block.indep_hash)}
	]),
	{invalid, invalid_wallet_list}.

validate_block(height, {BI, WalletList, NewB, TXs, OldB}) ->
	case ar_block:verify_height(NewB, OldB) of
		false ->
			{invalid, invalid_height};
		true ->
			validate_block(previous_block, {BI, WalletList, NewB, TXs, OldB})
	end;
validate_block(previous_block, {BI, WalletList, NewB, TXs, OldB}) ->
	case ar_block:verify_previous_block(NewB, OldB) of
		false ->
			{invalid, invalid_previous_block};
		true ->
			validate_block(poa, {BI, WalletList, NewB, TXs, OldB})
	end;
validate_block(poa, {BI, WalletList, NewB = #block{ poa = POA }, TXs, OldB}) ->
	case ar_poa:validate(OldB#block.indep_hash, OldB#block.weave_size, BI, POA) of
		false ->
			{invalid, invalid_poa};
		true ->
			validate_block(difficulty, {BI, WalletList, NewB, TXs, OldB})
	end;
validate_block(difficulty, {BI, WalletList, NewB, TXs, OldB}) ->
	case ar_retarget:validate_difficulty(NewB, OldB) of
		false ->
			{invalid, invalid_difficulty};
		true ->
			validate_block(pow, {BI, WalletList, NewB, TXs, OldB})
	end;
validate_block(
	pow,
	{
		BI,
		WalletList,
		NewB = #block{ nonce = Nonce, height = Height, diff = Diff, poa = POA },
		TXs,
		OldB
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
					validate_block(independent_hash, {BI, WalletList, NewB, TXs, OldB})
			end
	end;
validate_block(independent_hash, {BI, WalletList, NewB, TXs, OldB}) ->
	case ar_weave:indep_hash_post_fork_2_0(NewB) == NewB#block.indep_hash of
		false ->
			{invalid, invalid_independent_hash};
		true ->
			validate_block(wallet_list, {BI, WalletList, NewB, TXs, OldB})
	end;
validate_block(wallet_list, {BI, WalletList, NewB = #block{ poa = POA }, TXs, OldB}) ->
	case validate_wallet_list(WalletList) of
		false ->
			{invalid, invalid_wallet_list};
		true ->
			case ar_block:verify_wallet_list(NewB, OldB, POA, TXs) of
				false ->
					{invalid, invalid_wallet_list};
				true ->
					validate_block(block_field_sizes, {BI, WalletList, NewB, TXs, OldB})
			end
	end;
validate_block(block_field_sizes, {BI, WalletList, NewB, TXs, OldB}) ->
	case ar_block:block_field_size_limit(NewB) of
		false ->
			{invalid, invalid_field_size};
		true ->
			validate_block(txs, {BI, WalletList, NewB, TXs, OldB})
	end;
validate_block(
	txs,
	{
		BI,
		WalletList,
		NewB = #block{ timestamp = Timestamp, height = Height, diff = Diff },
		TXs,
		OldB
	}
) ->
	case ar_tx:verify_txs(TXs, Diff, Height - 1, OldB#block.wallet_list, Timestamp) of
		false ->
			{invalid, invalid_txs};
		true ->
			validate_block(tx_root, {BI, WalletList, NewB, TXs, OldB})
	end;
validate_block(tx_root, {BI, WalletList, NewB, TXs, OldB}) ->
	case ar_block:verify_tx_root(NewB#block { txs = TXs }) of
		false ->
			{invalid, invalid_tx_root};
		true ->
			validate_block(weave_size, {BI, WalletList, NewB, TXs, OldB})
	end;
validate_block(weave_size, {BI, WalletList, NewB, TXs, OldB}) ->
	case ar_block:verify_weave_size(NewB, OldB, TXs) of
		false ->
			{invalid, invalid_weave_size};
		true ->
			validate_block(block_index_root, {BI, WalletList, NewB, TXs, OldB})
	end;
validate_block(block_index_root, {BI, WalletList, NewB, TXs, OldB}) ->
	case ar_block:verify_block_hash_list_merkle(NewB, OldB, BI) of
		false ->
			{invalid, invalid_block_index_root};
		true ->
			validate_block(last_retarget, {BI, WalletList, NewB, TXs, OldB})
	end;
validate_block(last_retarget, {_BI, _WalletList, NewB, _TXs, OldB}) ->
	case ar_block:verify_last_retarget(NewB, OldB) of
		false ->
			{invalid, invalid_last_retarget};
		true ->
			valid
	end.

validate_pre_fork_2_0(_, _, NewB, _, _, _RecallB = unavailable, _, _) ->
	ar:info([{recall_block_unavailable, ar_util:encode(NewB#block.indep_hash)}]),
	{invalid, [recall_block_unavailable]};
validate_pre_fork_2_0(
		BI,
		WalletList,
		NewB =
			#block {
				wallet_list = WalletList,
				nonce = Nonce,
				diff = Diff,
				timestamp = Timestamp,
				height = Height
			},
		TXs,
		OldB,
		RecallB,
		RewardAddr,
		Tags) ->
	ar:d([performing_v1_block_validation, {height, Height}]),
	BDSHash = ar_weave:hash(
		ar_block:generate_block_data_segment_pre_2_0(OldB, RecallB, TXs, RewardAddr, Timestamp, Tags),
		Nonce,
		Height
	),
	Mine = ar_mine:validate(BDSHash, Diff, Height),
	Wallet = validate_wallet_list(WalletList),
	IndepRecall = ar_weave:verify_indep(RecallB, BI),
	Txs = ar_tx:verify_txs(TXs, Diff, Height - 1, OldB#block.wallet_list, Timestamp),
	DiffCheck = ar_retarget:validate_difficulty(NewB, OldB),
	IndepHash = ar_block:verify_indep_hash(NewB),
	Hash = ar_block:verify_dep_hash(NewB, BDSHash),
	WeaveSize = ar_block:verify_weave_size(NewB, OldB, TXs),
	Size = ar_block:block_field_size_limit(NewB),
	HeightCheck = ar_block:verify_height(NewB, OldB),
	RetargetCheck = ar_block:verify_last_retarget(NewB, OldB),
	PreviousBCheck = ar_block:verify_previous_block(NewB, OldB),
	HLCheck = ar_block:verify_block_hash_list(NewB, OldB),
	HLMerkleCheck = ar_block:verify_block_hash_list_merkle(NewB, OldB, noop),
	WalletListCheck = ar_block:verify_wallet_list(NewB, OldB, RecallB, TXs),
	CumulativeDiffCheck = ar_block:verify_cumulative_diff(NewB, OldB),

	ar:info(
		[
			{block_validation_results, ar_util:encode(NewB#block.indep_hash)},
			{height, NewB#block.height},
			{block_mine_validate, Mine},
			{block_data_segment_hash, BDSHash},
			{block_wallet_validate, Wallet},
			{block_indep_validate, IndepRecall},
			{block_txs_validate, Txs},
			{block_diff_validate, DiffCheck},
			{block_indep, IndepHash},
			{block_hash, Hash},
			{weave_size, WeaveSize},
			{block_size, Size},
			{block_height, HeightCheck},
			{block_retarget_time, RetargetCheck},
			{block_previous_check, PreviousBCheck},
			{block_hash_list, HLCheck},
			{block_wallet_list, WalletListCheck},
			{block_cumulative_diff, CumulativeDiffCheck},
			{hash_list_merkle, HLMerkleCheck}
		]
	),

	case IndepRecall of
		false ->
			ar:info(
				[
					{encountered_invalid_recall_block, ar_util:encode(RecallB#block.indep_hash)},
					moving_to_invalid_block_directory
				]
			),
			ar_storage:invalidate_block(RecallB);
		_ ->
			ok
	end,

	case Mine of false -> ar:info({invalid_nonce, BDSHash}); _ -> ok end,
	case Wallet of false -> ar:info(invalid_wallet_list); _ -> ok end,
	case Txs of false -> ar:info(invalid_txs); _ -> ok end,
	case DiffCheck of false -> ar:info(invalid_difficulty); _ -> ok end,
	case IndepHash of false -> ar:info(invalid_indep_hash); _ -> ok end,
	case Hash of false -> ar:info(invalid_dependent_hash); _ -> ok end,
	case WeaveSize of false -> ar:info(invalid_total_weave_size); _ -> ok end,
	case Size of false -> ar:info(invalid_size); _ -> ok end,
	case HeightCheck of false -> ar:info(invalid_height); _ -> ok end,
	case RetargetCheck of false -> ar:info(invalid_retarget); _ -> ok end,
	case PreviousBCheck of false -> ar:info(invalid_previous_block); _ -> ok end,
	case HLCheck of false -> ar:info(invalid_hash_list); _ -> ok end,
	case WalletListCheck of false -> ar:info(invalid_wallet_list_rewards); _ -> ok end,
	case CumulativeDiffCheck of false -> ar:info(invalid_cumulative_diff); _ -> ok end,
	case HLMerkleCheck of false -> ar:info(invalid_hash_list_merkle); _ -> ok end,

	Valid = (Mine
		andalso Wallet
		andalso IndepRecall
		andalso Txs
		andalso DiffCheck
		andalso IndepHash
		andalso Hash
		andalso WeaveSize
		andalso Size
		andalso HeightCheck
		andalso RetargetCheck
		andalso PreviousBCheck
		andalso HLCheck
		andalso WalletListCheck
		andalso CumulativeDiffCheck
		andalso HLMerkleCheck),
	InvalidReasons = case Hash of
		true -> [];
		false -> [dep_hash]
	end,
	case Valid of
		true ->
			valid;
		false ->
			{invalid, InvalidReasons}
	end;
validate_pre_fork_2_0(_BI, WL, NewB = #block { hash_list = unset }, TXs, OldB, RecallB, _, _) ->
	validate_pre_fork_2_0(unset, WL, NewB, TXs, OldB, RecallB, unclaimed, []);
validate_pre_fork_2_0(BI, _WL, NewB = #block { wallet_list = undefined }, TXs, OldB, RecallB, _, _) ->
	validate_pre_fork_2_0(BI, undefined, NewB, TXs, OldB, RecallB, unclaimed, []);
validate_pre_fork_2_0(_BI, _WL, NewB, _TXs, _OldB, _RecallB, _, _) ->
	ar:info([{block_not_accepted, ar_util:encode(NewB#block.indep_hash)}]),
	{invalid, [hash_list_or_wallet_list]}.

%% @doc Ensure that all wallets in the wallet list have a positive balance.
validate_wallet_list([]) ->
	true;
validate_wallet_list([{_, 0, Last} | _]) when byte_size(Last) == 0 ->
	false;
validate_wallet_list([{_, Qty, _} | _]) when Qty < 0 ->
	false;
validate_wallet_list([_ | Rest]) ->
	validate_wallet_list(Rest).

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
	case {Height, get_wallet_by_address(Addr, Wallets)} of
		{H, {Addr, _, _}} when H >= Fork_1_8 ->
			do_apply_tx(Wallets, TX);
		{_, {Addr, _, Last}} ->
			do_apply_tx(Wallets, TX);
		_ ->
			Wallets
	end.

get_wallet_by_address(Addr, WalletList) when is_list(WalletList) ->
	lists:keyfind(Addr, 1, WalletList);
get_wallet_by_address(Addr, WalletMap) when is_map(WalletMap) ->
	maps:get(Addr, WalletMap, false).

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
	case get_wallet_by_address(Addr, Wallets) of
		{_, Balance, _} ->
			update_wallet(
				Addr,
				{Addr, Balance - (Qty + Reward), ID},
				Wallets
			);

		_ ->
			Wallets
	end.

update_wallet(Addr, Wallet, WalletList) when is_list(WalletList) ->
	lists:keyreplace(
		Addr,
		1,
		WalletList,
		Wallet
	);
update_wallet(Addr, Wallet, WalletMap) when is_map(WalletMap) ->
	maps:put(Addr, Wallet, WalletMap).

update_recipient_balance(Wallets, #tx { quantity = 0 }) ->
	Wallets;
update_recipient_balance(
		Wallets,
		#tx {
			target = To,
			quantity = Qty
		}) ->
	case get_wallet_by_address(To, Wallets) of
		false ->
			insert_wallet(To, {To, Qty, <<>>}, Wallets);
		{To, OldBalance, LastTX} ->
			update_wallet(To, {To, OldBalance + Qty, LastTX}, Wallets)
	end.

insert_wallet(_Addr, Wallet, WalletList) when is_list(WalletList) ->
	[Wallet | WalletList];
insert_wallet(Addr, Wallet, WalletMap) when is_map(WalletMap) ->
	maps:put(Addr, Wallet, WalletMap).

%% @doc Alter a wallet in a wallet list.
alter_wallet(WalletList, Target, Adjustment) ->
	case lists:keyfind(Target, 1, WalletList) of
		false ->
			[{Target, Adjustment, <<>>}|WalletList];
		{Target, Balance, LastTX} ->
			lists:keyreplace(
				Target,
				1,
				WalletList,
				{Target, Balance + Adjustment, LastTX}
			)
	end.

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
