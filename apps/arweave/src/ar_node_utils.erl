%%%
%%% @doc Different utility functions for node and node worker.
%%%

-module(ar_node_utils).

-export([get_full_block/3]).
-export([find_recall_block/1, find_block/1]).
-export([calculate_reward/2]).
-export([calculate_reward_pool/8]).
-export([apply_mining_reward/4, apply_tx/3, apply_txs/3]).
-export([start_mining/1, reset_miner/1]).
-export([integrate_new_block/3]).
-export([fork_recover/3]).
-export([validate/5, validate/8, validate_wallet_list/1]).
-export([calculate_delay/1]).
-export([update_block_txs_pairs/3]).
-export([log_invalid_txs_drop_reason/1]).
-export([get_wallet_by_address/2, wallet_map_from_wallet_list/1]).

-include("ar.hrl").
-include("perpetual_storage.hrl").

%%%
%%% Public API.
%%%

%% @doc Get a full block (a block containing all transactions) by the independent hash.
%%      Try to find the block locally first. If we do not have the full block on disk, try to download it from peers.
get_full_block(Peers, ID, BI) when is_list(Peers) ->
	GetBlockFromPeersFun = fun() ->
		get_full_block_from_remote_peers(ar_util:unique(Peers), ID, BI)
	end,
	case ar_storage:read_block(ID, BI) of
		unavailable ->
			GetBlockFromPeersFun();
		Block ->
			case make_full_block(Block) of
				{error, unavailable} ->
					GetBlockFromPeersFun();
				{error, {txs_missing, MissingTXIDs}} ->
					ar:info([
						{transactions_missing_on_disk_for_block, ar_util:encode(ID)},
						{missing_txs, lists:map(fun ar_util:encode/1, MissingTXIDs)}
					]),
					GetBlockFromPeersFun();
				{ok, FinalB} ->
					FinalB
			end
	end;
get_full_block(Pid, ID, BI) when is_pid(Pid) ->
	%% Attempt to get block from local storage and add transactions.
	case make_full_block(ID, BI) of
		{ok, B} ->
			B;
		{error, _} ->
			unavailable
	end;
get_full_block(Peer, ID, BI) ->
	%% Handle external peer request.
	case ar_http_iface_client:get_full_block([Peer], ID, BI) of
		{_Peer, B} ->
			B;
		Error ->
			Error
	end.

%% @doc Attempt to get a full block from a HTTP peer, picking the node to query
%% randomly until the block is retreived.
get_full_block_from_remote_peers([], _ID, _BI) ->
	unavailable;
get_full_block_from_remote_peers(Peers, ID, BI) ->
	{Time, MaybeB} = timer:tc(fun() -> ar_http_iface_client:get_full_block(Peers, ID, BI) end),
	case MaybeB of
		{Peer, B} when ?IS_BLOCK(B) ->
			case ar_meta_db:get(http_logging) of
				true ->
					ar:info(
						[
							{downloaded_block, ar_util:encode(ID)},
							{peer, Peer},
							{time, Time}
						]
					);
				_ -> do_nothing
			end,
			B;
		_ ->
			unavailable
	end.

%% @doc Search a block list for the next recall block.
find_recall_block([B|_]) when ?IS_BLOCK(B) ->
	find_recall_block(B#block.block_index);
find_recall_block(BI = [{Hash, _}]) ->
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
	case Height >= ?FORK_2_0 of
		true ->
			RewardMultiplier = 1/POA#poa.option,
			PoolMultiplier = (1 - RewardMultiplier),
			case AR =< 0 of
				true ->
					{
						erlang:trunc(BaseReward * RewardMultiplier),
						NewPool + erlang:trunc(BaseReward * PoolMultiplier)
					};
				false ->
					{
						erlang:trunc((BaseReward + AR) * RewardMultiplier),
						(NewPool - AR) + erlang:trunc(BaseReward * PoolMultiplier)
					}
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
start_mining(StateIn) ->
	start_mining(StateIn, unforced).

start_mining(#{block_index := not_joined} = StateIn, _) ->
	% We don't have a block list. Wait until we have one before
	% starting to mine.
	StateIn;
start_mining(#{
		node := Node,
		block_index := BI,
		txs := TXs,
		reward_addr := RewardAddr,
		tags := Tags,
		block_txs_pairs := BlockTXPairs } = StateIn, ForceDiff)
		when length(BI) >= ?FORK_2_0 ->
	case ar_poa:generate(BI) of
		unavailable ->
			ar:info(
				[
					not_mining_this_block,
					{reason, data_unavailable_to_generate_poa},
					{generated_options_to_depth, ar_meta_db:get(max_option_depth)}
				]
			);
		POA ->
			ar_miner_log:started_hashing(),
			ar:info([{node_starting_to_mine, Node}]),
			B = ar_storage:read_block(element(1, hd(BI)), BI),
			case ForceDiff of
				unforced ->
					Miner = ar_mine:start(
						B,
						POA,
						TXs,
						RewardAddr,
						Tags,
						Node,
						BlockTXPairs
					),
					ar:info([{node, Node}, {started_miner, Miner}]),
					StateIn#{ miner => Miner };
				ForceDiff ->
					Miner = ar_mine:start(
						B,
						POA,
						TXs,
						RewardAddr,
						Tags,
						ForceDiff,
						Node,
						BlockTXPairs
					),
					ar:info([{node, Node}, {started_miner, Miner}, {forced_diff, ForceDiff}]),
					StateIn#{ miner => Miner, diff => ForceDiff }
			end
	end;
start_mining(#{
		node := Node,
		block_index := BI,
		txs := TXs,
		reward_addr := RewardAddr,
		tags := Tags,
		block_txs_pairs := BlockTXPairs } = StateIn, ForceDiff) ->
	% TODO REMOVE after 2.0
	case find_recall_block(BI) of
		unavailable ->
			B = ar_storage:read_block(element(1, hd(BI)), BI),
			RecallHash = ar_util:get_recall_hash(B, BI),
			% TODO: Cleanup.
			% FullBlock = get_encrypted_full_block(ar_bridge:get_remote_peers(whereis(http_bridge_node)), RecallHash),
			FullBlock = get_full_block(ar_bridge:get_remote_peers(whereis(http_bridge_node)), RecallHash, BI),
			case FullBlock of
				X when (X == unavailable) or (X == not_found) ->
					ar:info(
						[
							could_not_start_mining,
							could_not_retrieve_recall_block
						]
					);
				_ ->
					case ar_weave:verify_indep(FullBlock, BI) of
						true ->
							ar_storage:write_full_block(FullBlock),
							ar:info(
								[
									could_not_start_mining,
									stored_recall_block_for_foreign_verification
								]
							);
						false ->
							ar:info(
								[
									could_not_start_mining,
									{received_invalid_recall_block, FullBlock#block.indep_hash}
								]
							)
					end
			end,
			StateIn;
		RecallB ->
			case ?IS_BLOCK(RecallB) of
				false ->
					ar:report_console([{erroneous_recall_block, RecallB}]);
				true ->
					ar_miner_log:started_hashing(),
					ar:info([{node_starting_miner, Node}, {recall_block, RecallB#block.height}])
			end,
			case make_full_block(
				RecallB#block.indep_hash,
				BI
			) of
				{ok, RecallBFull} ->
					ar_key_db:put(
						RecallB#block.indep_hash,
						[
							{
								ar_block:generate_block_key(RecallBFull, element(1, hd(BI))),
								binary:part(element(1, hd(BI)), 0, 16)
							}
						]
					);
				{error, _} ->
					do_nothing
			end,
			B = ar_storage:read_block(element(1, hd(BI)), BI),
			case ForceDiff of
				unforced ->
					Miner = ar_mine:start(
						B,
						RecallB,
						TXs,
						RewardAddr,
						Tags,
						Node,
						BlockTXPairs
					),
					ar:info([{node, Node}, {started_miner, Miner}]),
					StateIn#{ miner => Miner };
				ForceDiff ->
					Miner = ar_mine:start(
						B,
						RecallB,
						TXs,
						RewardAddr,
						Tags,
						ForceDiff,
						Node,
						BlockTXPairs
					),
					ar:info([{node, Node}, {started_miner, Miner}, {forced_diff, ForceDiff}]),
					StateIn#{ miner => Miner, diff => ForceDiff }
			end
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
	NewBI = [ {NewB#block.indep_hash, NewB#block.weave_size} | BI],
	NewBlockTXPairs = update_block_txs_pairs(
		NewB#block.indep_hash,
		[TX#tx.id || TX <- BlockTXs],
		BlockTXPairs
	),
	{ValidTXs, InvalidTXs} = ar_tx_replay_pool:pick_txs_to_keep_in_mempool(
		NewBlockTXPairs,
		TXs -- BlockTXs,
		NewB#block.diff,
		NewB#block.height,
		NewB#block.wallet_list
	),
	log_invalid_txs_drop_reason(InvalidTXs),
	ar_miner_log:foreign_block(NewB#block.indep_hash),
	ar:report_console(
		[
			accepted_foreign_block,
			{indep_hash, ar_util:encode(NewB#block.indep_hash)},
			{header_hash, ar_util:encode(NewB#block.header_hash)},
			{height, NewB#block.height}
		]
	),
	case whereis(fork_recovery_server) of
		undefined -> do_nothing;
		PID ->
			PID ! {parent_accepted_block, NewB}
	end,
	ar_downloader:add_block(NewB),
	NewBI2 =
		case NewB#block.height of
			?FORK_2_0 ->
				io:format(
					"!!!CAUTION!!!~n"
					"Arweave is now transitioning to version 2.0. This may take a significant time.~n"
					"In the event of an error, please check the Arweave community's communication channels to liasse with your fellow miners.~n"),
				[{NewB#block.header_hash, NewB#block.weave_size}|ar_transition:generate_checkpoint(NewBI)];
			_ -> NewBI
		end,
	reset_miner(StateIn#{
		block_index     => NewBI2,
		current         => NewB#block.indep_hash,
		txs             => ValidTXs,
		height          => NewB#block.height,
		reward_pool     => NewB#block.reward_pool,
		diff            => NewB#block.diff,
		last_retarget   => NewB#block.last_retarget,
		weave_size      => NewB#block.weave_size,
		block_txs_pairs => NewBlockTXPairs
	}).

update_block_txs_pairs(BH, TXIDs, List) ->
	lists:sublist([{BH, TXIDs} | List], ?MAX_TX_ANCHOR_DEPTH).

log_invalid_txs_drop_reason(InvalidTXs) ->
	lists:foreach(
		fun({TXID, Reason}) ->
			case Reason of
				tx_already_in_weave ->
					noop;
				_ ->
					ar:info([
						{event, dropped_tx},
						{id, ar_util:encode(TXID)},
						{reason, Reason}
					])
			end
		end,
		InvalidTXs
	).

%% @doc Recovery from a fork.
fork_recover(#{ node := Node, block_index := BI, block_txs_pairs := BlockTXPairs } = StateIn, Peer, NewB) ->
	case {whereis(fork_recovery_server), whereis(join_server)} of
		{undefined, undefined} ->
			PrioritisedPeers = ar_util:unique(Peer) ++
				case whereis(http_bridge_node) of
					undefined -> [];
					BridgePID -> ar_bridge:get_remote_peers(BridgePID)
				end,
			erlang:monitor(
				process,
				PID = ar_fork_recovery:start(
					PrioritisedPeers,
					maps:get(trusted_peers, StateIn),
					NewB,
					BI,
					Node,
					BlockTXPairs
				)
			),
			case PID of
				undefined -> ok;
				_		  -> erlang:register(fork_recovery_server, PID)
			end;
		{undefined, _} ->
			ok;
		_ ->
			whereis(fork_recovery_server) ! {update_target_block, NewB, ar_util:unique(Peer)}
	end,
	% TODO: Check how an unchanged state has to be returned in
	% program flow.
	StateIn.

%% @doc Validate a block, given a node state and the dependencies.
validate(#{ block_index := BI, wallet_list := WalletList }, B, TXs, OldB, RecallB) ->
	validate(BI, WalletList, B, TXs, OldB, RecallB, B#block.reward_addr, B#block.tags).

%% @doc Validate a new block, given a server state, a claimed new block, the last block,
%% and the recall block.
validate(
		_BI,
		WalletList,
		NewB =
			#block {
				block_index = BI = [{LastHeaderHash, LastWeaveSize}|_],
				wallet_list = WalletList,
				nonce = Nonce,
				diff = Diff,
				timestamp = Timestamp,
				height = Height,
				poa = POA
			},
		TXs,
		OldB,
		_,
		RewardAddr,
		Tags
	) when Height >= ?FORK_2_0 ->
	ar:d([performing_v2_block_validation, {height, Height}]),
	POW = ar_weave:hash(
		ar_block:generate_block_data_segment(OldB, POA, TXs, RewardAddr, Timestamp, Tags),
		Nonce,
		Height
	),
	ar:d(
		[
			{validating_block, ar_util:encode(NewB#block.indep_hash)},
			{header_hash, ar_util:encode(NewB#block.header_hash)},
			{poa_block_header, ar_util:encode(ar_weave:header_hash((NewB#block.poa)#poa.recall_block))}
		]
	),
	{MicroSecs, Results} =
		timer:tc(
			fun() ->
				[
					{pow, ar_mine:validate(POW, Diff, Height)},
					{poa, ar_poa:validate(LastHeaderHash, LastWeaveSize, BI, POA)},
					{votables, ar_votable:validate(NewB, OldB)},
					{wallet_list, validate_wallet_list(WalletList)},
					{txs, ar_tx:verify_txs(NewB, TXs, Diff, Height - 1, OldB#block.wallet_list, Timestamp)},
					{tx_root, ar_block:verify_tx_root(NewB#block { txs = TXs })},
					{difficulty, ar_retarget:validate_difficulty(NewB, OldB)},
					{header_hash, ar_weave:header_hash(NewB) == NewB#block.header_hash},
					{dependent_hash, ar_block:verify_dep_hash(NewB, POW)},
					{weave_size, ar_block:verify_weave_size(NewB, OldB, TXs)},
					{block_field_sizes, ar_block:block_field_size_limit(NewB)},
					{height, ar_block:verify_height(NewB, OldB)},
					{last_retarget, ar_block:verify_last_retarget(NewB, OldB)},
					{previous_block, ar_block:verify_previous_block(NewB, OldB)},
					{block_index, ar_block:verify_block_index(NewB, OldB)},
					{block_index_root, ar_block:verify_block_index_merkle(NewB, OldB)},
					{wallet_list2, ar_block:verify_wallet_list(NewB, OldB, POA, TXs)},
					{cumulative_difficulty, ar_block:verify_cumulative_diff(NewB, OldB)}
				]
			end
		),
	FailedTests = [ TestName || {TestName, Result} <- Results, Result =/= true ],
	case FailedTests of
		[] ->
			ar:info(
				[
					{block_validation_successful, ar_util:encode(NewB#block.header_hash)},
					{time_taken, MicroSecs}
				]
			),
			valid;
		_ ->
			ar:info(
				[
					{block_validation_failed, ar_util:encode(NewB#block.header_hash)},
					{time_taken, MicroSecs}
				] ++ FailedTests
			),
			{invalid, FailedTests}
	end;
validate(_, _, NewB, _, _, _RecallB = unavailable, _, _) ->
	ar:info([{recall_block_unavailable, ar_util:encode(NewB#block.indep_hash)}]),
	{invalid, [recall_block_unavailable]};
validate(
		BI,
		WalletList,
		NewB =
			#block {
				block_index = BI,
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
	% TODO: Fix names.
	BDSHash = ar_weave:hash(
		ar_block:generate_block_data_segment(OldB, RecallB, TXs, RewardAddr, Timestamp, Tags),
		Nonce,
		Height
	),
	Mine = ar_mine:validate(BDSHash, Diff, Height),
	Wallet = validate_wallet_list(WalletList),
	IndepRecall = ar_weave:verify_indep(RecallB, BI),
	Txs = ar_tx:verify_txs(NewB, TXs, Diff, Height - 1, OldB#block.wallet_list, Timestamp),
	DiffCheck = ar_retarget:validate_difficulty(NewB, OldB),
	IndepHash = ar_block:verify_indep_hash(NewB),
	Hash = ar_block:verify_dep_hash(NewB, BDSHash),
	WeaveSize = ar_block:verify_weave_size(NewB, OldB, TXs),
	Size = ar_block:block_field_size_limit(NewB),
	%Time = ar_block:verify_timestamp(OldB, NewB),
	HeightCheck = ar_block:verify_height(NewB, OldB),
	RetargetCheck = ar_block:verify_last_retarget(NewB, OldB),
	PreviousBCheck = ar_block:verify_previous_block(NewB, OldB),
	BICheck = ar_block:verify_block_index(NewB, OldB),
	BIMerkleCheck = ar_block:verify_block_index_merkle(NewB, OldB),
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
			{block_block_index, BICheck},
			{block_wallet_list, WalletListCheck},
			{block_cumulative_diff, CumulativeDiffCheck},
			{block_index_merkle, BIMerkleCheck}
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
	case BICheck of false -> ar:info(invalid_block_index); _ -> ok end,
	case WalletListCheck of false -> ar:info(invalid_wallet_list_rewards); _ -> ok end,
	case CumulativeDiffCheck of false -> ar:info(invalid_cumulative_diff); _ -> ok end,
	case BIMerkleCheck of false -> ar:info(invalid_block_index_merkle); _ -> ok end,

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
		andalso BICheck
		andalso WalletListCheck
		andalso CumulativeDiffCheck
		andalso BIMerkleCheck),
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
validate(_BI, WL, NewB = #block { block_index = unset }, TXs, OldB, RecallB, _, _) ->
	validate(unset, WL, NewB, TXs, OldB, RecallB, unclaimed, []);
validate(BI, _WL, NewB = #block { wallet_list = undefined }, TXs,OldB, RecallB, _, _) ->
	validate(BI, undefined, NewB, TXs, OldB, RecallB, unclaimed, []);
validate(_BI, _WL, NewB, _TXs, _OldB, _RecallB, _, _) ->
	ar:info([{block_not_accepted, ar_util:encode(NewB#block.indep_hash)}]),
	{invalid, [block_index_or_wallet_list]}.

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

%% @doc Read a block shadow from disk, read its transactions from disk.
make_full_block(ID, BI) ->
	make_full_block(ar_storage:read_block(ID, BI)).

make_full_block(unavailable) ->
	{error, unavailable};
make_full_block(BShadow) ->
	{TXs, MissingTXIDs} = lists:foldr(
		fun(TXID, {TXs, MissingTXIDs}) ->
			case ar_storage:read_tx(TXID) of
				unavailable ->
					{TXs, [TXID | MissingTXIDs]};
				TX ->
					{[TX | TXs], MissingTXIDs}
			end
		end,
		{[], []},
		BShadow#block.txs
	),
	case MissingTXIDs of
		[] ->
			{ok, BShadow#block{ txs = TXs }};
		_ ->
			{error, {txs_missing, MissingTXIDs}}
	end.

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
-ifdef(DEBUG).
calculate_delay(_) -> 0.
-else.
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
-endif.