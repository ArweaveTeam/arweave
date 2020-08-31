-module(ar_mine).

-export([
	start/8, stop/1,
	mine/2, mine_spora/2,
	validate/4, validate/3, validate_spora/8,
	min_difficulty/1, max_difficulty/0,
	min_spora_difficulty/1,
	sha384_diff_to_randomx_diff/1,
	spora_solution_hash/2,
	pick_recall_byte/4,
	get_search_space_upper_bound/2
]).

-include("ar.hrl").
-include("ar_mine.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	parent, % miners parent process (initiator)
	current_block, % current block held by node
	candidate_block = not_set, % the product of mining
	block_txs_pairs, % list of {BH, TXIDs} pairs for latest ?MAX_TX_ANCHOR_DEPTH blocks
	poa, % proof of access
	txs, % the set of txs to be mined
	timestamp, % the block timestamp used for the mining
	timestamp_refresh_timer, % reference for timer for updating the timestamp
	data_segment = <<>>, % the data segment generated for mining
	data_segment_duration, % duration in seconds of the last generation of the data segment
	reward_addr, % the mining reward address
	reward_wallet_before_mining_reward = not_in_the_list,
	tags, % the block tags
	diff, % the current network difficulty
	delay = 0, % hashing delay used for testing
	max_miners = ?NUM_MINING_PROCESSES, % max mining process to start
	miners = [], % miner worker processes
	bds_base = not_generated, % part of the block data segment not changed during mining
	total_hashes_tried = 0, % the number of tried hashes, used to estimate the hashrate
	started_at = not_set, % the timestamp when the mining begins, used to estimate the hashrate
	total_sporas_tried = 0, % the number of Succinct Proofs of Random Access checked during mining
	%% The number of Succinct Proofs of Random Access discovered but not neccessarily
	%% checked (because they are not present locally) during mining.
	total_sporas_discovered = 0,
	block_index = not_set % the latest block index
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Spawns a new mining process and returns its PID.
start(CurrentB, POA, TXs, RewardAddr, Tags, Parent, BlockTXPairs, BI) ->
	CurrentHeight = CurrentB#block.height,
	CandidateB = #block{
		height = CurrentHeight + 1,
		hash_list = ?BI_TO_BHL(lists:sublist(BI, ?STORE_BLOCKS_BEHIND_CURRENT)),
		previous_block = CurrentB#block.indep_hash,
		hash_list_merkle = ar_block:compute_hash_list_merkle(CurrentB, BI),
		reward_addr = RewardAddr,
		poa = POA,
		tags = Tags
	},
	start_server(
		#state {
			parent = Parent,
			current_block = CurrentB,
			poa = POA,
			data_segment_duration = 0,
			reward_addr = RewardAddr,
			tags = Tags,
			max_miners = ar_meta_db:get(max_miners),
			block_txs_pairs = BlockTXPairs,
			started_at = erlang:timestamp(),
			candidate_block = CandidateB,
			txs = TXs,
			block_index =
				case CurrentHeight + 1 >= ar_fork:height_2_3() of
					true ->
						BI;
					false ->
						not_set
				end
		}
	).

%% @doc Stop a running mining server.
stop(PID) ->
	PID ! stop.

%% @doc Validate that a given hash/nonce satisfy the difficulty requirement.
validate(BDS, Nonce, Diff, Height) ->
	BDSHash = ar_weave:hash(BDS, Nonce, Height),
	case validate(BDSHash, Diff, Height) of
		true ->
			{valid, BDSHash};
		false ->
			{invalid, BDSHash}
	end.

%% @doc Validate that a given block data segment hash satisfies the difficulty requirement.
validate(BDSHash, Diff, Height) ->
	case ar_fork:height_1_8() of
		H when Height >= H ->
			binary:decode_unsigned(BDSHash, big) > Diff;
		_ ->
			case BDSHash of
				<< 0:Diff, _/bitstring >> ->
					true;
				_ ->
					false
			end
	end.

%% @doc Validate Succinct Proof of Random Access.
validate_spora(BDS, Nonce, Height, Diff, PrevH, SearchSpaceUpperBound, SPoA, BI) ->
	RXHash = ar_weave:hash(BDS, Nonce, Height),
	case validate(RXHash, ?SPORA_SLOW_HASH_DIFF(Height), Height) of
		false ->
			false;
		true ->
			SolutionHash = spora_solution_hash(RXHash, SPoA),
			case validate(SolutionHash, Diff, Height) of
				false ->
					false;
				true ->
					case pick_recall_byte(RXHash, PrevH, SearchSpaceUpperBound, Height) of
						{error, weave_size_too_small} ->
							SPoA == #poa{};
						{ok, RecallByte} ->
							case ar_poa:validate(RecallByte, BI, SPoA) of
								false ->
									false;
								true ->
									{true, SolutionHash}
							end
					end
			end
	end.

%% @doc Maximum linear difficulty.
%% Assumes using 256 bit RandomX hashes.
max_difficulty() ->
	erlang:trunc(math:pow(2, 256)).

-ifdef(DEBUG).
min_difficulty(_Height) ->
	1.
-else.
min_difficulty(Height) ->
	Diff = case Height >= ar_fork:height_1_7() of
		true ->
			case Height >= ar_fork:height_2_3() of
				true ->
					min_spora_difficulty(Height);
				false ->
					min_randomx_difficulty()
			end;
		false ->
			min_sha384_difficulty()
	end,
	case Height >= ar_fork:height_1_8() of
		true ->
			ar_retarget:switch_to_linear_diff(Diff);
		false ->
			Diff
	end.
-endif.

sha384_diff_to_randomx_diff(Sha384Diff) ->
	max(Sha384Diff + ?RANDOMX_DIFF_ADJUSTMENT, min_randomx_difficulty()).

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Takes a state and a set of transactions and return a new state with the
%% new set of transactions.
update_txs(
	S = #state {
		current_block = CurrentB,
		data_segment_duration = BDSGenerationDuration,
		block_txs_pairs = BlockTXPairs,
		reward_addr = RewardAddr,
		poa = POA,
		candidate_block = #block{ height = Height } = CandidateB,
		txs = TXs
	}
) ->
	NextBlockTimestamp = next_block_timestamp(BDSGenerationDuration),
	NextDiff = calc_diff(CurrentB, NextBlockTimestamp),
	ValidTXs = ar_tx_replay_pool:pick_txs_to_mine(
		BlockTXPairs,
		CurrentB#block.height,
		NextDiff,
		NextBlockTimestamp,
		ar_wallets:get(CurrentB#block.wallet_list, ar_tx:get_addresses(TXs)),
		TXs
	),
	NewBlockSize =
		lists:foldl(
			fun(TX, Acc) ->
				Acc + TX#tx.data_size
			end,
			0,
			ValidTXs
		),
	NewWeaveSize = CurrentB#block.weave_size + NewBlockSize,
	{FinderReward, _RewardPool} =
		ar_node_utils:calculate_reward_pool(
			CurrentB#block.reward_pool,
			ValidTXs,
			RewardAddr,
			POA,
			NewWeaveSize,
			CandidateB#block.height,
			NextDiff,
			NextBlockTimestamp
		),
	Addresses = [RewardAddr | ar_tx:get_addresses(ValidTXs)],
	Wallets = ar_wallets:get(CurrentB#block.wallet_list, Addresses),
	AppliedTXsWallets = ar_node_utils:apply_txs(Wallets, ValidTXs, CurrentB#block.height),
	RewardWalletBeforeMiningReward =
		case maps:get(RewardAddr, AppliedTXsWallets, not_found) of
			not_found ->
				not_in_the_list;
			{Balance, LastTX} ->
				{RewardAddr, Balance, LastTX}
		end,
	UpdatedWallets =
		ar_node_utils:apply_mining_reward(
			AppliedTXsWallets,
			RewardAddr,
			FinderReward,
			CandidateB#block.height
		),
	{ok, UpdatedRootHash} =
		ar_wallets:add_wallets(
			CurrentB#block.wallet_list,
			UpdatedWallets,
			RewardAddr,
			Height
		),
	NewCandidateB = CandidateB#block{
		txs = [TX#tx.id || TX <- ValidTXs],
		tx_root = ar_block:generate_tx_root_for_block(ValidTXs),
		block_size = NewBlockSize,
		weave_size = NewWeaveSize,
		wallet_list = UpdatedRootHash
	},
	BDSBase = ar_block:generate_block_data_segment_base(NewCandidateB),
	update_data_segment(
		S#state{
			candidate_block = NewCandidateB,
			bds_base = BDSBase,
			reward_wallet_before_mining_reward = RewardWalletBeforeMiningReward,
			txs = ValidTXs
		},
		NextBlockTimestamp,
		NextDiff
	).

%% @doc Generate a new timestamp to be used in the next block. To compensate for
%% the time it takes to generate the block data segment, adjust the timestamp
%% with the same time it took to generate the block data segment the last time.
next_block_timestamp(BDSGenerationDuration) ->
	os:system_time(seconds) + BDSGenerationDuration.

%% @doc Given a block calculate the difficulty to mine on for the next block.
%% Difficulty is retargeted each ?RETARGET_BlOCKS blocks, specified in ar.hrl
%% This is done in attempt to maintain on average a fixed block time.
calc_diff(CurrentB, NextBlockTimestamp) ->
	ar_retarget:maybe_retarget(
		CurrentB#block.height + 1,
		CurrentB#block.diff,
		NextBlockTimestamp,
		CurrentB#block.last_retarget
	).

%% @doc Generate a new data segment and update the timestamp, diff, and possibly
%% the transaction data.
%%
%% Revalidate transaction prices if we are mining a retarget block.
%% If the block is not a retarget block, the difficulty does not change
%% so the requried transaction fees should only go down, so we can
%% optimistically skip additional validation. If the block is a retarget block,
%% validate transaction fees again and if some of the transactions are no longer
%% valid, regenerate a data segment with the new transactions.
update_txs_or_data_segment(
	S = #state{
		current_block = #block{ height = Height, wallet_list = RootHash },
		txs = TXs,
		diff = Diff,
		timestamp = Timestamp
	}
) ->
	case ar_retarget:is_retarget_height(Height + 1) of
		false ->
			update_data_segment(S);
		true ->
			Wallets = ar_wallets:get(RootHash, ar_tx:get_addresses(TXs)),
			case filter_by_valid_tx_fee(TXs, Diff, Height, Wallets, Timestamp) of
				TXs ->
					update_data_segment(S);
				ValidTXs ->
					update_txs(S#state{ txs = ValidTXs })
			end
	end.

%% @doc Generate a new data_segment and update the timestamp and diff.
update_data_segment(
	S = #state {
		data_segment_duration = BDSGenerationDuration,
		current_block = CurrentB
	}
) ->
	BlockTimestamp = next_block_timestamp(BDSGenerationDuration),
	Diff = calc_diff(CurrentB, BlockTimestamp),
	update_data_segment(S, BlockTimestamp, Diff).

update_data_segment(S, BlockTimestamp, Diff) ->
	#state{
		current_block = CurrentB,
		candidate_block = CandidateB,
		reward_addr = RewardAddr,
		poa = POA,
		bds_base = BDSBase,
		reward_wallet_before_mining_reward = RewardWalletBeforeMiningReward,
		txs = TXs
	} = S,
	Height = CandidateB#block.height,
	NewLastRetarget =
		case ar_retarget:is_retarget_height(Height) of
			true -> BlockTimestamp;
			false -> CurrentB#block.last_retarget
		end,
	{FinderReward, RewardPool} =
		ar_node_utils:calculate_reward_pool(
			CurrentB#block.reward_pool,
			TXs,
			RewardAddr,
			POA,
			CandidateB#block.weave_size,
			Height,
			Diff,
			BlockTimestamp
		),
	RewardWallet = case RewardWalletBeforeMiningReward of
		not_in_the_list ->
			#{};
		{Addr, Balance, LastTX} ->
			#{ Addr => {Balance, LastTX} }
	end,
	NewRewardWallet =
		case maps:get(
			RewardAddr,
			ar_node_utils:apply_mining_reward(RewardWallet, RewardAddr, FinderReward, Height),
			not_found
		) of
			not_found ->
				#{};
			WalletData ->
				#{ RewardAddr => WalletData }
		end,
	{ok, UpdatedRootHash} =
		ar_wallets:update_wallets(
			CandidateB#block.wallet_list,
			NewRewardWallet,
			RewardAddr,
			Height
		),
	CDiff = ar_difficulty:next_cumulative_diff(
		CurrentB#block.cumulative_diff,
		Diff,
		Height
	),
	{DurationMicros, NewBDS} = timer:tc(
		fun() ->
			ar_block:generate_block_data_segment(
				BDSBase,
				CandidateB#block.hash_list_merkle,
				#{
					timestamp => BlockTimestamp,
					last_retarget => NewLastRetarget,
					diff => Diff,
					cumulative_diff => CDiff,
					reward_pool => RewardPool,
					wallet_list => UpdatedRootHash
				}
			)
		end
	),
	NewCandidateB = CandidateB#block{
		timestamp = BlockTimestamp,
		last_retarget = NewLastRetarget,
		diff = Diff,
		cumulative_diff = CDiff,
		reward_pool = RewardPool,
		wallet_list = UpdatedRootHash
	},
	NewS = S#state {
		timestamp = BlockTimestamp,
		diff = Diff,
		data_segment = NewBDS,
		data_segment_duration = round(DurationMicros / 1000000),
		candidate_block = NewCandidateB
	},
	reschedule_timestamp_refresh(NewS).

reschedule_timestamp_refresh(S = #state{
	timestamp_refresh_timer = Timer,
	data_segment_duration = BDSGenerationDuration,
	txs = TXs
}) ->
	timer:cancel(Timer),
	case ?MINING_TIMESTAMP_REFRESH_INTERVAL - BDSGenerationDuration  of
		TimeoutSeconds when TimeoutSeconds =< 0 ->
			TXIDs = lists:map(fun(TX) -> TX#tx.id end, TXs),
			ar:warn([
				ar_mine,
				slow_data_segment_generation,
				{duration, BDSGenerationDuration},
				{timestamp_refresh_interval, ?MINING_TIMESTAMP_REFRESH_INTERVAL},
				{txs, lists:map(fun ar_util:encode/1, lists:sort(TXIDs))}
			]),
			self() ! refresh_timestamp,
			S#state{ timestamp_refresh_timer = no_timer };
		TimeoutSeconds ->
			case timer:send_after(TimeoutSeconds * 1000, refresh_timestamp) of
				{ok, Ref} ->
					S#state{ timestamp_refresh_timer = Ref };
				{error, Reason} ->
					ar:err("ar_mine: Reschedule timestamp refresh failed: ~p", [Reason]),
					S
			end
	end.

%% @doc Start the main mining server.
start_server(S) ->
	spawn(fun() ->
		server(start_miners(update_txs(S)))
	end).

%% @doc The main mining server.
server(
	S = #state {
		miners = Miners,
		current_block = #block{
			height = Height,
			wallet_list = RootHash
		},
		total_hashes_tried = TotalHashesTried,
		started_at = StartedAt,
		txs = MinedTXs,
		candidate_block = #block { diff = Diff, timestamp = Timestamp },
		total_sporas_tried = TotalSPoRAsTried,
		total_sporas_discovered = TotalSPoRAsDiscovered
	}
) ->
	receive
		%% Stop the mining process and all the workers.
		stop ->
			stop_miners(Miners),
			case Height + 1 >= ar_fork:height_2_3() of
				true ->
					log_spora_performance(TotalSPoRAsTried, TotalSPoRAsDiscovered, StartedAt);
				false ->
					log_performance(TotalHashesTried, StartedAt)
			end;
		%% The block timestamp must be reasonable fresh since it's going to be
		%% validated on the remote nodes when it's propagated to them. Only blocks
		%% with a timestamp close to current time will be accepted in the propagation.
		refresh_timestamp ->
			server(restart_miners(update_txs_or_data_segment(S)));
		%% Count the number of hashes tried by all workers.
		{hashes_tried, HashesTried} ->
			server(S#state { total_hashes_tried = TotalHashesTried + HashesTried });
		{solution, Hash, Nonce, Timestamp} ->
			Wallets = ar_wallets:get(RootHash, ar_tx:get_addresses(MinedTXs)),
			case filter_by_valid_tx_fee(MinedTXs, Diff, Height, Wallets, Timestamp) of
				MinedTXs ->
					process_solution(S, Hash, Nonce, MinedTXs, Diff, Timestamp);
				ValidTXs ->
					server(restart_miners(update_txs(S#state{ txs = ValidTXs })))
			end;
		{solution, _, _, _, _, _} ->
			%% A stale solution.
			server(S);
		{sporas_tried, SPoRAsTried} ->
			server(
				S#state{
					total_sporas_tried = TotalSPoRAsTried + SPoRAsTried,
					total_sporas_discovered = TotalSPoRAsDiscovered + SPoRAsTried
				}
			);
		{sporas_discovered, SPoRAsDiscovered} ->
			server(
				S#state{
					total_sporas_discovered = TotalSPoRAsDiscovered + SPoRAsDiscovered
				}
			);
		{spora_solution, Hash, Nonce, SPoA, Timestamp} ->
			Wallets = ar_wallets:get(RootHash, ar_tx:get_addresses(MinedTXs)),
			case filter_by_valid_tx_fee(MinedTXs, Diff, Height, Wallets, Timestamp) of
				MinedTXs ->
					process_spora_solution(S, Hash, Nonce, SPoA, MinedTXs, Diff, Timestamp);
				ValidTXs ->
					server(restart_miners(update_txs(S#state{ txs = ValidTXs })))
			end;
		{spora_solution, _, _, _, _} ->
			%% A stale solution.
			ok
	end.

-ifdef(DEBUG).
filter_by_valid_tx_fee(TXs, Diff, Height, Wallets, Timestamp) ->
	lists:filter(
		fun
			(#tx{ signature = <<>> }) ->
				true;
			(TX) ->
				ar_tx:tx_cost_above_min(TX, Diff, Height, Wallets, TX#tx.target, Timestamp)
		end,
		TXs
	).
-else.
filter_by_valid_tx_fee(TXs, Diff, Height, Wallets, Timestamp) ->
	lists:filter(
		fun(TX) ->
			ar_tx:tx_cost_above_min(TX, Diff, Height, Wallets, TX#tx.target, Timestamp)
		end,
		TXs
	).
-endif.

process_solution(S, Hash, Nonce, MinedTXs, Diff, Timestamp) ->
	#state {
		parent = Parent,
		miners = Miners,
		current_block = #block{ indep_hash = CurrentBH },
		poa = POA,
		total_hashes_tried = TotalHashesTried,
		started_at = StartedAt,
		data_segment = BDS,
		candidate_block = #block { diff = Diff, timestamp = Timestamp } = CandidateB
	} = S,
	NewBBeforeHash = CandidateB#block{
		nonce = Nonce,
		hash = Hash
	},
	IndepHash = ar_weave:indep_hash_post_fork_2_0(BDS, Hash, Nonce),
	NewB = NewBBeforeHash#block{ indep_hash = IndepHash },
	Parent ! {work_complete, CurrentBH, NewB, MinedTXs, BDS, POA, TotalHashesTried},
	log_performance(TotalHashesTried, StartedAt),
	stop_miners(Miners).

log_performance(TotalHashesTried, StartedAt) ->
	Time = timer:now_diff(erlang:timestamp(), StartedAt),
	Rate = TotalHashesTried / (Time / 1000000),
	prometheus_histogram:observe(mining_rate, Rate),
	ar:console([
		{event, stopped_mining},
		{miner_hashes_per_second, Rate}
	]).

process_spora_solution(S, Hash, Nonce, SPoA, MinedTXs, Diff, Timestamp) ->
	#state {
		parent = Parent,
		miners = Miners,
		current_block = #block{ indep_hash = CurrentBH },
		total_sporas_tried = TotalSPoRAsTried,
		total_sporas_discovered = TotalSPoRAsDiscovered,
		started_at = StartedAt,
		data_segment = BDS,
		bds_base = BDSBase,
		candidate_block = #block { diff = Diff, timestamp = Timestamp } = CandidateB
	} = S,
	NewBBeforeHash = CandidateB#block{
		nonce = Nonce,
		hash = Hash,
		poa = SPoA
	},
	IndepHash = ar_weave:indep_hash_post_fork_2_3(BDS, Hash, Nonce, SPoA),
	NewB = NewBBeforeHash#block{ indep_hash = IndepHash },
	Parent ! {work_complete, CurrentBH, NewB, MinedTXs, BDSBase, SPoA, TotalSPoRAsTried},
	log_spora_performance(TotalSPoRAsTried, TotalSPoRAsDiscovered, StartedAt),
	stop_miners(Miners).

log_spora_performance(TotalSPoRAsTried, TotalSPoRAsDiscovered, StartedAt) ->
	Time = timer:now_diff(erlang:timestamp(), StartedAt),
	Rate = TotalSPoRAsTried / (Time / 1000000),
	prometheus_histogram:observe(mining_rate, Rate),
	DiscoverRate = TotalSPoRAsDiscovered / (Time / 1000000),
	ar:console([
		{event, stopped_mining},
		{miner_sporas_per_second, Rate},
		{miner_discovered_sporas_per_second, DiscoverRate}
	]).

%% @doc Start the workers and return the new state.
start_miners(
	S = #state{
		max_miners = MaxMiners,
		candidate_block = #block{ height = Height, previous_block = PrevH },
		poa = POA,
		diff = Diff,
		block_index = BI,
		data_segment = BDS,
		timestamp = Timestamp
	}
) ->
	Miners =
		case Height >= ar_fork:height_2_3() of
			true ->
				SearchSpaceUpperBound = get_search_space_upper_bound(BI, Height),
				WorkerState = #{
					data_segment => BDS,
					diff => Diff,
					timestamp => Timestamp,
					height => Height,
					prev_h => PrevH,
					search_space_upper_bound => SearchSpaceUpperBound,
					stats => #{}
				},
				[spawn(?MODULE, mine_spora, [WorkerState, self()])];
			false ->
				ModifiedDiff = ar_poa:modify_diff(Diff, POA#poa.option),
				WorkerState = #{
					data_segment => BDS,
					diff => ModifiedDiff,
					timestamp => Timestamp,
					height => Height
				},
				[spawn(?MODULE, mine, [WorkerState, self()]) || _ <- lists:seq(1, MaxMiners)]
		end,
	S#state{ miners = Miners }.

%% @doc Stop all workers.
stop_miners(Miners) ->
	lists:foreach(
		fun(PID) ->
			exit(PID, stop)
		end,
		Miners
	).

%% @doc Stop and then start the workers again and return the new state.
restart_miners(S) ->
	stop_miners(S#state.miners),
	start_miners(S).

%% @doc A worker process to hash the data segment searching for a solution
%% for the given diff.
mine(
	#{
		data_segment := BDS,
		diff := Diff,
		timestamp := Timestamp,
		height := Height
	},
	Supervisor
) ->
	process_flag(priority, low),
	{Nonce, Hash} = find_nonce(BDS, Diff, Height, Supervisor),
	Supervisor ! {solution, Hash, Nonce, Timestamp}.

get_search_space_upper_bound(BI, Height) ->
	SearchSpaceUpperBoundDepth = ?SEARCH_SPACE_UPPER_BOUND_DEPTH(Height),
	case length(BI) < SearchSpaceUpperBoundDepth of
		true ->
			element(2, lists:last(BI));
		false ->
			element(2, lists:nth(SearchSpaceUpperBoundDepth, BI))
	end.

find_nonce(BDS, Diff, Height, Supervisor) ->
	case randomx_bulk_hasher(Height) of
		{ok, Hasher} ->
			StartNonce =
				{crypto:strong_rand_bytes(256 div 8), crypto:strong_rand_bytes(256 div 8)},
			find_nonce(BDS, Diff, Height, StartNonce, Hasher, Supervisor);
		not_found ->
			ar:info("Mining is waiting on RandomX initialization"),
			timer:sleep(30 * 1000),
			find_nonce(BDS, Diff, Height, Supervisor)
	end.

randomx_bulk_hasher(Height) ->
	case ar_randomx_state:randomx_state_by_height(Height) of
		{state, {fast, FastState}} ->
			%% Use RandomX fast-mode, where hashing is fast but initialization is slow.
			Hasher = fun(Nonce, BDS, Diff) ->
				ar_mine_randomx:bulk_hash_fast(FastState, Nonce, BDS, Diff)
			end,
			{ok, Hasher};
		{state, {light, _}} ->
			not_found;
		{key, _} ->
			not_found
	end.

find_nonce(BDS, Diff, Height, Nonce, Hasher, Supervisor) ->
	{BDSHash, HashNonce, ExtraNonce, HashesTried} = Hasher(Nonce, BDS, Diff),
	Supervisor ! {hashes_tried, HashesTried},
	case validate(BDSHash, Diff, Height) of
		false ->
			%% Re-use the hash as the next nonce, since we get it for free.
			find_nonce(BDS, Diff, Height, {BDSHash, ExtraNonce}, Hasher, Supervisor);
		true ->
			{HashNonce, BDSHash}
	end.

%% @doc Search for a Succinct Proof of Random Access.
mine_spora(
	#{
		data_segment := BDS,
		diff := Diff,
		timestamp := Timestamp,
		height := Height,
		prev_h := PrevH,
		search_space_upper_bound := SearchSpaceUpperBound,
		stats := Stats
	} = WorkerState,
	Supervisor
) ->
	case randomx_hasher(Height) of
		{ok, Hasher} ->
			StartNonce = crypto:strong_rand_bytes(256 div 8),
			{Nonce, RXHash} = find_rx_hash(Hasher, StartNonce, BDS, Height),
			{FetchTime, SPoA} =
				case pick_recall_byte(RXHash, PrevH, SearchSpaceUpperBound, Height) of
					{error, weave_size_too_small} ->
						{0, #poa{}};
					{ok, RecallByte} ->
						timer:tc(fun() -> ar_poa:get_poa_from_v2_index(RecallByte) end)
				end,
			case SPoA of
				not_found ->
					Supervisor ! {sporas_discovered, 1},
					mine_spora(WorkerState, Supervisor);
				_ ->
					UpdatedStats = update_mining_stats(Stats, FetchTime, SPoA),
					SolutionHash = spora_solution_hash(RXHash, SPoA),
					case validate(SolutionHash, Diff, Height) of
						false ->
							Supervisor ! {sporas_tried, 1},
							mine_spora(WorkerState#{ stats => UpdatedStats }, Supervisor);
						true ->
							log_mining_stats(UpdatedStats),
							Supervisor ! {spora_solution, SolutionHash, Nonce, SPoA, Timestamp}
					end
			end;
		not_found ->
			ar:info("Mining is waiting on RandomX initialization"),
			timer:sleep(30 * 1000),
			mine_spora(WorkerState, Supervisor)
	end.

randomx_hasher(Height) ->
	case ar_randomx_state:randomx_state_by_height(Height) of
		{state, {fast, FastState}} ->
			%% Use RandomX fast-mode, where hashing is fast but initialization is slow.
			Hasher = fun(Nonce, BDS) ->
				ar_mine_randomx:hash_fast(FastState, << Nonce/binary, BDS/binary >>)
			end,
			{ok, Hasher};
		{state, {light, _}} ->
			not_found;
		{key, _} ->
			not_found
	end.

find_rx_hash(Hasher, Nonce, BDS, Height) ->
	H = Hasher(Nonce, BDS),
	case validate(H, ?SPORA_SLOW_HASH_DIFF(Height), Height) of
		true ->
			{Nonce, H};
		false ->
			find_rx_hash(Hasher, H, BDS, Height)
	end.

pick_recall_byte(H, PrevH, SearchSpaceUpperBound, Height) ->
	Subspaces = ?SPORA_SEARCH_SPACE_SUBSPACES_COUNT(Height),
	case SearchSpaceUpperBound < Subspaces of
		true ->
			{error, weave_size_too_small};
		false ->
			SubspaceNumber = binary:decode_unsigned(H, big) rem Subspaces,
			EvenSubspaceSize = SearchSpaceUpperBound div Subspaces,
			AbsoluteSubspaceStart = SubspaceNumber * EvenSubspaceSize,
			SubspaceSize = min(SearchSpaceUpperBound - AbsoluteSubspaceStart, EvenSubspaceSize),
			EncodedSubspaceNumber = binary:encode_unsigned(SubspaceNumber),
			SearchSubspaceSeed =
				binary:decode_unsigned(ar_deep_hash:hash([PrevH, EncodedSubspaceNumber]), big),
			SearchSubspaceStart = SearchSubspaceSeed rem SubspaceSize,
			SubspaceByteSeed = binary:decode_unsigned(crypto:hash(sha256, H), big),
			SubspaceByte = SubspaceByteSeed rem SubspaceSize,
			{ok, AbsoluteSubspaceStart + (SearchSubspaceStart + SubspaceByte) rem SubspaceSize}
	end.

update_mining_stats(Stats, _Time, SPoA) when SPoA == #poa{} ->
	Stats;
update_mining_stats(Stats, Time, SPoA) ->
	ChunkSize = byte_size(SPoA#poa.chunk),
	{_RunningAverage, Sum, Count} = maps:get(ChunkSize, Stats, {0, 0, 0}),
	maps:put(ChunkSize, {(Sum + Time) div (Count + 1), Sum + Time, Count + 1}, Stats).

spora_solution_hash(H, SPoA) ->
	crypto:hash(sha256, ar_deep_hash:hash([H, SPoA#poa.chunk])).

log_mining_stats(Stats) ->
	ar:info([{event, mining_stats} | maps:to_list(Stats)]).

-ifdef(DEBUG).
min_randomx_difficulty() -> 1.
-else.
min_randomx_difficulty() -> min_sha384_difficulty() + ?RANDOMX_DIFF_ADJUSTMENT.
min_sha384_difficulty() -> 31.
-endif.

min_spora_difficulty(Height) ->
	?SPORA_MIN_DIFFICULTY(Height).

%% Tests

%% @doc Test that found nonces abide by the difficulty criteria.
basic_test_() ->
	{timeout, 20, fun test_basic/0}.

test_basic() ->
	[B0] = ar_weave:init([]),
	{Node, _} = ar_test_node:start(B0),
	ar_node:mine(Node),
	BI = ar_test_node:wait_until_height(Node, 1),
	B1 = ar_storage:read_block(hd(BI)),
	start(B1, B1#block.poa, [], unclaimed, [], self(), [], BI),
	assert_mine_output(B1, B1#block.poa, []).

%% @doc Ensure that the block timestamp gets updated regularly while mining.
timestamp_refresh_test_() ->
	{timeout, 60, fun test_timestamp_refresh/0}.

test_timestamp_refresh() ->
	%% Start mining with a high enough difficulty, so that the block
	%% timestamp gets refreshed at least once. Since we might be unlucky
	%% and find the block too fast, we retry until it succeeds.
	[B0] = ar_weave:init([], ar_retarget:switch_to_linear_diff(18)),
	B = B0,
	Run = fun(_) ->
		TXs = [],
		StartTime = os:system_time(seconds),
		POA = #poa{},
		start(B, POA, TXs, unclaimed, [], self(), [], [ar_util:block_index_entry_from_block(B0)]),
		{_, MinedTimestamp} = assert_mine_output(B, POA, TXs),
		MinedTimestamp > StartTime + ?MINING_TIMESTAMP_REFRESH_INTERVAL
	end,
	?assert(lists:any(Run, lists:seq(1, 20))).

excludes_no_longer_valid_txs_test_() ->
	{timeout, 60, fun test_excludes_no_longer_valid_txs/0}.

test_excludes_no_longer_valid_txs() ->
	%% Start mining with a high enough difficulty, so that the block
	%% timestamp gets refreshed at least once. Since we might be unlucky
	%% and find the block too fast, we retry until it succeeds.
	Diff = ar_retarget:switch_to_linear_diff(18),
	Key = {_, Pub} = ar_wallet:new(),
	Address = ar_wallet:to_address(Pub),
	Wallets = [{Address, ?AR(1000000000000), <<>>}],
	[B] = ar_weave:init(Wallets, Diff),
	{Node, _} = ar_test_node:start(B),
	BI = ar_test_node:wait_until_height(Node, 0),
	Run = fun() ->
		Now = os:system_time(seconds),
		%% The transaction is invalid because its fee is based on a timestamp from the future.
		InvalidTX = ar_test_node:sign_tx(Key, #{
			last_tx => B#block.indep_hash,
			reward => ar_tx:calculate_min_tx_cost(0, Diff, 10, Wallets, <<>>, Now + 10000)
		}),
		ValidTX = ar_test_node:sign_tx(Key, #{
			last_tx => B#block.indep_hash,
			reward => ar_tx:calculate_min_tx_cost(0, Diff, 10, Wallets, <<>>, Now)
		}),
		TXs = [ValidTX, InvalidTX],
		start(B, #poa{}, TXs, unclaimed, [], self(), [{B#block.indep_hash, []}], BI),
		receive
			{work_complete, _BH, MinedB, MinedTXs, _BDS, _POA, _} ->
				{ValidTX, Now, MinedB#block.timestamp, MinedTXs}
		after 120000 ->
			error(timeout)
		end
	end,
	{ValidTX, _, _, MinedTXs} = run_until(
		fun({_, StartMineTimestamp, Timestamp, _}) ->
			Timestamp > StartMineTimestamp + ?MINING_TIMESTAMP_REFRESH_INTERVAL
		end,
		Run
	),
	?assertEqual([ValidTX#tx.id], [TX#tx.id || TX <- MinedTXs]).

run_until(Pred, Fun) ->
	Run = Fun(),
	case Pred(Run) of
		true ->
			Run;
		false ->
			run_until(Pred, Fun)
	end.

%% @doc Ensures ar_mine can be started and stopped.
start_stop_test() ->
	[B] = ar_weave:init(),
	{Node, _} = ar_test_node:start(B),
	BI = ar_test_node:wait_until_height(Node, 0),
	HighDiff = ar_retarget:switch_to_linear_diff(30),
	PID = start(B#block{ diff = HighDiff }, #poa{}, [], unclaimed, [], self(), [], BI),
	timer:sleep(500),
	assert_alive(PID),
	stop(PID),
	assert_not_alive(PID, 3000).

%% @doc Ensures a miner can be started and stopped.
miner_start_stop_test() ->
	S = #{
		diff => trunc(math:pow(2, 1000)),
		timestamp => os:system_time(seconds),
		data_segment => <<>>,
		height => 1
	},
	PID = spawn(?MODULE, mine, [S, self()]),
	timer:sleep(500),
	assert_alive(PID),
	stop_miners([PID]),
	assert_not_alive(PID, 3000).

assert_mine_output(B, POA, TXs) ->
	receive
		{work_complete, BH, NewB, MinedTXs, BDSOrBDSBase, POA, _} ->
			?assertEqual(BH, B#block.indep_hash),
			?assertEqual(lists:sort(TXs), lists:sort(MinedTXs)),
			BDS = ar_block:generate_block_data_segment(NewB),
			case NewB#block.height >= ar_fork:height_2_3() of
				true ->
					BDSOrBDSBase = ar_block:generate_block_data_segment_base(NewB),
					?assertEqual(
						spora_solution_hash(
							ar_weave:hash(BDS, NewB#block.nonce, B#block.height), POA),
						NewB#block.hash
					);
				false ->
					BDSOrBDSBase = BDS,
					?assertEqual(
						ar_weave:hash(BDS, NewB#block.nonce, B#block.height),
						NewB#block.hash
					)
			end,
			?assert(binary:decode_unsigned(NewB#block.hash) > NewB#block.diff),
			{NewB#block.diff, NewB#block.timestamp}
	after 20000 ->
		error(timeout)
	end.

assert_alive(PID) ->
	?assert(is_process_alive(PID)).

assert_not_alive(PID, Timeout) ->
	Do = fun () -> not is_process_alive(PID) end,
	?assert(ar_util:do_until(Do, 50, Timeout)).
