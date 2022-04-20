-module(ar_mine).

-export([start/1, stop/1, io_thread/1, validate/4, validate/3, validate_spora/1,
		min_difficulty/1, min_spora_difficulty/1, sha384_diff_to_randomx_diff/1,
		spora_solution_hash/5, spora_solution_hash_with_entropy/6, spora_h0_with_entropy/3,
		pick_recall_byte/3, get_search_space_upper_bound/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_block.hrl").
-include_lib("arweave/include/ar_mine.hrl").

-include_lib("eunit/include/eunit.hrl").

%% State record for miners.
-record(state, {
	current_block,
	candidate_block,
	block_anchors,	% List of block hashes of the latest ?MAX_TX_ANCHOR_DEPTH blocks.
	recent_txs_map, % A map TXID -> ok of the txs of the latest ?MAX_TX_ANCHOR_DEPTH blocks.
	txs,
	timestamp,
	timestamp_refresh_timer,
	data_segment,
	data_segment_duration,
	reward_addr,
	reward_wallet_before_mining_reward = not_in_the_list,
	tags,
	diff,
	bds_base = not_generated,
	stage_one_hasher,
	stage_two_hasher,
	recent_block_index,
	search_space_upper_bound,
	blocks_by_timestamp = #{},
	block_index,
	io_threads,
	hashing_threads,
	session_ref
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Spawns a new mining process and returns its PID.
start(Args) ->
	{CurrentB, TXs, RewardAddr, Tags, BlockAnchors, RecentTXMap, SearchSpaceUpperBound,
			RecentBI, IOThreads} = Args,
	CurrentHeight = CurrentB#block.height,
	Height = CurrentHeight + 1,
	CandidateB = #block{
		height = Height,
		previous_block = CurrentB#block.indep_hash,
		hash_list_merkle = ar_block:compute_hash_list_merkle(CurrentB),
		reward_addr = RewardAddr,
		tags = Tags
	},
	State =
		#state{
			current_block = CurrentB,
			data_segment_duration = 0,
			reward_addr = RewardAddr,
			tags = Tags,
			block_anchors = BlockAnchors,
			recent_txs_map = RecentTXMap,
			candidate_block = CandidateB,
			txs = TXs,
			search_space_upper_bound = SearchSpaceUpperBound,
			recent_block_index = RecentBI,
			io_threads = IOThreads,
			session_ref = make_ref()
		},
	Fork_2_5 = ar_fork:height_2_5(),
	State2 =
		case Height >= Fork_2_5 of
			false ->
				State;
			true ->
				{Rate, ScheduledRate} = ar_pricing:recalculate_usd_to_ar_rate(CurrentB),
				State#state{
					candidate_block =
						CandidateB#block{
							usd_to_ar_rate = Rate,
							scheduled_usd_to_ar_rate = ScheduledRate,
							packing_2_5_threshold =
								case Height == Fork_2_5 of
									true ->
										SearchSpaceUpperBound;
									false ->
										ar_block:shift_packing_2_5_threshold(
											CurrentB#block.packing_2_5_threshold)
								end,
							strict_data_split_threshold =
								case Height == Fork_2_5 of
									true ->
										CurrentB#block.weave_size;
									false ->
										CurrentB#block.strict_data_split_threshold
								end
						}
				}
		end,
	start_server(State2).

%% @doc Stop the running mining server.
stop(PID) ->
	PID ! stop.

%% @doc Validate that a given hash/nonce satisfy the difficulty requirement.
validate(BDS, Nonce, Diff, Height) ->
	BDSHash = ar_randomx_state:hash(Height, << Nonce/binary, BDS/binary >>),
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
validate_spora(Args) ->
	{BDS, Nonce, Timestamp, Height, Diff, PrevH, SearchSpaceUpperBound, PackingThreshold,
			RecentBI, StrictDataSplitThreshold, SPoA} = Args,
	Chunk = SPoA#poa.chunk,
	{H0, Entropy} = spora_h0_with_entropy(BDS, Nonce, Height),
	case pick_recall_byte(H0, PrevH, SearchSpaceUpperBound) of
		{error, weave_size_too_small} ->
			SolutionHash = spora_solution_hash(PrevH, Timestamp, H0, Chunk, Height),
			case SPoA == #poa{} of
				false ->
					{false, SolutionHash};
				true ->
					case validate(SolutionHash, Diff, Height) of
						false ->
							{false, SolutionHash};
						true ->
							{true, no_chunk, SolutionHash}
					end
			end;
		{ok, RecallByte} ->
			SolutionHash =
				case RecallByte >= PackingThreshold of
					true ->
						spora_solution_hash_with_entropy(PrevH, Timestamp, H0, Chunk, Entropy,
								Height);
					false ->
						spora_solution_hash(PrevH, Timestamp, H0, Chunk, Height)
				end,
			case validate(SolutionHash, Diff, Height) of
				false ->
					{false, SolutionHash};
				true ->
					validate_spoa({RecallByte, SPoA, SolutionHash, Height, PackingThreshold,
							RecentBI, StrictDataSplitThreshold})
			end
	end.

validate_spoa(Args) ->
	{RecallByte, SPoA, SolutionHash, Height, PackingThreshold,
			RecentBI, StrictDataSplitThreshold} = Args,
	{BlockStart, BlockEnd, TXRoot} = ar_block_index:get_block_bounds(RecallByte, RecentBI),
	case Height >= ar_fork:height_2_5()
			andalso RecallByte >= PackingThreshold of
		true ->
			case byte_size(SPoA#poa.chunk) /= (?DATA_CHUNK_SIZE) of
				true ->
					%% All packed chunks must be padded to 256 KiB before packing.
					{false, SolutionHash};
				false ->
					case ar_poa:validate(BlockStart, RecallByte, TXRoot,
							BlockEnd - BlockStart, SPoA, StrictDataSplitThreshold) of
						false ->
							{false, SolutionHash};
						true ->
							prometheus_counter:inc(validating_packed_spora),
							{true, packed_chunk, SolutionHash}
					end
			end;
		false ->
			case ar_poa:validate_pre_fork_2_5(RecallByte - BlockStart, TXRoot,
					BlockEnd - BlockStart, SPoA) of
				false ->
					{false, SolutionHash};
				true ->
					prometheus_counter:inc(validating_unpacked_spora),
					{true, unpacked_chunk, SolutionHash}
			end
	end.

-ifdef(DEBUG).
min_difficulty(_Height) ->
	1.
-else.
min_difficulty(Height) ->
	Diff =
		case Height >= ar_fork:height_1_7() of
			true ->
				case Height >= ar_fork:height_2_4() of
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
			case Height >= ar_fork:height_2_5() of
				true ->
					ar_retarget:switch_to_linear_diff(Diff);
				false ->
					ar_retarget:switch_to_linear_diff_pre_fork_2_5(Diff)
			end;
		false ->
			Diff
	end.
-endif.

sha384_diff_to_randomx_diff(Sha384Diff) ->
	max(Sha384Diff + ?RANDOMX_DIFF_ADJUSTMENT, min_randomx_difficulty()).

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_search_space_upper_bound(BI) ->
	SearchSpaceUpperBoundDepth = ?SEARCH_SPACE_UPPER_BOUND_DEPTH,
	case length(BI) < SearchSpaceUpperBoundDepth of
		true ->
			element(2, lists:last(BI));
		false ->
			element(2, lists:nth(SearchSpaceUpperBoundDepth, BI))
	end.

%% @doc Start the main mining server.
start_server(#state{ candidate_block = #block{ height = Height } } = S) ->
	case prepare_randomx(Height) of
		{ok, {StageOneHasher, StageTwoHasher}} ->
			spawn(fun() ->
				try
					S2 = S#state{ stage_one_hasher = StageOneHasher,
							stage_two_hasher = StageTwoHasher },
					server(notify_hashing_threads(start_miners(
							prometheus_histogram:observe_duration(
							block_construction_time_milliseconds,
							fun() -> update_txs(S2) end))))
				catch Type:Exception:StackTrace ->
					?LOG_ERROR(
						"event: mining_server_exception, type: ~p, exception: ~p,"
						" stacktrace: ~p",
						[Type, Exception, StackTrace]
					)
				end
			end);
		not_found ->
			?LOG_INFO([{event, mining_waiting_on_randomx_initialization}]),
			timer:sleep(10 * 1000),
			start_server(S)
	end.

%% @doc Takes a state and a set of transactions and return a new state with the
%% new set of transactions.
%% @end
update_txs(
	S = #state {
		current_block = CurrentB,
		data_segment_duration = BDSGenerationDuration,
		block_anchors = BlockAnchors,
		recent_txs_map = RecentTXMap,
		reward_addr = RewardAddr,
		candidate_block = #block{ height = Height } = CandidateB,
		txs = TXs,
		blocks_by_timestamp = BlocksByTimestamp
	}
) ->
	NextBlockTimestamp = next_block_timestamp(BlocksByTimestamp, BDSGenerationDuration),
	Rate = ar_pricing:usd_to_ar_rate(CurrentB),
	NextDiff = calc_diff(CurrentB, NextBlockTimestamp),
	ValidTXs =
		ar_tx_replay_pool:pick_txs_to_mine({
			BlockAnchors,
			RecentTXMap,
			CurrentB#block.height,
			Rate,
			NextBlockTimestamp,
			ar_wallets:get(CurrentB#block.wallet_list, ar_tx:get_addresses(TXs)),
			TXs
		}),
	BlockSize2 =
		lists:foldl(
			fun(TX, Acc) ->
				Acc + ar_tx:get_weave_size_increase(TX, Height)
			end,
			0,
			ValidTXs
		),
	WeaveSize2 = CurrentB#block.weave_size + BlockSize2,
	{FinderReward, EndowmentPool} =
		ar_pricing:get_miner_reward_and_endowment_pool({
			CurrentB#block.reward_pool,
			ValidTXs,
			RewardAddr,
			WeaveSize2,
			Height,
			NextBlockTimestamp,
			Rate
		}),
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
	Wallets2 =
		ar_node_utils:apply_mining_reward(AppliedTXsWallets, RewardAddr, FinderReward),
	{ok, RootHash2} =
		ar_wallets:add_wallets(
			CurrentB#block.wallet_list,
			Wallets2,
			RewardAddr,
			Height
		),
	CandidateB2 =
		CandidateB#block{
			txs = [TX#tx.id || TX <- ValidTXs],
			tx_root = ar_block:generate_tx_root_for_block(ValidTXs, Height),
			block_size = BlockSize2,
			weave_size = WeaveSize2,
			wallet_list = RootHash2,
			reward_pool = EndowmentPool
		},
	BDSBase = ar_block:generate_block_data_segment_base(CandidateB2),
	update_data_segment(
		S#state{
			candidate_block = CandidateB2,
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
%% @end
next_block_timestamp(BlocksByTimestamp, BDSGenerationDuration) ->
	Timestamp = os:system_time(seconds) + BDSGenerationDuration,
	case maps:get(Timestamp, BlocksByTimestamp, not_found) of
		not_found ->
			Timestamp;
		_ ->
			Timestamp + 1
	end.

%% @doc Given a block calculate the difficulty to mine on for the next block.
%% Difficulty is retargeted each ?RETARGET_BlOCKS blocks, specified in ar.hrl
%% This is done in attempt to maintain on average a fixed block time.
%% @end
calc_diff(CurrentB, NextBlockTimestamp) ->
	ar_retarget:maybe_retarget(
		CurrentB#block.height + 1,
		CurrentB#block.diff,
		NextBlockTimestamp,
		CurrentB#block.last_retarget,
		CurrentB#block.timestamp
	).

%% @doc Generate a new data_segment and update the timestamp and diff.
update_data_segment(
	S = #state {
		data_segment_duration = BDSGenerationDuration,
		current_block = CurrentB,
		blocks_by_timestamp = BlocksByTimestamp
	}
) ->
	BlockTimestamp = next_block_timestamp(BlocksByTimestamp, BDSGenerationDuration),
	Diff = calc_diff(CurrentB, BlockTimestamp),
	update_data_segment(S, BlockTimestamp, Diff).

update_data_segment(State, Timestamp, Diff) ->
	#state{ candidate_block = #block{ height = Height } } = State,
	case Height >= ar_fork:height_2_5() of
		true ->
			update_data_segment2(State, Timestamp, Diff);
		false ->
			update_data_segment_pre_fork_2_5(State, Timestamp, Diff)
	end.

update_data_segment2(State, Timestamp, Diff) ->
	#state{
		current_block = CurrentB,
		candidate_block = #block{ height = Height } = CandidateB,
		bds_base = BDSBase,
		session_ref = SessionRef,
		blocks_by_timestamp = BlocksByTimestamp
	} = State,
	LastRetarget2 =
		case ar_retarget:is_retarget_height(Height) of
			true ->
				Timestamp;
			false ->
				CurrentB#block.last_retarget
		end,
	CDiff =
		ar_difficulty:next_cumulative_diff(
			CurrentB#block.cumulative_diff,
			Diff,
			Height
		),
	{DurationMicros, BDS2} =
		timer:tc(
			fun() ->
				ar_block:generate_block_data_segment(
					BDSBase,
					CandidateB#block.hash_list_merkle,
					#{
						timestamp => Timestamp,
						last_retarget => LastRetarget2,
						diff => Diff,
						cumulative_diff => CDiff,
						reward_pool => CandidateB#block.reward_pool,
						wallet_list => CandidateB#block.wallet_list
					}
				)
			end
		),
	CandidateB2 =
		CandidateB#block{
			timestamp = Timestamp,
			last_retarget = LastRetarget2,
			diff = Diff,
			cumulative_diff = CDiff
		},
	BlocksByTimestamp2 =
		maps:filter(
			fun(T, _) ->
				T + 20 > Timestamp
			end,
			maps:put(Timestamp, {CandidateB2, BDS2}, BlocksByTimestamp)
		),
	State2 =
		State#state {
			timestamp = Timestamp,
			diff = Diff,
			data_segment = BDS2,
			data_segment_duration = round(DurationMicros / 1000000),
			candidate_block = CandidateB2,
			blocks_by_timestamp = BlocksByTimestamp2
		},
	PackingThreshold = CandidateB#block.packing_2_5_threshold,
	ets:insert(mining_state, {session, {SessionRef, Timestamp, PackingThreshold, Height}}),
	reschedule_timestamp_refresh(State2).

update_data_segment_pre_fork_2_5(S, BlockTimestamp, Diff) ->
	#state{
		current_block = CurrentB,
		candidate_block = CandidateB,
		reward_addr = RewardAddr,
		bds_base = BDSBase,
		reward_wallet_before_mining_reward = RewardWalletBeforeMiningReward,
		txs = TXs,
		blocks_by_timestamp = BlocksByTimestamp,
		session_ref = SessionRef
	} = S,
	Height = CandidateB#block.height,
	LastRetarget2 =
		case ar_retarget:is_retarget_height(Height) of
			true -> BlockTimestamp;
			false -> CurrentB#block.last_retarget
		end,
	{MinerReward, RewardPool} =
		ar_pricing:get_miner_reward_and_endowment_pool({
			CurrentB#block.reward_pool,
			TXs,
			RewardAddr,
			CandidateB#block.weave_size,
			Height,
			BlockTimestamp,
			?INITIAL_USD_TO_AR_PRE_FORK_2_5
		}),
	RewardWallet = case RewardWalletBeforeMiningReward of
		not_in_the_list ->
			#{};
		{Addr, Balance, LastTX} ->
			#{ Addr => {Balance, LastTX} }
	end,
	RewardWallet2 =
		case maps:get(
			RewardAddr,
			ar_node_utils:apply_mining_reward(RewardWallet, RewardAddr, MinerReward),
			not_found
		) of
			not_found ->
				#{};
			WalletData ->
				#{ RewardAddr => WalletData }
		end,
	{ok, RootHash2} =
		ar_wallets:update_wallets(
			CandidateB#block.wallet_list,
			RewardWallet2,
			RewardAddr,
			Height
		),
	CDiff = ar_difficulty:next_cumulative_diff(
		CurrentB#block.cumulative_diff,
		Diff,
		Height
	),
	{DurationMicros, BDS2} = timer:tc(
		fun() ->
			ar_block:generate_block_data_segment(
				BDSBase,
				CandidateB#block.hash_list_merkle,
				#{
					timestamp => BlockTimestamp,
					last_retarget => LastRetarget2,
					diff => Diff,
					cumulative_diff => CDiff,
					reward_pool => RewardPool,
					wallet_list => RootHash2
				}
			)
		end
	),
	CandidateB2 =
		CandidateB#block{
			timestamp = BlockTimestamp,
			last_retarget = LastRetarget2,
			diff = Diff,
			cumulative_diff = CDiff,
			reward_pool = RewardPool,
			wallet_list = RootHash2
		},
	BlocksByTimestamp2 =
		maps:filter(
			fun(Timestamp, _) ->
				Timestamp + 20 > BlockTimestamp
			end,
			maps:put(BlockTimestamp, {CandidateB2, BDS2}, BlocksByTimestamp)
		),
	S2 =
		S#state {
			timestamp = BlockTimestamp,
			diff = Diff,
			data_segment = BDS2,
			data_segment_duration = round(DurationMicros / 1000000),
			candidate_block = CandidateB2,
			blocks_by_timestamp = BlocksByTimestamp2
		},
	ets:insert(mining_state, {session, {SessionRef, BlockTimestamp, not_set, Height}}),
	reschedule_timestamp_refresh(S2).

reschedule_timestamp_refresh(S = #state{
	timestamp_refresh_timer = Timer,
	data_segment_duration = BDSGenerationDuration,
	txs = TXs
}) ->
	timer:cancel(Timer),
	case ?MINING_TIMESTAMP_REFRESH_INTERVAL - BDSGenerationDuration  of
		TimeoutSeconds when TimeoutSeconds =< 0 ->
			TXIDs = lists:map(fun(TX) -> TX#tx.id end, TXs),
			?LOG_WARNING([
				{event, ar_mine_slow_data_segment_generation},
				{duration, BDSGenerationDuration},
				{timestamp_refresh_interval, ?MINING_TIMESTAMP_REFRESH_INTERVAL},
				{txs_count, length(TXIDs)}
			]),
			self() ! refresh_timestamp,
			S#state{ timestamp_refresh_timer = no_timer };
		TimeoutSeconds ->
			case timer:send_after(TimeoutSeconds * 1000, refresh_timestamp) of
				{ok, Ref} ->
					S#state{ timestamp_refresh_timer = Ref };
				{error, Reason} ->
					?LOG_ERROR("ar_mine: Reschedule timestamp refresh failed: ~p", [Reason]),
					S
			end
	end.

%% @doc Start the workers and return the new state.
start_miners(S) ->
	ets:insert(mining_state, [
		{started_at, os:timestamp()},
		{sporas, 0},
		{bytes_read, 0},
		{recall_bytes_computed, 0}
	]),
	start_hashing_threads(S).

start_hashing_threads(S) ->
	#state{
		candidate_block = #block{ timestamp = Timestamp, diff = Diff, height = Height },
		current_block = #block{ indep_hash = PrevH },
		data_segment = BDS,
		stage_two_hasher = StageTwoHasher,
		search_space_upper_bound = SearchSpaceUpperBound,
		session_ref = SessionRef
	} = S,
	Subspaces = ?SPORA_SEARCH_SPACE_SUBSPACES_COUNT,
	SearchSubspaceSize = ?SPORA_SEARCH_SPACE_SIZE(SearchSpaceUpperBound) div Subspaces,
	case SearchSubspaceSize of
		0 ->
			Parent = self(),
			Thread = spawn(
				fun() ->
					small_weave_hashing_thread({
						Timestamp,
						Diff,
						BDS,
						PrevH,
						SearchSpaceUpperBound,
						StageTwoHasher,
						Parent,
						Height,
						SessionRef
					})
				end
			),
			S#state{ hashing_threads = [Thread] };
		_ ->
			start_hashing_threads2(S)
	end.

start_hashing_threads2(S) ->
	#state{ current_block = #block{ indep_hash = PrevH },
			candidate_block = #block{ timestamp = Timestamp, diff = Diff, height = Height,
					packing_2_5_threshold = PackingThreshold }, data_segment = BDS,
			io_threads = IOThreads, stage_one_hasher = Hasher, stage_two_hasher = StageTwoHasher,
			search_space_upper_bound = SearchSpaceUpperBound, session_ref = SessionRef } = S,
	{ok, Config} = application:get_env(arweave, config),
	Parent = self(),
	Schedulers = erlang:system_info(schedulers_online),
	%% Keep one scheduler free to make sure the VM continues to compute file offsets from
	%% recall bytes, schedule new chunk reads, and pass fetched chunks to the hashing processors
	%% while all the dirty CPU schedulers are busy.
	StageOneThreadCount = max(0, min(Schedulers - 1, Config#config.stage_one_hashing_threads)),
	StageTwoThreadCount = Config#config.stage_two_hashing_threads,
	ThreadCount = max(0, min(Schedulers - 1, StageOneThreadCount + StageTwoThreadCount)),
	HashingThreads =
		[spawn(
			fun() ->
				ShuffledIOThreads = lists:sort(fun(_, _) -> rand:uniform() > 0.5 end,
						IOThreads),
				Type =
					case N =< StageOneThreadCount of
						true ->
							stage_one_thread;
						_ ->
							stage_two_thread
					end,
				hashing_thread({Parent, PrevH, SearchSpaceUpperBound, PackingThreshold, Height,
						Timestamp, Diff, BDS, Hasher, StageTwoHasher, [], ShuffledIOThreads,
						Config#config.randomx_bulk_hashing_iterations, ar_mine_randomx:jit(),
						ar_mine_randomx:large_pages(), ar_mine_randomx:hardware_aes(),
						SessionRef}, Type)
			end)
			|| N <- lists:seq(1, ThreadCount)],
	S#state{ hashing_threads = HashingThreads }.

%% @doc The main mining server.
server(
	S = #state{
		txs = MinedTXs,
		current_block = #block{ indep_hash = PrevH },
		candidate_block = #block{ height = Height },
		search_space_upper_bound = SearchSpaceUpperBound,
		blocks_by_timestamp = BlocksByTimestamp,
		recent_block_index = RecentBI
	}
) ->
	receive
		%% Stop the mining process and all the workers.
		stop ->
			stop_miners(S),
			log_spora_performance();
		{solution, Nonce, H0, Timestamp, Hash} ->
			case maps:get(Timestamp, BlocksByTimestamp, not_found) of
				not_found ->
					%% A stale solution.
					server(S);
				{#block{ timestamp = Timestamp } = B, BDS} ->
					PackingThreshold = B#block.packing_2_5_threshold,
					case get_spoa(H0, PrevH, SearchSpaceUpperBound, Height, PackingThreshold) of
						{not_found, RecallByte} ->
							?LOG_WARNING([{event, found_chunk_but_no_proofs},
									{previous_block, ar_util:encode(PrevH)},
									{h0, ar_util:encode(H0)}, {byte, RecallByte}]),
							server(S);
						SPoA ->
							case validate_spora({BDS, Nonce, Timestamp, Height, B#block.diff,
									PrevH, SearchSpaceUpperBound,
									B#block.packing_2_5_threshold, RecentBI,
									B#block.strict_data_split_threshold, SPoA}) of
								{true, _, Hash} ->
									B2 =
										B#block{
											poa = SPoA,
											hash = Hash,
											nonce = Nonce
										},
									stop_miners(S),
									process_spora_solution(BDS, B2, MinedTXs, S);
								_ ->
									?LOG_ERROR([
										{event, miner_produced_invalid_spora},
										{hash, ar_util:encode(Hash)},
										{nonce, ar_util:encode(Nonce)},
										{prev_block, ar_util:encode(PrevH)},
										{segment, ar_util:encode(BDS)},
										{timestamp, Timestamp},
										{height, Height},
										{search_space_upper_bound, SearchSpaceUpperBound}
									]),
									server(S)
							end
					end
			end;
		%% The block timestamp must be reasonable fresh since it's going to be
		%% validated on the remote nodes when it's propagated to them. Only blocks
		%% with a timestamp close to current time will be accepted in the propagation.
		refresh_timestamp ->
			server(notify_hashing_threads(update_data_segment(S)));
		UnexpectedMessage ->
			?LOG_WARNING(
				"event: mining_server_got_unexpected_message, message: ~p",
				[UnexpectedMessage]
			),
			server(S)
	end.

stop_miners(S) ->
	ets:insert(
		mining_state,
		{session, {make_ref(), os:system_time(second), not_set, not_set}}
	),
	stop_hashing_threads(S).

stop_hashing_threads(#state{ hashing_threads = Threads }) ->
	lists:foreach(fun(Thread) -> exit(Thread, stop) end, Threads).

notify_hashing_threads(S) ->
	#state{
		hashing_threads = Threads,
		candidate_block = #block{ timestamp = Timestamp, diff = Diff },
		data_segment = BDS,
		session_ref = SessionRef
	} = S,
	{ok, Config} = application:get_env(arweave, config),
	StageTwoThreadCount = Config#config.stage_two_hashing_threads,
	StageTwoThreads = lists:sublist(Threads, StageTwoThreadCount),
	lists:foreach(
		fun(Thread) ->
			Thread ! {update_state, Timestamp, Diff, BDS, StageTwoThreads, SessionRef}
		end,
		Threads
	),
	S.

io_thread(SearchInRocksDB) ->
	[{_, {SessionRef, SessionTimestamp, PackingThreshold, Height}}]
		= ets:lookup(mining_state, session),
	receive
		{EncodedByte, H0, Entropy, Nonce, HashingThread, {Timestamp, Diff, SessionRef}}
				when Timestamp + 19 > SessionTimestamp ->
			io_thread_read_chunk({EncodedByte, H0, Nonce, HashingThread, Entropy, Timestamp,
					Diff, SessionRef, PackingThreshold, SearchInRocksDB, Height});
		{EncodedByte, H0, Nonce, HashingThread, {Timestamp, Diff, SessionRef}}
				when Timestamp + 19 > SessionTimestamp ->
			io_thread_read_chunk({EncodedByte, H0, Nonce, HashingThread, no_entropy, Timestamp,
					Diff, SessionRef, PackingThreshold, SearchInRocksDB, Height});
		{'EXIT', _From, _Reason} ->
			ar_chunk_storage:close_files();
		_ ->
			io_thread(SearchInRocksDB)
	after 200 ->
		io_thread(SearchInRocksDB)
	end.

io_thread_read_chunk(Args) ->
	{EncodedByte, H0, Nonce, HashingThread, Entropy, Timestamp, Diff, SessionRef,
			PackingThreshold, SearchInRocksDB, Height} = Args,
	Byte = binary:decode_unsigned(EncodedByte, big),
	case read_chunk(Byte, PackingThreshold, SearchInRocksDB, Height) of
		{error, _} ->
			io_thread(SearchInRocksDB);
		{ok, #{ chunk := Chunk }} ->
			ets:update_counter(mining_state, bytes_read, byte_size(Chunk)),
			HashingThread ! {chunk, H0, Byte, Nonce, Timestamp, Diff, Chunk, Entropy, SessionRef},
			io_thread(SearchInRocksDB);
		{ok, Chunk} ->
			ets:update_counter(mining_state, bytes_read, byte_size(Chunk)),
			HashingThread ! {chunk, H0, Byte, Nonce, Timestamp, Diff, Chunk, Entropy, SessionRef},
			io_thread(SearchInRocksDB)
	end.

read_chunk(Byte, PackingThreshold, SearchInRocksDB, Height) ->
	case Height >= ar_fork:height_2_5() andalso Byte >= PackingThreshold of
		true ->
			Options = #{
				packing => spora_2_5,
				pack => false,
				search_fast_storage_only => not SearchInRocksDB
			},
			ar_data_sync:get_chunk(Byte + 1, Options);
		false ->
			Options = #{
				packing => unpacked,
				pack => false,
				search_fast_storage_only => not SearchInRocksDB
			},
			ar_data_sync:get_chunk(Byte + 1, Options)
	end.

small_weave_hashing_thread(Args) ->
	{Timestamp, Diff, BDS, PrevH, SearchSpaceUpperBound, StageTwoHasher, Parent, Height,
			SessionRef} = Args,
	receive
		{update_state, Timestamp2, Diff2, BDS2, _Threads, SessionRef} ->
			small_weave_hashing_thread({Timestamp2, Diff2, BDS2, PrevH, SearchSpaceUpperBound,
					StageTwoHasher, Parent, Height, SessionRef})
	after 0 ->
		Nonce = crypto:strong_rand_bytes(32),
		{H0, _Entropy} = spora_h0_with_entropy(BDS, Nonce, Height),
		TimestampBinary = << Timestamp:(?TIMESTAMP_FIELD_SIZE_LIMIT * 8) >>,
		Preimage = [H0, PrevH, TimestampBinary, <<>>],
		case StageTwoHasher(Diff, Preimage) of
			{true, Hash} ->
				ets:update_counter(mining_state, sporas, 1),
				Parent ! {solution, Nonce, H0, Timestamp, Hash},
				small_weave_hashing_thread(Args);
			false ->
				small_weave_hashing_thread(Args)
		end
	end.

hashing_thread(S, Type) ->
	{Parent, PrevH, SearchSpaceUpperBound, PackingThreshold, Height, Timestamp, Diff, BDS,
			Hasher, StageTwoHasher, StageTwoThreads, IOThreads, HashingIterations, JIT,
			LargePages, HardwareAES, SessionRef} = S,
	T = case Type of stage_one_thread -> 0; stage_two_thread -> 200 end,
	receive
		{chunk, H0, Byte, Nonce, Timestamp2, Diff2, Chunk, Entropy, SessionRef}
				when Timestamp2 + 19 > Timestamp ->
			TimestampBinary = << Timestamp2:(?TIMESTAMP_FIELD_SIZE_LIMIT * 8) >>,
			Preimage = case Byte >= PackingThreshold of
				false ->
					[H0, PrevH, TimestampBinary, Chunk];
				true ->
					[H0, PrevH, TimestampBinary, Chunk, Entropy]
			end,
			case StageTwoHasher(Diff2, Preimage) of
				{true, Hash} ->
					Parent ! {solution, Nonce, H0, Timestamp2, Hash};
				false ->
					ok
			end,
			ets:update_counter(mining_state, sporas, 1),
			hashing_thread(S, Type);
		{update_state, Timestamp2, Diff2, BDS2, StageTwoThreads2, SessionRef} ->
			hashing_thread({Parent, PrevH, SearchSpaceUpperBound, PackingThreshold, Height,
					Timestamp2, Diff2, BDS2, Hasher, StageTwoHasher, StageTwoThreads2,
					IOThreads, HashingIterations, JIT, LargePages, HardwareAES, SessionRef},
					Type);
		_ ->
			hashing_thread(S, Type)
	after T ->
		case Type of
			stage_one_thread when StageTwoThreads /= [] ->
				Nonce1 = crypto:strong_rand_bytes(256 div 8),
				Nonce2 = crypto:strong_rand_bytes(256 div 8),
				Ref = {Timestamp, Diff, SessionRef},
				ok = Hasher(Nonce1, Nonce2, BDS, PrevH, SearchSpaceUpperBound, IOThreads,
						StageTwoThreads, HashingIterations, JIT, LargePages, HardwareAES, Ref),
				ets:update_counter(mining_state, recall_bytes_computed, HashingIterations);
			_ ->
				ok
		end,
		hashing_thread({Parent, PrevH, SearchSpaceUpperBound, PackingThreshold, Height,
				Timestamp, Diff, BDS, Hasher, StageTwoHasher, StageTwoThreads, IOThreads,
				HashingIterations, JIT, LargePages, HardwareAES, SessionRef}, Type)
	end.

get_spoa(H0, PrevH, SearchSpaceUpperBound, Height, PackingThreshold) ->
	case pick_recall_byte(H0, PrevH, SearchSpaceUpperBound) of
		{error, weave_size_too_small} ->
			#poa{};
		{ok, RecallByte} ->
			Packing =
				case Height >= ar_fork:height_2_5() andalso RecallByte >= PackingThreshold of
					true ->
						spora_2_5;
					false ->
						unpacked
				end,
			Options = #{ pack => true, packing => Packing },
			case ar_data_sync:get_chunk(RecallByte + 1, Options) of
				{ok, #{ chunk := Chunk, tx_path := TXPath, data_path := DataPath }} ->
					#poa{ option = 1, chunk = Chunk, tx_path = TXPath, data_path = DataPath };
				_ ->
					{not_found, RecallByte}
			end
	end.

log_spora_performance() ->
	[{_, StartedAt}] = ets:lookup(mining_state, started_at),
	Time = timer:now_diff(os:timestamp(), StartedAt),
	[{_, RecallBytes}] = ets:lookup(mining_state, recall_bytes_computed),
	[{_, BytesRead}] = ets:lookup(mining_state, bytes_read),
	KiBs = BytesRead / 1024,
	[{_, SPoRAs}] = ets:lookup(mining_state, sporas),
	RecallByteRate = RecallBytes / (Time / 1000000),
	Rate = SPoRAs / (Time / 1000000),
	ReadRate = KiBs / 1024 / (Time / 1000000),
	prometheus_histogram:observe(mining_rate, Rate),
	?LOG_INFO([{event, stopped_mining}, {recall_bytes_computed, RecallByteRate},
			{miner_sporas_per_second, Rate}, {miner_read_mibibytes_per_second, ReadRate},
			{round_time_seconds, Time div 1000000}]),
	ar:console("Miner spora rate: ~B h/s, recall bytes computed/s: ~B, MiB/s read: ~B,"
			" the round lasted ~B seconds.~n",
			[trunc(Rate), trunc(RecallByteRate), trunc(ReadRate), Time div 1000000]).

process_spora_solution(BDS, B, MinedTXs, S) ->
	#state{
		current_block = #block{ indep_hash = CurrentBH }
	} = S,
	SPoA = B#block.poa,
	IndepHash = ar_weave:indep_hash(BDS, B#block.hash, B#block.nonce, SPoA),
	B2 = B#block{ indep_hash = IndepHash },
	ar_events:send(block, {mined, B2, MinedTXs, CurrentBH}),
	log_spora_performance().

prepare_randomx(Height) ->
	case ar_randomx_state:randomx_state_by_height(Height) of
		{state, {fast, FastState}} ->
			%% Use RandomX fast-mode, where hashing is fast but initialization is slow.
			StageOneHasher =
				fun(Nonce1, Nonce2, BDS, PrevH, UpperBound, PIDs, ProxyPIDs, Iterations, JIT,
							LargePages, HardwareAES, Ref) ->
					case Height >= ar_fork:height_2_5() of
						true ->
							ar_mine_randomx:bulk_hash_fast_long_with_entropy(FastState, Nonce1,
									Nonce2, BDS, PrevH, UpperBound, PIDs, ProxyPIDs, Iterations,
									JIT, LargePages, HardwareAES, Ref);
						false ->
							ar_mine_randomx:bulk_hash_fast(FastState, Nonce1, Nonce2, BDS, PrevH,
									UpperBound, PIDs, ProxyPIDs, Iterations, JIT, LargePages,
									HardwareAES, Ref)
					end
				end,
			StageTwoHasher =
				fun(Diff, Preimage) ->
					ar_mine_randomx:hash_fast_verify(FastState, Diff, Preimage)
				end,
			{ok, {StageOneHasher, StageTwoHasher}};
		{state, {light, _}} ->
			not_found;
		{key, _} ->
			not_found
	end.

pick_recall_byte(H, PrevH, SearchSpaceUpperBound) ->
	Subspaces = ?SPORA_SEARCH_SPACE_SUBSPACES_COUNT,
	SubspaceNumber = binary:decode_unsigned(H, big) rem Subspaces,
	EvenSubspaceSize = SearchSpaceUpperBound div Subspaces,
	SearchSubspaceSize = ?SPORA_SEARCH_SPACE_SIZE(SearchSpaceUpperBound) div Subspaces,
	case SearchSubspaceSize of
		0 ->
			{error, weave_size_too_small};
		_ ->
			SubspaceStart = SubspaceNumber * EvenSubspaceSize,
			SubspaceSize = min(SearchSpaceUpperBound - SubspaceStart, EvenSubspaceSize),
			EncodedSubspaceNumber = binary:encode_unsigned(SubspaceNumber),
			SearchSubspaceSeed =
				binary:decode_unsigned(
					crypto:hash(sha256, << PrevH/binary, EncodedSubspaceNumber/binary >>),
					big
				),
			SearchSubspaceStart = SearchSubspaceSeed rem SubspaceSize,
			SearchSubspaceByteSeed = binary:decode_unsigned(crypto:hash(sha256, H), big),
			SearchSubspaceByte = SearchSubspaceByteSeed rem SearchSubspaceSize,
			{ok, SubspaceStart + (SearchSubspaceStart + SearchSubspaceByte) rem SubspaceSize}
	end.

spora_h0_with_entropy(BDS, Nonce, Height) ->
	HashData = << Nonce/binary, BDS/binary >>,
	true = Height >= ar_fork:height_1_7(),
	case Height >= ar_fork:height_2_5() of
		true ->
			ar_randomx_state:hash_long_with_entropy(Height, HashData);
		false ->
			{ar_randomx_state:hash(Height, HashData), no_entropy}
	end.

spora_solution_hash(PrevH, Timestamp, H0, Chunk, Height) ->
	ar_randomx_state:hash(Height, << H0/binary, PrevH/binary,
			Timestamp:(?TIMESTAMP_FIELD_SIZE_LIMIT * 8), Chunk/binary >>).

spora_solution_hash_with_entropy(PrevH, Timestamp, H0, Chunk, Entropy, Height) ->
	ar_randomx_state:hash(Height, << H0/binary, PrevH/binary,
			Timestamp:(?TIMESTAMP_FIELD_SIZE_LIMIT * 8), Chunk/binary, Entropy/binary >>).

-ifdef(DEBUG).
min_randomx_difficulty() -> 1.
-else.
min_randomx_difficulty() -> min_sha384_difficulty() + ?RANDOMX_DIFF_ADJUSTMENT.
min_sha384_difficulty() -> 31.
-endif.

min_spora_difficulty(Height) ->
	?SPORA_MIN_DIFFICULTY(Height).

%%%===================================================================
%%% Tests.
%%%===================================================================

%% Test that found nonces abide by the difficulty criteria.
basic_test_() ->
	{timeout, 20, fun test_basic/0}.

test_basic() ->
	[B0] = ar_weave:init([]),
	ar_test_node:start(B0),
	ar_node:mine(),
	BI = ar_test_node:wait_until_height(1),
	B1 = ar_test_node:read_block_when_stored(hd(BI)),
	Threads = maps:get(io_threads, sys:get_state(ar_node_worker)),
	ar_events:subscribe(block),
	start({B1, [], unclaimed, [], [], #{}, ar_mine:get_search_space_upper_bound(BI), BI,
			Threads}),
	assert_mine_output(B1, []).

%% Ensure that the block timestamp gets updated regularly while mining.
timestamp_refresh_test_() ->
	{timeout, 60, fun test_timestamp_refresh/0}.

test_timestamp_refresh() ->
	%% Start mining with a high enough difficulty, so that the block
	%% timestamp gets refreshed at least once. Since we might be unlucky
	%% and find the block too fast, we retry until it succeeds.
	[B0] = ar_weave:init([], ar_retarget:switch_to_linear_diff(18)),
	ar_test_node:start(B0),
	B = B0,
	ar_events:subscribe(block),
	Threads = maps:get(io_threads, sys:get_state(ar_node_worker)),
	Run = fun(_) ->
		TXs = [],
		StartTime = os:system_time(seconds),
		ar_events:subscribe(block),
		start({B, TXs, unclaimed, [], [], #{}, B0#block.weave_size,
				[ar_util:block_index_entry_from_block(B0)],Threads}),
		{_, MinedTimestamp} = assert_mine_output(B, TXs),
		MinedTimestamp > StartTime + ?MINING_TIMESTAMP_REFRESH_INTERVAL
	end,
	?assert(lists:any(Run, lists:seq(1, 20))).

%% @doc Ensures ar_mine can be started and stopped.
start_stop_test_() ->
	{timeout, 20, fun test_start_stop/0}.

test_start_stop() ->
	[B] = ar_weave:init(),
	{_Node, _} = ar_test_node:start(B),
	BI = ar_test_node:wait_until_height(0),
	HighDiff = ar_retarget:switch_to_linear_diff(30),
	Threads = maps:get(io_threads, sys:get_state(ar_node_worker)),
	PID = start({B#block{ diff = HighDiff }, [], unclaimed, [], [], #{},
			ar_mine:get_search_space_upper_bound(BI), BI, Threads}),
	timer:sleep(500),
	assert_alive(PID),
	stop(PID),
	assert_not_alive(PID, 3000).

assert_mine_output(B, TXs) ->
	receive
		{event, block, {mined, NewB, MinedTXs, BH}} ->
			?assertEqual(BH, B#block.indep_hash),
			?assertEqual(lists:sort(TXs), lists:sort(MinedTXs)),
			BDS = ar_block:generate_block_data_segment(NewB),
			#block{ height = Height, previous_block = PrevH, timestamp = Timestamp,
					nonce = Nonce, poa = #poa{ chunk = Chunk } } = NewB,
			{H0, _Entropy} = spora_h0_with_entropy(BDS, Nonce, Height),
			?assertEqual(spora_solution_hash(PrevH, Timestamp, H0, Chunk, Height),
					NewB#block.hash),
			?assert(binary:decode_unsigned(NewB#block.hash) > NewB#block.diff),
			{NewB#block.diff, NewB#block.timestamp};
		{event, block, {new, _NewB, _FromPeerID}} ->
			assert_mine_output(B, TXs)
	after 20000 ->
		error(timeout)
	end.

assert_alive(PID) ->
	?assert(is_process_alive(PID)).

assert_not_alive(PID, Timeout) ->
	Do = fun () -> not is_process_alive(PID) end,
	?assert(ar_util:do_until(Do, 50, Timeout)).
