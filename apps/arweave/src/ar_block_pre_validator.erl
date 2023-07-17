-module(ar_block_pre_validator).

-behaviour(gen_server).

-export([start_link/2, pre_validate/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-record(state, {
	%% The priority queue storing the validation requests.
	pqueue = gb_sets:new(),
	%% The total size in bytes of the priority queue.
	size = 0,
	%% The map IP => the timestamp of the last block from this IP.
	ip_timestamps = #{},
	throttle_by_ip_interval,
	%% The map SolutionHash => the timestamp of the last block with this solution hash.
	hash_timestamps = #{},
	throttle_by_solution_interval
}).

%% The maximum size in bytes the blocks enqueued for pre-validation can occupy.
-define(MAX_PRE_VALIDATION_QUEUE_SIZE, (200 * 1024 * 1024)).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Name, Workers) ->
	gen_server:start_link({local, Name}, ?MODULE, Workers, []).

%% @doc Partially validate the received block. The validation consists of multiple
%% stages. The process is aiming to increase resistance against DDoS attacks.
%% The first stage is the quickest and performed synchronously when this function
%% is called. Afterwards, the block is put in a limited-size priority queue.
%% Bigger-height blocks from better-rated peers have higher priority. Additionally,
%% the processing is throttled by IP and solution hash.
%% Returns: ok, invalid, skipped
pre_validate(B, Peer, ReceiveTimestamp) ->
	#block{ indep_hash = H } = B,
	ValidationStatus = case ar_ignore_registry:member(H) of
		true ->
			skipped;
		false ->
			pre_validate_is_peer_banned(B, Peer)
	end,
	case ValidationStatus of
		ok -> record_block_pre_validation_time(ReceiveTimestamp);
		_ -> ok
	end,
	ValidationStatus.


%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	gen_server:cast(?MODULE, pre_validate),
	ok = ar_events:subscribe(block),
	{ok, Config} = application:get_env(arweave, config),
	ThrottleBySolutionInterval = Config#config.block_throttle_by_solution_interval,
	ThrottleByIPInterval = Config#config.block_throttle_by_ip_interval,
	{ok, #state{ throttle_by_ip_interval = ThrottleByIPInterval,
			throttle_by_solution_interval = ThrottleBySolutionInterval }}.

handle_cast(pre_validate, #state{ pqueue = Q, size = Size, ip_timestamps = IPTimestamps,
			hash_timestamps = HashTimestamps,
			throttle_by_ip_interval = ThrottleByIPInterval,
			throttle_by_solution_interval = ThrottleBySolutionInterval } = State) ->
	case gb_sets:is_empty(Q) of
		true ->
			ar_util:cast_after(50, ?MODULE, pre_validate),
			{noreply, State};
		false ->
			{{_, {B, PrevB, SolutionResigned, Peer}},
					Q2} = gb_sets:take_largest(Q),
			BlockSize = byte_size(term_to_binary(B)),				
			Size2 = Size - BlockSize,
			case ar_ignore_registry:permanent_member(B#block.indep_hash) of
				true ->
					gen_server:cast(?MODULE, pre_validate),
					{noreply, State#state{ pqueue = Q2, size = Size2 }};
				false ->
					ThrottleByIPResult = throttle_by_ip(Peer, IPTimestamps,
							ThrottleByIPInterval),
					{IPTimestamps3, HashTimestamps3} =
						case ThrottleByIPResult of
							false ->
								{IPTimestamps, HashTimestamps};
							{true, IPTimestamps2} ->
								case throttle_by_solution_hash(B#block.hash, HashTimestamps,
										ThrottleBySolutionInterval) of
									{true, HashTimestamps2} ->
										Info = B#block.nonce_limiter_info,
										StepNumber
											= Info#nonce_limiter_info.global_step_number,
										?LOG_INFO([{event, processing_block},
												{peer, ar_util:format_peer(Peer)},
												{height, B#block.height},
												{step_number, StepNumber},
												{block, ar_util:encode(B#block.indep_hash)},
												{miner_address,
														ar_util:encode(B#block.reward_addr)},
												{previous_block,
													ar_util:encode(PrevB#block.indep_hash)}]),
										pre_validate_nonce_limiter_seed_data(B, PrevB,
												SolutionResigned, Peer),
										{IPTimestamps2, HashTimestamps2};
									false ->
										{IPTimestamps2, HashTimestamps}
								end
						end,
					gen_server:cast(?MODULE, pre_validate),
					{noreply, State#state{ pqueue = Q2, size = Size2,
							ip_timestamps = IPTimestamps3, hash_timestamps = HashTimestamps3 }}
			end
	end;

handle_cast({enqueue, {B, PrevB, SolutionResigned, Peer}},
		State) ->
	#state{ pqueue = Q, size = Size } = State,
	Priority = priority(B, Peer),
	BlockSize = byte_size(term_to_binary(B)),
	Size2 = Size + BlockSize,
	Q2 = gb_sets:add_element({Priority, {B, PrevB, SolutionResigned, Peer}}, Q),
	{Q3, Size3} =
		case Size2 > ?MAX_PRE_VALIDATION_QUEUE_SIZE of
			true ->
				drop_tail(Q2, Size2);
			false ->
				{Q2, Size2}
		end,
	{noreply, State#state{ pqueue = Q3, size = Size3 }};

handle_cast({may_be_remove_ip_timestamp, IP}, #state{ ip_timestamps = Timestamps,
		throttle_by_ip_interval = ThrottleInterval } = State) ->
	Now = os:system_time(millisecond),
	case maps:get(IP, Timestamps, not_set) of
		not_set ->
			{noreply, State};
		Timestamp when Timestamp < Now - ThrottleInterval ->
			{noreply, State#state{ ip_timestamps = maps:remove(IP, Timestamps) }};
		_ ->
			{noreply, State}
	end;

handle_cast({may_be_remove_h_timestamp, H}, #state{ hash_timestamps = Timestamps,
		throttle_by_solution_interval = ThrottleInterval } = State) ->
	Now = os:system_time(millisecond),
	case maps:get(H, Timestamps, not_set) of
		not_set ->
			{noreply, State};
		Timestamp when Timestamp < Now - ThrottleInterval ->
			{noreply, State#state{ hash_timestamps = maps:remove(H, Timestamps) }};
		_ ->
			{noreply, State}
	end;

handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_info({event, block, _}, State) ->
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

pre_validate_is_peer_banned(B, Peer) ->
	case ar_blacklist_middleware:is_peer_banned(Peer) of
		not_banned ->
			pre_validate_previous_block(B, Peer);
		banned ->
			skipped
	end.

pre_validate_previous_block(B, Peer) ->
	PrevH = B#block.previous_block,
	case ar_node:get_block_shadow_from_cache(PrevH) of
		not_found ->
			%% We have not seen the previous block yet - might happen if two
			%% successive blocks are distributed at the same time. Do not
			%% ban the peer as the block might be valid. If the network adopts
			%% this block, ar_poller will catch up.
			skipped;
		#block{ height = PrevHeight } = PrevB ->
			case B#block.height == PrevHeight + 1 of
				false ->
					invalid;
				true ->
					case B#block.height >= ar_fork:height_2_6() of
						true ->
							PrevCDiff = B#block.previous_cumulative_diff,
							case PrevB#block.cumulative_diff == PrevCDiff of
								true ->
									pre_validate_indep_hash(B, PrevB, Peer);
								false ->
									invalid
							end;
						false ->
							pre_validate_may_be_fetch_chunk(B, PrevB, Peer)
					end
			end
	end.

pre_validate_indep_hash(#block{ indep_hash = H } = B, PrevB, Peer) ->
	case catch compute_hash(B, PrevB#block.cumulative_diff) of
		{ok, {BDS, H}} ->
			ar_ignore_registry:add_temporary(H, 5000),
			pre_validate_timestamp(B, BDS, PrevB, Peer);
		{ok, H} ->
			case ar_ignore_registry:permanent_member(H) of
				true ->
					skipped;
				false ->
					ar_ignore_registry:add_temporary(H, 5000),
					pre_validate_timestamp(B, none, PrevB, Peer)
			end;
		{error, invalid_signature} ->
			post_block_reject_warn(B, check_signature, Peer),
			ar_events:send(block, {rejected, invalid_signature, B#block.indep_hash, Peer}),
			invalid;
		{ok, _DifferentH} ->
			post_block_reject_warn(B, check_indep_hash, Peer),
			ar_events:send(block, {rejected, invalid_hash, B#block.indep_hash, Peer}),
			invalid
	end.

pre_validate_timestamp(B, BDS, PrevB, Peer) ->
	#block{ indep_hash = H } = B,
	case ar_block:verify_timestamp(B, PrevB) of
		true ->
			pre_validate_existing_solution_hash(B, BDS, PrevB, Peer);
		false ->
			post_block_reject_warn(B, check_timestamp, Peer, [{block_time,
					B#block.timestamp}, {current_time, os:system_time(seconds)}]),
			ar_events:send(block, {rejected, invalid_timestamp, H, Peer}),
			ar_ignore_registry:remove_temporary(B#block.indep_hash),
			invalid
	end.

pre_validate_existing_solution_hash(B, BDS, PrevB, Peer) ->
	case B#block.height >= ar_fork:height_2_6() of
		false ->
			pre_validate_last_retarget(B, BDS, PrevB, false, Peer);
		true ->
			SolutionH = B#block.hash,
			#block{ hash = SolutionH, nonce = Nonce, reward_addr = RewardAddr,
					hash_preimage = HashPreimage, recall_byte = RecallByte,
					partition_number = PartitionNumber, recall_byte2 = RecallByte2,
					nonce_limiter_info = #nonce_limiter_info{ output = Output,
							global_step_number = StepNumber, seed = Seed,
							partition_upper_bound = UpperBound,
							last_step_checkpoints = LastStepCheckpoints } } = B,
			Fork_2_6 = ar_fork:height_2_6(),
			H = B#block.indep_hash,
			CDiff = B#block.cumulative_diff,
			PrevCDiff = PrevB#block.cumulative_diff,
			GetCachedSolution =
				case ar_block_cache:get_by_solution_hash(block_cache, SolutionH, H,
						CDiff, PrevCDiff) of
					not_found ->
						not_found;
					#block{ height = Height, hash = SolutionH, nonce = Nonce,
							reward_addr = RewardAddr, poa = PoA, hash_preimage = HashPreimage,
							recall_byte = RecallByte, partition_number = PartitionNumber,
							nonce_limiter_info = #nonce_limiter_info{ output = Output,
									last_step_checkpoints = LastStepCheckpoints,
									seed = Seed, partition_upper_bound = UpperBound,
									global_step_number = StepNumber },
							poa2 = PoA2, recall_byte2 = RecallByte2 } = CacheB
								when Height >= Fork_2_6 ->
						may_be_report_double_signing(B, CacheB),
						LastStepPrevOutput = get_last_step_prev_output(B),
						LastStepPrevOutput2 = get_last_step_prev_output(CacheB),
						case LastStepPrevOutput == LastStepPrevOutput2 of
							true ->
								{valid, B#block{ poa = PoA, poa2 = PoA2 }};
							false ->
								invalid
						end;
					_ ->
						invalid
				end,
			ValidatedCachedSolutionDiff =
				case GetCachedSolution of
					not_found ->
						not_found;
					invalid ->
						invalid;
					{valid, B2} ->
						case binary:decode_unsigned(SolutionH, big) > B#block.diff of
							true ->
								{valid, B2};
							false ->
								invalid
						end
				end,
			case ValidatedCachedSolutionDiff of
				not_found ->
					pre_validate_nonce_limiter_global_step_number(B, BDS, PrevB, false, Peer);
				invalid ->
					post_block_reject_warn(B, check_resigned_solution_hash, Peer),
					ar_events:send(block, {rejected, invalid_resigned_solution_hash,
							B#block.indep_hash, Peer}),
					invalid;
				{valid, B3} ->
					pre_validate_nonce_limiter_global_step_number(B3, BDS, PrevB, true, Peer)
			end
	end.

may_be_report_double_signing(B, B2) ->
	#block{ reward_key = {_, Key}, signature = Signature1, cumulative_diff = CDiff1,
			previous_solution_hash = PreviousSolutionH1,
			previous_cumulative_diff = PrevCDiff } = B,
	#block{ reward_key = {_, Key}, signature = Signature2, cumulative_diff = CDiff2,
			previous_cumulative_diff = PrevCDiff2,
			previous_solution_hash = PreviousSolutionH2 } = B2,
	case CDiff1 == CDiff2 orelse (CDiff1 > PrevCDiff2 andalso CDiff2 > PrevCDiff) of
		true ->
			Preimage1 = << PreviousSolutionH1/binary,
					(ar_block:generate_signed_hash(B))/binary >>,
			Preimage2 = << PreviousSolutionH2/binary,
					(ar_block:generate_signed_hash(B2))/binary >>,
			Proof = {Key, Signature1, CDiff1, PrevCDiff, Preimage1, Signature2, CDiff2,
					PrevCDiff2, Preimage2},
			ar_events:send(block, {double_signing, Proof});
		false ->
			ok
	end.

get_last_step_prev_output(B) ->
	#block{ nonce_limiter_info = Info } = B,
	#nonce_limiter_info{ steps = Steps, prev_output = PrevOutput } = Info,
	case Steps of
		[_, PrevStepOutput | _] ->
			PrevStepOutput;
		_ ->
			PrevOutput
	end.

pre_validate_nonce_limiter_global_step_number(B, BDS, PrevB, SolutionResigned, Peer) ->
	BlockInfo = B#block.nonce_limiter_info,
	StepNumber = BlockInfo#nonce_limiter_info.global_step_number,
	PrevBlockInfo = PrevB#block.nonce_limiter_info,
	PrevStepNumber = PrevBlockInfo#nonce_limiter_info.global_step_number,
	CurrentStepNumber =
		case ar_nonce_limiter:get_current_step_number(PrevB) of
			not_found ->
				%% Not necessarily computed already, but will be after we
				%% validate the previous block's chain.
				PrevStepNumber;
			N ->
				N
		end,
	IsAhead = ar_nonce_limiter:is_ahead_on_the_timeline(BlockInfo, PrevBlockInfo),
	MaxDistance = ?NONCE_LIMITER_MAX_CHECKPOINTS_COUNT,
	ExpectedStepCount = min(MaxDistance, StepNumber - PrevStepNumber),
	Steps = BlockInfo#nonce_limiter_info.steps,
	PrevOutput = BlockInfo#nonce_limiter_info.prev_output,
	case IsAhead andalso StepNumber - CurrentStepNumber =< MaxDistance
			andalso length(Steps) == ExpectedStepCount
			andalso PrevOutput == PrevBlockInfo#nonce_limiter_info.output of
		false ->
			post_block_reject_warn(B, check_nonce_limiter_step_number, Peer,
					[{block_step_number, StepNumber},
					{current_step_number, CurrentStepNumber}]),
			H = B#block.indep_hash,
			ar_events:send(block,
					{rejected, invalid_nonce_limiter_global_step_number, H, Peer}),
			ar_ignore_registry:remove_temporary(B#block.indep_hash),
			invalid;
		true ->
			prometheus_gauge:set(block_vdf_advance, StepNumber - CurrentStepNumber),
			pre_validate_previous_solution_hash(B, BDS, PrevB, SolutionResigned, Peer)
	end.

pre_validate_previous_solution_hash(B, BDS, PrevB, SolutionResigned, Peer) ->
	case B#block.previous_solution_hash == PrevB#block.hash of
		false ->
			post_block_reject_warn(B, check_previous_solution_hash, Peer),
			ar_events:send(block, {rejected, invalid_previous_solution_hash,
					B#block.indep_hash, Peer}),
			invalid;
		true ->
			pre_validate_last_retarget(B, BDS, PrevB, SolutionResigned, Peer)
	end.

pre_validate_last_retarget(B, BDS, PrevB, SolutionResigned, Peer) ->
	case B#block.height >= ar_fork:height_2_6() of
		false ->
			pre_validate_difficulty(B, BDS, PrevB, SolutionResigned, Peer);
		true ->
			case ar_block:verify_last_retarget(B, PrevB) of
				true ->
					pre_validate_difficulty(B, BDS, PrevB, SolutionResigned, Peer);
				false ->
					post_block_reject_warn(B, check_last_retarget, Peer),
					ar_events:send(block, {rejected, invalid_last_retarget,
							B#block.indep_hash, Peer}),
					invalid
			end
	end.

pre_validate_difficulty(B, BDS, PrevB, SolutionResigned, Peer) ->
	DiffValid =
		case B#block.height >= ar_fork:height_2_6() of
			true ->
				ar_retarget:validate_difficulty(B, PrevB);
			false ->
				B#block.diff >= ar_mine:min_difficulty(B#block.height)
		end,
	case DiffValid of
		true ->
			pre_validate_cumulative_difficulty(B, BDS, PrevB, SolutionResigned, Peer);
		_ ->
			post_block_reject_warn(B, check_difficulty, Peer),
			ar_events:send(block, {rejected, invalid_difficulty, B#block.indep_hash, Peer}),
			invalid
	end.

pre_validate_cumulative_difficulty(B, BDS, PrevB, SolutionResigned, Peer) ->
	case B#block.height >= ar_fork:height_2_6() of
		true ->
			case ar_block:verify_cumulative_diff(B, PrevB) of
				false ->
					post_block_reject_warn(B, check_cumulative_difficulty, Peer),
					ar_events:send(block, {rejected, invalid_cumulative_difficulty,
							B#block.indep_hash, Peer}),
					invalid;
				true ->
					case SolutionResigned of
						true ->
							gen_server:cast(?MODULE, {enqueue, {B, PrevB, true, Peer}}),
							ok;
						false ->
							pre_validate_quick_pow(B, PrevB, false, Peer)
					end
			end;
		false ->
			pre_validate_pow(B, BDS, PrevB, Peer)
	end.

pre_validate_quick_pow(B, PrevB, SolutionResigned, Peer) ->
	#block{ hash_preimage = HashPreimage, diff = Diff, nonce_limiter_info = NonceLimiterInfo,
			partition_number = PartitionNumber, reward_addr = RewardAddr } = B,
	PrevNonceLimiterInfo = get_prev_nonce_limiter_info(PrevB),
	SolutionHash =
		case PrevNonceLimiterInfo of
			not_found ->
				not_found;
			_ ->
				Seed = PrevNonceLimiterInfo#nonce_limiter_info.seed,
				NonceLimiterOutput = NonceLimiterInfo#nonce_limiter_info.output,
				H0 = ar_block:compute_h0(NonceLimiterOutput, PartitionNumber, Seed,
						RewardAddr),
				ar_block:compute_solution_h(H0, HashPreimage)
		end,
	case SolutionHash of
		not_found ->
			%% The new blocks should have been applied in the meantime since we
			%% looked for the previous block in the block cache.
			skipped;
		_ ->
			case binary:decode_unsigned(SolutionHash, big) > Diff of
				false ->
					post_block_reject_warn(B, check_hash_preimage, Peer),
					ar_events:send(block, {rejected, invalid_hash_preimage,
							B#block.indep_hash, Peer}),
					invalid;
				true ->
					gen_server:cast(?MODULE, {enqueue, {B, PrevB, SolutionResigned, Peer}}),
					ok
			end
	end.

get_prev_nonce_limiter_info(#block{ indep_hash = PrevH, height = PrevHeight } = PrevB) ->
	Fork_2_6 = ar_fork:height_2_6(),
	true = PrevHeight + 1 >= Fork_2_6,
	case PrevHeight + 1 == Fork_2_6 of
		true ->
			case ar_node:get_recent_partition_upper_bound_by_prev_h(PrevH) of
				not_found ->
					not_found;
				{Seed, PartitionUpperBound} ->
					Output = crypto:hash(sha256, Seed),
					#nonce_limiter_info{ output = Output, seed = Seed, next_seed = PrevH,
							partition_upper_bound = PartitionUpperBound,
							next_partition_upper_bound = PrevB#block.weave_size }
			end;
		false ->
			PrevB#block.nonce_limiter_info
	end.

pre_validate_nonce_limiter_seed_data(B, PrevB, SolutionResigned, Peer) ->
	Info = B#block.nonce_limiter_info,
	#nonce_limiter_info{ global_step_number = StepNumber, seed = Seed,
			next_seed = NextSeed, partition_upper_bound = PartitionUpperBound,
			next_partition_upper_bound = NextPartitionUpperBound } = Info,
	StepNumber = (B#block.nonce_limiter_info)#nonce_limiter_info.global_step_number,
	case get_prev_nonce_limiter_info(PrevB) of
		not_found ->
			%% The new blocks should have been applied in the meantime since we
			%% looked for the previous block in the block cache.
			skipped;
		PrevNonceLimiterInfo ->
			ExpectedSeedData = ar_nonce_limiter:get_seed_data(StepNumber, PrevNonceLimiterInfo,
					PrevB#block.indep_hash, PrevB#block.weave_size),
			case ExpectedSeedData == {Seed, NextSeed, PartitionUpperBound,
					NextPartitionUpperBound} of
				true ->
					pre_validate_partition_number(B, PrevB, PartitionUpperBound,
							SolutionResigned, Peer);
				false ->
					post_block_reject_warn(B, check_nonce_limiter_seed_data, Peer),
					ar_events:send(block, {rejected, invalid_nonce_limiter_seed_data,
							B#block.indep_hash, Peer}),
					invalid
			end
	end.

pre_validate_partition_number(B, PrevB, PartitionUpperBound, SolutionResigned, Peer) ->
	Max = max(0, PartitionUpperBound div ?PARTITION_SIZE - 1),
	case B#block.partition_number > Max of
		true ->
			post_block_reject_warn(B, check_partition_number, Peer),
			ar_events:send(block, {rejected, invalid_partition_number, B#block.indep_hash,
					Peer}),
			invalid;
		false ->
			pre_validate_nonce(B, PrevB, PartitionUpperBound, SolutionResigned, Peer)
	end.

pre_validate_nonce(B, PrevB, PartitionUpperBound, SolutionResigned, Peer) ->
	Max = max(0, (?RECALL_RANGE_SIZE) div ?DATA_CHUNK_SIZE - 1),
	case B#block.nonce > Max of
		true ->
			post_block_reject_warn(B, check_nonce, Peer),
			ar_events:send(block, {rejected, invalid_nonce, B#block.indep_hash, Peer}),
			invalid;
		false ->
			case SolutionResigned of
				true ->
					accept_block(B, Peer, false);
				false ->
					pre_validate_may_be_fetch_first_chunk(B, PrevB, PartitionUpperBound, Peer)
			end
	end.

pre_validate_may_be_fetch_first_chunk(#block{ recall_byte = RecallByte,
		poa = #poa{ chunk = <<>> } } = B, PrevB, PartitionUpperBound, Peer)
			when RecallByte /= undefined ->
	case ar_data_sync:get_chunk(RecallByte + 1, #{ pack => true,
			packing => {spora_2_6, B#block.reward_addr}, bucket_based_offset => true }) of
		{ok, #{ chunk := Chunk, data_path := DataPath, tx_path := TXPath }} ->
			prometheus_counter:inc(block2_fetched_chunks),
			B2 = B#block{ poa = #poa{ chunk = Chunk, data_path = DataPath,
					tx_path = TXPath } },
			pre_validate_may_be_fetch_second_chunk(B2, PrevB, PartitionUpperBound,
					Peer);
		_ ->
			ar_events:send(block, {rejected, failed_to_fetch_first_chunk, B#block.indep_hash,
					Peer}),
			invalid
	end;
pre_validate_may_be_fetch_first_chunk(B, PrevB, PartitionUpperBound, Peer) ->
	pre_validate_may_be_fetch_second_chunk(B, PrevB, PartitionUpperBound, Peer).

pre_validate_may_be_fetch_second_chunk(#block{ recall_byte2 = RecallByte2,
		poa2 = #poa{ chunk = <<>> } } = B, PrevB, PartitionUpperBound, Peer)
		  when RecallByte2 /= undefined ->
	case ar_data_sync:get_chunk(RecallByte2 + 1, #{ pack => true,
			packing => {spora_2_6, B#block.reward_addr}, bucket_based_offset => true }) of
		{ok, #{ chunk := Chunk, data_path := DataPath, tx_path := TXPath }} ->
			prometheus_counter:inc(block2_fetched_chunks),
			B2 = B#block{ poa2 = #poa{ chunk = Chunk, data_path = DataPath,
					tx_path = TXPath } },
			pre_validate_pow_2_6(B2, PrevB, PartitionUpperBound, Peer);
		_ ->
			ar_events:send(block, {rejected, failed_to_fetch_second_chunk, B#block.indep_hash,
					Peer}),
			invalid
	end;
pre_validate_may_be_fetch_second_chunk(B, PrevB, PartitionUpperBound, Peer) ->
	pre_validate_pow_2_6(B, PrevB, PartitionUpperBound, Peer).

pre_validate_pow_2_6(B, PrevB, PartitionUpperBound, Peer) ->
	NonceLimiterInfo = B#block.nonce_limiter_info,
	NonceLimiterOutput = NonceLimiterInfo#nonce_limiter_info.output,
	PrevNonceLimiterInfo = get_prev_nonce_limiter_info(PrevB),
	Seed = PrevNonceLimiterInfo#nonce_limiter_info.seed,
	H0 = ar_block:compute_h0(NonceLimiterOutput, B#block.partition_number, Seed,
			B#block.reward_addr),
	Chunk1 = (B#block.poa)#poa.chunk,
	{H1, Preimage1} = ar_block:compute_h1(H0, B#block.nonce, Chunk1),
	case H1 == B#block.hash andalso binary:decode_unsigned(H1, big) > B#block.diff
			andalso Preimage1 == B#block.hash_preimage
			andalso B#block.recall_byte2 == undefined of
		true ->
			pre_validate_poa(B, PrevB, PartitionUpperBound, H0, H1, Peer);
		false ->
			Chunk2 = (B#block.poa2)#poa.chunk,
			{H2, Preimage2} = ar_block:compute_h2(H1, Chunk2, H0),
			case H2 == B#block.hash andalso binary:decode_unsigned(H2, big) > B#block.diff
					andalso Preimage2 == B#block.hash_preimage of
				true ->
					pre_validate_poa(B, PrevB, PartitionUpperBound, H0, H1, Peer);
				false ->
					post_block_reject_warn(B, check_pow, Peer),
					ar_events:send(block, {rejected, invalid_pow, B#block.indep_hash, Peer}),
					invalid
			end
	end.

pre_validate_poa(B, PrevB, PartitionUpperBound, H0, H1, Peer) ->
	{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
			B#block.partition_number, PartitionUpperBound),
	RecallByte1 = RecallRange1Start + B#block.nonce * ?DATA_CHUNK_SIZE,
	{BlockStart1, BlockEnd1, TXRoot1} = ar_block_index:get_block_bounds(RecallByte1),
	BlockSize1 = BlockEnd1 - BlockStart1,
	case ar_poa:validate({BlockStart1, RecallByte1, TXRoot1, BlockSize1, B#block.poa,
			?STRICT_DATA_SPLIT_THRESHOLD, {spora_2_6, B#block.reward_addr}})
				andalso RecallByte1 == B#block.recall_byte of
		error ->
			?LOG_ERROR([{event, failed_to_validate_proof_of_access},
					{block, ar_util:encode(B#block.indep_hash)}]),
			invalid;
		false ->
			post_block_reject_warn(B, check_poa, Peer),
			ar_events:send(block, {rejected, invalid_poa, B#block.indep_hash, Peer}),
			invalid;
		true ->
			case B#block.hash == H1 of
				true ->
					pre_validate_nonce_limiter(B, PrevB, Peer);
				false ->
					RecallByte2 = RecallRange2Start + B#block.nonce * ?DATA_CHUNK_SIZE,
					{BlockStart2, BlockEnd2, TXRoot2} = ar_block_index:get_block_bounds(
							RecallByte2),
					BlockSize2 = BlockEnd2 - BlockStart2,
					case ar_poa:validate({BlockStart2, RecallByte2, TXRoot2, BlockSize2,
							B#block.poa2, ?STRICT_DATA_SPLIT_THRESHOLD,
							{spora_2_6, B#block.reward_addr}})
								andalso RecallByte2 == B#block.recall_byte2 of
						error ->
							?LOG_ERROR([{event, failed_to_validate_proof_of_access},
									{block, ar_util:encode(B#block.indep_hash)}]),
							invalid;
						false ->
							post_block_reject_warn(B, check_poa2, Peer),
							ar_events:send(block, {rejected, invalid_poa2,
									B#block.indep_hash, Peer}),
							invalid;
						true ->
							pre_validate_nonce_limiter(B, PrevB, Peer)
					end
			end
	end.

pre_validate_nonce_limiter(B, PrevB, Peer) ->
	PrevOutput = get_last_step_prev_output(B),
	case ar_nonce_limiter:validate_last_step_checkpoints(B, PrevB, PrevOutput) of
		{false, cache_mismatch} ->
			ar_ignore_registry:add(B#block.indep_hash),
			post_block_reject_warn(B, check_nonce_limiter, Peer),
			ar_events:send(block, {rejected, invalid_nonce_limiter_cache_mismatch, B#block.indep_hash, Peer}),
			invalid;
		false ->
			post_block_reject_warn(B, check_nonce_limiter, Peer),
			ar_events:send(block, {rejected, invalid_nonce_limiter, B#block.indep_hash, Peer}),
			invalid;
		{true, cache_match} ->
			accept_block(B, Peer, true);
		true ->
			accept_block(B, Peer, false)
	end.

accept_block(B, Peer, Gossip) ->
	ar_ignore_registry:add(B#block.indep_hash),
	ar_events:send(block, {new, B, 
		#{ source => {peer, Peer}, gossip => Gossip }}),
	?LOG_INFO([{event, accepted_block}, {height, B#block.height},
			{indep_hash, ar_util:encode(B#block.indep_hash)}]),
	ok.

pre_validate_may_be_fetch_chunk(#block{ recall_byte = RecallByte,
		poa = #poa{ chunk = <<>> } } = B, PrevB, Peer) when RecallByte /= undefined ->
	Options = #{ pack => false, packing => spora_2_5, bucket_based_offset => true },
	case ar_data_sync:get_chunk(RecallByte + 1, Options) of
		{ok, #{ chunk := Chunk, data_path := DataPath, tx_path := TXPath }} ->
			prometheus_counter:inc(block2_fetched_chunks),
			B2 = B#block{ poa = #poa{ chunk = Chunk, tx_path = TXPath,
					data_path = DataPath } },
			pre_validate_indep_hash(B2, PrevB, Peer);
		_ ->
			ar_events:send(block, {rejected, failed_to_fetch_chunk, B#block.indep_hash, Peer}),
			invalid
	end;
pre_validate_may_be_fetch_chunk(B, PrevB, Peer) ->
	pre_validate_indep_hash(B, PrevB, Peer).

pre_validate_pow(B, BDS, PrevB, Peer) ->
	#block{ indep_hash = PrevH } = PrevB,
	MaybeValid =
		case ar_node:get_recent_partition_upper_bound_by_prev_h(PrevH) of
			not_found ->
				not_found;
			{_H, PartitionUpperBound} ->
				validate_spora_pow(B, PrevB, BDS, PartitionUpperBound)
		end,
	case MaybeValid of
		not_found ->
			%% The new blocks should have been applied in the meantime since we
			%% looked for the previous block in the block cache.
			skipped;
		{true, RecallByte} ->
			H = B#block.indep_hash,
			%% Include all transactions found in the mempool in place of the
			%% corresponding transaction identifiers so that we can gossip them to
			%% peers who miss them along with the block.
			B2 = B#block{ txs = include_transactions(B#block.txs) },
			ar_events:send(block, {new, B2, #{
				source => {peer, Peer}, recall_byte => RecallByte }}),
			prometheus_counter:inc(block2_received_transactions,
					count_received_transactions(B#block.txs)),
			?LOG_INFO([{event, accepted_block}, {indep_hash, ar_util:encode(H)}]),
			ok;
		false ->
			post_block_reject_warn(B, check_pow, Peer),
			ar_events:send(block, {rejected, invalid_pow, B#block.indep_hash, Peer}),
			invalid
	end.

compute_hash(B, PrevCDiff) ->
	case B#block.height >= ar_fork:height_2_6() of
		false ->
			BDS = ar_block:generate_block_data_segment(B),
			{ok, {BDS, ar_block:indep_hash(BDS, B)}};
		true ->
			SignedH = ar_block:generate_signed_hash(B),
			case ar_block:verify_signature(SignedH, PrevCDiff, B) of
				false ->
					{error, invalid_signature};
				true ->
					{ok, ar_block:indep_hash2(SignedH, B#block.signature)}
			end
	end.

include_transactions([#tx{} = TX | TXs]) ->
	[TX | include_transactions(TXs)];
include_transactions([]) ->
	[];
include_transactions([TXID | TXs]) ->
	case ar_mempool:get_tx(TXID) of
		not_found ->
			[TXID | include_transactions(TXs)];
		TX ->
			[TX | include_transactions(TXs)]
	end.

count_received_transactions(TXs) ->
	count_received_transactions(TXs, 0).

count_received_transactions([#tx{} | TXs], N) ->
	count_received_transactions(TXs, N + 1);
count_received_transactions([_ | TXs], N) ->
	count_received_transactions(TXs, N);
count_received_transactions([], N) ->
	N.

post_block_reject_warn(B, Step, Peer) ->
	?LOG_WARNING([{event, post_block_rejected},
			{hash, ar_util:encode(B#block.indep_hash)}, {step, Step},
			{peer, ar_util:format_peer(Peer)}]).

post_block_reject_warn(B, Step, Peer, Params) ->
	?LOG_WARNING([{event, post_block_rejected},
			{hash, ar_util:encode(B#block.indep_hash)}, {step, Step},
			{params, Params}, {peer, ar_util:format_peer(Peer)}]).

validate_spora_pow(B, PrevB, BDS, PartitionUpperBound) ->
	#block{ height = PrevHeight, indep_hash = PrevH } = PrevB,
	#block{ height = Height, nonce = Nonce, timestamp = Timestamp,
			poa = #poa{ chunk = Chunk } = SPoA } = B,
	Root = ar_block:compute_hash_list_merkle(PrevB),
	case {Root, PrevHeight + 1} == {B#block.hash_list_merkle, Height} of
		false ->
			false;
		true ->
			{H0, Entropy} = ar_mine:spora_h0_with_entropy(BDS, Nonce, Height),
			ComputeSolutionHash =
				case ar_mine:pick_recall_byte(H0, PrevH, PartitionUpperBound) of
					{error, weave_size_too_small} ->
						case SPoA == #poa{} of
							false ->
								invalid_poa;
							true ->
								{ar_mine:spora_solution_hash(PrevH, Timestamp, H0, Chunk,
										Height), undefined}
						end;
					{ok, Byte} ->
						{ar_mine:spora_solution_hash_with_entropy(PrevH, Timestamp, H0, Chunk,
								Entropy, Height), Byte}
				end,
			case ComputeSolutionHash of
				invalid_poa ->
					false;
				{{SolutionHash, HashPreimage}, RecallByte} ->
					case binary:decode_unsigned(SolutionHash, big) > B#block.diff
							andalso SolutionHash == B#block.hash
							%% In 2.5 blocks both are <<>>.
							andalso HashPreimage == B#block.hash_preimage of
						false ->
							false;
						true ->
							{true, RecallByte}
					end
			end
	end.

record_block_pre_validation_time(ReceiveTimestamp) ->
	TimeMs = timer:now_diff(erlang:timestamp(), ReceiveTimestamp) / 1000,
	prometheus_histogram:observe(block_pre_validation_time, TimeMs).

priority(B, Peer) ->
	{B#block.height, get_peer_score(Peer)}.

get_peer_score(Peer) ->
	get_peer_score(Peer, ar_peers:get_peers(), 0).

get_peer_score(Peer, [Peer | _Peers], N) ->
	N;
get_peer_score(Peer, [_Peer | Peers], N) ->
	get_peer_score(Peer, Peers, N - 1);
get_peer_score(_Peer, [], N) ->
	N - rand:uniform(100).

drop_tail(Q, Size) when Size =< ?MAX_PRE_VALIDATION_QUEUE_SIZE ->
	{Q, 0};
drop_tail(Q, Size) ->
	{{_Priority, {_B, _PrevB, _SolutionResigned, _Peer, _Timestamp, _ReadBodyTime,
			BodySize}}, Q2} = gb_sets:take_smallest(Q),
	drop_tail(Q2, Size - BodySize).

throttle_by_ip(Peer, Timestamps, ThrottleInterval) ->
	IP = get_ip(Peer),
	Now = os:system_time(millisecond),
	ar_util:cast_after(ThrottleInterval * 2, ?MODULE, {may_be_remove_ip_timestamp, IP}),
	case maps:get(IP, Timestamps, not_set) of
		not_set ->
			{true, maps:put(IP, Now, Timestamps)};
		Timestamp when Timestamp < Now - ThrottleInterval ->
			{true, maps:put(IP, Now, Timestamps)};
		_ ->
			false
	end.

get_ip({A, B, C, D, _Port}) ->
	{A, B, C, D}.

throttle_by_solution_hash(H, Timestamps, ThrottleInterval) ->
	Now = os:system_time(millisecond),
	ar_util:cast_after(ThrottleInterval * 2, ?MODULE, {may_be_remove_h_timestamp, H}),
	case maps:get(H, Timestamps, not_set) of
		not_set ->
			{true, maps:put(H, Now, Timestamps)};
		Timestamp when Timestamp < Now - ThrottleInterval ->
			{true, maps:put(H, Now, Timestamps)};
		_ ->
			false
	end.
