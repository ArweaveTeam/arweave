-module(ar_block).

-export([partition_size/0,
		get_replica_2_9_entropy_sector_size/0, get_replica_2_9_entropy_partition_size/0,
		get_sub_chunks_per_replica_2_9_entropy/0, get_replica_2_9_entropy_count/0,
		strict_data_split_threshold/0,
		block_field_size_limit/1, verify_timestamp/2, get_max_timestamp_deviation/0, verify_last_retarget/2,
		verify_weave_size/3, verify_cumulative_diff/2, verify_block_hash_list_merkle/2,
		compute_hash_list_merkle/1, compute_h0/2, compute_h0/5, compute_h0/6,
		compute_h1/3, compute_h2/3, compute_solution_h/2,
		indep_hash/1, indep_hash/2, indep_hash2/2, get_block_signature_preimage/4,
		generate_signed_hash/1, verify_signature/3, get_reward_key/2,
		generate_block_data_segment/1, generate_block_data_segment/2,
		generate_block_data_segment_base/1, get_recall_range/3, verify_tx_root/1,
		hash_wallet_list/1, generate_hash_list_for_block/2,
		generate_tx_root_for_block/1, generate_tx_root_for_block/2,
		generate_size_tagged_list_from_txs/2, generate_tx_tree/1, generate_tx_tree/2,
		test_wallet_list_performance/0, test_wallet_list_performance/1,
		test_wallet_list_performance/2, test_wallet_list_performance/3,
		poa_to_list/1, shift_packing_2_5_threshold/1,
		get_packing_threshold/2, compute_next_vdf_difficulty/1,
		validate_proof_size/1, vdf_step_number/1, get_packing/3,
		validate_replica_format/3,
		get_max_nonce/1, get_recall_range_size/1, get_recall_byte/3,
		get_sub_chunk_size/1, get_nonces_per_chunk/1, get_nonces_per_recall_range/1,
		get_sub_chunk_index/2,
		get_chunk_padded_offset/1, get_double_signing_condition/4]).

-include("ar.hrl").
-include("ar_consensus.hrl").
-include("ar_block.hrl").
-include("ar_vdf.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Expose constants through a function to allow mocking/injection in tests.
partition_size() -> ?PARTITION_SIZE.
strict_data_split_threshold() -> ?STRICT_DATA_SPLIT_THRESHOLD.

%% @doc Return the 2.9 entropy sector size - the largest total size in bytes of the contiguous
%% area where the 2.9 entropy of every chunk is unique.
-spec get_replica_2_9_entropy_sector_size() -> pos_integer().
get_replica_2_9_entropy_sector_size() ->
	?REPLICA_2_9_ENTROPY_COUNT * ?COMPOSITE_PACKING_SUB_CHUNK_SIZE.

%% @doc Return the size of the 2.9 entropy partition.
-spec get_replica_2_9_entropy_partition_size() -> pos_integer().
get_replica_2_9_entropy_partition_size() ->
	?REPLICA_2_9_ENTROPY_COUNT * ?REPLICA_2_9_ENTROPY_SIZE.

%% @doc Return the number of sub-chunks per entropy. We'll generally create 32x entropies
%% in order to fully encipher this many chunks.
-spec get_sub_chunks_per_replica_2_9_entropy() -> pos_integer().
get_sub_chunks_per_replica_2_9_entropy() ->
	?REPLICA_2_9_ENTROPY_SIZE div ?COMPOSITE_PACKING_SUB_CHUNK_SIZE.

%% @doc Return the number of entropies per partition.
-spec get_replica_2_9_entropy_count() -> pos_integer().
get_replica_2_9_entropy_count() ->
	?REPLICA_2_9_ENTROPY_COUNT div ?COMPOSITE_PACKING_SUB_CHUNK_COUNT.

%% @doc Check whether the block fields conform to the specified size limits.
block_field_size_limit(B = #block{ reward_addr = unclaimed }) ->
	block_field_size_limit(B#block{ reward_addr = <<>> });
block_field_size_limit(B) ->
	DiffBytesLimit =
		case ar_fork:height_1_8() of
			Height when B#block.height >= Height ->
				78;
			_ ->
				10
		end,
	{ChunkSize, DataPathSize} =
		case B#block.poa of
			POA when is_record(POA, poa) ->
				{
					byte_size((B#block.poa)#poa.chunk),
					byte_size((B#block.poa)#poa.data_path)
				};
			_ -> {0, 0}
		end,
	RewardAddrCheck = byte_size(B#block.reward_addr) =< 32,
	Check = (byte_size(B#block.nonce) =< 512) and
		(byte_size(B#block.previous_block) =< 48) and
		(byte_size(integer_to_binary(B#block.timestamp)) =< ?TIMESTAMP_FIELD_SIZE_LIMIT) and
		(byte_size(integer_to_binary(B#block.last_retarget))
				=< ?TIMESTAMP_FIELD_SIZE_LIMIT) and
		(byte_size(integer_to_binary(B#block.diff)) =< DiffBytesLimit) and
		(byte_size(integer_to_binary(B#block.height)) =< 20) and
		(byte_size(B#block.hash) =< 48) and
		(byte_size(B#block.indep_hash) =< 48) and
		RewardAddrCheck and
		validate_tags_size(B) and
		(byte_size(integer_to_binary(B#block.weave_size)) =< 64) and
		(byte_size(integer_to_binary(B#block.block_size)) =< 64) and
		(ChunkSize =< ?DATA_CHUNK_SIZE) and
		(DataPathSize =< ?MAX_PATH_SIZE),
	case Check of
		false ->
			?LOG_INFO(
				[
					{event, received_block_with_invalid_field_size},
					{nonce, byte_size(B#block.nonce)},
					{previous_block, byte_size(B#block.previous_block)},
					{timestamp, byte_size(integer_to_binary(B#block.timestamp))},
					{last_retarget, byte_size(integer_to_binary(B#block.last_retarget))},
					{diff, byte_size(integer_to_binary(B#block.diff))},
					{height, byte_size(integer_to_binary(B#block.height))},
					{hash, byte_size(B#block.hash)},
					{indep_hash, byte_size(B#block.indep_hash)},
					{reward_addr, byte_size(B#block.reward_addr)},
					{tags, byte_size(list_to_binary(B#block.tags))},
					{weave_size, byte_size(integer_to_binary(B#block.weave_size))},
					{block_size, byte_size(integer_to_binary(B#block.block_size))}
				]
			);
		_ ->
			ok
	end,
	Check.

%% @doc Verify the block timestamp is not too far in the future nor too far in
%% the past. We calculate the maximum reasonable clock difference between any
%% two nodes. This is a simplification since there is a chaining effect in the
%% network which we don't take into account. Instead, we assume two nodes can
%% deviate JOIN_CLOCK_TOLERANCE seconds in the opposite direction from each
%% other.
verify_timestamp(#block{ timestamp = Timestamp }, #block{ timestamp = PrevTimestamp }) ->
	MaxNodesClockDeviation = get_max_timestamp_deviation(),
	case Timestamp >= PrevTimestamp - MaxNodesClockDeviation of
		false ->
			false;
		true ->
			CurrentTime = os:system_time(seconds),
			Timestamp =< CurrentTime + MaxNodesClockDeviation
	end.

%% @doc Return the largest possible value by which the previous block's timestamp
%% may exceed the next block's timestamp.
get_max_timestamp_deviation() ->
	?JOIN_CLOCK_TOLERANCE * 2 + ?CLOCK_DRIFT_MAX.

%% @doc Verify the retarget timestamp on NewB is correct.
verify_last_retarget(NewB, OldB) ->
	case ar_retarget:is_retarget_height(NewB#block.height) of
		true ->
			NewB#block.last_retarget == NewB#block.timestamp;
		false ->
			NewB#block.last_retarget == OldB#block.last_retarget
	end.

%% @doc Verify the new weave size is computed correctly given the previous block
%% and the list of transactions of the new block.
verify_weave_size(NewB, OldB, TXs) ->
	BlockSize = lists:foldl(
		fun(TX, Acc) ->
			Acc + ar_tx:get_weave_size_increase(TX, NewB#block.height)
		end,
		0,
		TXs
	),
	(NewB#block.height < ar_fork:height_2_6() orelse BlockSize == NewB#block.block_size)
			andalso NewB#block.weave_size == OldB#block.weave_size + BlockSize.

%% @doc Verify the new cumulative difficulty is computed correctly.
verify_cumulative_diff(NewB, OldB) ->
	NewB#block.cumulative_diff ==
		ar_difficulty:next_cumulative_diff(
			OldB#block.cumulative_diff,
			NewB#block.diff,
			NewB#block.height
		).

%% @doc Verify the root of the new block tree is computed correctly.
verify_block_hash_list_merkle(NewB, CurrentB) ->
	true = NewB#block.height > ar_fork:height_2_0(),
	NewB#block.hash_list_merkle == ar_unbalanced_merkle:root(CurrentB#block.hash_list_merkle,
			{CurrentB#block.indep_hash, CurrentB#block.weave_size, CurrentB#block.tx_root},
			fun ar_unbalanced_merkle:hash_block_index_entry/1).

%% @doc Compute the root of the new block tree given the previous block.
compute_hash_list_merkle(B) ->
	ar_unbalanced_merkle:root(
		B#block.hash_list_merkle,
		{B#block.indep_hash, B#block.weave_size, B#block.tx_root},
		fun ar_unbalanced_merkle:hash_block_index_entry/1
	).

%% @doc Compute "h0" - a cryptographic hash used as a source of entropy when choosing
%% two recall ranges on the weave as unlocked by the given nonce limiter output.
compute_h0(B, PrevB) ->
	#block{ nonce_limiter_info = NonceLimiterInfo,
			partition_number = PartitionNumber, reward_addr = MiningAddr,
			packing_difficulty = PackingDifficulty } = B,
	PrevNonceLimiterInfo = PrevB#block.nonce_limiter_info,
	Seed = PrevNonceLimiterInfo#nonce_limiter_info.seed,
	NonceLimiterOutput = NonceLimiterInfo#nonce_limiter_info.output,
	compute_h0(NonceLimiterOutput, PartitionNumber, Seed, MiningAddr, PackingDifficulty).

compute_h0(NonceLimiterOutput, PartitionNumber, Seed, MiningAddr, PackingDifficulty) ->
	compute_h0(NonceLimiterOutput, PartitionNumber, Seed, MiningAddr, PackingDifficulty,
			ar_packing_server:get_packing_state()).

%% @doc Compute "h0" - a cryptographic hash used as a source of entropy when choosing
%% two recall ranges on the weave as unlocked by the given nonce limiter output.
compute_h0(NonceLimiterOutput, PartitionNumber, Seed, MiningAddr, PackingDifficulty,
		PackingState) ->
	Preimage =
		case PackingDifficulty of
			0 ->
				<< NonceLimiterOutput:32/binary,
					PartitionNumber:256, Seed:32/binary, MiningAddr/binary >>;
			_ ->
				<< NonceLimiterOutput:32/binary,
					PartitionNumber:256, Seed:32/binary, MiningAddr/binary,
					PackingDifficulty:8 >>
		end,
	RandomXState = ar_packing_server:get_randomx_state_for_h0(PackingDifficulty, PackingState),
	ar_mine_randomx:hash(RandomXState, Preimage).

%% @doc Compute "h1" - a cryptographic hash which is either the hash of a solution not
%% involving the second chunk or a carrier of the information about the first chunk
%% used when computing the solution hash off the second chunk.
compute_h1(H0, Nonce, Chunk) ->
	Preimage = crypto:hash(sha256, << H0:32/binary, Nonce:64, Chunk/binary >>),
	{compute_solution_h(H0, Preimage), Preimage}.

%% @doc Compute "h2" - the hash of a solution involving the second chunk.
compute_h2(H1, Chunk, H0) ->
	Preimage = crypto:hash(sha256, << H1:32/binary, Chunk/binary >>),
	{compute_solution_h(H0, Preimage), Preimage}.

%% @doc Compute the solution hash from the preimage and H0.
compute_solution_h(H0, Preimage) ->
	crypto:hash(sha256, << H0:32/binary, Preimage/binary >>).

compute_next_vdf_difficulty(PrevB) ->
	Height = PrevB#block.height + 1,
	#nonce_limiter_info{
		vdf_difficulty = VDFDifficulty,
		next_vdf_difficulty = NextVDFDifficulty
	} = PrevB#block.nonce_limiter_info,
	case ar_block_time_history:has_history(Height) of
		true ->
			case (Height rem ?VDF_DIFFICULTY_RETARGET == 0) andalso
					(VDFDifficulty == NextVDFDifficulty) of
				false ->
					NextVDFDifficulty;
				true ->
					case Height < ar_fork:height_2_7_1() of
						true ->
							HistoryPart = lists:nthtail(?VDF_HISTORY_CUT,
									ar_block_time_history:get_history(PrevB)),
							{IntervalTotal, VDFIntervalTotal} =
								lists:foldl(
									fun({BlockInterval, VDFInterval, _ChunkCount}, {Acc1, Acc2}) ->
										{
											Acc1 + BlockInterval,
											Acc2 + VDFInterval
										}
									end,
									{0, 0},
									HistoryPart
								),
							NewVDFDifficulty =
								(VDFIntervalTotal * VDFDifficulty) div IntervalTotal,
							?LOG_DEBUG([{event, vdf_difficulty_retarget},
									{height, Height},
									{old_vdf_difficulty, VDFDifficulty},
									{new_vdf_difficulty, NewVDFDifficulty},
									{interval_total, IntervalTotal},
									{vdf_interval_total, VDFIntervalTotal}]),
							NewVDFDifficulty;
						false ->
							HistoryPartCut1 = lists:nthtail(?VDF_HISTORY_CUT,
								ar_block_time_history:get_history(PrevB)),
							HistoryPart = lists:sublist(HistoryPartCut1, ?VDF_DIFFICULTY_RETARGET),
							{IntervalTotal, VDFIntervalTotal} =
								lists:foldl(
									fun({BlockInterval, VDFInterval, _ChunkCount}, {Acc1, Acc2}) ->
										{
											Acc1 + BlockInterval,
											Acc2 + VDFInterval
										}
									end,
									{0, 0},
									HistoryPart
								),
							NewVDFDifficulty =
								(VDFIntervalTotal * VDFDifficulty) div IntervalTotal,
							EMAVDFDifficulty = (9*VDFDifficulty + NewVDFDifficulty) div 10,
							?LOG_DEBUG([{event, vdf_difficulty_retarget},
									{height, Height},
									{old_vdf_difficulty, VDFDifficulty},
									{new_vdf_difficulty, NewVDFDifficulty},
									{ema_vdf_difficulty, EMAVDFDifficulty},
									{interval_total, IntervalTotal},
									{vdf_interval_total, VDFIntervalTotal}]),
							EMAVDFDifficulty
					end
			end;
		false ->
			?VDF_DIFFICULTY
	end.

validate_proof_size(PoA) ->
	byte_size(PoA#poa.tx_path) =< ?MAX_TX_PATH_SIZE andalso
			byte_size(PoA#poa.data_path) =< ?MAX_DATA_PATH_SIZE andalso
			byte_size(PoA#poa.chunk) =< ?DATA_CHUNK_SIZE andalso
			byte_size(PoA#poa.unpacked_chunk) =< ?DATA_CHUNK_SIZE.

%% @doc Compute the block identifier (also referred to as "independent hash").
indep_hash(B) ->
	case B#block.height >= ar_fork:height_2_6() of
		true ->
			H = ar_block:generate_signed_hash(B),
			indep_hash2(H, B#block.signature);
		false ->
			BDS = ar_block:generate_block_data_segment(B),
			indep_hash(BDS, B)
	end.

%% @doc Compute the hash signed by the block producer.
generate_signed_hash(#block{ previous_block = PrevH, timestamp = TS,
		nonce = Nonce, height = Height, diff = Diff, cumulative_diff = CDiff,
		last_retarget = LastRetarget, hash = Hash, block_size = BlockSize,
		weave_size = WeaveSize, tx_root = TXRoot, wallet_list = WalletList,
		hash_list_merkle = HashListMerkle, reward_pool = RewardPool,
		packing_2_5_threshold = Packing_2_5_Threshold, reward_addr = Addr,
		reward_key = RewardKey, strict_data_split_threshold = StrictChunkThreshold,
		usd_to_ar_rate = {RateDividend, RateDivisor},
		scheduled_usd_to_ar_rate = {ScheduledRateDividend, ScheduledRateDivisor},
		tags = Tags, txs = TXs,
		reward = Reward, hash_preimage = HashPreimage, recall_byte = RecallByte,
		partition_number = PartitionNumber, recall_byte2 = RecallByte2,
		nonce_limiter_info = NonceLimiterInfo,
		previous_solution_hash = PreviousSolutionHash,
		price_per_gib_minute = PricePerGiBMinute,
		scheduled_price_per_gib_minute = ScheduledPricePerGiBMinute,
		reward_history_hash = RewardHistoryHash,
		block_time_history_hash = BlockTimeHistoryHash, debt_supply = DebtSupply,
		kryder_plus_rate_multiplier = KryderPlusRateMultiplier,
		kryder_plus_rate_multiplier_latch = KryderPlusRateMultiplierLatch,
		denomination = Denomination, redenomination_height = RedenominationHeight,
		double_signing_proof = DoubleSigningProof, previous_cumulative_diff = PrevCDiff,
		merkle_rebase_support_threshold = RebaseThreshold,
		poa = #poa{ data_path = DataPath, tx_path = TXPath },
		poa2 = #poa{ data_path = DataPath2, tx_path = TXPath2 },
		chunk_hash = ChunkHash, chunk2_hash = Chunk2Hash,
		packing_difficulty = PackingDifficulty,
		unpacked_chunk_hash = UnpackedChunkHash,
		unpacked_chunk2_hash = UnpackedChunk2Hash,
		replica_format = ReplicaFormat }) ->
	GetTXID = fun(TXID) when is_binary(TXID) -> TXID; (TX) -> TX#tx.id end,
	Nonce2 = binary:encode_unsigned(Nonce),
	%% The only block where reward_address may be unclaimed
	%% is the genesis block of a new weave.
	Addr2 = case Addr of unclaimed -> <<>>; _ -> Addr end,
	RewardKey2 = case RewardKey of undefined -> undefined; {_Type, Pub} -> Pub end,
	#nonce_limiter_info{ output = Output, global_step_number = N, seed = Seed,
			next_seed = NextSeed, partition_upper_bound = PartitionUpperBound,
			next_partition_upper_bound = NextPartitionUpperBound,
			steps = Steps, prev_output = PrevOutput,
			last_step_checkpoints = LastStepCheckpoints,
			vdf_difficulty = VDFDifficulty,
			next_vdf_difficulty = NextVDFDifficulty } = NonceLimiterInfo,
	{RebaseThresholdBin, DataPathBin, TXPathBin, DataPath2Bin, TXPath2Bin,
			ChunkHashBin, Chunk2HashBin, BlockTimeHistoryHashBin,
			VDFDifficultyBin, NextVDFDifficultyBin} =
		case Height >= ar_fork:height_2_7() of
			true ->
				{encode_int(RebaseThreshold, 16), ar_serialize:encode_bin(DataPath, 24),
						ar_serialize:encode_bin(TXPath, 24),
						ar_serialize:encode_bin(DataPath2, 24),
						ar_serialize:encode_bin(TXPath2, 24),
						<< ChunkHash:32/binary >>,
						ar_serialize:encode_bin(Chunk2Hash, 8),
						<< BlockTimeHistoryHash:32/binary >>,
						ar_serialize:encode_int(VDFDifficulty, 8),
						ar_serialize:encode_int(NextVDFDifficulty, 8)};
			false ->
				{<<>>, <<>>, <<>>, <<>>, <<>>, <<>>, <<>>, <<>>, <<>>, <<>>}
		end,
	{PackingDifficultyBin, UnpackedChunkHashBin, UnpackedChunk2HashBin} =
		case Height >= ar_fork:height_2_8() of
			true ->
				{<< PackingDifficulty:8 >>,
						ar_serialize:encode_bin(UnpackedChunkHash, 8),
						ar_serialize:encode_bin(UnpackedChunk2Hash, 8)};
			false ->
				{<<>>, <<>>, <<>>}
		end,
	ReplicaFormatBin =
		case Height >= ar_fork:height_2_9() of
			true ->
				<< ReplicaFormat:8 >>;
			false ->
				<<>>
		end,
	%% The elements must be either fixed-size or separated by the size separators (
	%% the ar_serialize:encode_* functions).
	Segment = << (encode_bin(PrevH, 8))/binary, (encode_int(TS, 8))/binary,
			(encode_bin(Nonce2, 16))/binary, (encode_int(Height, 8))/binary,
			(encode_int(Diff, 16))/binary, (encode_int(CDiff, 16))/binary,
			(encode_int(LastRetarget, 8))/binary, (encode_bin(Hash, 8))/binary,
			(encode_int(BlockSize, 16))/binary, (encode_int(WeaveSize, 16))/binary,
			(encode_bin(Addr2, 8))/binary, (encode_bin(TXRoot, 8))/binary,
			(encode_bin(WalletList, 8))/binary,
			(encode_bin(HashListMerkle, 8))/binary, (encode_int(RewardPool, 8))/binary,
			(encode_int(Packing_2_5_Threshold, 8))/binary,
			(encode_int(StrictChunkThreshold, 8))/binary,
					(encode_int(RateDividend, 8))/binary,
			(encode_int(RateDivisor, 8))/binary,
					(encode_int(ScheduledRateDividend, 8))/binary,
			(encode_int(ScheduledRateDivisor, 8))/binary,
			(encode_bin_list(Tags, 16, 16))/binary,
			(encode_bin_list([GetTXID(TX) || TX <- TXs], 16, 8))/binary,
			(encode_int(Reward, 8))/binary,
			(encode_int(RecallByte, 16))/binary, (encode_bin(HashPreimage, 8))/binary,
			(encode_int(RecallByte2, 16))/binary, (encode_bin(RewardKey2, 16))/binary,
			(encode_int(PartitionNumber, 8))/binary, Output:32/binary, N:64,
			Seed:48/binary, NextSeed:48/binary, PartitionUpperBound:256,
			NextPartitionUpperBound:256, (encode_bin(PrevOutput, 8))/binary,
			(length(Steps)):16, (iolist_to_binary(Steps))/binary,
			(length(LastStepCheckpoints)):16, (iolist_to_binary(LastStepCheckpoints))/binary,
			(encode_bin(PreviousSolutionHash, 8))/binary,
			(encode_int(PricePerGiBMinute, 8))/binary,
			(encode_int(ScheduledPricePerGiBMinute, 8))/binary,
			RewardHistoryHash:32/binary, (encode_int(DebtSupply, 8))/binary,
			KryderPlusRateMultiplier:24, KryderPlusRateMultiplierLatch:8, Denomination:24,
			(encode_int(RedenominationHeight, 8))/binary,
			(ar_serialize:encode_double_signing_proof(DoubleSigningProof, Height))/binary,
			(encode_int(PrevCDiff, 16))/binary, RebaseThresholdBin/binary,
			DataPathBin/binary, TXPathBin/binary, DataPath2Bin/binary, TXPath2Bin/binary,
			ChunkHashBin/binary, Chunk2HashBin/binary, BlockTimeHistoryHashBin/binary,
			VDFDifficultyBin/binary, NextVDFDifficultyBin/binary,
			PackingDifficultyBin/binary, UnpackedChunkHashBin/binary,
			UnpackedChunk2HashBin/binary, ReplicaFormatBin/binary >>,
	crypto:hash(sha256, Segment).

%% @doc Compute the block identifier from the signed hash and block signature.
indep_hash2(SignedH, Signature) ->
	crypto:hash(sha384, << SignedH:32/binary, Signature/binary >>).

%% @doc Compute the block identifier of a pre-2.6 block.
indep_hash(BDS, B) ->
	case B#block.height >= ar_fork:height_2_4() of
		true ->
			ar_deep_hash:hash([BDS, B#block.hash, B#block.nonce,
					ar_block:poa_to_list(B#block.poa)]);
		false ->
			ar_deep_hash:hash([BDS, B#block.hash, B#block.nonce])
	end.

%% @doc Return the signed block signature preimage.
get_block_signature_preimage(CDiff, PrevCDiff, Preimage, Height) ->
	EncodedCDiff = ar_serialize:encode_int(CDiff, 16),
	EncodedPrevCDiff = ar_serialize:encode_int(PrevCDiff, 16),
	SignaturePreimage = << EncodedCDiff/binary,
			EncodedPrevCDiff/binary, Preimage/binary >>,
	case Height >= ar_fork:height_2_9() of
		false ->
			SignaturePreimage;
		true ->
			<< 0:(32 * 8), SignaturePreimage/binary >>
	end.

%% @doc Verify the block signature.
verify_signature(BlockPreimage, PrevCDiff,
		#block{ signature = Signature, reward_key = {?RSA_KEY_TYPE, Pub} = RewardKey,
				reward_addr = RewardAddr, previous_solution_hash = PrevSolutionH,
				cumulative_diff = CDiff, height = Height })
		when byte_size(Signature) == ?RSA_BLOCK_SIG_SIZE,
				byte_size(Pub) == ?RSA_BLOCK_SIG_SIZE ->
	SignaturePreimage = get_block_signature_preimage(CDiff, PrevCDiff,
			<< PrevSolutionH/binary, BlockPreimage/binary >>, Height),
	ar_wallet:to_address(RewardKey) == RewardAddr andalso
			ar_wallet:verify(RewardKey, SignaturePreimage, Signature);
verify_signature(BlockPreimage, PrevCDiff,
		#block{ signature = Signature, reward_key = {?ECDSA_KEY_TYPE, Pub} = RewardKey,
				reward_addr = RewardAddr, previous_solution_hash = PrevSolutionH,
				cumulative_diff = CDiff, height = Height })
		when byte_size(Signature) == ?ECDSA_SIG_SIZE, byte_size(Pub) == ?ECDSA_PUB_KEY_SIZE ->
	SignaturePreimage = get_block_signature_preimage(CDiff, PrevCDiff,
			<< PrevSolutionH/binary, BlockPreimage/binary >>, Height),
	case Height >= ar_fork:height_2_9() of
		true ->
			ar_wallet:to_address(RewardKey) == RewardAddr andalso
					ar_wallet:verify(RewardKey, SignaturePreimage, Signature);
		false ->
			false
	end;
verify_signature(_BlockPreimage, _PrevCDiff, _B) ->
	false.

%% @doc Return the key suitable for ar_wallet:sign/3 from the given public key.
get_reward_key(Pub, Height) ->
	case Height >= ar_fork:height_2_9() of
		false ->
			{?DEFAULT_KEY_TYPE, Pub};
		true ->
			case byte_size(Pub) of
				?ECDSA_PUB_KEY_SIZE ->
					{?ECDSA_KEY_TYPE, Pub};
				_ ->
					{?RSA_KEY_TYPE, Pub}
			end
	end.

%% @doc Generate a block data segment for a pre-2.6 block. It is combined with a nonce
%% when computing a solution candidate.
generate_block_data_segment(B) ->
	generate_block_data_segment(generate_block_data_segment_base(B), B).

%% @doc Generate a pre-2.6 block data segment given the computed "base".
generate_block_data_segment(BDSBase, B) ->
	Props = [
		BDSBase,
		integer_to_binary(B#block.timestamp),
		integer_to_binary(B#block.last_retarget),
		integer_to_binary(B#block.diff),
		integer_to_binary(B#block.cumulative_diff),
		integer_to_binary(B#block.reward_pool),
		B#block.wallet_list,
		B#block.hash_list_merkle
	],
	ar_deep_hash:hash(Props).

%% @doc Generate a hash, which is used to produce a block data segment
%% when combined with the time-dependent parameters, which frequently
%% change during mining - timestamp, last retarget timestamp, difficulty,
%% cumulative difficulty, (before the fork 2.4, also miner's wallet, reward pool).
%% Also excludes the merkle root of the block index, which is hashed with the rest
%% as the last step - it was used before the fork 2.4 to allow verifiers to quickly
%% validate PoW against the current state. After the fork 2.4, the hash of the
%% previous block prefixes the solution hash preimage of the new block.
generate_block_data_segment_base(B) ->
	GetTXID = fun(TXID) when is_binary(TXID) -> TXID; (TX) -> TX#tx.id end,
	case B#block.height >= ar_fork:height_2_4() of
		true ->
			Props = [
				integer_to_binary(B#block.height),
				B#block.previous_block,
				B#block.tx_root,
				lists:map(GetTXID, B#block.txs),
				integer_to_binary(B#block.block_size),
				integer_to_binary(B#block.weave_size),
				case B#block.reward_addr of
					unclaimed ->
						<<"unclaimed">>;
					_ ->
						B#block.reward_addr
				end,
				encode_tags(B)
			],
			Props2 =
				case B#block.height >= ar_fork:height_2_5() of
					true ->
						{RateDividend, RateDivisor} = B#block.usd_to_ar_rate,
						{ScheduledRateDividend, ScheduledRateDivisor} =
							B#block.scheduled_usd_to_ar_rate,
						[
							integer_to_binary(RateDividend),
							integer_to_binary(RateDivisor),
							integer_to_binary(ScheduledRateDividend),
							integer_to_binary(ScheduledRateDivisor),
							integer_to_binary(B#block.packing_2_5_threshold),
							integer_to_binary(B#block.strict_data_split_threshold)
							| Props
						];
					false ->
						Props
				end,
			ar_deep_hash:hash(Props2);
		false ->
			ar_deep_hash:hash([
				integer_to_binary(B#block.height),
				B#block.previous_block,
				B#block.tx_root,
				lists:map(GetTXID, B#block.txs),
				integer_to_binary(B#block.block_size),
				integer_to_binary(B#block.weave_size),
				case B#block.reward_addr of
					unclaimed ->
						<<"unclaimed">>;
					_ ->
						B#block.reward_addr
				end,
				encode_tags(B),
				poa_to_list(B#block.poa)
			])
	end.

%% @doc Return {RecallRange1Start, RecallRange2Start} - the start offsets
%% of the two recall ranges.
get_recall_range(H0, PartitionNumber, PartitionUpperBound) ->
	RecallRange1Offset = binary:decode_unsigned(binary:part(H0, 0, 8), big),
	RecallRange1Start = PartitionNumber * ar_block:partition_size()
			+ RecallRange1Offset rem min(ar_block:partition_size(), PartitionUpperBound),
	RecallRange2Start = binary:decode_unsigned(H0, big) rem PartitionUpperBound,
	{RecallRange1Start, RecallRange2Start}.

vdf_step_number(#block{ nonce_limiter_info = Info }) ->
	Info#nonce_limiter_info.global_step_number.

get_packing(PackingDifficulty, MiningAddress, 0) ->
	case PackingDifficulty >= 1 of
		true ->
			{composite, MiningAddress, PackingDifficulty};
		false ->
			{spora_2_6, MiningAddress}
	end;
get_packing(_PackingDifficulty, MiningAddress, 1) ->
	{replica_2_9, MiningAddress}.

validate_replica_format(Height, PackingDifficulty, 1) ->
	Height >= ar_fork:height_2_9()
			andalso PackingDifficulty == ?REPLICA_2_9_PACKING_DIFFICULTY;
validate_replica_format(Height, 0, 0) ->
	%% Support for spora_2_6 discontinued at
	%% ar_fork:height_2_8() + ?SPORA_PACKING_EXPIRATION_PERIOD_BLOCKS.
	Height - ?SPORA_PACKING_EXPIRATION_PERIOD_BLOCKS < ar_fork:height_2_8();
validate_replica_format(Height, CompositePackingDifficulty, 0) ->
	case Height - ?COMPOSITE_PACKING_EXPIRATION_PERIOD_BLOCKS < ar_fork:height_2_9() of
		true ->
			%% Composite is still supported - difficulty 1 through 32
			Height >= ar_fork:height_2_8()
				andalso CompositePackingDifficulty =< ?MAX_PACKING_DIFFICULTY;
		false ->
			%% Composite packing is no longer supported.
			false
	end;
validate_replica_format(_, _, _) ->
	false.

get_recall_range_size(0) ->
	?LEGACY_RECALL_RANGE_SIZE;
get_recall_range_size(PackingDifficulty) ->
	?RECALL_RANGE_SIZE div PackingDifficulty.

get_recall_byte(RecallRangeStart, Nonce, 0) ->
	RecallRangeStart + Nonce * ?DATA_CHUNK_SIZE;
get_recall_byte(RecallRangeStart, Nonce, _PackingDifficulty) ->
	ChunkNumber = Nonce div ?COMPOSITE_PACKING_SUB_CHUNK_COUNT,
	RecallRangeStart + ChunkNumber * ?DATA_CHUNK_SIZE.

%% @doc Return the number of bytes per sub-chunk. This also drives how far each mining nonce
%% increments the recall byte.
get_sub_chunk_size(0) ->
	?DATA_CHUNK_SIZE;
get_sub_chunk_size(_PackingDifficulty) ->
	?COMPOSITE_PACKING_SUB_CHUNK_SIZE.

%% @doc Return the number of mining nonces contained in each data chunk.
get_nonces_per_chunk(0) ->
	1;
get_nonces_per_chunk(_PackingDifficulty) ->
	?COMPOSITE_PACKING_SUB_CHUNK_COUNT.

get_nonces_per_recall_range(PackingDifficulty) ->
	max(1, get_recall_range_size(PackingDifficulty) div get_sub_chunk_size(PackingDifficulty)).

%% @doc For packing difficulty 0 (aka spora_2_6 packing), there is one nonce per chunk, so
%% the max nonce is the same as the max chunk number. For packing difficulty >= 1 (aka
%% composite packing and the 2.9 replication), there are ?COMPOSITE_PACKING_SUB_CHUNK_COUNT
%% nonces per chunk.
get_max_nonce(PackingDifficulty) ->
	%% The max(...) is included mostly for testing, where the recall range can be less than
	%% a chunk.
	max(get_nonces_per_chunk(PackingDifficulty) - 1,
		get_nonces_per_recall_range(PackingDifficulty) - 1).

%% @doc Return the 0-based sub-chunk index the mining nonce is pointing to.
get_sub_chunk_index(0, _Nonce) ->
	-1;
get_sub_chunk_index(_PackingDifficulty, Nonce) ->
	Nonce rem ?COMPOSITE_PACKING_SUB_CHUNK_COUNT.

%% @doc Return Offset if it is smaller than or equal to ar_block:strict_data_split_threshold().
%% Otherwise, return the offset of the last byte of the chunk + the size of the padding.
-spec get_chunk_padded_offset(Offset :: non_neg_integer()) -> non_neg_integer().
get_chunk_padded_offset(Offset) ->
	case Offset > ar_block:strict_data_split_threshold() of
		true ->
			ar_poa:get_padded_offset(Offset, ar_block:strict_data_split_threshold());
		false ->
			Offset
	end.

%% @doc Return true if the given cumulative difficulty - previous cumulative difficulty
%% pairs satisfy the double signing condition.
-spec get_double_signing_condition(
		CDiff1 :: non_neg_integer(),
		PrevCDiff1 :: non_neg_integer(),
		CDiff2 :: non_neg_integer(),
		PrevCDiff2 :: non_neg_integer()
) -> boolean().
get_double_signing_condition(CDiff1, PrevCDiff1, CDiff2, PrevCDiff2) ->
	CDiff1 == CDiff2 orelse (CDiff1 > PrevCDiff2 andalso CDiff2 > PrevCDiff1).

%%%===================================================================
%%% Private functions.
%%%===================================================================

validate_tags_size(B) ->
	case B#block.height >= ar_fork:height_2_5() of
		true ->
			Tags = B#block.tags,
			validate_tags_length(Tags, 0) andalso byte_size(list_to_binary(Tags)) =< 2048;
		false ->
			byte_size(list_to_binary(B#block.tags)) =< 2048
	end.

validate_tags_length(_, N) when N > 2048 ->
	false;
validate_tags_length([_ | Tags], N) ->
	validate_tags_length(Tags, N + 1);
validate_tags_length([], _) ->
	true.

encode_int(N, S) -> ar_serialize:encode_int(N, S).
encode_bin(N, S) -> ar_serialize:encode_bin(N, S).
encode_bin_list(L, LS, ES) -> ar_serialize:encode_bin_list(L, LS, ES).

hash_wallet_list(WalletList) ->
	ar_patricia_tree:compute_hash(WalletList,
		fun	(Addr, {Balance, LastTX}) ->
				EncodedBalance = binary:encode_unsigned(Balance),
				ar_deep_hash:hash([Addr, EncodedBalance, LastTX]);
			(Addr, {Balance, LastTX, Denomination, MiningPermission}) ->
				MiningPermissionBin =
					case MiningPermission of
						true ->
							<<1>>;
						false ->
							<<0>>
					end,
				Preimage = << (ar_serialize:encode_bin(Addr, 8))/binary,
						(ar_serialize:encode_int(Balance, 8))/binary,
						(ar_serialize:encode_bin(LastTX, 8))/binary,
						(ar_serialize:encode_int(Denomination, 8))/binary,
						MiningPermissionBin/binary >>,
				crypto:hash(sha384, Preimage)
		end
	).

%% @doc Generate the TX tree and set the TX root for a block.
generate_tx_tree(B) ->
	SizeTaggedTXs = generate_size_tagged_list_from_txs(B#block.txs, B#block.height),
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	generate_tx_tree(B, SizeTaggedDataRoots).

generate_tx_tree(B, SizeTaggedDataRoots) ->
	{Root, Tree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
	B#block{ tx_tree = Tree, tx_root = Root }.

generate_size_tagged_list_from_txs(TXs, Height) ->
	lists:reverse(
		element(2,
			lists:foldl(
				fun(TX, {Pos, List}) ->
					DataSize = TX#tx.data_size,
					End = Pos + DataSize,
					case Height >= ar_fork:height_2_5() of
						true ->
							Padding = ar_tx:get_weave_size_increase(DataSize, Height)
									- DataSize,
							%% Encode the padding information in the Merkle tree.
							case Padding > 0 of
								true ->
									PaddingRoot = ?PADDING_NODE_DATA_ROOT,
									{End + Padding, [{{padding, PaddingRoot}, End + Padding},
											{{TX#tx.id, get_tx_data_root(TX)}, End} | List]};
								false ->
									{End, [{{TX#tx.id, get_tx_data_root(TX)}, End} | List]}
							end;
						false ->
							{End, [{{TX#tx.id, get_tx_data_root(TX)}, End} | List]}
					end
				end,
				{0, []},
				lists:sort(TXs)
			)
		)
	).

%% @doc Find the appropriate block hash list for a block, from a block index.
generate_hash_list_for_block(_BlockOrHash, []) -> [];
generate_hash_list_for_block(B, BI) when ?IS_BLOCK(B) ->
	generate_hash_list_for_block(B#block.indep_hash, BI);
generate_hash_list_for_block(Hash, BI) ->
	do_generate_hash_list_for_block(Hash, BI).

do_generate_hash_list_for_block(_, []) ->
	error(cannot_generate_hash_list);
do_generate_hash_list_for_block(IndepHash, [{IndepHash, _, _} | BI]) -> ?BI_TO_BHL(BI);
do_generate_hash_list_for_block(IndepHash, [_ | Rest]) ->
	do_generate_hash_list_for_block(IndepHash, Rest).

encode_tags(B) ->
	case B#block.height >= ar_fork:height_2_5() of
		true ->
			B#block.tags;
		false ->
			ar_tx:tags_to_list(B#block.tags)
	end.

poa_to_list(POA) ->
	[
		integer_to_binary(POA#poa.option),
		POA#poa.tx_path,
		POA#poa.data_path,
		POA#poa.chunk
	].

%% @doc Compute the 2.5 packing threshold.
get_packing_threshold(B, SearchSpaceUpperBound) ->
	#block{ height = Height, packing_2_5_threshold = PrevPackingThreshold } = B,
	Fork_2_5 = ar_fork:height_2_5(),
	case Height + 1 == Fork_2_5 of
		true ->
			SearchSpaceUpperBound;
		false ->
			case Height + 1 > Fork_2_5 of
				true ->
					ar_block:shift_packing_2_5_threshold(PrevPackingThreshold);
				false ->
					undefined
			end
	end.

%% @doc Move the fork 2.5 packing threshold.
shift_packing_2_5_threshold(0) ->
	0;
shift_packing_2_5_threshold(Threshold) ->
	TargetTime = ar_testnet:target_block_time(ar_fork:height_2_5()),
	Shift = (?DATA_CHUNK_SIZE) * (?PACKING_2_5_THRESHOLD_CHUNKS_PER_SECOND) * TargetTime,
	max(0, Threshold - Shift).

verify_tx_root(B) ->
	B#block.tx_root == generate_tx_root_for_block(B).

%% @doc Given a list of TXs in various formats, or a block, generate the
%% correct TX merkle tree root.
generate_tx_root_for_block(B) when is_record(B, block) ->
	generate_tx_root_for_block(B#block.txs, B#block.height).

generate_tx_root_for_block(TXIDs = [TXID | _], Height) when is_binary(TXID) ->
	generate_tx_root_for_block(ar_storage:read_tx(TXIDs), Height);
generate_tx_root_for_block([], _Height) ->
	<<>>;
generate_tx_root_for_block(TXs = [TX | _], Height) when is_record(TX, tx) ->
	SizeTaggedTXs = generate_size_tagged_list_from_txs(TXs, Height),
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	{Root, _Tree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
	Root.

get_tx_data_root(#tx{ format = 2, data_root = DataRoot }) ->
	DataRoot;
get_tx_data_root(TX) ->
	(ar_tx:generate_chunk_tree(TX))#tx.data_root.

%%%===================================================================
%%% Tests.
%%%===================================================================

hash_list_gen_test_() ->
	{timeout, 60, fun test_hash_list_gen/0}.

test_hash_list_gen() ->
	[B0] = ar_weave:init(),
	ar_test_node:start(B0),
	ar_test_node:mine(),
	BI1 = ar_test_node:wait_until_height(main, 1),
	B1 = ar_storage:read_block(hd(BI1)),
	ar_test_node:mine(),
	BI2 = ar_test_node:wait_until_height(main, 2),
	B2 = ar_storage:read_block(hd(BI2)),
	?assertEqual([B0#block.indep_hash], generate_hash_list_for_block(B1, BI2)),
	?assertEqual([H || {H, _, _} <- BI1],
			generate_hash_list_for_block(B2#block.indep_hash, BI2)).

generate_size_tagged_list_from_txs_test() ->
	Fork_2_5 = ar_fork:height_2_5(),
	?assertEqual([], generate_size_tagged_list_from_txs([], Fork_2_5)),
	?assertEqual([], generate_size_tagged_list_from_txs([], Fork_2_5 - 1)),
	EmptyV1Root = (ar_tx:generate_chunk_tree(#tx{}))#tx.data_root,
	?assertEqual([{{<<>>, EmptyV1Root}, 0}],
			generate_size_tagged_list_from_txs([#tx{}], Fork_2_5)),
	?assertEqual([{{<<>>, <<>>}, 0}],
			generate_size_tagged_list_from_txs([#tx{ format = 2 }], Fork_2_5)),
	?assertEqual([{{<<>>, <<>>}, 0}],
			generate_size_tagged_list_from_txs([#tx{ format = 2}], Fork_2_5 - 1)),
	?assertEqual([{{<<>>, <<"r">>}, 1}, {{padding, <<>>}, 262144}],
			generate_size_tagged_list_from_txs([#tx{ format = 2, data_root = <<"r">>,
					data_size = 1 }], Fork_2_5)),
	?assertEqual([
			{{<<"1">>, <<"r">>}, 1}, {{padding, <<>>}, 262144},
			{{<<"2">>, <<>>}, 262144},
			{{<<"3">>, <<>>}, 262144 * 5},
			{{<<"4">>, <<>>}, 262144 * 5},
			{{<<"5">>, <<>>}, 262144 * 5},
			{{<<"6">>, <<>>}, 262144 * 6}],
			generate_size_tagged_list_from_txs([
					#tx{ id = <<"1">>, format = 2, data_root = <<"r">>, data_size = 1 },
					#tx{ id = <<"2">>, format = 2 },
					#tx{ id = <<"3">>, format = 2, data_size = 262144 * 4 },
					#tx{ id = <<"4">>, format = 2 },
					#tx{ id = <<"5">>, format = 2 },
					#tx{ id = <<"6">>, format = 2, data_size = 262144 }], Fork_2_5)).

test_wallet_list_performance() ->
	test_wallet_list_performance(250_000, ar_deep_hash, mixed).

test_wallet_list_performance(Length) ->
	test_wallet_list_performance(Length, ar_deep_hash, mixed).

test_wallet_list_performance(Length, Algo) ->
	test_wallet_list_performance(Length, Algo, mixed).

test_wallet_list_performance(Length, Algo, Denominations) ->
	SupportedAlgos = [ar_deep_hash, no_ar_deep_hash_sha384, sha256],
	case lists:member(Algo, SupportedAlgos) of
		false ->
			io:format("Supported Algo: ~p~n", [SupportedAlgos]);
		true ->
			SupportedDenominations = [old, new, mixed],
			case lists:member(Denominations, SupportedDenominations) of
				false ->
					io:format("Supported Algo: ~p~n", [SupportedDenominations]);
				true ->
					test_wallet_list_performance2(Length, Algo, Denominations)
			end
	end.

test_wallet_list_performance2(Length, Algo, Denominations) ->

	io:format("# ~B wallets, denominations: ~p, algo: ~p~n", [Length, Denominations, Algo]),
	io:format("============~n"),
	WL = [random_wallet() || _ <- lists:seq(1, Length)],
	{Time1, T1} =
		timer:tc(
			fun() ->
				lists:foldl(
					fun({A, B, LastTX}, Acc) ->
						case Denominations of
							old ->
								ar_patricia_tree:insert(A, {B, LastTX}, Acc);
							new ->
								ar_patricia_tree:insert(A, {B, LastTX,
										1 + rand:uniform(10), true}, Acc);
							mixed ->
								case rand:uniform(2) == 1 of
									true ->
										ar_patricia_tree:insert(A, {B, LastTX}, Acc);
									false ->
										ar_patricia_tree:insert(A, {B, LastTX,
												1 + rand:uniform(10), true}, Acc)
								end
						end
					end,
					ar_patricia_tree:new(),
					WL
				)
			end
		),
	io:format("tree buildup                    | ~f seconds~n", [Time1 / 1000000]),
	{Time2, Binary} =
		timer:tc(
			fun() ->
				ar_serialize:jsonify(
					ar_serialize:wallet_list_to_json_struct(unclaimed, false, T1)
				)
			end
		),
	io:format("serialization                   | ~f seconds~n", [Time2 / 1000000]),
	io:format("                                | ~B bytes~n", [byte_size(Binary)]),
	ComputeHashFun =
		fun	(Addr, {Balance, LastTX}) ->
				case Algo of
					ar_deep_hash ->
						EncodedBalance = binary:encode_unsigned(Balance),
						ar_deep_hash:hash([Addr, EncodedBalance, LastTX]);
					_ ->
						Denomination = 0,
						MiningPermissionBin = <<1>>,
						Preimage = << (ar_serialize:encode_bin(Addr, 8))/binary,
								(ar_serialize:encode_int(Balance, 8))/binary,
								(ar_serialize:encode_bin(LastTX, 8))/binary,
								(ar_serialize:encode_int(Denomination, 8))/binary,
								MiningPermissionBin/binary >>,
						case Algo of
							no_ar_deep_hash_sha384 ->
								crypto:hash(sha384, Preimage);
							sha256 ->
								crypto:hash(sha256, Preimage)
						end
				end;
			(Addr, {Balance, LastTX, Denomination, MiningPermission}) ->
				MiningPermissionBin =
					case MiningPermission of
						true ->
							<<1>>;
						false ->
							<<0>>
					end,
				Preimage = << (ar_serialize:encode_bin(Addr, 8))/binary,
						(ar_serialize:encode_int(Balance, 8))/binary,
						(ar_serialize:encode_bin(LastTX, 8))/binary,
						(ar_serialize:encode_int(Denomination, 8))/binary,
						MiningPermissionBin/binary >>,
				case Algo of
					sha256 ->
						crypto:hash(sha256, Preimage);
					_ ->
						crypto:hash(sha384, Preimage)
				end
		end,
	{Time3, {_, T2, _}} =
		timer:tc(fun() -> ar_patricia_tree:compute_hash(T1, ComputeHashFun) end),
	io:format("root hash from scratch          | ~f seconds~n", [Time3 / 1000000]),
	{Time4, T3} =
		timer:tc(
			fun() ->
				lists:foldl(
					fun({A, B, LastTX}, Acc) ->
						ar_patricia_tree:insert(A, {B, LastTX}, Acc)
					end,
					T2,
					[random_wallet() || _ <- lists:seq(1, 2000)]
				)
			end
		),
	io:format("2000 inserts                    | ~f seconds~n", [Time4 / 1000000]),
	{Time5, _} =
		timer:tc(fun() -> ar_patricia_tree:compute_hash(T3, ComputeHashFun) end),
	io:format("recompute hash after 2k inserts | ~f seconds~n", [Time5 / 1000000]),
	{Time6, T4} =
		timer:tc(
			fun() ->
				{A, B, LastTX} = random_wallet(),
				ar_patricia_tree:insert(A, {B, LastTX}, T2)
			end
		),
	io:format("1 insert                        | ~f seconds~n", [Time6 / 1000000]),
	{Time7, _} =
		timer:tc(fun() -> ar_patricia_tree:compute_hash(T4, ComputeHashFun) end),
	io:format("recompute hash after 1 insert   | ~f seconds~n", [Time7 / 1000000]).

random_wallet() ->
	{
		crypto:strong_rand_bytes(32),
		rand:uniform(1000000000000000000),
		crypto:strong_rand_bytes(32)
	}.

validate_replica_format_test_() ->
	[
		ar_test_node:test_with_mocked_functions([
				{ar_fork, height_2_8, fun() -> 10 end},
				{ar_fork, height_2_9, fun() -> 20 end}
			],
			fun test_validate_replica_format/0, 30)
	].
test_validate_replica_format() ->
	%% pre 2.8, only spora_2_6 is supported
	?assertEqual(true, validate_replica_format(0, 0, 0)),
	?assertEqual(false, validate_replica_format(0, 1, 0)),
	?assertEqual(false, validate_replica_format(0, 33, 0)),
	?assertEqual(false, validate_replica_format(0, 25, 0)),
	?assertEqual(false, validate_replica_format(0, 0, 1)),
	?assertEqual(false, validate_replica_format(0, 1, 1)),
	?assertEqual(false, validate_replica_format(0, 33, 1)),
	?assertEqual(false, validate_replica_format(0, 25, 1)),
	%% post-2.8, pre-2.9, spora_2_6 and composite are supported
	?assertEqual(true, validate_replica_format(15, 0, 0)),
	?assertEqual(true, validate_replica_format(15, 1, 0)),
	?assertEqual(false, validate_replica_format(15, 33, 0)),
	?assertEqual(false, validate_replica_format(15, 100, 0)),
	?assertEqual(false, validate_replica_format(15, 0, 1)),
	?assertEqual(false, validate_replica_format(15, 1, 1)),
	?assertEqual(false, validate_replica_format(15, 33, 1)),
	?assertEqual(false, validate_replica_format(15, 25, 1)),
	%% post-2.9, pre-composite expiration
	?assertEqual(true, validate_replica_format(25, 0, 0)),
	?assertEqual(true, validate_replica_format(25, 1, 0)),
	?assertEqual(false, validate_replica_format(25, 33, 0)),
	?assertEqual(false, validate_replica_format(25, 100, 0)),
	?assertEqual(false, validate_replica_format(25, 0, 1)),
	?assertEqual(false, validate_replica_format(25, 1, 1)),
	?assertEqual(false, validate_replica_format(25, 33, 1)),
	?assertEqual(true, validate_replica_format(25, 2, 1)), %% 2 in tests.
	%% post-2.9, post-composite expiration
	CompositeExpiration = ar_fork:height_2_9() + ?COMPOSITE_PACKING_EXPIRATION_PERIOD_BLOCKS,
	?assertEqual(true, validate_replica_format(CompositeExpiration, 0, 0)),
	?assertEqual(false, validate_replica_format(CompositeExpiration, 1, 0)),
	?assertEqual(false, validate_replica_format(CompositeExpiration, 33, 0)),
	?assertEqual(false, validate_replica_format(CompositeExpiration, 25, 0)),
	?assertEqual(false, validate_replica_format(CompositeExpiration, 0, 1)),
	?assertEqual(false, validate_replica_format(CompositeExpiration, 1, 1)),
	?assertEqual(false, validate_replica_format(CompositeExpiration, 33, 1)),
	?assertEqual(true, validate_replica_format(CompositeExpiration, 2, 1)),
	%% post-2.9, post-spora expiration
	SporaExpiration = ar_fork:height_2_8() + ?SPORA_PACKING_EXPIRATION_PERIOD_BLOCKS,
	?assertEqual(false, validate_replica_format(SporaExpiration, 0, 0)),
	?assertEqual(false, validate_replica_format(SporaExpiration, 1, 0)),
	?assertEqual(false, validate_replica_format(SporaExpiration, 33, 0)),
	?assertEqual(false, validate_replica_format(SporaExpiration, 25, 0)),
	?assertEqual(false, validate_replica_format(SporaExpiration, 0, 1)),
	?assertEqual(false, validate_replica_format(SporaExpiration, 1, 1)),
	?assertEqual(false, validate_replica_format(SporaExpiration, 33, 1)),
	?assertEqual(true, validate_replica_format(SporaExpiration, 2, 1)).
