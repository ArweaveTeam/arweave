%%% @doc The module contains the serialization and deserialization utilities for the
%%% various protocol entitities - transactions, blocks, proofs, etc
-module(ar_serialize).

-export([block_to_binary/1, binary_to_block/1, json_struct_to_block/1,
		block_to_json_struct/1,
		block_announcement_to_binary/1, binary_to_block_announcement/1,
		binary_to_block_announcement_response/1, block_announcement_response_to_binary/1,
		tx_to_binary/1, binary_to_tx/1,

		poa_map_to_binary/1, binary_to_poa/1,
		poa_no_chunk_map_to_binary/1, binary_to_no_chunk_map/1,
		poa_map_to_json_map/1, poa_no_chunk_map_to_json_map/1, json_map_to_poa_map/1,

		block_index_to_binary/1, binary_to_block_index/1, encode_double_signing_proof/1,
		json_struct_to_poa/1, poa_to_json_struct/1,
		tx_to_json_struct/1, json_struct_to_tx/1, json_struct_to_v1_tx/1,
		etf_to_wallet_chunk_response/1, wallet_list_to_json_struct/3,
		wallet_to_json_struct/2, json_struct_to_wallet_list/1,
		block_index_to_json_struct/1, json_struct_to_block_index/1,
		jsonify/1, dejsonify/1, json_decode/1, json_decode/2,
		query_to_json_struct/1, json_struct_to_query/1,
		encode_int/2, encode_bin/2,
		encode_bin_list/3, signature_type_to_binary/1, binary_to_signature_type/1,
		reward_history_to_binary/1, binary_to_reward_history/1,
		block_time_history_to_binary/1, binary_to_block_time_history/1, parse_32b_list/1,
		nonce_limiter_update_to_binary/2, binary_to_nonce_limiter_update/2,
		nonce_limiter_update_response_to_binary/1, binary_to_nonce_limiter_update_response/1,
		candidate_to_json_struct/1, solution_to_json_struct/1, json_map_to_solution/1,
		json_map_to_candidate/1,
		jobs_to_json_struct/1, json_struct_to_jobs/1,
		partial_solution_response_to_json_struct/1,
		pool_cm_jobs_to_json_struct/1, json_map_to_pool_cm_jobs/1, json_map_to_info_map/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("arweave/include/ar_pool.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Serialize the block.
block_to_binary(#block{ indep_hash = H, previous_block = PrevH, timestamp = TS,
		nonce = Nonce, height = Height, diff = Diff, cumulative_diff = CDiff,
		last_retarget = LastRetarget, hash = Hash, block_size = BlockSize,
		weave_size = WeaveSize, reward_addr = Addr, tx_root = TXRoot,
		wallet_list = WalletList, hash_list_merkle = HashListMerkle,
		reward_pool = RewardPool, packing_2_5_threshold = Threshold,
		strict_data_split_threshold = StrictChunkThreshold,
		usd_to_ar_rate = Rate, scheduled_usd_to_ar_rate = ScheduledRate,
		poa = #poa{ option = Option, chunk = Chunk, data_path = DataPath,
				tx_path = TXPath }, tags = Tags, txs = TXs } = B) ->
	Addr2 = case Addr of unclaimed -> <<>>; _ -> Addr end,
	{RateDividend, RateDivisor} = case Rate of undefined -> {undefined, undefined};
			_ -> Rate end,
	{ScheduledRateDividend, ScheduledRateDivisor} =
			case ScheduledRate of
				undefined ->
					{undefined, undefined};
				_ ->
					ScheduledRate
			end,
	Nonce2 = case B#block.height >= ar_fork:height_2_6() of
			true -> binary:encode_unsigned(Nonce, big); false -> Nonce end,
	<< H:48/binary, (encode_bin(PrevH, 8))/binary, (encode_int(TS, 8))/binary,
			(encode_bin(Nonce2, 16))/binary, (encode_int(Height, 8))/binary,
			(encode_int(Diff, 16))/binary, (encode_int(CDiff, 16))/binary,
			(encode_int(LastRetarget, 8))/binary, (encode_bin(Hash, 8))/binary,
			(encode_int(BlockSize, 16))/binary, (encode_int(WeaveSize, 16))/binary,
			(encode_bin(Addr2, 8))/binary, (encode_bin(TXRoot, 8))/binary,
			(encode_bin(WalletList, 8))/binary, (encode_bin(HashListMerkle, 8))/binary,
			(encode_int(RewardPool, 8))/binary, (encode_int(Threshold, 8))/binary,
			(encode_int(StrictChunkThreshold, 8))/binary,
			(encode_int(RateDividend, 8))/binary,
			(encode_int(RateDivisor, 8))/binary,
			(encode_int(ScheduledRateDividend, 8))/binary,
			(encode_int(ScheduledRateDivisor, 8))/binary, (encode_int(Option, 8))/binary,
			(encode_bin(Chunk, 24))/binary, (encode_bin(TXPath, 24))/binary,
			(encode_bin(DataPath, 24))/binary, (encode_bin_list(Tags, 16, 16))/binary,
			(encode_transactions(TXs))/binary, (encode_post_2_6_fields(B))/binary >>.

%% @doc Deserialize the block.
binary_to_block(<< H:48/binary, PrevHSize:8, PrevH:PrevHSize/binary,
		TSSize:8, TS:(TSSize * 8),
		NonceSize:16, Nonce:NonceSize/binary,
		HeightSize:8, Height:(HeightSize * 8),
		DiffSize:16, Diff:(DiffSize * 8),
		CDiffSize:16, CDiff:(CDiffSize * 8),
		LastRetargetSize:8, LastRetarget:(LastRetargetSize * 8),
		HashSize:8, Hash:HashSize/binary,
		BlockSizeSize:16, BlockSize:(BlockSizeSize * 8),
		WeaveSizeSize:16, WeaveSize:(WeaveSizeSize * 8),
		AddrSize:8, Addr:AddrSize/binary,
		TXRootSize:8, TXRoot:TXRootSize/binary, % 0 or 32
		WalletListSize:8, WalletList:WalletListSize/binary,
		HashListMerkleSize:8, HashListMerkle:HashListMerkleSize/binary,
		RewardPoolSize:8, RewardPool:(RewardPoolSize * 8),
		PackingThresholdSize:8, Threshold:(PackingThresholdSize * 8),
		StrictChunkThresholdSize:8, StrictChunkThreshold:(StrictChunkThresholdSize * 8),
		RateDividendSize:8, RateDividend:(RateDividendSize * 8),
		RateDivisorSize:8, RateDivisor:(RateDivisorSize * 8),
		SchedRateDividendSize:8, SchedRateDividend:(SchedRateDividendSize * 8),
		SchedRateDivisorSize:8, SchedRateDivisor:(SchedRateDivisorSize * 8),
		PoAOptionSize:8, PoAOption:(PoAOptionSize * 8),
		ChunkSize:24, Chunk:ChunkSize/binary,
		TXPathSize:24, TXPath:TXPathSize/binary,
		DataPathSize:24, DataPath:DataPathSize/binary,
		Rest/binary >>) when NonceSize =< 512 ->
	Threshold2 = case PackingThresholdSize of 0 -> undefined; _ -> Threshold end,
	StrictChunkThreshold2 = case StrictChunkThresholdSize of 0 -> undefined;
			_ -> StrictChunkThreshold end,
	Rate = case RateDivisorSize of 0 -> undefined;
			_ -> {RateDividend, RateDivisor} end,
	ScheduledRate = case SchedRateDivisorSize of 0 -> undefined;
			_ -> {SchedRateDividend, SchedRateDivisor} end,
	Addr2 = case {AddrSize, Height >= ar_fork:height_2_6()} of
			{0, false} -> unclaimed; _ -> Addr end,
	B = #block{ indep_hash = H, previous_block = PrevH, timestamp = TS,
			nonce = Nonce, height = Height, diff = Diff, cumulative_diff = CDiff,
			last_retarget = LastRetarget, hash = Hash, block_size = BlockSize,
			weave_size = WeaveSize, reward_addr = Addr2, tx_root = TXRoot,
			wallet_list = WalletList, hash_list_merkle = HashListMerkle,
			reward_pool = RewardPool, packing_2_5_threshold = Threshold2,
			strict_data_split_threshold = StrictChunkThreshold2,
			usd_to_ar_rate = Rate, scheduled_usd_to_ar_rate = ScheduledRate,
			poa = #poa{ option = PoAOption, chunk = Chunk, data_path = DataPath,
					tx_path = TXPath }},
	parse_block_tags_transactions(Rest, B);
binary_to_block(_Bin) ->
	{error, invalid_block_input}.

%% @doc Convert a block record into a JSON struct.
block_to_json_struct(
	#block{ nonce = Nonce, previous_block = PrevHash, timestamp = TimeStamp,
			last_retarget = LastRetarget, diff = Diff, height = Height, hash = Hash,
			indep_hash = IndepHash, txs = TXs, tx_root = TXRoot, wallet_list = WalletList,
			reward_addr = RewardAddr, tags = Tags, reward_pool = RewardPool,
			weave_size = WeaveSize, block_size = BlockSize, cumulative_diff = CDiff,
			hash_list_merkle = MR, poa = POA,
			previous_cumulative_diff = PrevCDiff,
			merkle_rebase_support_threshold = RebaseThreshold } = B) ->
	{JSONDiff, JSONCDiff} =
		case Height >= ar_fork:height_1_8() of
			true ->
				{integer_to_binary(Diff), integer_to_binary(CDiff)};
			false ->
				{Diff, CDiff}
	end,
	{JSONRewardPool, JSONBlockSize, JSONWeaveSize} =
		case Height >= ar_fork:height_2_4() of
			true ->
				{integer_to_binary(RewardPool), integer_to_binary(BlockSize),
					integer_to_binary(WeaveSize)};
			false ->
				{RewardPool, BlockSize, WeaveSize}
	end,
	Tags2 =
		case Height >= ar_fork:height_2_5() of
			true ->
				[ar_util:encode(Tag) || Tag <- Tags];
			false ->
				Tags
		end,
	Nonce2 = case B#block.height >= ar_fork:height_2_6() of
			true -> binary:encode_unsigned(Nonce); false -> Nonce end,
	JSONElements =
		[{nonce, ar_util:encode(Nonce2)}, {previous_block, ar_util:encode(PrevHash)},
				{timestamp, TimeStamp}, {last_retarget, LastRetarget}, {diff, JSONDiff},
				{height, Height}, {hash, ar_util:encode(Hash)},
				{indep_hash, ar_util:encode(IndepHash)},
				{txs,
					lists:map(
						fun(TXID) when is_binary(TXID) ->
							ar_util:encode(TXID);
						(TX) ->
							ar_util:encode(TX#tx.id)
						end,
						TXs
					)
				}, {tx_root, ar_util:encode(TXRoot)}, {tx_tree, []},
				{wallet_list, ar_util:encode(WalletList)},
				{reward_addr,
					case RewardAddr of unclaimed -> list_to_binary("unclaimed");
							_ -> ar_util:encode(RewardAddr) end}, {tags, Tags2},
				{reward_pool, JSONRewardPool}, {weave_size, JSONWeaveSize},
				{block_size, JSONBlockSize}, {cumulative_diff, JSONCDiff},
				{hash_list_merkle, ar_util:encode(MR)}, {poa, poa_to_json_struct(POA)}],
	JSONElements2 =
		case Height < ar_fork:height_1_6() of
			true ->
				KeysToDelete = [cumulative_diff, hash_list_merkle],
				delete_keys(KeysToDelete, JSONElements);
			false ->
				JSONElements
		end,
	JSONElements3 =
		case Height >= ar_fork:height_2_4() of
			true ->
				delete_keys([tx_tree], JSONElements2);
			false ->
				JSONElements2
		end,
	JSONElements4 =
		case Height >= ar_fork:height_2_5() of
			true ->
				{RateDividend, RateDivisor} = B#block.usd_to_ar_rate,
				{ScheduledRateDividend,
						ScheduledRateDivisor} = B#block.scheduled_usd_to_ar_rate,
				[
					{usd_to_ar_rate,
						[integer_to_binary(RateDividend), integer_to_binary(RateDivisor)]},
					{scheduled_usd_to_ar_rate,
						[integer_to_binary(ScheduledRateDividend),
							integer_to_binary(ScheduledRateDivisor)]},
					{packing_2_5_threshold,
							integer_to_binary(B#block.packing_2_5_threshold)},
					{strict_data_split_threshold,
							integer_to_binary(B#block.strict_data_split_threshold)}
					| JSONElements3
				];
			false ->
				JSONElements3
		end,
	JSONElements5 =
		case Height >= ar_fork:height_2_6() of
			true ->
				PricePerGiBMinute = B#block.price_per_gib_minute,
				ScheduledPricePerGiBMinute = B#block.scheduled_price_per_gib_minute,
				DebtSupply = B#block.debt_supply,
				KryderPlusRateMultiplier = B#block.kryder_plus_rate_multiplier,
				KryderPlusRateMultiplierLatch = B#block.kryder_plus_rate_multiplier_latch,
				Denomination = B#block.denomination,
				RedenominationHeight = B#block.redenomination_height,
				DoubleSigningProof =
					case B#block.double_signing_proof of
						undefined ->
							{[]};
						{Key, Sig1, CDiff1, PrevCDiff1, Preimage1, Sig2, CDiff2,
								PrevCDiff2, Preimage2} ->
							{[{pub_key, ar_util:encode(Key)}, {sig1, ar_util:encode(Sig1)},
									{cdiff1, integer_to_binary(CDiff1)},
									{prev_cdiff1, integer_to_binary(PrevCDiff1)},
									{preimage1, ar_util:encode(Preimage1)},
									{sig2, ar_util:encode(Sig2)},
									{cdiff2, integer_to_binary(CDiff2)},
									{prev_cdiff2, integer_to_binary(PrevCDiff2)},
									{preimage2, ar_util:encode(Preimage2)}]}
					end,
				JSONElements6 =
					[{hash_preimage, ar_util:encode(B#block.hash_preimage)},
							{recall_byte, integer_to_binary(B#block.recall_byte)},
							{reward, integer_to_binary(B#block.reward)},
							{previous_solution_hash,
									ar_util:encode(B#block.previous_solution_hash)},
							{partition_number, B#block.partition_number},
							{nonce_limiter_info, nonce_limiter_info_to_json_struct(
									B#block.height, B#block.nonce_limiter_info)},
							{poa2, poa_to_json_struct(B#block.poa2)},
							{signature, ar_util:encode(B#block.signature)},
							{reward_key, ar_util:encode(element(2, B#block.reward_key))},
							{price_per_gib_minute, integer_to_binary(PricePerGiBMinute)},
							{scheduled_price_per_gib_minute,
									integer_to_binary(ScheduledPricePerGiBMinute)},
							{reward_history_hash,
									ar_util:encode(B#block.reward_history_hash)},
							{debt_supply, integer_to_binary(DebtSupply)},
							{kryder_plus_rate_multiplier,
									integer_to_binary(KryderPlusRateMultiplier)},
							{kryder_plus_rate_multiplier_latch,
									integer_to_binary(KryderPlusRateMultiplierLatch)},
							{denomination, integer_to_binary(Denomination)},
							{redenomination_height, RedenominationHeight},
							{double_signing_proof, DoubleSigningProof},
							{previous_cumulative_diff, integer_to_binary(PrevCDiff)}
							| JSONElements4],
				case B#block.recall_byte2 of
					undefined ->
						JSONElements6;
					RecallByte2 ->
						[{recall_byte2, integer_to_binary(RecallByte2)} | JSONElements6]
				end;
			false ->
				JSONElements4
		end,
	JSONElements8 =
		case Height >= ar_fork:height_2_7() of
			true ->
				JSONElements7 = [
						{merkle_rebase_support_threshold, integer_to_binary(RebaseThreshold)},
						{chunk_hash, ar_util:encode(B#block.chunk_hash)},
						{block_time_history_hash,
							ar_util:encode(B#block.block_time_history_hash)}
						| JSONElements5],
				case B#block.chunk2_hash of
					undefined ->
						JSONElements7;
					_ ->
						[{chunk2_hash, ar_util:encode(B#block.chunk2_hash)} | JSONElements7]
				end;
			false ->
				JSONElements5
		end,
	{JSONElements8}.

reward_history_to_binary(RewardHistory) ->
	reward_history_to_binary(RewardHistory, []).

reward_history_to_binary([], IOList) ->
	iolist_to_binary(IOList);
reward_history_to_binary([{Addr, HashRate, Reward, Denomination} | RewardHistory], IOList) ->
	reward_history_to_binary(RewardHistory, [Addr, ar_serialize:encode_int(HashRate, 8),
			ar_serialize:encode_int(Reward, 8), << Denomination:24 >> | IOList]).

binary_to_reward_history(Bin) ->
	binary_to_reward_history(Bin, []).

binary_to_reward_history(<< Addr:32/binary, HashRateSize:8, HashRate:(HashRateSize * 8),
		RewardSize:8, Reward:(RewardSize * 8), Denomination:24, Rest/binary >>,
		RewardHistory) ->
	binary_to_reward_history(Rest, [{Addr, HashRate, Reward, Denomination} | RewardHistory]);
binary_to_reward_history(<<>>, RewardHistory) ->
	{ok, RewardHistory};
binary_to_reward_history(_Rest, _RewardHistory) ->
	{error, invalid_reward_history}.

block_time_history_to_binary(BlockTimeHistory) ->
	block_time_history_to_binary(BlockTimeHistory, []).

block_time_history_to_binary([], IOList) ->
	iolist_to_binary(IOList);
block_time_history_to_binary([{BlockInterval, VDFInterval, ChunkCount} | BlockTimeHistory],
		IOList) ->
	block_time_history_to_binary(BlockTimeHistory, [
			ar_serialize:encode_int(BlockInterval, 8),
			ar_serialize:encode_int(VDFInterval, 8),
			ar_serialize:encode_int(ChunkCount, 8)
	| IOList]).

binary_to_block_time_history(Bin) ->
	binary_to_block_time_history(Bin, []).

binary_to_block_time_history(<< BlockIntervalSize:8,
			BlockInterval:(BlockIntervalSize * 8),
			VDFIntervalSize:8, VDFInterval:(VDFIntervalSize * 8),
			ChunkCountSize:8, ChunkCount:(ChunkCountSize * 8), Rest/binary >>,
		BlockTimeHistory) ->
	binary_to_block_time_history(Rest,
			[{BlockInterval, VDFInterval, ChunkCount} | BlockTimeHistory]);
binary_to_block_time_history(<<>>, BlockTimeHistory) ->
	{ok, BlockTimeHistory};
binary_to_block_time_history(_Rest, _BlockTimeHistory) ->
	{error, invalid_block_time_history}.

%% Note: the #nonce_limiter_update and #vdf_session records are only serialized for communication
%% between a VDF server and VDF client. Only fields that are required for this communication are
%% serialized.
%% 
%% For example, the vdf_difficulty and next_vdf_difficulty fields are omitted as they are only used
%% by nodes that compute their own VDF and never need to be shared from VDF server to VDF client.
nonce_limiter_update_to_binary(2 = _Format, #nonce_limiter_update{
			session_key = {NextSeed, Interval, NextVDFDifficulty},
		session = Session, is_partial = IsPartial }) ->
	#vdf_session{ step_number = StepNumber, step_checkpoints_map = Map } = Session,
	Checkpoints = maps:get(StepNumber, Map, []),
	IsPartialBin = case IsPartial of true -> << 1:8 >>; _ -> << 0:8 >> end,
	CheckpointLen = length(Checkpoints),
	<< NextSeed:48/binary, (ar_serialize:encode_int(NextVDFDifficulty, 8))/binary,
			Interval:64, IsPartialBin/binary, CheckpointLen:16,
			(iolist_to_binary(Checkpoints))/binary, (encode_vdf_session(2, Session))/binary >>;

nonce_limiter_update_to_binary(3 = _Format, #nonce_limiter_update{
			session_key = {NextSeed, Interval, NextVDFDifficulty},
			session = Session, is_partial = IsPartial }) ->
	#vdf_session{ step_checkpoints_map = Map } = Session,
	CheckpointsMapBin = encode_step_checkpoints_map(Map),
	CheckpointsMapSize = byte_size(CheckpointsMapBin),
	IsPartialBin = case IsPartial of true -> << 1:8 >>; _ -> << 0:8 >> end,
	<< NextSeed:48/binary, (ar_serialize:encode_int(NextVDFDifficulty, 8))/binary,
			Interval:64, IsPartialBin/binary, CheckpointsMapSize:24,
			CheckpointsMapBin:CheckpointsMapSize/binary,
			(encode_vdf_session(2, Session))/binary >>;

nonce_limiter_update_to_binary(4 = _Format, #nonce_limiter_update{
			session_key = {NextSeed, Interval, NextVDFDifficulty},
			session = Session, is_partial = IsPartial }) ->
	#vdf_session{ step_checkpoints_map = Map } = Session,
	CheckpointsMapBin = encode_step_checkpoints_map(Map),
	CheckpointsMapSize = byte_size(CheckpointsMapBin),
	IsPartialBin = case IsPartial of true -> << 1:8 >>; _ -> << 0:8 >> end,
	<< NextSeed:48/binary, (ar_serialize:encode_int(NextVDFDifficulty, 8))/binary,
			Interval:64, IsPartialBin/binary, CheckpointsMapSize:24,
			CheckpointsMapBin:CheckpointsMapSize/binary,
			(encode_vdf_session(4, Session))/binary >>.

encode_step_checkpoints_map(Map) ->
	encode_step_checkpoints_map(maps:keys(Map), Map, <<>>).

encode_step_checkpoints_map([], _Map, Bin) ->
	Bin;
encode_step_checkpoints_map([Key | Keys], Map, Bin) ->
	Checkpoints = maps:get(Key, Map),
	CheckpointLen = length(Checkpoints),
	encode_step_checkpoints_map(Keys, Map,
		<< Key:64, CheckpointLen:16, (iolist_to_binary(Checkpoints))/binary, Bin/binary >>).

encode_vdf_session(2 = _Format, #vdf_session{ step_number = StepNumber, seed = Seed, steps = Steps,
		prev_session_key = PrevSessionKey, upper_bound = UpperBound,
		next_upper_bound = NextUpperBound }) ->
	StepsLen = length(Steps),
	<< StepNumber:64, Seed:48/binary, (encode_int(UpperBound, 8))/binary,
			(encode_int(NextUpperBound, 8))/binary, StepsLen:16,
			(iolist_to_binary(Steps))/binary,
			(encode_session_key(2, PrevSessionKey))/binary >>;

encode_vdf_session(4 = _Format, #vdf_session{
		step_number = StepNumber, seed = Seed, steps = Steps,
		prev_session_key = PrevSessionKey,
		upper_bound = UpperBound, next_upper_bound = NextUpperBound,
		vdf_difficulty = VDFDifficulty }) ->
	StepsLen = length(Steps),
	<< StepNumber:64, Seed:48/binary, (encode_int(UpperBound, 8))/binary,
			(encode_int(NextUpperBound, 8))/binary, StepsLen:16,
			(iolist_to_binary(Steps))/binary,
			(encode_int(VDFDifficulty, 8))/binary,
			(encode_session_key(2, PrevSessionKey))/binary >>.

encode_session_key(undefined) ->
	<<>>;
encode_session_key({NextSeed, Interval, NextDifficulty}) ->
	<< NextSeed:48/binary, (ar_serialize:encode_int(NextDifficulty, 8))/binary, Interval:64 >>.

encode_session_key(2 = _Format, SessionKey) ->
	encode_session_key(SessionKey).

decode_session_key(<<>>) ->
	undefined;
decode_session_key(<<
		NextSeed:48/binary,
		NextVDFDifficultySize:8, NextVDFDifficulty:(NextVDFDifficultySize * 8),
		Interval:64 >>) ->
	{NextSeed, Interval, NextVDFDifficulty};
decode_session_key(_) ->
	error.

binary_to_nonce_limiter_update(2, % Format
			<< NextSeed:48/binary,
			NextVDFDifficultySize:8, NextVDFDifficulty:(NextVDFDifficultySize * 8),
			Interval:64, IsPartial:8,
			CheckpointLen:16, Checkpoints:(CheckpointLen * 32)/binary,
			StepNumber:64, Seed:48/binary, UpperBoundSize:8, UpperBound:(UpperBoundSize * 8),
			NextUpperBoundSize:8, NextUpperBound:(NextUpperBoundSize * 8),
			StepsLen:16, Steps:(StepsLen * 32)/binary,
			PrevSessionKeyBin/binary >>)
		when UpperBoundSize > 0, StepsLen > 0, CheckpointLen == ?VDF_CHECKPOINT_COUNT_IN_STEP ->
	NextUpperBound2 = case NextUpperBoundSize of 0 -> undefined; _ -> NextUpperBound end,
	Update = #nonce_limiter_update{ session_key = {NextSeed, Interval, NextVDFDifficulty},
			is_partial = case IsPartial of 0 -> false; _ -> true end,
			session = Session = #vdf_session{ step_number = StepNumber, seed = Seed,
					step_checkpoints_map = #{ StepNumber => parse_32b_list(Checkpoints) },
					upper_bound = UpperBound, next_upper_bound = NextUpperBound2,
					steps = parse_32b_list(Steps) } },
	case decode_session_key(PrevSessionKeyBin) of
		undefined ->
			{ok, Update};
		error ->
			{error, invalid1};
		SessionKey ->
			Session2 = Session#vdf_session{ prev_session_key = SessionKey },
			{ok, Update#nonce_limiter_update{ session = Session2 }}
	end;
binary_to_nonce_limiter_update(2, _Bin) ->
	{error, invalid2};

binary_to_nonce_limiter_update(3, % Format = 3.
			<< NextSeed:48/binary,
			NextVDFDifficultySize:8, NextVDFDifficulty:(NextVDFDifficultySize * 8),
			Interval:64, IsPartial:8,
			CheckpointsMapSize:24, CheckpointsMapBin:CheckpointsMapSize/binary,
			StepNumber:64, Seed:48/binary, UpperBoundSize:8, UpperBound:(UpperBoundSize * 8),
			NextUpperBoundSize:8, NextUpperBound:(NextUpperBoundSize * 8),
			StepsLen:16, Steps:(StepsLen * 32)/binary,
			PrevSessionKeyBin/binary >>)
		when UpperBoundSize > 0, StepsLen > 0 ->
	NextUpperBound2 = case NextUpperBoundSize of 0 -> undefined; _ -> NextUpperBound end,
	case decode_step_checkpoints_map(CheckpointsMapBin, #{}) of
		{error, _} = Error ->
			Error;
		{ok, StepCheckpointsMap} ->
			Update = #nonce_limiter_update{
					session_key = {NextSeed, Interval, NextVDFDifficulty},
					is_partial = case IsPartial of 0 -> false; _ -> true end,
					session = Session = #vdf_session{ step_number = StepNumber, seed = Seed,
							upper_bound = UpperBound, next_upper_bound = NextUpperBound2,
							steps = parse_32b_list(Steps),
							step_checkpoints_map = StepCheckpointsMap } },
			case decode_session_key(PrevSessionKeyBin) of
				undefined ->
					{ok, Update};
				error ->
					{error, invalid1};
				PrevSessionKey ->
					Session2 = Session#vdf_session{ prev_session_key = PrevSessionKey },
					{ok, Update#nonce_limiter_update{ session = Session2 }}
			end
	end;
binary_to_nonce_limiter_update(3, _Bin) ->
	{error, invalid2};

binary_to_nonce_limiter_update(4, % Format = 4.
			<< NextSeed:48/binary,
			NextVDFDifficultySize:8, NextVDFDifficulty:(NextVDFDifficultySize * 8),
			Interval:64, IsPartial:8,
			CheckpointsMapSize:24, CheckpointsMapBin:CheckpointsMapSize/binary,
			StepNumber:64, Seed:48/binary, UpperBoundSize:8, UpperBound:(UpperBoundSize * 8),
			NextUpperBoundSize:8, NextUpperBound:(NextUpperBoundSize * 8),
			StepsLen:16, Steps:(StepsLen * 32)/binary,
			VDFDifficultySize:8, VDFDifficulty:(VDFDifficultySize * 8),
			PrevSessionKeyBin/binary >>)
		when UpperBoundSize > 0, StepsLen > 0 ->
	NextUpperBound2 = case NextUpperBoundSize of 0 -> undefined; _ -> NextUpperBound end,
	case decode_step_checkpoints_map(CheckpointsMapBin, #{}) of
		{error, _} = Error ->
			Error;
		{ok, StepCheckpointsMap} ->
			Update = #nonce_limiter_update{
					session_key = {NextSeed, Interval, NextVDFDifficulty},
					is_partial = case IsPartial of 0 -> false; _ -> true end,
					session = Session = #vdf_session{ step_number = StepNumber, seed = Seed,
							upper_bound = UpperBound, next_upper_bound = NextUpperBound2,
							vdf_difficulty = VDFDifficulty,
							next_vdf_difficulty = NextVDFDifficulty,
							steps = parse_32b_list(Steps),
							step_checkpoints_map = StepCheckpointsMap } },
			case decode_session_key(PrevSessionKeyBin) of
				undefined ->
					{ok, Update};
				error ->
					{error, invalid1};
				PrevSessionKey ->
					Session2 = Session#vdf_session{ prev_session_key = PrevSessionKey },
					{ok, Update#nonce_limiter_update{ session = Session2 }}
			end
	end;
binary_to_nonce_limiter_update(4, _Bin) ->
	{error, invalid2};

binary_to_nonce_limiter_update(_, _Bin) ->
	{error, invalid_format}.

decode_step_checkpoints_map(<<>>, Map) ->
	{ok, Map};
decode_step_checkpoints_map(<< StepNumber:64,
		CheckpointLen:16, Checkpoints:(CheckpointLen * 32)/binary, Rest/binary >>, Map)
			when CheckpointLen == ?VDF_CHECKPOINT_COUNT_IN_STEP ->
	decode_step_checkpoints_map(Rest, maps:put(StepNumber, parse_32b_list(Checkpoints), Map));
decode_step_checkpoints_map(_Bin, _Map) ->
	{error, invalid_checkpoints_map}.

parse_32b_list(<<>>) ->
	[];
parse_32b_list(<< El:32/binary, Rest/binary >>) ->
	[El | parse_32b_list(Rest)].

nonce_limiter_update_response_to_binary(#nonce_limiter_update_response{
		session_found = SessionFound, step_number = StepNumber, postpone = Postpone,
		format = Format }) ->
	SessionFoundBin = case SessionFound of false -> << 0:8 >>; _ -> << 1:8 >> end,
	<< SessionFoundBin/binary, (encode_int(StepNumber, 8))/binary, Postpone:8, Format:8 >>.

binary_to_nonce_limiter_update_response(<< SessionFoundBin:8, StepNumberSize:8,
		StepNumber:(StepNumberSize * 8) >>) ->
	binary_to_nonce_limiter_update_response(
		SessionFoundBin, StepNumberSize, StepNumber, 0, 1);
binary_to_nonce_limiter_update_response(<< SessionFoundBin:8, StepNumberSize:8,
		StepNumber:(StepNumberSize * 8), Postpone:8 >>) ->
	binary_to_nonce_limiter_update_response(
		SessionFoundBin, StepNumberSize, StepNumber, Postpone, 1);
binary_to_nonce_limiter_update_response(<< SessionFoundBin:8, StepNumberSize:8,
		StepNumber:(StepNumberSize * 8), Postpone:8, Format:8 >>) ->
	binary_to_nonce_limiter_update_response(
		SessionFoundBin, StepNumberSize, StepNumber, Postpone, Format);
binary_to_nonce_limiter_update_response(_Bin) ->
	{error, invalid2}.

binary_to_nonce_limiter_update_response(
	SessionFoundBin, StepNumberSize, StepNumber, Postpone, Format) 
		when SessionFoundBin == 0; SessionFoundBin == 1 ->
	SessionFound = case SessionFoundBin of 0 -> false; 1 -> true end,
	StepNumber2 = case StepNumberSize of 0 -> undefined; _ -> StepNumber end,
	{ok, #nonce_limiter_update_response{ session_found = SessionFound,
			step_number = StepNumber2, postpone = Postpone, format = Format }};
binary_to_nonce_limiter_update_response(
		_SessionFoundBin, _StepNumberSize, _StepNumber, _Postpone, _Format) ->
	{error, invalid1}.

encode_double_signing_proof(undefined) ->
	<< 0:8 >>;
encode_double_signing_proof(Proof) ->
	{Key, Sig1, CDiff1, PrevCDiff1, Preimage1, Sig2, CDiff2, PrevCDiff2, Preimage2} = Proof,
	<< 1:8, Key:512/binary, Sig1:512/binary,
		(ar_serialize:encode_int(CDiff1, 16))/binary,
		(ar_serialize:encode_int(PrevCDiff1, 16))/binary, Preimage1:64/binary,
		Sig2:512/binary, (ar_serialize:encode_int(CDiff2, 16))/binary,
		(ar_serialize:encode_int(PrevCDiff2, 16))/binary, Preimage2:64/binary >>.

%%%===================================================================
%%% Private functions.
%%%===================================================================

encode_post_2_6_fields(#block{ height = Height, hash_preimage = HashPreimage,
			recall_byte = RecallByte, reward = Reward,
			previous_solution_hash = PreviousSolutionHash,
			partition_number = PartitionNumber,
			signature = Sig, nonce_limiter_info = NonceLimiterInfo,
			poa2 = #poa{ chunk = Chunk, data_path = DataPath, tx_path = TXPath },
			recall_byte2 = RecallByte2, price_per_gib_minute = PricePerGiBMinute,
			scheduled_price_per_gib_minute = ScheduledPricePerGiBMinute,
			reward_history_hash = RewardHistoryHash, debt_supply = DebtSupply,
			kryder_plus_rate_multiplier = KryderPlusRateMultiplier,
			kryder_plus_rate_multiplier_latch = KryderPlusRateMultiplierLatch,
			denomination = Denomination, redenomination_height = RedenominationHeight,
			double_signing_proof = DoubleSigningProof,
			previous_cumulative_diff = PrevCDiff } = B) ->
	RewardKey = case B#block.reward_key of undefined -> <<>>; {_Type, Key} -> Key end,
	case Height >= ar_fork:height_2_6() of
		false ->
			<<>>;
		true ->
			<< (encode_bin(HashPreimage, 8))/binary, (encode_int(RecallByte, 16))/binary,
				(encode_int(Reward, 8))/binary, (encode_bin(Sig, 16))/binary,
				(encode_int(RecallByte2, 16))/binary,
				(encode_bin(PreviousSolutionHash, 8))/binary, PartitionNumber:256,
				(encode_nonce_limiter_info(NonceLimiterInfo))/binary,
				(encode_bin(Chunk, 24))/binary, (encode_bin(RewardKey, 16))/binary,
				(encode_bin(TXPath, 24))/binary, (encode_bin(DataPath, 24))/binary,
				(encode_int(PricePerGiBMinute, 8))/binary,
				(encode_int(ScheduledPricePerGiBMinute, 8))/binary,
				RewardHistoryHash:32/binary, (encode_int(DebtSupply, 8))/binary,
				KryderPlusRateMultiplier:24, KryderPlusRateMultiplierLatch:8,
				Denomination:24, (encode_int(RedenominationHeight, 8))/binary,
				(encode_int(PrevCDiff, 16))/binary,
				(encode_double_signing_proof(DoubleSigningProof))/binary,
				(encode_post_2_7_fields(B))/binary >>
	end.

encode_post_2_7_fields(#block{ height = Height,
		merkle_rebase_support_threshold = Threshold, chunk_hash = ChunkHash,
		chunk2_hash = Chunk2Hash,
		block_time_history_hash = BlockTimeHistoryHash,
		nonce_limiter_info = #nonce_limiter_info{ vdf_difficulty = VDFDifficulty,
				next_vdf_difficulty = NextVDFDifficulty } }) ->
	case Height >= ar_fork:height_2_7() of
		true ->
			<< (encode_int(Threshold, 16))/binary, ChunkHash:32/binary,
					(encode_bin(Chunk2Hash, 8))/binary,
					BlockTimeHistoryHash:32/binary,
					(encode_int(VDFDifficulty, 8))/binary,
					(encode_int(NextVDFDifficulty, 8))/binary >>;
		false ->
			<<>>
	end.

encode_nonce_limiter_info(#nonce_limiter_info{ output = Output, global_step_number = N,
		seed = Seed, next_seed = NextSeed, partition_upper_bound = PartitionUpperBound,
		next_partition_upper_bound = NextPartitionUpperBound, prev_output = PrevOutput,
		last_step_checkpoints = Checkpoints, steps = Steps }) ->
	CheckpointsLen = length(Checkpoints),
	StepsLen = length(Steps),
	<< Output:32/binary, N:64, Seed:48/binary, NextSeed:48/binary,
			(encode_bin(PrevOutput, 8))/binary,
			PartitionUpperBound:256, NextPartitionUpperBound:256,
			CheckpointsLen:16, (iolist_to_binary(Checkpoints))/binary,
			StepsLen:16, (iolist_to_binary(Steps))/binary >>.

encode_int(undefined, SizeBits) ->
	<< 0:SizeBits >>;
encode_int(N, SizeBits) ->
	Bin = binary:encode_unsigned(N, big),
	<< (byte_size(Bin)):SizeBits, Bin/binary >>.

encode_bin(undefined, SizeBits) ->
	<< 0:SizeBits >>;
encode_bin(Bin, SizeBits) ->
	<< (byte_size(Bin)):SizeBits, Bin/binary >>.

encode_bin_list(Bins, LenBits, ElemSizeBits) ->
	encode_bin_list(Bins, [], 0, LenBits, ElemSizeBits).

encode_bin_list([], Encoded, N, LenBits, _ElemSizeBits) ->
	<< N:LenBits, (iolist_to_binary(Encoded))/binary >>;
encode_bin_list([Bin | Bins], Encoded, N, LenBits, ElemSizeBits) ->
	Elem = encode_bin(Bin, ElemSizeBits),
	encode_bin_list(Bins, [Elem | Encoded], N + 1, LenBits, ElemSizeBits).

encode_transactions(TXs) ->
	encode_transactions(TXs, [], 0).

encode_transactions([], Encoded, N) ->
	<< N:16, (iolist_to_binary(Encoded))/binary >>;
encode_transactions([<< TXID:32/binary >> | TXs], Encoded, N) ->
	encode_transactions(TXs, [<< 32:24, TXID:32/binary >> | Encoded], N + 1);
encode_transactions([TX | TXs], Encoded, N) ->
	Bin = encode_tx(TX),
	TXSize = byte_size(Bin),
	encode_transactions(TXs, [<< TXSize:24, Bin/binary >> | Encoded], N + 1).

encode_tx(#tx{ format = Format, id = TXID, last_tx = LastTX, owner = Owner,
		tags = Tags, target = Target, quantity = Quantity, data = Data,
		data_size = DataSize, data_root = DataRoot, signature = Signature,
		reward = Reward, signature_type = SignatureType } = TX) ->
	<< Format:8, TXID:32/binary,
			(encode_bin(LastTX, 8))/binary, (encode_bin(Owner, 16))/binary,
			(encode_bin(Target, 8))/binary, (encode_int(Quantity, 8))/binary,
			(encode_int(DataSize, 16))/binary, (encode_bin(DataRoot, 8))/binary,
			(encode_bin(Signature, 16))/binary, (encode_int(Reward, 8))/binary,
			(encode_bin(Data, 24))/binary, (encode_tx_tags(Tags))/binary,
			(encode_signature_type(SignatureType))/binary,
			(may_be_encode_tx_denomination(TX))/binary >>.

encode_tx_tags(Tags) ->
	encode_tx_tags(Tags, [], 0).

encode_tx_tags([], Encoded, N) ->
	<< N:16, (iolist_to_binary(Encoded))/binary >>;
encode_tx_tags([{Name, Value} | Tags], Encoded, N) ->
	TagNameSize = byte_size(Name),
	TagValueSize = byte_size(Value),
	Tag = << TagNameSize:16, TagValueSize:16, Name/binary, Value/binary >>,
	encode_tx_tags(Tags, [Tag | Encoded], N + 1).

encode_signature_type(?DEFAULT_KEY_TYPE) ->
	<<>>;
encode_signature_type({?ECDSA_SIGN_ALG, secp256k1}) ->
	<< 1:8 >>;
encode_signature_type({?EDDSA_SIGN_ALG, ed25519}) ->
	<< 2:8 >>.

may_be_encode_tx_denomination(#tx{ denomination = 0 }) ->
	<<>>;
may_be_encode_tx_denomination(#tx{ denomination = Denomination }) ->
	<< Denomination:24 >>.

parse_block_tags_transactions(Bin, B) ->
	case parse_block_tags(Bin) of
		{error, Reason} ->
			{error, Reason};
		{ok, Tags, Rest} ->
			parse_block_transactions(Rest, B#block{ tags = Tags })
	end.

parse_block_transactions(Bin, B) ->
	case {parse_block_transactions(Bin), B#block.height < ar_fork:height_2_6()} of
		{{error, Reason}, _} ->
			{error, Reason};
		{{ok, TXs, <<>>}, true} ->
			{ok, B#block{ txs = TXs }};
		{{ok, TXs, Rest}, false} ->
			parse_block_post_2_6_fields(B#block{ txs = TXs }, Rest);
		_ ->
			{error, invalid_input1}
	end.

parse_block_post_2_6_fields(B, << HashPreimageSize:8, HashPreimage:HashPreimageSize/binary,
		RecallByteSize:16, RecallByte:(RecallByteSize * 8), RewardSize:8,
		Reward:(RewardSize * 8), SigSize:16, Sig:SigSize/binary,
		RecallByte2Size:16, RecallByte2:(RecallByte2Size * 8), PreviousSolutionHashSize:8,
		PreviousSolutionHash:PreviousSolutionHashSize/binary,
		PartitionNumber:256, NonceLimiterOutput:32/binary,
		GlobalStepNumber:64, Seed:48/binary, NextSeed:48/binary,
		PrevOutputSize:8, PrevOutput:PrevOutputSize/binary,
		PartitionUpperBound:256, NextPartitionUpperBound:256,
		LastCheckpointsLen:16, LastCheckpoints:(LastCheckpointsLen * 32)/binary,
		StepsLen:16, Steps:(StepsLen * 32)/binary,
		ChunkSize:24, Chunk:ChunkSize/binary, RewardKeySize:16,
		RewardKey:RewardKeySize/binary, TXPathSize:24, TXPath:TXPathSize/binary,
		DataPathSize:24, DataPath:DataPathSize/binary,
		PricePerGiBMinuteSize:8, PricePerGiBMinute:(PricePerGiBMinuteSize * 8),
		ScheduledPricePerGiBMinuteSize:8,
		ScheduledPricePerGiBMinute:(ScheduledPricePerGiBMinuteSize * 8),
		RewardHistoryHash:32/binary, DebtSupplySize:8, DebtSupply:(DebtSupplySize * 8),
		KryderPlusRateMultiplier:24, KryderPlusRateMultiplierLatch:8,
		Denomination:24, RedenominationHeightSize:8,
		RedenominationHeight:(RedenominationHeightSize * 8),
		PrevCDiffSize:16, PrevCDiff:(PrevCDiffSize * 8),
		Rest/binary >>) ->
	%% The only block where recall_byte may be undefined is the genesis block
	%% of a new weave.
	RecallByte_2 = case RecallByteSize of 0 -> undefined; _ -> RecallByte end,
	Height = B#block.height,
	Nonce = binary:decode_unsigned(B#block.nonce, big),
	NonceLimiterInfo = #nonce_limiter_info{ output = NonceLimiterOutput,
			prev_output = PrevOutput, global_step_number = GlobalStepNumber,
			seed = Seed, next_seed = NextSeed,
			partition_upper_bound = PartitionUpperBound,
			next_partition_upper_bound = NextPartitionUpperBound,
			last_step_checkpoints = parse_checkpoints(LastCheckpoints, Height),
			steps = parse_checkpoints(Steps, Height) },
	RecallByte2_2 = case RecallByte2Size of 0 -> undefined; _ -> RecallByte2 end,
	B2 = B#block{ hash_preimage = HashPreimage, recall_byte = RecallByte_2,
			reward = Reward, nonce = Nonce, recall_byte2 = RecallByte2_2,
			previous_solution_hash = PreviousSolutionHash,
			signature = Sig, partition_number = PartitionNumber,
			reward_key = {{?RSA_SIGN_ALG, 65537}, RewardKey},
			nonce_limiter_info = NonceLimiterInfo,
			poa2 = #poa{ chunk = Chunk, data_path = DataPath, tx_path = TXPath },
			price_per_gib_minute = PricePerGiBMinute,
			scheduled_price_per_gib_minute = ScheduledPricePerGiBMinute,
			reward_history_hash = RewardHistoryHash, debt_supply = DebtSupply,
			kryder_plus_rate_multiplier = KryderPlusRateMultiplier,
			kryder_plus_rate_multiplier_latch = KryderPlusRateMultiplierLatch,
			denomination = Denomination, redenomination_height = RedenominationHeight,
			previous_cumulative_diff = PrevCDiff },
	parse_double_signing_proof(Rest, B2);
parse_block_post_2_6_fields(_B, _Rest) ->
	{error, invalid_input4}.

parse_checkpoints(<<>>, 0) ->
	[];
parse_checkpoints(_, 0) ->
	{error, invalid_checkpoints};
parse_checkpoints(<< Checkpoint:32/binary >>, _Height) ->
	%% The block must have at least one checkpoint (the last nonce limiter output).
	[Checkpoint];
parse_checkpoints(<< Checkpoint:32/binary, Rest/binary >>, Height) ->
	[Checkpoint | parse_checkpoints(Rest, Height)].

parse_block_tags(<< TagsLen:16, Rest/binary >>) when TagsLen =< 2048 ->
	parse_block_tags(TagsLen, Rest, [], 0);
parse_block_tags(_Bin) ->
	{error, invalid_tags_input}.

parse_block_tags(0, Rest, Tags, _TotalSize) ->
	{ok, Tags, Rest};
parse_block_tags(N, << TagSize:16, Tag:TagSize/binary, Rest/binary >>, Tags, TotalSize)
		when TotalSize + TagSize =< 2048 ->
	parse_block_tags(N - 1, << Rest/binary >>, [Tag | Tags], TotalSize + TagSize);
parse_block_tags(_N, _Bin, _Tags, _TotalSize) ->
	{error, invalid_tag_input}.

parse_block_transactions(<< Count:16, Rest/binary >>) when Count =< 1000 ->
	parse_block_transactions(Count, Rest, []);
parse_block_transactions(_Bin) ->
	{error, invalid_transactions_input}.

parse_block_transactions(0, Rest, TXs) ->
	{ok, TXs, Rest};
parse_block_transactions(N, << Size:24, Bin:Size/binary, Rest/binary >>, TXs)
		when N > 0 ->
	case parse_tx(Bin) of
		{error, Reason} ->
			{error, Reason};
		{ok, TX} ->
			parse_block_transactions(N - 1, Rest, [TX | TXs])
	end;
parse_block_transactions(_N, _Rest, _TXs) ->
	{error, invalid_transactions2_input}.

parse_double_signing_proof(<< 0:8, Rest/binary >>, B) ->
	parse_post_2_7_fields(Rest, B);
parse_double_signing_proof(<< 1:8, Key:512/binary, Sig1:512/binary,
		CDiff1Size:16, CDiff1:(CDiff1Size * 8),
		PrevCDiff1Size:16, PrevCDiff1:(PrevCDiff1Size * 8),
		Preimage1:64/binary, Sig2:512/binary, CDiff2Size:16, CDiff2:(CDiff2Size * 8),
		PrevCDiff2Size:16, PrevCDiff2:(PrevCDiff2Size * 8),
		Preimage2:64/binary, Rest/binary >>, B) ->
	B2 = B#block{ double_signing_proof = {Key, Sig1, CDiff1, PrevCDiff1, Preimage1,
			Sig2, CDiff2, PrevCDiff2, Preimage2} },
	parse_post_2_7_fields(Rest, B2);
parse_double_signing_proof(_Bin, _B) ->
	{error, invalid_double_signing_proof_input}.

parse_post_2_7_fields(Rest, #block{ height = Height } = B) ->
	case {Rest, Height >= ar_fork:height_2_7()} of
		{<<>>, false} ->
			{ok, B};
		{<< ThresholdSize:16, Threshold:(ThresholdSize*8), ChunkHash:32/binary,
				Chunk2HashSize:8, Chunk2Hash:Chunk2HashSize/binary,
				BlockTimeHistoryHash:32/binary,
				VDFDifficultySize:8, VDFDifficulty:(VDFDifficultySize * 8),
				NextVDFDifficultySize:8, NextVDFDifficulty:(NextVDFDifficultySize * 8) >>, true} ->
			Chunk2Hash2 = case Chunk2HashSize of 0 -> undefined; _ -> Chunk2Hash end,
			{ok, B#block{ merkle_rebase_support_threshold = Threshold,
					chunk_hash = ChunkHash, chunk2_hash = Chunk2Hash2,
					block_time_history_hash = BlockTimeHistoryHash,
					nonce_limiter_info = (B#block.nonce_limiter_info)#nonce_limiter_info{
							vdf_difficulty = VDFDifficulty,
							next_vdf_difficulty = NextVDFDifficulty } }};
		_ ->
			{error, invalid_merkle_rebase_support_threshold}
	end.

parse_tx(<< TXID:32/binary >>) ->
	{ok, TXID};
parse_tx(<< Format:8, TXID:32/binary,
		LastTXSize:8, LastTX:LastTXSize/binary,
		OwnerSize:16, Owner:OwnerSize/binary,
		TargetSize:8, Target:TargetSize/binary,
		QuantitySize:8, Quantity:(QuantitySize * 8),
		DataSizeSize:16, DataSize:(DataSizeSize * 8),
		DataRootSize:8, DataRoot:DataRootSize/binary,
		SignatureSize:16, Signature:SignatureSize/binary,
		RewardSize:8, Reward:(RewardSize * 8),
		DataEncodingSize:24, Data:DataEncodingSize/binary,
		Rest/binary >>) when Format == 1 orelse Format == 2 ->
	case parse_tx_tags(Rest) of
		{error, Reason} ->
			{error, Reason};
		{ok, Tags, Rest2} ->
			case parse_tx_denomination(Rest2) of
				{ok, Denomination} ->
					DataSize2 = case Format of 1 -> byte_size(Data); _ -> DataSize end,
					{ok, #tx{ format = Format, id = TXID, last_tx = LastTX, owner = Owner,
							target = Target, quantity = Quantity, data_size = DataSize2,
							data_root = DataRoot, signature = Signature, reward = Reward,
							data = Data, tags = Tags, denomination = Denomination }};
				{error, Reason} ->
					{error, Reason}
			end
	end;
parse_tx(_Bin) ->
	{error, invalid_tx_input}.

parse_tx_tags(<< TagsLen:16, Rest/binary >>) when TagsLen =< 2048 ->
	parse_tx_tags(TagsLen, Rest, []);
parse_tx_tags(_Bin) ->
	{error, invalid_tx_tags_input}.

parse_tx_tags(0, Rest, Tags) ->
	{ok, Tags, Rest};
parse_tx_tags(N, << TagNameSize:16, TagValueSize:16,
		TagName:TagNameSize/binary, TagValue:TagValueSize/binary, Rest/binary >>, Tags)
		when N > 0 ->
	parse_tx_tags(N - 1, Rest, [{TagName, TagValue} | Tags]);
parse_tx_tags(_N, _Bin, _Tags) ->
	{error, invalid_tx_tag_input}.

parse_tx_denomination(<<>>) ->
	{ok, 0};
parse_tx_denomination(<< Denomination:24 >>) when Denomination > 0 ->
	{ok, Denomination};
parse_tx_denomination(_Rest) ->
	{error, invalid_denomination}.

%parse_signature_type(<<>>) ->
%	{ok, ?DEFAULT_KEY_TYPE};
%parse_signature_type(<< 1:8 >>) ->
%	{ok, {?ECDSA_SIGN_ALG, secp256k1}};
%parse_signature_type(<< 2:8 >>) ->
%	{ok, {?EDDSA_SIGN_ALG, ed25519}};
%parse_signature_type(_Rest) ->
%	{error, invalid_input}.

tx_to_binary(TX) ->
	Bin = encode_tx(TX),
	TXSize = byte_size(Bin),
	<< TXSize:24, Bin/binary >>.

binary_to_tx(<< Size:24, Bin:Size/binary >>) ->
	parse_tx(Bin);
binary_to_tx(_Rest) ->
	{error, invalid_input7}.

block_announcement_to_binary(#block_announcement{ indep_hash = H,
		previous_block = PrevH, tx_prefixes = L, recall_byte = O, recall_byte2 = O2,
		solution_hash = SolutionH }) ->
	<< H:48/binary, PrevH:48/binary, (encode_int(O, 8))/binary,
			(encode_tx_prefixes(L))/binary, (case O2 of undefined -> <<>>;
					_ -> encode_int(O2, 8) end)/binary,
			(encode_solution_hash(SolutionH))/binary >>.

encode_tx_prefixes(L) ->
	<< (length(L)):16, (encode_tx_prefixes(L, []))/binary >>.

encode_tx_prefixes([], Encoded) ->
	iolist_to_binary(Encoded);
encode_tx_prefixes([Prefix | Prefixes], Encoded) ->
	encode_tx_prefixes(Prefixes, [<< Prefix:8/binary >> | Encoded]).

encode_solution_hash(undefined) ->
	<<>>;
encode_solution_hash(H) ->
	<< H:32/binary >>.

binary_to_block_announcement(<< H:48/binary, PrevH:48/binary,
		RecallByteSize:8, RecallByte:(RecallByteSize * 8), N:16, Rest/binary >>) ->
	RecallByte2 = case RecallByteSize of 0 -> undefined; _ -> RecallByte end,
	case parse_tx_prefixes_and_recall_byte2_and_solution_hash(N, Rest) of
		{error, Reason} ->
			{error, Reason};
		{ok, {Prefixes, RecallByte3, SolutionH}} ->
			{ok, #block_announcement{ indep_hash = H, previous_block = PrevH,
					recall_byte = RecallByte2, tx_prefixes = Prefixes,
					recall_byte2 = RecallByte3, solution_hash = SolutionH }}
	end;
binary_to_block_announcement(_Rest) ->
	{error, invalid_input}.

parse_tx_prefixes_and_recall_byte2_and_solution_hash(N, Bin) ->
	parse_tx_prefixes_and_recall_byte2_and_solution_hash(N, Bin, []).

parse_tx_prefixes_and_recall_byte2_and_solution_hash(0, Rest, Prefixes) ->
	case Rest of
		<<>> ->
			{ok, {Prefixes, undefined, undefined}};
		<< RecallByte2Size:8, RecallByte2:(RecallByte2Size * 8), SolutionH:32/binary >> ->
			{ok, {Prefixes, RecallByte2, SolutionH}};
		<< SolutionH:32/binary >> ->
			{ok, {Prefixes, undefined, SolutionH}};
		_ ->
			{error, invalid_recall_byte2_and_solution_hash_input}
	end;
parse_tx_prefixes_and_recall_byte2_and_solution_hash(N, << Prefix:8/binary, Rest/binary >>,
		Prefixes) when N > 0 ->
	parse_tx_prefixes_and_recall_byte2_and_solution_hash(N - 1, Rest, [Prefix | Prefixes]);
parse_tx_prefixes_and_recall_byte2_and_solution_hash(_N, _Rest, _Prefixes) ->
	{error, invalid_tx_prefixes_input}.

binary_to_block_announcement_response(<< ChunkMissing:8, Rest/binary >>)
		when ChunkMissing == 1 orelse ChunkMissing == 0 ->
	case parse_missing_tx_indices_and_missing_chunk2(Rest) of
		{ok, {Indices, MissingChunk2}} ->
			{ok, #block_announcement_response{ missing_chunk = ar_util:int_to_bool(ChunkMissing),
					missing_tx_indices = Indices, missing_chunk2 = MissingChunk2 }};
		{error, Reason} ->
			{error, Reason}
	end;
binary_to_block_announcement_response(_Rest) ->
	{error, invalid_block_announcement_response_input}.

parse_missing_tx_indices_and_missing_chunk2(Bin) ->
	parse_missing_tx_indices_and_missing_chunk2(Bin, []).

parse_missing_tx_indices_and_missing_chunk2(<<>>, Indices) ->
	{ok, {Indices, undefined}};
parse_missing_tx_indices_and_missing_chunk2(<< MissingChunk2:8 >>, Indices) ->
	case MissingChunk2 of
		0 ->
			{ok, {Indices, false}};
		1 ->
			{ok, {Indices, true}};
		_ ->
			{error, invalid_missing_chunk2_input}
	end;
parse_missing_tx_indices_and_missing_chunk2(<< Index:16, Rest/binary >>, Indices) ->
	parse_missing_tx_indices_and_missing_chunk2(Rest, [Index | Indices]);
parse_missing_tx_indices_and_missing_chunk2(_Rest, _Indices) ->
	{error, invalid_missing_tx_indices_input}.

block_announcement_response_to_binary(#block_announcement_response{
		missing_tx_indices = L, missing_chunk = Reply, missing_chunk2 = Reply2 }) ->
	<< (ar_util:bool_to_int(Reply)):8, (encode_missing_tx_indices(L))/binary,
			(case Reply2 of undefined -> <<>>; false -> << 0:8 >>;
					true -> << 1:8 >> end)/binary >>.

encode_missing_tx_indices(L) ->
	encode_missing_tx_indices(L, []).

encode_missing_tx_indices([], Encoded) ->
	iolist_to_binary(Encoded);
encode_missing_tx_indices([Index | Indices], Encoded) ->
	encode_missing_tx_indices(Indices, [<< Index:16 >> | Encoded]).

poa_map_to_binary(#{ chunk := Chunk, tx_path := TXPath, data_path := DataPath,
		packing := Packing }) ->
	Packing2 =
		case Packing of
			unpacked ->
				<<"unpacked">>;
			spora_2_5 ->
				<<"spora_2_5">>;
			{spora_2_6, Addr} ->
				iolist_to_binary([<<"spora_2_6_">>, ar_util:encode(Addr)])
		end,
	<< (encode_bin(Chunk, 24))/binary, (encode_bin(TXPath, 24))/binary,
			(encode_bin(DataPath, 24))/binary, (encode_bin(Packing2, 8))/binary >>.

poa_no_chunk_map_to_binary(#{ tx_path := TXPath, data_path := DataPath }) ->
	<< (encode_bin(TXPath, 24))/binary, (encode_bin(DataPath, 24))/binary >>.

binary_to_poa(<< ChunkSize:24, Chunk:ChunkSize/binary,
		TXPathSize:24, TXPath:TXPathSize/binary,
		DataPathSize:24, DataPath:DataPathSize/binary,
		PackingSize:8, Packing:PackingSize/binary >>) ->
	Packing2 =
		case Packing of
			<<"unpacked">> ->
				unpacked;
			<<"spora_2_5">> ->
				spora_2_5;
			<< Type:10/binary, Addr/binary >>
					when Type == <<"spora_2_6_">>, byte_size(Addr) =< 64 ->
				case ar_util:safe_decode(Addr) of
					{ok, DecodedAddr} ->
						{spora_2_6, DecodedAddr};
					_ ->
						error
				end;
			_ ->
				error
		end,
	case Packing2 of
		error ->
			{error, invalid_packing};
		_ ->
			{ok, #{ chunk => Chunk, data_path => DataPath, tx_path => TXPath,
					packing => Packing2 }}
	end;
binary_to_poa(_Rest) ->
	{error, invalid_input}.

binary_to_no_chunk_map(<< TXPathSize:24, TXPath:TXPathSize/binary,
		DataPathSize:24, DataPath:DataPathSize/binary >>) ->
	{ok, #{ data_path => DataPath, tx_path => TXPath }};
binary_to_no_chunk_map(_Rest) ->
	{error, invalid_input}.

block_index_to_binary(BI) ->
	block_index_to_binary(BI, []).

block_index_to_binary([], Encoded) ->
	iolist_to_binary(Encoded);
block_index_to_binary([{BH, WeaveSize, TXRoot} | BI], Encoded) ->
	block_index_to_binary(BI,
			[<< BH:48/binary, (encode_int(WeaveSize, 16))/binary,
				(encode_bin(TXRoot, 8))/binary >> | Encoded]).

binary_to_block_index(Bin) ->
	binary_to_block_index(Bin, []).

binary_to_block_index(<<>>, BI) ->
	{ok, BI};
binary_to_block_index(<< BH:48/binary, WeaveSizeSize:16, WeaveSize:(WeaveSizeSize * 8),
		TXRootSize:8, TXRoot:TXRootSize/binary, Rest/binary >>, BI) ->
	binary_to_block_index(Rest, [{BH, WeaveSize, TXRoot} | BI]);
binary_to_block_index(_Rest, _BI) ->
	{error, invalid_input}.

%% @doc Take a JSON struct and produce JSON string.
jsonify(JSONStruct) ->
	iolist_to_binary(jiffy:encode(JSONStruct)).

%% @doc Decode JSON string into a JSON struct.
%% @deprecated In favor of json_decode/1
dejsonify(JSON) ->
	case json_decode(JSON) of
		{ok, V} -> V;
		{error, Reason} -> throw({error, Reason})
	end.

json_decode(JSON) ->
	json_decode(JSON, []).

json_decode(JSON, JiffyOpts) ->
	case catch jiffy:decode(JSON, JiffyOpts) of
		{'EXIT', {Reason, _Stacktrace}} ->
			{error, Reason};
		DecodedJSON ->
			{ok, DecodedJSON}
	end.

delete_keys([], Proplist) ->
	Proplist;
delete_keys([Key | Keys], Proplist) ->
	delete_keys(
		Keys,
		lists:keydelete(Key, 1, Proplist)
	).

%% @doc Convert parsed JSON blocks fields from a HTTP request into a block.
json_struct_to_block(JSONBlock) when is_binary(JSONBlock) ->
	json_struct_to_block(dejsonify(JSONBlock));
json_struct_to_block({BlockStruct}) ->
	Height = find_value(<<"height">>, BlockStruct),
	true = is_integer(Height) andalso Height < ar_fork:height_2_6(),
	Fork_2_5 = ar_fork:height_2_5(),
	TXIDs = find_value(<<"txs">>, BlockStruct),
	WalletList = find_value(<<"wallet_list">>, BlockStruct),
	HashList = find_value(<<"hash_list">>, BlockStruct),
	TagsValue = find_value(<<"tags">>, BlockStruct),
	Tags =
		case Height >= Fork_2_5 of
			true ->
				[ar_util:decode(Tag) || Tag <- TagsValue];
			false ->
				true = (byte_size(list_to_binary(TagsValue)) =< 2048),
				TagsValue
		end,
	Fork_1_8 = ar_fork:height_1_8(),
	Fork_1_6 = ar_fork:height_1_6(),
	CDiff =
		case find_value(<<"cumulative_diff">>, BlockStruct) of
			_ when Height < Fork_1_6 -> 0;
			undefined -> 0; % In case it's an invalid block (in the pre-fork format).
			BinaryCDiff when Height >= Fork_1_8 -> binary_to_integer(BinaryCDiff);
			CD -> CD
		end,
	Diff =
		case find_value(<<"diff">>, BlockStruct) of
			BinaryDiff when Height >= Fork_1_8 -> binary_to_integer(BinaryDiff);
			D -> D
		end,
	MR =
		case find_value(<<"hash_list_merkle">>, BlockStruct) of
			_ when Height < Fork_1_6 -> <<>>;
			undefined -> <<>>; % In case it's an invalid block (in the pre-fork format).
			R -> ar_util:decode(R)
		end,
	RewardAddr =
		case find_value(<<"reward_addr">>, BlockStruct) of
			<<"unclaimed">> -> unclaimed;
			AddrBinary -> AddrBinary
		end,
	RewardAddr2 =
		case RewardAddr of
			unclaimed ->
				unclaimed;
			_ ->
				ar_wallet:base64_address_with_optional_checksum_to_decoded_address(RewardAddr)
		end,
	{RewardPool, BlockSize, WeaveSize} =
		case Height >= ar_fork:height_2_4() of
			true ->
				{
					binary_to_integer(find_value(<<"reward_pool">>, BlockStruct)),
					binary_to_integer(find_value(<<"block_size">>, BlockStruct)),
					binary_to_integer(find_value(<<"weave_size">>, BlockStruct))
				};
			false ->
				{
					find_value(<<"reward_pool">>, BlockStruct),
					find_value(<<"block_size">>, BlockStruct),
					find_value(<<"weave_size">>, BlockStruct)
				}
		end,
	{Rate, ScheduledRate, Packing_2_5_Threshold, StrictDataSplitThreshold} =
		case Height >= Fork_2_5 of
			true ->
				[RateDividendBinary, RateDivisorBinary] =
					find_value(<<"usd_to_ar_rate">>, BlockStruct),
				[ScheduledRateDividendBinary, ScheduledRateDivisorBinary] =
					find_value(<<"scheduled_usd_to_ar_rate">>, BlockStruct),
				{{binary_to_integer(RateDividendBinary),
						binary_to_integer(RateDivisorBinary)},
					{binary_to_integer(ScheduledRateDividendBinary),
						binary_to_integer(ScheduledRateDivisorBinary)},
							binary_to_integer(find_value(<<"packing_2_5_threshold">>,
								BlockStruct)),
							binary_to_integer(find_value(<<"strict_data_split_threshold">>,
								BlockStruct))};
			false ->
				{undefined, undefined, undefined, undefined}
		end,
	Timestamp = find_value(<<"timestamp">>, BlockStruct),
	true = is_integer(Timestamp),
	LastRetarget = find_value(<<"last_retarget">>, BlockStruct),
	true = is_integer(LastRetarget),
	DecodedTXIDs = [ar_util:decode(TXID) || TXID <- TXIDs],
	[] = [TXID || TXID <- DecodedTXIDs, byte_size(TXID) /= 32],
	#block{
		nonce = ar_util:decode(find_value(<<"nonce">>, BlockStruct)),
		previous_block = ar_util:decode(find_value(<<"previous_block">>, BlockStruct)),
		timestamp = Timestamp,
		last_retarget = LastRetarget,
		diff = Diff,
		height = Height,
		hash = ar_util:decode(find_value(<<"hash">>, BlockStruct)),
		indep_hash = ar_util:decode(find_value(<<"indep_hash">>, BlockStruct)),
		txs = DecodedTXIDs,
		hash_list =
			case HashList of
				undefined -> unset;
				_		  -> [ar_util:decode(Hash) || Hash <- HashList]
			end,
		wallet_list = ar_util:decode(WalletList),
		reward_addr = RewardAddr2,
		tags = Tags,
		reward_pool = RewardPool,
		weave_size = WeaveSize,
		block_size = BlockSize,
		cumulative_diff = CDiff,
		hash_list_merkle = MR,
		tx_root =
			case find_value(<<"tx_root">>, BlockStruct) of
				undefined -> <<>>;
				Root -> ar_util:decode(Root)
			end,
		poa =
			case find_value(<<"poa">>, BlockStruct) of
				undefined -> #poa{};
				POAStruct -> json_struct_to_poa(POAStruct)
			end,
		usd_to_ar_rate = Rate,
		scheduled_usd_to_ar_rate = ScheduledRate,
		packing_2_5_threshold = Packing_2_5_Threshold,
		strict_data_split_threshold = StrictDataSplitThreshold
	}.

%% @doc Convert a transaction record into a JSON struct.
tx_to_json_struct(
	#tx{
		id = ID,
		format = Format,
		last_tx = Last,
		owner = Owner,
		tags = Tags,
		target = Target,
		quantity = Quantity,
		data = Data,
		reward = Reward,
		signature = Sig,
		data_size = DataSize,
		data_root = DataRoot,
		denomination = Denomination
	}) ->
	Fields = [
		{format,
			case Format of
				undefined ->
					1;
				_ ->
					Format
			end},
		{id, ar_util:encode(ID)},
		{last_tx, ar_util:encode(Last)},
		{owner, ar_util:encode(Owner)},
		{tags,
			lists:map(
				fun({Name, Value}) ->
					{
						[
							{name, ar_util:encode(Name)},
							{value, ar_util:encode(Value)}
						]
					}
				end,
				Tags
			)
		},
		{target, ar_util:encode(Target)},
		{quantity, integer_to_binary(Quantity)},
		{data, ar_util:encode(Data)},
		{data_size, integer_to_binary(DataSize)},
		{data_tree, []},
		{data_root, ar_util:encode(DataRoot)},
		{reward, integer_to_binary(Reward)},
		{signature, ar_util:encode(Sig)}
	],
	Fields2 =
		case Denomination > 0 of
			true ->
				Fields ++ [{denomination, integer_to_binary(Denomination)}];
			false ->
				Fields
		end,
	{Fields2}.

poa_to_json_struct(POA) ->
	{[
		{option, integer_to_binary(POA#poa.option)},
		{tx_path, ar_util:encode(POA#poa.tx_path)},
		{data_path, ar_util:encode(POA#poa.data_path)},
		{chunk, ar_util:encode(POA#poa.chunk)}
	]}.

nonce_limiter_info_to_json_struct(Height,
		#nonce_limiter_info{ output = Output, global_step_number = N,
		seed = Seed, next_seed = NextSeed, partition_upper_bound = ZoneUpperBound,
		next_partition_upper_bound = NextZoneUpperBound, last_step_checkpoints = Checkpoints,
		steps = Steps, prev_output = PrevOutput,
		vdf_difficulty = VDFDifficulty, next_vdf_difficulty = NextVDFDifficulty }) ->
	Fields = [{output, ar_util:encode(Output)}, {global_step_number, N},
			{seed, ar_util:encode(Seed)},
			{next_seed, ar_util:encode(NextSeed)}, {zone_upper_bound, ZoneUpperBound},
			{next_zone_upper_bound, NextZoneUpperBound},
			{prev_output, ar_util:encode(PrevOutput)},
			{last_step_checkpoints, [ar_util:encode(Elem) || Elem <- Checkpoints]},
			%% Keeping  'checkpoints' as JSON key (rather than 'steps') for backwards
			%% compatibility.
			{checkpoints, [ar_util:encode(Elem) || Elem <- Steps]}],
	Fields2 =
		case Height >= ar_fork:height_2_7() of
			false ->
				Fields;
			true ->
				Fields ++ [{vdf_difficulty, integer_to_binary(VDFDifficulty)},
						{next_vdf_difficulty, integer_to_binary(NextVDFDifficulty)}]
		end,
	{Fields2}.

diff_pair_to_json_list(DiffPair) ->
	{PoA1Diff, Diff} = DiffPair,
	[
		ar_util:integer_to_binary(PoA1Diff),
		ar_util:integer_to_binary(Diff)
	].

json_struct_to_poa({JSONStruct}) ->
	#poa{
		option = binary_to_integer(find_value(<<"option">>, JSONStruct)),
		tx_path = ar_util:decode(find_value(<<"tx_path">>, JSONStruct)),
		data_path = ar_util:decode(find_value(<<"data_path">>, JSONStruct)),
		chunk = ar_util:decode(find_value(<<"chunk">>, JSONStruct))
	}.

% FIXME. remove [return_maps] in ar_http_iface_middleware and make code uniform
json_struct_to_poa_from_map(JSONStruct) ->
	#poa{
		option = binary_to_integer(maps:get(<<"option">>, JSONStruct)),
		tx_path = ar_util:decode(maps:get(<<"tx_path">>, JSONStruct)),
		data_path = ar_util:decode(maps:get(<<"data_path">>, JSONStruct)),
		chunk = ar_util:decode(maps:get(<<"chunk">>, JSONStruct))
	}.

%% @doc Convert parsed JSON tx fields from a HTTP request into a
%% transaction record.
json_struct_to_tx(JSONTX) when is_binary(JSONTX) ->
	json_struct_to_tx(dejsonify(JSONTX));
json_struct_to_tx({TXStruct}) ->
	json_struct_to_tx(TXStruct, true).

json_struct_to_v1_tx(JSONTX) when is_binary(JSONTX) ->
	{TXStruct} = dejsonify(JSONTX),
	json_struct_to_tx(TXStruct, false).

json_struct_to_tx(TXStruct, ComputeDataSize) ->
	Tags =
		case find_value(<<"tags">>, TXStruct) of
			undefined ->
				[];
			Xs ->
				Xs
		end,
	Data = ar_util:decode(find_value(<<"data">>, TXStruct)),
	Format =
		case find_value(<<"format">>, TXStruct) of
			undefined ->
				1;
			N when is_integer(N) ->
				N;
			N when is_binary(N) ->
				binary_to_integer(N)
		end,
	Denomination =
		case find_value(<<"denomination">>, TXStruct) of
			undefined ->
				0;
			EncodedDenomination ->
				MaybeDenomination = binary_to_integer(EncodedDenomination),
				true = MaybeDenomination > 0,
				MaybeDenomination
		end,
	TXID = ar_util:decode(find_value(<<"id">>, TXStruct)),
	32 = byte_size(TXID),
	#tx{
		format = Format,
		id = TXID,
		last_tx = ar_util:decode(find_value(<<"last_tx">>, TXStruct)),
		owner = ar_util:decode(find_value(<<"owner">>, TXStruct)),
		tags = [{ar_util:decode(Name), ar_util:decode(Value)}
				%% Only the elements matching this pattern are included in the list.
				|| {[{<<"name">>, Name}, {<<"value">>, Value}]} <- Tags],
		target = ar_wallet:base64_address_with_optional_checksum_to_decoded_address(
				find_value(<<"target">>, TXStruct)),
		quantity = binary_to_integer(find_value(<<"quantity">>, TXStruct)),
		data = Data,
		reward = binary_to_integer(find_value(<<"reward">>, TXStruct)),
		signature = ar_util:decode(find_value(<<"signature">>, TXStruct)),
		data_size = parse_data_size(Format, TXStruct, Data, ComputeDataSize),
		data_root =
			case find_value(<<"data_root">>, TXStruct) of
				undefined -> <<>>;
				DR -> ar_util:decode(DR)
			end,
		denomination = Denomination
	}.

json_list_to_diff_pair(List) ->
	[PoA1DiffBin, DiffBin] = 
		case List of
			undefined -> [<<"0">>, <<"0">>];
			_ -> List
		end,
	PoA1Diff = ar_util:binary_to_integer(PoA1DiffBin),
	Diff = ar_util:binary_to_integer(DiffBin),
	{PoA1Diff, Diff}.
	
parse_data_size(1, _TXStruct, Data, true) ->
	byte_size(Data);
parse_data_size(_Format, TXStruct, _Data, _ComputeDataSize) ->
	binary_to_integer(find_value(<<"data_size">>, TXStruct)).

etf_to_wallet_chunk_response(ETF) ->
	catch etf_to_wallet_chunk_response_unsafe(ETF).

etf_to_wallet_chunk_response_unsafe(ETF) ->
	#{ next_cursor := NextCursor, wallets := Wallets } = binary_to_term(ETF, [safe]),
	true = is_binary(NextCursor) orelse NextCursor == last,
	lists:foreach(
		fun	({Addr, {Balance, LastTX}})
						when is_binary(Addr), is_binary(LastTX), is_integer(Balance),
							Balance >= 0 ->
				ok;
			({Addr, {Balance, LastTX, Denomination, MiningPermission}})
						when is_binary(Addr), is_binary(LastTX), is_integer(Balance),
								Balance >= 0,
								is_integer(Denomination),
								Denomination > 0,
								is_boolean(MiningPermission) ->
				ok
		end,
		Wallets
	),
	{ok, #{ next_cursor => NextCursor, wallets => Wallets }}.

%% @doc Convert a wallet list into a JSON struct.
%% The order of the wallets is somewhat weird for historical reasons. If the reward address,
%% appears in the list for the first time, it is placed on the first position. Except for that,
%% wallets are sorted in the alphabetical order.
wallet_list_to_json_struct(RewardAddr, IsRewardAddrNew, WL) ->
	List = ar_patricia_tree:foldr(
		fun(Addr, Value, Acc) ->
			case Addr == RewardAddr andalso IsRewardAddrNew of
				true ->
					Acc;
				false ->
					[wallet_to_json_struct(Addr, Value) | Acc]
			end
		end,
		[],
		WL
	),
	case {ar_patricia_tree:get(RewardAddr, WL), IsRewardAddrNew} of
		{not_found, _} ->
			List;
		{_, false} ->
			List;
		{Value, true} ->
			%% Place the reward wallet first, for backwards-compatibility.
			[wallet_to_json_struct(RewardAddr, Value) | List]
	end.

wallet_to_json_struct(Address, {Balance, LastTX}) ->
	{[{address, ar_util:encode(Address)}, {balance, list_to_binary(integer_to_list(Balance))},
			{last_tx, ar_util:encode(LastTX)}]};
wallet_to_json_struct(Address, {Balance, LastTX, Denomination, MiningPermission}) ->
	{[{address, ar_util:encode(Address)}, {balance, list_to_binary(integer_to_list(Balance))},
			{last_tx, ar_util:encode(LastTX)}, {denomination, Denomination},
			{mining_permission, MiningPermission}]}.

%% @doc Convert parsed JSON from fields into a valid wallet list.
json_struct_to_wallet_list(JSON) when is_binary(JSON) ->
	json_struct_to_wallet_list(dejsonify(JSON));
json_struct_to_wallet_list(WalletsStruct) ->
	lists:foldl(
		fun(WalletStruct, Acc) ->
			{Address, Value} = json_struct_to_wallet(WalletStruct),
			ar_patricia_tree:insert(Address, Value, Acc)
		end,
		ar_patricia_tree:new(),
		WalletsStruct
	).

json_struct_to_wallet({Wallet}) ->
	Address = ar_util:decode(find_value(<<"address">>, Wallet)),
	Balance = binary_to_integer(find_value(<<"balance">>, Wallet)),
	true = Balance >= 0,
	LastTX = ar_util:decode(find_value(<<"last_tx">>, Wallet)),
	case find_value(<<"denomination">>, Wallet) of
		undefined ->
			{Address, {Balance, LastTX}};
		Denomination when is_integer(Denomination), Denomination > 0 ->
			MiningPermission = find_value(<<"mining_permission">>, Wallet),
			true = is_boolean(MiningPermission),
			{Address, {Balance, LastTX, Denomination, MiningPermission}}
	end.

%% @doc Find the value associated with a key in parsed a JSON structure list.
find_value(Key, List) ->
	case lists:keyfind(Key, 1, List) of
		{Key, Val} -> Val;
		false -> undefined
	end.

%% @doc Convert an ARQL query into a JSON struct
query_to_json_struct({Op, Expr1, Expr2}) ->
	{
		[
			{op, list_to_binary(atom_to_list(Op))},
			{expr1, query_to_json_struct(Expr1)},
			{expr2, query_to_json_struct(Expr2)}
		]
	};
query_to_json_struct(Expr) ->
	Expr.

%% @doc Convert parsed JSON from fields into an internal ARQL query.
json_struct_to_query(QueryJSON) ->
	case json_decode(QueryJSON) of
		{ok, Decoded} ->
			{ok, do_json_struct_to_query(Decoded)};
		{error, _} ->
			{error, invalid_json}
	end.

do_json_struct_to_query({Query}) ->
	{
		list_to_existing_atom(binary_to_list(find_value(<<"op">>, Query))),
		do_json_struct_to_query(find_value(<<"expr1">>, Query)),
		do_json_struct_to_query(find_value(<<"expr2">>, Query))
	};
do_json_struct_to_query(Query) ->
	Query.

%% @doc Generate a JSON structure representing a block index.
block_index_to_json_struct(BI) ->
	lists:map(
		fun
			({BH, WeaveSize, TXRoot}) ->
				Keys1 = [{<<"hash">>, ar_util:encode(BH)}],
				Keys2 =
					case WeaveSize of
						not_set ->
							Keys1;
						_ ->
							[{<<"weave_size">>, integer_to_binary(WeaveSize)} | Keys1]
					end,
				Keys3 =
					case TXRoot of
						not_set ->
							Keys2;
						_ ->
							[{<<"tx_root">>, ar_util:encode(TXRoot)} | Keys2]
					end,
				{Keys3};
			(BH) ->
				ar_util:encode(BH)
		end,
		BI
	).

%% @doc Convert a JSON structure into a block index.
json_struct_to_block_index(JSONStruct) ->
	lists:map(
		fun
			(Hash) when is_binary(Hash) ->
				ar_util:decode(Hash);
			({JSON}) ->
				Hash = ar_util:decode(find_value(<<"hash">>, JSON)),
				WeaveSize =
					case find_value(<<"weave_size">>, JSON) of
						undefined ->
							not_set;
						WS ->
							binary_to_integer(WS)
					end,
				TXRoot =
					case find_value(<<"tx_root">>, JSON) of
						undefined ->
							not_set;
						R ->
							ar_util:decode(R)
					end,
				{Hash, WeaveSize, TXRoot}
		end,
		JSONStruct
	).

poa_map_to_json_map(Map) ->
	#{ chunk := Chunk, tx_path := TXPath, data_path := DataPath, packing := Packing } = Map,
	SerializedPacking =
		case Packing of
			unpacked ->
				<<"unpacked">>;
			spora_2_5 ->
				<<"spora_2_5">>;
			{spora_2_6, Addr} ->
				iolist_to_binary([<<"spora_2_6_">>, ar_util:encode(Addr)])
		end,
	Map2 = #{
		chunk => ar_util:encode(Chunk),
		tx_path => ar_util:encode(TXPath),
		data_path => ar_util:encode(DataPath),
		packing => SerializedPacking
	},
	case maps:get(end_offset, Map, not_found) of
		not_found ->
			Map2;
		EndOffset ->
			Map2#{ end_offset => integer_to_binary(EndOffset) }
	end.

poa_no_chunk_map_to_json_map(Map) ->
	#{ tx_path := TXPath, data_path := DataPath } = Map,
	Map2 = #{
		tx_path => ar_util:encode(TXPath),
		data_path => ar_util:encode(DataPath)
	},
	case maps:get(end_offset, Map, not_found) of
		not_found ->
			Map2;
		EndOffset ->
			Map2#{ end_offset => integer_to_binary(EndOffset) }
	end.

json_map_to_poa_map(JSON) ->
	Map = #{
		data_root => ar_util:decode(maps:get(<<"data_root">>, JSON, <<>>)),
		chunk => ar_util:decode(maps:get(<<"chunk">>, JSON)),
		data_path => ar_util:decode(maps:get(<<"data_path">>, JSON)),
		tx_path => ar_util:decode(maps:get(<<"tx_path">>, JSON, <<>>)),
		data_size => binary_to_integer(maps:get(<<"data_size">>, JSON, <<"0">>))
	},
	Map2 =
		case maps:get(<<"packing">>, JSON, <<"unpacked">>) of
			<<"unpacked">> ->
				maps:put(packing, unpacked, Map);
			<<"spora_2_5">> ->
				maps:put(packing, spora_2_5, Map);
			<< Type:10/binary, Addr/binary >>
					when Type == <<"spora_2_6_">>, byte_size(Addr) =< 64 ->
				case ar_util:safe_decode(Addr) of
					{ok, DecodedAddr} ->
						maps:put(packing, {spora_2_6, DecodedAddr}, Map);
					_ ->
						error(unsupported_packing)
				end;
			_ ->
				error(unsupported_packing)
		end,
	case maps:get(<<"offset">>, JSON, none) of
		none ->
			Map2;
		Offset ->
			Map2#{ offset => binary_to_integer(Offset) }
	end.

signature_type_to_binary(SigType) ->
	case SigType of
		{?RSA_SIGN_ALG, 65537} -> <<"PS256_65537">>;
		{?ECDSA_SIGN_ALG, secp256k1} -> <<"ES256K">>;
		{?EDDSA_SIGN_ALG, ed25519} -> <<"Ed25519">>
	end.

binary_to_signature_type(List) ->
	case List of
		undefined -> {?RSA_SIGN_ALG, 65537};
		<<"PS256_65537">> -> {?RSA_SIGN_ALG, 65537};
		<<"ES256K">> -> {?ECDSA_SIGN_ALG, secp256k1};
		<<"Ed25519">> -> {?EDDSA_SIGN_ALG, ed25519};
		%% For backwards-compatibility.
		_ -> {?RSA_SIGN_ALG, 65537}
	end.

candidate_to_json_struct(
	#mining_candidate{
		cm_diff = DiffPair,
		cm_h1_list = H1List,
		h0 = H0,
		h1 = H1,
		h2 = H2,
		mining_address = MiningAddress,
		nonce = Nonce,
		next_seed = NextSeed,
		next_vdf_difficulty = NextVDFDifficulty,
		nonce_limiter_output = NonceLimiterOutput,
		partition_number = PartitionNumber,
		partition_number2 = PartitionNumber2,
		partition_upper_bound = PartitionUpperBound,
		poa2 = PoA2,
		preimage = Preimage,
		seed = Seed,
		session_key = SessionKey,
		start_interval_number = StartIntervalNumber,
		step_number = StepNumber,
		label = Label
	}) ->
	JSON = [
		{cm_diff, diff_pair_to_json_list(DiffPair)},
		{cm_h1_list, h1_list_to_json_struct(H1List)},
		{mining_address, ar_util:encode(MiningAddress)},
		{h0, ar_util:encode(H0)},
		{partition_number, integer_to_binary(PartitionNumber)},
		{partition_number2, integer_to_binary(PartitionNumber2)},
		{partition_upper_bound, integer_to_binary(PartitionUpperBound)},
		{seed, ar_util:encode(Seed)},
		{next_seed, ar_util:encode(NextSeed)},
		{next_vdf_difficulty, integer_to_binary(NextVDFDifficulty)},
		{session_key, session_key_json_struct(SessionKey)},
		{start_interval_number, integer_to_binary(StartIntervalNumber)},
		{step_number, integer_to_binary(StepNumber)},
		{nonce_limiter_output, ar_util:encode(NonceLimiterOutput)},
		{label, Label}
	],

	JSON2 = encode_if_set(JSON, h1, H1, fun ar_util:encode/1),
	JSON3 = encode_if_set(JSON2, h2, H2, fun ar_util:encode/1),
	JSON4 = encode_if_set(JSON3, nonce, Nonce, fun integer_to_binary/1),
	JSON5 = encode_if_set(JSON4, poa2, PoA2, fun poa_to_json_struct/1),
	{encode_if_set(JSON5, preimage, Preimage, fun ar_util:encode/1)}.

h1_list_to_json_struct(H1List) ->
	lists:map(fun ({H1, Nonce}) ->
		{[
			{h1, ar_util:encode(H1)},
			{nonce, integer_to_binary(Nonce)}
		]}
	end,
	H1List).

session_key_json_struct({NextSeed, Interval, NextDifficulty}) ->
	{[
		{next_seed, ar_util:encode(NextSeed)},
		{interval, integer_to_binary(Interval)},
		{next_difficulty, integer_to_binary(NextDifficulty)}
	]}.

json_map_to_candidate(JSON) ->
	DiffPair = json_list_to_diff_pair(maps:get(<<"cm_diff">>, JSON)),
	H1List = json_struct_to_h1_list(maps:get(<<"cm_h1_list">>, JSON)),
	H0 = ar_util:decode(maps:get(<<"h0">>, JSON)),
	H1 = decode_if_set(JSON, <<"h1">>, fun ar_util:decode/1, not_set),
	H2 = decode_if_set(JSON, <<"h2">>, fun ar_util:decode/1, not_set),
	MiningAddress = ar_util:decode(maps:get(<<"mining_address">>, JSON)),
	NextSeed = ar_util:decode(maps:get(<<"next_seed">>, JSON)),
	NextVDFDifficulty = binary_to_integer(maps:get(<<"next_vdf_difficulty">>, JSON)),
	Nonce = decode_if_set(JSON, <<"nonce">>, fun binary_to_integer/1, not_set),
	NonceLimiterOutput = ar_util:decode(maps:get(<<"nonce_limiter_output">>, JSON)),
	PartitionNumber = binary_to_integer(maps:get(<<"partition_number">>, JSON)),
	PartitionNumber2 = binary_to_integer(maps:get(<<"partition_number2">>, JSON)),
	PartitionUpperBound = binary_to_integer(maps:get(<<"partition_upper_bound">>, JSON)),
	PoA2 = decode_if_set(JSON, <<"poa2">>, fun json_struct_to_poa_from_map/1, not_set),
	Preimage = decode_if_set(JSON, <<"preimage">>, fun ar_util:decode/1, not_set),
	Seed = ar_util:decode(maps:get(<<"seed">>, JSON)),
	SessionKey = json_struct_to_session_key(maps:get(<<"session_key">>, JSON)),
	StartIntervalNumber = binary_to_integer(maps:get(<<"start_interval_number">>, JSON)),
	StepNumber = binary_to_integer(maps:get(<<"step_number">>, JSON)),
	Label = maps:get(<<"label">>, JSON, <<"not_set">>),

	#mining_candidate{
		cm_diff = DiffPair,
		cm_h1_list = H1List,
		h0 = H0,
		h1 = H1,
		h2 = H2,
		mining_address = MiningAddress,
		next_seed = NextSeed,
		next_vdf_difficulty = NextVDFDifficulty,
		nonce = Nonce,
		nonce_limiter_output = NonceLimiterOutput,
		partition_number = PartitionNumber,
		partition_number2 = PartitionNumber2,
		partition_upper_bound = PartitionUpperBound,
		poa2 = PoA2,
		preimage = Preimage,
		seed = Seed,
		session_key = SessionKey,
		start_interval_number = StartIntervalNumber,
		step_number = StepNumber,
		label = Label
	}.

json_struct_to_h1_list(JSON) ->
	lists:map(fun (JSONElement) ->
		H1 = ar_util:decode(maps:get(<<"h1">>, JSONElement)),
		Nonce = binary_to_integer(maps:get(<<"nonce">>, JSONElement)),
		{H1, Nonce}
	end, JSON).

json_struct_to_session_key(JSON) ->
	{
		ar_util:decode(maps:get(<<"next_seed">>, JSON)),
		binary_to_integer(maps:get(<<"interval">>, JSON)),
		binary_to_integer(maps:get(<<"next_difficulty">>, JSON))
	}.

solution_to_json_struct(
	#mining_solution{
		last_step_checkpoints = LastStepCheckpoints,
		mining_address = MiningAddress,
		next_seed = NextSeed,
		next_vdf_difficulty = NextVDFDifficulty,
		nonce = Nonce,
		nonce_limiter_output = NonceLimiterOutput,
		partition_number = PartitionNumber,
		partition_upper_bound = PartitionUpperBound,
		poa1 = PoA1,
		poa2 = PoA2,
		preimage = Preimage,
		recall_byte1 = RecallByte1,
		recall_byte2 = RecallByte2,
		seed = Seed,
		solution_hash = SolutionHash,
		start_interval_number = StartIntervalNumber,
		step_number = StepNumber,
		steps = Steps
	}) ->
	JSON = [
		{last_step_checkpoints, ar_util:encode(iolist_to_binary(LastStepCheckpoints))},
		{mining_address, ar_util:encode(MiningAddress)},
		{nonce, Nonce},
		{nonce_limiter_output, ar_util:encode(NonceLimiterOutput)},
		{next_seed, ar_util:encode(NextSeed)},
		{next_vdf_difficulty, integer_to_binary(NextVDFDifficulty)},
		{partition_number, integer_to_binary(PartitionNumber)},
		{partition_upper_bound, integer_to_binary(PartitionUpperBound)},
		{poa1, poa_to_json_struct(PoA1)},
		{poa2, poa_to_json_struct(PoA2)},
		{preimage, ar_util:encode(Preimage)},
		{recall_byte1, integer_to_binary(RecallByte1)},
		{seed, ar_util:encode(Seed)},
		{solution_hash, ar_util:encode(SolutionHash)},
		{start_interval_number, integer_to_binary(StartIntervalNumber)},
		{step_number, integer_to_binary(StepNumber)},
		{steps, ar_util:encode(iolist_to_binary(Steps))}
	],
	{encode_if_set(JSON, recall_byte2, RecallByte2, fun integer_to_binary/1)}.

json_map_to_solution(JSON) ->
	LastStepCheckpoints = parse_json_checkpoints(
			ar_util:decode(maps:get(<<"last_step_checkpoints">>, JSON, <<>>))),
	MiningAddress = ar_util:decode(maps:get(<<"mining_address">>, JSON)),
	NextSeed = ar_util:decode(maps:get(<<"next_seed">>, JSON)),
	NextVDFDifficulty = maps:get(<<"next_vdf_difficulty">>, JSON),
	NextVDFDifficulty2 =
		case is_binary(NextVDFDifficulty) of
			true ->
				binary_to_integer(NextVDFDifficulty);
			false ->
				NextVDFDifficulty
		end,
	Nonce = maps:get(<<"nonce">>, JSON),
	NonceLimiterOutput = ar_util:decode(maps:get(<<"nonce_limiter_output">>, JSON)),
	PartitionNumber = binary_to_integer(maps:get(<<"partition_number">>, JSON)),
	PartitionUpperBound = binary_to_integer(maps:get(<<"partition_upper_bound">>, JSON)),
	PoA1 = json_struct_to_poa_from_map(maps:get(<<"poa1">>, JSON)),
	PoA2 = json_struct_to_poa_from_map(maps:get(<<"poa2">>, JSON)),
	Preimage = ar_util:decode(maps:get(<<"preimage">>, JSON)),
	RecallByte1 = binary_to_integer(maps:get(<<"recall_byte1">>, JSON)),
	RecallByte2 = decode_if_set(JSON, <<"recall_byte2">>, fun binary_to_integer/1, undefined),
	Seed = ar_util:decode(maps:get(<<"seed">>, JSON)),
	SolutionHash = ar_util:decode(maps:get(<<"solution_hash">>, JSON)),
	StartIntervalNumber = binary_to_integer(maps:get(<<"start_interval_number">>, JSON)),
	StepNumber = binary_to_integer(maps:get(<<"step_number">>, JSON)),
	Steps = parse_json_checkpoints(ar_util:decode(maps:get(<<"steps">>, JSON, <<>>))),

	#mining_solution{
		last_step_checkpoints = LastStepCheckpoints,
		mining_address = MiningAddress,
		next_seed = NextSeed,
		next_vdf_difficulty = NextVDFDifficulty2,
		nonce = Nonce,
		nonce_limiter_output = NonceLimiterOutput,
		partition_number = PartitionNumber,
		partition_upper_bound = PartitionUpperBound,
		poa1 = PoA1,
		poa2 = PoA2,
		preimage = Preimage,
		recall_byte1 = RecallByte1,
		recall_byte2 = RecallByte2,
		seed = Seed,
		solution_hash = SolutionHash,
		start_interval_number = StartIntervalNumber,
		step_number = StepNumber,
		steps = Steps
	}.

json_map_to_info_map(JSONMap) ->
	lists:foldl(
        fun(Key, Acc) ->
            BinaryKey = atom_to_binary(Key, utf8),
            case maps:get(BinaryKey, JSONMap, undefined) of
                undefined -> Acc;
                Value -> maps:put(Key, Value, Acc)
            end
        end,
        #{},
        ar_info:get_keys()
    ).

encode_if_set(JSON, _JSONProperty, not_set, _Encoder) ->
	JSON;
encode_if_set(JSON, _JSONProperty, undefined, _Encoder) ->
	JSON;
encode_if_set(JSON, JSONProperty, Value, Encoder) ->
	[{JSONProperty, Encoder(Value)} | JSON].

decode_if_set(JSON, JSONProperty, Decoder, Default) ->
	case maps:get(JSONProperty, JSON, not_found) of
		not_found ->
			Default;
		EncodedValue ->
			Decoder(EncodedValue)
	end.

parse_json_checkpoints(<<>>) ->
	[];
parse_json_checkpoints(<< Checkpoint:32/binary, Rest/binary >>) ->
	[Checkpoint | parse_json_checkpoints(Rest)].

jobs_to_json_struct(Jobs) ->
	#jobs{ jobs = JobList, partial_diff = PartialDiff,
			seed = Seed, next_seed = NextSeed, interval_number = IntervalNumber,
			next_vdf_difficulty = NextVDFDiff } = Jobs,
	
	{[{jobs, [job_to_json_struct(Job) || Job <- JobList]},
		{partial_diff, diff_pair_to_json_list(PartialDiff)},
		{seed, ar_util:encode(Seed)},
		{next_seed, ar_util:encode(NextSeed)},
		{interval_number, integer_to_binary(IntervalNumber)},
		{next_vdf_difficulty, integer_to_binary(NextVDFDiff)}
	]}.

job_to_json_struct(Job) ->
	#job{ output = Output, global_step_number = StepNumber,
			partition_upper_bound = PartitionUpperBound } = Job,
	{[{nonce_limiter_output, ar_util:encode(Output)},
			{step_number, integer_to_binary(StepNumber)},
			{partition_upper_bound, integer_to_binary(PartitionUpperBound)}]}.

json_struct_to_jobs(Struct) ->
	{Keys} = Struct,
	PartialDiff = json_list_to_diff_pair(proplists:get_value(<<"partial_diff">>, Keys)),
	Seed = ar_util:decode(proplists:get_value(<<"seed">>, Keys, <<>>)),
	NextSeed = ar_util:decode(proplists:get_value(<<"next_seed">>, Keys, <<>>)),
	NextVDFDiff = binary_to_integer(proplists:get_value(<<"next_vdf_difficulty">>, Keys,
			<<"0">>)),
	IntervalNumber = binary_to_integer(proplists:get_value(<<"interval_number">>, Keys,
			<<"0">>)),
	Jobs = [json_struct_to_job(Job) || Job <- proplists:get_value(<<"jobs">>, Keys, [])],
	#jobs{ jobs = Jobs, seed = Seed, next_seed = NextSeed,
			interval_number = IntervalNumber,
			next_vdf_difficulty = NextVDFDiff, partial_diff = PartialDiff }.

json_struct_to_job(Struct) ->
	{Keys} = Struct,
	Output = ar_util:decode(proplists:get_value(<<"nonce_limiter_output">>, Keys, <<>>)),
	StepNumber = binary_to_integer(proplists:get_value(<<"step_number">>, Keys,
			<<"0">>)),
	PartitionUpperBound = binary_to_integer(proplists:get_value(<<"partition_upper_bound">>,
			Keys, <<"0">>)),
	#job{ output = Output, global_step_number = StepNumber,
			partition_upper_bound = PartitionUpperBound }.

partial_solution_response_to_json_struct(Response) ->
	#partial_solution_response{ indep_hash = H, status = S } = Response,
	{[{<<"indep_hash">>, ar_util:encode(H)}, {<<"status">>, S}]}.

pool_cm_jobs_to_json_struct(Jobs) ->
	#pool_cm_jobs{ h1_to_h2_jobs = H1ToH2Jobs, h1_read_jobs = H1ReadJobs,
			partitions = Partitions } = Jobs,
	{[
		{h1_to_h2_jobs, [pool_cm_h1_to_h2_job_to_json_struct(Job) || Job <- H1ToH2Jobs]},
		{h1_read_jobs, [pool_cm_h1_read_job_to_json_struct(Job) || Job <- H1ReadJobs]},
		{partitions, Partitions}
	]}.

pool_cm_h1_to_h2_job_to_json_struct(Job) ->
	candidate_to_json_struct(Job).

pool_cm_h1_read_job_to_json_struct(Job) ->
	candidate_to_json_struct(Job).

json_map_to_pool_cm_jobs(Map) ->
	H1ToH2Jobs = [json_map_to_candidate(Job)
			|| Job <- maps:get(<<"h1_to_h2_jobs">>, Map, [])],
	H1ReadJobs = [json_map_to_candidate(Job)
			|| Job <- maps:get(<<"h1_read_jobs">>, Map, [])],
	#pool_cm_jobs{ h1_to_h2_jobs = H1ToH2Jobs, h1_read_jobs = H1ReadJobs }.
