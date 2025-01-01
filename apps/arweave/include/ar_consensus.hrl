%% The number of RandomX hashes to compute to pack a chunk.
-define(PACKING_DIFFICULTY, 20).

%% The number of RandomX hashes to compute to pack a chunk after the fork 2.6.
%% we want packing x30 longer than regular one
%% 8   iterations - 2 ms
%% 360 iterations - 59 ms
%% 360/8 = 45
-define(PACKING_DIFFICULTY_2_6, 45).

-define(RANDOMX_PACKING_ROUNDS, 8 * (?PACKING_DIFFICULTY)).
-define(RANDOMX_PACKING_ROUNDS_2_6, 8 * (?PACKING_DIFFICULTY_2_6)).

%% Stop supporting the legacy non-composite packing after this number of blocks
%% passed since the fork 2.8. 365 * 24 * 60 * 60 / 128 = 246375.
-define(SPORA_PACKING_EXPIRATION_PERIOD_BLOCKS, (246375 * 4)).

%% Stop supporting the composite packing ~60 days have passed since 2.9 fork.
%% 30 days = 30 * 24 * 60 * 60 / 128 = 20250.
-define(COMPOSITE_PACKING_EXPIRATION_PERIOD_BLOCKS, (20250 * 2)).

%% The number of times we apply an RX hash in each RX2 lane in-between every pair
%% of mixings.
-define(REPLICA_2_9_RANDOMX_PROGRAM_COUNT, 6).

%% The number of RX2 lanes.
-define(REPLICA_2_9_RANDOMX_LANE_COUNT, 4).

%% The RX2 depth: the number of RX2 rounds. A round of RX2 has:
%% 1. REPLICA_2_9_RANDOMX_LANE_COUNT lanes
%% 2. Each lane evaluates REPLICA_2_9_RANDOMX_PROGRAM_COUNT RandomX programs
%% 3. The output entropy of each lane is then mixed with crc32 (aka "near mix")
%% 4. The the mixed output from all lanes is then shuffled (aka "far mix")
-define(REPLICA_2_9_RANDOMX_DEPTH, 3).

%% The size in bytes of the component RX2 scratchpad (aka the output from each RX2 lane). This
%% is NOT the total output entropy (that size is defined in REPLICA_2_9_ENTROPY_SIZE).
-define(RANDOMX_SCRATCHPAD_SIZE, 2097152).

%% The size in bytes of the total RX2 entropy (# of lanes * scratchpad size).
-ifdef(DEBUG).
-define(REPLICA_2_9_ENTROPY_SIZE, (3 * ?COMPOSITE_PACKING_SUB_CHUNK_SIZE)).
-else.
%% 8_388_608 bytes worth of entropy.
-define(REPLICA_2_9_ENTROPY_SIZE, (
	?REPLICA_2_9_RANDOMX_LANE_COUNT * ?RANDOMX_SCRATCHPAD_SIZE
)).
-endif.

%% The additional number of entropies generated per partition.
%% The value is chosen depending on the PARTITION_SIZE
%% and REPLICA_2_9_ENTROPY_SIZE constants
%% such that the sector size (num entropies * sub-chunk size) is evenly divisible
%% by ?DATA_CHUNK_SIZE. This proves very convenient for chunk-by-chunk syncing.
-ifdef(DEBUG).
-define(REPLICA_2_9_EXTRA_ENTROPY_COUNT, 11).
-else.
%% Extra entropies to be added to each partition so that the partition holds a multiple
%% of 32 entropies.
%% 
%% 3_600_000_000_000 / 8_388_608 = 429153.4423828125;
%% (429153 + 31) * 8192 / 262144 == 13412 - the first evenly divisible number of
%% the form (429153 + X) * 8192.
-define(REPLICA_2_9_EXTRA_ENTROPY_COUNT, 31).
-endif.

%% The effective packing difficulty of the new replication format (replica_format=1.)
%% Determines the recall range size and the mining difficulty of the mining nonces.
-ifdef(DEBUG).
-define(REPLICA_2_9_PACKING_DIFFICULTY, 2).
-else.
-define(REPLICA_2_9_PACKING_DIFFICULTY, 10).
-endif.

%% The size of the mining partition. The weave is broken down into partitions
%% of equal size. A miner can search for a solution in each of the partitions
%% in parallel, per mining address.
-ifdef(DEBUG).
-define(PARTITION_SIZE, 2097152).
-else.
-define(PARTITION_SIZE, 3600000000000). % 90% of 4 TB.
-endif.

%% The size of a recall range. The first range is randomly chosen from the given
%% mining partition. The second range is chosen from the entire weave.
-ifdef(DEBUG).
-define(RECALL_RANGE_SIZE, (128 * 1024)).
-else.
-define(RECALL_RANGE_SIZE, 26214400). % == 25 * 1024 * 1024
-endif.

%% The size of a recall range before the fork 2.8.
-ifdef(DEBUG).
-define(LEGACY_RECALL_RANGE_SIZE, (512 * 1024)).
-else.
-define(LEGACY_RECALL_RANGE_SIZE, 104857600). % == 100 * 1024 * 1024
-endif.

-ifdef(FORKS_RESET).
	-ifdef(DEBUG).
		-define(STRICT_DATA_SPLIT_THRESHOLD, (262144 * 3)).
	-else.
		-define(STRICT_DATA_SPLIT_THRESHOLD, 0).
	-endif.
-else.
%% The threshold was determined on the mainnet at the 2.5 fork block. The chunks
%% submitted after the threshold must adhere to stricter validation rules.
-define(STRICT_DATA_SPLIT_THRESHOLD, 30607159107830).
-endif.

-ifdef(FORKS_RESET).
	-ifdef(DEBUG).
		-define(MERKLE_REBASE_SUPPORT_THRESHOLD, (?STRICT_DATA_SPLIT_THRESHOLD * 2)).
	-else.
		-define(MERKLE_REBASE_SUPPORT_THRESHOLD, 0).
	-endif.
-else.
%% The threshold was determined on the mainnet at the 2.7 fork block. The chunks
%% submitted after the threshold must adhere to a different set of validation rules.
-define(MERKLE_REBASE_SUPPORT_THRESHOLD, 151066495197430).
-endif.

%% Recall bytes are only picked from the subspace up to the size
%% of the weave at the block of the depth defined by this constant.
-ifdef(DEBUG).
-define(SEARCH_SPACE_UPPER_BOUND_DEPTH, 3).
-else.
-define(SEARCH_SPACE_UPPER_BOUND_DEPTH, 50).
-endif.

%% The maximum mining difficulty. 2 ^ 256. The network difficulty
%% may theoretically be at most ?MAX_DIFF - 1.
-define(MAX_DIFF, (
	115792089237316195423570985008687907853269984665640564039457584007913129639936
)).

%% Increase the difficulty of PoA1 solutions by this multiplier (e.g. 100x).
-ifdef(DEBUG).
-define(POA1_DIFF_MULTIPLIER, 1).
-else.
-ifndef(POA1_DIFF_MULTIPLIER).
-define(POA1_DIFF_MULTIPLIER, 100).
-endif.
-endif.

%% The number of nonce limiter steps sharing the entropy. We add the entropy
%% from a past block every so often. If we did not add any entropy at all, even
%% a slight speedup of the nonce limiting function (considering its low cost) allows
%% one to eventually pre-compute a very significant amount of nonces opening up
%% the possibility of mining without the speed limitation. On the other hand,
%% adding the entropy at certain blocks (rather than nonce limiter steps) allows
%% miners to use extra bandwidth (bearing almost no additional costs) to compute
%% nonces on the short forks with different-entropy nonce limiting chains.
-ifndef(NONCE_LIMITER_RESET_FREQUENCY).
-define(NONCE_LIMITER_RESET_FREQUENCY, (10 * 120)).
-endif.

%% The maximum number of one-step checkpoints the block header may include.
-ifndef(NONCE_LIMITER_MAX_CHECKPOINTS_COUNT).
-define(NONCE_LIMITER_MAX_CHECKPOINTS_COUNT, 10800).
-endif.

%% The minimum difficulty allowed.
-ifndef(SPORA_MIN_DIFFICULTY).
-define(SPORA_MIN_DIFFICULTY(Height), fun() ->
	Forks = {
		ar_fork:height_2_4(),
		ar_fork:height_2_6()
	},
	case Forks of
		{_Fork_2_4, Fork_2_6} when Height >= Fork_2_6 ->
			2;
		{Fork_2_4, _Fork_2_6} when Height >= Fork_2_4 ->
			21
	end
end()).
-else.
-define(SPORA_MIN_DIFFICULTY(_Height), ?SPORA_MIN_DIFFICULTY).
-endif.

%%%===================================================================
%%% Pre-fork 2.6 constants.
%%%===================================================================

%% The size of the search space - a share of the weave randomly sampled
%% at every block. The solution must belong to the search space.
-define(SPORA_SEARCH_SPACE_SIZE(SearchSpaceUpperBound), fun() ->
	%% The divisor must be equal to SPORA_SEARCH_SPACE_SHARE
	%% defined in c_src/ar_mine_randomx.h.
	SearchSpaceUpperBound div 10 % 10% of the weave.
end()).

%% The number of contiguous subspaces of the search space, a roughly equal
%% share of the search space is sampled from each of the subspaces.
%% Must be equal to SPORA_SUBSPACES_COUNT defined in c_src/ar_mine_randomx.h.
-define(SPORA_SEARCH_SPACE_SUBSPACES_COUNT, 1024).

%% The key to initialize the RandomX state from, for RandomX packing.
-define(RANDOMX_PACKING_KEY, <<"default arweave 2.5 pack key">>).

-define(RANDOMX_HASHING_MODE_FAST, 0).
-define(RANDOMX_HASHING_MODE_LIGHT, 1).

%% The original plan was to cap the proof at 262144 (also the maximum chunk size).
%% The maximum tree depth is then (262144 - 64) / (32 + 32 + 32) = 2730.
%% Later we added support for offset rebases by recognizing the extra 32 bytes,
%% possibly at every branching point, as indicating a rebase. To preserve the depth maximum,
%% we now cap the size at 2730 * (96 + 32) + 65 = 349504.
-define(MAX_DATA_PATH_SIZE, 349504).

%% We may have at most 1000 transactions + 1000 padding nodes => depth=11
%% => at most 11 * 96 + 64 bytes worth of the proof. Due to its small size, we
%% extend it somewhat for better future-compatibility.
-define(MAX_TX_PATH_SIZE, 2176).
