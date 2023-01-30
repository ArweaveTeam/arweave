%% The number of RandomX hashes to compute to pack a chunk.
-define(PACKING_DIFFICULTY, 20).

%% The number of RandomX hashes to compute to pack a chunk after the fork 2.6.
%% we want packing x30 longer than regular one
%% 8   iterations - 2 ms
%% 360 iterations - 59 ms
%% 360/8 = 45
-define(PACKING_DIFFICULTY_2_6, 45).

%% The size of the mining partition. The weave is broken down into partitions
%% of equal size. A miner can search for a solution in each of the partitions
%% in parallel, per mining address.
-ifdef(DEBUG).
-define(PARTITION_SIZE, 1800000).
-else.
-define(PARTITION_SIZE, 3600000000000). % 90% of 4 TB.
-endif.

%% The size of a recall range. The first range is randomly chosen from the given
%% mining partition. The second range is chosen from the entire weave.
-ifdef(DEBUG).
-define(RECALL_RANGE_SIZE, (512 * 1024)).
-else.
-define(RECALL_RANGE_SIZE, 104857600). % == 100 * 1024 * 1024
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

%% The number of nonce limiter steps sharing the entropy. We add the entropy
%% from a past block every so often. If we did not add any entropy at all, even
%% a slight speedup of the nonce limiting function (considering its low cost) allows
%% one to eventually pre-compute a very significant amount of nonces opening up
%% the possibility of mining without the speed limitation. On the other hand,
%% adding the entropy at certain blocks (rather than nonce limiter steps) allows
%% miners to use extra bandwidth (bearing almost no additional costs) to compute
%% nonces on the short forks with different-entropy nonce limiting chains.
-ifdef(DEBUG).
-define(NONCE_LIMITER_RESET_FREQUENCY, 5).
-else.
-define(NONCE_LIMITER_RESET_FREQUENCY, (25 * 120)).
-endif.

%% The number of last-step checkpoints every block must include.
-define(LAST_STEP_NONCE_LIMITER_CHECKPOINTS_COUNT, 25).

%% The maximum number of one-step checkpoints the block header may include.
-define(NONCE_LIMITER_MAX_CHECKPOINTS_COUNT, 240).

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
