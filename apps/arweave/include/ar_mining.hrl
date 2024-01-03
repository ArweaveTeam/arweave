-ifndef(AR_MINING_HRL).
-define(AR_MINING_HRL, true).

%% fields prefixed with cm_ are only set when a solution is distributed across miners as part
%% of a coordinated mining set.
-record(mining_candidate, {
	cache_ref = not_set, %% not serialized
	chunk1 = not_set, %% not serialized
	chunk2 = not_set, %% not serialized
	cm_diff = not_set, %% serialized. set to the difficulty used by the H1 miner
	cm_h1_list = [], %% serialized. list of {h1, nonce} pairs
	cm_lead_peer = not_set, %% not serialized. if set, this candidate came from another peer
	h0 = not_set, %% serialized
	h1 = not_set, %% serialized
	h2 = not_set, %% serialized
	mining_address = not_set, %% serialized
	next_seed = not_set, %% serialized
	next_vdf_difficulty = not_set, %% serialized
	nonce = not_set, %% serialized
	nonce_limiter_output = not_set, %% serialized
	partition_number = not_set, %% serialized
	partition_number2 = not_set, %% serialized
	partition_upper_bound = not_set, %% serialized
	poa2 = not_set, %% serialized
	preimage = not_set, %% serialized. this can be either the h1 or h2 preimage
	seed = not_set, %% serialized
	session_key = not_set, %% serialized
	start_interval_number = not_set, %% serialized
	step_number = not_set %% serialized
}).

-record(mining_solution, {
	last_step_checkpoints = [],
	merkle_rebase_threshold = 0,
	next_seed = << 0:256 >>,
	next_vdf_difficulty = 0,
	nonce = 0,
	nonce_limiter_output = << 0:256 >>,
	partition_number = 0,
	poa1 = #poa{},
	poa2 = #poa{},
	preimage = << 0:256 >>,
	recall_byte1 = 0,
	recall_byte2 = undefined,
	solution_hash = << 0:256 >>,
	start_interval_number = 0,
	step_number = 0,
	steps = [],
	seed = << 0:256 >>,
	mining_address = << 0:256 >>,
	partition_upper_bound = 0
}).

-endif.
