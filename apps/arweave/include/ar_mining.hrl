-ifndef(AR_MINING_HRL).
-define(AR_MINING_HRL, true).

%% fields prefixed with cm_ are only set when a solution is distributed across miners as part
%% of a coordinated mining set.
-record(mining_candidate, {
	cache_ref = not_set, %% not serialized
	chunk1 = not_set, %% not serialized
	chunk2 = not_set, %% not serialized
	cm_diff = not_set, %% serialized. set to the difficulty used by the H1 miner
	cm_lead_peer = not_set, %% not serialized. if set, this candidate came from another peer
	h0 = not_set, %% serialized
	h1 = not_set, %% serialized
	h2 = not_set, %% serialized
	mining_address = not_set, %% serialized
	next_seed = not_set, %% serialized
	nonce = not_set, %% serialized
	nonce_limiter_output = not_set, %% serialized
	partition_number = not_set, %% serialized
	partition_number2 = not_set, %% serialized
	partition_upper_bound = not_set, %% serialized
	poa2 = not_set, %% serialized
	preimage = not_set, %% serialized. this can be either the h1 or h2 preimage
	seed = not_set, %% serialized
	session_ref = not_set, %% not serialized
	start_interval_number = not_set, %% serialized
	step_number = not_set %% serialized
}).

-record(mining_solution, {
	last_step_checkpoints = not_set,
	next_seed = not_set,
	nonce = not_set,
	nonce_limiter_output = not_set,
	partition_number = not_set,
	poa1 = not_set,
	poa2 = not_set,
	preimage = not_set,
	recall_byte1 = undefined, %% undefined instead of not_set for compatibility with existing code
	recall_byte2 = undefined,
	solution_hash = not_set,
	start_interval_number = not_set,
	step_number = not_set,
	steps = not_set,
	%% Not used in block, but cached to improve validation and logging:
	seed = not_set,
	mining_address = not_set,
	partition_upper_bound = not_set
}).

-endif.
