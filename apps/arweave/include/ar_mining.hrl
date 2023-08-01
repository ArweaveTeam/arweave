-ifndef(AR_MINING_HRL).
-define(AR_MINING_HRL, true).

%% fields prefixed with cm_ are only set when a solution is distributed across miners as part
%% of a coordinated mining set.
-record(mining_candidate, {
	cache_ref,
	chunk1 = not_set,
	chunk2 = not_set,
	cm_diff = not_set, %% set to the difficulty used by cm_lead_peer
	cm_lead_peer = not_set, %% if set, this candidate came from another peer and will be sent back
	h0 = not_set,
	h1 = not_set,
	h2 = not_set,
	mining_address,
	next_seed,
	nonce = not_set,
	nonce_limiter_output,
	partition_number,
	partition_number2,
	partition_upper_bound,	
	poa1 = not_set,
	poa2 = not_set,
	preimage = not_set, %% this can be either the h1 or h2 preimage
	seed,
	session_ref,
	start_interval_number,
	step_number
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
	recall_byte1 = undefined,
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
