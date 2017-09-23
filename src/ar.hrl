%%% A collection of record structures used throughout the ArkChain server.

%% The hashing algorithm used to verify that the weave has not been tampered
%% with.
-define(HASH_ALG, sha256).
-define(SIGN_ALG, rsa).
-define(PRIV_KEY_SZ, 512).
-define(DEFAULT_DIFF, 8).
-define(TARGET_TIME, 300).

%% ENABLE ONLY WHILE TESTING
-define(DEBUG, true).
%% Speed to run the network at when simulating.
-define(DEBUG_TIME_SCALAR, 0.1).

%% Calculate MS to wait in order to hit target block time.
-define(DEFAULT_MINING_DELAY,
	((?TARGET_TIME * 1000) div erlang:trunc(math:pow(2, ?DEFAULT_DIFF - 1)))).

%% Log output directory
-define(LOG_DIR, "logs").

%% A block on the weave.
-record(block, {
	nonce,
	diff = ?DEFAULT_DIFF, % How many zeros need to preceed the next hash?
	height, % How many blocks have passed since the Genesis block?
	hash, % A hash of this block, the previous block and the recall block.
	indep_hash, % A hash of just this block.
	txs, % A list of transaction records associated with this block.
	hash_list, % A list of every indep hash to this point, or undefined.
	wallet_list % A map of wallet blanaces, or undefined.
}).

%% A transaction, as stored in a block.
-record(tx, {
	id,
	owner,
	tags = [],
	target,
	quantity = 0,
	type = transfer,
	data = undefined,
	signature
}).


%% Gossip protocol state. Passed to and from the gossip library functions.
-record(gs_state, {
	peers, % A list of the peers known to this node.
	heard = [], % Hashes of the messages received thus far.
	% Simulation attributes:
	loss_probability = 0,
	delay = 0,
	xfer_speed = undefined % In bytes per second
}).

%% A message intended to be handled by the gossip protocol library.
-record(gs_msg, {
	hash,
	data
}).
