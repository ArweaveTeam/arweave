%%% A collection of record structures used throughout the ArkChain server.

%% The hashing algorithm used to verify that the weave has not been tampered
%% with.
-define(HASH_ALG, sha256).
-define(SIGN_ALG, rsa).
-define(PRIV_KEY_SZ, 512).
-define(DEFAULT_DIFF, 8).

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

%% A weave with associated meta data. Might even be unnecessary?
-record(weave, {
	block_hashes,
	wallets,
	blocks
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
	loss_probability = 0
}).

%% A message intended to be handled by the gossip protocol library.
-record(gs_msg, {
	hash,
	data
}).
