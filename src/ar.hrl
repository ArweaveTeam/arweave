%%% A collection of record structures used throughout the ArkChain server.

%% How should nodes on the network identify themselves?
-define(NETWORK_NAME, "archain.BATN.3").
%% What is the current version/release number (should be an integer).
-define(CLIENT_VERSION, 3).

%% Should ar:report_console/1 /actually/ report to the console?
-define(SILENT, true).

%% The hashing algorithm used to verify that the weave has not been tampered
%% with.
-define(HASH_ALG, sha256).
-define(SIGN_ALG, rsa).
-define(PRIV_KEY_SZ, 4096).
%% NOTE: Setting the default difficulty too high will cause TNT to fail!
-define(DEFAULT_DIFF, 14).
-define(TARGET_TIME, 150).
-define(RETARGET_BLOCKS, 10).
-define(RETARGET_TOLERANCE, 0.1).
-define(BLOCK_PAD_SIZE, (1024*1024*1)).

%% The total supply of tokens in the Genesis block,
-define(GENESIS_TOKENS, 55000000).

%% The minimum cost/reward for a single TX.
-define(MIN_TX_REWARD, 1).
%% The minimum cost/reward for a single TX.
-define(COST_PER_BYTE, 0.001).

%% The amount of the weave to store. 1.0 = 100%; 0.5 = 50% etc.
-define(WEAVE_STOR_AMT, 1.0).

%% ENABLE ONLY WHILE TESTING
-define(DEBUG, true).
%% Speed to run the network at when simulating.
-define(DEBUG_TIME_SCALAR, 1.0).

%% Lenght of time to wait before giving up on test(s).
-define(TEST_TIMEOUT, 5 * 60).

%% Calculate MS to wait in order to hit target block time.
-define(DEFAULT_MINING_DELAY,
	((?TARGET_TIME * 1000) div erlang:trunc(math:pow(2, ?DEFAULT_DIFF - 1)))).

%% Default timeout value for network requests.
-define(NET_TIMEOUT, 10000).

%% Log output directory
-define(LOG_DIR, "logs").

%% Port to use for cross-machine message transfer.
-define(DEFAULT_HTTP_IFACE_PORT, 1984).

%% A block on the weave.
-record(block, {
	nonce = <<>>,
	previous_block = <<>>,
	timestamp = ar:timestamp(), % Unix time of block discovery
	last_retarget = -1, % Unix timestamp of the last defficulty retarget
	diff = ?DEFAULT_DIFF, % How many zeros need to preceed the next hash?
	height = -1, % How many blocks have passed since the Genesis block?
	hash = <<>>, % A hash of this block, the previous block and the recall block.
	indep_hash = [], % A hash of just this block.
	txs = [], % A list of transaction records associated with this block.
	hash_list = undefined, % A list of every indep hash to this point, or undefined.
	wallet_list = [], % A map of wallet blanaces, or undefined.
	reward_addr = unclaimed
}).

%% A transaction, as stored in a block.
-record(tx, {
	id = <<>>,
	owner = <<>>,
	tags = [],
	target = <<>>,
	quantity = 0,
	type = transfer,
	data = <<>>,
	signature = <<>>
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

%% Describes a known Archain network service.
-record(service, {
	name,
	host,
	expires
}).
