%%% A collection of record structures used throughout the Arweave server.

%% How should nodes on the network identify themselves?
-define(NETWORK_NAME, "arweave.TN.3").
%% What is the current version/release number (should be an integer).
-define(CLIENT_VERSION, 3).

%% Should ar:report_console/1 /actually/ report to the console?
-define(SILENT, true).
%% The hashing algorithm used to calculate wallet addresses
-define(HASH_ALG, sha256).
%% The hashing algorithm used to verify that the weave has not been tampered
%% with.
-define(MINING_HASH_ALG, sha384).
-define(HASH_SZ, 256).
-define(SIGN_ALG, rsa).
-define(PRIV_KEY_SZ, 4096).
%% NOTE: Setting the default difficulty too high will cause TNT to fail!
-define(DEFAULT_DIFF, 8).
-define(TARGET_TIME, 120).
-define(RETARGET_BLOCKS, 10).
-define(RETARGET_TOLERANCE, 0.1).
-define(BLOCK_PAD_SIZE, (1024*1024*1)).

%% The total supply of tokens in the Genesis block,
-define(GENESIS_TOKENS, 55000000).

%% Winstons per AR.
-define(WINSTON_PER_AR, 1000000000000).
%% The base cost of a byte in AR
-define(BASE_BYTES_PER_AR, 1000000).
%% The minimum cost per byte for a single TX.
-define(COST_PER_BYTE, (?WINSTON_PER_AR div ?BASE_BYTES_PER_AR)).
%% The difficulty "center" at which 1 byte costs ?BASE_BYTES_PER_AR
-define(DIFF_CENTER, 25).

%% The amount of the weave to store. 1.0 = 100%; 0.5 = 50% etc.
-define(WEAVE_STOR_AMT, 1.0).
%% The number of blocks behind the most recent block to store.
-define(STORE_BLOCKS_BEHIND_CURRENT, 25).
%% WARNING: ENABLE ONLY WHILE TESTING
%-define(DEBUG, debug).
%% Speed to run the network at when simulating.
-define(DEBUG_TIME_SCALAR, 1.0).

%% Lenght of time to wait before giving up on test(s).
-define(TEST_TIMEOUT, 5 * 60).

%% Calculate MS to wait in order to hit target block time.
-define(DEFAULT_MINING_DELAY,
	((?TARGET_TIME * 1000) div erlang:trunc(math:pow(2, ?DEFAULT_DIFF - 1)))).
%% The maximum size of a single POST body.
-define(MAX_BODY_SIZE, 5 * 1024 * 1024).
%% Default timeout value for network requests.
-define(NET_TIMEOUT, 300 * 1000).
%% Default timeout value for local requests
-define(LOCAL_NET_TIMEOUT, 10000).
%% Default timeout for initial request
-define(CONNECT_TIMEOUT, 30 * 1000).
%% Default time to wait after a failed join to retry
-define(REJOIN_TIMEOUT, 10000).
%% Time between attempts to find(/optimise) peers.
-define(REFRESH_MINE_DATA_TIMER, 60000).
%% Time between attempts to find(/optimise) peers.
-define(GET_MORE_PEERS_TIME,  240 * 1000).
%% Time to wait before not ignoring bad peers
-define(IGNORE_PEERS_TIME, 5 * 60 * 1000).
%% Number of transfers for which not to score (and potentially drop) new peers.
-define(PEER_GRACE_PERIOD, 100).
%% Never drop to lower than this number of peers.
-define(MINIMUM_PEERS, 4).
%% Never have more than this number of peers (new peers excluded).
-define(MAXIMUM_PEERS, 20).
%% Amount of peers without a given transaction to send a new transaction to
-define(NUM_REGOSSIP_TX, 6).
%% Maximum nunber of requests allowed by an IP in any 5 second period.
-define(MAX_REQUESTS, 50).
%% Default list of peers if no others are specified
-define(DEFAULT_PEER_LIST,
	[
		{104,236,121,142,1984},
		{107,170,220,199,1984},
		{188,226,184,142,1984},
		{128,199,168,25,1984},
		{178,62,4,18,1984},
		{207,154,238,1,1984},
		{165,227,40,8,1984},
		{139,59,81,47,1984}
	]).
-define(PEER_PERMANENT_BLACKLIST,
[
    {52,56,88,132,1984},
    {18,228,44,243,1984},
    {54,191,202,164,1984},
    {13,124,23,69,1984},
    {18,221,54,143,1984},
    {204,48,27,249,1984},
    {204,48,27,8,1984},
    {204,48,25,210,1984},
    {159,89,225,231,1984},
    {204,48,27,27,1984},
    {204,48,27,17,1984},
    {209,97,142,167,1984},
    {209,97,142,68,1984},
    {209,97,134,129,1984},
    {209,97,128,161,1984},
    {206,189,121,5,1984},
    {209,97,142,170,1984},
    {209,97,142,169,1984},
    {209,97,142,143,1984},
    {167,99,249,72,1984},
    {167,99,241,245,1984},
    {167,99,241,234,1984},
    {167,99,246,120,1984},
    {167,99,135,122,1984},
    {167,99,249,43,1984},
    {206,189,164,166,1984},
    {167,99,99,34,1984},
    {165,227,10,14,1984},
    {165,227,4,33,1984},
    {206,189,170,147,1984},
    {167,99,98,48,1984},
    {206,189,146,219,1984},
    {206,189,86,98,1984},
    {209,97,175,74,1984},
    {206,189,157,247,1984},
    {209,97,160,239,1984},
    {209,97,160,159,1984},
    {206,189,5,4,1984},
    {206,189,5,3,1984},
    {206,189,13,16,1984},
    {206,189,5,230,1984},
    {206,189,5,178,1984},
    {206,189,5,91,1984},
    {138,197,135,169,1984},
    {159,203,60,21,1984},
    {159,203,33,159,1984},
    {138,197,168,215,1984},
    {138,197,160,5,1984},
    {138,197,131,159,1984},
    {159,65,156,138,1984},
    {159,89,174,32,1984},
    {159,65,146,30,1984},
    {159,65,146,28,1984},
    {159,65,156,214,1984}
]).
%% Length of time to wait (seconds) before dropping after last activity
-define(PEER_TIMEOUT, 480).
%% Log output directory
-define(LOG_DIR, "logs").
%% Port to use for cross-machine message transfer.
-define(DEFAULT_HTTP_IFACE_PORT, 1984).
%% Number of mining processes to spawn
%% For best mining, this is set to the number of available processers minus 1. More mining can be performed
%% With every core utilised, but at significant cost to node performance
-define(NUM_MINING_PROCESSES, max(1, (erlang:system_info(schedulers_online) - 1))).
%% Target number of blocks per year
-define(BLOCK_PER_YEAR, 525600/(?TARGET_TIME/60) ).
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
    reward_addr = unclaimed, % Address to credit mining reward to
    tags = [], % Miner specified tags
	reward_pool = 0, % Current pool of mining reward (10% issued to block finder)
	weave_size = 0 % The current size of the weave in bytes (data only)
}).
%% A transaction, as stored in a block.
-record(tx, {
	id = <<>>, % TX UID.
	last_tx = <<>>, % Get last TX hash.
	owner = <<>>, % Public key of transaction owner.
	tags = [], % Indexable TX category identifiers.
	target = <<>>, % Address of target of the tx.
	quantity = 0, % Amount to send
	data = <<>>, % Data in transaction (if data transaction).
	signature = <<>>, % Transaction signature.
	reward = 0 % Transaction mining reward.
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

%% Describes a known Arweave network service.
-record(service, {
	name,
	host,
	expires
}).

% HTTP Performance results for a given node.
-record(performance, {
	bytes = 0,
	time = 0,
	transfers = 0,
	timestamp = 0,
	timeout = os:system_time(seconds)
}).

%% Helper macros
% Return number of winstons per given AR.
-define(AR(AR), (?WINSTON_PER_AR * AR)).
% Return whether an object is a block
-define(IS_BLOCK(X), (is_record(X, block))).
-define(IS_ADDR(Addr), (is_binary(Addr) and (bit_size(Addr) == ?HASH_SZ))).