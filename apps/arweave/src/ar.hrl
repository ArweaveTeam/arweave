-ifndef(AR_HRL).
-define(AR_HRL, true).

%%% A collection of record structures used throughout the Arweave server.

%% @doc How nodes identify they are on the same network.
-define(NETWORK_NAME, "arweave.N.1").

%% @doc Current release number of the arweave client software.
-define(CLIENT_VERSION, 5).

%% @doc The current build number -- incremented for every release.
-define(RELEASE_NUMBER, 38).

-define(DEFAULT_REQUEST_HEADERS,
	[
		{<<"X-Version">>, <<"8">>},
		{<<"X-Block-Format">>, <<"3">>}
	]).

-define(CORS_HEADERS,
	#{<<"access-control-allow-origin">> => <<"*">>}).

%% @doc Specifies whether the software should be run in debug mode
%% (excuting ifdef code blocks).
%% WARNING: Only define debug during testing.
%-define(DEBUG, debug).

-ifdef(DEBUG).
-define(FORK_1_6, 0).
-else.
%%% FORK INDEX
%%% @deprecated Fork heights from 1.7 on are defined in the ar_fork module.
-define(FORK_1_6, 95000).
-endif.

%% @doc Default auto-update watch address.
-define(DEFAULT_UPDATE_ADDR, "8L1NmHR2qY9wH-AqgsOmdw98FMwrdIzTS5-bJi9YDZ4").

%% @doc The hashing algorithm used to calculate wallet addresses
-define(HASH_ALG, sha256).

%% @doc The hashing algorithm used to verify that the weave has not been
%% tampered with.
-define(MINING_HASH_ALG, sha384).
-define(DEEP_HASH_ALG, sha384).
-define(MERKLE_HASH_ALG, sha384).
-define(HASH_SZ, 256).
-define(SIGN_ALG, rsa).
-define(PRIV_KEY_SZ, 4096).

%% @doc NB: Setting the default difficulty high will cause TNT to fail.
-define(DEFAULT_DIFF, 8).

-ifndef(TARGET_TIME).
-define(TARGET_TIME, 120).
-endif.

-ifndef(RETARGET_BLOCKS).
-define(RETARGET_BLOCKS, 10).
-endif.

-define(RETARGET_TOLERANCE, 0.1).

-define(JOIN_CLOCK_TOLERANCE, 15).
-define(MAX_BLOCK_PROPAGATION_TIME, 60).
-define(CLOCK_DRIFT_MAX, 5).

-ifdef(DEBUG).
-define(MINING_TIMESTAMP_REFRESH_INTERVAL, 1).
-else.
-define(MINING_TIMESTAMP_REFRESH_INTERVAL, 10).
-endif.

-define(BLOCK_PAD_SIZE, (1024*1024*1)).

%% @doc The total supply of tokens in the Genesis block,
-define(GENESIS_TOKENS, 55000000).

%% @doc Winstons per AR.
-define(WINSTON_PER_AR, 1000000000000).

%% The base cost of a byte in AR
-define(BASE_BYTES_PER_AR, 10000000).

%% Base wallet generation fee
-define(WALLET_GEN_FEE, 250000000000).

%% @doc The minimum cost per byte for a single TX.
-define(COST_PER_BYTE, (?WINSTON_PER_AR div ?BASE_BYTES_PER_AR)).

%% The difficulty "center" at which 1 byte costs ?BASE_BYTES_PER_AR
-define(DIFF_CENTER, 40).

%% @doc The amount of the weave to store. 1.0 = 100%; 0.5 = 50% etc.
-define(WEAVE_STORE_AMT, 1.0).

%% @doc The number of blocks behind the most recent block to store.
%% The maximum lag when fork recovery is still possible.
-define(STORE_BLOCKS_BEHIND_CURRENT, 50).

%% Speed to run the network at when simulating.
-define(DEBUG_TIME_SCALAR, 1.0).

%% @doc Length of time to wait before giving up on test(s).
-define(TEST_TIMEOUT, 15 * 60).

%% @doc Calculate MS to wait in order to hit target block time.
-define(DEFAULT_MINING_DELAY,
    ((?TARGET_TIME * 1000) div erlang:trunc(math:pow(2, ?DEFAULT_DIFF - 1)))).

%% @doc The maximum size of a single POST body.
-define(MAX_BODY_SIZE, 15 * 1024 * 1024).

%% @doc The maximum allowed size in bytes for the data field of
%% a format=1 transaction.
-ifdef(DEBUG).
-define(TX_DATA_SIZE_LIMIT, 10 * 1024).
-else.
-define(TX_DATA_SIZE_LIMIT, 10 * 1024 * 1024).
-endif.

%% @doc The maximum allowed size in bytes for the combined data fields of
%% the format=1 transactions included in a block.
-define(BLOCK_TX_DATA_SIZE_LIMIT, ?TX_DATA_SIZE_LIMIT). % Must be greater or equal to tx data size limit.
%% @doc The maximum number of transactions (both format=1 and format=2) in a block.
-ifdef(DEBUG).
-define(BLOCK_TX_COUNT_LIMIT, 10).
-else.
-define(BLOCK_TX_COUNT_LIMIT, 1000).
-endif.

%% @doc Byte size of the TX headers, tags allowance, etc.
-define(TX_SIZE_BASE, 3210).

%% @doc Mempool Limits.
%%
%% The reason we have two different mempool limits has to do with the way
%% format=2 transactions are distributed. To achieve faster consensus and
%% reduce the network congestion, the miner does not gossip data of format=2
%% transactions, but serves it later to those who request it after the
%% corresponding transaction is included into a block. A single mempool limit
%% would therefore be reached much quicker by a peer accepting format=2
%% transactions with data. This would prevent this miner from accepting any
%% further transactions. Having a separate limit for data allows the miner
%% to continue accepting transaction headers.


%% @doc The maximum allowed size of transaction headers stored in mempool.
%% The data field of a format=1 transaction is considered to be part of
%% its headers.
-ifdef(DEBUG).
-define(MEMPOOL_HEADER_SIZE_LIMIT, 50 * 1024).
-else.
-define(MEMPOOL_HEADER_SIZE_LIMIT, 250 * 1024 * 1024).
-endif.

%% @doc The maximum allowed size of transaction data stored in mempool.
%% The format=1 transactions are not counted as their data is considered
%% to be part of the header.
-ifdef(DEBUG).
-define(MEMPOOL_DATA_SIZE_LIMIT, 50 * 1024).
-else.
-define(MEMPOOL_DATA_SIZE_LIMIT, 500 * 1024 * 1024).
-endif.

%% @doc The size limits for the transaction priority queue.
%%
%% The two limits are used to align the priority queues of the miners
%% accepting transactions with and without data. See above for the
%% explanation of why two mempool size limits are used. Simply not
%% accounting for transaction data size in the priority queue would not
%% work because big format=2 transaction would never be dropped from
%% the queue due to the full data mempool.

%% @doc The maximum allowed size in bytes of the transaction headers
%% in the transaction queue. The data fields of format=1 transactions
%% count as transaction headers. When the limit is reached, transactions
%% with the lowest utility score are dropped from the queue.
%%
%% This limit has to be lower than the corresponding mempool limit,
%% otherwise transactions would never be dropped from the queue.
-define(TX_QUEUE_HEADER_SIZE_LIMIT, 200 * 1024 * 1024).

%% @doc The maximum allowed size in bytes of the transaction data
%% in the transaction queue. The data fields of format=1 transactions
%% does not count as transaction data. When the limit is reached, transactions
%% with the lowest utility score are dropped from the queue.
%%
%% This limit has to be lower than the corresponding mempool limit,
%% otherwise transactions would never be dropped from the queue.
-define(TX_QUEUE_DATA_SIZE_LIMIT, 400 * 1024 * 1024).

-define(MAX_TX_ANCHOR_DEPTH, ?STORE_BLOCKS_BEHIND_CURRENT).

%% @doc Default timeout for establishing an HTTP connection.
-define(HTTP_REQUEST_CONNECT_TIMEOUT, 10 * 1000).
%% @doc Default timeout used when sending to and receiving from a TCP socket
%%      when making an HTTP request.
-define(HTTP_REQUEST_SEND_TIMEOUT, 60 * 1000).

%% @doc Default timeout value for local requests
-define(LOCAL_NET_TIMEOUT, 30 * 1000).

%% @doc Default time to wait after a failed join to retry
-define(REJOIN_TIMEOUT, 3 * 1000).

%% @doc Time between attempts to find optimise peers.
-define(GET_MORE_PEERS_TIME,  240 * 1000).

%% @doc Number of transfers for which not to score (and potentially drop)
%% new peers.
-define(PEER_GRACE_PERIOD, 100).

%% @doc Never drop to lower than this number of peers.
-define(MINIMUM_PEERS, 4).

%% @doc Never have more than this number of peers (new peers excluded).
-define(MAXIMUM_PEERS, 20).

%% Maximum allowed number of accepted requests per minute per IP.
-define(DEFAULT_REQUESTS_PER_MINUTE_LIMIT, 900).

%% @doc Number of seconds an IP address should be completely banned from doing
%% HTTP requests after posting a block with bad PoW.
-define(BAD_POW_BAN_TIME, 24 * 60 * 60).

%% @doc Delay before mining rewards manifest.
-define(REWARD_DELAY, ?BLOCK_PER_YEAR/4).

%% @doc Peers to never add to peer list
-define(PEER_PERMANENT_BLACKLIST,[]).

%% @doc Length of time to wait (seconds) before dropping after last activity
-define(PEER_TIMEOUT, 8 * 60).

%% @doc A part of transaction propagation delay independent from the size.
-ifdef(DEBUG).
-define(BASE_TX_PROPAGATION_DELAY, 0). % seconds
-else.
-define(BASE_TX_PROPAGATION_DELAY, 30). % seconds
-endif.

%% @doc A conservative assumption of the network speed used to
%% estimate the transaction propagation delay. It does not include
%% the base delay, the time the transaction spends in the priority
%% queue, and the time it takes to propagate the transaction to peers.
-ifdef(DEBUG).
-define(TX_PROPAGATION_BITS_PER_SECOND, 1000000000).
-else.
-define(TX_PROPAGATION_BITS_PER_SECOND, 160000). % 160 kbps
-endif.

%% @doc The number of the best peers to send new transactions to in parallel.
%% Can be overriden by a command line argument.
-define(TX_PROPAGATION_PARALLELIZATION, 4).

%% @doc The number of the best peers to send new blocks to in parallel.
-define(BLOCK_PROPAGATION_PARALLELIZATION, 30).

%% @doc The maximum number of peers to propagate blocks or txs to, by default.
-define(DEFAULT_MAX_PROPAGATION_PEERS, 50).

%% @doc When the transaction data size is smaller than this number of bytes,
%% the transaction is gossiped to the peer without a prior check if the peer
%% already has this transaction.
-define(TX_SEND_WITHOUT_ASKING_SIZE_LIMIT, 1000).

%% @doc Log output directory
-define(LOG_DIR, "logs").
-define(BLOCK_DIR, "blocks").
-define(TX_DIR, "txs").
-define(STORAGE_MIGRATIONS_DIR, "data/storage_migrations").

%% @doc Backup block hash list storage directory.
-define(HASH_LIST_DIR, "hash_lists").
%% @doc Directory for storing miner wallets.
-define(WALLET_DIR, "wallets").
%% @doc Directory for storing unique wallet lists.
-define(WALLET_LIST_DIR, "wallet_lists").
%% @doc Directory for storing the ArQL v2 index.
-define(SQLITE3_DIR, "data/sqlite3").

%% @doc Port to use for cross-machine message transfer.
-define(DEFAULT_HTTP_IFACE_PORT, 1984).

%% @doc Number of mining processes to spawn
%% For best mining, this is set to the number of available processers minus 1.
%% More mining can be performed with every core utilised, but at significant
%% cost to node performance.
-define(NUM_MINING_PROCESSES, max(1, (erlang:system_info(schedulers_online) - 1))).

%% @doc Number of transaction propagation processes to spawn.
%% Each emitter picks a transaction from the queue and propagates it
%% to the best peers, a configured number of peers at a time.
%% Can be overriden by a command line argument.
-define(NUM_EMITTER_PROCESSES, 2).

%% @doc Target number of blocks per year.
-define(BLOCK_PER_YEAR, (525600 / (?TARGET_TIME/60)) ).

%% @doc The adjustment of difficutly going from SHA-384 to RandomX.
-define(RANDOMX_DIFF_ADJUSTMENT, (-14)).
-ifdef(DEBUG).
-define(RANDOMX_KEY_SWAP_FREQ, (?STORE_BLOCKS_BEHIND_CURRENT + 1)).
-define(RANDOMX_MIN_KEY_GEN_AHEAD, 1).
-define(RANDOMX_MAX_KEY_GEN_AHEAD, 4).
-define(RANDOMX_STATE_POLL_INTERVAL, 2).
-define(RANDOMX_KEEP_KEY, 3).
-else.
-define(RANDOMX_KEY_SWAP_FREQ, 2000).
-define(RANDOMX_MIN_KEY_GEN_AHEAD, (30 + ?STORE_BLOCKS_BEHIND_CURRENT)).
-define(RANDOMX_MAX_KEY_GEN_AHEAD, (90 + ?STORE_BLOCKS_BEHIND_CURRENT)).
-define(RANDOMX_STATE_POLL_INTERVAL, ?TARGET_TIME).
-define(RANDOMX_KEEP_KEY, ?STORE_BLOCKS_BEHIND_CURRENT).
-endif.

%% @doc Max allowed difficulty multiplication and division factors.
%% The adjustment is lower when the difficulty goes down than when
%% it goes up to prevent forks - stalls are preferred over forks.
-define(DIFF_ADJUSTMENT_DOWN_LIMIT, 2).
-define(DIFF_ADJUSTMENT_UP_LIMIT, 4).

%% @doc Max size of a single data chunk, in bytes.
-ifdef(DEBUG).
-define(DATA_CHUNK_SIZE, 128).
-else.
-define(DATA_CHUNK_SIZE, (256 * 1024)).
-endif.
%% @doc Max size of the PoA data path, in bytes.
-define(MAX_PATH_SIZE, (256 * 1024)).
%% @doc The size of data chunk hashes, in bytes.
-define(CHUNK_ID_HASH_SIZE, 32).

%% @doc A succinct proof of access to a recall byte found in a TX.
-record(poa, {
	option = 1, % The recall byte option (a sequence number) chosen.
	tx_path = <<>>, % Path through the Merkle tree of TXs in the block.
	data_path = <<>>, % Path through the Merkle tree of chunk IDs to the required chunk.
	chunk = <<>> % The required data chunk.
}).

%% @doc A full block or block shadow.
-record(block, {
	nonce = <<>>, % The nonce used to satisfy the PoW problem when mined.
	previous_block = <<>>, % indep_hash of the previous block in the weave.
	timestamp = os:system_time(seconds), % POSIX time of block discovery.
	last_retarget = -1, % POSIX time of the last difficulty retarget.
	diff = ?DEFAULT_DIFF, % The PoW difficulty - the number a PoW hash must be greater than.
	height = -1, % How many blocks have passed since the genesis block.
	hash = <<>>, % PoW hash of the block, must satisfy the block's difficulty.
	indep_hash = [], % The hash of the block, including `hash` and `nonce`, the block identifier.
	txs = [], % A list of tx records in full blocks, or a list of TX identifiers in block shadows.
	tx_root = <<>>, % Merkle root of the tree of transactions' data roots.
	tx_tree = [], % Merkle tree of transactions' data roots. Not stored.
	%% A list of all previous independent hashes. Not returned in the /block/[hash] API endpoint.
	%% In the block shadows only the last ?STORE_BLOCKS_BEHIND_CURRENT hashes are included.
	%% Reconstructed on the receiving side. Not stored in the block files.
	hash_list = unset,
	hash_list_merkle = <<>>, % The merkle root of the block index.
	%% A list of {address, balance, last TX ID} tuples. In the block shadows and
	%% in the /block/[hash] API endpoint the list is replaced with a hash.
	%% Reconstructed on the receiving side. Stored in the separate files.
	wallet_list = [],
	%% A field for internal use, not serialized. Added to avoid hashing the wallet list
	%% when the hash was known before the list was reconstructed.
	wallet_list_hash = not_set,
    reward_addr = unclaimed, % Address to credit mining reward or the unclaimed atom.
    tags = [], % Miner specified tags to store with the block.
	reward_pool = 0, % Current pool of mining rewards.
	weave_size = 0, % Current size of the weave in bytes (counts tx data fields).
	block_size = 0, % The total size of transaction data inside this block.
	%% The sum of average number of hashes tried to mine blocks over all previous blocks.
	cumulative_diff = 0,
	poa = #poa{} % The access proof used to generate this block.
}).

%% @doc A transaction, as stored in a block.
-record(tx, {
	format = 1, % 1 or 2.
	id = <<>>, % TX identifier.
	%% Either the ID of the previous transaction made from this wallet or
	%% the hash of one of the last ?MAX_TX_ANCHOR_DEPTH blocks.
	last_tx = <<>>,
	owner = <<>>, % Public key of transaction owner.
	tags = [], % Indexable TX category identifiers.
	target = <<>>, % Address of the recipient, if any.
	quantity = 0, % Amount of Winston to send, if any.
	data = <<>>, % May be empty. May be submitted in a transfer transaction.
	data_size = 0, % Size (in bytes) of the transaction data.
	data_tree = [], % The merkle tree of data chunks, the field is not signed.
	data_root = <<>>, % The merkle root of the merkle tree of data chunks.
	signature = <<>>, % Transaction signature.
	reward = 0 % Transaction fee, in Winston.
}).

%% @doc Gossip protocol state.
%% Passed to and from the gossip library functions (ar_gossip).
-record(gs_state, {
	peers, % A list of the peers known to this node.
	heard = [], % Hashes of the messages received thus far.
	loss_probability = 0, % Message loss probability for network simulation.
	delay = 0, % Message passing delay for network simulation.
	xfer_speed = undefined % Transfer speed in bytes/s for network simulation.
}).

%% @doc A message intended to be handled by the gossip protocol
%% library (ar_gossip).
-record(gs_msg, {
	hash,
	data
}).

%% @doc A record to describe a known Arweave network service.
-record(service, {
	name,
	host,
	expires
}).

% @doc A record to define HTTP Performance results for a given node.
-record(performance, {
	bytes = 0,
	time = 0,
	transfers = 0,
	timestamp = 0,
	timeout = os:system_time(seconds)
}).

%% @doc A Macro to return number of winstons per given AR.
-define(AR(AR), (?WINSTON_PER_AR * AR)).

%% @doc A Macro to return whether an object is a block.
-define(IS_BLOCK(X), (is_record(X, block))).

%% @doc Convert a v2.0 block index into an old style block hash list.
-define(BI_TO_BHL(BI), ([BH || {BH, _, _} <- BI])).

%% @doc A Macro to return whether a value is an address.
-define(IS_ADDR(Addr), (is_binary(Addr) and (bit_size(Addr) == ?HASH_SZ))).

%% @doc Pattern matches on ok-tuple and returns the value.
-define(OK(Tuple), begin (case (Tuple) of {ok, SuccessValue} -> (SuccessValue) end) end).

%% The messages to be stored inside the genesis block.
-define(GENESIS_BLOCK_MESSAGES, []).

%% @doc Minimum number of characters for internal API secret.
-define(INTERNAL_API_SECRET_MIN_LEN, 16).

-endif.
