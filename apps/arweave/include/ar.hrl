-ifndef(AR_HRL).
-define(AR_HRL, true).

%%% A collection of record structures used throughout the Arweave server.

%% How nodes identify they are on the same network.
-ifndef(NETWORK_NAME).
-define(NETWORK_NAME, "arweave.N.1").
-endif.

%% Current release number of the arweave client software.
%% @deprecated Not used apart from being included in the /info response.
-define(CLIENT_VERSION, 5).

%% The current build number -- incremented for every release.
-define(RELEASE_NUMBER, 47).

-define(DEFAULT_REQUEST_HEADERS,
	[
		{<<"X-Network">>, ?NETWORK_NAME},
		{<<"X-Version">>, <<"8">>},
		{<<"X-Block-Format">>, <<"3">>}
	]).

-define(CORS_HEADERS,
	#{<<"access-control-allow-origin">> => <<"*">>}).

-ifdef(FORKS_RESET).
-define(FORK_1_6, 0).
-else.
%%% FORK INDEX
%%% @deprecated Fork heights from 1.7 on are defined in the ar_fork module.
-define(FORK_1_6, 95000).
-endif.

%% The hashing algorithm used to calculate wallet addresses.
-define(HASH_ALG, sha256).

-define(DEEP_HASH_ALG, sha384).

-define(MERKLE_HASH_ALG, sha384).

-define(HASH_SZ, 256).

-define(SIGN_ALG, rsa).

-define(PRIV_KEY_SZ, 4096).

%% The difficulty a new weave is started with.
-define(DEFAULT_DIFF, 8).

-ifndef(TARGET_TIME).
-define(TARGET_TIME, 120).
-endif.

-ifndef(RETARGET_BLOCKS).
-define(RETARGET_BLOCKS, 10).
-endif.

%% We only do retarget if the time it took to mine ?RETARGET_BLOCKS is bigger than
%% or equal to ?RETARGET_TOLERANCE_UPPER_BOUND or smaller than or equal to
%% ?RETARGET_TOLERANCE_LOWER_BOUND.
-define(RETARGET_TOLERANCE_UPPER_BOUND, ((?TARGET_TIME * ?RETARGET_BLOCKS) + ?TARGET_TIME)).

%% We only do retarget if the time it took to mine ?RETARGET_BLOCKS is bigger than
%% or equal to ?RETARGET_TOLERANCE_UPPER_BOUND or smaller than or equal to
%% ?RETARGET_TOLERANCE_LOWER_BOUND.
-define(RETARGET_TOLERANCE_LOWER_BOUND, ((?TARGET_TIME * ?RETARGET_BLOCKS) - ?TARGET_TIME)).

%% We only do retarget if the time it took to mine ?RETARGET_BLOCKS is more than
%% 1.1 times bigger or smaller than ?TARGET_TIME * ?RETARGET_BLOCKS. Was used before
%% the fork 2.5 where we got rid of the floating point calculations.
-define(RETARGET_TOLERANCE, 0.1).

-define(JOIN_CLOCK_TOLERANCE, 15).

-define(MAX_BLOCK_PROPAGATION_TIME, 60).

-define(CLOCK_DRIFT_MAX, 5).

-ifdef(DEBUG).
-define(MINING_TIMESTAMP_REFRESH_INTERVAL, 1).
-else.
-define(MINING_TIMESTAMP_REFRESH_INTERVAL, 10).
-endif.

%% The total supply of tokens in the Genesis block.
-define(GENESIS_TOKENS, 55000000).

%% Winstons per AR.
-define(WINSTON_PER_AR, 1000000000000).

%% How far into the past or future the block can be in order to be accepted for
%% processing. The maximum lag when fork recovery (chain reorganisation) is performed.
-ifdef(DEBUG).
-define(STORE_BLOCKS_BEHIND_CURRENT, 10).
-else.
-define(STORE_BLOCKS_BEHIND_CURRENT, 50).
-endif.

%% How long to wait before giving up on test(s).
-define(TEST_TIMEOUT, 30 * 60).

%% The maximum byte size of a single POST body.
-define(MAX_BODY_SIZE, 15 * 1024 * 1024).

%% The maximum allowed size in bytes for the data field of
%% a format=1 transaction.
-define(TX_DATA_SIZE_LIMIT, 10 * 1024 * 1024).

%% The maximum allowed size in bytes for the combined data fields of
%% the format=1 transactions included in a block. Must be greater than
%% or equal to ?TX_DATA_SIZE_LIMIT.
-define(BLOCK_TX_DATA_SIZE_LIMIT, ?TX_DATA_SIZE_LIMIT).

%% The maximum number of transactions (both format=1 and format=2) in a block.
-ifdef(DEBUG).
-define(BLOCK_TX_COUNT_LIMIT, 10).
-else.
-define(BLOCK_TX_COUNT_LIMIT, 1000).
-endif.

%% The base transaction size the transaction fee must pay for.
-define(TX_SIZE_BASE, 3210).

%% Mempool Limits.
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


%% The maximum allowed size of transaction headers stored in mempool.
%% The data field of a format=1 transaction is considered to belong to
%% its headers.
-ifdef(DEBUG).
-define(MEMPOOL_HEADER_SIZE_LIMIT, 50 * 1024 * 1024).
-else.
-define(MEMPOOL_HEADER_SIZE_LIMIT, 250 * 1024 * 1024).
-endif.

%% The maximum allowed size of transaction data stored in mempool.
%% The format=1 transactions are not counted as their data is considered
%% to be part of the header.
-ifdef(DEBUG).
-define(MEMPOOL_DATA_SIZE_LIMIT, 50 * 1024 * 1024).
-else.
-define(MEMPOOL_DATA_SIZE_LIMIT, 500 * 1024 * 1024).
-endif.

%% The size limits for the transaction priority queue.
%%
%% The two limits are used to align the priority queues of the miners
%% accepting transactions with and without data. See above for the
%% explanation of why two mempool size limits are used. Simply not
%% accounting for transaction data size in the priority queue would not
%% work because big format=2 transaction would never be dropped from
%% the queue due to the full data mempool.

%% The maximum allowed size in bytes of the transaction headers
%% in the transaction queue. The data fields of format=1 transactions
%% count as transaction headers. When the limit is reached, transactions
%% with the lowest utility score are dropped from the queue.
%%
%% This limit has to be lower than the corresponding mempool limit,
%% otherwise transactions would never be dropped from the queue.
-define(TX_QUEUE_HEADER_SIZE_LIMIT, 200 * 1024 * 1024).

%% The maximum allowed size in bytes of the transaction data
%% in the transaction queue. The data fields of format=1 transactions
%% does not count as transaction data. When the limit is reached, transactions
%% with the lowest utility score are dropped from the queue.
%%
%% This limit has to be lower than the corresponding mempool limit,
%% otherwise transactions would never be dropped from the queue.
-define(TX_QUEUE_DATA_SIZE_LIMIT, 400 * 1024 * 1024).

-define(MAX_TX_ANCHOR_DEPTH, ?STORE_BLOCKS_BEHIND_CURRENT).

%% Default timeout for establishing an HTTP connection.
-define(HTTP_REQUEST_CONNECT_TIMEOUT, 10 * 1000).

%% Default timeout used when sending to and receiving from a TCP socket
%% when making an HTTP request.
-define(HTTP_REQUEST_SEND_TIMEOUT, 60 * 1000).

%% The time in milliseconds to wait before retrying
%% a failed join (block index download) attempt.
-define(REJOIN_TIMEOUT, 10 * 1000).

%% How many times to retry fetching the block index from each of
%% the peers before giving up.
-define(REJOIN_RETRIES, 3).

%% The frequency in milliseconds of asking peers for their peers.
-define(GET_MORE_PEERS_TIME,  240 * 1000).

%% Number of transfers for which not to score (and potentially drop)
%% new peers.
-define(PEER_GRACE_PERIOD, 100).

%% Never drop to lower than this number of peers.
-define(MINIMUM_PEERS, 4).

%% Never have more than this number of peers (new peers excluded).
-define(MAXIMUM_PEERS, 20).

%% Maximum allowed number of accepted requests per minute per IP.
-define(DEFAULT_REQUESTS_PER_MINUTE_LIMIT, 900).

%% Number of seconds an IP address should be completely banned from doing
%% HTTP requests after posting a block with bad PoW.
-define(BAD_POW_BAN_TIME, 24 * 60 * 60).

%% Delay before mining rewards manifest.
-define(REWARD_DELAY, ?BLOCK_PER_YEAR/4).

%% Peers to never add to the peer list.
-define(PEER_PERMANENT_BLACKLIST, []).

%% How long to wait before dropping after last activity, in seconds.
-define(PEER_TIMEOUT, 8 * 60).

%% A part of transaction propagation delay independent from the size, in seconds.
-ifdef(DEBUG).
-define(BASE_TX_PROPAGATION_DELAY, 0).
-else.
-define(BASE_TX_PROPAGATION_DELAY, 30).
-endif.

%% A conservative assumption of the network speed used to
%% estimate the transaction propagation delay. It does not include
%% the base delay, the time the transaction spends in the priority
%% queue, and the time it takes to propagate the transaction to peers.
-ifdef(DEBUG).
-define(TX_PROPAGATION_BITS_PER_SECOND, 1000000000).
-else.
-define(TX_PROPAGATION_BITS_PER_SECOND, 160000). % 160 kbps
-endif.

%% The number of the best peers to send new transactions to in parallel.
%% Can be overriden by a command line argument.
-define(TX_PROPAGATION_PARALLELIZATION, 4).

%% The number of the best peers to send new blocks to in parallel.
-define(BLOCK_PROPAGATION_PARALLELIZATION, 30).

%% The maximum number of peers to propagate blocks or txs to, by default.
-define(DEFAULT_MAX_PROPAGATION_PEERS, 50).

%% When the transaction data size is smaller than this number of bytes,
%% the transaction is gossiped to the peer without a prior check if the peer
%% already has this transaction.
-define(TX_SEND_WITHOUT_ASKING_SIZE_LIMIT, 1000).

%% Block headers directory, relative to the data dir.
-define(BLOCK_DIR, "blocks").

%% Transaction headers directory, relative to the data dir.
-define(TX_DIR, "txs").

%% Directory with files indicating completed storage migrations, relative to the data dir.
-define(STORAGE_MIGRATIONS_DIR, "data/storage_migrations").

%% Backup block hash list storage directory, relative to the data dir.
-define(HASH_LIST_DIR, "hash_lists").

%% Directory for storing miner wallets, relative to the data dir.
-define(WALLET_DIR, "wallets").

%% Directory for storing unique wallet lists, relative to the data dir.
-define(WALLET_LIST_DIR, "wallet_lists").

%% Directory for storing the ArQL v2 index, relative to the data dir.
-define(SQLITE3_DIR, "data/sqlite3").

%% Directory for storing data chunks, relative to the data dir.
-define(DATA_CHUNK_DIR, "data_chunks").

%% Directory for RocksDB key-value storages, relative to the data dir.
-define(ROCKS_DB_DIR, "rocksdb").

%% Log output directory, NOT relative to the data dir.
-define(LOG_DIR, "logs").

%% The directory for persisted metrics, NOT relative to the data dir.
-define(METRICS_DIR, "metrics").

%% Default TCP port.
-define(DEFAULT_HTTP_IFACE_PORT, 1984).

%% Number of transaction propagation processes to spawn.
%% Each emitter picks a transaction from the queue and propagates it
%% to the best peers, a configured number of peers at a time.
%% Can be overriden by a command line argument.
-define(NUM_EMITTER_PROCESSES, 2).

%% Target number of blocks per year.
-define(BLOCK_PER_YEAR, (525600 / (?TARGET_TIME / 60))).

%% The adjustment of difficutly going from SHA-384 to RandomX.
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

%% Max allowed difficulty multiplication and division factors, before the fork 2.4.
-define(DIFF_ADJUSTMENT_DOWN_LIMIT, 2).
-define(DIFF_ADJUSTMENT_UP_LIMIT, 4).

%% Maximum size of a single data chunk, in bytes.
-define(DATA_CHUNK_SIZE, (256 * 1024)).

%% Maximum size of a `data_path`, in bytes.
-define(MAX_PATH_SIZE, (256 * 1024)).

%% The size of data chunk hashes, in bytes.
-define(CHUNK_ID_HASH_SIZE, 32).

-define(NOTE_SIZE, 32).

%% @doc A succinct proof of access to a recall byte found in a TX.
-record(poa, {
	option = 1,			% The recall byte option chosen, a sequence number.
	tx_path = <<>>,		% The path through the Merkle tree of transactions' `data_root`s,
						% from the `data_root` being proven to the corresponding `tx_root`.
	data_path = <<>>,	% The path through the Merkle tree of identifiers of chunks of the
						% corresponding transaction, from the chunk being proven to the
						% corresponding `data_root`.
	chunk = <<>>		% The data chunk.
}).

%% @doc A full block - the txs field is a list of tx records, or a block shadow -
%% the txs field is a list of transaction identifiers.
%% @end
-record(block, {
	nonce,									% The nonce chosen to solve the mining problem.
	previous_block,							% `indep_hash` of the previous block in the weave.
	timestamp = os:system_time(seconds),	% POSIX time of block discovery.
	last_retarget,							% POSIX time of the last difficulty retarget.
	diff,									% Mining difficulty, the number `hash` must be
											% greater than.
	height,									% How many blocks have passed since the genesis block.
	hash,									% Mining solution hash of the block, must satisfy the
											% mining difficulty.
	indep_hash,								% The block identifier.
	txs,									% A list of tx records in full blocks, or a list of TX
											% identifiers in block shadows.
	tx_root,								% Merkle root of the tree of Merkle roots of
											% block's transactions' data.
	tx_tree = [],							% The Merkle tree of Merkle roots of block's
											% transactions' data. Used internally, not gossiped.
	hash_list = unset,						% The list of the block identifiers of the last
											% ?STORE_BLOCKS_BEHIND_CURRENT blocks.
	hash_list_merkle,						% The Merkle root of the block index - the list of
											% {`indep_hash`, `weave_size`, `tx_root`} triplets
											% describing the past blocks not including this one.
	wallet_list = unset,					% The root hash of the Merkle Patricia Tree containing
											% all wallet (account) balances and the identifiers
											% of the last transactions posted by them, if any.
    reward_addr = unclaimed,				% The address (SHA2-256 hash of the public key) of
											% the miner or the atom 'unclaimed'.
    tags,									% Miner-specified tags to store with the block.
	reward_pool,							% The number of Winston in the endowment pool.
	weave_size,								% The total byte size of transactions' data in the
											% weave including this block.
	block_size,								% The total byte size of transactions' data inside
											% this block.
	cumulative_diff,						% The sum of the average number of hashes computed
											% by the network to produce the past blocks including
											% this one.
	size_tagged_txs = unset,				% The list of {{`tx_id`, `data_root`}, `offset`}.
											% Used internally, not gossiped.
	poa = #poa{},							% The proof of access.
	usd_to_ar_rate,							% The estimated USD to AR conversion rate used
											% in the pricing calculations.
											% A tuple {Dividend, Divisor}.
	scheduled_usd_to_ar_rate				% The estimated USD to AR conversion rate scheduled
											% to be used a bit later, used to compute the
											% necessary fee for the currently signed txs.
											% A tuple {Dividend, Divisor}.
}).

%% @doc A transaction.
-record(tx, {
	format = 1,			% 1 or 2.
	id = <<>>,			% The transaction identifier.
	last_tx = <<>>,		% Either the identifier of the previous transaction from the same
						% wallet or the identifier of one of the last ?MAX_TX_ANCHOR_DEPTH blocks.
	owner =	<<>>,		% The public key the transaction is signed with.
	tags = [],			% A list of arbitrary key-value pairs. Keys and values are binaries.
	target = <<>>,		% The address of the recipient, if any. The SHA2-256 hash of the public key.
	quantity = 0,		% The amount of Winstons to send to the recipient, if any.
	data = <<>>,		% The data to upload, if any. For v2 transactions, the field is
						% optional - a fee is charged based on the `data_size` field,
						% data may be uploaded any time later in chunks.
	data_size = 0,		% Size in bytes of the transaction data.
	data_tree = [],		% The Merkle tree of data chunks. Used internally, not gossiped.
	data_root = <<>>,	% The Merkle root of the Merkle tree of data chunks.
	signature = <<>>,	% The signature.
	reward = 0			% The fee in Winstons.
}).

%% Peering performance of a node.
-record(performance, {
	bytes = 0,
	time = 0,
	transfers = 0,
	timestamp = 0,
	timeout = os:system_time(seconds)
}).

%% A macro to convert AR into Winstons.
-define(AR(AR), (?WINSTON_PER_AR * AR)).

%% A macro to return whether a term is a block record.
-define(IS_BLOCK(X), (is_record(X, block))).

%% Convert a v2.0 block index into an old style block hash list.
-define(BI_TO_BHL(BI), ([BH || {BH, _, _} <- BI])).

%% A macro to return whether a value is an address.
-define(IS_ADDR(Addr), (is_binary(Addr) and (bit_size(Addr) == ?HASH_SZ))).

%% Pattern matches on ok-tuple and returns the value.
-define(OK(Tuple), begin (case (Tuple) of {ok, SuccessValue} -> (SuccessValue) end) end).

%% The messages to be stored inside the genesis block.
-define(GENESIS_BLOCK_MESSAGES, []).

%% Minimum number of characters for internal API secret. Used in the optional HTTP API
%% for signing transactions.
-define(INTERNAL_API_SECRET_MIN_LEN, 16).

%% Log the "No foreign blocks received" message after so many time in milliseconds
%% has passed without new blocks.
-ifdef(DEBUG).
-define(FOREIGN_BLOCK_ALERT_TIME, 3 * 1000).
-else.
-define(FOREIGN_BLOCK_ALERT_TIME, 60 * 60 * 1000).
-endif.

%% The frequency of issuing a reminder to the console and the logfile
%% about the insufficient disk space, in milliseconds.
-define(DISK_SPACE_WARNING_FREQUENCY, 24 * 60 * 60 * 1000).

%% Use a standard way of logging.
%% For more details see https://erlang.org/doc/man/logger.html#macros.
-include_lib("kernel/include/logger.hrl").

-endif.
