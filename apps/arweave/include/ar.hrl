-ifndef(AR_HRL).
-define(AR_HRL, true).

%%% A collection of record structures used throughout the Arweave server.

%% True if arweave was launched with -setcookie=test
%% (e.g. bin/test or bin/shell)
-define(IS_TEST, erlang:get_cookie() == test).

%% The mainnet name. Does not change at the hard forks.
-ifndef(NETWORK_NAME).
	-ifdef(DEBUG).
		-define(NETWORK_NAME, "arweave.localtest").
	-else.
		-define(NETWORK_NAME, "arweave.N.1").
	-endif.
-endif.

%% When a request is received without specifing the X-Network header, this network name
%% is assumed.
-ifndef(DEFAULT_NETWORK_NAME).
	-define(DEFAULT_NETWORK_NAME, "arweave.N.1").
-endif.

%% The current release number of the arweave client software.
%% @deprecated Not used apart from being included in the /info response.
-define(CLIENT_VERSION, 5).

%% The current build number -- incremented for every release.
-define(RELEASE_NUMBER, 70).

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

-define(RSA_SIGN_ALG, rsa).
-define(RSA_PRIV_KEY_SZ, 4096).

-define(ECDSA_SIGN_ALG, ecdsa).
-define(ECDSA_TYPE_BYTE, <<2>>).

-define(EDDSA_SIGN_ALG, eddsa).
-define(EDDSA_TYPE_BYTE, <<3>>).

%% The default key type used by transactions that do not specify a signature type.
-define(DEFAULT_KEY_TYPE, {?RSA_SIGN_ALG, 65537}).

%% The difficulty a new weave is started with.
-define(DEFAULT_DIFF, 6).

-ifndef(TARGET_BLOCK_TIME).
-define(TARGET_BLOCK_TIME, 120).
-endif.

-ifndef(RETARGET_BLOCKS).
-define(RETARGET_BLOCKS, 10).
-endif.

%% We only do retarget if the time it took to mine ?RETARGET_BLOCKS is more than
%% 1.1 times bigger or smaller than ?TARGET_BLOCK_TIME * ?RETARGET_BLOCKS. Was used before
%% the fork 2.5 where we got rid of the floating point calculations.
-define(RETARGET_TOLERANCE, 0.1).

-define(JOIN_CLOCK_TOLERANCE, 15).

-define(MAX_BLOCK_PROPAGATION_TIME, 60).

-define(CLOCK_DRIFT_MAX, 5).

%% The total supply of tokens in the Genesis block.
-define(GENESIS_TOKENS, 55000000).

%% Winstons per AR.
-define(WINSTON_PER_AR, 1000000000000).

%% The number of bytes in a gibibyte.
-define(MiB, (1024 * 1024)).
-define(GiB, (1024 * ?MiB)).
-define(TiB, (1024 * ?GiB)).

%% How far into the past or future the block can be in order to be accepted for
%% processing.
-ifdef(DEBUG).
-define(STORE_BLOCKS_BEHIND_CURRENT, 10).
-else.
-define(STORE_BLOCKS_BEHIND_CURRENT, 50).
-endif.

%% The maximum lag when fork recovery (chain reorganisation) is performed.
-ifdef(DEBUG).
-define(CHECKPOINT_DEPTH, 4).
-else.
-define(CHECKPOINT_DEPTH, 18).
-endif.

%% The recommended depth of the block to use as an anchor for transactions.
%% The corresponding block hash is returned by the GET /tx_anchor endpoint.
-ifdef(DEBUG).
-define(SUGGESTED_TX_ANCHOR_DEPTH, 5).
-else.
-define(SUGGESTED_TX_ANCHOR_DEPTH, 6).
-endif.

%% The number of blocks returned in the /info 'recent' field
-ifdef(DEBUG).
-define(INFO_BLOCKS, 5).
-define(INFO_BLOCKS_WITHOUT_TIMESTAMP, 2).
-else.
-define(INFO_BLOCKS, 10).
-define(INFO_BLOCKS_WITHOUT_TIMESTAMP, 5).
-endif.

%% How long to wait before giving up on test(s).
-define(TEST_TIMEOUT, 90 * 60).

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

%% Maximum allowed number of accepted requests per minute per IP.
-define(DEFAULT_REQUESTS_PER_MINUTE_LIMIT, 900).

%% Number of seconds an IP address should be completely banned from doing
%% HTTP requests after posting an invalid block.
-define(BAD_BLOCK_BAN_TIME, 24 * 60 * 60).

%% A part of transaction propagation delay independent from the size, in seconds.
-ifdef(DEBUG).
-define(BASE_TX_PROPAGATION_DELAY, 0).
-else.
-ifndef(BASE_TX_PROPAGATION_DELAY).
-define(BASE_TX_PROPAGATION_DELAY, 30).
-endif.
-endif.

%% A conservative assumption of the network speed used to
%% estimate the transaction propagation delay. It does not include
%% the base delay, the time the transaction spends in the priority
%% queue, and the time it takes to propagate the transaction to peers.
-ifdef(DEBUG).
-define(TX_PROPAGATION_BITS_PER_SECOND, 1000000000).
-else.
-define(TX_PROPAGATION_BITS_PER_SECOND, 3000000). % 3 mbps
-endif.

%% The number of peers to send new blocks to in parallel.
-define(BLOCK_PROPAGATION_PARALLELIZATION, 20).

%% The maximum number of peers to propagate txs to, by default.
-define(DEFAULT_MAX_PROPAGATION_PEERS, 16).

%% The maximum number of peers to propagate blocks to, by default.
-define(DEFAULT_MAX_BLOCK_PROPAGATION_PEERS, 1000).

%% When the transaction data size is smaller than this number of bytes,
%% the transaction is gossiped to the peer without a prior check if the peer
%% already has this transaction.
-define(TX_SEND_WITHOUT_ASKING_SIZE_LIMIT, 1000).

%% Block headers directory, relative to the data dir.
-define(BLOCK_DIR, "blocks").

%% Transaction headers directory, relative to the data dir.
-define(TX_DIR, "txs").

%% Disk cache directory, relative to the data dir.
-define(DISK_CACHE_DIR, "disk_cache").

%% Block headers directory, relative to the disk cache directory.
-define(DISK_CACHE_BLOCK_DIR, "blocks").

%% Transaction headers directory, relative to the disk cache directory.
-define(DISK_CACHE_TX_DIR, "txs").

%% Directory with files indicating completed storage migrations, relative to the data dir.
-define(STORAGE_MIGRATIONS_DIR, "data/storage_migrations").

%% Backup block hash list storage directory, relative to the data dir.
-define(HASH_LIST_DIR, "hash_lists").

%% Directory for storing miner wallets, relative to the data dir.
-define(WALLET_DIR, "wallets").

%% Directory for storing unique wallet lists, relative to the data dir.
-define(WALLET_LIST_DIR, "wallet_lists").

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
%% Each emitter picks the most valued transaction from the queue
%% and propagates it to the chosen peers.
%% Can be overriden by a command line argument.
-define(NUM_EMITTER_PROCESSES, 16).

%% The adjustment of difficutly going from SHA-384 to RandomX.
-define(RANDOMX_DIFF_ADJUSTMENT, (-14)).
-ifdef(DEBUG).
-define(RANDOMX_KEY_SWAP_FREQ, (?STORE_BLOCKS_BEHIND_CURRENT + 1)).
-define(RANDOMX_MIN_KEY_GEN_AHEAD, 1).
-define(RANDOMX_MAX_KEY_GEN_AHEAD, 4).
-define(RANDOMX_KEEP_KEY, 3).
-else.
-define(RANDOMX_KEY_SWAP_FREQ, 2000).
-define(RANDOMX_MIN_KEY_GEN_AHEAD, (30 + ?STORE_BLOCKS_BEHIND_CURRENT)).
-define(RANDOMX_MAX_KEY_GEN_AHEAD, (90 + ?STORE_BLOCKS_BEHIND_CURRENT)).
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

%% Disk cache size in MB
-ifdef(DEBUG).
-define(DISK_CACHE_SIZE, 1).
-define(DISK_CACHE_CLEAN_PERCENT_MAX, 20).
-else.
-define(DISK_CACHE_SIZE, 5120).
-define(DISK_CACHE_CLEAN_PERCENT_MAX, 20).
-endif.

%% The speed in chunks/s of moving the fork 2.5 packing threshold.
-ifdef(DEBUG).
-define(PACKING_2_5_THRESHOLD_CHUNKS_PER_SECOND, 1).
-else.
-define(PACKING_2_5_THRESHOLD_CHUNKS_PER_SECOND, 10).
-endif.

%% The data_root of the system "padding" nodes inserted in the transaction Merkle trees
%% since the 2.5 fork block. User transactions cannot set <<>> for data_root unless
%% data_size == 0. The motivation is to place all chunks including those
%% smaller than 256 KiB into the 256 KiB buckets on the weave, to even out their chances to be
%% picked as recall chunks and therefore equally incentivize the storage.
-define(PADDING_NODE_DATA_ROOT, <<>>).

-ifdef(DEBUG).
-define(INITIAL_VDF_DIFFICULTY, 2).
-else.
-define(INITIAL_VDF_DIFFICULTY, 600_000).
-endif.

%% @doc A chunk with the proofs of its presence in the weave at a particular offset.
-record(poa, {
	%% DEPRECATED. Not used since the fork 2.4.
	option = 1,
	%% The path through the Merkle tree of transactions' "data_root"s.
	%% Proofs the inclusion of the "data_root" in the corresponding "tx_root"
	%% under the particular offset.
	tx_path = <<>>,
	%% The path through the Merkle tree of the identifiers of the chunks
	%% of the corresponding transaction. Proofs the inclusion of the chunk
	%% in the corresponding "data_root" under a particular offset.
	data_path = <<>>,
	chunk = <<>>
}).

%% @doc The information which simplifies validation of the nonce limiting procedures.
-record(nonce_limiter_info, {
	%% The output of the latest step - the source of the entropy for the mining nonces.
	output = <<>>,
	%% The output of the latest step of the previous block.
	prev_output = <<>>,
	%% The hash of the latest block mined below the current reset line.
	seed = <<>>,
	%% The hash of the latest block mined below the future reset line.
	next_seed = <<>>,
	%% The weave size of the latest block mined below the current reset line.
	partition_upper_bound = 0,
	%% The weave size of the latest block mined below the future reset line.
	next_partition_upper_bound = 0,
	%% The global sequence number of the nonce limiter step at which the block was found.
	global_step_number = 1,
	%% ?VDF_CHECKPOINT_COUNT_IN_STEP checkpoints from the most recent step in the nonce
	%% limiter process.
	last_step_checkpoints = [],
	%% A list of the output of each step of the nonce limiting process. Note: each step
	%% has ?VDF_CHECKPOINT_COUNT_IN_STEP checkpoints, the last of which is that step's output.
	steps = [],

	%% The fields added at the fork 2.7

	%% The number of SHA2-256 iterations in a single VDF checkpoint. The protocol aims to keep the
	%% checkoint calculation time to around 40ms by varying this paramter. Note: there are
	%% 25 checkpoints in a single VDF step - so the protocol aims to keep the step calculation at
	%% 1 second by varying this parameter.
	vdf_difficulty = ?INITIAL_VDF_DIFFICULTY,
	%% The VDF difficulty scheduled for to be applied after the next VDF reset line.
	next_vdf_difficulty = ?INITIAL_VDF_DIFFICULTY
}).

%% @doc A VDF session.
-record(vdf_session, {
	step_number,
	seed,
	step_checkpoints_map = #{},
	steps,
	prev_session_key,
	upper_bound,
	next_upper_bound,
	vdf_difficulty,
	next_vdf_difficulty
}).

%% @doc The format of the nonce limiter update provided by the configured trusted peer.
-record(nonce_limiter_update, {
	session_key,
	session,
	is_partial = true
}).

%% @doc The format of the response to nonce limiter updates by configured trusted peers.
-record(nonce_limiter_update_response, {
	session_found = true,
	step_number,
	postpone = 0,
	format = 2
}).

%% @doc A compact announcement of a new block gossiped to peers. Peers
%% who have not received this block yet and decide to receive it from us,
%% should reply with a #block_announcement_response.
-record(block_announcement, {
	indep_hash,
	previous_block,
	recall_byte,
	tx_prefixes = [], % 8 byte prefixes of transaction identifiers.
	recall_byte2,
	solution_hash
}).

%% @doc A reply to a block announcement when we are willing to receive this
%% block from the announcing peer.
-record(block_announcement_response, {
	missing_chunk = false,
	missing_tx_indices = [], % Missing transactions' indices, 0 =<, =< 999.
	missing_chunk2
}).

%% @doc A block (txs is a list of tx records) or a block shadow (txs is a list of
%% transaction identifiers).
-record(block, {
	%% The nonce chosen to solve the mining problem.
	nonce,
	%% `indep_hash` of the previous block in the weave.
	previous_block = <<>>,
	%% POSIX time of block discovery.
	timestamp,
	%% POSIX time of the last difficulty retarget.
	last_retarget,
	%% Mining difficulty, the number `hash` must be greater than.
	diff,
	height = 0,
	%% Mining solution hash.
	hash = <<>>,
	%% The block identifier.
	indep_hash,
	%% The list of transaction identifiers or transactions (tx records).
	txs = [],
	%% The Merkle root of the tree of Merkle roots of block's transactions' data.
	tx_root = <<>>,
	%% The Merkle tree of Merkle roots of block's transactions' data. Used internally,
	%% not gossiped.
	tx_tree = [],
	%% Deprecated. Not used, not gossiped.
	hash_list = unset,
	%% The Merkle root of the block index - the list of
	%% {`indep_hash`, `weave_size`, `tx_root`} triplets describing the past blocks
	%% excluding this one.
	hash_list_merkle = <<>>,
	%% The root hash of the Merkle Patricia Tree containing all wallet (account) balances and
	%% the identifiers of the last transactions posted by them, if any
	wallet_list,
	%% The mining address. Before the fork 2.6, either the atom 'unclaimed' or
	%% a SHA2-256 hash of the RSA PSS public key. In 2.6, 'unclaimed' is not supported.
    reward_addr = unclaimed,
	%% Miner-specified tags (a list of strings) to store with the block.
    tags = [],
	%% The number of Winston in the endowment pool.
	reward_pool,
	%% The total number of bytes whose storage is incentivized.
	weave_size,
	%% The total number of bytes added to the storage incentivization by this block.
	block_size,
	%% The sum of the average number of hashes computed by the network to produce the past
	%% blocks including this one.
	cumulative_diff,
	%% The list of {{`tx_id`, `data_root`}, `offset`} pairs. Used internally, not gossiped.
	size_tagged_txs = unset,
	%% The first proof of access.
	poa = #poa{},
	%% The estimated USD to AR conversion rate used in the pricing calculations.
	%% A tuple {Dividend, Divisor}.
	%% Used until the transition to the new fee calculation method is complete.
	usd_to_ar_rate,
	%% The estimated USD to AR conversion rate scheduled to be used a bit later, used to
	%% compute the necessary fee for the currently signed txs. A tuple {Dividend, Divisor}.
	%% Used until the transition to the new fee calculation method is complete.
	scheduled_usd_to_ar_rate,
	%% The offset on the weave separting the data which has to be packed for mining after the
	%% fork 2.5 from the data which does not have to be packed yet. It is set to the
	%% weave_size of the 50th previous block at the hard fork block and moves down at a speed
	%% of ?PACKING_2_5_THRESHOLD_CHUNKS_PER_SECOND chunks/s. The motivation behind the
	%% threshold is a smooth transition to the new algorithm - big miners who might not want
	%% to adopt the new algorithm are still incentivized to upgrade and stay in the network
	%% for some time.
	packing_2_5_threshold,
	%% The offset on the weave separating the data which has to be split according to the
	%% stricter rules introduced in the fork 2.5 from the historical data. The new rules
	%% require all chunk sizes to be 256 KiB excluding the last or the only chunks of the
	%% corresponding transactions and the second last chunks of their transactions where they
	%% exceed 256 KiB in size when combined with the following (last) chunk. Furthermore, the
	%% new chunks may not be smaller than their Merkle proofs unless they are the last chunks.
	%% The motivation is to be able to put all chunks into 256 KiB buckets. It makes all
	%% chunks equally attractive because they have equal chances of being chosen as recall
	%% chunks. Moreover, every chunk costs the same in terms of storage and computation
	%% expenditure when packed (smaller chunks are simply padded before packing).
	strict_data_split_threshold,
	%% Used internally by tests.
	account_tree,

	%%
	%% The fields below were added at the fork 2.6.
	%%

	%% A part of the solution hash preimage. Used for the initial solution validation
	%% without a data chunk.
	hash_preimage = <<>>,
	%% The absolute recall offset.
	recall_byte,
	%% The total amount of winston the miner receives for this block.
	reward = 0,
	%% The solution hash of the previous block.
	previous_solution_hash = <<>>,
	%% The sequence number of the mining partition where the block was found.
	partition_number,
	%% The nonce limiter information.
	nonce_limiter_info = #nonce_limiter_info{},
	%% The second proof of access (empty when the solution was found with only one chunk).
	poa2 = #poa{},
	%% The absolute second recall offset.
	recall_byte2,
	%% The block signature.
	signature = <<>>,
	%% {KeyType, PubKey} - the public key the block was signed with.
	%% The only supported KeyType is currently {rsa, 65537}.
	reward_key,
	%% The estimated number of Winstons it costs the network to store one gibibyte
	%% for one minute.
	price_per_gib_minute = 0,
	%% The updated estimation of the number of Winstons it costs the network to store
	%% one gibibyte for one minute.
	scheduled_price_per_gib_minute = 0,
	%% The recursive hash of the network hash rates, block rewards, and mining addresses of
	%% the latest ?REWARD_HISTORY_BLOCKS blocks.
	reward_history_hash,
	%% The network hash rates, block rewards, and mining addresses from the latest
	%% ?REWARD_HISTORY_BLOCKS + ?STORE_BLOCKS_BEHIND_CURRENT blocks. Used internally, not gossiped.
	reward_history = [],
	%% The total number of Winston emitted when the endowment was not sufficient
	%% to compensate mining.
	debt_supply = 0,
	%% An additional multiplier for the transaction fees doubled every time the
	%% endowment pool becomes empty.
	kryder_plus_rate_multiplier = 1,
	%% A lock controlling the updates of kryder_plus_rate_multiplier. It is set to 1
	%% after the update and back to 0 when the endowment pool is bigger than
	%% ?RESET_KRYDER_PLUS_LATCH_THRESHOLD (redenominated according to the denomination
	%% used at the time).
	kryder_plus_rate_multiplier_latch = 0,
	%% The code for the denomination of AR in base units.
	%% 1 is the default which corresponds to the original denomination of 1^12 base units.
	%% Every time the available supply falls below ?REDENOMINATION_THRESHOLD,
	%% the denomination is multiplied by 1000, the code is incremented.
	%% Transaction denomination code must not exceed the block's denomination code.
	denomination = 1,
	%% The biggest known redenomination height (0 means there were no redenominations yet).
	redenomination_height = 0,
	%% The proof of signing the same block several times or extending two equal forks.
	double_signing_proof,
	%% The cumulative difficulty of the previous block.
	previous_cumulative_diff = 0,

	%%
	%% The fields below were added at the fork 2.7 (note that 2.6.8 was a hard fork too).
	%%

	%% The merkle trees of the data written after this weave offset may be constructed
	%% in a way where some subtrees are "rebased", i.e., their offsets start from 0 as if
	%% they were the leftmost subtree of the entire tree. The merkle paths for the chunks
	%% belonging to the subtrees will include a 32-byte 0-sequence preceding the pivot to
	%% the corresponding subtree. The rebases allow for flexible combination of data before
	%% registering it on the weave, extremely useful e.g., for the bundling services.
	merkle_rebase_support_threshold,
	%% The SHA2-256 of the packed chunk.
	chunk_hash,
	%% The SHA2-256 of the packed chunk2, when present.
	chunk2_hash,

	%% The hashes of the history of block times (in seconds), VDF times (in steps),
	%% and solution types (one-chunk vs two-chunk) of the latest
	%% ?BLOCK_TIME_HISTORY_BLOCKS blocks.
	block_time_history_hash,
	%% The block times (in seconds), VDF times (in steps), and solution types (one-chunk vs
	%% two-chunk) of the latest ?BLOCK_TIME_HISTORY_BLOCKS blocks.
	%% Used internally, not gossiped.
	block_time_history = [], % {block_interval, vdf_interval, chunk_count}

	%% Used internally, not gossiped. Convenient for validating potentially non-unique
	%% merkle proofs assigned to the different signatures of the same solution
	%% (see validate_poa_against_cached_poa in ar_block_pre_validator.erl).
	poa_cache,
	%% Used internally, not gossiped. Convenient for validating potentially non-unique
	%% merkle proofs assigned to the different signatures of the same solution
	%% (see validate_poa_against_cached_poa in ar_block_pre_validator.erl).
	poa2_cache,

	%% Used internally, not gossiped.
	receive_timestamp
}).

%% @doc A transaction.
-record(tx, {
	%% 1 or 2.
	format = 1,
	%% The transaction identifier.
	id = <<>>,
	%% Either the identifier of the previous transaction from
	%% the same wallet or the identifier of one of the
	%% last ?MAX_TX_ANCHOR_DEPTH blocks.
	last_tx = <<>>,
	%% The public key the transaction is signed with.
	owner =	<<>>,
	%% A list of arbitrary key-value pairs. Keys and values are binaries.
	tags = [],
	%% The address of the recipient, if any. The SHA2-256 hash of the public key.
	target = <<>>,
	%% The amount of Winstons to send to the recipient, if any.
	quantity = 0,
	%% The data to upload, if any. For v2 transactions, the field is optional - a fee
	%% is charged based on the "data_size" field, data itself may be uploaded any time
	%% later in chunks.
	data = <<>>,
	%% Size in bytes of the transaction data.
	data_size = 0,
	%% Deprecated. Not used, not gossiped.
	data_tree = [],
	%% The Merkle root of the Merkle tree of data chunks.
	data_root = <<>>,
	%% The signature.
	signature = <<>>,
	%% The fee in Winstons.
	reward = 0,

	%% The code for the denomination of AR in base units.
	%%
	%% 1 corresponds to the original denomination of 1^12 base units.
	%% Every time the available supply falls below ?REDENOMINATION_THRESHOLD,
	%% the denomination is multiplied by 1000, the code is incremented.
	%%
	%% 0 is the default denomination code. It is treated as the denomination code of the
	%% current block. We do NOT default to 1 because we want to distinguish between the
	%% transactions with the explicitly assigned denomination (the denomination then becomes
	%% a part of the signature preimage) and transactions signed the way they were signed
	%% before the upgrade. The motivation is to keep supporting legacy client libraries after
	%% redenominations and at the same time protect users from an attack where
	%% a post-redenomination transaction is included in a pre-redenomination block. The attack
	%% is prevented by forbidding inclusion of transactions with denomination=0 in the 100
	%% blocks preceding the redenomination block.
	%%
	%% Transaction denomination code must not exceed the block's denomination code.
	denomination = 0,

	%% The type of signature this transaction was signed with. A system field,
	%% not used by the protocol yet.
	signature_type = ?DEFAULT_KEY_TYPE
}).

%% A macro to convert AR into Winstons.
-define(AR(AR), (?WINSTON_PER_AR * AR)).

%% A macro to return whether a term is a block record.
-define(IS_BLOCK(X), (is_record(X, block))).

%% Convert a v2.0 block index into an old style block hash list.
-define(BI_TO_BHL(BI), ([BH || {BH, _, _} <- BI])).

%% Pattern matches on ok-tuple and returns the value.
-define(OK(Tuple), begin (case (Tuple) of {ok, SuccessValue} -> (SuccessValue) end) end).

%% The messages to be stored inside the genesis block.
-define(GENESIS_BLOCK_MESSAGES, []).

%% Minimum number of characters for internal API secret. Used in the optional HTTP API
%% for signing transactions.
-define(INTERNAL_API_SECRET_MIN_LEN, 16).

%% The frequency of issuing a reminder to the console and the logfile
%% about the insufficient disk space, in milliseconds.
-define(DISK_SPACE_WARNING_FREQUENCY, 24 * 60 * 60 * 1000).

%% Use a standard way of logging.
%% For more details see https://erlang.org/doc/man/logger.html#macros.
-include_lib("kernel/include/logger.hrl").

-endif.
