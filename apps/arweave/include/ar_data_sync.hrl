%% The size in bits of the offset key in kv databases.
-define(OFFSET_KEY_BITSIZE, 256).

%% The size in bits of the key prefix used in prefix bloom filter
%% when looking up chunks by offsets from kv database.
%% 29 bytes of the prefix correspond to the 16777216 (16 Mib) max distance
%% between the keys with the same prefix. The prefix should be bigger than
%% max chunk size (256 KiB) so that the chunk in question is likely to be
%% found in the filter and smaller than an SST table (200 MiB) so that the
%% filter lookup can narrow the search down to a single table.
-define(OFFSET_KEY_PREFIX_BITSIZE, 232).

%% The number of block confirmations to track. When the node
%% joins the network or a chain reorg occurs, it uses its record about
%% the last ?TRACK_CONFIRMATIONS blocks and the new block index to
%% determine the orphaned portion of the weave.
-define(TRACK_CONFIRMATIONS, (?STORE_BLOCKS_BEHIND_CURRENT * 2)).

%% The upper size limit for a serialized chunk with its proof
%% as it travels around the network.
%%
%% It is computed as ?MAX_PATH_SIZE (data_path) + DATA_CHUNK_SIZE (chunk) +
%% 32 * 1000 (tx_path, considering the 1000 txs per block limit),
%% multiplied by 1.34 (Base64), rounded to the nearest 50000 -
%% the difference is sufficient to fit an offset, a data_root,
%% and special JSON chars.
-define(MAX_SERIALIZED_CHUNK_PROOF_SIZE, 750000).

%% Transaction data bigger than this limit is not served in
%% GET /tx/<id>/data endpoint. Clients interested in downloading
%% such data should fetch it chunk by chunk.
-define(MAX_SERVED_TX_DATA_SIZE, 12 * 1024 * 1024).

%% The time to wait until the next full disk pool scan.
-ifdef(DEBUG).
-define(DISK_POOL_SCAN_DELAY_MS, 1000).
-else.
-define(DISK_POOL_SCAN_DELAY_MS, 10000).
-endif.

%% How often to measure the number of chunks in the disk pool index.
-ifdef(DEBUG).
-define(RECORD_DISK_POOL_CHUNKS_COUNT_FREQUENCY_MS, 1000).
-else.
-define(RECORD_DISK_POOL_CHUNKS_COUNT_FREQUENCY_MS, 5000).
-endif.

%% How long to keep the offsets of the recently processed "matured" chunks in a cache.
%% We use the cache to quickly skip matured chunks when scanning the disk pool.
-define(CACHE_RECENTLY_PROCESSED_DISK_POOL_OFFSET_LIFETIME_MS, 3 * 60 * 60 * 1000).

%% The frequency of removing expired data roots from the disk pool.
-define(REMOVE_EXPIRED_DATA_ROOTS_FREQUENCY_MS, 60000).

%% The frequency of storing the server state on disk.
-define(STORE_STATE_FREQUENCY_MS, 30000).

%% The maximum number of intervals to schedule for syncing. Should be big enough so
%% that many sync jobs cannot realistically process it completely before the process
%% that collects intervals fills it up.
-define(SYNC_INTERVALS_MAX_QUEUE_SIZE, 10000).

%% The maximum number of chunks currently being downloaded or processed.
-ifdef(DEBUG).
-define(SYNC_BUFFER_SIZE, 100).
-else.
-define(SYNC_BUFFER_SIZE, 1000).
-endif.

%% Defines how long we keep the interval excluded from syncing.
%% If we cannot find an interval by peers, we temporarily exclude
%% it from the sought ranges to prevent the syncing process from slowing down.
-ifdef(DEBUG).
-define(EXCLUDE_MISSING_INTERVAL_TIMEOUT_MS, 2000).
-else.
-define(EXCLUDE_MISSING_INTERVAL_TIMEOUT_MS, 10 * 60 * 1000).
-endif.

%% @doc The state of the server managing data synchronization.
-record(sync_data_state, {
	%% The last ?TRACK_CONFIRMATIONS entries of the block index.
	%% Used to determine orphaned data upon startup or chain reorg.
	block_index,
	%% The current weave size. The upper limit for the absolute chunk end offsets.
	weave_size,
	%% A reference to the on-disk key-value storage mapping
	%% AbsoluteChunkEndOffset
	%%   => {ChunkDataKey, TXRoot, DataRoot, TXPath, ChunkOffset, ChunkSize}
	%%
	%% Chunks themselves and their DataPaths are stored separately (in chunk_data_db)
	%% because the offsets may change after a reorg. However, after the offset falls below
	%% DiskPoolThreshold, the chunk is packed for mining and recorded in the fast storage
	%% under the offset key.
	%%
	%% The index is used to look up the chunk by a random offset when a peer
	%% asks for it and to look up chunks of a transaction.
	chunks_index,
	%% A reference to the on-disk key-value storage mapping
	%% << DataRoot/binary, TXSize/binary, AbsoluteTXStartOffset/binary >> => TXPath.
	%%
	%% The index is used to look up tx_root for a submitted chunk and compute
	%% AbsoluteChunkEndOffset for the accepted chunk.
	%%
	%% We need the index because users should be able to submit their data without
	%% monitoring the chain, otherwise chain reorganisations might make the experience
	%% very unnerving. The index is NOT consulted when serving random chunks therefore
	%% it is possible to develop a lightweight client which would sync and serve random
	%% portions of the weave without maintaining this index.
	data_root_index,
	data_root_index_old,
	%% A reference to the on-disk key-value storage mapping
	%% AbsoluteBlockStartOffset => {TXRoot, BlockSize, DataRootIndexKeySet}.
	%% Each key in DataRootIndexKeySet is a << DataRoot/binary, TXSize:256 >> binary.
	%% Used to remove orphaned entries from DataRootIndex.
	data_root_offset_index,
	%% A reference to the on-disk key value storage mapping
	%% << DataRootTimestamp:256, ChunkDataIndexKey/binary >> =>
	%%     {RelativeChunkEndOffset, ChunkSize, DataRoot, TXSize, ChunkDataKey, IsStrictSplit}.
	%%
	%% The index is used to keep track of pending, orphaned, and recent chunks.
	%% A periodic process iterates over chunks from earliest to latest, consults
	%% DiskPoolDataRoots and data_root_index to decide whether each chunk needs to
	%% be removed from disk as orphaned, reincluded into the weave (by updating chunks_index),
	%% or removed from disk_pool_chunks_index by expiration.
	disk_pool_chunks_index,
	disk_pool_chunks_index_old,
	%% One of the keys from disk_pool_chunks_index or the atom "first".
	%% The disk pool is processed chunk by chunk going from the oldest entry to the newest,
	%% trying not to block the syncing process if the disk pool accumulates a lot of orphaned
	%% and pending chunks. The cursor remembers the key after the last processed on the
	%% previous iteration. After reaching the last key in the storage, we go back to
	%% the first one. Not stored.
	disk_pool_cursor,
	%% The weave offset for the disk pool - chunks above this offset are stored there.
	disk_pool_threshold = 0,
	%% A reference to the on-disk key value storage mapping
	%% TXID => {AbsoluteTXEndOffset, TXSize}.
	%% Is used to serve transaction data by TXID.
	tx_index,
	%% A reference to the on-disk key value storage mapping
	%% AbsoluteTXStartOffset => TXID. Is used to cleanup orphaned transactions from tx_index.
	tx_offset_index,
	%% A reference to the on-disk key value storage mapping
	%% << Timestamp:256, DataPathHash/binary >> to raw chunk data (possibly packed).
	%%
	%% Is used to store disk pool chunks (their global offsets cannot be determined with
	%% certainty yet).
	%%
	%% The timestamp prefix is used to make the written entries sorted from the start,
	%% to minimize the LSTM compaction overhead.
	chunk_data_db,
	%% A reference to the on-disk key value storage mapping migration names to their stages.
	migrations_index,
	%% The offsets of the chunks currently scheduled for (re-)packing (keys) and
	%% some chunk metadata needed for storing the chunk once it is packed.
	packing_map = #{},
	%% The end offset of the last interval possibly scheduled for repacking or 0.
	repacking_cursor,
	%% If true, the node repacks 2.5-packed data in the default storage with 2.6 packing.
	repack_legacy_storage = false,
	%% The queue with unique {Start, End, Peer} triplets. Sync jobs are taking intervals
	%% from this queue and syncing them.
	sync_intervals_queue = gb_sets:new(),
	%% A compact set of non-overlapping intervals containing all the intervals from the
	%% sync intervals queue. We use it to quickly check which intervals have been queued
	%% already and avoid syncing the same interval twice.
	sync_intervals_queue_intervals = ar_intervals:new(),
	%% A key marking the beginning of a full disk pool scan.
	disk_pool_full_scan_start_key = none,
	%% The timestamp of the beginning of a full disk pool scan. Used to measure
	%% the time it takes to scan the current disk pool - if it is too short, we postpone
	%% the next scan to save some disk IO.
	disk_pool_full_scan_start_timestamp,
	%% A cache of the offsets of the recently "matured" chunks. We use it to quickly
	%% skip matured chunks when scanning the disk pool. The reason the chunk is still
	%% in the disk pool is some of its offsets have not matured yet (the same data can be
	%% submitted several times).
	recently_processed_disk_pool_offsets = #{},
	%% A registry of the currently processed disk pool chunks consulted by different
	%% disk pool jobs to avoid double-processing.
	currently_processed_disk_pool_keys = sets:new(),
	%% A flag used to temporarily pause all disk pool jobs.
	disk_pool_scan_pause = false,
	%% The mining address the chunks are packed with in 2.6.
	mining_address,
	%% The identifier of the storage module the process is responsible for.
	store_id,
	%% The identifier of the ETS table storing the intervals to skip when syncing.
	skip_intervals_table,
	%% The start offset of the range the module is responsible for.
	range_start = -1,
	%% The end offset of the range the module is responsible for.
	range_end = -1
}).
