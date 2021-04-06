%% The size in bytes of a portion of the disk space reserved to account for the lag
%% between getting close to no available space and receiving the information about it.
%% The node would only sync data if it has at least so much of the available space.
-ifdef(DEBUG).
-define(DISK_DATA_BUFFER_SIZE, 30 * 1024 * 1024).
-else.
-define(DISK_DATA_BUFFER_SIZE, 20 * 1024 * 1024 * 1024). % >15 GiB ~5 mins of syncing at 60 MiB/s
-endif.

%% How many peers to collect sync records from.
-define(PEER_SYNC_RECORD_COUNT, 50).

%% The upper limit for how many synced intervals to keep per peer.
-define(MAX_PEER_INTERVALS_COUNT, 100000).

%% How often to pick a new set of peers to collect sync records from.
-ifdef(DEBUG).
-define(SHUFFLE_PEERS_FREQUENCY_MS, 2000).
-else.
-define(SHUFFLE_PEERS_FREQUENCY_MS, 10 * 60 * 1000).
-endif.

%% How long to wait before asking peer for yet another sync record chunk.
-ifdef(DEBUG).
-define(PEER_SYNC_RECORD_NEW_CHUNK_DELAY_MS, 500).
-else.
-define(PEER_SYNC_RECORD_NEW_CHUNK_DELAY_MS, 10 * 1000).
-endif.

%% How long to wait before asking peer for sync record updates after fetching its full record.
-ifdef(DEBUG).
-define(PEER_SYNC_RECORD_UPDATE_DELAY_MS, 500).
-else.
-define(PEER_SYNC_RECORD_UPDATE_DELAY_MS, 60 * 1000).
-endif.

%% How long to wait after peer fails to serve a sync record before asking again.
-ifdef(DEBUG).
-define(PEER_FAILED_TO_SERVE_SYNC_RECORD_DELAY_MS, 1000).
-else.
-define(PEER_FAILED_TO_SERVE_SYNC_RECORD_DELAY_MS, 60 * 1000).
-endif.

%% The time a sync job waits after it fails to find an interval
%% by any of its peers.
-define(SEARCH_FOR_SYNC_INTERVAL_DELAY_MS, 1000).

%% The size in bits of the offset key in kv databases.
-define(OFFSET_KEY_BITSIZE, 256).

%% The size in bits of the key prefix used in prefix bloom filter
%% when looking up chunks by offsets from kv database.
%% 29 bytes of the prefix correspond to the 16777216 (16 Mib) max distance
%% between the keys with the same prefix. The prefix should be bigger than
%% max chunk size (256 KiB) so that the chunk in question is likely to be
%% found in the filter and smaller than an SST table (200 MiB) so that the
%% filter lookup can narrow the search down to a single table. @end
-define(OFFSET_KEY_PREFIX_BITSIZE, 232).

%% The number of block confirmations to track. When the node
%% joins the network or a chain reorg occurs, it uses its record about
%% the last ?TRACK_CONFIRMATIONS blocks and the new block index to
%% determine the orphaned portion of the weave.
-define(TRACK_CONFIRMATIONS, ?STORE_BLOCKS_BEHIND_CURRENT * 2).

%% Try to have so many spread out continuous intervals.
-ifdef(DEBUG).
-define(SYNCED_INTERVALS_TARGET, 10).
-else.
-define(SYNCED_INTERVALS_TARGET, 2000).
-endif.

%% The maximum number of synced intervals shared with peers.
-ifdef(DEBUG).
-define(MAX_SHARED_SYNCED_INTERVALS_COUNT, 20).
-else.
-define(MAX_SHARED_SYNCED_INTERVALS_COUNT, 10000).
-endif.

%% The upper limit for the size of a sync record serialized using Erlang Term Format.
-define(MAX_ETF_SYNC_RECORD_SIZE, 80 * ?MAX_SHARED_SYNCED_INTERVALS_COUNT).

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
-define(DISK_POOL_SCAN_FREQUENCY_MS, 1000).
-else.
-define(DISK_POOL_SCAN_FREQUENCY_MS, 10000).
-endif.

%% The frequency of removing expired data roots from the disk pool.
-define(REMOVE_EXPIRED_DATA_ROOTS_FREQUENCY_MS, 60000).

%% Time to wait before retrying a failed migration step.
-define(MIGRATION_RETRY_DELAY_MS, 10000).

%% The frequency of storing the server state on disk.
-define(STORE_STATE_FREQUENCY_MS, 30000).

%% @doc The state of the server managing data synchronization.
-record(sync_data_state, {
	%% The mapping Peer => SyncRecord containing sync records of the best peers.
	peer_sync_records,
	%% The last ?TRACK_CONFIRMATIONS entries of the block index.
	%% Used to determine orphaned data upon startup or chain reorg.
	block_index,
	%% The current weave size. The upper limit for the absolute chunk end offsets.
	weave_size,
	%% A reference to the on-disk key-value storage mapping
	%% AbsoluteChunkEndOffset => {ChunkDataIndexKey, TXRoot, DataRoot, TXPath, ChunkSize}.
	%% for all synced chunks.
	%%
	%% Chunks themselves and their DataPaths are stored separately because the offsets
	%% may change after a reorg. However, after the offset falls below DiskPoolThreshold,
	%% the chunk is packed for mining and recorded in the fast storage under the offset key.
	%%
	%% The index is used to look up the chunk by a random offset when a peer
	%% asks for it and to look up chunks of a transaction.
	chunks_index,
	%% A reference to the on-disk key-value storage mapping
	%% << DataRoot/binary, TXSize:256 >> => TXRoot => AbsoluteTXStartOffset => TXPath.
	%%
	%% The index is used to look up tx_root for a submitted chunk and
	%% to compute AbsoluteChunkEndOffset for the accepted chunk.
	%%
	%% We need the index because users should be able to submit their
	%% data without monitoring the chain, otherwise chain reorganisations
	%% might make the experience very unnerving. The index is NOT used
	%% for serving random chunks therefore it is possible to develop
	%% a lightweight client which would sync and serve random portions
	%% of the weave without maintaining this index.
	%%
	%% The index contains data roots of all stored chunks therefore it is used
	%% to determine if an orphaned data root can be deleted (the same data root
	%% can belong to multiple tx roots). A potential lightweight client without
	%% this index can simply only sync properly confirmed chunks that are extremely
	%% unlikely to be orphaned.
	data_root_index,
	%% A reference to the on-disk key-value storage mapping
	%% AbsoluteBlockStartOffset => {TXRoot, BlockSize, DataRootIndexKeySet}.
	%% Each key in DataRootIndexKeySet is a << DataRoot/binary, TXSize:256 >> binary.
	%%
	%% Used to remove orphaned entries from DataRootIndex and to determine
	%% TXRoot when syncing random offsets of the weave.
	%% DataRootIndexKeySet may be empty - in this case, the corresponding index entry
	%% is only used to for syncing the weave.
	data_root_offset_index,
	%% A map of pending, orphaned, and recent data roots
	%% << DataRoot/binary, TXSize:256 >> => {Size, Timestamp, TXIDSet}.
	%%
	%% Unconfirmed chunks can be accepted only after their data roots end up in this set.
	%% Each time a pending data root is added to the map the size is set to 0. New chunks
	%% for these data roots are accepted until the corresponding size reaches
	%% #config.max_disk_pool_data_root_buffer_mb or the total size of added pending chunks
	%% reaches #config.max_disk_pool_buffer_mb. When a data root is orphaned, its timestamp
	%% is refreshed so that the chunks have chance to be reincluded later.
	%% After a data root expires, the corresponding chunks are removed from
	%% disk_pool_chunks_index and if they are not in data_root_index - from storage.
	%% TXIDSet keeps track of pending transaction identifiers - if all pending transactions
	%% with the << DataRoot/binary, TXSize:256 >> key are dropped from the mempool,
	%% the corresponding entry is removed from DiskPoolDataRoots. When a data root is confirmed,
	%% TXIDSet is set to not_set - from this point on, the key cannot be dropped.
	disk_pool_data_roots,
	%% A reference to the on-disk key value storage mapping
	%% << DataRootTimestamp:256, ChunkDataIndexKey/binary >> =>
	%%     {RelativeChunkEndOffset, ChunkSize, DataRoot, TXSize, ChunkDataIndexKey}.
	%%
	%% The index is used to keep track of pending, orphaned, and recent chunks.
	%% A periodic process iterates over chunks from earliest to latest, consults
	%% DiskPoolDataRoots and data_root_index to decide whether each chunk needs to
	%% be removed from disk as orphaned, reincluded into the weave (by updating chunks_index),
	%% or removed from disk_pool_chunks_index by expiration.
	disk_pool_chunks_index,
	%% The sum of sizes of all pending chunks. When it reaches
	%% ?MAX_DISK_POOL_BUFFER_MB, new chunks with these data roots are rejected.
	disk_pool_size,
	%% One of the keys from disk_pool_chunks_index or the atom "first".
	%% The disk pool is processed chunk by chunk going from the oldest entry to the newest,
	%% trying not to block the syncing process if the disk pool accumulates a lot of orphaned
	%% and pending chunks. The cursor remembers the key after the last processed on the
	%% previous iteration. After reaching the last key in the storage, we go back to
	%% the first one. Not stored.
	disk_pool_cursor,
	%% The weave offset for the disk pool - chunks above this offset are stored there.
	disk_pool_threshold,
	%% A reference to the on-disk key value storage mapping
	%% TXID => {AbsoluteTXEndOffset, TXSize}.
	%% It is used to serve transaction data by TXID.
	tx_index,
	%% A reference to the on-disk key value storage mapping
	%% AbsoluteTXStartOffset => TXID. It is used to cleanup orphaned transactions from tx_index.
	tx_offset_index,
	%% A reference to the on-disk key value storage mapping
	%% << Timestamp:256, DataPathHash/binary >> of the chunks to chunk data.
	%% The motivation to not store chunk data directly in the chunks_index is to save the
	%% space by not storing identical chunks placed under different offsets several time
	%% and to be able to quickly move chunks from the disk pool to the on-chain storage.
	%% The timestamp prefix is used to make the written entries sorted from the start,
	%% to minimize the LSTM compaction overhead.
	chunk_data_db,
	%% A reference to the on-disk key value storage mapping migration names to their stages.
	migrations_index,
	%% A flag indicating whether the disk is full. If true, we avoid writing anything to it.
	disk_full = false,
	%% A flag indicating whether there is sufficient disk space for syncing more data.
	sync_disk_space = true
}).
