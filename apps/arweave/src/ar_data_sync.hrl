%% @doc The size in bytes of a portion of the disk space reserved to account for the lag
%% between getting close to no available space and receiving the information about it.
%% The node would only sync data if it has at least so much of the available space.
-ifdef(DEBUG).
-define(DISK_DATA_BUFFER_SIZE, 30 * 1024 * 1024).
-else.
-define(DISK_DATA_BUFFER_SIZE, 20 * 1024 * 1024 * 1024). % >15 GiB ~5 mins of syncing at 60 MiB/s
-endif.

%% @doc The number of peer sync records to consult each time we look for an interval to sync.
-define(CONSULT_PEER_RECORDS_COUNT, 5).
%% @doc The number of best peers to pick ?CONSULT_PEER_RECORDS_COUNT from, to fetch the
%% corresponding number of sync records.
-define(PICK_PEERS_OUT_OF_RANDOM_N, 20).

%% @doc The frequency of updating best peers' sync records.
-ifdef(DEBUG).
-define(PEER_SYNC_RECORDS_FREQUENCY_MS, 500).
-else.
-define(PEER_SYNC_RECORDS_FREQUENCY_MS, 2 * 60 * 1000).
-endif.

%% @doc The size in bits of the offset key in kv databases.
-define(OFFSET_KEY_BITSIZE, 256).

%% @doc The size in bits of the key prefix used in prefix bloom filter
%% when looking up chunks by offsets from kv database.
%% 29 bytes of the prefix correspond to the 16777216 (16 Mib) max distance
%% between the keys with the same prefix. The prefix should be bigger than
%% max chunk size (256 KiB) so that the chunk in question is likely to be
%% found in the filter and smaller than an SST table (200 MiB) so that the
%% filter lookup can narrow the search down to a single table. @end
-define(OFFSET_KEY_PREFIX_BITSIZE, 232).

%% @doc The number of block confirmations to track. When the node
%% joins the network or a chain reorg occurs, it uses its record about
%% the last ?TRACK_CONFIRMATIONS blocks and the new block index to
%% determine the orphaned portion of the weave.
-define(TRACK_CONFIRMATIONS, ?STORE_BLOCKS_BEHIND_CURRENT * 2).

%% @doc The maximum number of synced intervals shared with peers.
-ifdef(DEBUG).
-define(MAX_SHARED_SYNCED_INTERVALS_COUNT, 20).
-else.
-define(MAX_SHARED_SYNCED_INTERVALS_COUNT, 10000).
-endif.

%% @doc The upper limit for the size of a sync record serialized using Erlang Term Format.
-define(MAX_ETF_SYNC_RECORD_SIZE, 80 * ?MAX_SHARED_SYNCED_INTERVALS_COUNT).

%% @doc The upper size limit for a serialized chunk with its proof
%% as it travels around the network.
%%
%% It is computed as ?MAX_PATH_SIZE (data_path) + DATA_CHUNK_SIZE (chunk) +
%% 32 * 1000 (tx_path, considering the 1000 txs per block limit),
%% multiplied by 1.34 (Base64), rounded to the nearest 50000 -
%% the difference is sufficient to fit an offset, a data_root,
%% and special JSON chars.
-define(MAX_SERIALIZED_CHUNK_PROOF_SIZE, 750000).

%% @doc Transaction data bigger than this limit is not served in
%% GET /tx/<id>/data endpoint. Clients interested in downloading
%% such data should fetch it chunk by chunk.
-define(MAX_SERVED_TX_DATA_SIZE, 12 * 1024 * 1024).

%% @doc The default expiration time for a data root in the disk pool.
-define(DISK_POOL_DATA_ROOT_EXPIRATION_TIME_S, 2 * 60 * 60).

%% @doc The time to wait until the next full disk pool scan.
-ifdef(DEBUG).
-define(DISK_POOL_SCAN_FREQUENCY_MS, 2000).
-else.
-define(DISK_POOL_SCAN_FREQUENCY_MS, 120000).
-endif.

%% @doc The frequency of removing expired data roots from the disk pool.
-define(REMOVE_EXPIRED_DATA_ROOTS_FREQUENCY_MS, 60000).

%% @doc The default size limit for unconfirmed chunks, per data root.
-define(MAX_DISK_POOL_DATA_ROOT_BUFFER_MB, 50).

%% @doc The default total size limit for unconfirmed chunks.
-ifdef(DEBUG).
-define(MAX_DISK_POOL_BUFFER_MB, 100).
-else.
-define(MAX_DISK_POOL_BUFFER_MB, 2000).
-endif.

%% @doc The condition which is true if the chunk is too small compared to the proof.
%% Small chunks make syncing slower and increase space amplification.
-define(IS_CHUNK_PROOF_RATIO_NOT_ATTRACTIVE(Chunk, DataPath),
	byte_size(DataPath) == 0 orelse byte_size(DataPath) > byte_size(Chunk)).

%% @doc Time to wait before retrying a failed migration step.
-define(MIGRATION_RETRY_DELAY_MS, 10000).

%% @doc The frequency of storing the server state on disk.
-define(STORE_STATE_FREQUENCY_MS, 30000).

%% @doc The state of the server managing data synchronization.
-record(sync_data_state, {
	%% @doc A set of non-overlapping intervals of global byte offsets ((end, start))
	%% denoting the synced data. End offsets are defined on [1, weave size], start
	%% offsets are defined on [0, weave size).
	%%
	%% The set serves as a compact map of what is synced by the node. No matter
	%% how big the weave is or how much of it the node stores, this record
	%% can remain very small, compared to storing all chunk and transaction identifiers,
	%% whose number can effectively grow unlimited with time.
	sync_record,
	%% @doc The mapping peer -> sync_record containing sync records of the best peers.
	peer_sync_records,
	%% @doc The last ?TRACK_CONFIRMATIONS entries of the block index.
	%% Used to determine orphaned data upon startup or chain reorg.
	block_index,
	%% @doc The current weave size. The upper limit for the absolute chunk end offsets.
	weave_size,
	%% @doc A reference to the on-disk key-value storage mapping
	%% absolute_chunk_end_offset -> {chunk_data_index_key, tx_root, data_root, tx_path, chunk_size}
	%% for all synced chunks.
	%%
	%% Chunks themselves and their data_paths are stored separately
	%% in files identified by data_path hashes. This is made to avoid moving
	%% potentially huge data around during chain reorgs.
	%%
	%% The index is used to look up the chunk by a random offset when a peer
	%% asks about it and to look up chunks of a transaction.
	%%
	%% Every time a chunk is written to sync_record, it is also
	%% written to the chunks_index.
	chunks_index,
	%% @doc A reference to the on-disk key-value storage mapping
	%% << data_root, tx_size >> -> tx_root -> absolute_tx_start_offset -> tx_path.
	%%
	%% The index is used to look up tx_root for a submitted chunk and
	%% to compute absolute_chunk_end_offset for the accepted chunk.
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
	%% @doc A reference to the on-disk key-value storage mapping
	%% absolute_block_start_offset -> {tx_root, block_size, data_root_index_key_set}.
	%% Each key in data_root_index_key_set is a << data_root, tx_size >> binary.
	%%
	%% Used to remove orphaned entries from data_root_index and to determine
	%% tx_root when syncing random offsets of the weave.
	%% data_root_index_key_set may be empty - in this case, the corresponding index entry
	%% is only used to for syncing the weave.
	data_root_offset_index,
	%% @doc A map of pending, orphaned, and recent data roots
	%% << data_root, tx_size >> -> {size, timestamp, tx_id_set}.
	%%
	%% Unconfirmed chunks can be accepted only after their data roots end up in this set.
	%% Each time a pending data root is added to the map the size is set to 0. New chunks
	%% for these data roots are accepted until the corresponding size reaches
	%% ?MAX_DISK_POOL_DATA_ROOT_BUFFER_MB or the total size of added pending chunks
	%% reaches ?MAX_DISK_POOL_BUFFER_MB. When a data root is orphaned, it's timestamp
	%% is refreshed so that the chunks have chance to be reincluded later.
	%% After a data root expires, the corresponding chunks are removed from
	%% disk_pool_chunks_index and if they are not in data_root_index - from storage.
	%% tx_id_set keeps track of pending transaction identifiers - if all pending transactions
	%% with the << data_root, tx_size >> key are dropped from the mempool, the corresponding
	%% entry is removed from disk_pool_data_roots. When a data root is confirmed, tx_id_set
	%% is set to not_set - from this point on, the key can't be dropped after a mempool drop.
	disk_pool_data_roots,
	%% @doc A reference to the on-disk key value storage mapping
	%% << data_root_timestamp, chunk_data_index_key >> ->
	%%     {relative_chunk_end_offset, chunk_size, data_root, tx_size, chunk_data_index_key}.
	%%
	%% The index is used to keep track of pending, orphaned, and recent chunks.
	%% A periodic process iterates over chunks from earliest to latest, consults
	%% disk_pool_data_roots and data_root_index to decide whether each chunk needs to
	%% be removed from disk as orphaned, reincluded into the weave (by updating chunks_index),
	%% or removed from disk_pool_chunks_index by expiration.
	disk_pool_chunks_index,
	%% @doc The sum of sizes of all pending chunks. When it reaches
	%% ?MAX_DISK_POOL_BUFFER_MB, new chunks with these data roots are rejected.
	disk_pool_size,
	%% @doc One of the keys from disk_pool_chunks_index or the atom "first".
	%% The disk pool is processed chunk by chunk going from the oldest entry to the newest,
	%% trying not to block the syncing process if the disk pool accumulates a lot of orphaned
	%% and pending chunks. The cursor remembers the key after the last processed on the
	%% previous iteration. After reaching the last key in the storage, we go back to
	%% the first one. Not stored.
	disk_pool_cursor,
	%% @doc A reference to the on-disk key value storage mapping
	%% tx_id -> {absolute_end_offset, tx_size}.
	%% It is used to serve transaction data by TXID.
	tx_index,
	%% @doc A reference to the on-disk key value storage mapping
	%% absolute_tx_start_offset -> tx_id. It is used to cleanup orphaned
	%% transactions from tx_index.
	tx_offset_index,
	%% @doc A reference to the on-disk key value storage mapping
	%% << timestamp, data_path_hash >> of the chunks to chunk data.
	%% The motivation to not store chunk data directly in the chunks_index is to save the
	%% space by not storing identical chunks placed under different offsets several time
	%% and to be able to quickly move chunks from the disk pool to the on-chain storage.
	%% The timestamp prefix is used to make the written entries sorted from the start,
	%% to minimize the compaction overhead.
	chunk_data_db,
	%% @doc A reference to the on-disk key value storage mapping migration names to their
	%% stages.
	migrations_index
}).
