%% The size in bits of the offset key in kv databases.
-define(OFFSET_KEY_BITSIZE, 256).

%% Polling interval for ar_device_lock cooperative locking. Sync workers
%% that fail to acquire (or are paused on) a device lock re-cast themselves
%% after this delay to retry without busy-spinning.
-ifdef(AR_TEST).
-define(DEVICE_LOCK_WAIT, 100).
-else.
-define(DEVICE_LOCK_WAIT, 5_000).
-endif.

%% A single sync unit: fetch the byte range [start_offset, end_offset) from
%% `peer` into storage module `store_id`. `footprint_key` groups chunks that
%% share the same 256 MiB entropy (replica.2.9 mode) for admission control;
%% `none` means the task has no footprint constraint. `retry_count` counts
%% down on transient errors; the task is abandoned at 0.
-record(sync_task, {
	start_offset,
	end_offset,
	peer,
	store_id,
	retry_count = 3,
	footprint_key = none
}).

%% The size in bits of the key prefix used in prefix bloom filter
%% when looking up chunks by offsets from kv database.
%% 29 bytes of the prefix correspond to the 16777216 (16 Mib) max distance
%% between the keys with the same prefix. The prefix should be bigger than
%% max chunk size (256 KiB) so that the chunk in question is likely to be
%% found in the filter and smaller than an SST table (200 MiB) so that the
%% filter lookup can narrow the search down to a single table.
-define(OFFSET_KEY_PREFIX_BITSIZE, 232).

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

%% The frequency of storing the server state on disk.
-define(STORE_STATE_FREQUENCY_MS, 30000).

%% The maximum number of chunks currently being downloaded or processed.
-ifdef(AR_TEST).
-define(SYNC_BUFFER_SIZE, 100).
-else.
-define(SYNC_BUFFER_SIZE, 1000).
-endif.

%% Defines how long we keep the interval excluded from syncing.
%% If we cannot find an interval by peers, we temporarily exclude
%% it from the sought ranges to prevent the syncing process from slowing down.
-ifdef(AR_TEST).
-define(EXCLUDE_MISSING_INTERVAL_TIMEOUT_MS, 5000).
-else.
-define(EXCLUDE_MISSING_INTERVAL_TIMEOUT_MS, 10 * 60 * 1000).
-endif.

%% Let at least this many chunks stack up, per storage module, then write them on disk in the
%% ascending order, to reduce out-of-order disk writes causing fragmentation.
-ifdef(AR_TEST).
-define(STORE_CHUNK_QUEUE_FLUSH_SIZE_THRESHOLD, 2).
-else.
-define(STORE_CHUNK_QUEUE_FLUSH_SIZE_THRESHOLD, 100). % ~ 25 MB worth of chunks.
-endif.

%% If a chunk spends longer than this in the store queue, write it on disk without waiting
%% for ?STORE_CHUNK_QUEUE_FLUSH_SIZE_THRESHOLD chunks to stack up.
-ifdef(AR_TEST).
-define(STORE_CHUNK_QUEUE_FLUSH_TIME_THRESHOLD, 1000).
-else.
-define(STORE_CHUNK_QUEUE_FLUSH_TIME_THRESHOLD, 2_000). % 2 seconds.
-endif.

-define(WORKER_LOAD_TABLE, worker_load).

%% ar_peer_sync's state is opaque to ar_data_sync. It is constructed
%% via `ar_peer_sync:new/3' at module init and threaded through
%% `enqueue/1', `sync/1', `task_completed/3', and
%% `set_weave_size/2'. Treat it as an unstructured term outside
%% ar_peer_sync.

%% @doc The state of the server managing data synchronization.
-record(data_sync_state, {
	%% The last entries of the block index.
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
	%% A reference to the on-disk key value storage mapping
	%% << DataRootTimestamp:256, ChunkDataIndexKey/binary >> =>
	%%     {RelativeChunkEndOffset, ChunkSize, DataRoot, TXSize, ChunkDataKey, IsStrictSplit}.
	%%
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
	%% A flag indicating the process has started collecting the intervals for syncing.
	%% We consult the other storage modules first, then search among the network peers.
	sync_status = undefined,
	%% The offsets of the chunks currently scheduled for (re-)packing (keys) and
	%% some chunk metadata needed for storing the chunk once it is packed.
	packing_map = #{},
	%% Opaque ar_peer_sync state (task queue + in-progress enqueue pass
	%% + range bookkeeping). Built via ar_peer_sync:new/3 at init and
	%% threaded through ar_peer_sync's API. Do not inspect outside
	%% ar_peer_sync.
	peer_sync = undefined,
	%% The mining address the chunks are packed with in 2.6.
	mining_address,
	%% The identifier of the storage module the process is responsible for.
	store_id,
	%% The start offset of the range the module is responsible for.
	range_start = -1,
	%% The end offset of the range the module is responsible for.
	range_end = -1,
	%% The priority queue of chunks sorted by offset. The motivation is to have chunks
	%% stack up, per storage module, before writing them on disk so that we can write
	%% them in the ascending order and reduce out-of-order disk writes causing fragmentation.
	store_chunk_queue = gb_sets:new(),
	%% The length of the store chunk queue.
	store_chunk_queue_len = 0,
	%% The threshold controlling the brief accumuluation of the chunks in the queue before
	%% the actual disk dump, to reduce the chance of out-of-order write causing disk
	%% fragmentation.
	store_chunk_queue_threshold = ?STORE_CHUNK_QUEUE_FLUSH_SIZE_THRESHOLD
}).
