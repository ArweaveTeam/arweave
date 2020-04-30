-ifdef(DEBUG).
-define(TRACK_CONFIRMATIONS, 5).
-else.
-define(TRACK_CONFIRMATIONS, ?STORE_BLOCKS_BEHIND_CURRENT).
-endif.

-define(SYNC_CHUNK_SIZE, 20 * 1024 * 1024).

%% 2 * ?MAX_PATH_SIZE
%% + DATA_CHUNK_SIZE
%% + log10(2 ^ (NOTE_SIZE * 8))
%% + some space for JSON special chars
%% multiplied by ~1.4 to account for Base64.
-define(MAX_SERIALIZED_CHUNK_PROOF_SIZE, 1100000).

-record(state, {
	%% An end offset -> start offset mapping sorted
	%% by end offset. Every such pair denotes a synced
	%% half-closed interval on the (0, confirmed weave size]
	%% half-closed interval. The confirmed weave size is
	%% from the block with ?TRACK_CONFIRMATIONS confirmations.
	%%
	%% This mapping serves as a compact map of what is synced
	%% by the node. No matter how big the weave is or how much
	%% of it the node stores, this record can remain very small,
	%% compared to storing all chunk and transaction identifiers,
	%% whose number can effectively grow unlimited with time.
	%% We do not keep unconfirmed data in this mapping as
	%% chunks from different forks can have the same offsets,
	%% what is easier to manage via chunk and transaction
	%% identifiers.
	%%
	%% Every time a chunk is written to sync_record, it is also
	%% written to the chunk_index.
	%%
	%% The record is consulted by GET /chunk/<offset>,
	%% GET /tx/<id>/data, and POST /chunk.
	sync_record,
	%% The weave size from the block with ?TRACK_CONFIRMATIONS
	%% confirmations. Must be bigger than or equal the biggest
	%% key in sync_record.
	confirmed_size,
	%% See comments on the sync_tip record fields.
	sync_tip,
	%% A reference to the on-disk key-value storage of synced chunks.
	%% Chunks are identified by offset. A proof is stored
	%% along with every chunk. Only confirmed chunks are stored.
	chunks_index,
	%% A reference to the on-disk key-value storage mapping
	%% block start offsets (keys) to their transaction roots (values).
	%% Used when syncing a random chunk of the weave. A transaction root
	%% is looked up and used in proof verification.
	tx_root_index,
	%% A reference to the on-disk key-value storage mapping
	%% transaction roots (keys) to their block start offsets (values).
	%% Used when accepting a chunk. Users do not need to look up
	%% block offsets to submit data so when a node accepts a chunk,
	%% it needs to lookup the block start offset for the given transaction root.
	block_offset_index,
	%% A reference to the on-disk key value storage of the offsets
	%% of synced transaction identifiers. Only confirmed transactions
	%% are stored. Consulted by GET /tx/<id>/data. The index is not
	%% maintained by default as miners do not need it.
	tx_index
}).

%% The record keeps track of all the unconfirmed synced
%% chunks, transactions, and transaction roots. The orphaned
%% data is not erased immediately, but after ?TRACK_CONFIRMATIONS.
%% The record is consulted by POST /chunk, GET /tx/<id>/data,
%% and when transaction data is written in ar_storage:write_tx_data/1,
%% but not by GET /chunk/<offset>.
-record(sync_tip, {
	%% Every time a new block gets ?TRACK_CONFIRMATIONS confirmations,
	%% this map is used to erase uncle data. To collect synced chunk ids
	%% to erase, we consult chunk_ids_by_tx_root and
	%% tx_roots_with_proofs_by_chunk_id (note that the same chunk can
	%% belong to mulitple different transactions).
	tx_roots_by_height,
	chunk_ids_by_tx_root,
	tx_roots_with_proofs_by_chunk_id,
	%% The confirmed height is advanced each time unconfirmed_tx_roots
	%% grows longer than ?TRACK_CONFIRMATIONS - 1.
	%% Each time the confirmed height is advanced, the synced data
	%% from the orphaned transaction roots is erased, the synced
	%% data from the new confirmed transaction root is recorded in sync_record.
	confirmed_height,
	%% Must be updated every time the confirmed height is updated.
	confirmed_tx_root,
	%% The list of unconfirmed transaction roots ordered from most recent
	%% to least recent, no longer than ?TRACK_CONFIRMATIONS - 1.
	unconfirmed_tx_roots,
	%% Keeps track of the transaction identifiers of the unconfirmed synced
	%% chunks. Consulted by GET /tx/<id>/data. The mapping is not maintained
	%% by default as miners do not need it.
	chunk_ids_by_tx_id,
	%% Each time the confirmed height is advanced, the transaction identifiers
	%% from the orphaned blocks are removed from chunk_ids_by_tx_id, if the
	%% mapping is maintained.
	size_tagged_tx_ids_by_tx_root,
	block_offset_by_tx_root
}).
