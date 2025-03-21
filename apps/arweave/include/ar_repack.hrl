-ifndef(AR_REPACK_HRL).
-define(AR_REPACK_HRL, true).

-record(chunk_metadata, {
	chunk_data_key = not_set,
	tx_root = not_set,
	data_root = not_set,
	tx_path = not_set,
	chunk_size = not_set
}).

-record(chunk_offsets, {
	absolute_offset = not_set,
	bucket_end_offset = not_set,
	padded_end_offset = not_set,
	relative_offset = not_set
}).

-record(chunk_info, {
	state = needs_chunk :: 
		needs_chunk | invalid | entropy_only | already_repacked | needs_data_path |
		needs_repack | needs_entropy | needs_encipher | needs_write | error,
	metadata = not_set :: not_set | not_found | #chunk_metadata{},
	offsets = not_set :: not_set | not_found | #chunk_offsets{},
	data_path = not_set :: not_set | not_found | binary(),
	source_packing = not_set :: not_set | not_found | ar_packing:packing(),
	target_packing = not_set :: not_set | not_found | ar_packing:packing(),
	chunk = not_set :: not_set | not_found | binary(),
	entropy = not_set :: not_set | binary()
}).

-type chunk_info() :: #chunk_info{}.

-endif.
