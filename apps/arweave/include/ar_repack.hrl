-ifndef(AR_REPACK_HRL).
-define(AR_REPACK_HRL, true).

-record(repack_chunk, {
	state = needs_chunk :: 
		needs_chunk | invalid | entropy_only | already_repacked | needs_data_path |
		needs_repack | needs_entropy | needs_encipher | needs_write | error,
	metadata = not_set :: not_set | not_found | #chunk_metadata{},
	offsets = not_set :: not_set | not_found | #chunk_offsets{},
	%% source_packing is used to track the current packing format of the chunk. It starts
	%% set to the original format of the chunk, but will be updated as the chunk is
	%% repacked (sometimes through intermediate formats) and ultimately will be set equal
	%% to target_packing.
	source_packing = not_set :: not_set | not_found | ar_packing:packing(),
	target_packing = not_set :: not_set | not_found | ar_packing:packing(),
	chunk = not_set :: not_set | not_found | binary(),
	source_entropy = not_set :: not_set | binary(),
	target_entropy = not_set :: not_set | binary()
}).

-endif.
