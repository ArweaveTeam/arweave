-ifndef(AR_DISK_POOL_HRL).
-define(AR_DISK_POOL_HRL, true).

-record(disk_pool_state, {
	%% One of the keys from disk_pool_chunks_index or the atom "first".
	%% The disk pool is processed chunk by chunk going from the oldest entry to the newest,
	%% trying not to block the syncing process if the disk pool accumulates a lot of orphaned
	%% and pending chunks. The cursor remembers the key after the last processed on the
	%% previous iteration. After reaching the last key in the storage, we go back to
	%% the first one. Not stored.
	cursor = first,
	%% A key marking the beginning of a full disk pool scan.
	full_scan_start_key = none,
	%% The timestamp of the beginning of a full disk pool scan. Used to measure
	%% the time it takes to scan the current disk pool - if it is too short, we postpone
	%% the next scan to save some disk IO.
	full_scan_start_timestamp,
	%% A cache of the offsets of the recently "matured" chunks. We use it to quickly
	%% skip matured chunks when scanning the disk pool. The reason the chunk is still
	%% in the disk pool is some of its offsets have not matured yet (the same data can be
	%% submitted several times).
	recently_processed_offsets = #{},
	%% A registry of the currently processed disk pool chunks consulted by different
	%% disk pool jobs to avoid double-processing.
	currently_processed_keys = sets:new()
}).

-endif.
