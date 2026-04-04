-ifndef(AR_DISK_POOL_HRL).
-define(AR_DISK_POOL_HRL, true).

-record(disk_pool_state, {
	cursor = first,
	full_scan_start_key = none,
	full_scan_start_timestamp,
	recently_processed_offsets = #{},
	currently_processed_keys = sets:new(),
	scan_pause = false
}).

-endif.
