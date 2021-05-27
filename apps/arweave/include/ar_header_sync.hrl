%% The number of recent blocks tracked, used for erasing the orphans.
-define(HEADER_SYNC_TRACK_CONFIRMATIONS, 100).

%% The size in bytes of a portion of the disk space reserved for recent block
%% and transaction headers. The node stops syncing historical headers when the remaining
%% disk space is smaller than this amount.
-ifdef(DEBUG).
-define(DISK_HEADERS_BUFFER_SIZE, 20 * 1024 * 1024).
-else.
-define(DISK_HEADERS_BUFFER_SIZE, 10 * 1024 * 1024 * 1024).
-endif.

%% The threshold in bytes for the remaining disk space. When reached, the node
%% removes the oldest of the stored block and transaction headers until the space
%% exceeding this threshold is available.
-ifdef(DEBUG).
-define(DISK_HEADERS_CLEANUP_THRESHOLD, 10 * 1024 * 1024).
-else.
-define(DISK_HEADERS_CLEANUP_THRESHOLD, 2 * 1024 * 1024 * 1024).
-endif.

%% The frequency of processing items in the queue.
-ifdef(DEBUG).
-define(PROCESS_ITEM_INTERVAL_MS, 1000).
-else.
-define(PROCESS_ITEM_INTERVAL_MS, 100).
-endif.

%% The frequency of checking if there are headers to sync after everything
%% is synced. Also applies to a fresh node without any data waiting for a block index.
%% Another case is when the process misses a few blocks (e.g. blocks were sent while the
%% supervisor was restarting it after a crash).
-ifdef(DEBUG).
-define(CHECK_AFTER_SYNCED_INTERVAL_MS, 500).
-else.
-define(CHECK_AFTER_SYNCED_INTERVAL_MS, 5000).
-endif.

%% The initial value for the exponential backoff for failing requests.
-define(INITIAL_BACKOFF_INTERVAL_S, 30).

%% The maximum exponential backoff interval for failing requests.
-define(MAX_BACKOFF_INTERVAL_S, 2 * 60 * 60).

%% The frequency of storing the server state on disk.
-define(STORE_HEADER_STATE_FREQUENCY_MS, 30000).
