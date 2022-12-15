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
-ifdef(DEBUG).
-define(INITIAL_BACKOFF_INTERVAL_S, 1).
-else.
-define(INITIAL_BACKOFF_INTERVAL_S, 30).
-endif.

%% The maximum exponential backoff interval for failing requests.
-ifdef(DEBUG).
-define(MAX_BACKOFF_INTERVAL_S, 2).
-else.
-define(MAX_BACKOFF_INTERVAL_S, 2 * 60 * 60).
-endif.

%% The frequency of storing the server state on disk.
-define(STORE_HEADER_STATE_FREQUENCY_MS, 30000).
