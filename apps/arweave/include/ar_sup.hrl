%% The number of milliseconds the supervisor gives every process for shutdown.
-ifdef(DEBUG).
-define(SHUTDOWN_TIMEOUT, 10000).
-else.
-define(SHUTDOWN_TIMEOUT, 30000).
-endif.
