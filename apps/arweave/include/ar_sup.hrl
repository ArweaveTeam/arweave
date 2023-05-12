%% The number of milliseconds the supervisor gives every process for shutdown.
-ifdef(DEBUG).
-define(SHUTDOWN_TIMEOUT, 10000).
-else.
-define(SHUTDOWN_TIMEOUT, 30000).
-endif.

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, ?SHUTDOWN_TIMEOUT, Type, [I]}).

%% From the Erlang docs:
%%
%% An integer time-out value means that the supervisor tells the child process to terminate
%% by calling exit(Child,shutdown) and then wait for an exit signal with reason shutdown back
%% from the child process. If no exit signal is received within the specified number of
%% milliseconds, the child process is unconditionally terminated using exit(Child,kill).
%% If the child process is another supervisor, the shutdown time must be set to infinity to
%% give the subtree ample time to shut down.
-define(CHILD_SUP(I, Type), {I, {I, start_link, []}, permanent, infinity, Type, [I]}).
