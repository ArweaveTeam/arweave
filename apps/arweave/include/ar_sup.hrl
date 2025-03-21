%% The number of milliseconds the supervisor gives every process for shutdown.
-ifdef(AR_TEST).
-define(SHUTDOWN_TIMEOUT, 30_000).
-else.
-define(SHUTDOWN_TIMEOUT, 300_000).
-endif.

-define(CHILD(I, Type), #{
	id => I,
	start => {I, start_link, []},
	restart => permanent,
	shutdown => ?SHUTDOWN_TIMEOUT,
	type => Type,
	modules => [I]
}).

-define(CHILD_WITH_ARGS(I, Type, Name, Args), #{
	id => Name,
	start => {I, start_link, Args},
	restart => permanent,
	shutdown => ?SHUTDOWN_TIMEOUT,
	type => Type,
	modules => [Name]
}).

%% From the Erlang docs:
%%
%% An integer time-out value means that the supervisor tells the child process to terminate
%% by calling exit(Child,shutdown) and then wait for an exit signal with reason shutdown back
%% from the child process. If no exit signal is received within the specified number of
%% milliseconds, the child process is unconditionally terminated using exit(Child,kill).
%% If the child process is another supervisor, the shutdown time must be set to infinity to
%% give the subtree ample time to shut down.
-define(CHILD_SUP(I, Type), #{
	id => I,
	start => {I, start_link, []},
	restart => permanent,
	shutdown => infinity,
	type => Type,
	modules => [I]
}).
