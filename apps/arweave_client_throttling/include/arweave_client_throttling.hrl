-ifndef(ARWEAVE_CLIENT_THROTTLING_HRL).
-define(ARWEAVE_CLIENT_THROTTLING_HRL, true).

%% Default budget assumed for a peer the first time it is seen, before any
%% remote response refreshes the value. The same value is used as the
%% initial `total' quota.
-define(ARWEAVE_CLIENT_THROTTLING_DEFAULT_INITIAL_REMAINING, 10).

%% Hard cap on the number of waiting callers we are willing to queue per
%% peer. Calls received over the cap are rejected immediately with
%% {error, queue_full}.
-define(ARWEAVE_CLIENT_THROTTLING_DEFAULT_MAX_QUEUE_LENGTH, 1000).

%% Window during which two `update_quota' messages are considered to
%% describe the same logical batch of concurrent in-flight requests. Inside
%% the window we take the minimum of the reported `remaining' values
%% (because the smallest one is the most recent server-side view). Outside
%% the window we trust the new value.
-define(ARWEAVE_CLIENT_THROTTLING_DEFAULT_CONCURRENCY_WINDOW_MS, 1000).

%% Fraction of the configured `initial_remaining' that the new
%% `remaining' reported by a remote response must differ by, relative
%% to the value already stored for the peer, before we bother to
%% update the peer state. Updates reporting `remaining = 0' are always
%% applied regardless of this ratio.
-define(SIGNIFICANTLY_DIFFERENT_RATIO, 0.1).

-endif.
