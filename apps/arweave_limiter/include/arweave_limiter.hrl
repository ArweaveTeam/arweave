-ifndef(ARWEAVE_LIMITER_HRL).
-define(ARWEAVE_LIMITER_HRL, true).

%% Number of worker gen_servers per limiter group. Peers are sharded
%% across them by `arweave_limiter_util:worker_ref/3`. The per-group
%% default lives in `arweave_config_options_limiter' (`workers' field);
%% this constant exists so eunit tests can assert against the same
%% default the sup uses at boot.
-define(DEFAULT_ARWEAVE_LIMITER_GROUP_WORKERS, 5).

%% gen_server:call timeout for `register_or_reject_call/2'. Picked to
%% match the maximum acceptable rate-limit decision latency before the
%% caller times out and the request is treated as rejected.
-define(DEFAULT_ARWEAVE_LIMITER_CALL_TIMEOUT, 1000).

-endif.
