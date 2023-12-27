%% The number of VDF steps ("jobs") the pool server serves at a time.
-define(GET_JOBS_COUNT, 10).

%% The time in seconds the pool server waits before giving up on replying with
%% new jobs when the client already has the newest job.
-define(GET_JOBS_TIMEOUT_S, 2).
