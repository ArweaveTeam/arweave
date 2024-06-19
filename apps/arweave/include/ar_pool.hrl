%% The number of VDF steps ("jobs") the pool server serves at a time.
-define(GET_JOBS_COUNT, 10).

%% The time in seconds the pool server waits before giving up on replying with
%% new jobs when the client already has the newest job.
-define(GET_JOBS_TIMEOUT_S, 2).

%% The frequency in milliseconds of asking the pool or CM exit node about new jobs.
-define(FETCH_JOBS_FREQUENCY_MS, 5000).

%% The time in milliseconds we wait before retrying a failed fetch jobs request.
-define(FETCH_JOBS_RETRY_MS, 2000).

%% The frequency in milliseconds of asking the pool or CM exit node about new CM jobs.
-define(FETCH_CM_JOBS_FREQUENCY_MS, 1000).

%% The time in milliseconds we wait before retrying a failed fetch CM jobs request.
-define(FETCH_CM_JOBS_RETRY_MS, 2000).

%% @doc A collection of mining jobs.
-record(jobs, {
	jobs = [], %% The information about a single VDF output (a "job").
	partial_diff = {0, 0}, %% Partial difficulty.
	seed = <<>>,
	next_seed = <<>>,
	interval_number = 0,
	next_vdf_difficulty = 0
}).

%% @doc A mining job.
-record(job, {
	output = <<>>,
	global_step_number = 0,
	partition_upper_bound = 0
}).

%% @doc Partial solution validation response.
-record(partial_solution_response, {
	indep_hash = <<>>,
	status = <<>>
}).

%% @doc A set of coordinated mining jobs provided by the pool.
%%
%% Miners fetch and submit pool CM jobs via the same POST /pool_cm_jobs endpoint.
%% When miners fetch jobs, they specify the partitions and leave the job fields empty.
%% When miners submit jobs, they leave the partitions field empty.
-record(pool_cm_jobs, {
	h1_to_h2_jobs = [], % [#mining_candidate{}]
	h1_read_jobs = [], % [#mining_candidate{}]
	%% A list of {[{bucket, ...}, {bucketsize, ...}, {addr, ...}]} or
	%% {[{bucket, ...}, {bucketsize, ...}, {addr, ...}, {pdiff, ...}]} JSON structs.
	partitions = []
}).
