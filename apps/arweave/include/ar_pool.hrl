%% The number of VDF steps ("jobs") the pool server serves at a time.
-define(GET_JOBS_COUNT, 10).

%% The time in seconds the pool server waits before giving up on replying with
%% new jobs when the client already has the newest job.
-define(GET_JOBS_TIMEOUT_S, 2).

%% The frequency in milliseconds of asking the pool proxy or CM exit node about new jobs.
-define(FETCH_JOBS_FREQUENCY_MS, 5000).

%% The time in milliseconds we wait before retrying a failed fetch jobs request.
-define(FETCH_JOBS_RETRY_MS, 2000).

%% @doc A collection of mining jobs.
-record(jobs, {
	vdf = [], % The information about a single VDF output (a "job").
	diff = 0, % Partial difficulty.
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
