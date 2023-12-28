-module(ar_pool).

-behaviour(gen_server).

-export([start_link/0, is_client/0, get_current_sessions/0, get_jobs/1,
		get_latest_output/0, cache_jobs/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_pool.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	%% The most recent keys come first.
	session_keys = [],
	%% Key => [{Output, StepNumber, PartitionUpperBound, Seed, Diff}, ...]
	jobs_by_session_key = maps:new()
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Return true if we are a pool client.
is_client() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.pool_client == true.

%% @doc Return a list of up to two most recently cached VDF session keys.
get_current_sessions() ->
	gen_server:call(?MODULE, get_current_sessions, infinity).

%% @doc Return a set of the most recent cached jobs.
get_jobs(PrevOutput) ->
	gen_server:call(?MODULE, {get_jobs, PrevOutput}, infinity).

%% @doc Return the most recent cached VDF output. Return an empty binary if the
%% cache is empty.
get_latest_output() ->
	gen_server:call(?MODULE, get_latest_output, infinity).

%% @doc Cache the given jobs.
cache_jobs(Jobs) ->
	gen_server:cast(?MODULE, {cache_jobs, Jobs}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, #state{}}.

handle_call(get_current_sessions, _From, State) ->
	{reply, lists:sublist(State#state.session_keys, 2), State};

handle_call({get_jobs, PrevOutput}, _From, State) ->
	SessionKeys = State#state.session_keys,
	JobCache = State#state.jobs_by_session_key,
	{reply, get_jobs(PrevOutput, SessionKeys, JobCache), State};

handle_call(get_latest_output, _From, State) ->
	case State#state.session_keys of
		[] ->
			{reply, <<>>, State};
		[Key | _] ->
			{O, _SN, _U, _S, _Diff} = hd(maps:get(Key, State#state.jobs_by_session_key)),
			{reply, O, State}
	end;

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({cache_jobs, #jobs{ vdf = [] }}, State) ->
	{noreply, State};
handle_cast({cache_jobs, Jobs}, State) ->
	#jobs{ vdf = VDF, diff = Diff, next_seed = NextSeed, seed = Seed,
			interval_number = IntervalNumber,
			next_vdf_difficulty = NextVDFDifficulty } = Jobs,
	SessionKey = {NextSeed, IntervalNumber, NextVDFDifficulty},
	SessionKeys = State#state.session_keys,
	SessionKeys2 = [SessionKey | SessionKeys],
	JobList = [{Job#job.output, Job#job.global_step_number,
			Job#job.partition_upper_bound, Seed, Diff} || Job <- VDF],
	PrevJobList = maps:get(SessionKey, State#state.jobs_by_session_key, []),
	JobList2 = JobList ++ PrevJobList,
	JobsBySessionKey = maps:put(SessionKey, JobList2, State#state.jobs_by_session_key),
	{SessionKeys3, JobsBySessionKey2} =
		case length(SessionKeys2) == 3 of
			true ->
				{RemoveKey, SessionKeys4} = lists:last(SessionKeys2),
				{SessionKeys4, maps:remove(RemoveKey, JobsBySessionKey)};
			false ->
				{SessionKeys2, JobsBySessionKey}
		end,
	{noreply, State#state{ session_keys = SessionKeys3,
			jobs_by_session_key = JobsBySessionKey2 }};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_jobs(PrevOutput, SessionKeys, JobCache) ->
	case SessionKeys of
		[] ->
			#jobs{};
		[{NextSeed, Interval, NextVDFDifficulty} = SessionKey | _] ->
			Jobs = maps:get(SessionKey, JobCache),
			{Seed, Diff, Jobs2} = collect_jobs(Jobs, PrevOutput, ?GET_JOBS_COUNT),
			Jobs3 = [#job{ output = O, global_step_number = SN,
					partition_upper_bound = U } || {O, SN, U} <- Jobs2],
			#jobs{ vdf = Jobs3, seed = Seed, diff = Diff, next_seed = NextSeed,
					interval_number = Interval, next_vdf_difficulty = NextVDFDifficulty }
	end.

collect_jobs([], _PrevO, _N) ->
	{<<>>, 0, []};
collect_jobs(_Jobs, _PrevO, 0) ->
	{<<>>, 0, []};
collect_jobs([{O, _SN, _U, _S, _Diff} | _Jobs], O, _N) ->
	{<<>>, 0, []};
collect_jobs([{O, SN, U, S, Diff} | Jobs], PrevO, N) ->
	{S, Diff, [{O, SN, U} | collect_jobs(Jobs, PrevO, N - 1, Diff)]}.

collect_jobs([], _PrevO, _N, _Diff) ->
	[];
collect_jobs(_Jobs, _PrevO, 0, _Diff) ->
	[];
collect_jobs([{O, _SN, _U, _S, _Diff} | _Jobs], O, _N, _Diff2) ->
	[];
collect_jobs([{O, SN, U, _S, Diff} | Jobs], PrevO, N, Diff) ->
	[{O, SN, U} | collect_jobs(Jobs, PrevO, N - 1, Diff)];
collect_jobs(_Jobs, _PrevO, _N, _Diff) ->
	%% Diff mismatch.
	[].

get_jobs_test() ->
	?assertEqual(#jobs{}, get_jobs(<<>>, [], maps:new())),

	?assertEqual(#jobs{ next_seed = ns, interval_number = in, next_vdf_difficulty = nvd },
			get_jobs(o, [{ns, in, nvd}],
			#{ {ns, in, nvd} => [{o, gsn, u, s, d}] })),

	?assertEqual(#jobs{ vdf = [#job{ output = o, global_step_number = gsn,
							partition_upper_bound = u }],
						diff = d,
						seed = s,
						next_seed = ns,
						interval_number = in,
						next_vdf_difficulty = nvd },
			get_jobs(a, [{ns, in, nvd}],
						#{ {ns, in, nvd} => [{o, gsn, u, s, d}] })),

	%% d2 /= d (the difficulties are different) => only take the latest job.
	?assertEqual(#jobs{ vdf = [#job{ output = o, global_step_number = gsn,
							partition_upper_bound = u }],
						diff = d,
						seed = s,
						next_seed = ns,
						interval_number = in,
						next_vdf_difficulty = nvd },
			get_jobs(a, [{ns, in, nvd}, {ns2, in2, nvd2}],
						#{ {ns, in, nvd} => [{o, gsn, u, s, d}, {o2, gsn2, u2, s, d2}],
							%% Same difficulty, but a different VDF session => not picked.
							{ns2, in2, nvd2} => [{o3, gsn3, u3, s3, d}] })),

	%% d2 == d => take both.
	?assertEqual(#jobs{ vdf = [#job{ output = o, global_step_number = gsn,
							partition_upper_bound = u }, #job{ output = o2,
									global_step_number = gsn2, partition_upper_bound = u2 }],
						diff = d,
						seed = s,
						next_seed = ns,
						interval_number = in,
						next_vdf_difficulty = nvd },
			get_jobs(a, [{ns, in, nvd}, {ns2, in2, nvd2}],
						#{ {ns, in, nvd} => [{o, gsn, u, s, d}, {o2, gsn2, u2, s, d}],
							{ns2, in2, nvd2} => [{o2, gsn2, u2, s2, d2}] })),

	%% Take strictly above the previous output.
	?assertEqual(#jobs{ vdf = [#job{ output = o, global_step_number = gsn,
								partition_upper_bound = u }],
						diff = d,
						seed = s,
						next_seed = ns,
						interval_number = in,
						next_vdf_difficulty = nvd },
			get_jobs(o2, [{ns, in, nvd}, {ns2, in2, nvd2}],
						#{ {ns, in, nvd} => [{o, gsn, u, s, d}, {o2, gsn2, u2, s, d}],
							{ns2, in2, nvd2} => [{o2, gsn2, u2, s2, d2}] })).
