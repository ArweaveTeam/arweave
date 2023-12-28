-module(ar_pool).

-behaviour(gen_server).

-export([start_link/0, is_client/0, get_current_sessions/0, get_jobs/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_pool.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	%% The most recent keys come first.
	session_keys = gb_sets:new(),
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

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, #state{}}.

handle_call(get_current_sessions, _From, State) ->
	{reply, lists:sublist(gb_sets:to_list(State#state.session_keys), 2), State};

handle_call({get_jobs, PrevOutput}, _From, State) ->
	SessionKeys = State#state.session_keys,
	JobCache = State#state.jobs_by_session_key,
	{reply, get_jobs(PrevOutput, SessionKeys, JobCache), State};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

% TODO periodic process that fetches jobs (from pool host if we are an exit or standalone node
% or exit node otherwise), triggers events, and, if we are an exit node, caches them

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_jobs(PrevOutput, SessionKeys, JobCache) ->
	case gb_sets:is_empty(SessionKeys) of
		true ->
			#jobs{};
		false ->
			{NextSeed, Interval, NextVDFDifficulty} = SessionKey
				= gb_sets:smallest(SessionKeys),
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
	?assertEqual(#jobs{}, get_jobs(<<>>, gb_sets:new(), maps:new())),

	?assertEqual(#jobs{ next_seed = ns, interval_number = in, next_vdf_difficulty = nvd },
			get_jobs(o, gb_sets:from_list([{ns, in, nvd}]),
			#{ {ns, in, nvd} => [{o, gsn, u, s, d}] })),

	?assertEqual(#jobs{ vdf = [#job{ output = o, global_step_number = gsn,
							partition_upper_bound = u }],
						diff = d,
						seed = s,
						next_seed = ns,
						interval_number = in,
						next_vdf_difficulty = nvd },
			get_jobs(a, gb_sets:from_list([{ns, in, nvd}]),
						#{ {ns, in, nvd} => [{o, gsn, u, s, d}] })),

	%% d2 /= d (the difficulties are different) => only take the latest job.
	?assertEqual(#jobs{ vdf = [#job{ output = o, global_step_number = gsn,
							partition_upper_bound = u }],
						diff = d,
						seed = s,
						next_seed = ns,
						interval_number = in,
						next_vdf_difficulty = nvd },
			get_jobs(a, gb_sets:from_list([{ns, in, nvd}, {ns2, in2, nvd2}]),
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
			get_jobs(a, gb_sets:from_list([{ns, in, nvd}, {ns2, in2, nvd2}]),
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
			get_jobs(o2, gb_sets:from_list([{ns, in, nvd}, {ns2, in2, nvd2}]),
						#{ {ns, in, nvd} => [{o, gsn, u, s, d}, {o2, gsn2, u2, s, d}],
							{ns2, in2, nvd2} => [{o2, gsn2, u2, s2, d2}] })).
