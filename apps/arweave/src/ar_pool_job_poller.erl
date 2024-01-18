-module(ar_pool_job_poller).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_pool.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	case ar_pool:is_client() of
		true ->
			gen_server:cast(self(), fetch_jobs);
		false ->
			ok
	end,
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(fetch_jobs, State) ->
	PrevOutput = (ar_pool:get_latest_job())#job.output,
	{ok, Config} = application:get_env(arweave, config),
	{Peer, GetJobsFromExitNode} =
		case {Config#config.coordinated_mining, Config#config.cm_exit_peer} of
			{true, not_set} ->
				%% We are a CM exit node.
				{Config#config.pool_server_address, false};
			{true, ExitPeer} ->
				%% We are a CM miner.
				{ExitPeer, true};
			_ ->
				%% We are a standalone pool client (a non-CM miner and a pool client).
				{Config#config.pool_server_address, false}
		end,
	case ar_http_iface_client:get_jobs(Peer, PrevOutput, GetJobsFromExitNode) of
		{ok, Jobs} ->
			emit_pool_jobs(Jobs),
			ar_pool:cache_jobs(Jobs),
			ar_util:cast_after(?FETCH_JOBS_FREQUENCY_MS, self(), fetch_jobs);
		{error, Error} ->
			?LOG_WARNING([{event, failed_to_fetch_pool_jobs},
					{error, io_lib:format("~p", [Error])}]),
			ar_util:cast_after(?FETCH_JOBS_RETRY_MS, self(), fetch_jobs)
	end,
	{noreply, State};

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

emit_pool_jobs(Jobs) ->
	SessionKey = {Jobs#jobs.next_seed, Jobs#jobs.interval_number,
			Jobs#jobs.next_vdf_difficulty},
	emit_pool_jobs(Jobs#jobs.jobs, SessionKey, Jobs#jobs.partial_diff).

emit_pool_jobs([], _SessionKey, _PartialDiff) ->
	ok;
emit_pool_jobs([Job | Jobs], SessionKey, PartialDiff) ->
	#job{ output = O, global_step_number = SN, partition_upper_bound = U } = Job,
	Args = {SessionKey, SN, O, U, PartialDiff},
	ar_mining_server:add_pool_job(Args),
	emit_pool_jobs(Jobs, SessionKey, PartialDiff).
