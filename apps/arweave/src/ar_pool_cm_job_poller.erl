-module(ar_pool_cm_job_poller).

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
	case {ar_pool:is_client(), ar_coordination:is_exit_peer()} of
		{true, true} ->
			gen_server:cast(self(), fetch_cm_jobs);
		_ ->
			%% If we are a CM miner and not an exit peer, our exit peer will push
			%% the pool CM jobs to us.
			ok
	end,
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(fetch_cm_jobs, State) ->
	Peer = ar_pool:pool_peer(),
	case ar_http_iface_client:get_pool_cm_jobs(Peer) of
		{ok, Jobs} ->
			push_cm_jobs_to_cm_peers(Jobs),
			ar_pool:process_cm_jobs(Jobs, Peer),
			ar_util:cast_after(?FETCH_CM_JOBS_FREQUENCY_MS, self(), fetch_cm_jobs);
		{error, Error} ->
			?LOG_WARNING([{event, failed_to_fetch_pool_cm_jobs},
					{error, io_lib:format("~p", [Error])}]),
			ar_util:cast_after(?FETCH_CM_JOBS_RETRY_MS, self(), fetch_cm_jobs)
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

push_cm_jobs_to_cm_peers(Jobs) ->
	{ok, Config} = application:get_env(arweave, config),
	Peers = Config#config.cm_peers,
	Payload = ar_serialize:jsonify(ar_serialize:pool_cm_jobs_to_json_struct(Jobs)),
	push_cm_jobs_to_cm_peers(Payload, Peers).

push_cm_jobs_to_cm_peers(_Payload, []) ->
	ok;
push_cm_jobs_to_cm_peers(Payload, [Peer | Peers]) ->
	spawn(fun() -> ar_http_iface_client:post_pool_cm_jobs(Peer, Payload) end),
	push_cm_jobs_to_cm_peers(Payload, Peers).
