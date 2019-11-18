-module(app_net_crawler).
-export([crawl/0, crawl/1, reload_jobs/0]).

-define(JOBS_QUEUE_NAME, fetch_peers_queue).
-define(PARALLEL_FETCHES, 100).

%%% A parallel network crawler that will find all nodes in the network.
%%% The number of concurrent fetch requests is limited by the 'jobs' library
%%% (https://github.com/uwiger/jobs), which is NOT defined as a dependency. Therefor
%%% it's recommended to add 'jobs' to $ERL_LIBS to be able to run this.

%% @doc Crawls the network and returns a map of peers by node.
crawl() ->
	StartPeers = app_net_explore:filter_local_peers(
		ar_bridge:get_remote_peers(whereis(http_bridge_node))
	),
	crawl(StartPeers).

crawl(StartPeers) ->
	case reload_jobs() of
		jobs_missing ->
			ar:err("Application jobs could not be started. Make sure it's in your local ERL_LIBS directory."),
			{error, jobs_missing};
		ok ->
			InitState = start_fetching(#{}, StartPeers),
			io:format("Starting to crawl~n"),
			{ok, crawl_loop(InitState)}
	end.

%% We don't have a config file, so we hack in the jobs config here.
reload_jobs() ->
	application:stop(jobs),
	application:set_env(jobs, queues, jobs_queues_config()),
	case application:ensure_started(jobs) of
		ok -> ok;
		{error, _} -> jobs_missing
	end.

jobs_queues_config() ->
	[{?JOBS_QUEUE_NAME, [{regulators, [{counter, [{limit, ?PARALLEL_FETCHES}]}]}]}].

%% @doc Main loop for the crawler.
crawl_loop(State) ->
	case count_ongoing(State) of
		0 ->
			State;
		_ ->
			crawl_loop(wait_for_more(State))
	end.

%% @doc Returns the number of ongoing fetches.
count_ongoing(State) ->
	IsOngoing = fun
		(fetching) -> true;
		(_) -> false
	end,
	length(lists:filter(IsOngoing, maps:values(State))).

%% @doc Wait for one fetch worker to finish and start new fetch workers for the
%% new nodes found.
wait_for_more(State) ->
	receive
		{found_peers, Node, unavailable} ->
			maps:update(Node, unavailable, State);
		{found_peers, Node, Peers} ->
			UnseenNodes = lists:usort(Peers -- maps:keys(State)),
			NewState = start_fetching(State, UnseenNodes),
			maps:update(Node, Peers, NewState)
	end.

%% @doc Start fetches for all the supplied nodes.
start_fetching(State, Nodes) ->
	NonLocalNodes = app_net_explore:filter_local_peers(Nodes),
	lists:foreach(fun(Node) -> start_worker(Node) end, NonLocalNodes),
	StateOverlay = [{Node, fetching} || Node <- NonLocalNodes],
	maps:merge(State, maps:from_list(StateOverlay)).

%% @doc Start a fetch worker for one node.
start_worker(Node) ->
	Master = self(),
	spawn_link(
		fun() ->
			Master ! {found_peers, Node, fetch_peers(Node)}
		end
	).

%% @doc Try to fetch peers for a node. Will timeout and try up to 4 times with
%% a 20 seconds delay in between.
fetch_peers(Node) ->
	fetch_peers(Node, {1, 4}).

fetch_peers(_, {_Attempt, _MaxAttempts}) when _Attempt >= _MaxAttempts ->
	unavailable;
fetch_peers(Node, {Attempt, MaxAttempts}) when Attempt > 1 ->
	timer:sleep(30 * 1000),
	io:format(
		"Retrying (try ~B of ~B) fetching peers from: ~p~n",
		[Attempt, MaxAttempts, Node]
	),
	fetch_peers1(Node, {Attempt, MaxAttempts});
fetch_peers(Node, {Attempt, MaxAttempts}) ->
	fetch_peers1(Node, {Attempt, MaxAttempts}).

fetch_peers1(Node, {Attempt, MaxAttempts}) ->
	Fetch = fun() -> ar_http_iface_client:get_peers(Node, 10 * 1000) end,
	case jobs:run(?JOBS_QUEUE_NAME, Fetch) of
		unavailable ->
			fetch_peers(Node, {Attempt + 1, MaxAttempts});
		Peers ->
			Peers
	end.
