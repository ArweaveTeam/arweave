%%% @doc Arweave metrics application supervisor.
-module(arweave_metrics_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 5, 10}, [cache_child_spec()]}}.

cache_child_spec() ->
    #{
      id => arweave_metrics_cache,
      start => {arweave_metrics_cache, start_link, []}
     }.
