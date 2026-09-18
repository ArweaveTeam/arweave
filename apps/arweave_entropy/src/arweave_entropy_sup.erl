%%% Own entropy caches independently of the host node's preparation workers.
-module(arweave_entropy_sup).
-behaviour(supervisor).
-export([start_link/0, activate/0, deactivate/0, init/1]).
-include_lib("arweave/include/ar_sup.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

activate() ->
    Child = #{
        id => arweave_entropy_runtime_sup,
        start => {arweave_entropy_runtime_sup, start_link, []},
        restart => temporary,
        shutdown => infinity,
        type => supervisor
    },
    case supervisor:start_child(?MODULE, Child) of
        {error, {already_started, PID}} -> {ok, PID};
        Result -> Result
    end.

deactivate() ->
    case supervisor:terminate_child(?MODULE, arweave_entropy_runtime_sup) of
        ok -> ok;
        {error, not_found} -> ok
    end.

init([]) ->
    ets:new(ar_entropy_cache, [set, public, named_table]),
    ets:new(ar_entropy_cache_ordered_keys, [ordered_set, public, named_table]),
    ets:new(entropy_generation_stats, [ordered_set, public, named_table]),
    ets:new(arweave_entropy_generation, [set, public, named_table]),
    {ok, {{one_for_one, 5, 10}, []}}.
