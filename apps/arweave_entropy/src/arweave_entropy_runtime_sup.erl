%%% Preparation workers activate after packing, storage and device scheduling.
-module(arweave_entropy_runtime_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Workers = arweave_entropy_preparation:register_workers(
        arweave_entropy_preparation
    ),
    {ok, {{one_for_one, 5, 10}, Workers}}.
