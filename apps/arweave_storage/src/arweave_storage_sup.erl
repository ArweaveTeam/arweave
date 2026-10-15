%%% Own storage metadata independently of the host node's lifetime.
-module(arweave_storage_sup).
-behaviour(supervisor).
-export([start_link/0, activate/0, activate/1, deactivate/0, init/1]).
-include_lib("arweave/include/ar_sup.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

activate() ->
    activate(node).

activate(Mode) when Mode == node; Mode == standalone ->
    Child = #{
        id => arweave_storage_runtime_sup,
        start => {arweave_storage_runtime_sup, start_link, [Mode]},
        restart => temporary,
        shutdown => infinity,
        type => supervisor
    },
    case supervisor:start_child(?MODULE, Child) of
        {error, {already_started, PID}} -> {ok, PID};
        Result -> Result
    end.

deactivate() ->
    case supervisor:terminate_child(?MODULE, arweave_storage_runtime_sup) of
        ok -> ok;
        {error, not_found} -> ok
    end.

init([]) ->
    ets:new(arweave_storage, [set, public, named_table]),
    {ok, {{one_for_one, 5, 10}, []}}.
