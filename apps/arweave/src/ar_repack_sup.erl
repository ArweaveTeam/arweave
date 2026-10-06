-module(ar_repack_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks.
%% ===================================================================

init([]) ->

    Workers = ar_repack:register_workers() ++
        ar_entropy_gen:register_workers(ar_entropy_gen),
    {ok, {{one_for_one, 5, 10}, Workers}}.
