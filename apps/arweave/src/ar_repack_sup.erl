-module(ar_repack_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks.
%% ===================================================================

init([]) ->
    %% The repack workers read the per-store RocksDB databases
    %% (chunk_data_db, tx_index, ...) in their init, so this supervisor
    %% must start after ar_data_sync_sup, whose workers open them.
    {ok, {{one_for_one, 5, 10}, ar_repack:register_workers()}}.
