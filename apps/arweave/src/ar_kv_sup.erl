-module(ar_kv_sup).

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
    ar_kv:create_ets(),
    {ok, {{one_for_one, 5, 10}, [ar_kv_child()]}}.

ar_kv_child() ->
    %% Give RocksDB flush/sync/close time to finish before the supervisor kills ar_kv.
    (?CHILD(ar_kv, worker))#{shutdown => 300_000}.
