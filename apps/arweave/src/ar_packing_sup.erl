-module(ar_packing_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-include_lib("arweave/include/ar_sup.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks.
%% ===================================================================

init([]) ->
    ets:new(ar_packing_server, [set, public, named_table]),
	ets:new(ar_entropy_cache, [set, public, named_table]),
	ets:new(ar_entropy_cache_ordered_keys, [ordered_set, public, named_table]),

	{ok, {{one_for_one, 5, 10}, [
		?CHILD(ar_packing_server, worker),
		?CHILD(ar_entropy_cache, worker)
	]}}.
