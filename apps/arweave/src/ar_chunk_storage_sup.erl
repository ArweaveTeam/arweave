-module(ar_chunk_storage_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave/include/ar_config.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks.
%% ===================================================================

init([]) ->
	ets:new(chunk_storage_file_index, [set, public, named_table, {read_concurrency, true}]),

	Workers = ar_chunk_storage:register_workers() ++
		ar_repack:register_workers() ++
		ar_entropy_gen:register_workers(ar_entropy_gen) ++
		ar_entropy_gen:register_workers(ar_entropy_storage),
	{ok, {{one_for_one, 5, 10}, Workers}}.
