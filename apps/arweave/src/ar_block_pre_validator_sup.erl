-module(ar_block_pre_validator_sup).

-behaviour(supervisor).

-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave/include/ar_config.hrl").

-export([start_link/0]).
-export([init/1]).

%%%===================================================================
%%% Public API.
%%%===================================================================

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks.
%%%===================================================================

init([]) ->
	Children = [{ar_block_pre_validator, {ar_block_pre_validator, start_link,
			[ar_block_pre_validator, []]}, permanent, ?SHUTDOWN_TIMEOUT, worker,
			[ar_block_pre_validator]}],
	{ok, {{one_for_one, 5, 10}, Children}}.
