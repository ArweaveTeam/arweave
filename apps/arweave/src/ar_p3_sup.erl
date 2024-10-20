-module(ar_p3_sup).

-behaviour(supervisor).

-include_lib("arweave/include/ar_config.hrl").

-export([start_link/0, start_ledger/1]).
-export([init/1]).

-export([get_ledger_config_opts/0]).



%%%===================================================================
%%% Definitions.
%%%===================================================================



-define(ar_p3_ledger_shutdown_ms, 20000). %% 20s



%%%===================================================================
%%% Public interface.
%%%===================================================================



start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).



start_ledger(PeerAddress) ->
	supervisor:start_child(?MODULE, [PeerAddress]).



%% ===================================================================
%% Supervisor callbacks.
%% ===================================================================



init([]) ->
	{CheckpointIntervalRecords, ShutdownTimeoutMs} = get_ledger_config_opts(),
	SupFlags = #{
		strategy => simple_one_for_one,
		intensity => 5,
		period => 1
	},
	ChildSpecs = [#{
		id => ar_p3_ledger,
		start => {ar_p3_ledger, start_link, [CheckpointIntervalRecords, ShutdownTimeoutMs]},
		shutdown => ?ar_p3_ledger_shutdown_ms
	}],
	{ok, {SupFlags, ChildSpecs}}.



%%%
%%%
%%%



get_ledger_config_opts() ->
	{ok, Config} = application:get_env(arweave, config),
	{
		Config#config.p3_ledger_checkpoint_interval,
		Config#config.p3_ledger_shutdown_timeout * 1000
	}.
