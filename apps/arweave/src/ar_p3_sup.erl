-module(ar_p3_sup).

-behaviour(supervisor).

-export([start_link/0, start_ledger/1]).

-export([init/1]).



%%%===================================================================
%%% Definitions.
%%%===================================================================



-define(ar_p3_ledger_shutdown_ms, 20000). %% 20s



%%%===================================================================
%%% Public interface.
%%%===================================================================



-spec start_link() ->
	Ret :: supervisor:startlink_ret().

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).



-spec start_ledger(PeerAddress :: binary()) ->
	Ret :: supervisor:startchild_ret().

start_ledger(PeerAddress) ->
	supervisor:start_child(?MODULE, [PeerAddress]).



%% ===================================================================
%% Supervisor callbacks.
%% ===================================================================



-spec init(Args :: []) ->
	Ret :: {ok, {
		SupFlags :: supervisor:sup_flags(),
		[ChildSpec :: supervisor:child_spec()]
	}} | ignore.

init([]) ->
	SupFlags = #{
		strategy => simple_one_for_one,
		intensity => 0,
		period => 1
	},
	ChildSpecs = [#{
		id => ar_p3_ledger,
		start => {ar_p3_ledger, start_link, []},
		shutdown => ?ar_p3_ledger_shutdown_ms
	}],
	{ok, {SupFlags, ChildSpecs}}.
