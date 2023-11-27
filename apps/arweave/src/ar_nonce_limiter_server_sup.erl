-module(ar_nonce_limiter_server_sup).

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
	{ok, Config} = application:get_env(arweave, config),
	Workers = lists:map(
		fun(Peer) ->
			Name = list_to_atom("ar_nonce_limiter_server_worker_"
					++ ar_util:peer_to_str(Peer)),
			?CHILD_WITH_ARGS(ar_nonce_limiter_server_worker, worker, Name, [Name, Peer])
		end,
		Config#config.nonce_limiter_client_peers
	),
	Workers2 = [?CHILD(ar_nonce_limiter_server, worker) | Workers],
	{ok, {{one_for_one, 5, 10}, Workers2}}.
