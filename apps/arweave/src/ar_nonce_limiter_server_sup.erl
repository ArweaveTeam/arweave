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
					++ peer_to_snake_case_list(Peer)),
			{Name, {ar_nonce_limiter_server_worker, start_link, [Name, Peer]}, permanent,
					?SHUTDOWN_TIMEOUT, worker, [Name]}
		end,
		Config#config.nonce_limiter_client_peers
	),
	{ok, {{one_for_one, 5, 10}, Workers}}.

peer_to_snake_case_list({A, B, C, D, Port}) ->
	integer_to_list(A) ++ "_" ++ integer_to_list(B) ++ "_" ++ integer_to_list(C) ++ "_"
			++ integer_to_list(D) ++ "_" ++ integer_to_list(Port).
