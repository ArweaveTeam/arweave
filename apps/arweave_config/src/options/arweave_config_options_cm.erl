%%% @doc Specs for the `cm` option group. Options for
%%% configuring a node's participation in coordinated mining.
-module(arweave_config_options_cm).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").

specs() ->
	[
		#{
			enabled => true,
			option_key => [cm, enabled],
			default => false,
			type => boolean,
			legacy => coordinated_mining,
			short_description =>
				<<"Enable coordinated mining.">>,
			long_description =>
				<<"If you are a solo pool miner coordinating on a "
				  "replica with other pool miners, set this flag too. "
				  "To connect the internal nodes, set cm.api_secret, "
				  "and the cm_peer / cm_exit_peer roles on each "
				  "internal peer. Make sure every node specifies "
				  "every other node in the cluster. The same peer "
				  "may carry both the cm and cm_exit roles. Also, "
				  "set the mine flag on every CM peer. You may or "
				  "may not set the mine flag on the exit peer.">>
		},
		#{
			enabled => true,
			option_key => [cm, api_secret],
			default => not_set,
			legacy => cm_api_secret,
			short_description =>
				<<"Coordinated-mining secret used to authenticate "
				  "requests between private peers.">>,
			long_description =>
				<<"You need to also set cm.enabled, plus the cm_peer "
				  "and cm_exit_peer roles on each internal peer.">>
		},
		#{
			enabled => true,
			option_key => [cm, out_batch_timeout],
			default => ?DEFAULT_CM_BATCH_TIMEOUT_MS,
			type => pos_integer,
			legacy => cm_out_batch_timeout,
			short_description =>
				<<"Milliseconds between sending batches of H1 values "
				  "to other coordinated-mining peers.">>,
			long_description =>
				<<"A higher value reduces network traffic; a lower "
				  "value reduces hashing latency.">>
		},
		#{
			enabled => true,
			option_key => [cm, poll_interval],
			default => ?DEFAULT_CM_POLL_INTERVAL_MS,
			type => pos_integer,
			legacy => cm_poll_interval,
			short_description =>
				<<"Milliseconds between polling other "
				  "coordinated-mining peers for their partition "
				  "tables.">>
		}
	].

validate() ->
	case validate_cm_pool() of
		ok -> validate_cm_requirements();
		{error, _} = Err -> Err
	end.

%% @doc Cross-cutting: also reads [pool, is_server], [pool, is_client], [mining, enabled].
validate_cm_pool() ->
	CM = arweave_config:get([cm, enabled]),
	PoolSrv = arweave_config:get([pool, is_server]),
	PoolCli = arweave_config:get([pool, is_client]),
	Mine = arweave_config:get([mining, enabled]),
	case {CM, PoolSrv} of
		{true, true} ->
			{error, <<"The pool server node cannot participate "
					"in the coordinated mining.">>};
		_ ->
			case {PoolSrv, PoolCli} of
				{true, true} ->
					{error, <<"The node cannot be a pool server and a pool client "
							"at the same time.">>};
				_ ->
					case {PoolCli, Mine} of
						{true, false} ->
							{error, <<"The mine flag must be set along with "
									"the is_pool_client flag.">>};
						_ ->
							ok
					end
			end
	end.

%% @doc Cross-cutting: also reads [mining, enabled].
validate_cm_requirements() ->
	case arweave_config:get([cm, enabled]) of
		true ->
			Secret = arweave_config:get([cm, api_secret]),
			Mine = arweave_config:get([mining, enabled]),
			case Secret of
				not_set ->
					{error, <<"The cm_api_secret must be set when "
							"coordinated_mining is set.">>};
				_ ->
					case Mine of
						false ->
							{error, <<"The mine flag must be set when "
									"coordinated_mining is set.">>};
						true ->
							ok
					end
			end;
		_ ->
			ok
	end.

group_description() ->
	<<"Control coordinated-mining participation and coordination.">>.
