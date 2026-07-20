%%% @doc Specs for the `pool` option group. Options for
%%% configuring how the node participates in mining pools.
-module(arweave_config_options_pool).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").

specs() ->
    [
        #{
            enabled => true,
            option_key => [pool, is_client],
            default => false,
            type => boolean,
            legacy => is_pool_client,
            short_description =>
                <<"Configure the node as a pool client.">>,
            long_description =>
                <<"The node may be an exit peer in the coordinated "
                  "mining setup or a standalone node.">>
        },
        #{
            enabled => true,
            option_key => [pool, is_server],
            runtime => true,
            default => false,
            type => boolean,
            legacy => is_pool_server,
            short_description =>
                <<"Configure the node as a pool server.">>,
            long_description =>
                <<"The pool node may not participate in coordinated "
                  "mining.">>
        },
        #{
            enabled => true,
            option_key => [pool, api_key],
            runtime => true,
            default => not_set,
            legacy => pool_api_key,
            short_description =>
                <<"API key for requests to the pool.">>
        },
        #{
            enabled => true,
            option_key => [pool, server_address],
            runtime => true,
            default => not_set,
            legacy => pool_server_address,
            short_description =>
                <<"The pool address.">>
        },
        #{
            enabled => true,
            option_key => [pool, worker_name],
            runtime => true,
            default => not_set,
            legacy => pool_worker_name,
            short_description =>
                <<"Optional pool worker name.">>,
            long_description =>
                <<"Useful if you have multiple machines (or "
                  "replicas) and you want to monitor them separately "
                  "on the pool.">>
        }
    ].

validate() ->
    ok.

group_description() ->
    <<"Manage mining pool participation and connectivity.">>.
