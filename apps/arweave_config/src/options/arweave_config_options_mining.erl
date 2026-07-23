%%% @doc Specs for the `mining` option group. Options for
%%% controlling the node's mining role and work execution.
-module(arweave_config_options_mining).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").

specs() ->
    [
        #{
            enabled => true,
            option_key => [mining, enabled],
            default => false,
            type => boolean,
            legacy => mine,
            short_description =>
                <<"Automatically start mining once the network has "
                  "been joined.">>
        },
        #{
            enabled => true,
            option_key => [mining, address],
            default => not_set,
            type => address,
            legacy => mining_addr,
            short_description =>
                <<"Address that mining rewards should be credited to.">>,
            long_description =>
                <<"If `mining.enabled` is set but no address is "
                  "specified, an RSA PSS key is created and stored "
                  "in the [data_dir]/wallets directory. If the "
                  "directory already contains such keys, the one "
                  "written later is picked. Accepts URL-safe base64 "
                  "form.">>
        },
        #{
            enabled => true,
            option_key => [mining, cache_size],
            runtime => true,
            type => pos_integer,
            legacy => mining_cache_size_mb,
            short_description =>
                <<"Total cache size in MiB allocated to store "
                  "unprocessed chunks while mining.">>,
            long_description =>
                <<"The mining server will only read new data when "
                  "there is room in the cache to store more chunks. "
                  "This cache is subdivided into sub-caches for each "
                  "mined partition. When omitted, it is determined "
                  "based on the number of mining partitions.">>,
            handle_set => fun(_K, V, _S, _A) ->
                ok = ar_mining_server:set_cache_size(V),
                {store, V}
            end
        },
        #{
            enabled => true,
            option_key => [mining, hashing_threads],
            default => ?NUM_HASHING_PROCESSES,
            type => pos_integer,
            legacy => hashing_threads,
            short_description =>
                <<"Number of hashing processes to spawn.">>
        }
    ].

validate() ->
    ok.

group_description() ->
    <<"Control mining behavior and reward attribution.">>.
