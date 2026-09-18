%%% @doc Specs for the `packing` option group. Options for
%%% governing the storage-encoding work applied to chunks.
-module(arweave_config_options_packing).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").

specs() ->
    [
        #{
            enabled => true,
            option_key => [packing, workers],
            default => ?DEFAULT_PACKING_WORKERS,
            type => pos_integer,
            legacy => packing_workers,
            short_description =>
                <<"Number of packing workers to spawn.">>,
            long_description =>
                <<"The default is the number of logical CPU cores.">>
        },
        #{
            enabled => true,
            option_key => [packing, cache_size],
            runtime => true,
            type => finite_pos_integer,
            legacy => data_cache_size_limit,
            short_description =>
                <<
                    "Maximum in-memory chunk cache size in MiB, shared by "
                    "syncing, copying and packing."
                >>,
            long_description =>
                <<
                    "Shared by network sync, local copying, disk-pool storage "
                    "and repacking. Includes space for each chunk's input, "
                    "unpacked intermediate and packed output until processing "
                    "completes. Leave unset for automatic sizing alongside "
                    "the separate packing.entropy.cache_size and peer-interval "
                    "cache. This limits cached chunks, not the total data "
                    "packed or total node memory. disk_pool.max_buffer_size "
                    "limits pending data on disk instead. Legacy "
                    "data_cache_size_limit counts 256 KiB chunks and is "
                    "converted to MiB."
                >>,
            handle_set => fun(_K, V, _S, _A) ->
                case ar_chunk_cache:configure([packing, cache_size], V) of
                    ok -> {store, V};
                    Error -> Error
                end
            end
        },
        #{
            enabled => true,
            option_key => [packing, repack, batch_size],
            runtime => true,
            default => undefined,
            type => pos_integer,
            legacy => repack_batch_size,
            short_description =>
                <<"Read batch size for in-place repacking "
                  "(unset = auto-derive, recommended).">>,
            long_description =>
                <<"When unset (the default), the batch size is derived "
                  "automatically from [packing, entropy, cache_size] "
                  "and the number of repack modules, so the entropy "
                  "footprint is always full and the entropy cache is not "
                  "thrashed. Set a positive value only to override the "
                  "derivation (e.g. for non-replica.2.9 repacks, which "
                  "generate no entropy, or for benchmarking).">>,
            handle_set => fun(_K, V, _S, _A) ->
                ok = ar_repack:recompute_sizing(),
                {store, V}
            end
        },
        #{
            enabled => true,
            option_key => [packing, entropy, cache_size],
            runtime => true,
            default => ?DEFAULT_REPLICA_2_9_ENTROPY_CACHE_SIZE_MB,
            type => pos_integer,
            legacy => replica_2_9_entropy_cache_size_mb,
            short_description =>
                <<"Maximum in-memory cache size in MiB for entropy shared "
                  "by syncing and repacking.">>,
            long_description =>
                <<
                    "Holds reusable replica.2.9 entropy, not chunk data. "
                    "Separate from the chunk cache limited by "
                    "packing.cache_size. Each entropy "
                    "is 8 MiB; one footprint needs 32 entropies (256 MiB). "
                    "Its size determines active network-sync footprint "
                    "capacity and automatic repack batch sizing. When tuning "
                    "replica.2.9 throughput, start with this setting and "
                    "normally leave packing.cache_size unset. "
                    "Leave memory available for the chunk cache and other "
                    "node processes."
                >>,
            handle_set => fun(_K, V, _S, _A) ->
                case ar_chunk_cache:configure([packing, entropy, cache_size], V) of
                    ok ->
                        ok = ar_repack:recompute_sizing(),
                        {store, V};
                    Error ->
                        Error
                end
            end
        },
        #{
            enabled => true,
            option_key => [packing, entropy, workers],
            runtime => true,
            default => ?DEFAULT_REPLICA_2_9_WORKERS,
            type => pos_integer,
            legacy => replica_2_9_workers,
            short_description =>
                <<"Number of entropy workers to spawn.">>,
            long_description =>
                <<"Entropy workers generate entropy for the "
                  "replica.2.9 format. By default, at most one "
                  "worker is active per physical disk at a time.">>,
            handle_set => fun(_K, V, _S, _A) ->
                ok = ar_device_lock:set_entropy_workers(V),
                {store, V}
            end
        }
    ].

validate() ->
    ar_chunk_cache:validate_config().

group_description() ->
    <<"Tune chunk packing and repacking behavior.">>.
