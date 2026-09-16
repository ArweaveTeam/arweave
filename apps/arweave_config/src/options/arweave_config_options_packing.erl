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
            legacy => packing_cache_size_limit,
            short_description =>
                <<"Maximum size in MiB of the in-memory cache for chunks "
                  "being packed or unpacked (approximate).">>,
            long_description =>
                <<"Leave unset for automatic sizing from system memory; "
                  "normally only override for advanced tuning. Limits "
                  "chunks queued for or undergoing packing/unpacking, "
                  "including work from syncing and repacking. Sync chunks "
                  "also count toward sync.cache_size until their write "
                  "path completes; these limits overlap rather than "
                  "representing disjoint memory allocations. Entropy is "
                  "controlled separately by packing.entropy.cache_size.">>,
            handle_set => fun(_K, V, _S, _A) ->
                ok = ar_packing_server:set_cache_size(V),
                {store, V}
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
                <<"Holds reusable replica.2.9 entropy, not chunk data. "
                  "Separate from the chunk buffers limited by "
                  "sync.cache_size and packing.cache_size. Each entropy "
                  "is 8 MiB; one footprint needs 32 entropies (256 MiB). "
                  "Its size determines active network-sync footprint "
                  "capacity and automatic repack batch sizing. When tuning "
                  "replica.2.9 throughput, start with this setting and "
                  "normally leave the two chunk-buffer options unset. "
                  "Leave memory available for those buffers and other "
                  "node processes.">>,
            handle_set => fun(_K, V, _S, _A) ->
                ok = ar_repack:recompute_sizing(),
                {store, V}
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
    ok.

group_description() ->
    <<"Tune chunk packing and repacking behavior.">>.
