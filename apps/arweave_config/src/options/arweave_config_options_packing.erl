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
			type => pos_integer,
			legacy => packing_cache_size_limit,
			short_description =>
				<<"Maximum number of data chunks kept in memory by "
				  "the packing process (approximate).">>,
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
				<<"Maximum cache size in MiB allocated for entropy.">>,
			long_description =>
				<<"Each cached entropy is 256 MiB. The bigger the "
				  "cache, the more replica.2.9 data can be synced "
				  "or repacked concurrently.">>,
			handle_set => fun(_K, V, _S, _A) ->
				ok = ar_sync_dispatcher:set_entropy_cache_size(V),
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
