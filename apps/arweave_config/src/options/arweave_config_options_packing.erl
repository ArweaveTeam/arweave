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
			default => ?DEFAULT_REPACK_BATCH_SIZE,
			type => pos_integer,
			legacy => repack_batch_size,
			short_description =>
				<<"Number of batches to process at a time during "
				  "in-place repacking.">>,
			long_description =>
				<<"For each partition being repacked, a batch "
				  "requires about 512 MiB of memory.">>
		},
		#{
			enabled => true,
			option_key => [packing, repack, cache_size],
			default => ?DEFAULT_REPACK_CACHE_SIZE_MB,
			type => pos_integer,
			legacy => repack_cache_size_mb,
			short_description =>
				<<"Cache size in MiB for in-place repacking.">>,
			long_description =>
				<<"The node restricts the cache size to this amount "
				  "for each partition being repacked.">>
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
				  "concurrently.">>,
			handle_set => fun(_K, V, _S, _A) ->
				ok = ar_sync_dispatcher:set_entropy_cache_size(V),
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
