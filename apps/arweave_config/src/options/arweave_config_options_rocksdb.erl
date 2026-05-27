%%% @doc Specs for the `rocksdb` option group. Options for
%%% controlling RocksDB behavior.
-module(arweave_config_options_rocksdb).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").

specs() ->
	[
		#{
			enabled => true,
			option_key => [rocksdb, flush_interval],
			default => ?DEFAULT_ROCKSDB_FLUSH_INTERVAL_S,
			type => pos_integer,
			legacy => rocksdb_flush_interval_s,
			short_description =>
				<<"RocksDB flush interval in seconds.">>
		},
		#{
			enabled => true,
			option_key => [rocksdb, wal_sync_interval],
			default => ?DEFAULT_ROCKSDB_WAL_SYNC_INTERVAL_S,
			type => pos_integer,
			legacy => rocksdb_wal_sync_interval_s,
			short_description =>
				<<"RocksDB WAL sync interval in seconds.">>
		}
	].

validate() ->
	ok.

group_description() ->
	<<"Set RocksDB flush and synchronization behavior.">>.
