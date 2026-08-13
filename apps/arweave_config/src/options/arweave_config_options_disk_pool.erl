%%% @doc Specs for the `disk_pool` option group. Options for
%%% governing the temporary store for pending chunks.
-module(arweave_config_options_disk_pool).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").

specs() ->
    [
        #{
            enabled => true,
            option_key => [disk_pool, data_root_expiration_time],
            runtime => true,
            default => ?DEFAULT_DISK_POOL_DATA_ROOT_EXPIRATION_TIME_S,
            type => pos_integer,
            legacy => disk_pool_data_root_expiration_time,
            short_description =>
                <<"Seconds a pending or orphaned data root is kept "
                  "in the disk pool before being purged.">>
        },
        #{
            enabled => true,
            option_key => [disk_pool, workers],
            default => ?DEFAULT_DISK_POOL_JOBS,
            type => pos_integer,
            legacy => disk_pool_jobs,
            short_description =>
                <<"Number of disk-pool jobs.">>,
            long_description =>
                <<"Disk-pool jobs scan the disk pool to index "
                  "no-longer-pending or orphaned chunks, schedule "
                  "packing for chunks with a sufficient number of "
                  "confirmations, and remove abandoned chunks.">>
        },
        #{
            enabled => true,
            option_key => [disk_pool, max_buffer_size],
            runtime => true,
            default => ?DEFAULT_MAX_DISK_POOL_BUFFER_MB,
            type => pos_integer,
            legacy => max_disk_pool_buffer_mb,
            short_description =>
                <<"Maximum total size in MiB of pending chunks in "
                  "the disk pool.">>
        },
        #{
            enabled => true,
            option_key => [disk_pool, max_data_root_buffer_size],
            runtime => true,
            default => ?DEFAULT_MAX_DISK_POOL_DATA_ROOT_BUFFER_MB,
            type => pos_integer,
            legacy => max_disk_pool_data_root_buffer_mb,
            short_description =>
                <<"Maximum size in MiB per data root of pending "
                  "chunks in the disk pool.">>
        }
    ].

validate() ->
    ok.

group_description() ->
    <<"Manage pending chunk buffering and expiration.">>.
