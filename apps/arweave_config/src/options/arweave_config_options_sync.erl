%%% @doc Specs for the `sync` option group. Options for
%%% controlling how the node discovers and fetches chunk data.
-module(arweave_config_options_sync).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").

specs() ->
    [
        #{
            enabled => true,
            option_key => [sync, cache_size],
            runtime => true,
            type => finite_pos_integer,
            legacy => data_cache_size_limit,
            short_description =>
                <<"Total memory budget in MiB for the syncing processes' "
                  "in-memory caches (approximate).">>,
            long_description =>
                <<"Split ~90/10 between the fetched-chunk cache (downloaded "
                  "chunks awaiting the write path) and the peer interval "
                  "cache (warmed per-peer sync intervals; 10% share, floored "
                  "at 64 MiB). Note: this option previously sized only the "
                  "chunk cache; it now covers both, so the chunk cache gets "
                  "~90% of a previously-tuned value.">>,
            handle_set =>
                fun(_K, V, _S, _A) ->
                    %% set_chunk_cache_size_limit/1 returns the resolved limit (in
                    %% chunks), or ok only before the ETS table exists - don't match ok.
                    _ = ar_data_sync:set_chunk_cache_size_limit(V),
                    {store, V}
                end
        },
        #{
            enabled => true,
            option_key => [sync, request_packed_chunks],
            runtime => true,
            default => false,
            type => boolean,
            legacy => data_sync_request_packed_chunks,
            short_description =>
                <<"Request packed chunks from peers during data sync.">>
        },
        #{
            enabled => true,
            option_key => [sync, local_peers_only],
            runtime => true,
            default => false,
            type => boolean,
            legacy => sync_from_local_peers_only,
            short_description =>
                <<"Sync data (not headers) only from local-network "
                  "peers configured via the local_peer option.">>
        },
        #{
            enabled => true,
            option_key => [sync, max_download_rate],
            runtime => true,
            default => infinity,
            type => pos_integer,
            legacy => sync_max_download_rate,
            short_description =>
                <<"Maximum sync download rate in bytes per second.">>,
            long_description =>
                <<"Aggregate budget for chunk fetching. `infinity` (the "
                  "default) syncs as fast as peers, disks, and the link "
                  "allow; 0 at startup disables data syncing entirely. "
                  "Fetch concurrency is sized automatically from per-peer "
                  "behavior, so this rate is the only sync-throughput "
                  "dial. The rate may be changed at runtime; a runtime 0 "
                  "pauses dispatch but does not stop the sync processes. "
                  "(Replaces the removed sync_jobs / [sync, workers] "
                  "option.)">>
        }
    ].

validate() ->
    ok.

group_description() ->
    <<"Manage weave data discovery and synchronization.">>.
