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
                <<"Memory budget in MiB for buffered sync chunks and "
                  "cached peer intervals (approximate).">>,
            long_description =>
                <<"Leave unset for automatic sizing from system memory; "
                  "normally only override for advanced tuning. Split ~90/10 "
                  "between chunks awaiting storage (including local copies "
                  "and disk-pool handoffs) and cached peer intervals. "
                  "Sync chunks remain counted while being packed/unpacked "
                  "and awaiting writes. packing.cache_size separately "
                  "limits the packing stage and can count the same chunks. "
                  "Entropy is outside this budget and is controlled by "
                  "packing.entropy.cache_size. This is not a total node "
                  "memory limit. The chunk cache has a 250 MiB minimum "
                  "(1000 chunks), and peer intervals have a 64 MiB minimum, "
                  "so small budgets may be exceeded. Legacy "
                  "data_cache_size_limit is still in 256 KiB chunks and is "
                  "converted to MiB, rounding up. New-style sync.cache_size "
                  "must be specified in MiB, not chunks or bytes.">>,
            handle_set =>
                fun(_K, V, _S, _A) ->
                    %% Returns the chunk limit, or ok before ETS exists.
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
                <<"Sync data (not headers) only from peers configured in "
                  "peers.local.">>
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
                  "Local copies, disk-pool processing, header sync, and "
                  "entropy preparation are controlled separately. "
                  "Fetch concurrency is sized automatically from per-peer "
                  "behavior, so this rate is the only sync-throughput "
                  "dial. The rate may be changed at runtime; a runtime 0 "
                  "pauses dispatch but does not stop the sync processes. "
                  "Replaces the removed sync_jobs / sync.jobs / "
                  "sync.workers options; old worker counts cannot be "
                  "converted to a download rate.">>
        }
    ].

validate() ->
    ok.

group_description() ->
    <<"Manage weave data discovery and synchronization.">>.
