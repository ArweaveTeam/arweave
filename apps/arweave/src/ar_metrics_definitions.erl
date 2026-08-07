%%% @doc Declarative catalogue of all Arweave Prometheus metrics.
%%%
%%% `all_metrics/0' returns the full list of metrics as
%%% `[{MetricType, Definition}]' tuples, where `MetricType' is the
%%% prometheus module implementing the metric (e.g. `prometheus_counter',
%%% `prometheus_gauge', `prometheus_histogram') and `Definition' is the
%%% proplist accepted by that module's `new/1' function. The `name'
%%% property is mandatory and is used by `arweave_metrics:cleanup/0' to
%%% deregister the metric.
%%%
-module(ar_metrics_definitions).

-export([all_metrics/0]).

%% @doc Return every Arweave metric as a `{MetricType, Definition}' tuple.
-spec all_metrics() -> [{module(), [{atom(), term()}]}].
all_metrics() ->
    [
        %% App info
        {prometheus_gauge, [
            {name, arweave_release},
            {help, "Arweave release number"}
        ]},

        %% Networking.
        {prometheus_counter, [
            {name, http_server_accepted_bytes_total},
            {help, "The total amount of bytes accepted by the HTTP server, per endpoint"},
            {labels, [route]}
        ]},
        {prometheus_counter, [
            {name, http_server_served_bytes_total},
            {help, "The total amount of bytes served by the HTTP server, per endpoint"},
            {labels, [route]}
        ]},
        {prometheus_counter, [
            {name, http_client_downloaded_bytes_total},
            {help, "The total amount of bytes requested via HTTP, per remote endpoint"},
            {labels, [route]}
        ]},
        {prometheus_counter, [
            {name, http_client_uploaded_bytes_total},
            {help, "The total amount of bytes posted via HTTP, per remote endpoint"},
            {labels, [route]}
        ]},
        {prometheus_gauge, [
            {name, arweave_peer_count},
            {help, "peer count"}
        ]},
        {prometheus_counter, [
            {name, gun_requests_total},
            {labels, [http_method, route, status_class]},
            {
                help,
                "The total number of GUN requests."
            }
        ]},
        %% NOTE: the erlang prometheus client looks at the metric name to determine units.
        %%       If it sees <name>_duration_<unit> it assumes the observed value is in
        %%       native units and it converts it to <unit> .To query native units, use:
        %%       erlant:monotonic_time() without any arguments.
        %%       See: https://github.com/deadtrickster/prometheus.erl/blob/6dd56bf321e99688108bb976283a80e4d82b3d30/src/prometheus_time.erl#L2-L84
        {prometheus_histogram, [
            {name, ar_http_request_duration_seconds},
            {buckets, [infinity]}, %% we don't care about the histogram portion
            {labels, [http_method, route, status_class]},
            {
                help,
                "The total duration of an ar_http:req call. This includes more than just the GUN "
                "request itself (e.g. establishing a connection, throttling, etc...)"
            }
        ]},
        {prometheus_histogram, [
            {name, http_client_get_chunk_duration_seconds},
            {buckets, [infinity]}, %% we don't care about the histogram portion
            {labels, [status_class, peer]},
            {
                help,
                "The total duration of an HTTP GET chunk request made to a peer."
            }
        ]},
        {prometheus_gauge, [
            {name, downloader_queue_size},
            {help, "The size of the back-off queue for the block and transaction headers "
                    "the node failed to sync and will retry later."}
        ]},
        {prometheus_gauge, [{name, outbound_connections},
                {help, "The current number of the open outbound network connections"}]},

        %% Transaction and block propagation.
        {prometheus_gauge, [
            {name, tx_queue_size},
            {help, "The size of the transaction propagation queue"}
        ]},
        {prometheus_counter, [
            {name, propagated_transactions_total},
            {labels, [status_class]},
            {
                help,
                "The total number of propagated transactions. Increases "
                "with the number of peers the node propagates transactions to."
            }
        ]},
        {prometheus_histogram, [
            {name, tx_propagation_bits_per_second},
            {buckets, [infinity]}, %% we don't care about the histogram portion
            {help, "The throughput (in bits/s) of transaction propagation."}
        ]},
        {prometheus_gauge, [
            {name, mempool_header_size_bytes},
            {
                help,
                "The size (in bytes) of the memory pool of transaction headers. "
                "The data fields of format=1 transactions are considered to be "
                "parts of transaction headers."
            }
        ]},
        {prometheus_gauge, [
            {name, mempool_data_size_bytes},
            {
                help,
                "The size (in bytes) of the memory pool of transaction data. "
                "The data fields of format=1 transactions are NOT considered "
                "to be transaction data."
            }
        ]},
        {prometheus_counter, [{name, block_announcement_missing_transactions},
                {help, "The total number of tx prefixes reported to us via "
                        "POST /block_announcement and not found in the mempool or block cache."}]},
        {prometheus_counter, [{name, block_announcement_reported_transactions},
                {help, "The total number of tx prefixes reported to us via "
                        "POST /block_announcement."}]},
        {prometheus_counter, [{name, block2_received_transactions},
                {help, "The total number of transactions received via POST /block2."}]},
        {prometheus_counter, [{name, block_announcement_missing_chunks},
                {help, "The total number of chunks reported to us via "
                        "POST /block_announcement and not found locally."}]},
        {prometheus_counter, [{name, block_announcement_reported_chunks},
                {help, "The total number of chunks reported to us via "
                        "POST /block_announcement."}]},
        {prometheus_counter, [{name, block2_fetched_chunks},
                {help, "The total number of chunks fetched locally during the successful"
                        " processing of POST /block2."}]},
        {prometheus_histogram, [
            {name, ar_mempool_add_tx_duration_milliseconds},
            {buckets, [infinity]}, %% we don't care about the histogram portion
            {help, "The duration in milliseconds it took to add a transaction to the mempool."}
        ]},
        {prometheus_histogram, [
            {name, reverify_mempool_chunk_duration_milliseconds},
            {buckets, [infinity]}, %% we don't care about the histogram portion
            {help, "The duration in milliseconds it took to reverify a chunk of transactions "
                    "in the mempool."}
        ]},
        {prometheus_histogram, [
            {name, drop_txs_duration_milliseconds},
            {buckets, [infinity]}, %% we don't care about the histogram portion
            {help, "The duration in milliseconds it took to drop a chunk of transactions "
                    "from the mempool."}
        ]},
        {prometheus_histogram, [
            {name, del_from_propagation_queue_duration_milliseconds},
            {buckets, [infinity]}, %% we don't care about the histogram portion
            {help, "The duration in milliseconds it took to remove a transaction from the "
                    "propagation queue after it was emitted to peers."}
        ]},

        %% Data seeding.
        {prometheus_gauge, [
            {name, weave_size},
            {help, "The size of the weave (in bytes)."}
        ]},
        {prometheus_gauge, [
            {name, v2_index_data_size},
            {help, "The size (in bytes) of the data stored and indexed. Note: if "
                    "multiple storage modules cover the same range of data, that "
                    "range will be counted multiple times."}
        ]},
        {prometheus_gauge, [
            {name, v2_index_data_size_by_packing},
            {labels, [store_id, packing, partition_number, storage_module_size, storage_module_index,
                packing_difficulty]},
            {help, "The size (in bytes) of the data stored and indexed. Grouped by the "
                    "store ID, packing, partition number, storage module size, "
                    "storage module index, and packing difficulty."}
        ]},
        {prometheus_gauge, [
            {name, tip_partition_data_size_by_packing},
            {labels, [packing]},
            {help, "The size (in bytes) of the data stored and indexed for the tip "
                    "partition (floor(weave_size / partition_size)), summed across "
                    "all storage modules covering that partition. Grouped by packing."}
        ]},

        %% Disk pool.
        {prometheus_gauge, [
            {name, pending_chunks_size},
            {
                help,
                "The total size in bytes of stored pending and seeded chunks."
            }
        ]},
        {prometheus_gauge, [
            {name, disk_pool_chunks_count},
            {
                help,
                "The approximate number of chunks in the disk pool."
                "The disk pool includes pending, recent, and orphaned chunks."
            }
        ]},
        {prometheus_counter, [
            {name, disk_pool_processed_chunks},
            {
                help,
                "The counter is incremented every time the periodic process"
                " looks up a chunk from the disk pool and decides whether to"
                " remove it, include it in the weave, or keep in the disk pool."
            }
        ]},

        %% Consensus.
        {prometheus_gauge, [
            {name, arweave_block_height},
            {help, "The block height."}
        ]},
        {prometheus_gauge, [{name, block_time},
                {help, "The time in seconds between two blocks as recorded by the miners."}]},
        {prometheus_gauge, [
            {name, block_vdf_time},
            {help, "The number of the VDF steps between two consequent blocks."}
        ]},
        {prometheus_gauge, [
            {name, block_vdf_advance},
            {help, "The number of the VDF steps a received block is ahead of our current step."}
        ]},
        {prometheus_gauge, [
            {name, wallet_list_size},
            {
                help,
                "The total number of wallets in the system."
            }
        ]},
        {prometheus_histogram, [
            {name, account_tree_call_duration_milliseconds},
            {labels, [call]},
            {buckets, [1, 5, 10, 50, 100, 500, 1000, 5000, 30000]},
            {help,
                "The duration in milliseconds of an ar_account_tree gen_server call, "
                "labeled by request type."}
        ]},
        {prometheus_histogram, [
            {name, account_tree_sink_move_hops},
            {labels, [call]},
            {buckets, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 50]},
            {help,
                "The number of diff DAG edges traversed when the ETS account tree is "
                "repositioned to another representation, labeled by the operation that "
                "triggered the move. Grows with fork depth."}
        ]},
        {prometheus_histogram, [
            {name, account_tree_rehashed_nodes},
            {buckets, [10, 100, 1000, 10000, 100000, 1000000, 10000000]},
            {help,
                "The number of dirty account tree nodes re-hashed by one compute_hash pass. "}
        ]},
        {prometheus_histogram, [
            {name, fork_recovery_depth},
            {buckets, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 50]},
            {help,
                "The number of blocks on the fork we switch to, counted from the first "
                "block after the last block shared with the abandoned chain."}
        ]},
        {prometheus_histogram, [
            {name, block_pre_validation_time},
            {buckets, [infinity]}, %% we don't care about the histogram portion
            {help,
                "The time in milliseconds taken to parse the POST /block input and perform a "
                "preliminary validation before relaying the block to peers."}
        ]},
        {prometheus_histogram, [
            {name, block_processing_time},
            {buckets, [infinity]}, %% we don't care about the histogram portion
            {help,
                "The time in seconds taken to validate the block and apply it on top of "
                "the current state, possibly involving a chain reorganisation."}
        ]},
        {prometheus_gauge, [
            {name, synced_blocks},
            {
                help,
                "The total number of synced block headers."
            }
        ]},

        %% Mining.
        {prometheus_gauge, [
            {name, mining_rate},
            {labels, [type, partition]},
            {help, "Tracks 3 different mining rate metrics, each with a different type label. "
                    "The type label can be 'read', 'raw_read', 'hash', or 'ideal'. "
                    "'read' tracks the number of chunks read per second - recorded in MiB per second. "
                    "This is the effective mining read rate as it considers all limiting factors like "
                    "nonce limiter, hashing speed, etc..."
                    "'raw_read' tracks the average read rate of the partition ignoring any other "
                    "limiting factors - recorded in MiB per second."
                    "'hash' tracks the number of solutions candidates generated per second. "
                    "'ideal' tracks the ideal read rate given the current VDF step time and amount of "
                    "data synced. The partition label breaks the mining rate down by partition. "
                    "The overall mining rate is inidcated by 'total'."}
        ]},
        {prometheus_gauge, [
            {name, cm_h1_rate},
            {labels, [peer, direction]},
            {help, "The number of H1 hashes exchanged with a coordinated mining peer per second. "
                    "The peer label indicates the peer that the value is exchanged with, and the "
                    "direction label can be 'to' or 'from'."}
        ]},
        {prometheus_gauge, [
            {name, cm_h2_count},
            {labels, [peer, direction]},
            {help, "The total number of H2 hashes exchanged with a coordinated mining peer. "
                    "The peer label indicates the peer that the value is exchanged with, and the "
                    "direction label can be 'to' or 'from'."}
        ]},
        {prometheus_gauge, [
            {name, mining_server_chunk_cache_size},
            {labels, [partition, type]},
            {help, "The amount of data (measured in bytes) "
                "fetched during mining and not processed yet. "
                "The type label can be 'total', 'reserved'."}
        ]},
        {prometheus_gauge, [
            {name, mining_server_task_queue_len},
            {labels, [task]},
            {help, "The number of items in the mining server task queue."}
        ]},
        {prometheus_gauge, [
            {name, mining_solution},
            {labels, [reason]},
            {help, "Incremented whenever the miner generates a solution. The 'reason' label "
                    "will be 'success' if a block was successfully prepared from the solution, "
                    "and will list a failure reason otherwise. Note: even if a block is "
                    "successfully prepared from a solution, it does not necessarily mean "
                    "the block ended up in the blockchain."}
        ]},
        {prometheus_histogram, [
            {name, chunk_storage_sync_record_check_duration_milliseconds},
            {labels, [requested_chunk_count]},
            {buckets, [infinity]}, %% we don't care about the histogram portion
            {help, "The time in milliseconds it took to check the fetched chunk range "
                    "is actually registered by the chunk storage."}
        ]},
        {prometheus_gauge, [
            {name, mining_server_tasks},
            {labels, [task]},
            {help, "Incremented each time the mining server adds a task to the task queue."}
        ]},
        {prometheus_gauge, [
            {name, mining_vdf_step},
            {help, "Incremented each time the mining server processes a VDF step."}
        ]},

        %% VDF.
        {prometheus_histogram, [
            {name, vdf_step_time_milliseconds},
            {buckets, [infinity]}, %% we don't care about the histogram portion
            {labels, []},
            {help, "The time in milliseconds it took to compute a VDF step."}
        ]},
        {prometheus_gauge, [
            {name, vdf_step},
            {help, "The current VDF step."}
        ]},
        {prometheus_gauge, [
            {name, vdf_difficulty},
            {labels, [type]},
            {help, "The cached VDF difficulty. 'type' can be either 'current' or 'next'."}
        ]},

        %% Economic metrics.
        {prometheus_gauge, [
            {name, average_network_hash_rate},
            {help, "The average network hash rate measured over the last ~30 days of blocks"}
        ]},
        {prometheus_gauge, [
            {name, average_block_reward},
            {help, "The average block reward in Winston computed from the last ~30 days of blocks"}
        ]},
        {prometheus_gauge, [
            {name, expected_block_reward},
            {help, "The block reward required to sustain 20 replicas of the present weave"
                    " as currently estimated by the protocol."}
        ]},
        {prometheus_gauge, [
            {name, network_data_size},
            {help, "Total size of the network data in bytes."}
        ]},
        {prometheus_gauge, [
            {name, v2_price_per_gibibyte_minute},
            {help, "The price of storing 1 GiB for one minute as it will be calculated once the"
                    " transition to the new pricing protocol is complete."}
        ]},
        {prometheus_gauge, [
            {name, price_per_gibibyte_minute},
            {help, "The price of storing 1 GiB for one minute as currently estimated by "
                    "the protocol."}
        ]},
        {prometheus_gauge, [
            {name, legacy_price_per_gibibyte_minute},
            {help, "The price of storing 1 GiB for one minute as estimated by the previous ("
                    "USD to AR benchmark-based) version of the protocol."}
        ]},
        {prometheus_gauge, [
            {name, endowment_pool},
            {help, "The amount of Winston in the endowment pool."}
        ]},
        {prometheus_gauge, [
            {name, kryder_plus_rate_multiplier},
            {help, "Kryder+ rate multiplier."}
        ]},
        {prometheus_gauge, [
            {name, endowment_pool_take},
            {help, "Value we take from endowment pool to miner to compensate difference between expected and real reward."}
        ]},
        {prometheus_gauge, [
            {name, endowment_pool_give},
            {help, "Value we give to endowment pool from transaction fees."}
        ]},
        {prometheus_gauge, [
            {name, available_supply},
            {help, "The total supply minus the endowment, in Winston."}
        ]},
        {prometheus_gauge, [
            {name, debt_supply},
            {help, "The amount of Winston emitted when the endowment pool was not sufficiently"
                    " large to compensate mining."}
        ]},
        {prometheus_gauge, [
            {name, poa_count},
            {labels, [chunks]},
            {help, "A count of the number of 1-chunk and 2-chunk blocks in the last 21,600 blocks. "
                    "The 'chunks' label is 1 for the count of 1-chunk blocks, and 2 for the count of "
                    "2-chunk blocks."}
        ]},
        {prometheus_gauge, [
            {name, log_diff},
            {labels, [chunk]},
            {help, "The current linear difficulty converted to log scale. The chunk label "
                    "is either 'poa1' or 'poa2'."}
        ]},
        {prometheus_gauge, [
            {name, network_hashrate},
            {help, "An estimation of the network hash rate based on the mining difficulty "
                    "of the latest block."}
        ]},
        {prometheus_gauge, [
            {name, expected_minimum_200_years_storage_costs_decline_rate},
            {help, "The expected minimum decline rate sufficient to subsidize storage of "
                    "the current weave for 200 years according to the legacy (2.5) estimations."}
        ]},
        {prometheus_gauge, [
            {name, expected_minimum_200_years_storage_costs_decline_rate_10_usd_ar},
            {help, "The expected minimum decline rate sufficient to subsidize storage of "
                    "the current weave for 200 years according to the legacy (2.6) estimations"
                    "and assuming 10 $/AR."}
        ]},

        %% Packing.
        {prometheus_histogram, [
            {name, packing_duration_milliseconds},
            {labels, [type, packing, trigger]},
            {buckets, [infinity]}, %% we don't care about the histogram portion
            {help, "The packing/unpacking time in milliseconds. The type label indicates what "
                    "type of operation was requested either: 'pack', 'unpack',"
                    "'unpack_sub_chunk', or 'pack_sub_chunk'. The packing "
                    "label differs based on the type. If type is 'unpack' then the packing label "
                    "indicates the format of the chunk before being unpacked. If type is 'pack' "
                    "then the packing label indicates the format that the chunk will be packed "
                    "to. In all cases its value can be 'spora_2_5', 'spora_2_6', "
                    "or 'replica_2_9'. The trigger label shows where the request was triggered: "
                    "'external' (e.g. an HTTP request) or 'internal' (e.g. during syncing or "
                    "repacking)."}
        ]},
        {prometheus_counter, [
            {name, packing_requests},
            {labels, [type, packing]},
            {help, "The number of packing requests received. The type label indicates what "
                    "type of operation was requested either: 'pack', 'unpack', or "
                    "'unpack_sub_chunk'. The packing "
                    "label differs based on the type. If type is 'unpack' then the packing label "
                    "indicates the format of the chunk before being unpacked. If type is 'pack' "
                    "then the packing label indicates the format that the chunk will be packed "
                    "to. In all cases its value can be 'unpacked', 'unpacked_padded', "
                    "'spora_2_5', 'spora_2_6', or 'replica_2_9'."}
        ]},
        {prometheus_counter, [
            {name, validating_packed_spora},
            {labels, [packing]},
            {help, "The number of SPoRA solutions based on packed chunks entered validation. "
                    "The packing label can be 'spora_2_5', 'spora_2_6', or 'replica_2_9'."}
        ]},
        {prometheus_gauge, [{name, packing_buffer_size},
            {help, "The number of chunks in the packing server queue."}]},
        {prometheus_gauge, [{name, chunk_cache_size},
                {help, "The number of chunks scheduled for downloading."}]},
        {prometheus_counter, [{name, chunks_stored},
            {labels, [packing, store_id]},
            {help, "The counter is incremented every time a chunk is written to "
                    "chunk_storage."}]},
        {prometheus_counter, [{name, chunks_read},
            {labels, [store_id]},
            {help, "The counter is incremented every time a chunk is read from "
                    "chunk_storage."}]},
        {prometheus_histogram, [
            {name, chunk_read_rate_bytes_per_second},
            {labels, [store_id, type]},
            {buckets, [infinity]}, %% we don't care about the histogram portion
            {help, "The rate, in bytes per second, at which chunks are read from storage. "
                    "The type label can be 'raw' or 'repack'."}
        ]},
        {prometheus_histogram, [
            {name, chunk_write_rate_bytes_per_second},
            {labels, [store_id, type]},
            {buckets, [infinity]}, %% we don't care about the histogram portion
            {help, "The rate, in bytes per second, at which chunks are written to storage."}
        ]},
        {prometheus_gauge, [{name, data_discovery},
            {labels, [type, store_id, stat]},
            {help, "Tracks peer availability statistics from data discovery. "
                    "'type' is 'normal' or 'footprint'. "
                    "'stat' is 'num_peers' - distinct peers offering data "
                    "anywhere in this store_id's range."}]},
        {prometheus_gauge, [{name, peer_interval_cache_size},
            {labels, [unit]},
            {help, "Size of ar_data_discovery's peer interval cache "
                    "(per-(peer, window, mode) entries). 'unit' is 'rows' "
                    "(row count) or 'bytes' (ets:info memory * wordsize, "
                    "capped by ?MAX_INTERVAL_CACHE_BYTES). Sustained pressure "
                    "near the byte cap indicates the cap should be raised or "
                    "peer scanning is thrashing."}]},
        {prometheus_counter, [{name, peer_interval_cache_evictions},
            {labels, [reason]},
            {help, "Cumulative rows evicted from ar_data_discovery's peer "
                    "interval cache. 'reason' is 'trim' (LRU cap fired) or "
                    "'peer_removed' (whole-peer wipe on remove_peer)."}]},
        {prometheus_counter, [{name, sync_tasks},
            {labels, [state, peer]},
            {help, "Sync task counters per peer. queued_in/queued_out track "
                    "every queue add/remove (queue depth = queued_in - "
                    "queued_out). Other states are flavor: dispatched, "
                    "completed, activate_footprint, deactivate_footprint, "
                    "rebalance_cut, reaped, dropped_unavailable."}]},
        {prometheus_counter, [{name, sync_chunks_skipped},
            {labels, [reason]},
            {help, "The number of chunks skipped during syncing."}]},
        {prometheus_gauge, [{name, device_lock_status},
            {labels, [store_id, mode]},
            {help, "The device lock status of the storage module. "
                    "-1: off, 0: paused, 1: active, 2: complete -2: unknown"}]},
        {prometheus_gauge, [{name, sync_task_queue_inflight_bytes},
            {labels, [store_id]},
            {help, "Total bytes in the per-StoreID sync_task_queue's "
                    "inflight_intervals (ranges ar_peer_sync has pushed to "
                    "ar_sync_dispatcher and not yet released, used by its dedup "
                    "gate). Climbing without bound flags a dedup-overlay leak."}]},
        {prometheus_gauge, [{name, repack_chunk_states},
            {labels, [store_id, type, state]},
            {help, "The count of chunks in each state. 'type' can be 'cache' or 'queue'."}]},

        %% ---------------------------------------------------------------------------------------
        %% Replica 2.9 metrics
        %% ---------------------------------------------------------------------------------------
        {prometheus_counter, [{name, replica_2_9_entropy_stored},
            {labels, [store_id]},
            {help, "The number of bytes of replica.2.9 entropy written to chunk storage."}]},
        {prometheus_counter, [{name, replica_2_9_entropy_generated},
            {help, "The number of bytes of replica.2.9 entropy generated."}]},
        {prometheus_gauge, [{name, replica_2_9_entropy_cache},
            {help, "The size (in bytes) of the replica.2.9 entropy cache."}]},
        {prometheus_counter, [{name, replica_2_9_entropy_stats},
            {labels, [partition, stat]},
            {help, "Count of different replica_2_9 entropy events: 'cache_hit', 'cache_miss', "
                "'redundant'."}]},
        {prometheus_histogram, [
            {name, replica_2_9_entropy_duration_milliseconds},
            {buckets, [infinity]}, %% we don't care about the histogram portion
            {help, "The time, in milliseconds, to generate 256 MiB of replica.2.9 entropy."}
        ]},

        %% ---------------------------------------------------------------------------------------
        %% Pool related metrics
        %% ---------------------------------------------------------------------------------------
        {prometheus_counter, [
            {name, pool_job_request_count},
            {help, "The number of requests to pool /job from start of arweave node"}
        ]},
        {prometheus_counter, [
            {name, pool_total_job_got_count},
            {help, "The number of jobs received from /job requests."}
        ]},

        %% ---------------------------------------------------------------------------------------
        %% Debug-only metrics
        %% ---------------------------------------------------------------------------------------
        {prometheus_counter, [{name, process_functions},
                {labels, [process]},
                {help, "Sampling active functions. The 'process' label is a fully qualified "
                        "function name with the format 'process~module:function/arith'. "
                        "Only set when debug=true."}]},
        %% process_info gets unregistered and re-registered in ar_process_sampler.erl
        {prometheus_gauge, [{name, process_info},
                {labels, [process, type]},
                {help, "Sampling info about active processes. Only set when debug=true."}]},
        {prometheus_gauge, [{name, scheduler_utilization},
                {labels, [type]},
                {help, "Average scheduler utilization. `type` maps to the sched_type as defined here: "
                    "https://www.erlang.org/doc/man/scheduler#type-sched_util_result. "
                    "Only set when debug=true."}]},
        {prometheus_gauge, [{name, allocator},
                {labels, [type, instance, section, metric]},
                {help, "Erlang VM memory allocator metrics. Only set when debug=true."}]}
    ].
