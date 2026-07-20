%%% @doc The Arweave metrics application.
%%%
-module(arweave_metrics).

-behaviour(application).

-export([start/0, stop/0]).
-export([start/2, stop/1]).

-export([start_cache/0, register/0, cleanup/0, get_status_class/1, record_rate_metric/4]).

%% Safe runtime metric helpers — see the "Safe metric helpers" section below.
-export([gauge_set/2, gauge_set/3, gauge_inc/1, gauge_inc/2, gauge_inc/3,
		gauge_dec/1, gauge_dec/2, gauge_dec/3, gauge_deregister/1,
		gauge_value/1, gauge_value/2,
		counter_inc/1, counter_inc/2, counter_inc/3,
		histogram_observe/2, histogram_observe/3]).

-include_lib("kernel/include/logger.hrl").
-include_lib("arweave/include/ar.hrl"). %% FIXME: this is a circular dependency

%% @doc Start the `arweave_metrics' application and its dependencies.
-spec start() -> ok | {error, term()}.
start() ->
	case application:ensure_all_started(?MODULE, permanent) of
		{ok, Dependencies} ->
			?LOG_DEBUG("arweave_metrics started dependencies: ~p", [Dependencies]),
			ok;
		Else ->
			Else
	end.

%% @doc Stop the `arweave_metrics' application.
-spec stop() -> ok.
stop() ->
	application:stop(?MODULE).

%% @doc `application' callback. Register the Prometheus collectors and
%% declare the Arweave metrics, then bring up the supervisor which owns
%% the render-cache ETS table and the `arweave_metrics_cache' renderer.
start(_StartType, _StartArgs) ->
	prometheus_registry:register_collector(prometheus_process_collector),
	prometheus_registry:register_collector(arweave_metrics_collector),
	arweave_metrics:register(),
	arweave_metrics_sup:start_link().

%% @doc `application' callback.
stop(_State) ->
	arweave_metrics:cleanup(),
	ok.

%%%===================================================================
%%% Public interface.
%%%===================================================================
%% @doc start metrics cache
start_cache() ->
    arweave_metrics_sup:start_cache().

%% @doc Declare Arweave metrics.
register() ->
	%% App info
	prometheus_gauge:new([
		{name, arweave_release},
		{help, "Arweave release number"}
	]),
	%% Release number never changes so just set it here.
	prometheus_gauge:set(arweave_release, ?RELEASE_NUMBER),

	%% Networking.
	prometheus_counter:new([
		{name, http_server_accepted_bytes_total},
		{help, "The total amount of bytes accepted by the HTTP server, per endpoint"},
		{labels, [route]}
	]),
	prometheus_counter:new([
		{name, http_server_served_bytes_total},
		{help, "The total amount of bytes served by the HTTP server, per endpoint"},
		{labels, [route]}
	]),
	prometheus_counter:new([
		{name, http_client_downloaded_bytes_total},
		{help, "The total amount of bytes requested via HTTP, per remote endpoint"},
		{labels, [route]}
	]),
	prometheus_counter:new([
		{name, http_client_uploaded_bytes_total},
		{help, "The total amount of bytes posted via HTTP, per remote endpoint"},
		{labels, [route]}
	]),
	prometheus_gauge:new([
		{name, arweave_peer_count},
		{help, "peer count"}
	]),
	prometheus_counter:new([
		{name, gun_requests_total},
		{labels, [http_method, route, status_class]},
		{
			help,
			"The total number of GUN requests."
		}
	]),
	%% NOTE: the erlang prometheus client looks at the metric name to determine units.
	%%       If it sees <name>_duration_<unit> it assumes the observed value is in
	%%       native units and it converts it to <unit> .To query native units, use:
	%%       erlant:monotonic_time() without any arguments.
	%%       See: https://github.com/deadtrickster/prometheus.erl/blob/6dd56bf321e99688108bb976283a80e4d82b3d30/src/prometheus_time.erl#L2-L84
	prometheus_histogram:new([
		{name, ar_http_request_duration_seconds},
		{buckets, [infinity]}, %% we don't care about the histogram portion
        {labels, [http_method, route, status_class]},
		{
			help,
			"The total duration of an ar_http:req call. This includes more than just the GUN "
			"request itself (e.g. establishing a connection, throttling, etc...)"
		}
	]),
	prometheus_histogram:new([
		{name, http_client_get_chunk_duration_seconds},
		{buckets, [infinity]}, %% we don't care about the histogram portion
        {labels, [status_class, peer]},
		{
			help,
			"The total duration of an HTTP GET chunk request made to a peer."
		}
	]),

	prometheus_gauge:new([
		{name, downloader_queue_size},
		{help, "The size of the back-off queue for the block and transaction headers "
				"the node failed to sync and will retry later."}
	]),
	prometheus_gauge:new([{name, outbound_connections},
			{help, "The current number of the open outbound network connections"}]),

	%% Transaction and block propagation.
	prometheus_gauge:new([
		{name, tx_queue_size},
		{help, "The size of the transaction propagation queue"}
	]),
	prometheus_counter:new([
		{name, propagated_transactions_total},
		{labels, [status_class]},
		{
			help,
			"The total number of propagated transactions. Increases "
			"with the number of peers the node propagates transactions to."
		}
	]),
	prometheus_histogram:declare([
		{name, tx_propagation_bits_per_second},
		{buckets, [infinity]}, %% we don't care about the histogram portion
		{help, "The throughput (in bits/s) of transaction propagation."}
	]),
	prometheus_gauge:declare([
		{name, mempool_header_size_bytes},
		{
			help,
			"The size (in bytes) of the memory pool of transaction headers. "
			"The data fields of format=1 transactions are considered to be "
			"parts of transaction headers."
		}
	]),
	prometheus_gauge:new([
		{name, mempool_data_size_bytes},
		{
			help,
			"The size (in bytes) of the memory pool of transaction data. "
			"The data fields of format=1 transactions are NOT considered "
			"to be transaction data."
		}
	]),
	prometheus_counter:new([{name, block_announcement_missing_transactions},
			{help, "The total number of tx prefixes reported to us via "
					"POST /block_announcement and not found in the mempool or block cache."}]),
	prometheus_counter:new([{name, block_announcement_reported_transactions},
			{help, "The total number of tx prefixes reported to us via "
					"POST /block_announcement."}]),
	prometheus_counter:new([{name, block2_received_transactions},
			{help, "The total number of transactions received via POST /block2."}]),
	prometheus_counter:new([{name, block_announcement_missing_chunks},
			{help, "The total number of chunks reported to us via "
					"POST /block_announcement and not found locally."}]),
	prometheus_counter:new([{name, block_announcement_reported_chunks},
			{help, "The total number of chunks reported to us via "
					"POST /block_announcement."}]),
	prometheus_counter:new([{name, block2_fetched_chunks},
			{help, "The total number of chunks fetched locally during the successful"
					" processing of POST /block2."}]),
	prometheus_histogram:new([
		{name, ar_mempool_add_tx_duration_milliseconds},
		{buckets, [infinity]}, %% we don't care about the histogram portion
		{help, "The duration in milliseconds it took to add a transaction to the mempool."}
	]),
	prometheus_histogram:new([
		{name, reverify_mempool_chunk_duration_milliseconds},
		{buckets, [infinity]}, %% we don't care about the histogram portion
		{help, "The duration in milliseconds it took to reverify a chunk of transactions "
				"in the mempool."}
	]),
	prometheus_histogram:new([
		{name, drop_txs_duration_milliseconds},
		{buckets, [infinity]}, %% we don't care about the histogram portion
		{help, "The duration in milliseconds it took to drop a chunk of transactions "
				"from the mempool."}
	]),
	prometheus_histogram:new([
		{name, del_from_propagation_queue_duration_milliseconds},
		{buckets, [infinity]}, %% we don't care about the histogram portion
		{help, "The duration in milliseconds it took to remove a transaction from the "
				"propagation queue after it was emitted to peers."}
	]),

	%% Data seeding.
	prometheus_gauge:new([
		{name, weave_size},
		{help, "The size of the weave (in bytes)."}
	]),
	prometheus_gauge:new([
		{name, v2_index_data_size},
		{help, "The size (in bytes) of the data stored and indexed. Note: if "
				"multiple storage modules cover the same range of data, that "
				"range will be counted multiple times."}
	]),
	prometheus_gauge:new([
		{name, v2_index_data_size_by_packing},
		{labels, [store_id, packing, partition_number, storage_module_size, storage_module_index,
			  packing_difficulty]},
		{help, "The size (in bytes) of the data stored and indexed. Grouped by the "
				"store ID, packing, partition number, storage module size, "
				"storage module index, and packing difficulty."}
	]),
	prometheus_gauge:new([
		{name, tip_partition_data_size_by_packing},
		{labels, [packing]},
		{help, "The size (in bytes) of the data stored and indexed for the tip "
				"partition (floor(weave_size / partition_size)), summed across "
				"all storage modules covering that partition. Grouped by packing."}
	]),

	%% Disk pool.
	prometheus_gauge:new([
		{name, pending_chunks_size},
		{
			help,
			"The total size in bytes of stored pending and seeded chunks."
		}
	]),
	prometheus_gauge:new([
		{name, disk_pool_chunks_count},
		{
			help,
			"The approximate number of chunks in the disk pool."
			"The disk pool includes pending, recent, and orphaned chunks."
		}
	]),
	prometheus_counter:new([
		{name, disk_pool_processed_chunks},
		{
			help,
			"The counter is incremented every time the periodic process"
			" looks up a chunk from the disk pool and decides whether to"
			" remove it, include it in the weave, or keep in the disk pool."
		}
	]),

	%% Consensus.
	prometheus_gauge:new([
		{name, arweave_block_height},
		{help, "The block height."}
	]),
	prometheus_gauge:new([{name, block_time},
			{help, "The time in seconds between two blocks as recorded by the miners."}]),
	prometheus_gauge:new([
		{name, block_vdf_time},
		{help, "The number of the VDF steps between two consequent blocks."}
	]),
	prometheus_gauge:new([
		{name, block_vdf_advance},
		{help, "The number of the VDF steps a received block is ahead of our current step."}
	]),

	prometheus_counter:new([
		{name, wallet_list_size},
		{
			help,
			"The total number of wallets in the system."
		}
	]),
	prometheus_histogram:new([
		{name, block_pre_validation_time},
		{buckets, [infinity]}, %% we don't care about the histogram portion
		{help,
			"The time in milliseconds taken to parse the POST /block input and perform a "
			"preliminary validation before relaying the block to peers."}
	]),
	prometheus_histogram:new([
		{name, block_processing_time},
		{buckets, [infinity]}, %% we don't care about the histogram portion
		{help,
			"The time in seconds taken to validate the block and apply it on top of "
			"the current state, possibly involving a chain reorganisation."}
	]),
	prometheus_gauge:new([
		{name, synced_blocks},
		{
			help,
			"The total number of synced block headers."
		}
	]),

	%% Mining.
	prometheus_gauge:new([
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
	]),
	prometheus_gauge:new([
		{name, cm_h1_rate},
		{labels, [peer, direction]},
		{help, "The number of H1 hashes exchanged with a coordinated mining peer per second. "
				"The peer label indicates the peer that the value is exchanged with, and the "
				"direction label can be 'to' or 'from'."}
	]),
	prometheus_gauge:new([
		{name, cm_h2_count},
		{labels, [peer, direction]},
		{help, "The total number of H2 hashes exchanged with a coordinated mining peer. "
				"The peer label indicates the peer that the value is exchanged with, and the "
				"direction label can be 'to' or 'from'."}
	]),
	prometheus_gauge:new([
		{name, mining_server_chunk_cache_size},
		{labels, [partition, type]},
		{help, "The amount of data (measured in bytes) "
			"fetched during mining and not processed yet. "
		  "The type label can be 'total', 'reserved'."}
	]),
	prometheus_gauge:new([
		{name, mining_server_task_queue_len},
		{labels, [task]},
		{help, "The number of items in the mining server task queue."}
	]),
	prometheus_gauge:new([
		{name, mining_solution},
		{labels, [reason]},
		{help, "Incremented whenever the miner generates a solution. The 'reason' label "
				"will be 'success' if a block was successfully prepared from the solution, "
				"and will list a failure reason otherwise. Note: even if a block is "
				"successfully prepared from a solution, it does not necessarily mean "
				"the block ended up in the blockchain."}
	]),
	prometheus_histogram:new([
		{name, chunk_storage_sync_record_check_duration_milliseconds},
		{labels, [requested_chunk_count]},
		{buckets, [infinity]}, %% we don't care about the histogram portion
		{help, "The time in milliseconds it took to check the fetched chunk range "
				"is actually registered by the chunk storage."}
	]),
	prometheus_gauge:new([
		{name, mining_server_tasks},
		{labels, [task]},
		{help, "Incremented each time the mining server adds a task to the task queue."}
	]),
	prometheus_gauge:new([
		{name, mining_vdf_step},
		{help, "Incremented each time the mining server processes a VDF step."}
	]),
	%% VDF.
	prometheus_histogram:new([
		{name, vdf_step_time_milliseconds},
		{buckets, [infinity]}, %% we don't care about the histogram portion
		{labels, []},
		{help, "The time in milliseconds it took to compute a VDF step."}
	]),
	prometheus_gauge:new([
		{name, vdf_step},
		{help, "The current VDF step."}
	]),
	prometheus_gauge:new([
		{name, vdf_difficulty},
		{labels, [type]},
		{help, "The cached VDF difficulty. 'type' can be either 'current' or 'next'."}
	]),

	%% Economic metrics.
	prometheus_gauge:new([
		{name, average_network_hash_rate},
		{help, "The average network hash rate measured over the last ~30 days of blocks"}
	]),
	prometheus_gauge:new([
		{name, average_block_reward},
		{help, "The average block reward in Winston computed from the last ~30 days of blocks"}
	]),
	prometheus_gauge:new([
		{name, expected_block_reward},
		{help, "The block reward required to sustain 20 replicas of the present weave"
				" as currently estimated by the protocol."}
	]),
	prometheus_gauge:new([
		{name, network_data_size},
		{help, "Total size of the network data in bytes."}
	]),
	prometheus_gauge:new([
		{name, v2_price_per_gibibyte_minute},
		{help, "The price of storing 1 GiB for one minute as it will be calculated once the"
				" transition to the new pricing protocol is complete."}
	]),
	prometheus_gauge:new([
		{name, price_per_gibibyte_minute},
		{help, "The price of storing 1 GiB for one minute as currently estimated by "
				"the protocol."}
	]),
	prometheus_gauge:new([
		{name, legacy_price_per_gibibyte_minute},
		{help, "The price of storing 1 GiB for one minute as estimated by the previous ("
				"USD to AR benchmark-based) version of the protocol."}
	]),
	prometheus_gauge:new([
		{name, endowment_pool},
		{help, "The amount of Winston in the endowment pool."}
	]),
	prometheus_gauge:new([
		{name, kryder_plus_rate_multiplier},
		{help, "Kryder+ rate multiplier."}
	]),
	prometheus_gauge:new([
		{name, endowment_pool_take},
		{help, "Value we take from endowment pool to miner to compensate difference between expected and real reward."}
	]),
	prometheus_gauge:new([
		{name, endowment_pool_give},
		{help, "Value we give to endowment pool from transaction fees."}
	]),
	prometheus_gauge:new([
		{name, available_supply},
		{help, "The total supply minus the endowment, in Winston."}
	]),
	prometheus_gauge:new([
		{name, debt_supply},
		{help, "The amount of Winston emitted when the endowment pool was not sufficiently"
				" large to compensate mining."}
	]),
	prometheus_gauge:new([
		{name, poa_count},
		{labels, [chunks]},
		{help, "A count of the number of 1-chunk and 2-chunk blocks in the last 21,600 blocks. "
				"The 'chunks' label is 1 for the count of 1-chunk blocks, and 2 for the count of "
				"2-chunk blocks."}
	]),
	prometheus_gauge:new([
		{name, log_diff},
		{labels, [chunk]},
		{help, "The current linear difficulty converted to log scale. The chunk label "
				"is either 'poa1' or 'poa2'."}
	]),
	prometheus_gauge:new([
		{name, network_hashrate},
		{help, "An estimation of the network hash rate based on the mining difficulty "
				"of the latest block."}
	]),
	prometheus_gauge:new([
		{name, expected_minimum_200_years_storage_costs_decline_rate},
		{help, "The expected minimum decline rate sufficient to subsidize storage of "
				"the current weave for 200 years according to the legacy (2.5) estimations."}
	]),
	prometheus_gauge:new([
		{name, expected_minimum_200_years_storage_costs_decline_rate_10_usd_ar},
		{help, "The expected minimum decline rate sufficient to subsidize storage of "
				"the current weave for 200 years according to the legacy (2.6) estimations"
				"and assuming 10 $/AR."}
	]),

	%% Packing.
	prometheus_histogram:new([
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
	]),
	prometheus_counter:new([
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
	]),
	prometheus_counter:new([
		{name, validating_packed_spora},
		{labels, [packing]},
		{help, "The number of SPoRA solutions based on packed chunks entered validation. "
				"The packing label can be 'spora_2_5', 'spora_2_6', or 'replica_2_9'."}
	]),

	prometheus_gauge:new([{name, packing_buffer_size},
		{help, "The number of chunks in the packing server queue."}]),
	prometheus_gauge:new([{name, chunk_cache_size},
			{help, "The number of chunks scheduled for downloading."}]),
	prometheus_counter:new([{name, chunks_stored},
		{labels, [packing, store_id]},
		{help, "The counter is incremented every time a chunk is written to "
				"chunk_storage."}]),
	prometheus_counter:new([{name, chunks_read},
		{labels, [store_id]},
		{help, "The counter is incremented every time a chunk is read from "
				"chunk_storage."}]),
	prometheus_histogram:new([
		{name, chunk_read_rate_bytes_per_second},
		{labels, [store_id, type]},
		{buckets, [infinity]}, %% we don't care about the histogram portion
		{help, "The rate, in bytes per second, at which chunks are read from storage. "
				"The type label can be 'raw' or 'repack'."}
	]),
	prometheus_histogram:new([
		{name, chunk_write_rate_bytes_per_second},
		{labels, [store_id, type]},
		{buckets, [infinity]}, %% we don't care about the histogram portion
		{help, "The rate, in bytes per second, at which chunks are written to storage."}
	]),

	prometheus_gauge:new([{name, data_discovery},
		{labels, [type, store_id, stat]},
		{help, "Tracks peer availability statistics from data discovery. "
				"'type' is 'normal' or 'footprint'. "
				"'stat' is 'num_peers' - distinct peers offering data "
				"anywhere in this store_id's range."}]),

	prometheus_gauge:new([{name, peer_interval_cache_size},
		{labels, [unit]},
		{help, "Size of ar_data_discovery's peer interval cache "
				"(per-(peer, window, mode) entries). 'unit' is 'rows' "
				"(row count) or 'bytes' (ets:info memory * wordsize, "
				"capped by ?MAX_INTERVAL_CACHE_BYTES). Sustained pressure "
				"near the byte cap indicates the cap should be raised or "
				"peer scanning is thrashing."}]),

	prometheus_counter:new([{name, peer_interval_cache_evictions},
		{labels, [reason]},
		{help, "Cumulative rows evicted from ar_data_discovery's peer "
				"interval cache. 'reason' is 'trim' (LRU cap fired) or "
				"'peer_removed' (whole-peer wipe on remove_peer)."}]),

	prometheus_counter:new([{name, sync_tasks},
		{labels, [state, peer]},
		{help, "Sync task counters per peer. queued_in/queued_out track "
				"every queue add/remove (queue depth = queued_in - "
				"queued_out). Other states are flavor: dispatched, "
				"completed, activate_footprint, deactivate_footprint, "
				"rebalance_cut, reaped, dropped_unavailable."}]),

	prometheus_counter:new([{name, sync_chunks_skipped},
		{labels, [reason]},
		{help, "The number of chunks skipped during syncing."}]),

	prometheus_gauge:new([{name, device_lock_status},
		{labels, [store_id, mode]},
		{help, "The device lock status of the storage module. "
				"-1: off, 0: paused, 1: active, 2: complete -2: unknown"}]),
	prometheus_gauge:new([{name, sync_task_queue_inflight_bytes},
		{labels, [store_id]},
		{help, "Total bytes in the per-StoreID sync_task_queue's "
				"inflight_intervals (ranges ar_peer_sync has pushed to "
				"ar_sync_dispatcher and not yet released, used by its dedup "
				"gate). Climbing without bound flags a dedup-overlay leak."}]),

	prometheus_gauge:new([{name, repack_chunk_states},
		{labels, [store_id, type, state]},
		{help, "The count of chunks in each state. 'type' can be 'cache' or 'queue'."}]),


	%% ---------------------------------------------------------------------------------------
	%% Replica 2.9 metrics
	%% ---------------------------------------------------------------------------------------
	prometheus_counter:new([{name, replica_2_9_entropy_stored},
		{labels, [store_id]},
		{help, "The number of bytes of replica.2.9 entropy written to chunk storage."}]),
	prometheus_counter:new([{name, replica_2_9_entropy_generated},
		{help, "The number of bytes of replica.2.9 entropy generated."}]),
	prometheus_gauge:new([{name, replica_2_9_entropy_cache},
		{help, "The size (in bytes) of the replica.2.9 entropy cache."}]),
	prometheus_counter:new([{name, replica_2_9_entropy_stats},
		{labels, [partition, stat]},
		{help, "Count of different replica_2_9 entropy events: 'cache_hit', 'cache_miss', "
			   "'redundant'."}]),
	prometheus_histogram:new([
		{name, replica_2_9_entropy_duration_milliseconds},
		{buckets, [infinity]}, %% we don't care about the histogram portion
		{help, "The time, in milliseconds, to generate 256 MiB of replica.2.9 entropy."}
	]),

	%% ---------------------------------------------------------------------------------------
	%% Pool related metrics
	%% ---------------------------------------------------------------------------------------
	prometheus_counter:new([
		{name, pool_job_request_count},
		{help, "The number of requests to pool /job from start of arweave node"}
	]),

	prometheus_counter:new([
		{name, pool_total_job_got_count},
		{help, "The number of jobs received from /job requests."}
	]),

	%% ---------------------------------------------------------------------------------------
	%% Debug-only metrics
	%% ---------------------------------------------------------------------------------------
	prometheus_counter:new([{name, process_functions},
			{labels, [process]},
			{help, "Sampling active functions. The 'process' label is a fully qualified "
					"function name with the format 'process~module:function/arith'. "
					"Only set when debug=true."}]),
	%% process_info gets unregistered and re-registered in ar_process_sampler.erl
	prometheus_gauge:new([{name, process_info},
			{labels, [process, type]},
			{help, "Sampling info about active processes. Only set when debug=true."}]),
	prometheus_gauge:new([{name, scheduler_utilization},
			{labels, [type]},
			{help, "Average scheduler utilization. `type` maps to the sched_type as defined here: "
				"https://www.erlang.org/doc/man/scheduler#type-sched_util_result. "
				"Only set when debug=true."}]),
	prometheus_gauge:new([{name, allocator},
			{labels, [type, instance, section, metric]},
			{help, "Erlang VM memory allocator metrics. Only set when debug=true."}]).

record_rate_metric(StartTime, Bytes, Metric, Labels) ->
	EndTime = erlang:monotonic_time(),
	ElapsedTime =
		erlang:convert_time_unit(EndTime - StartTime,
								native,
								microsecond),
	%% bytes per second
	Rate =
		case ElapsedTime > 0 of
			true -> 1_000_000 * Bytes / ElapsedTime;
			false -> 0
		end,
	histogram_observe(Metric, Labels, Rate).


%% @doc Return the HTTP status class label for cowboy_requests_total and gun_requests_total
%% metrics.
get_status_class({ok, {{Status, _}, _, _, _, _}}) ->
	get_status_class(Status);
get_status_class({error, connection_closed}) ->
	"connection_closed";
get_status_class({error, connect_timeout}) ->
	"connect_timeout";
get_status_class({error, timeout}) ->
	"timeout";
get_status_class({error,{shutdown,timeout}}) ->
	"shutdown_timeout";
get_status_class({error, econnrefused}) ->
	"econnrefused";
get_status_class({error, {shutdown,econnrefused}}) ->
	"shutdown_econnrefused";
get_status_class({error, {shutdown,ehostunreach}}) ->
	"shutdown_ehostunreach";
get_status_class({error, {shutdown,normal}}) ->
	"shutdown_normal";
get_status_class({error, {closed,_}}) ->
	"closed";
get_status_class({error, noproc}) ->
	"noproc";
get_status_class({error, {down,_}}) ->
	"down";
get_status_class({error, {stream_error,_}}) ->
	"stream_error";
get_status_class({error, client_error}) ->
	"client_error";
get_status_class(Data) when is_integer(Data), Data > 0 ->
	integer_to_list(Data);
get_status_class(Data) when is_binary(Data) ->
	case catch binary_to_integer(Data) of
		{_, _} ->
			?LOG_DEBUG([{event, unknown_status}, {status, Data}]),
			"unknown";
		Status ->
			get_status_class(Status)
	end;
get_status_class(Data) when is_atom(Data) ->
	atom_to_list(Data);
get_status_class(Data) ->
	?LOG_DEBUG([{event, unknown_status}, {status, Data}]),
	"unknown".

%%%===================================================================
%%% Safe metric helpers.
%%%
%%% Error-swallowing wrappers around the prometheus runtime API for all
%%% runtime metric writes/reads. The prometheus_* ETS tables can be
%%% transiently absent while the node (re)starts or stops, and an unguarded
%%% crash in a periodic gen_server write can cascade to
%%% reached_max_restart_intensity and halt the BEAM. Metric declarations
%%% (prometheus_*:new/declare) are NOT wrapped — a failed declaration is a bug.
%%%===================================================================

gauge_set(Name, Value) ->
	try prometheus_gauge:set(Name, Value) catch _:_ -> ok end.
gauge_set(Name, Labels, Value) ->
	try prometheus_gauge:set(Name, Labels, Value) catch _:_ -> ok end.

gauge_inc(Name) ->
	try prometheus_gauge:inc(Name) catch _:_ -> ok end.
gauge_inc(Name, Value) ->
	try prometheus_gauge:inc(Name, Value) catch _:_ -> ok end.
gauge_inc(Name, Labels, Value) ->
	try prometheus_gauge:inc(Name, Labels, Value) catch _:_ -> ok end.

gauge_dec(Name) ->
	try prometheus_gauge:dec(Name) catch _:_ -> ok end.
gauge_dec(Name, Value) ->
	try prometheus_gauge:dec(Name, Value) catch _:_ -> ok end.
gauge_dec(Name, Labels, Value) ->
	try prometheus_gauge:dec(Name, Labels, Value) catch _:_ -> ok end.

gauge_deregister(Name) ->
	try prometheus_gauge:deregister(Name) catch _:_ -> ok end.

gauge_value(Name) ->
	try prometheus_gauge:value(Name) catch _:_ -> undefined end.
gauge_value(Name, Labels) ->
	try prometheus_gauge:value(Name, Labels) catch _:_ -> undefined end.

counter_inc(Name) ->
	try prometheus_counter:inc(Name) catch _:_ -> ok end.
counter_inc(Name, Value) ->
	try prometheus_counter:inc(Name, Value) catch _:_ -> ok end.
counter_inc(Name, Labels, Value) ->
	try prometheus_counter:inc(Name, Labels, Value) catch _:_ -> ok end.

histogram_observe(Name, Value) ->
	try prometheus_histogram:observe(Name, Value) catch _:_ -> ok end.
histogram_observe(Name, Labels, Value) ->
	try prometheus_histogram:observe(Name, Labels, Value) catch _:_ -> ok end.




cleanup() ->
	%% App info
	prometheus_gauge:deregister(arweave_release),

	%% Networking.
	prometheus_counter:deregister(http_server_accepted_bytes_total),
	prometheus_counter:deregister(http_server_served_bytes_total),
	prometheus_counter:deregister(http_client_downloaded_bytes_total),
	prometheus_counter:deregister(http_client_uploaded_bytes_total),
	prometheus_gauge:deregister(arweave_peer_count),
	prometheus_counter:deregister(gun_requests_total),
	prometheus_histogram:deregister(ar_http_request_duration_seconds),
	prometheus_histogram:deregister(http_client_get_chunk_duration_seconds),
	prometheus_gauge:deregister(downloader_queue_size),
	prometheus_gauge:deregister(outbound_connections),

	%% Transaction and block propagation.
	prometheus_gauge:deregister(tx_queue_size),
	prometheus_counter:deregister(propagated_transactions_total),
	prometheus_histogram:declare(tx_propagation_bits_per_second),
	prometheus_gauge:deregister(mempool_header_size_bytes),
	prometheus_gauge:deregister(mempool_data_size_bytes),
	prometheus_counter:deregister(block_announcement_missing_transactions),
	prometheus_counter:deregister(block_announcement_reported_transactions),
	prometheus_counter:deregister(block2_received_transactions),
	prometheus_counter:deregister(block_announcement_missing_chunks),
	prometheus_counter:deregister(block_announcement_reported_chunks),
	prometheus_counter:deregister(block2_fetched_chunks),
	prometheus_histogram:deregister(ar_mempool_add_tx_duration_milliseconds),
	prometheus_histogram:deregister(reverify_mempool_chunk_duration_milliseconds),
	prometheus_histogram:deregister(drop_txs_duration_milliseconds),
	prometheus_histogram:deregister(del_from_propagation_queue_duration_milliseconds),

	%% Data seeding.
	prometheus_gauge:deregister(weave_size),
	prometheus_gauge:deregister(v2_index_data_size),
	prometheus_gauge:deregister(v2_index_data_size_by_packing),
	prometheus_gauge:deregister(tip_partition_data_size_by_packing),

	%% Disk pool.
	prometheus_gauge:deregister(pending_chunks_size),
	prometheus_gauge:deregister(disk_pool_chunks_count),
	prometheus_counter:deregister(disk_pool_processed_chunks),

	%% Consensus.
	prometheus_gauge:deregister(arweave_block_height),
	prometheus_gauge:deregister(block_time),
	prometheus_gauge:deregister(block_vdf_time),
	prometheus_gauge:deregister(block_vdf_advance),

	prometheus_counter:deregister(wallet_list_size),
	prometheus_histogram:deregister(block_pre_validation_time),
	prometheus_histogram:deregister(block_processing_time),
	prometheus_gauge:deregister(synced_blocks),

	%% Mining.
	prometheus_gauge:deregister(mining_rate),
	prometheus_gauge:deregister(cm_h1_rate),
	prometheus_gauge:deregister(cm_h2_count),
	prometheus_gauge:deregister(mining_server_chunk_cache_size),
	prometheus_gauge:deregister(mining_server_task_queue_len),
	prometheus_gauge:deregister(mining_solution),
	prometheus_histogram:deregister(chunk_storage_sync_record_check_duration_milliseconds),
	prometheus_gauge:deregister(mining_server_tasks),
	prometheus_gauge:deregister(mining_vdf_step),

	%% VDF.
	prometheus_histogram:deregister(vdf_step_time_milliseconds),
	prometheus_gauge:deregister(vdf_step),
	prometheus_gauge:deregister(vdf_difficulty),

	%% Economic metrics.
	prometheus_gauge:deregister(average_network_hash_rate),
	prometheus_gauge:deregister(average_block_reward),
	prometheus_gauge:deregister(expected_block_reward),
	prometheus_gauge:deregister(network_data_size),
	prometheus_gauge:deregister(v2_price_per_gibibyte_minute),
	prometheus_gauge:deregister(price_per_gibibyte_minute),
	prometheus_gauge:deregister(legacy_price_per_gibibyte_minute),
	prometheus_gauge:deregister(endowment_pool),
	prometheus_gauge:deregister(kryder_plus_rate_multiplier),
	prometheus_gauge:deregister(endowment_pool_take),
	prometheus_gauge:deregister(endowment_pool_give),
	prometheus_gauge:deregister(available_supply),
	prometheus_gauge:deregister(debt_supply),
	prometheus_gauge:deregister(poa_count),
	prometheus_gauge:deregister(log_diff),
	prometheus_gauge:deregister(network_hashrate),
	prometheus_gauge:deregister(expected_minimum_200_years_storage_costs_decline_rate),
	prometheus_gauge:deregister(expected_minimum_200_years_storage_costs_decline_rate_10_usd_ar),

	%% Packing.
	prometheus_histogram:deregister(packing_duration_milliseconds),
	prometheus_counter:deregister(packing_requests),
	prometheus_counter:deregister(validating_packed_spora),
	prometheus_gauge:deregister(packing_buffer_size),
	prometheus_gauge:deregister(chunk_cache_size),
	prometheus_counter:deregister(chunks_stored),
	prometheus_counter:deregister(chunks_read),
	prometheus_histogram:deregister(chunk_read_rate_bytes_per_second),
	prometheus_histogram:deregister(chunk_write_rate_bytes_per_second),
	prometheus_gauge:deregister(data_discovery),
	prometheus_gauge:deregister(peer_interval_cache_size),
	prometheus_counter:deregister(peer_interval_cache_evictions),
	prometheus_counter:deregister(sync_tasks),
	prometheus_counter:deregister(sync_chunks_skipped),
	prometheus_gauge:deregister(device_lock_status),
	prometheus_gauge:deregister(sync_task_queue_inflight_bytes),
	prometheus_gauge:deregister(repack_chunk_states),

	%% ---------------------------------------------------------------------------------------
	%% Replica 2.9 metrics
	%% ---------------------------------------------------------------------------------------
	prometheus_counter:deregister(replica_2_9_entropy_stored),
	prometheus_counter:deregister(replica_2_9_entropy_generated),
	prometheus_gauge:deregister(replica_2_9_entropy_cache),
	prometheus_counter:deregister(replica_2_9_entropy_stats),
	prometheus_histogram:deregister(replica_2_9_entropy_duration_milliseconds),

	%% Pool related metrics
	prometheus_counter:deregister(pool_job_request_count),

	prometheus_counter:deregister(pool_total_job_got_count),
	%% Debug-only metrics
	prometheus_counter:deregister(process_functions),
	%% process_info gets unregistered and re-registered in ar_process_sampler.erl
	prometheus_gauge:deregister(process_info),
	prometheus_gauge:deregister(scheduler_utilization),
	prometheus_gauge:deregister(allocator),
	ok.
	
