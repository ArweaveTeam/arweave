-module(ar_metrics).

-include("ar.hrl").

-export([register/0, get_status_class/1, record_rate_metric/4]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

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
	prometheus_gauge:new([
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
		{name, fixed_broken_chunk_storage_records},
		{help, "The number of fixed broken chunk storage records detected when "
				"reading a range of chunks."}
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
				"to. In all cases its value can be 'spora_2_5', 'spora_2_6', 'composite', "
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
				"'spora_2_5', 'spora_2_6', 'composite', or 'replica_2_9'."}
	]),
	prometheus_counter:new([
		{name, validating_packed_spora},
		{labels, [packing]},
		{help, "The number of SPoRA solutions based on packed chunks entered validation. "
				"The packing label can be 'spora_2_5', 'spora_2_6', 'composite', "
				" or replica_2_9."}
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

	prometheus_gauge:new([{name, sync_tasks},
		{labels, [state, type, peer]},
		{help, "The number of syncing tasks. 'state' can be 'queued' or 'scheduled'. "
				"'type' can be 'sync_range' or 'read_range'. 'peer' is the peer the task "
				"is intended for - for 'read_range' tasks this will be 'localhost'."}]),

	prometheus_gauge:new([{name, device_lock_status},
		{labels, [store_id, mode]},
		{help, "The device lock status of the storage module. "
				"-1: off, 0: paused, 1: active, 2: complete -2: unknown"}]),
	prometheus_gauge:new([{name, sync_intervals_queue_size},
		{labels, [store_id]},
		{help, "The size of the syncing intervals queue."}]),

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
	prometheus_histogram:observe(Metric, Labels, Rate).


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
get_status_class(208) ->
	"already_processed";
get_status_class(418) ->
	"missing_transactions";
get_status_class(419) ->
	"missing_chunk";
get_status_class(Data) when is_integer(Data), Data > 0 ->
	integer_to_list(Data);
get_status_class(Data) when is_binary(Data) ->
	case catch binary_to_integer(Data) of
		{_, _} ->
			"unknown";
		Status ->
			get_status_class(Status)
	end;
get_status_class(Data) when is_atom(Data) ->
	atom_to_list(Data);
get_status_class(_) ->
	"unknown".
