-module(ar_metrics).

-export([register/1, store/1, label_http_path/1, get_status_class/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-define(ENDPOINTS, ["info", "block", "block_announcement", "block2", "tx", "tx2",
		"queue", "recent_hash_list", "recent_hash_list_diff" "tx_anchor", "arql", "time",
		"chunk", "chunk2", "data_sync_record", "sync_buckets", "wallet", "unsigned_tx",
		"peers", "hash_list", "block_index", "block_index2", "wallet_list", "height",
		"metrics"]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Declare Arweave metrics.
register(MetricsDir) ->
	filelib:ensure_dir(MetricsDir ++ "/"),
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
	prometheus_gauge:new([
		{name, downloader_queue_size},
		{help, "The size of the back-off queue for the block and transaction headers "
				"the node failed to sync and will retry later."}
	]),

	%% SQLite.
	prometheus_histogram:new([
		{name, sqlite_query_time},
		{buckets, [1, 10, 100, 500, 1000, 2000, 10000, 30000]},
		{labels, [query_type]},
		{help, "The time in milliseconds of SQLite queries."}
	]),

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
		{buckets, [10, 100, 1000, 100000, 1000000, 100000000, 1000000000]},
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

	%% Data seeding.
	prometheus_gauge:new([
		{name, weave_size},
		{help, "The size of the weave (in bytes)."}
	]),
	prometheus_gauge:new([
		{name, v2_index_data_size},
		{help, "The size (in bytes) of the data stored and indexed."}
	]),
	prometheus_gauge:new([
		{name, v2_index_data_size_by_packing},
		{labels, [store_id, packing]},
		{help, "The size (in bytes) of the data stored and indexed. Groupped by "
				"store ID and packing."}
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
		{help, "Block height"}
	]),
	prometheus_gauge:new([{name, block_time}, {help, "Block time"}]),
	prometheus_histogram:new([
		{name, fork_recovery_depth},
		{buckets, lists:seq(1, 50)},
		{help, "Fork recovery depth metric"}
	]),
	prometheus_histogram:new([
		{name, block_construction_time_milliseconds},
		{buckets, [1, 10, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 2000, 10000, 30000]},
		{help, "The time it takes to pick and validate transactions for a block and generate"
				" a preimage to use in mining."}
	]),
	prometheus_gauge:new([
		{name, wallet_list_size},
		{
			help,
			"The total number of wallets in the system."
		}
	]),
	prometheus_histogram:new([
		{name, block_pre_validation_time},
		{buckets, [0.1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 50, 100, 1000, 2000, 5000, 10000]},
		{help,
			"The time in milliseconds taken to parse the POST /block input and perform a "
			"preliminary validation before relaying the block to peers."}
	]),
	prometheus_histogram:new([
		{name, block_processing_time},
		{buckets, [0.1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60]},
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
	prometheus_histogram:new([
		{name, mining_rate},
		{buckets,
			[1, 2, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 1000, 2000, 5000, 10000, 20000]},
		{help,
			"The per second average rate of the number of tried solution candidates "
			"computed over the last block time."}
	]),
	prometheus_gauge:new([
		{name, mining_server_chunk_cache_size},
		{help, "The number of chunks fetched during mining and not processed yet."}
	]),
	prometheus_histogram:new([
		{name, vdf_step_time_milliseconds},
		{buckets, [100, 250, 500, 750, 1000, 1250, 1500, 1750, 2000, 2500, 3000, 3500, 4000,
				4500, 5000, 5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000, 15000,
				20000, 30000]},
		{labels, []},
		{help, "The time in milliseconds it took to compute a VDF step."}
	]),
	prometheus_gauge:new([
		{name, network_hashrate},
		{help, "An estimation of the network hashrate based on the mining difficulty."}
	]),
	prometheus_gauge:new([
		{name, network_burden},
		{help, "The estimated cost of storing the current weave assuming the 0.5% storage "
				"costs decline rate, in Winston."}
	]),
	prometheus_gauge:new([
		{name, network_burden_10_usd_ar},
		{help, "The estimated cost of storing the current weave assuming the 0.5% storage "
				"costs decline rate and 10 $/AR, in Winston."}
	]),
	prometheus_gauge:new([
		{name, endowment_pool},
		{help, "The amount of Winston in the endowment pool."}
	]),
	prometheus_gauge:new([
		{name, network_burden_200_years},
		{help, "The estimated cost of storing the current weave for 200 years assuming the "
				"0.5% storage costs decline rate, in Winston."}
	]),
	prometheus_gauge:new([
		{name, network_burden_200_years_10_usd_ar},
		{help, "The estimated cost of storing the current weave for 200 years assuming the "
				"0.5% storage costs decline rate and 10 $/AR, in Winston."}
	]),
	prometheus_gauge:new([
		{name, expected_minimum_200_years_storage_costs_decline_rate},
		{help, "The expected minimum decline rate sufficient to subsidize storage of "
				"the current weave for 200 years."}
	]),
	prometheus_gauge:new([
		{name, expected_minimum_200_years_storage_costs_decline_rate_10_usd_ar},
		{help, "The expected minimum decline rate sufficient to subsidize storage of "
				"the current weave for 200 years, assuming 10 $/AR."}
	]),

	%% Packing.
	prometheus_histogram:new([
		{name, packing_duration_milliseconds},
		{labels, [type, trigger]},
		{buckets, lists:seq(1, 200)},
		{help, "The packing/unpacking time in milliseconds. The type label distinguishes"
				"packing from unpacking. The trigger label shows whether packing was triggered"
				"externally (an HTTP request) or internally (during syncing or repacking)."}
	]),
	prometheus_counter:new([
		{name, validating_packed_spora},
		{help, "The number of SPoRA solutions based on packed chunks entered validation."}
	]),
	prometheus_counter:new([
		{name, validating_unpacked_spora},
		{help, "The number of SPoRA solutions based on unpacked chunks entered validation."}
	]),
	prometheus_counter:new([
		{name, validating_packed_2_6_spora},
		{help, "The number of SPoRA solutions based on chunks packed for 2.6 entered "
				"validation."}
	]),

	prometheus_gauge:new([{name, packing_buffer_size},
			{help, "The number of chunks in the packing server queue."}]),
	prometheus_gauge:new([{name, sync_buffer_size}, {labels, [store_id]},
			{help, "The number of chunks scheduled for downloading."}]).

%% @doc Store the given metric in a file.
store(Name) ->
	{ok, Config} = application:get_env(arweave, config),
	ar_storage:write_term(Config#config.metrics_dir, Name, prometheus_gauge:value(Name)).

%% @doc Return the HTTP path label for cowboy_requests_total and gun_requests_total metrics.
label_http_path(Path) ->
	name_route(split_path(Path)).

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
get_status_class({error, econnrefused}) ->
	"econnrefused";
get_status_class(208) ->
	"already_processed";
get_status_class(418) ->
	"missing_transactions";
get_status_class(419) ->
	"missing_chunk";
get_status_class(Data) when is_integer(Data), Data > 0 ->
	prometheus_http:status_class(Data);
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

%%%===================================================================
%%% Private functions.
%%%===================================================================

split_path(Path) ->
	lists:filter(
		fun(C) -> C /= <<>> end,
		string:split(Path, <<"/">>, all)
	).

name_route([]) ->
	"/";
name_route([<<"current_block">>]) ->
	"/current/block";
name_route([<<_Hash:43/binary, _MaybeExt/binary>>]) ->
	"/{hash}[.{ext}]";
name_route([Bin]) ->
	L = binary_to_list(Bin),
	case lists:member(L, ?ENDPOINTS) of
		true ->
			"/" ++ L;
		false ->
			undefined
	end;
name_route([<<"peer">> | _]) ->
	"/peer/...";

name_route([<<"tx">>, <<"pending">>]) ->
	"/tx/pending";
name_route([<<"tx">>, _Hash, <<"status">>]) ->
	"/tx/{hash}/status";
name_route([<<"tx">>, _Hash]) ->
	"/tx/{hash}";
name_route([<<"tx2">>, _Hash]) ->
	"/tx2/{hash}";
name_route([<<"unconfirmed_tx">>, _Hash]) ->
	"/unconfirmed_tx/{hash}";
name_route([<<"unconfirmed_tx2">>, _Hash]) ->
	"/unconfirmed_tx2/{hash}";
name_route([<<"tx">>, _Hash, << "data" >>]) ->
	"/tx/{hash}/data";
name_route([<<"tx">>, _Hash, << "data.", _/binary >>]) ->
	"/tx/{hash}/data.{ext}";
name_route([<<"tx">>, _Hash, << "offset" >>]) ->
	"/tx/{hash}/offset";
name_route([<<"tx">>, _Hash, _Field]) ->
	"/tx/hash/field";

name_route([<<"chunk">>, _Offset]) ->
	"/chunk/{offset}";
name_route([<<"chunk2">>, _Offset]) ->
	"/chunk2/{offset}";

name_route([<<"data_sync_record">>, _Start, _Limit]) ->
	"/data_sync_record/{start}/{limit}";

name_route([<<"price">>, _SizeInBytes]) ->
	"/price/{bytes}";
name_route([<<"price">>, _SizeInBytes, _Addr]) ->
	"/price/{bytes}/{address}";

name_route([<<"wallet">>, _Addr, <<"balance">>]) ->
	"/wallet/{addr}/balance";
name_route([<<"wallet">>, _Addr, <<"last_tx">>]) ->
	"/wallet/{addr}/last_tx";
name_route([<<"wallet">>, _Addr, <<"txs">>]) ->
	"/wallet/{addr}/txs";
name_route([<<"wallet">>, _Addr, <<"txs">>, _EarliestTX]) ->
	"/wallet/{addr}/txs/{earliest_tx}";
name_route([<<"wallet">>, _Addr, <<"deposits">>]) ->
	"/wallet/{addr}/deposits";
name_route([<<"wallet">>, _Addr, <<"deposits">>, _EarliestDeposit]) ->
	"/wallet/{addr}/deposits/{earliest_deposit}";

name_route([<<"wallet_list">>, _Root]) ->
	"/wallet_list/{root_hash}";
name_route([<<"wallet_list">>, _Root, _Cursor]) ->
	"/wallet_list/{root_hash}/{cursor}";
name_route([<<"wallet_list">>, _Root, _Addr, <<"balance">>]) ->
	"/wallet_list/{root_hash}/{addr}/balance";

name_route([<<"block_index">>, _From, _To]) ->
	"/block_index/{from}/{to}";

name_route([<<"block">>, <<"hash">>, _IndepHash]) ->
	"/block/hash/{indep_hash}";
name_route([<<"block">>, <<"height">>, _Height]) ->
	"/block/height/{height}";
name_route([<<"block2">>, <<"hash">>, _IndepHash]) ->
	"/block2/hash/{indep_hash}";
name_route([<<"block2">>, <<"height">>, _Height]) ->
	"/block2/height/{height}";
name_route([<<"block">>, _Type, _IDBin, _Field]) ->
	"/block/{type}/{id_bin}/{field}";
name_route([<<"block">>, <<"current">>]) ->
	"/block/current";

name_route(_) ->
	undefined.
