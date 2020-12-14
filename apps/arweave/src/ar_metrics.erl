-module(ar_metrics).

-export([
	register/0,
	store/1,
	label_http_path/1,
	get_status_class/1
]).

-include("ar.hrl").

register() ->
	filelib:ensure_dir(ar_meta_db:get(metrics_dir) ++ "/"),
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
	prometheus_histogram:new([
		{name, fork_recovery_depth},
		{buckets, lists:seq(1, 50)},
		{help, "Fork recovery depth metric"}
	]),
	prometheus_gauge:new([
		{name, arweave_block_height},
		{help, "Block height"}
	]),
	prometheus_gauge:new([
		{name, arweave_peer_count},
		{help, "peer count"}
	]),
	prometheus_gauge:new([
		{name, downloader_queue_size},
		{help, "The size of the downloader queue"}
	]),
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
	prometheus_counter:new([
		{name, gun_requests_total},
		{labels, [http_method, route, status_class]},
		{
			help,
			"The total number of GUN requests."
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
	prometheus_gauge:new([
		{name, weave_size},
		{
			help,
			"The size of the weave (in bytes)."
		}
	]),
	prometheus_gauge:new([
		{name, v2_index_data_size},
		{
			help,
			"The size (in bytes) of the data stored and indexed in 2.1 chunk index."
		}
	]),
	prometheus_gauge:new([
		{name, pending_chunks_size},
		{
			help,
			"The total size in bytes of stored pending chunks."
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
	load_gauge(disk_pool_chunks_count),
	prometheus_counter:new([
		{name, disk_pool_processed_chunks},
		{
			help,
			"The counter is incremented every time the periodic process"
			" looks up a chunk from the disk pool and decides whether to"
			" remove it, include it in the weave, or keep in the disk pool."
		}
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
	prometheus_histogram:new([
		{name, sqlite_query_time},
		{buckets, [1, 10, 100, 500, 1000, 2000, 10000, 30000]},
		{labels, [query_type]},
		{help, "The time in milliseconds of SQLite queries."}
	]).

load_gauge(Name) ->
	case ar_storage:read_term(ar_meta_db:get(metrics_dir), Name) of
		{ok, Value} ->
			prometheus_gauge:set(Name, Value);
		not_found ->
			nothing_is_stored;
		{error, Reason} ->
			ar:err([{event, failed_to_load_metric}, {error, Reason}])
	end.

store(Name) ->
	ar_storage:write_term(ar_meta_db:get(metrics_dir), Name, prometheus_gauge:value(Name)).

label_http_path(Path) ->
	name_route(split_path(Path)).

split_path(Path) ->
	lists:filter(
		fun(C) -> C /= <<>> end,
		string:split(Path, <<"/">>, all)
	).

name_route([]) ->
	"/";
name_route([<<"info">>]) ->
	"/info";
name_route([<<"block">>]) ->
	"/block";
name_route([<<"tx">>]) ->
	"/tx";
name_route([<<"tx_anchor">>]) ->
	"/tx_anchor";
name_route([<<"peer">>|_]) ->
	"/peer/...";
name_route([<<"arql">>]) ->
	"/arql";
name_route([<<"time">>]) ->
	"/time";
name_route([<<"tx">>, <<"pending">>]) ->
	"/tx/pending";
name_route([<<"tx">>, _Hash, <<"status">>]) ->
	"/tx/{hash}/status";
name_route([<<"tx">>, _Hash]) ->
	"/tx/{hash}";
name_route([<<"tx">>, _Hash, << "data" >>]) ->
	"/tx/{hash}/data";
name_route([<<"tx">>, _Hash, << "data.", _/binary >>]) ->
	"/tx/{hash}/data.{ext}";
name_route([<<"tx">>, _Hash, << "offset" >>]) ->
	"/tx/{hash}/offset";
name_route([<<"chunk">>, _Offset]) ->
	"/chunk/{offset}";
name_route([<<"chunk">>]) ->
	"/chunk";
name_route([<<"data_sync_record">>]) ->
	"/data_sync_record";
name_route([<<"wallet">>]) ->
	"/wallet";
name_route([<<"unsigned_tx">>]) ->
	"/unsigned_tx";
name_route([<<"peers">>]) ->
	"/peers";
name_route([<<"price">>, _SizeInBytes]) ->
	"/price/{bytes}";
name_route([<<"price">>, _SizeInBytes, _Addr]) ->
	"/price/{bytes}/{address}";
name_route([<<"hash_list">>]) ->
	"/hash_list";
name_route([<<"block_index">>]) ->
	"/block_index";
name_route([<<"wallet_list">>]) ->
	"/wallet_list";
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
name_route([<<"block">>, <<"hash">>, _IndepHash]) ->
	"/block/hash/{indep_hash}";
name_route([<<"block">>, <<"height">>, _Height]) ->
	"/block/height/{height}";
name_route([<<"block">>, _Type, _IDBin, _Field]) ->
	"/block/{type}/{id_bin}/{field}";
name_route([<<"block">>, <<"current">>]) ->
	"/block/current";
name_route([<<"current_block">>]) ->
	"/current/block";
name_route([<<"services">>]) ->
	"/services";
name_route([<<"tx">>, _Hash, _Field]) ->
	"/tx/hash/field";
name_route([<<"height">>]) ->
	"/height";
name_route([<<_Hash:43/binary, _MaybeExt/binary>>]) ->
	"/{hash}[.{ext}]";
name_route([<<"metrics">>]) ->
	"/metrics";
name_route(_) ->
	undefined.

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
