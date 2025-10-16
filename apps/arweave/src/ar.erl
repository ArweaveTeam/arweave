%%%
%%% @doc Arweave server entrypoint and basic utilities.
%%%
-module(ar).

-behaviour(application).

-export([main/0, main/1, create_wallet/0, create_wallet/1,
		create_ecdsa_wallet/0, create_ecdsa_wallet/1,
		benchmark_packing/1, benchmark_packing/0, benchmark_2_9/0, benchmark_2_9/1,
		benchmark_vdf/0, benchmark_vdf/1,
		benchmark_hash/1, benchmark_hash/0, start/0,
		start/1, start/2, stop/1, stop_dependencies/0, start_dependencies/0,
		tests/0, tests/1, tests/2, e2e/0, e2e/1, shell/0, stop_shell/0,
		docs/0, shutdown/1, console/1, console/2, prep_stop/1]).

-include("ar.hrl").
-include("ar_consensus.hrl").
-include("ar_verify_chunks.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include_lib("eunit/include/eunit.hrl").

%% Supported feature flags (default behaviour)
% http_logging (false)
% disk_logging (false)
% miner_logging (true)
% subfield_queries (false)
% blacklist (true)
% time_syncing (true)

%% @doc Command line program entrypoint. Takes a list of arguments.
main() ->
	show_help().

main("") ->
	show_help();
main(Args) ->
	arweave_config:start(),
	ConfigFile = parse_config_file(Args, [], #config{}),
	start(ConfigFile).

show_help() ->
	io:format("Usage: arweave-server [options]~n"),
	io:format("Compatible with network: ~s~n", [?NETWORK_NAME]),
	io:format("Options:~n"),
	lists:foreach(
		fun({Opt, Desc}) ->
			io:format("\t~s~s~n",
				[
					string:pad(Opt, 40, trailing, $ ),
					Desc
				]
			)
		end,
		[
			{"config_file (path)", io_lib:format("Load the configuration from the "
				"specified JSON file.~n~n"
				"The configuration file is currently the only place where you may configure "
				"webhooks and tune semaphores.~n~n"
				"An example:~n~n"
				"{~n"
				"  \"webhooks\": [~n"
				"    {~n"
				"      \"events\": [\"transaction\", \"block\"],~n"
				"      \"url\": \"https://example.com/block_or_tx\",~n"
				"      \"headers\": {~n"
				"        \"Authorization\": \"Bearer 123\"~n"
				"       }~n"
				"    },~n"
				"    {~n"
				"      \"events\": [\"transaction_data\"],~n"
				"      \"url\": \"http://127.0.0.1:1985/tx_data\"~n"
				"    },~n"
				"    {~n"
				"      \"events\": [\"solution\"],~n"
				"      \"url\": \"http://127.0.0.1:1985/solution\"~n"
				"    },~n"
				"  \"semaphores\": {\"post_tx\": 100}~n"
				"}~n~n"
				"100 means the node will validate up to 100 incoming transactions in "
				"parallel.~n"
				"The supported semaphore keys are get_chunk, get_and_pack_chunk, get_tx_data, "
				"post_chunk, post_tx, get_block_index, get_wallet_list, get_sync_record.~n~n",
				[])},
			{"peer (IP:port)", "Join a network on a peer (or set of peers)."},
			{"block_gossip_peer (IP:port)", "Optionally specify peer(s) to always"
					" send blocks to."},
			{"local_peer (IP:port)", "The local network peer. Local peers do not rate limit "
					"each other so we recommend you connect all your nodes from the same "
					"network via this configuration parameter."},
			{"sync_from_local_peers_only", "If set, the data (not headers) is only synced "
					"from the local network peers specified via the local_peer parameter."},
			{"start_from_latest_state", "Start the node from the latest stored state."},
			{"start_from_block (block hash)", "Start the node from the state corresponding "
					"to the given block hash."},
			{"start_from_block_index", "The legacy name for start_from_latest_state."},
			{"mine", "Automatically start mining once the netwok has been joined."},
			{"port", "The local port to use for mining. "
						"This port must be accessible by remote peers."},
			{"data_dir",
				"The directory for storing the weave and the wallets (when generated)."},
			{"log_dir", "The directory for logs. If the \"debug\" flag is set, the debug logs "
					"are written to logs/debug_logs/. The RocksDB logs are written to "
					"logs/rocksdb/."},
			{"storage_module", "A storage module is responsible for syncronizing and storing "
					"a particular data range. The data and metadata related to the module "
					"are stored in a dedicated folder "
					"([data_dir]/storage_modules/storage_module_[partition_number]_[replica_type]/"
					") where replica_type is either {mining_address} or"
					" {mining address}.{composite packing difficulty} or"
					" {mining address}.replica.2.9 or \"unpacked\"."
					" Example: storage_module 0,En2eqsVJARnTVOSh723PBXAKGmKgrGSjQ2YIGwE_ZRI.1. "
					"To configure a module of a custom size, set "
					"storage_module {number},{size_in_bytes},{replica_type}. For instance, "
					"storage_module "
					"22,1000000000000,En2eqsVJARnTVOSh723PBXAKGmKgrGSjQ2YIGwE_ZRI.1 will be "
					"syncing the weave data between the offsets 22 TB and 23 TB. Make sure "
					"the corresponding disk contains some extra space for the proofs and "
					"other metadata, about 10% of the configured size."

					"You may repack a storage module in-place. To do that, specify "
					"storage_module "
					"{partition_number},{packing},repack_in_place,{target_packing}. "
					"For example, if you want to repack a storage module "
					"22,En2eqsVJARnTVOSh723PBXAKGmKgrGSjQ2YIGwE_ZRI.1 to the new address "
					"Q5EfKawrRazp11HEDf_NJpxjYMV385j21nlQNjR8_pY, specify "
					"storage_module "
					"22,En2eqsVJARnTVOSh723PBXAKGmKgrGSjQ2YIGwE_ZRI.1,repack_in_place,"
					"Q5EfKawrRazp11HEDf_NJpxjYMV385j21nlQNjR8_pY.replica.2.9. This storage module "
					"will only do the repacking - it won't be used for mining and won't "
					"serve any data to peers. Once the repacking is complete, a message will "
					"be logged to the file and written to the console. We suggest you rename "
					"the storage module folder according to the new packing then. "

					"Note: as of 2.9.1 you can only repack in place to the replica_2_9 "
					"format."
			},
			{"repack_batch_size", io_lib:format("The number of batches to process at a time "
				"during in-place repacking. For each partition being repacked, a batch "
				"requires about 512 MiB of memory. Default: ~B.",
				[?DEFAULT_REPACK_BATCH_SIZE])},
			{"repack_cache_size_mb", io_lib:format("The size (in MiB) of the cache for "
				"in-place repacking. The node will restrict the cache size to this amount for "
				"each partition being repacked. Default: ~B.",
				[?DEFAULT_REPACK_CACHE_SIZE_MB])},
			{"polling (num)", lists:flatten(
					io_lib:format(
						"Ask some peers about new blocks every N seconds. Default is ~p.",
						[?DEFAULT_POLLING_INTERVAL]
					)
			)},
			{"block_pollers (num)", io_lib:format(
					"How many peer polling jobs to run. Default is ~p.",
					[?DEFAULT_BLOCK_POLLERS])},
			{"no_auto_join", "Do not automatically join the network of your peers."},
			{"join_workers (num)", io_lib:format("The number of workers fetching the recent "
					"blocks and transactions simultaneously when joining the network. "
					"Default: ~B. ", [?DEFAULT_JOIN_WORKERS])},
			{"mining_addr (addr)",
				io_lib:format(
				"The address mining rewards should be credited to. If the \"mine\" flag"
				" is set but no mining_addr is specified, an RSA PSS key is created"
				" and stored in the [data_dir]/~s directory. If the directory already"
				" contains such keys, the one written later is picked, no new files are"
				" created. After the fork 2.6, the specified address is also a replication key, "
				"so it is used to prepare synced data for mining even if the \"mine\" flag is not "
				"specified. The data already packed with different addresses is not repacked.",
				[?WALLET_DIR])},
			{"hashing_threads (num)", io_lib:format("The number of hashing processes to spawn."
					" Takes effect starting from the fork 2.6 block."
					" Default is ~B.", [?NUM_HASHING_PROCESSES])},
			{"data_cache_size_limit (num)", "The approximate maximum number of data chunks "
					"kept in memory by the syncing processes."},
			{"packing_cache_size_limit (num)", "The approximate maximum number of data chunks "
					"kept in memory by the packing process."},
			{"mining_cache_size_mb (num)", "The total amount of cache "
				"(in MiB) allocated to store unprocessed chunks while mining. The mining "
				"server will only read new data when there is room in the cache to store "
				"more chunks. This cache is subdivided into sub-caches for each mined "
				"partition. When omitted, it is determined based on the number of "
				"mining partitions."},
			{"max_emitters (num)", io_lib:format("The number of transaction propagation "
				"processes to spawn. Must be at least 1. Default is ~B.", [?NUM_EMITTER_PROCESSES])},
			{"post_tx_timeout", io_lib:format("The time in seconds to wait for the available"
				" tx validation process before dropping the POST /tx request. Default is ~B."
				" By default ~B validation processes are running. You can override it by"
				" setting a different value for the post_tx key in the semaphores object"
				" in the configuration file.", [?DEFAULT_POST_TX_TIMEOUT,
						?MAX_PARALLEL_POST_TX_REQUESTS])},
			{"max_propagation_peers", io_lib:format(
				"The maximum number of peers to propagate transactions to. "
				"Default is ~B.", [?DEFAULT_MAX_PROPAGATION_PEERS])},
			{"max_block_propagation_peers", io_lib:format(
				"The maximum number of best peers to propagate blocks to. "
				"Default is ~B.", [?DEFAULT_MAX_BLOCK_PROPAGATION_PEERS])},
			{"sync_jobs (num)",
				io_lib:format(
					"The number of data syncing jobs to run. Default: ~B."
					" Each job periodically picks a range and downloads it from peers.",
					[?DEFAULT_SYNC_JOBS]
				)},
			{"header_sync_jobs (num)",
				io_lib:format(
					"The number of header syncing jobs to run. Default: ~B."
					" Each job periodically picks the latest not synced block header"
					" and downloads it from peers.",
					[?DEFAULT_HEADER_SYNC_JOBS]
				)},
			{"data_sync_request_packed_chunks",
				"Enables requesting the packed chunks from peers."},
			{"disk_pool_jobs (num)",
				io_lib:format(
					"The number of disk pool jobs to run. Default: ~B."
					" Disk pool jobs scan the disk pool to index no longer pending or"
					" orphaned chunks, schedule packing for chunks with a sufficient"
					" number of confirmations and remove abandoned chunks.",
					[?DEFAULT_DISK_POOL_JOBS]
				)},
			{"load_mining_key (file)", "DEPRECATED. Does not take effect anymore."},
			{"transaction_blacklist (file)", "A file containing blacklisted transactions. "
											 "One Base64 encoded transaction ID per line."},
			{"transaction_blacklist_url", "An HTTP endpoint serving a transaction blacklist."},
			{"transaction_whitelist (file)", "A file containing whitelisted transactions. "
											 "One Base64 encoded transaction ID per line. "
											 "If a transaction is in both lists, it is "
											 "considered whitelisted."},
			{"transaction_whitelist_url", "An HTTP endpoint serving a transaction whitelist."},
			{"disk_space_check_frequency (num)",
				io_lib:format(
					"The frequency in seconds of requesting the information "
					"about the available disk space from the operating system, "
					"used to decide on whether to continue syncing the historical "
					"data or clean up some space. Default is ~B.",
					[?DISK_SPACE_CHECK_FREQUENCY_MS div 1000]
				)},
			{"init", "Start a new weave."},
			{"internal_api_secret (secret)",
				lists:flatten(io_lib:format(
						"Enables the internal API endpoints, only accessible with this secret."
						" Min. ~B chars.",
						[?INTERNAL_API_SECRET_MIN_LEN]))},
			{"enable (feature)", "Enable a specific (normally disabled) feature. For example, "
					"subfield_queries."},
			{"disable (feature)", "Disable a specific (normally enabled) feature."},
			{"requests_per_minute_limit (number)", "Limit the maximum allowed number of HTTP "
					"requests per IP address per minute. Default is 900."},
			{"max_connections", io:format(
				"The number of connections to be handled concurrently. "
				"Its purpose is to prevent your system from being overloaded and "
				"ensuring all the connections are handled optimally. "
				"Default is ~p.",
				[?DEFAULT_COWBOY_TCP_MAX_CONNECTIONS]
			)},
			{"disk_pool_data_root_expiration_time",
				"The time in seconds of how long a pending or orphaned data root is kept in "
				"the disk pool. The default is 2 * 60 * 60 (2 hours)."},
			{"max_disk_pool_buffer_mb",
				"The max total size (in MiB)) of the pending chunks in the disk pool."
				"The default is 2000 (2 GiB)."},
			{"max_disk_pool_data_root_buffer_mb",
				"The max size (in MiB) per data root of the pending chunks in the disk"
				" pool. The default is 50."},
			{"disk_cache_size_mb",
				lists:flatten(io_lib:format(
					"The maximum size (in MiB) of the disk space allocated for"
					" storing recent block and transaction headers. Default is ~B.",
					[?DISK_CACHE_SIZE]
				)
			)},
			{"packing_workers (num)",
				"The number of packing workers to spawn. The default is the number of "
				"logical CPU cores."},
			{"replica_2_9_workers (num)", io_lib:format(
				"The number of replica 2.9 workers to spawn. Replica 2.9 workers are used "
				"to generate entropy for the replica.2.9 format. By default, at most one "
				"worker will be active per physical disk at a time. Default: ~B",
				[?DEFAULT_REPLICA_2_9_WORKERS]
			)},
			{"disable_replica_2_9_device_limit",
				"Disable the device limit for the replica.2.9 format. By default, at most "
				"one worker will be active per physical disk at a time, setting this flag "
				"removes this limit allowing multiple workers to be active on a given "
				"physical disk."
			},
			{"replica_2_9_entropy_cache_max_entropies (num)", io_lib:format(
				"The maximum number of replica 2.9 entropies to cache at a time "
				"while syncing data. Each entropy is 256 MB. Default: ~B",
				[?DEFAULT_REPLICA_2_9_ENTROPY_CACHE_MAX_ENTROPIES]
			)},
			{"max_vdf_validation_thread_count", io_lib:format("\tThe maximum number "
					"of threads used for VDF validation. Default: ~B",
					[?DEFAULT_MAX_NONCE_LIMITER_VALIDATION_THREAD_COUNT])},
			{"max_vdf_last_step_validation_thread_count", io_lib:format(
					"\tThe maximum number of threads used for VDF last step "
					"validation. Default: ~B",
					[?DEFAULT_MAX_NONCE_LIMITER_LAST_STEP_VALIDATION_THREAD_COUNT])},
			{"vdf_server_trusted_peer", "If the option is set, we expect the given "
					"peer(s) to push VDF updates to us; we will thus not compute VDF outputs "
					"ourselves. Recommended on CPUs without hardware extensions for computing"
					" SHA-2. We will nevertheless validate VDF chains in blocks. Also, we "
					"recommend you specify at least two trusted peers to aim for shorter "
					"mining downtime."},
			{"vdf_client_peer", "If the option is set, the node will push VDF updates "
					"to this peer. You can specify several vdf_client_peer options."},
			{"debug",
				"Enable extended logging."},
			{"run_defragmentation",
				"Run defragmentation of chunk storage files."},
			{"defragmentation_trigger_threshold",
				"File size threshold in bytes for it to be considered for defragmentation."},
			{"block_throttle_by_ip_interval (number)",
					io_lib:format("The number of milliseconds that have to pass before "
							"we accept another block from the same IP address. Default: ~B.",
							[?DEFAULT_BLOCK_THROTTLE_BY_IP_INTERVAL_MS])},
			{"block_throttle_by_solution_interval (number)",
					io_lib:format("The number of milliseconds that have to pass before "
							"we accept another block with the same solution hash. "
							"Default: ~B.",
							[?DEFAULT_BLOCK_THROTTLE_BY_SOLUTION_INTERVAL_MS])},
			{"defragment_module",
				"Run defragmentation of the chunk storage files from the given storage module."
				" Assumes the run_defragmentation flag is provided."},
			{"tls_cert_file",
				"Optional path to the TLS certificate file for TLS support, "
				"depends on 'tls_key_file' being set as well."},
			{"tls_key_file",
				"The path to the TLS key file for TLS support, depends "
				"on 'tls_cert_file' being set as well."},
			{"coordinated_mining", "Enable coordinated mining. If you are a solo pool miner "
					"coordinating on a replica with other pool miners, set this flag too. "
					"To connect the internal nodes, set cm_api_secret, cm_peer, "
					"and cm_exit_peer. Make sure every node specifies every other node in the "
					"cluster via cm_peer or cm_exit_peer. The same peer may be both cm_peer "
					"and cm_exit_peer. Also, set the mine flag on every CM peer. You may or "
					"may not set the mine flag on the exit peer."},
			{"cm_api_secret", "Coordinated mining secret for authenticated "
					"requests between private peers. You need to also set coordinated_mining, "
					"cm_peer, and cm_exit_peer."},
			{"cm_poll_interval", io_lib:format("The frequency in milliseconds of asking the "
					"other nodes in the coordinated mining setup about their partition "
					"tables. Default is ~B.", [?DEFAULT_CM_POLL_INTERVAL_MS])},
			{"cm_out_batch_timeout (num)", io_lib:format("The frequency in milliseconds of "
					"sending other nodes in the coordinated mining setup a batch of H1 "
					"values to hash. A higher value reduces network traffic, a lower value "
					"reduces hashing latency. Default is ~B.",
					[?DEFAULT_CM_BATCH_TIMEOUT_MS])},
			{"cm_peer (IP:port)", "The peer(s) to mine in coordination with. You need to also "
					"set coordinated_mining, cm_api_secret, and cm_exit_peer. The same "
					"peer may be specified as cm_peer and cm_exit_peer. If we are an exit "
					"peer, make sure to also set cm_peer for every miner we work with."},
			{"cm_exit_peer (IP:port)", "The peer to send mining solutions to in the "
					"coordinated mining mode. You need to also set coordinated_mining, "
					"cm_api_secret, and cm_peer. If cm_exit_peer is not set, we are the "
					"exit peer. When is_pool_client is set, the exit peer "
					"is a proxy through which we communicate with the pool."},
			{"is_pool_server", "Configure the node as a pool server. The pool node may not "
					"participate in the coordinated mining."},
			{"is_pool_client", "Configure the node as a pool client. The node may be an "
					"exit peer in the coordinated mining setup or a standalone node."},
			{"pool_api_key", "API key for the requests to the pool."},
			{"pool_server_address", "The pool address"},
			{"pool_worker_name", "(optional) The pool worker name. "
					"Useful if you have multiple machines (or replicas) "
					"and you want to monitor them separately on pool"},
			{"rocksdb_flush_interval", "RocksDB flush interval in seconds"},
			{"rocksdb_wal_sync_interval", "RocksDB WAL sync interval in seconds"},
			{"verify", "Run in verify. There are two valid values 'purge' or 'log'. "
				"The node will run several checks on all listed storage_modules, and flag any "
				"errors. In 'log' mode the error are just logged, in 'purge' node the chunks "
				"are invalidated so that they have to be repacked. After completing a full "
				"verification cycle, you can restart the node in normal mode to have it "
				"resync and/or repack any flagged chunks. When running in verify mode several "
				"flags are disallowed. See the node output for details."},
			{"verify_samples (num)", io_lib:format("Number of chunks to sample and unpack "
				"during 'verify'. Default is ~B.", [?SAMPLE_CHUNK_COUNT])},
			{"vdf (mode)", io_lib:format("VDF implementation (openssl (default), openssllite,"
				" fused, hiopt_m4). Default is openssl.", [])},

			% Shutdown management
			{"network.tcp.shutdown.connection_timeout", io_lib:format(
				"Configure shutdown TCP connection timeout (seconds). "
				"Default is '~p'.",
				[?SHUTDOWN_TCP_CONNECTION_TIMEOUT]
			)},
			{"network.tcp.shutdown.mode", io_lib:format(
				"Configure shutdown TCP mode (shutdown or close). "
				"Default is '~p'.",
				[?SHUTDOWN_TCP_MODE]
			)},

			% Global socket configuration
			{"network.socket.backend", io_lib:format(
				"Configure Erlang default socket backend (inet or socket). "
				"Default is '~p'.",
				[?DEFAULT_SOCKET_BACKEND]
			)},

			% Gun HTTP Client Tuning
			{"http_client.http.closing_timeout", io_lib:format(
				"Configure HTTP Client closing timeout parameter (milliseconds). "
				"Default is '~p'.",
				[?DEFAULT_GUN_HTTP_CLOSING_TIMEOUT]
			)},
			{"http_client.http.keepalive", io_lib:format(
				"Configure HTTP Client keep alive parameter (seconds or infinity). "
				"Default is '~p'.",
				[?DEFAULT_GUN_HTTP_KEEPALIVE]
			)},
			{"http_client.tcp.delay_send", io_lib:format(
				"Configure HTTP Client TCP delay send parameter (boolean). "
				"Default is '~p'.",
				[?DEFAULT_GUN_TCP_DELAY_SEND]
			)},
			{"http_client.tcp.keepalive", io_lib:format(
				"Configure HTTP Client TCP keepalive parameter (boolean). "
				"Default is '~p'.",
				[?DEFAULT_GUN_TCP_KEEPALIVE]
			)},
			{"http_client.tcp.linger", io_lib:format(
				"Configure HTTP Client TCP linger parameter (boolean). "
				"Default is '~p'.",
				[?DEFAULT_GUN_TCP_LINGER]
			)},
			{"http_client.tcp.linger_timeout", io_lib:format(
				"Configure HTTP Client TCP linger timeout parameter (seconds). "
				"Default is '~p'.",
				[?DEFAULT_GUN_TCP_LINGER_TIMEOUT]
			)},
			{"http_client.tcp.nodelay", io_lib:format(
				"Configure HTTP Client TCP nodelay parameter (boolean). "
				"Default is '~p'.",
				[?DEFAULT_GUN_TCP_NODELAY]
			)},
			{"http_client.tcp.send_timeout_close", io_lib:format(
				"Configure HTTP Client TCP send timeout close parameter (boolean). "
				"Default is '~p'.",
				[?DEFAULT_GUN_TCP_SEND_TIMEOUT_CLOSE]
			)},
			{"http_client.tcp.send_timeout", io_lib:format(
				"Configure HTTP Client TCP send timeout parameter (milliseconds). "
				"Default is '~p'.",
				[?DEFAULT_GUN_TCP_SEND_TIMEOUT]
			)},

			% Cowboy HTTP Server Tuning
			{"http_api.http.active_n", io_lib:format(
				"Configure HTTP Server number of packets requested per sockets (integer). "
				"Default is '~p'.",
				[?DEFAULT_COWBOY_HTTP_ACTIVE_N]
			)},
			{"http_api.tcp.idle_timeout_seconds", io_lib:format(
				"The number of seconds allowed for incoming API client connections to be idle "
				"before closing them. Default is '~p' seconds. "
				"Please, do not set this value too low "
				"as it will negatively affect the performance of the node.",
				[?DEFAULT_COWBOY_TCP_IDLE_TIMEOUT_SECOND]
			)},
			{"http_api.http.inactivity_timeout", io_lib:format(
				"Configure HTTP Server inactivity timeout (milliseconds). "
				"Default is '~p'.",
				[?DEFAULT_COWBOY_HTTP_INACTIVITY_TIMEOUT]
			)},
			{"http_api.http.linger_timeout", io_lib:format(
				"Configure HTTP Server linger timeout (milliseconds). "
				"Default is '~p'.",
				[?DEFAULT_COWBOY_HTTP_LINGER_TIMEOUT]
			)},
			{"http_api.http.request_timeout", io_lib:format(
				"Configure HTTP Server request timeout (milliseconds). "
				"Default is '~p'.",
				[?DEFAULT_COWBOY_HTTP_REQUEST_TIMEOUT]
			)},
			{"http_api.tcp.backlog", io_lib:format(
				"Configure HTTP Server TCP backlog parameter (integer). "
				"Default is '~p'.",
				[?DEFAULT_COWBOY_TCP_BACKLOG]
			)},
			{"http_api.tcp.delay_send", io_lib:format(
				"Configure HTTP Server TCP delay send parameter (boolean). "
				"Default is '~p'.",
				[?DEFAULT_COWBOY_TCP_DELAY_SEND]
			)},
			{"http_api.tcp.keepalive", io_lib:format(
				"Configure HTTP Server TCP keepalive parameter (boolean). "
				"Default is '~p'.",
				[?DEFAULT_COWBOY_TCP_KEEPALIVE]
			)},
			{"http_api.tcp.linger", io_lib:format(
				"Configure HTTP Server TCP linger parameter (boolean). "
				"Default is '~p'.",
				[?DEFAULT_COWBOY_TCP_LINGER]
			)},
			{"http_api.tcp.linger_timeout", io_lib:format(
				"Configure HTTP Server TCP linger timeout parameter (seconds). "
				"Default is '~p'.",
				[?DEFAULT_COWBOY_TCP_LINGER_TIMEOUT]
			)},
			{"http_api.tcp.listener_shutdown", io_lib:format(
				"Configure HTTP Server listener shutdown (seconds)."
				"Default is '~p'.",
				[?DEFAULT_COWBOY_TCP_LISTENER_SHUTDOWN]
			)},
			{"http_api.tcp.nodelay", io_lib:format(
				"Configure HTTP Server TCP nodelay parameter (boolean). "
				"Default is '~p'.",
				[?DEFAULT_COWBOY_TCP_NODELAY]
			)},
			{"http_api.tcp.num_acceptors", io_lib:format(
				"Configure HTTP Server TCP acceptors (integer). "
				"Default is '~p'.",
				[?DEFAULT_COWBOY_TCP_NUM_ACCEPTORS]
			)},
			{"http_api.tcp.send_timeout_close", io_lib:format(
				"Configure HTTP Server TCP send timeout close parameter (boolean). "
				"Default is '~p'.",
				 [?DEFAULT_COWBOY_TCP_SEND_TIMEOUT_CLOSE]
			)},
			{"http_api.tcp.send_timeout", io_lib:format(
				"Configure HTTP Server TCP send timeout parameter (milliseconds). "
				"Default is '~p'.",
				[?DEFAULT_COWBOY_TCP_SEND_TIMEOUT]
			)}
		]
	),
	init:stop(1).

parse_config_file(["config_file", Path | Rest], Skipped, _) ->
	case read_config_from_file(Path) of
		{ok, Config} ->
			parse_config_file(Rest, Skipped, Config);
		{error, Reason, Item} ->
			io:format("Failed to parse config: ~p: ~p.~n", [Reason, Item]),
			show_help();
		{error, Reason} ->
			io:format("Failed to parse config: ~p.~n", [Reason]),
			show_help()
	end;
parse_config_file([Arg | Rest], Skipped, Config) ->
	parse_config_file(Rest, [Arg | Skipped], Config);
parse_config_file([], Skipped, Config) ->
	Args = lists:reverse(Skipped),
	parse_cli_args(Args, Config).

read_config_from_file(Path) ->
	case file:read_file(Path) of
		{ok, FileData} -> ar_config:parse(FileData);
		{error, _} -> {error, file_unreadable, Path}
	end.

parse_cli_args([], C) -> C;
parse_cli_args(["mine" | Rest], C) ->
	parse_cli_args(Rest, C#config{ mine = true });
parse_cli_args(["verify", "purge" | Rest], C) ->
	parse_cli_args(Rest, C#config{ verify = purge });
parse_cli_args(["verify", "log" | Rest], C) ->
	parse_cli_args(Rest, C#config{ verify = log });
parse_cli_args(["verify", _ | _], _C) ->
	io:format("Invalid verify mode. Valid modes are 'purge' or 'log'.~n"),
	timer:sleep(1000),
	init:stop(1);
parse_cli_args(["verify_samples", "all" | Rest], C) ->
	parse_cli_args(Rest, C#config{ verify_samples = all });
parse_cli_args(["verify_samples", N | Rest], C) ->
	parse_cli_args(Rest, C#config{ verify_samples = list_to_integer(N) });
parse_cli_args(["vdf", Mode | Rest], C) ->
	ParsedMode = case Mode of
		"openssl" ->openssl;
		"openssllite" ->openssllite;
		"fused" ->fused;
		"hiopt_m4" ->hiopt_m4;
		_ ->
			io:format("VDF ~p is invalid.~n", [Mode]),
			openssl
	end,
	parse_cli_args(Rest, C#config{ vdf = ParsedMode });
parse_cli_args(["peer", Peer | Rest], C = #config{ peers = Ps }) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ValidPeers} when is_list(ValidPeers) ->
			NewConfig = C#config{peers = ValidPeers ++ Ps},
			parse_cli_args(Rest, NewConfig);
		{error, _} ->
			io:format("Peer ~p is invalid.~n", [Peer]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["block_gossip_peer", Peer | Rest],
		C = #config{ block_gossip_peers = Peers }) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ValidPeer} when is_list(ValidPeer) ->
			parse_cli_args(Rest, C#config{ block_gossip_peers = ValidPeer ++ Peers });
		{error, _} ->
			io:format("Peer ~p invalid ~n", [Peer]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["local_peer", Peer | Rest], C = #config{ local_peers = Peers }) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ValidPeer} when is_list(ValidPeer) ->
			parse_cli_args(Rest, C#config{ local_peers = ValidPeer ++ Peers });
		{error, _} ->
			io:format("Peer ~p is invalid.~n", [Peer]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["sync_from_local_peers_only" | Rest], C) ->
	parse_cli_args(Rest, C#config{ sync_from_local_peers_only = true });
parse_cli_args(["transaction_blacklist", File | Rest],
	C = #config{ transaction_blacklist_files = Files } ) ->
	parse_cli_args(Rest, C#config{ transaction_blacklist_files = [File | Files] });
parse_cli_args(["transaction_blacklist_url", URL | Rest],
		C = #config{ transaction_blacklist_urls = URLs} ) ->
	parse_cli_args(Rest, C#config{ transaction_blacklist_urls = [URL | URLs] });
parse_cli_args(["transaction_whitelist", File | Rest],
		C = #config{ transaction_whitelist_files = Files } ) ->
	parse_cli_args(Rest, C#config{ transaction_whitelist_files = [File | Files] });
parse_cli_args(["transaction_whitelist_url", URL | Rest],
		C = #config{ transaction_whitelist_urls = URLs} ) ->
	parse_cli_args(Rest, C#config{ transaction_whitelist_urls = [URL | URLs] });
parse_cli_args(["port", Port | Rest], C) ->
	parse_cli_args(Rest, C#config{ port = list_to_integer(Port) });
parse_cli_args(["data_dir", DataDir | Rest], C) ->
	parse_cli_args(Rest, C#config{ data_dir = DataDir });
parse_cli_args(["log_dir", Dir | Rest], C) ->
	parse_cli_args(Rest, C#config{ log_dir = Dir });
parse_cli_args(["storage_module", StorageModuleString | Rest], C) ->
	try
		case ar_config:parse_storage_module(StorageModuleString) of
			{ok, StorageModule} ->
				StorageModules = C#config.storage_modules,
				parse_cli_args(Rest, C#config{
						storage_modules = [StorageModule | StorageModules] });
			{repack_in_place, StorageModule} ->
				StorageModules = C#config.repack_in_place_storage_modules,
				parse_cli_args(Rest, C#config{
						repack_in_place_storage_modules = [StorageModule | StorageModules] })
		end
	catch _:_ ->
		io:format("~nstorage_module value must be "
				"in the {number},{address}[,repack_in_place,{to_packing}] format.~n~n"),
		init:stop(1)
	end;
parse_cli_args(["repack_batch_size", N | Rest], C) ->
	parse_cli_args(Rest, C#config{ repack_batch_size = list_to_integer(N) });
parse_cli_args(["repack_cache_size_mb", N | Rest], C) ->
	parse_cli_args(Rest, C#config{ repack_cache_size_mb = list_to_integer(N) });
parse_cli_args(["polling", Frequency | Rest], C) ->
	parse_cli_args(Rest, C#config{ polling = list_to_integer(Frequency) });
parse_cli_args(["block_pollers", N | Rest], C) ->
	parse_cli_args(Rest, C#config{ block_pollers = list_to_integer(N) });
parse_cli_args(["no_auto_join" | Rest], C) ->
	parse_cli_args(Rest, C#config{ auto_join = false });
parse_cli_args(["join_workers", N | Rest], C) ->
	parse_cli_args(Rest, C#config{ join_workers = list_to_integer(N) });
parse_cli_args(["diff", N | Rest], C) ->
	parse_cli_args(Rest, C#config{ diff = list_to_integer(N) });
parse_cli_args(["mining_addr", Addr | Rest], C) ->
	case C#config.mining_addr of
		not_set ->
			case ar_util:safe_decode(Addr) of
				{ok, DecodedAddr} when byte_size(DecodedAddr) == 32 ->
					parse_cli_args(Rest, C#config{ mining_addr = DecodedAddr });
				_ ->
					io:format("~nmining_addr must be a valid Base64Url string, 43"
							" characters long.~n~n"),
					init:stop(1)
			end;
		_ ->
			io:format("~nYou may specify at most one mining_addr.~n~n"),
			init:stop(1)
	end;
parse_cli_args(["hashing_threads", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ hashing_threads = list_to_integer(Num) });
parse_cli_args(["data_cache_size_limit", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{
			data_cache_size_limit = list_to_integer(Num) });
parse_cli_args(["packing_cache_size_limit", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{
			packing_cache_size_limit = list_to_integer(Num) });
parse_cli_args(["mining_cache_size_mb", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{
			mining_cache_size_mb = list_to_integer(Num) });
parse_cli_args(["max_emitters", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ max_emitters = list_to_integer(Num) });
parse_cli_args(["disk_space_check_frequency", Frequency | Rest], C) ->
	parse_cli_args(Rest, C#config{
		disk_space_check_frequency = list_to_integer(Frequency) * 1000
	});
parse_cli_args(["start_from_block_index" | Rest], C) ->
	parse_cli_args(Rest, C#config{ start_from_latest_state = true });
parse_cli_args(["start_from_block", H | Rest], C) ->
	case ar_util:safe_decode(H) of
		{ok, Decoded} when byte_size(Decoded) == 48 ->
			parse_cli_args(Rest, C#config{ start_from_block = Decoded });
		_ ->
			io:format("Invalid start_from_block.~n", []),
			timer:sleep(1000),
			init:stop(1)
	end;
parse_cli_args(["start_from_latest_state" | Rest], C) ->
	parse_cli_args(Rest, C#config{ start_from_latest_state = true });
parse_cli_args(["init" | Rest], C)->
	parse_cli_args(Rest, C#config{ init = true });
parse_cli_args(["internal_api_secret", Secret | Rest], C)
		when length(Secret) >= ?INTERNAL_API_SECRET_MIN_LEN ->
	parse_cli_args(Rest, C#config{ internal_api_secret = list_to_binary(Secret)});
parse_cli_args(["internal_api_secret", _ | _], _) ->
	io:format("~nThe internal_api_secret must be at least ~B characters long.~n~n",
			[?INTERNAL_API_SECRET_MIN_LEN]),
	init:stop(1);
parse_cli_args(["enable", Feature | Rest ], C = #config{ enable = Enabled }) ->
	parse_cli_args(Rest, C#config{ enable = [ list_to_atom(Feature) | Enabled ] });
parse_cli_args(["disable", Feature | Rest ], C = #config{ disable = Disabled }) ->
	parse_cli_args(Rest, C#config{ disable = [ list_to_atom(Feature) | Disabled ] });
parse_cli_args(["custom_domain", _ | Rest], C = #config{ }) ->
	?LOG_WARNING("Deprecated option found 'custom_domain': "
			" this option has been removed and is a no-op.", []),
	parse_cli_args(Rest, C#config{ });
parse_cli_args(["requests_per_minute_limit", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ requests_per_minute_limit = list_to_integer(Num) });
parse_cli_args(["max_propagation_peers", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ max_propagation_peers = list_to_integer(Num) });
parse_cli_args(["max_block_propagation_peers", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ max_block_propagation_peers = list_to_integer(Num) });
parse_cli_args(["sync_jobs", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ sync_jobs = list_to_integer(Num) });
parse_cli_args(["header_sync_jobs", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ header_sync_jobs = list_to_integer(Num) });
parse_cli_args(["data_sync_request_packed_chunks" | Rest], C) ->
	parse_cli_args(Rest, C#config{ data_sync_request_packed_chunks = true });
parse_cli_args(["post_tx_timeout", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { post_tx_timeout = list_to_integer(Num) });
parse_cli_args(["max_connections", Num | Rest], C) ->
	try list_to_integer(Num) of
		N when N >= 1 ->
			parse_cli_args(Rest, C#config{ 'http_api.tcp.max_connections' = N });
		_ ->
			io:format("Invalid max_connections ~p", [Num]),
			parse_cli_args(Rest, C)
	catch
		_:_ ->
			io:format("Invalid max_connections ~p", [Num]),
			parse_cli_args(Rest, C)

	end;
parse_cli_args(["disk_pool_data_root_expiration_time", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{
			disk_pool_data_root_expiration_time = list_to_integer(Num) });
parse_cli_args(["max_disk_pool_buffer_mb", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ max_disk_pool_buffer_mb = list_to_integer(Num) });
parse_cli_args(["max_disk_pool_data_root_buffer_mb", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ max_disk_pool_data_root_buffer_mb = list_to_integer(Num) });
parse_cli_args(["disk_cache_size_mb", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ disk_cache_size = list_to_integer(Num) });
parse_cli_args(["packing_workers", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ packing_workers = list_to_integer(Num) });
parse_cli_args(["replica_2_9_workers", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ replica_2_9_workers = list_to_integer(Num) });
parse_cli_args(["disable_replica_2_9_device_limit" | Rest], C) ->
	parse_cli_args(Rest, C#config{ disable_replica_2_9_device_limit = true });
parse_cli_args(["replica_2_9_entropy_cache_max_entropies", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ replica_2_9_entropy_cache_max_entropies = list_to_integer(Num) });
parse_cli_args(["max_vdf_validation_thread_count", Num | Rest], C) ->
	parse_cli_args(Rest,
			C#config{ max_nonce_limiter_validation_thread_count = list_to_integer(Num) });
parse_cli_args(["max_vdf_last_step_validation_thread_count", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{
			max_nonce_limiter_last_step_validation_thread_count = list_to_integer(Num) });
parse_cli_args(["vdf_server_trusted_peer", Peer | Rest], C) ->
	#config{ nonce_limiter_server_trusted_peers = Peers } = C,
	parse_cli_args(Rest, C#config{ nonce_limiter_server_trusted_peers = [Peer | Peers] });
parse_cli_args(["vdf_client_peer", RawPeer | Rest],
		C = #config{ nonce_limiter_client_peers = Peers }) ->
	parse_cli_args(Rest, C#config{ nonce_limiter_client_peers = [RawPeer | Peers] });
parse_cli_args(["debug" | Rest], C) ->
	parse_cli_args(Rest, C#config{ debug = true });
parse_cli_args(["run_defragmentation" | Rest], C) ->
	parse_cli_args(Rest, C#config{ run_defragmentation = true });
parse_cli_args(["defragmentation_trigger_threshold", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ defragmentation_trigger_threshold = list_to_integer(Num) });
parse_cli_args(["block_throttle_by_ip_interval", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ block_throttle_by_ip_interval = list_to_integer(Num) });
parse_cli_args(["block_throttle_by_solution_interval", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{
			block_throttle_by_solution_interval = list_to_integer(Num) });
parse_cli_args(["defragment_module", DefragModuleString | Rest], C) ->
	DefragModules = C#config.defragmentation_modules,
	try
		{ok, DefragModule} = ar_config:parse_storage_module(DefragModuleString),
		DefragModules2 = [DefragModule | DefragModules],
		parse_cli_args(Rest, C#config{ defragmentation_modules = DefragModules2 })
	catch _:_ ->
		io:format("~ndefragment_module value must be in the {number},{address} format.~n~n"),
		init:stop(1)
	end;
parse_cli_args(["tls_cert_file", CertFilePath | Rest], C) ->
    AbsCertFilePath = filename:absname(CertFilePath),
    ar_util:assert_file_exists_and_readable(AbsCertFilePath),
    parse_cli_args(Rest, C#config{ tls_cert_file = AbsCertFilePath });
parse_cli_args(["tls_key_file", KeyFilePath | Rest], C) ->
    AbsKeyFilePath = filename:absname(KeyFilePath),
    ar_util:assert_file_exists_and_readable(AbsKeyFilePath),
    parse_cli_args(Rest, C#config{ tls_key_file = AbsKeyFilePath });
parse_cli_args(["http_api.tcp.idle_timeout_seconds", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { http_api_transport_idle_timeout = list_to_integer(Num) * 1000 });
parse_cli_args(["coordinated_mining" | Rest], C) ->
	parse_cli_args(Rest, C#config{ coordinated_mining = true });
parse_cli_args(["cm_api_secret", CMSecret | Rest], C)
		when length(CMSecret) >= ?INTERNAL_API_SECRET_MIN_LEN ->
	parse_cli_args(Rest, C#config{ cm_api_secret = list_to_binary(CMSecret) });
parse_cli_args(["cm_api_secret", _ | _], _) ->
	io:format("~nThe cm_api_secret must be at least ~B characters long.~n~n",
			[?INTERNAL_API_SECRET_MIN_LEN]),
	init:stop(1);
parse_cli_args(["cm_poll_interval", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ cm_poll_interval = list_to_integer(Num) });
parse_cli_args(["cm_peer", Peer | Rest], C = #config{ cm_peers = Ps }) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ValidPeer} when is_list(ValidPeer) ->
			parse_cli_args(Rest, C#config{ cm_peers = ValidPeer ++ Ps });
		{error, _} ->
			io:format("Peer ~p is invalid.~n", [Peer]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["cm_exit_peer", Peer | Rest], C) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, [ValidPeer|_]} ->
			parse_cli_args(Rest, C#config{ cm_exit_peer = ValidPeer });
		{error, _} ->
			io:format("Peer ~p is invalid.~n", [Peer]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["cm_out_batch_timeout", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ cm_out_batch_timeout = list_to_integer(Num) });
parse_cli_args(["is_pool_server" | Rest], C) ->
	parse_cli_args(Rest, C#config{ is_pool_server = true });
parse_cli_args(["is_pool_client" | Rest], C) ->
	parse_cli_args(Rest, C#config{ is_pool_client = true });
parse_cli_args(["pool_api_key", Key | Rest], C) ->
	parse_cli_args(Rest, C#config{ pool_api_key = list_to_binary(Key) });
parse_cli_args(["pool_server_address", Host | Rest], C) ->
	parse_cli_args(Rest, C#config{ pool_server_address = list_to_binary(Host) });
parse_cli_args(["pool_worker_name", Host | Rest], C) ->
	parse_cli_args(Rest, C#config{ pool_worker_name = list_to_binary(Host) });
parse_cli_args(["rocksdb_flush_interval", Seconds | Rest], C) ->
	parse_cli_args(Rest, C#config{ rocksdb_flush_interval_s = list_to_integer(Seconds) });
parse_cli_args(["rocksdb_wal_sync_interval", Seconds | Rest], C) ->
	parse_cli_args(Rest, C#config{ rocksdb_wal_sync_interval_s = list_to_integer(Seconds) });

%% tcp shutdown procedure
parse_cli_args(["network.tcp.connection_timeout", Delay|Rest], C) ->
	parse_cli_args(Rest, C#config{ shutdown_tcp_connection_timeout = list_to_integer(Delay) });
parse_cli_args(["network.tcp.shutdown.mode", RawMode|Rest], C) ->
	case RawMode of
		"shutdown" ->
			parse_cli_args(Rest, C#config{ shutdown_tcp_mode = shutdown});
		"close" ->
			parse_cli_args(Rest, C#config{ shutdown_tcp_mode = close });
		Mode ->
			io:format("Mode ~p is invalid.~n", [Mode]),
			parse_cli_args(Rest, C)
	end;

%% global socket configuration
parse_cli_args(["network.socket.backend", Backend|Rest], C) ->
	case Backend of
		"inet" ->
			parse_cli_args(Rest, C#config{ 'socket.backend' = inet });
		"socket" ->
			parse_cli_args(Rest, C#config{ 'socket.backend' = socket });
		_ ->
			io:format("Invalid socket.backend ~p.", [Backend]),
			parse_cli_args(Rest, C)
	end;

%% gun http client configuration
parse_cli_args(["http_client.http.keepalive", "infinity"|Rest], C) ->
	parse_cli_args(Rest, C#config{ 'http_client.http.keepalive' = infinity});
parse_cli_args(["http_client.http.keepalive", Keepalive|Rest], C) ->
	try list_to_integer(Keepalive) of
		K when K >= 0 ->
			parse_cli_args(Rest, C#config{ 'http_client.http.keepalive' = K });
		_ ->
			io:format("Invalid http_client.http.keepalive ~p.", [Keepalive]),
			parse_cli_args(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_client.http.keepalive ~p.", [Keepalive]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_client.tcp.delay_send", DelaySend|Rest], C) ->
	case DelaySend of
		"true" ->
			parse_cli_args(Rest, C#config{ 'http_client.tcp.delay_send' = true });
		"false" ->
			parse_cli_args(Rest, C#config{ 'http_client.tcp.delay_send' = false });
		_ ->
			io:format("Invalid http_client.tcp.delay_send ~p.", [DelaySend]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_client.tcp.keepalive", Keepalive|Rest], C) ->
	case Keepalive of
		"true" ->
			parse_cli_args(Rest, C#config{ 'http_client.tcp.keepalive' = true });
		"false" ->
			parse_cli_args(Rest, C#config{ 'http_client.tcp.keepalive' = false });
		_ ->
			io:format("Invalid http_client.tcp.keepalive ~p.", [Keepalive]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_client.tcp.linger", Linger|Rest], C) ->
	case Linger of
		"true" ->
			parse_cli_args(Rest, C#config{ 'http_client.tcp.linger' = true });
		"false" ->
			parse_cli_args(Rest, C#config{ 'http_client.tcp.linger' = false});
		_ ->
			io:format("Invalid http_client.tcp.linger ~p.", [Linger]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_client.tcp.linger_timeout", Timeout|Rest], C) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			parse_cli_args(Rest, C#config{ 'http_client.tcp.linger_timeout' = T });
		_ ->
			io:format("Invalid http_client.tcp.linger_timeout ~p.", [Timeout]),
			parse_cli_args(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_client.tcp.linger_timeout timeout ~p.", [Timeout]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_client.tcp.nodelay", Nodelay|Rest], C) ->
	case Nodelay of
		"true" ->
			parse_cli_args(Rest, C#config{ 'http_client.tcp.nodelay' = true });
		"false" ->
			parse_cli_args(Rest, C#config{ 'http_client.tcp.nodelay' = false });
		_ ->
			io:format("Invalid http_client.tcp.nodelay ~p.", [Nodelay]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_client.tcp.send_timeout_close", Value|Rest], C) ->
	case Value of
		"true" ->
			parse_cli_args(Rest, C#config{ 'http_client.tcp.send_timeout_close' = true });
		"false" ->
			parse_cli_args(Rest, C#config{ 'http_client.tcp.send_timeout_close' = false });
		_ ->
			io:format("Invalid http_client.tcp.send_timeout_close ~p.", [Value]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_client.tcp.send_timeout", Timeout|Rest], C) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			parse_cli_args(Rest, C#config{ 'http_client.tcp.send_timeout' = T });
		_ ->
			io:format("Invalid http_client.tcp.send_timeout ~p.", [Timeout]),
			parse_cli_args(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_client.tcp.send_timeout ~p.", [Timeout]),
			parse_cli_args(Rest, C)
	end;

%% cowboy http server configuration
parse_cli_args(["http_api.http.active_n", Active|Rest], C) ->
	try list_to_integer(Active) of
		N when N >= 1 ->
			parse_cli_args(Rest, C#config{ 'http_api.http.active_n' = N });
		_ ->
			io:format("Invalid http_api.http.active_n ~p.", [Active]),
			parse_cli_args(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.http.active_n ~p.", [Active]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_api.http.inactivity_timeout", Timeout|Rest], C) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			parse_cli_args(Rest, C#config{ 'http_api.http.inactivity_timeout' = T });
		_ ->
			io:format("Invalid http_api.http.inactivity_timeout ~p.", [Timeout]),
			parse_cli_args(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.http.inactivity_timeout ~p.", [Timeout]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_api.http.linger_timeout", Timeout|Rest], C) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			parse_cli_args(Rest, C#config{ 'http_api.http.linger_timeout' = T });
		_ ->
			io:format("Invalid http_api.http.linger_timeout ~p.", [Timeout]),
			parse_cli_args(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.http.linger_timeout ~p.", [Timeout]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_api.http.request_timeout", Timeout|Rest], C) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			parse_cli_args(Rest, C#config{ 'http_api.http.request_timeout' = T });
		_ ->
			io:format("Invalid http_api.http.request_timeout ~p.", [Timeout]),
			parse_cli_args(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.http.request_timeout ~p.", [Timeout]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_api.tcp.backlog", Backlog|Rest], C) ->
	try list_to_integer(Backlog)of
		B when B >= 1 ->
			parse_cli_args(Rest, C#config{ 'http_api.tcp.backlog' = B });
		_ ->
			io:format("Invalid http_api.tcp.backlog ~p.", [Backlog]),
			parse_cli_args(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.tcp.backlog ~p.", [Backlog]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_api.tcp.delay_send", DelaySend|Rest], C) ->
	case DelaySend of
		"true" ->
			parse_cli_args(Rest, C#config{ 'http_api.tcp.delay_send' = true });
		"false" ->
			parse_cli_args(Rest, C#config{ 'http_api.tcp.delay_send' = false });
		_ ->
			io:format("Invalid http_api.tcp.delay_send ~p.", [DelaySend]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_api.tcp.keepalive", "true"|Rest], C) ->
	parse_cli_args(Rest, C#config{ 'http_api.tcp.keepalive' = true});
parse_cli_args(["http_api.tcp.keepalive", "false"|Rest], C) ->
	parse_cli_args(Rest, C#config{ 'http_api.tcp.keepalive' = false});
parse_cli_args(["http_api.tcp.keepalive", Keepalive|Rest], C) ->
	io:format("Invalid http_api.tcp.keepalive ~p.", [Keepalive]),
	parse_cli_args(Rest, C);
parse_cli_args(["http_api.tcp.linger", Linger|Rest], C) ->
	case Linger of
		"true" ->
			parse_cli_args(Rest, C#config{ 'http_api.tcp.linger' = true });
		"false" ->
			parse_cli_args(Rest, C#config{ 'http_api.tcp.linger' = false});
		_ ->
			io:format("Invalid http_api.tcp.linger ~p.", [Linger]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_api.tcp.linger_timeout", Timeout|Rest], C) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			parse_cli_args(Rest, C#config{ 'http_api.tcp.linger_timeout' = T });
		_ ->
			io:format("Invalid http_api.tcp.linger_timeout ~p.", [Timeout]),
			parse_cli_args(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.tcp.linger_timeout ~p.", [Timeout]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_api.tcp.listener_shutdown", "brutal_kill"|Rest], C) ->
	parse_cli_args(Rest, C#config{ 'http_api.tcp.listener_shutdown' = brutal_kill});
parse_cli_args(["http_api.tcp.listener_shutdown", "infinity"|Rest], C) ->
	parse_cli_args(Rest, C#config{ 'http_api.tcp.listener_shutdown' = infinity });
parse_cli_args(["http_api.tcp.listener_shutdown", Shutdown|Rest], C) ->
	try list_to_integer(Shutdown) of
		S when S >= 0 ->
			parse_cli_args(Rest, C#config{ 'http_api.tcp.listener_shutdown' = S });
		_ ->
			io:format("Invalid http_api.tcp.listener_shutdown ~p.", [Shutdown]),
			parse_cli_args(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.tcp.listener_shutdown ~p.", [Shutdown]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_api.tcp.nodelay", Nodelay|Rest], C) ->
	case Nodelay of
		"true" ->
			parse_cli_args(Rest, C#config{ 'http_api.tcp.nodelay' = true });
		"false" ->
			parse_cli_args(Rest, C#config{ 'http_api.tcp.nodelay' = false });
		_ ->
			io:format("Invalid http_api.tcp.nodelay ~p.", [Nodelay]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_api.tcp.num_acceptors", Acceptors|Rest], C) ->
	try list_to_integer(Acceptors) of
		N when N >= 0 ->
			parse_cli_args(Rest, C#config{ 'http_api.tcp.num_acceptors' = N });
		_ ->
			io:format("Invalid http_api.tcp.num_acceptors ~p.", [Acceptors]),
			parse_cli_args(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.tcp.num_acceptors ~p.", [Acceptors]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_api.tcp.send_timeout_close", Value|Rest], C) ->
	case Value of
		"true" ->
			parse_cli_args(Rest, C#config{ 'http_api.tcp.send_timeout_close' = true });
		"false" ->
			parse_cli_args(Rest, C#config{ 'http_api.tcp.send_timeout_close' = false });
		_ ->
			io:format("Invalid http_api.tcp.send_timeout_close ~p.", [Value]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["http_api.tcp.send_timeout", Timeout|Rest], C) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			parse_cli_args(Rest, C#config{ 'http_api.tcp.send_timeout' = T });
		_ ->
			io:format("Invalid http_api.tcp.send_timeout ~p.", [Timeout]),
			parse_cli_args(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.tcp.send_timeout ~p.", [Timeout]),
			parse_cli_args(Rest, C)
	end;

%% Undocumented/unsupported options
parse_cli_args(["chunk_storage_file_size", Num | Rest], C) ->
	parse_cli_args(Rest, C#config{ chunk_storage_file_size = list_to_integer(Num) });

parse_cli_args([Arg | _Rest], _O) ->
	io:format("~nUnknown argument: ~s.~n", [Arg]),
	show_help().

%% @doc Start an Arweave node on this BEAM.
start() ->
	start(?DEFAULT_HTTP_IFACE_PORT).
start(Port) when is_integer(Port) ->
	start(#config{ port = Port });
start(Config) ->
	%% Start the logging system.
	case os:getenv("TERM") of
		"dumb" ->
			% Set logger to output all levels of logs to the console
			% when running in a dumb terminal.
			logger:add_handler(console, logger_std_h, #{level => all});
		_->
			ok
	end,
	case ar_config:validate_config(Config) of
		true ->
			ok;
		false ->
			timer:sleep(2000),
			init:stop(1)
	end,
	Config2 = ar_config:set_dependent_flags(Config),
	ok = arweave_config:set_env(Config2),
	filelib:ensure_dir(Config2#config.log_dir ++ "/"),
	warn_if_single_scheduler(),
	case Config2#config.nonce_limiter_server_trusted_peers of
		[] ->
			VDFSpeed = ar_bench_vdf:run_benchmark(),
			?LOG_INFO([{event, vdf_benchmark}, {vdf_s, VDFSpeed / 1000000}]);
		_ ->
			ok
	end,
	start_dependencies().


start(normal, _Args) ->
	% Load configuration from environment variable, it will
	% impact only feature supporting arweave_config.
	arweave_config_environment:load(),

	% Load the old configuration from arweave_config.
	{ok, Config} = arweave_config:get_env(),

	% arweave_config can now switch in runtime mode. Setting
	% parameters without "runtime" flag set to true will fail now.
	arweave_config:runtime(),

	%% Set erlang socket backend
	persistent_term:put({kernel, inet_backend}, Config#config.'socket.backend'),

	%% Configure logger
	ar_logger:init(Config),

	%% Start the Prometheus metrics subsystem.
	prometheus_registry:register_collector(prometheus_process_collector),
	prometheus_registry:register_collector(ar_metrics_collector),

	%% Register custom metrics.
	ar_metrics:register(),

	%% Start other apps which we depend on.
	set_mining_address(Config),
	ar_chunk_storage:run_defragmentation(),

	%% Start Arweave.
	ar_sup:start_link().

set_mining_address(#config{ mining_addr = not_set } = C) ->
	case ar_wallet:get_or_create_wallet([{?RSA_SIGN_ALG, 65537}]) of
		{error, Reason} ->
			ar:console("~nFailed to create a wallet, reason: ~p.~n",
				[io_lib:format("~p", [Reason])]),
			timer:sleep(500),
			init:stop(1);
		W ->
			Addr = ar_wallet:to_address(W),
			ar:console("~nSetting the mining address to ~s.~n", [ar_util:encode(Addr)]),
			C2 = C#config{ mining_addr = Addr },
			arweave_config:set_env(C2),
			set_mining_address(C2)
	end;
set_mining_address(#config{ mine = false }) ->
	ok;
set_mining_address(#config{ mining_addr = Addr, cm_exit_peer = CmExitPeer,
		is_pool_client = PoolClient }) ->
	case ar_wallet:load_key(Addr) of
		not_found ->
			case {CmExitPeer, PoolClient} of
				{not_set, false} ->
					ar:console("~nThe mining key for the address ~s was not found."
						" Make sure you placed the file in [data_dir]/~s (the node is looking for"
						" [data_dir]/~s/[mining_addr].json or "
						"[data_dir]/~s/arweave_keyfile_[mining_addr].json file)."
						" Do not specify \"mining_addr\" if you want one to be generated.~n~n",
						[ar_util:encode(Addr), ?WALLET_DIR, ?WALLET_DIR, ?WALLET_DIR]),
					init:stop(1);
				_ ->
					ok
			end;
		_Key ->
			ok
	end.

create_wallet([DataDir]) ->
	create_wallet(DataDir, ?RSA_KEY_TYPE);
create_wallet(_) ->
	create_wallet_fail(?RSA_KEY_TYPE).

create_ecdsa_wallet() ->
	create_wallet_fail(?ECDSA_KEY_TYPE).

create_ecdsa_wallet([DataDir]) ->
	create_wallet(DataDir, ?ECDSA_KEY_TYPE);
create_ecdsa_wallet(_) ->
	create_wallet_fail(?ECDSA_KEY_TYPE).

create_wallet(DataDir, KeyType) ->
	case filelib:is_dir(DataDir) of
		false ->
			create_wallet_fail(KeyType);
		true ->
			ok = arweave_config:set_env(#config{ data_dir = DataDir }),
			case ar_wallet:new_keyfile(KeyType) of
				{error, Reason} ->
					ar:console("Failed to create a wallet, reason: ~p.~n~n",
							[io_lib:format("~p", [Reason])]),
					timer:sleep(500),
					init:stop(1);
				W ->
					Addr = ar_wallet:to_address(W),
					ar:console("Created a wallet with address ~s.~n", [ar_util:encode(Addr)]),
					init:stop(1)
			end
	end.

create_wallet() ->
	create_wallet_fail(?RSA_KEY_TYPE).

create_wallet_fail(?RSA_KEY_TYPE) ->
	io:format("Usage: ./bin/create-wallet [data_dir]~n"),
	init:stop(1);
create_wallet_fail(?ECDSA_KEY_TYPE) ->
	io:format("Usage: ./bin/create-ecdsa-wallet [data_dir]~n"),
	init:stop(1).

benchmark_packing() ->
	benchmark_packing([]).
benchmark_packing(Args) ->
	ar_bench_timer:initialize(),
	ar_bench_packing:run_benchmark_from_cli(Args),
	init:stop(1).

benchmark_vdf() ->
	benchmark_vdf([]).
benchmark_vdf(Args) ->
	ok = arweave_config:set_env(#config{}),
	ar_bench_vdf:run_benchmark_from_cli(Args),
	init:stop(1).

benchmark_hash() ->
	benchmark_hash([]).
benchmark_hash(Args) ->
	ar_bench_hash:run_benchmark_from_cli(Args),
	init:stop(1).

benchmark_2_9() ->
	ar_bench_2_9:show_help().
benchmark_2_9(Args) ->
	ar_bench_2_9:run_benchmark_from_cli(Args),
	init:stop(1).

shutdown([NodeName]) ->
	rpc:cast(NodeName, init, stop, []).

prep_stop(State) ->
	% the service will be stopped, ar_shutdown_manager
	% must be noticed and its state modified.
	_ = ar_shutdown_manager:shutdown(),

	% When arweave is stopped, the first step is to stop
	% accepting connections from other peers, and then
	% start the shutdown procedure.
	ok = ranch:suspend_listener(ar_http_iface_listener),

	% all timers/intervals must be stopped.
	ar_timer:terminate_timers(),
	State.

stop(_State) ->
	?LOG_INFO([{stop, ?MODULE}]).

stop_dependencies() ->
	?LOG_INFO("========== Stopping Arweave Node  =========="),
	{ok, [_Kernel, _Stdlib, _SASL, _OSMon | Deps]} = application:get_key(arweave, applications),
	lists:foreach(fun(Dep) -> application:stop(Dep) end, Deps).

start_dependencies() ->
	?LOG_INFO("========== Starting Arweave Node  =========="),
	{ok, Config} = arweave_config:get_env(),
	ar_config:log_config(Config),
	{ok, _} = application:ensure_all_started(arweave, permanent),
	ok.

%% One scheduler => one dirty scheduler => Calculating a RandomX hash, e.g.
%% for validating a block, will be blocked on initializing a RandomX dataset,
%% which takes minutes.
warn_if_single_scheduler() ->
	case erlang:system_info(schedulers_online) of
		1 ->
			?LOG_WARNING(
				"WARNING: Running only one CPU core / Erlang scheduler may cause issues.");
		_ ->
			ok
	end.

shell() ->
	arweave_config:start(),
	Config = #config{ debug = true },
	start_for_tests(test,Config),
	ar_test_node:boot_peers(test).

stop_shell() ->
	ar_test_node:stop_peers(test),
	init:stop().

%% @doc Run all of the tests associated with the core project.
tests() ->
	tests(test, [], #config{ debug = true }).

tests(Mod) ->
	tests(test, Mod).

tests(TestType, Mods, Config) when is_list(Mods) ->
	TotalTimeout = case TestType of
		e2e -> ?E2E_TEST_SUITE_TIMEOUT;
		_ -> ?TEST_SUITE_TIMEOUT
	end,
	try
		arweave_config:start(),
		start_for_tests(TestType, Config),
		ar_test_node:boot_peers(TestType),
		ar_test_node:wait_for_peers(TestType)
	catch
		Type:Reason:S ->
			io:format("Failed to start the peers due to ~p:~p:~p~n", [Type, Reason, S]),
			init:stop(1)
	end,
	Result =
		try
			eunit:test({timeout, TotalTimeout, [Mods]}, [verbose, {print_depth, 100}])
		after
			ar_test_node:stop_peers(TestType)
		end,
	case Result of
		ok -> ok;
		_ -> init:stop(1)
	end.


start_for_tests(TestType, Config) ->
	UniqueName = ar_test_node:get_node_namespace(),
	TestConfig = Config#config{
		peers = [],
		data_dir = ".tmp/data_" ++ atom_to_list(TestType) ++ "_main_" ++ UniqueName,
		port = ar_test_node:get_unused_port(),
		disable = [randomx_jit],
		auto_join = false
	},
	start(TestConfig).

%% @doc Run the tests for a set of module(s).
%% Supports strings so that it can be trivially induced from a unix shell call.
tests(TestType, Mod) when not is_list(Mod) -> tests(TestType, [Mod]);
tests(TestType, Args) ->
	Mods =
		lists:map(
			fun(Mod) when is_atom(Mod) -> Mod;
			   (Str) -> list_to_atom(Str)
			end,
			Args
		),
	tests(TestType, Mods, #config{ debug = true }).

e2e() ->
	tests(e2e, [ar_sync_pack_mine_tests, ar_repack_mine_tests, ar_repack_in_place_mine_tests]).
e2e(Mod) ->
	tests(e2e, Mod).

%% @doc Generate the project documentation.
docs() ->
	Mods =
		lists:filter(
			fun(File) -> filename:extension(File) == ".erl" end,
			element(2, file:list_dir("apps/arweave/src"))
		),
	edoc:files(
		["apps/arweave/src/" ++ Mod || Mod <- Mods],
		[
			{dir, "source_code_docs"},
			{hidden, true},
			{private, true}
		]
	).

%% @doc Ensure that parsing of core command line options functions correctly.
commandline_parser_test_() ->
	{timeout, 60, fun() ->
		Addr = crypto:strong_rand_bytes(32),
		Tests =
			[
				{"peer 1.2.3.4 peer 5.6.7.8:9", #config.peers, [{5,6,7,8,9},{1,2,3,4,1984}]},
				{"mine", #config.mine, true},
				{"port 22", #config.port, 22},
				{"mining_addr "
					++ binary_to_list(ar_util:encode(Addr)), #config.mining_addr, Addr}
			],
		X = string:split(string:join([ L || {L, _, _} <- Tests ], " "), " ", all),
		C = parse_cli_args(X, #config{}),
		lists:foreach(
			fun({_, Index, Value}) ->
				?assertEqual(element(Index, C), Value)
			end,
			Tests
		)
	end}.

-ifdef(AR_TEST).
console(Format) ->
	?LOG_INFO(io_lib:format(Format, [])).

console(Format, Params) ->
	?LOG_INFO(io_lib:format(Format, Params)).
-else.
console(Format) ->
	io:format(Format).

console(Format, Params) ->
	io:format(Format, Params).
-endif.
