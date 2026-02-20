%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2025 (c) Arweave
%%% @author Arweave Team
%%% @doc `ar_cli_parser' module legacy command line argument parser.
%%%
%%% This module has been created from `ar' module. The code is mostly
%%% the same, except the exported interfaces have been renamed.
%%%
%%% @end
%%%===================================================================
-module(ar_cli_parser).
-compile(warnings_as_errors).
-export([
	eval/2,
	parse/2,
	show_help/0
]).
-include("ar.hrl").
-include("ar_consensus.hrl").
-include("ar_verify_chunks.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% @doc show help and stop the node.
%% @end
%%--------------------------------------------------------------------
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
			{"start_from_state (folder)", "Start the node from the state stored in the "
					"specified folder. This folder must be different from data_dir. "
					"Implicitly sets start_from_latest_state to true."},
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
			{"replica_2_9_entropy_cache_size_mb (num)", io_lib:format(
				"The maximum cache size (in MiB) to allocate for for replica.2.9 entropy. "
				"Each cached entropy is 256 MiB. The bigger the cache, the more replica.2.9 data "
				"can be synced concurrently. Default: ~B",
				[?DEFAULT_REPLICA_2_9_ENTROPY_CACHE_SIZE_MB]
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
	).


%%--------------------------------------------------------------------
%% @doc evaluate `parse_cli_args/2' function and execute actions if
%% required (returned by `parse/2' function).
%% @end
%%--------------------------------------------------------------------
-spec eval(Args, Config) -> Return when
	Args :: [string()],
	Config :: #config{},
	Return :: Config | [term()].

eval(Args, Config) ->
	case parse(Args, Config) of
		{ok, C} -> C;
		{error, Actions, _C} ->
			[ {M, F, A, erlang:apply(M, F, A)}
			  || {M,F,A} <- Actions
			]
	end.

%%--------------------------------------------------------------------
%% @doc Legacy argument parser. This function will return the
%% configuration as `#config{}' record in case of success, or returns
%% an error with a list of actions to execute as MFA.
%% @end
%%--------------------------------------------------------------------
-spec parse(Args, Config) -> Return when
	Args :: [string()],
	Config :: #config{},
	Return :: {ok, Config} |
		{error, Actions, Config},
	Actions :: {M, F, A},
	M :: atom(),
	F :: atom(),
	A :: [term()].

parse([], C) ->
	{ok, C};
parse(["config_file",_|Rest], C) ->
	% ignore config_file parameter when using arguments parser.
	parse(Rest, C);
parse(["mine" | Rest], C) ->
	parse(Rest, C#config{ mine = true });
parse(["verify", "purge" | Rest], C) ->
	parse(Rest, C#config{ verify = purge });
parse(["verify", "log" | Rest], C) ->
	parse(Rest, C#config{ verify = log });
parse(["verify", _ | _], C) ->
	io:format("Invalid verify mode. Valid modes are 'purge' or 'log'.~n"),
	{error, [
		{timer, sleep, [1000]},
		{init, stop, [1]}
	], C};
parse(["verify_samples", "all" | Rest], C) ->
	parse(Rest, C#config{ verify_samples = all });
parse(["verify_samples", N | Rest], C) ->
	parse(Rest, C#config{ verify_samples = list_to_integer(N) });
parse(["vdf", Mode | Rest], C) ->
	ParsedMode = case Mode of
		"openssl" ->openssl;
		"openssllite" ->openssllite;
		"fused" ->fused;
		"hiopt_m4" ->hiopt_m4;
		_ ->
			io:format("VDF ~p is invalid.~n", [Mode]),
			openssl
	end,
	parse(Rest, C#config{ vdf = ParsedMode });
parse(["peer", Peer | Rest], C = #config{ peers = Ps }) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ValidPeers} when is_list(ValidPeers) ->
			NewConfig = C#config{peers = ValidPeers ++ Ps},
			parse(Rest, NewConfig);
		{error, _} ->
			io:format("Peer ~p is invalid.~n", [Peer]),
			parse(Rest, C)
	end;
parse(["block_gossip_peer", Peer | Rest],
		C = #config{ block_gossip_peers = Peers }) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ValidPeer} when is_list(ValidPeer) ->
			parse(Rest, C#config{ block_gossip_peers = ValidPeer ++ Peers });
		{error, _} ->
			io:format("Peer ~p invalid ~n", [Peer]),
			parse(Rest, C)
	end;
parse(["local_peer", Peer | Rest], C = #config{ local_peers = Peers }) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ValidPeer} when is_list(ValidPeer) ->
			parse(Rest, C#config{ local_peers = ValidPeer ++ Peers });
		{error, _} ->
			io:format("Peer ~p is invalid.~n", [Peer]),
			parse(Rest, C)
	end;
parse(["sync_from_local_peers_only" | Rest], C) ->
	parse(Rest, C#config{ sync_from_local_peers_only = true });
parse(["transaction_blacklist", File | Rest],
	C = #config{ transaction_blacklist_files = Files } ) ->
	parse(Rest, C#config{ transaction_blacklist_files = [File | Files] });
parse(["transaction_blacklist_url", URL | Rest],
		C = #config{ transaction_blacklist_urls = URLs} ) ->
	parse(Rest, C#config{ transaction_blacklist_urls = [URL | URLs] });
parse(["transaction_whitelist", File | Rest],
		C = #config{ transaction_whitelist_files = Files } ) ->
	parse(Rest, C#config{ transaction_whitelist_files = [File | Files] });
parse(["transaction_whitelist_url", URL | Rest],
		C = #config{ transaction_whitelist_urls = URLs} ) ->
	parse(Rest, C#config{ transaction_whitelist_urls = [URL | URLs] });
parse(["port", Port | Rest], C) ->
	parse(Rest, C#config{ port = list_to_integer(Port) });
parse(["data_dir", DataDir | Rest], C) ->
	parse(Rest, C#config{ data_dir = DataDir });
parse(["log_dir", Dir | Rest], C) ->
	parse(Rest, C#config{ log_dir = Dir });
parse(["storage_module", StorageModuleString | Rest], C) ->
	try
		case ar_config:parse_storage_module(StorageModuleString) of
			{ok, StorageModule} ->
				StorageModules = C#config.storage_modules,
				parse(Rest, C#config{
						storage_modules = [StorageModule | StorageModules] });
			{repack_in_place, StorageModule} ->
				StorageModules = C#config.repack_in_place_storage_modules,
				parse(Rest, C#config{
						repack_in_place_storage_modules = [StorageModule | StorageModules] })
		end
	catch _:_ ->
		io:format("~nstorage_module value must be "
				"in the {number},{address}[,repack_in_place,{to_packing}] format.~n~n"),
		{error, [
			{init, stop, [1]}
		], C}
	end;
parse(["repack_batch_size", N | Rest], C) ->
	parse(Rest, C#config{ repack_batch_size = list_to_integer(N) });
parse(["repack_cache_size_mb", N | Rest], C) ->
	parse(Rest, C#config{ repack_cache_size_mb = list_to_integer(N) });
parse(["polling", Frequency | Rest], C) ->
	parse(Rest, C#config{ polling = list_to_integer(Frequency) });
parse(["block_pollers", N | Rest], C) ->
	parse(Rest, C#config{ block_pollers = list_to_integer(N) });
parse(["no_auto_join" | Rest], C) ->
	parse(Rest, C#config{ auto_join = false });
parse(["join_workers", N | Rest], C) ->
	parse(Rest, C#config{ join_workers = list_to_integer(N) });
parse(["diff", N | Rest], C) ->
	parse(Rest, C#config{ diff = list_to_integer(N) });
parse(["mining_addr", Addr | Rest], C) ->
	case C#config.mining_addr of
		not_set ->
			case ar_util:safe_decode(Addr) of
				{ok, DecodedAddr} when byte_size(DecodedAddr) == 32 ->
					parse(Rest, C#config{ mining_addr = DecodedAddr });
				_ ->
					io:format("~nmining_addr must be a valid Base64Url string, 43"
							" characters long.~n~n"),
					{error, [{init, stop, [1]}], C}
			end;
		_ ->
			io:format("~nYou may specify at most one mining_addr.~n~n"),
			{error, [{init, stop, [1]}], C}
	end;
parse(["hashing_threads", Num | Rest], C) ->
	parse(Rest, C#config{ hashing_threads = list_to_integer(Num) });
parse(["data_cache_size_limit", Num | Rest], C) ->
	parse(Rest, C#config{
			data_cache_size_limit = list_to_integer(Num) });
parse(["packing_cache_size_limit", Num | Rest], C) ->
	parse(Rest, C#config{
			packing_cache_size_limit = list_to_integer(Num) });
parse(["mining_cache_size_mb", Num | Rest], C) ->
	parse(Rest, C#config{
			mining_cache_size_mb = list_to_integer(Num) });
parse(["max_emitters", Num | Rest], C) ->
	parse(Rest, C#config{ max_emitters = list_to_integer(Num) });
parse(["disk_space_check_frequency", Frequency | Rest], C) ->
	parse(Rest, C#config{
		disk_space_check_frequency = list_to_integer(Frequency) * 1000
	});
parse(["start_from_block_index" | Rest], C) ->
	parse(Rest, C#config{ start_from_latest_state = true });
parse(["start_from_state", Folder | Rest], C) ->
	parse(Rest, C#config{ start_from_state = Folder });
parse(["start_from_block", H | Rest], C) ->
	case ar_util:safe_decode(H) of
		{ok, Decoded} when byte_size(Decoded) == 48 ->
			parse(Rest, C#config{ start_from_block = Decoded });
		_ ->
			io:format("Invalid start_from_block.~n", []),
			{error, [
				{timer, sleep, [1000]},
				{init, stop, [1]}
			], C}
	end;
parse(["start_from_latest_state" | Rest], C) ->
	parse(Rest, C#config{ start_from_latest_state = true });
parse(["init" | Rest], C)->
	parse(Rest, C#config{ init = true });
parse(["internal_api_secret", Secret | Rest], C)
		when length(Secret) >= ?INTERNAL_API_SECRET_MIN_LEN ->
	parse(Rest, C#config{ internal_api_secret = list_to_binary(Secret)});
parse(["internal_api_secret", _ | _], C) ->
	io:format("~nThe internal_api_secret must be at least ~B characters long.~n~n",
			[?INTERNAL_API_SECRET_MIN_LEN]),
	{error, [
		{init, stop, [1]}
	], C};
parse(["enable", Feature | Rest ], C = #config{ enable = Enabled }) ->
	parse(Rest, C#config{ enable = [ list_to_atom(Feature) | Enabled ] });
parse(["disable", Feature | Rest ], C = #config{ disable = Disabled }) ->
	parse(Rest, C#config{ disable = [ list_to_atom(Feature) | Disabled ] });
parse(["custom_domain", _ | Rest], C = #config{ }) ->
	?LOG_WARNING("Deprecated option found 'custom_domain': "
			" this option has been removed and is a no-op.", []),
	parse(Rest, C#config{ });
parse(["requests_per_minute_limit", Num | Rest], C) ->
	parse(Rest, C#config{ requests_per_minute_limit = list_to_integer(Num) });
parse(["max_propagation_peers", Num | Rest], C) ->
	parse(Rest, C#config{ max_propagation_peers = list_to_integer(Num) });
parse(["max_block_propagation_peers", Num | Rest], C) ->
	parse(Rest, C#config{ max_block_propagation_peers = list_to_integer(Num) });
parse(["sync_jobs", Num | Rest], C) ->
	parse(Rest, C#config{ sync_jobs = list_to_integer(Num) });
parse(["header_sync_jobs", Num | Rest], C) ->
	parse(Rest, C#config{ header_sync_jobs = list_to_integer(Num) });
parse(["data_sync_request_packed_chunks" | Rest], C) ->
	parse(Rest, C#config{ data_sync_request_packed_chunks = true });
parse(["post_tx_timeout", Num | Rest], C) ->
	parse(Rest, C#config { post_tx_timeout = list_to_integer(Num) });
parse(["max_connections", Num | Rest], C) ->
	try list_to_integer(Num) of
		N when N >= 1 ->
			parse(Rest, C#config{ 'http_api.tcp.max_connections' = N });
		_ ->
			io:format("Invalid max_connections ~p", [Num]),
			parse(Rest, C)
	catch
		_:_ ->
			io:format("Invalid max_connections ~p", [Num]),
			parse(Rest, C)

	end;
parse(["disk_pool_data_root_expiration_time", Num | Rest], C) ->
	parse(Rest, C#config{
			disk_pool_data_root_expiration_time = list_to_integer(Num) });
parse(["max_disk_pool_buffer_mb", Num | Rest], C) ->
	parse(Rest, C#config{ max_disk_pool_buffer_mb = list_to_integer(Num) });
parse(["max_disk_pool_data_root_buffer_mb", Num | Rest], C) ->
	parse(Rest, C#config{ max_disk_pool_data_root_buffer_mb = list_to_integer(Num) });
parse(["disk_cache_size_mb", Num | Rest], C) ->
	parse(Rest, C#config{ disk_cache_size = list_to_integer(Num) });
parse(["packing_workers", Num | Rest], C) ->
	parse(Rest, C#config{ packing_workers = list_to_integer(Num) });
parse(["replica_2_9_workers", Num | Rest], C) ->
	parse(Rest, C#config{ replica_2_9_workers = list_to_integer(Num) });
parse(["disable_replica_2_9_device_limit" | Rest], C) ->
	parse(Rest, C#config{ disable_replica_2_9_device_limit = true });
parse(["replica_2_9_entropy_cache_size_mb", Num | Rest], C) ->
	parse(Rest, C#config{ replica_2_9_entropy_cache_size_mb = list_to_integer(Num) });
parse(["max_vdf_validation_thread_count", Num | Rest], C) ->
	parse(Rest,
			C#config{ max_nonce_limiter_validation_thread_count = list_to_integer(Num) });
parse(["max_vdf_last_step_validation_thread_count", Num | Rest], C) ->
	parse(Rest, C#config{
			max_nonce_limiter_last_step_validation_thread_count = list_to_integer(Num) });
parse(["vdf_server_trusted_peer", Peer | Rest], C) ->
	#config{ nonce_limiter_server_trusted_peers = Peers } = C,
	parse(Rest, C#config{ nonce_limiter_server_trusted_peers = [Peer | Peers] });
parse(["vdf_client_peer", RawPeer | Rest],
		C = #config{ nonce_limiter_client_peers = Peers }) ->
	parse(Rest, C#config{ nonce_limiter_client_peers = [RawPeer | Peers] });
parse(["debug" | Rest], C) ->
	parse(Rest, C#config{ debug = true });
parse(["run_defragmentation" | Rest], C) ->
	parse(Rest, C#config{ run_defragmentation = true });
parse(["defragmentation_trigger_threshold", Num | Rest], C) ->
	parse(Rest, C#config{ defragmentation_trigger_threshold = list_to_integer(Num) });
parse(["block_throttle_by_ip_interval", Num | Rest], C) ->
	parse(Rest, C#config{ block_throttle_by_ip_interval = list_to_integer(Num) });
parse(["block_throttle_by_solution_interval", Num | Rest], C) ->
	parse(Rest, C#config{
			block_throttle_by_solution_interval = list_to_integer(Num) });
parse(["defragment_module", DefragModuleString | Rest], C) ->
	DefragModules = C#config.defragmentation_modules,
	try
		{ok, DefragModule} = ar_config:parse_storage_module(DefragModuleString),
		DefragModules2 = [DefragModule | DefragModules],
		parse(Rest, C#config{ defragmentation_modules = DefragModules2 })
	catch _:_ ->
		io:format("~ndefragment_module value must be in the {number},{address} format.~n~n"),
		{error, [
			{init, stop, [1]}
		], C}
	end;
parse(["tls_cert_file", CertFilePath | Rest], C) ->
    AbsCertFilePath = filename:absname(CertFilePath),
    ar_util:assert_file_exists_and_readable(AbsCertFilePath),
    parse(Rest, C#config{ tls_cert_file = AbsCertFilePath });
parse(["tls_key_file", KeyFilePath | Rest], C) ->
    AbsKeyFilePath = filename:absname(KeyFilePath),
    ar_util:assert_file_exists_and_readable(AbsKeyFilePath),
    parse(Rest, C#config{ tls_key_file = AbsKeyFilePath });
parse(["http_api.tcp.idle_timeout_seconds", Num | Rest], C) ->
	parse(Rest, C#config { http_api_transport_idle_timeout = list_to_integer(Num) * 1000 });
parse(["coordinated_mining" | Rest], C) ->
	parse(Rest, C#config{ coordinated_mining = true });
parse(["cm_api_secret", CMSecret | Rest], C)
		when length(CMSecret) >= ?INTERNAL_API_SECRET_MIN_LEN ->
	parse(Rest, C#config{ cm_api_secret = list_to_binary(CMSecret) });
parse(["cm_api_secret", _ | _], C) ->
	io:format("~nThe cm_api_secret must be at least ~B characters long.~n~n",
			[?INTERNAL_API_SECRET_MIN_LEN]),
	{error, [
		{init, stop, [1]}
	], C};
parse(["cm_poll_interval", Num | Rest], C) ->
	parse(Rest, C#config{ cm_poll_interval = list_to_integer(Num) });
parse(["cm_peer", Peer | Rest], C = #config{ cm_peers = Ps }) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ValidPeer} when is_list(ValidPeer) ->
			parse(Rest, C#config{ cm_peers = ValidPeer ++ Ps });
		{error, _} ->
			io:format("Peer ~p is invalid.~n", [Peer]),
			parse(Rest, C)
	end;
parse(["cm_exit_peer", Peer | Rest], C) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, [ValidPeer|_]} ->
			parse(Rest, C#config{ cm_exit_peer = ValidPeer });
		{error, _} ->
			io:format("Peer ~p is invalid.~n", [Peer]),
			parse(Rest, C)
	end;
parse(["cm_out_batch_timeout", Num | Rest], C) ->
	parse(Rest, C#config{ cm_out_batch_timeout = list_to_integer(Num) });
parse(["is_pool_server" | Rest], C) ->
	parse(Rest, C#config{ is_pool_server = true });
parse(["is_pool_client" | Rest], C) ->
	parse(Rest, C#config{ is_pool_client = true });
parse(["pool_api_key", Key | Rest], C) ->
	parse(Rest, C#config{ pool_api_key = list_to_binary(Key) });
parse(["pool_server_address", Host | Rest], C) ->
	parse(Rest, C#config{ pool_server_address = list_to_binary(Host) });
parse(["pool_worker_name", Host | Rest], C) ->
	parse(Rest, C#config{ pool_worker_name = list_to_binary(Host) });
parse(["rocksdb_flush_interval", Seconds | Rest], C) ->
	parse(Rest, C#config{ rocksdb_flush_interval_s = list_to_integer(Seconds) });
parse(["rocksdb_wal_sync_interval", Seconds | Rest], C) ->
	parse(Rest, C#config{ rocksdb_wal_sync_interval_s = list_to_integer(Seconds) });

%% tcp shutdown procedure
parse(["network.tcp.connection_timeout", Delay|Rest], C) ->
	parse(Rest, C#config{ shutdown_tcp_connection_timeout = list_to_integer(Delay) });
parse(["network.tcp.shutdown.mode", RawMode|Rest], C) ->
	case RawMode of
		"shutdown" ->
			parse(Rest, C#config{ shutdown_tcp_mode = shutdown});
		"close" ->
			parse(Rest, C#config{ shutdown_tcp_mode = close });
		Mode ->
			io:format("Mode ~p is invalid.~n", [Mode]),
			parse(Rest, C)
	end;

%% global socket configuration
parse(["network.socket.backend", Backend|Rest], C) ->
	case Backend of
		"inet" ->
			parse(Rest, C#config{ 'socket.backend' = inet });
		"socket" ->
			parse(Rest, C#config{ 'socket.backend' = socket });
		_ ->
			io:format("Invalid socket.backend ~p.", [Backend]),
			parse(Rest, C)
	end;

%% gun http client configuration
parse(["http_client.http.keepalive", "infinity"|Rest], C) ->
	parse(Rest, C#config{ 'http_client.http.keepalive' = infinity});
parse(["http_client.http.keepalive", Keepalive|Rest], C) ->
	try list_to_integer(Keepalive) of
		K when K >= 0 ->
			parse(Rest, C#config{ 'http_client.http.keepalive' = K });
		_ ->
			io:format("Invalid http_client.http.keepalive ~p.", [Keepalive]),
			parse(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_client.http.keepalive ~p.", [Keepalive]),
			parse(Rest, C)
	end;
parse(["http_client.tcp.delay_send", DelaySend|Rest], C) ->
	case DelaySend of
		"true" ->
			parse(Rest, C#config{ 'http_client.tcp.delay_send' = true });
		"false" ->
			parse(Rest, C#config{ 'http_client.tcp.delay_send' = false });
		_ ->
			io:format("Invalid http_client.tcp.delay_send ~p.", [DelaySend]),
			parse(Rest, C)
	end;
parse(["http_client.tcp.keepalive", Keepalive|Rest], C) ->
	case Keepalive of
		"true" ->
			parse(Rest, C#config{ 'http_client.tcp.keepalive' = true });
		"false" ->
			parse(Rest, C#config{ 'http_client.tcp.keepalive' = false });
		_ ->
			io:format("Invalid http_client.tcp.keepalive ~p.", [Keepalive]),
			parse(Rest, C)
	end;
parse(["http_client.tcp.linger", Linger|Rest], C) ->
	case Linger of
		"true" ->
			parse(Rest, C#config{ 'http_client.tcp.linger' = true });
		"false" ->
			parse(Rest, C#config{ 'http_client.tcp.linger' = false});
		_ ->
			io:format("Invalid http_client.tcp.linger ~p.", [Linger]),
			parse(Rest, C)
	end;
parse(["http_client.tcp.linger_timeout", Timeout|Rest], C) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			parse(Rest, C#config{ 'http_client.tcp.linger_timeout' = T });
		_ ->
			io:format("Invalid http_client.tcp.linger_timeout ~p.", [Timeout]),
			parse(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_client.tcp.linger_timeout timeout ~p.", [Timeout]),
			parse(Rest, C)
	end;
parse(["http_client.tcp.nodelay", Nodelay|Rest], C) ->
	case Nodelay of
		"true" ->
			parse(Rest, C#config{ 'http_client.tcp.nodelay' = true });
		"false" ->
			parse(Rest, C#config{ 'http_client.tcp.nodelay' = false });
		_ ->
			io:format("Invalid http_client.tcp.nodelay ~p.", [Nodelay]),
			parse(Rest, C)
	end;
parse(["http_client.tcp.send_timeout_close", Value|Rest], C) ->
	case Value of
		"true" ->
			parse(Rest, C#config{ 'http_client.tcp.send_timeout_close' = true });
		"false" ->
			parse(Rest, C#config{ 'http_client.tcp.send_timeout_close' = false });
		_ ->
			io:format("Invalid http_client.tcp.send_timeout_close ~p.", [Value]),
			parse(Rest, C)
	end;
parse(["http_client.tcp.send_timeout", Timeout|Rest], C) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			parse(Rest, C#config{ 'http_client.tcp.send_timeout' = T });
		_ ->
			io:format("Invalid http_client.tcp.send_timeout ~p.", [Timeout]),
			parse(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_client.tcp.send_timeout ~p.", [Timeout]),
			parse(Rest, C)
	end;

%% cowboy http server configuration
parse(["http_api.http.active_n", Active|Rest], C) ->
	try list_to_integer(Active) of
		N when N >= 1 ->
			parse(Rest, C#config{ 'http_api.http.active_n' = N });
		_ ->
			io:format("Invalid http_api.http.active_n ~p.", [Active]),
			parse(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.http.active_n ~p.", [Active]),
			parse(Rest, C)
	end;
parse(["http_api.http.inactivity_timeout", Timeout|Rest], C) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			parse(Rest, C#config{ 'http_api.http.inactivity_timeout' = T });
		_ ->
			io:format("Invalid http_api.http.inactivity_timeout ~p.", [Timeout]),
			parse(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.http.inactivity_timeout ~p.", [Timeout]),
			parse(Rest, C)
	end;
parse(["http_api.http.linger_timeout", Timeout|Rest], C) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			parse(Rest, C#config{ 'http_api.http.linger_timeout' = T });
		_ ->
			io:format("Invalid http_api.http.linger_timeout ~p.", [Timeout]),
			parse(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.http.linger_timeout ~p.", [Timeout]),
			parse(Rest, C)
	end;
parse(["http_api.http.request_timeout", Timeout|Rest], C) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			parse(Rest, C#config{ 'http_api.http.request_timeout' = T });
		_ ->
			io:format("Invalid http_api.http.request_timeout ~p.", [Timeout]),
			parse(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.http.request_timeout ~p.", [Timeout]),
			parse(Rest, C)
	end;
parse(["http_api.tcp.backlog", Backlog|Rest], C) ->
	try list_to_integer(Backlog)of
		B when B >= 1 ->
			parse(Rest, C#config{ 'http_api.tcp.backlog' = B });
		_ ->
			io:format("Invalid http_api.tcp.backlog ~p.", [Backlog]),
			parse(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.tcp.backlog ~p.", [Backlog]),
			parse(Rest, C)
	end;
parse(["http_api.tcp.delay_send", DelaySend|Rest], C) ->
	case DelaySend of
		"true" ->
			parse(Rest, C#config{ 'http_api.tcp.delay_send' = true });
		"false" ->
			parse(Rest, C#config{ 'http_api.tcp.delay_send' = false });
		_ ->
			io:format("Invalid http_api.tcp.delay_send ~p.", [DelaySend]),
			parse(Rest, C)
	end;
parse(["http_api.tcp.keepalive", "true"|Rest], C) ->
	parse(Rest, C#config{ 'http_api.tcp.keepalive' = true});
parse(["http_api.tcp.keepalive", "false"|Rest], C) ->
	parse(Rest, C#config{ 'http_api.tcp.keepalive' = false});
parse(["http_api.tcp.keepalive", Keepalive|Rest], C) ->
	io:format("Invalid http_api.tcp.keepalive ~p.", [Keepalive]),
	parse(Rest, C);
parse(["http_api.tcp.linger", Linger|Rest], C) ->
	case Linger of
		"true" ->
			parse(Rest, C#config{ 'http_api.tcp.linger' = true });
		"false" ->
			parse(Rest, C#config{ 'http_api.tcp.linger' = false});
		_ ->
			io:format("Invalid http_api.tcp.linger ~p.", [Linger]),
			parse(Rest, C)
	end;
parse(["http_api.tcp.linger_timeout", Timeout|Rest], C) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			parse(Rest, C#config{ 'http_api.tcp.linger_timeout' = T });
		_ ->
			io:format("Invalid http_api.tcp.linger_timeout ~p.", [Timeout]),
			parse(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.tcp.linger_timeout ~p.", [Timeout]),
			parse(Rest, C)
	end;
parse(["http_api.tcp.listener_shutdown", "brutal_kill"|Rest], C) ->
	parse(Rest, C#config{ 'http_api.tcp.listener_shutdown' = brutal_kill});
parse(["http_api.tcp.listener_shutdown", "infinity"|Rest], C) ->
	parse(Rest, C#config{ 'http_api.tcp.listener_shutdown' = infinity });
parse(["http_api.tcp.listener_shutdown", Shutdown|Rest], C) ->
	try list_to_integer(Shutdown) of
		S when S >= 0 ->
			parse(Rest, C#config{ 'http_api.tcp.listener_shutdown' = S });
		_ ->
			io:format("Invalid http_api.tcp.listener_shutdown ~p.", [Shutdown]),
			parse(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.tcp.listener_shutdown ~p.", [Shutdown]),
			parse(Rest, C)
	end;
parse(["http_api.tcp.nodelay", Nodelay|Rest], C) ->
	case Nodelay of
		"true" ->
			parse(Rest, C#config{ 'http_api.tcp.nodelay' = true });
		"false" ->
			parse(Rest, C#config{ 'http_api.tcp.nodelay' = false });
		_ ->
			io:format("Invalid http_api.tcp.nodelay ~p.", [Nodelay]),
			parse(Rest, C)
	end;
parse(["http_api.tcp.num_acceptors", Acceptors|Rest], C) ->
	try list_to_integer(Acceptors) of
		N when N >= 0 ->
			parse(Rest, C#config{ 'http_api.tcp.num_acceptors' = N });
		_ ->
			io:format("Invalid http_api.tcp.num_acceptors ~p.", [Acceptors]),
			parse(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.tcp.num_acceptors ~p.", [Acceptors]),
			parse(Rest, C)
	end;
parse(["http_api.tcp.send_timeout_close", Value|Rest], C) ->
	case Value of
		"true" ->
			parse(Rest, C#config{ 'http_api.tcp.send_timeout_close' = true });
		"false" ->
			parse(Rest, C#config{ 'http_api.tcp.send_timeout_close' = false });
		_ ->
			io:format("Invalid http_api.tcp.send_timeout_close ~p.", [Value]),
			parse(Rest, C)
	end;
parse(["http_api.tcp.send_timeout", Timeout|Rest], C) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			parse(Rest, C#config{ 'http_api.tcp.send_timeout' = T });
		_ ->
			io:format("Invalid http_api.tcp.send_timeout ~p.", [Timeout]),
			parse(Rest, C)
	catch
		_:_ ->
			io:format("Invalid http_api.tcp.send_timeout ~p.", [Timeout]),
			parse(Rest, C)
	end;

%% Undocumented/unsupported options
parse(["chunk_storage_file_size", Num | Rest], C) ->
	parse(Rest, C#config{ chunk_storage_file_size = list_to_integer(Num) });

parse([Arg | _Rest], C) ->
	io:format("~nUnknown argument: ~s.~n", [Arg]),
	{error, [
		{?MODULE, show_help, []}
	], C}.

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
		C = eval(X, #config{}),
		lists:foreach(
			fun({_, Index, Value}) ->
				?assertEqual(element(Index, C), Value)
			end,
			Tests
		)
	end}.
