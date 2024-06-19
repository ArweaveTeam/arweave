-module(ar_config).

-export([use_remote_vdf_server/0, pull_from_remote_vdf_server/0, compute_own_vdf/0,
		is_vdf_server/0, is_public_vdf_server/0,
		parse/1, parse_storage_module/1, log_config/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_p3.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

use_remote_vdf_server() ->
	{ok, Config} = application:get_env(arweave, config),
	case Config#config.nonce_limiter_server_trusted_peers of
		[] ->
			false;
		_ ->
			true
	end.

pull_from_remote_vdf_server() ->
	{ok, Config} = application:get_env(arweave, config),
	not lists:member(vdf_server_pull, Config#config.disable).

compute_own_vdf() ->
	{ok, Config} = application:get_env(arweave, config),
	case Config#config.nonce_limiter_server_trusted_peers of
		[] ->
			%% Not a VDF client - compute VDF unless explicitly disabled.
			not lists:member(compute_own_vdf, Config#config.disable);
		_ ->
			%% Computing your own VDF needs to be explicitly enabled on a VDF client.
			lists:member(compute_own_vdf, Config#config.enable)
	end.

is_vdf_server() ->
	{ok, Config} = application:get_env(arweave, config),
	case Config#config.nonce_limiter_client_peers of
		[] ->
			lists:member(public_vdf_server, Config#config.enable);
		_ ->
			true
	end.

is_public_vdf_server() ->
	{ok, Config} = application:get_env(arweave, config),
	lists:member(public_vdf_server, Config#config.enable).

parse(Config) when is_binary(Config) ->
	case ar_serialize:json_decode(Config) of
		{ok, JsonValue} -> parse_options(JsonValue);
		{error, _} -> {error, bad_json, Config}
	end.

parse_storage_module(IOList) ->
	Bin = iolist_to_binary(IOList),
	case binary:split(Bin, <<",">>, [global]) of
		[PartitionNumberBin, PackingBin, <<"repack_in_place">>, ToPackingBin] ->
			PartitionNumber = binary_to_integer(PartitionNumberBin),
			true = PartitionNumber >= 0,
			parse_storage_module(PartitionNumber, ?PARTITION_SIZE, PackingBin, ToPackingBin);
		[RangeNumberBin, RangeSizeBin, PackingBin, <<"repack_in_place">>, ToPackingBin] ->
			RangeNumber = binary_to_integer(RangeNumberBin),
			true = RangeNumber >= 0,
			RangeSize = binary_to_integer(RangeSizeBin),
			true = RangeSize >= 0,
			parse_storage_module(RangeNumber, RangeSize, PackingBin, ToPackingBin);
		[PartitionNumberBin, PackingBin] ->
			PartitionNumber = binary_to_integer(PartitionNumberBin),
			true = PartitionNumber >= 0,
			parse_storage_module(PartitionNumber, ?PARTITION_SIZE, PackingBin);
		[RangeNumberBin, RangeSizeBin, PackingBin] ->
			RangeNumber = binary_to_integer(RangeNumberBin),
			true = RangeNumber >= 0,
			RangeSize = binary_to_integer(RangeSizeBin),
			true = RangeSize >= 0,
			parse_storage_module(RangeNumber, RangeSize, PackingBin)
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

parse_options({KVPairs}) when is_list(KVPairs) ->
	parse_options(KVPairs, #config{});
parse_options(JsonValue) ->
	{error, root_not_object, JsonValue}.

parse_options([{_, null} | Rest], Config) ->
	parse_options(Rest, Config);

parse_options([{<<"config_file">>, _} | _], _) ->
	{error, config_file_set};

parse_options([{<<"peers">>, Peers} | Rest], Config) when is_list(Peers) ->
	case parse_peers(Peers, []) of
		{ok, ParsedPeers} ->
			parse_options(Rest, Config#config{ peers = ParsedPeers });
		error ->
			{error, bad_peers, Peers}
	end;
parse_options([{<<"peers">>, Peers} | _], _) ->
	{error, {bad_type, peers, array}, Peers};

parse_options([{<<"block_gossip_peers">>, Peers} | Rest], Config) when is_list(Peers) ->
	case parse_peers(Peers, []) of
		{ok, ParsedPeers} ->
			parse_options(Rest, Config#config{ block_gossip_peers = ParsedPeers });
		error ->
			{error, bad_peers, Peers}
	end;
parse_options([{<<"block_gossip_peers">>, Peers} | _], _) ->
	{error, {bad_type, peers, array}, Peers};

parse_options([{<<"local_peers">>, Peers} | Rest], Config) when is_list(Peers) ->
	case parse_peers(Peers, []) of
		{ok, ParsedPeers} ->
			parse_options(Rest, Config#config{ local_peers = ParsedPeers });
		error ->
			{error, bad_local_peers, Peers}
	end;
parse_options([{<<"local_peers">>, Peers} | _], _) ->
	{error, {bad_type, local_peers, array}, Peers};

parse_options([{<<"start_from_latest_state">>, true} | Rest], Config) ->
	parse_options(Rest, Config#config{ start_from_latest_state = true });
parse_options([{<<"start_from_latest_state">>, false} | Rest], Config) ->
	parse_options(Rest, Config#config{ start_from_latest_state = false });
parse_options([{<<"start_from_latest_state">>, Opt} | _], _) ->
	{error, {bad_type, start_from_latest_state, boolean}, Opt};

parse_options([{<<"start_from_block">>, H} | Rest], Config) when is_binary(H) ->
	case ar_util:safe_decode(H) of
		{ok, Decoded} when byte_size(Decoded) == 48 ->
			parse_options(Rest, Config#config{ start_from_block = Decoded });
		_ ->
			{error, bad_block, H}
	end;
parse_options([{<<"start_from_block">>, Opt} | _], _) ->
	{error, {bad_type, start_from_block, string}, Opt};

parse_options([{<<"start_from_block_index">>, true} | Rest], Config) ->
	parse_options(Rest, Config#config{ start_from_latest_state = true });
parse_options([{<<"start_from_block_index">>, false} | Rest], Config) ->
	parse_options(Rest, Config#config{ start_from_latest_state = false });
parse_options([{<<"start_from_block_index">>, Opt} | _], _) ->
	{error, {bad_type, start_from_block_index, boolean}, Opt};

parse_options([{<<"mine">>, true} | Rest], Config) ->
	parse_options(Rest, Config#config{ mine = true });
parse_options([{<<"mine">>, false} | Rest], Config) ->
	parse_options(Rest, Config);
parse_options([{<<"mine">>, Opt} | _], _) ->
	{error, {bad_type, mine, boolean}, Opt};

parse_options([{<<"port">>, Port} | Rest], Config) when is_integer(Port) ->
	parse_options(Rest, Config#config{ port = Port });
parse_options([{<<"port">>, Port} | _], _) ->
	{error, {bad_type, port, number}, Port};

parse_options([{<<"data_dir">>, DataDir} | Rest], Config) when is_binary(DataDir) ->
	parse_options(Rest, Config#config{ data_dir = binary_to_list(DataDir) });
parse_options([{<<"data_dir">>, DataDir} | _], _) ->
	{error, {bad_type, data_dir, string}, DataDir};

parse_options([{<<"log_dir">>, Dir} | Rest], Config) when is_binary(Dir) ->
	parse_options(Rest, Config#config{ log_dir = binary_to_list(Dir) });
parse_options([{<<"log_dir">>, Dir} | _], _) ->
	{error, {bad_type, log_dir, string}, Dir};

parse_options([{<<"storage_modules">>, L} | Rest], Config) when is_list(L) ->
	try
		{StorageModules, RepackInPlaceStorageModules} =
			lists:foldr(
				fun(Bin, {Acc1, Acc2}) ->
					case parse_storage_module(Bin) of
						{ok, Module} ->
							{[Module | Acc1], Acc2};
						{repack_in_place, Module} ->
							{Acc1, [Module | Acc2]}
					end
				end,
				{[], []},
				L
			),
		parse_options(Rest, Config#config{
				storage_modules = StorageModules,
				repack_in_place_storage_modules = RepackInPlaceStorageModules })
	catch _:_ ->
		{error, {bad_format, storage_modules, "an array of "
				"\"{number},{address}[,repack_in_place,{to_packing}]\""}, L}
	end;
parse_options([{<<"storage_modules">>, Bin} | _], _) ->
	{error, {bad_type, storage_modules, array}, Bin};

parse_options([{<<"repack_batch_size">>, N} | Rest], Config) when is_integer(N) ->
	parse_options(Rest, Config#config{ repack_batch_size = N });
parse_options([{<<"repack_batch_size">>, Opt} | _], _) ->
	{error, {bad_type, repack_batch_size, number}, Opt};

parse_options([{<<"polling">>, Frequency} | Rest], Config) when is_integer(Frequency) ->
	parse_options(Rest, Config#config{ polling = Frequency });
parse_options([{<<"polling">>, Opt} | _], _) ->
	{error, {bad_type, polling, number}, Opt};

parse_options([{<<"block_pollers">>, N} | Rest], Config) when is_integer(N) ->
	parse_options(Rest, Config#config{ block_pollers = N });
parse_options([{<<"block_pollers">>, Opt} | _], _) ->
	{error, {bad_type, block_pollers, number}, Opt};

parse_options([{<<"no_auto_join">>, true} | Rest], Config) ->
	parse_options(Rest, Config#config{ auto_join = false });
parse_options([{<<"no_auto_join">>, false} | Rest], Config) ->
	parse_options(Rest, Config);
parse_options([{<<"no_auto_join">>, Opt} | _], _) ->
	{error, {bad_type, no_auto_join, boolean}, Opt};

parse_options([{<<"join_workers">>, N} | Rest], Config) when is_integer(N)->
	parse_options(Rest, Config#config{ join_workers = N });
parse_options([{<<"join_workers">>, Opt} | _], _) ->
	{error, {bad_type, join_workers, number}, Opt};

parse_options([{<<"diff">>, Diff} | Rest], Config) when is_integer(Diff) ->
	parse_options(Rest, Config#config{ diff = Diff });
parse_options([{<<"diff">>, Diff} | _], _) ->
	{error, {bad_type, diff, number}, Diff};

parse_options([{<<"mining_addr">>, Addr} | Rest], Config) when is_binary(Addr) ->
	case Config#config.mining_addr of
		not_set ->
			case ar_util:safe_decode(Addr) of
				{ok, D} when byte_size(D) == 32 ->
					parse_options(Rest, Config#config{ mining_addr = D });
				_ -> {error, bad_mining_addr, Addr}
			end;
		_ ->
			{error, at_most_one_mining_addr_is_supported, Addr}
	end;
parse_options([{<<"mining_addr">>, Addr} | _], _) ->
	{error, {bad_type, mining_addr, string}, Addr};

parse_options([{<<"hashing_threads">>, Threads} | Rest], Config) when is_integer(Threads) ->
	parse_options(Rest, Config#config{ hashing_threads = Threads });
parse_options([{<<"hashing_threads">>, Threads} | _], _) ->
	{error, {bad_type, hashing_threads, number}, Threads};

parse_options([{<<"data_cache_size_limit">>, Limit} | Rest], Config)
		when is_integer(Limit) ->
	parse_options(Rest, Config#config{ data_cache_size_limit = Limit });
parse_options([{<<"data_cache_size_limit">>, Limit} | _], _) ->
	{error, {bad_type, data_cache_size_limit, number}, Limit};

parse_options([{<<"packing_cache_size_limit">>, Limit} | Rest], Config)
		when is_integer(Limit) ->
	parse_options(Rest, Config#config{ packing_cache_size_limit = Limit });
parse_options([{<<"packing_cache_size_limit">>, Limit} | _], _) ->
	{error, {bad_type, packing_cache_size_limit, number}, Limit};

parse_options([{<<"mining_server_chunk_cache_size_limit">>, Limit} | Rest], Config)
		when is_integer(Limit) ->
	parse_options(Rest, Config#config{ mining_server_chunk_cache_size_limit = Limit });
parse_options([{<<"mining_server_chunk_cache_size_limit">>, Limit} | _], _) ->
	{error, {bad_type, mining_server_chunk_cache_size_limit, number}, Limit};

parse_options([{<<"max_emitters">>, Value} | Rest], Config) when is_integer(Value) ->
	parse_options(Rest, Config#config{ max_emitters = Value });
parse_options([{<<"max_emitters">>, Value} | _], _) ->
	{error, {bad_type, max_emitters, number}, Value};

parse_options([{<<"tx_validators">>, Value} | Rest], Config)
		when is_integer(Value) ->
	parse_options(Rest, Config#config{ tx_validators = Value });
parse_options([{<<"tx_validators">>, Value} | _], _) ->
	{error, {bad_type, tx_validators, number}, Value};

parse_options([{<<"post_tx_timeout">>, Value} | Rest], Config)
		when is_integer(Value) ->
	parse_options(Rest, Config#config{ post_tx_timeout = Value });
parse_options([{<<"post_tx_timeout">>, Value} | _], _) ->
	{error, {bad_type, post_tx_timeout, number}, Value};

parse_options([{<<"tx_propagation_parallelization">>, Value} | Rest], Config)
		when is_integer(Value) ->
	parse_options(Rest, Config#config{ tx_propagation_parallelization = Value });
parse_options([{<<"tx_propagation_parallelization">>, Value} | _], _) ->
	{error, {bad_type, tx_propagation_parallelization, number}, Value};

parse_options([{<<"max_propagation_peers">>, Value} | Rest], Config)
		when is_integer(Value) ->
	parse_options(Rest, Config#config{ max_propagation_peers = Value });
parse_options([{<<"max_propagation_peers">>, Value} | _], _) ->
	{error, {bad_type, max_propagation_peers, number}, Value};

parse_options([{<<"max_block_propagation_peers">>, Value} | Rest], Config)
		when is_integer(Value) ->
	parse_options(Rest, Config#config{ max_block_propagation_peers = Value });
parse_options([{<<"max_block_propagation_peers">>, Value} | _], _) ->
	{error, {bad_type, max_block_propagation_peers, number}, Value};

parse_options([{<<"sync_jobs">>, Value} | Rest], Config)
		when is_integer(Value) ->
	parse_options(Rest, Config#config{ sync_jobs = Value });
parse_options([{<<"sync_jobs">>, Value} | _], _) ->
	{error, {bad_type, sync_jobs, number}, Value};

parse_options([{<<"header_sync_jobs">>, Value} | Rest], Config)
		when is_integer(Value) ->
	parse_options(Rest, Config#config{ header_sync_jobs = Value });
parse_options([{<<"header_sync_jobs">>, Value} | _], _) ->
	{error, {bad_type, header_sync_jobs, number}, Value};

parse_options([{<<"disk_pool_jobs">>, Value} | Rest], Config)
		when is_integer(Value) ->
	parse_options(Rest, Config#config{ disk_pool_jobs = Value });
parse_options([{<<"disk_pool_jobs">>, Value} | _], _) ->
	{error, {bad_type, disk_pool_jobs, number}, Value};

parse_options([{<<"requests_per_minute_limit">>, L} | Rest], Config) when is_integer(L) ->
	parse_options(Rest, Config#config{ requests_per_minute_limit = L });
parse_options([{<<"requests_per_minute_limit">>, L} | _], _) ->
	{error, {bad_type, requests_per_minute_limit, number}, L};

parse_options([{<<"requests_per_minute_limit_by_ip">>, Object} | Rest], Config)
		when is_tuple(Object) ->
	case parse_requests_per_minute_limit_by_ip(Object) of
		{ok, ParsedMap} ->
			parse_options(Rest, Config#config{ requests_per_minute_limit_by_ip = ParsedMap });
		error ->
			{error, bad_requests_per_minute_limit_by_ip, Object}
	end;
parse_options([{<<"requests_per_minute_limit_by_ip">>, Object} | _], _) ->
	{error, {bad_type, requests_per_minute_limit_by_ip, object}, Object};

parse_options([{<<"transaction_blacklists">>, TransactionBlacklists} | Rest], Config)
		when is_list(TransactionBlacklists) ->
	case safe_map(fun binary_to_list/1, TransactionBlacklists) of
		{ok, TransactionBlacklistStrings} ->
			parse_options(Rest, Config#config{
				transaction_blacklist_files = TransactionBlacklistStrings
			});
		error ->
			{error, bad_transaction_blacklists}
	end;
parse_options([{<<"transaction_blacklists">>, TransactionBlacklists} | _], _) ->
	{error, {bad_type, transaction_blacklists, array}, TransactionBlacklists};

parse_options([{<<"transaction_blacklist_urls">>, TransactionBlacklistURLs} | Rest], Config)
		when is_list(TransactionBlacklistURLs) ->
	case safe_map(fun binary_to_list/1, TransactionBlacklistURLs) of
		{ok, TransactionBlacklistURLStrings} ->
			parse_options(Rest, Config#config{
				transaction_blacklist_urls = TransactionBlacklistURLStrings
			});
		error ->
			{error, bad_transaction_blacklist_urls}
	end;
parse_options([{<<"transaction_blacklist_urls">>, TransactionBlacklistURLs} | _], _) ->
	{error, {bad_type, transaction_blacklist_urls, array}, TransactionBlacklistURLs};

parse_options([{<<"transaction_whitelists">>, TransactionWhitelists} | Rest], Config)
		when is_list(TransactionWhitelists) ->
	case safe_map(fun binary_to_list/1, TransactionWhitelists) of
		{ok, TransactionWhitelistStrings} ->
			parse_options(Rest, Config#config{
				transaction_whitelist_files = TransactionWhitelistStrings
			});
		error ->
			{error, bad_transaction_whitelists}
	end;
parse_options([{<<"transaction_whitelists">>, TransactionWhitelists} | _], _) ->
	{error, {bad_type, transaction_whitelists, array}, TransactionWhitelists};

parse_options([{<<"transaction_whitelist_urls">>, TransactionWhitelistURLs} | Rest], Config)
		when is_list(TransactionWhitelistURLs) ->
	case safe_map(fun binary_to_list/1, TransactionWhitelistURLs) of
		{ok, TransactionWhitelistURLStrings} ->
			parse_options(Rest, Config#config{
				transaction_whitelist_urls = TransactionWhitelistURLStrings
			});
		error ->
			{error, bad_transaction_whitelist_urls}
	end;
parse_options([{<<"transaction_whitelist_urls">>, TransactionWhitelistURLs} | _], _) ->
	{error, {bad_type, transaction_whitelist_urls, array}, TransactionWhitelistURLs};

parse_options([{<<"disk_space">>, DiskSpace} | Rest], Config) when is_integer(DiskSpace) ->
	parse_options(Rest, Config#config{ disk_space = DiskSpace * 1024 * 1024 * 1024 });
parse_options([{<<"disk_space">>, DiskSpace} | _], _) ->
	{error, {bad_type, disk_space, number}, DiskSpace};

parse_options([{<<"disk_space_check_frequency">>, Frequency} | Rest], Config)
		when is_integer(Frequency) ->
	parse_options(Rest, Config#config{ disk_space_check_frequency = Frequency * 1000 });
parse_options([{<<"disk_space_check_frequency">>, Frequency} | _], _) ->
	{error, {bad_type, disk_space_check_frequency, number}, Frequency};

parse_options([{<<"init">>, true} | Rest], Config) ->
	parse_options(Rest, Config#config{ init = true });
parse_options([{<<"init">>, false} | Rest], Config) ->
	parse_options(Rest, Config#config{ init = false });
parse_options([{<<"init">>, Opt} | _], _) ->
	{error, {bad_type, init, boolean}, Opt};

parse_options([{<<"internal_api_secret">>, Secret} | Rest], Config)
		when is_binary(Secret), byte_size(Secret) >= ?INTERNAL_API_SECRET_MIN_LEN ->
	parse_options(Rest, Config#config{ internal_api_secret = Secret });
parse_options([{<<"internal_api_secret">>, Secret} | _], _) ->
	{error, bad_secret, Secret};

parse_options([{<<"enable">>, Features} | Rest], Config) when is_list(Features) ->
	case safe_map(fun(Feature) -> binary_to_atom(Feature, latin1) end, Features) of
		{ok, FeatureAtoms} ->
			parse_options(Rest, Config#config{ enable = FeatureAtoms });
		error ->
			{error, bad_enable}
	end;
parse_options([{<<"enable">>, Features} | _], _) ->
	{error, {bad_type, enable, array}, Features};

parse_options([{<<"disable">>, Features} | Rest], Config) when is_list(Features) ->
	case safe_map(fun(Feature) -> binary_to_atom(Feature, latin1) end, Features) of
		{ok, FeatureAtoms} ->
			parse_options(Rest, Config#config{ disable = FeatureAtoms });
		error ->
			{error, bad_disable}
	end;
parse_options([{<<"disable">>, Features} | _], _) ->
	{error, {bad_type, disable, array}, Features};
parse_options([{<<"gateway">>, _} | Rest], Config) ->
	?LOG_WARNING("Deprecated option found 'gateway': "
		" this option has been removed and is a no-op.", []),
	parse_options(Rest, Config);
parse_options([{<<"custom_domains">>, _} | Rest], Config) ->
	?LOG_WARNING("Deprecated option found 'custom_domains': "
			" this option has been removed and is a no-op.", []),
	parse_options(Rest, Config);
parse_options([{<<"webhooks">>, WebhookConfigs} | Rest], Config) when is_list(WebhookConfigs) ->
	case parse_webhooks(WebhookConfigs, []) of
		{ok, ParsedWebhooks} ->
			parse_options(Rest, Config#config{ webhooks = ParsedWebhooks });
		error ->
			{error, bad_webhooks, WebhookConfigs}
	end;
parse_options([{<<"webhooks">>, Webhooks} | _], _) ->
	{error, {bad_type, webhooks, array}, Webhooks};

parse_options([{<<"semaphores">>, Semaphores} | Rest], Config) when is_tuple(Semaphores) ->
	case parse_atom_number_map(Semaphores, Config#config.semaphores) of
		{ok, ParsedSemaphores} ->
			parse_options(Rest, Config#config{ semaphores = ParsedSemaphores });
		error ->
			{error, bad_semaphores, Semaphores}
	end;
parse_options([{<<"semaphores">>, Semaphores} | _], _) ->
	{error, {bad_type, semaphores, object}, Semaphores};

parse_options([{<<"max_connections">>, MaxConnections} | Rest], Config)
		when is_integer(MaxConnections) ->
	parse_options(Rest, Config#config{ max_connections = MaxConnections });

parse_options([{<<"max_gateway_connections">>, MaxGatewayConnections} | Rest], Config)
		when is_integer(MaxGatewayConnections) ->
	parse_options(Rest, Config#config{ max_gateway_connections = MaxGatewayConnections });

parse_options([{<<"max_poa_option_depth">>, MaxPOAOptionDepth} | Rest], Config)
		when is_integer(MaxPOAOptionDepth) ->
	parse_options(Rest, Config#config{ max_poa_option_depth = MaxPOAOptionDepth });

parse_options([{<<"disk_pool_data_root_expiration_time">>, D} | Rest], Config)
		when is_integer(D) ->
	parse_options(Rest, Config#config{ disk_pool_data_root_expiration_time = D });

parse_options([{<<"max_disk_pool_buffer_mb">>, D} | Rest], Config) when is_integer(D) ->
	parse_options(Rest, Config#config{ max_disk_pool_buffer_mb= D });

parse_options([{<<"max_disk_pool_data_root_buffer_mb">>, D} | Rest], Config)
		when is_integer(D) ->
	parse_options(Rest, Config#config{ max_disk_pool_data_root_buffer_mb = D });

parse_options([{<<"disk_cache_size_mb">>, D} | Rest], Config) when is_integer(D) ->
	parse_options(Rest, Config#config{ disk_cache_size = D });

parse_options([{<<"packing_rate">>, D} | Rest], Config) when is_integer(D) ->
	parse_options(Rest, Config#config{ packing_rate = D });

parse_options([{<<"max_nonce_limiter_validation_thread_count">>, D} | Rest], Config)
		when is_integer(D) ->
	parse_options(Rest, Config#config{ max_nonce_limiter_validation_thread_count = D });

parse_options([{<<"max_nonce_limiter_last_step_validation_thread_count">>, D} | Rest], Config)
		when is_integer(D) ->
	parse_options(Rest,
			Config#config{ max_nonce_limiter_last_step_validation_thread_count = D });

parse_options([{<<"vdf_server_trusted_peer">>, <<>>} | Rest], Config) ->
	parse_options(Rest, Config);
parse_options([{<<"vdf_server_trusted_peer">>, Peer} | Rest], Config) ->
	parse_options(Rest, parse_vdf_server_trusted_peer(Peer, Config));

parse_options([{<<"vdf_server_trusted_peers">>, Peers} | Rest], Config) when is_list(Peers) ->
	parse_options(Rest, parse_vdf_server_trusted_peers(Peers, Config));
parse_options([{<<"vdf_server_trusted_peers">>, Peers} | _], _) ->
	{error, {bad_type, vdf_server_trusted_peers, array}, Peers};

parse_options([{<<"vdf_client_peers">>, Peers} | Rest], Config) when is_list(Peers) ->
	parse_options(Rest, Config#config{ nonce_limiter_client_peers = Peers });
parse_options([{<<"vdf_client_peers">>, Peers} | _], _) ->
	{error, {bad_type, vdf_client_peers, array}, Peers};

parse_options([{<<"debug">>, B} | Rest], Config) when is_boolean(B) ->
	parse_options(Rest, Config#config{ debug = B });

parse_options([{<<"repair_rocksdb">>, L} | Rest], Config) ->
	parse_options(Rest, Config#config{
			repair_rocksdb = [filename:absname(binary_to_list(El)) || El <- L] });

parse_options([{<<"run_defragmentation">>, B} | Rest], Config) when is_boolean(B) ->
	parse_options(Rest, Config#config{ run_defragmentation = B });

parse_options([{<<"defragmentation_trigger_threshold">>, D} | Rest], Config)
		when is_integer(D) ->
	parse_options(Rest, Config#config{ defragmentation_trigger_threshold = D });

parse_options([{<<"block_throttle_by_ip_interval">>, D} | Rest], Config)
		when is_integer(D) ->
	parse_options(Rest, Config#config{ block_throttle_by_ip_interval = D });

parse_options([{<<"block_throttle_by_solution_interval">>, D} | Rest], Config)
		when is_integer(D) ->
	parse_options(Rest, Config#config{ block_throttle_by_solution_interval = D });

parse_options([{<<"defragment_modules">>, L} | Rest], Config) when is_list(L) ->
	try
		DefragModules =
			lists:foldr(
				fun(Bin, Acc) ->
					{ok, M} = parse_storage_module(Bin),
					[M | Acc]
				end,
				[],
				L
			),
		parse_options(Rest, Config#config{ defragmentation_modules = DefragModules })
	catch _:_ ->
		{error, {bad_format, defragment_modules, "an array of \"{number},{address}\""}, L}
	end;
parse_options([{<<"defragment_modules">>, Bin} | _], _) ->
	{error, {bad_type, defragment_modules, array}, Bin};

parse_options([{<<"p3">>, {P3Config}} | Rest], Config) ->
	try
		P3 = ar_p3_config:parse_p3(P3Config, #p3_config{}),
		parse_options(Rest, Config#config{ p3 = P3 })
	catch error:Reason ->
		{error,
			{bad_format, p3, Reason},
			P3Config}
	end;

parse_options([{<<"coordinated_mining">>, true} | Rest], Config) ->
	parse_options(Rest, Config#config{ coordinated_mining = true });
parse_options([{<<"coordinated_mining">>, false} | Rest], Config) ->
	parse_options(Rest, Config);
parse_options([{<<"coordinated_mining">>, Opt} | _], _) ->
	{error, {bad_type, coordinated_mining, boolean}, Opt};

parse_options([{<<"cm_api_secret">>, CMSecret} | Rest], Config)
  		when is_binary(CMSecret), byte_size(CMSecret) >= ?INTERNAL_API_SECRET_MIN_LEN ->
	parse_options(Rest, Config#config{ cm_api_secret = CMSecret });
parse_options([{<<"cm_api_secret">>, CMSecret} | _], _) ->
	{error, {bad_type, cm_api_secret, string}, CMSecret};

parse_options([{<<"cm_poll_interval">>, CMPollInterval} | Rest], Config)
  		when is_integer(CMPollInterval) ->
	parse_options(Rest, Config#config{ cm_poll_interval = CMPollInterval });
parse_options([{<<"cm_poll_interval">>, CMPollInterval} | _], _) ->
	{error, {bad_type, cm_poll_interval, number}, CMPollInterval};

parse_options([{<<"cm_peers">>, Peers} | Rest], Config) when is_list(Peers) ->
	case parse_peers(Peers, []) of
		{ok, ParsedPeers} ->
			parse_options(Rest, Config#config{ cm_peers = ParsedPeers });
		error ->
			{error, bad_peers, Peers}
	end;

parse_options([{<<"cm_exit_peer">>, Peer} | Rest], Config) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ParsedPeer} ->
			parse_options(Rest, Config#config{ cm_exit_peer = ParsedPeer });
		{error, _} ->
			{error, bad_cm_exit_peer, Peer}
	end;

parse_options([{<<"cm_out_batch_timeout">>, CMBatchTimeout} | Rest], Config)
  		when is_integer(CMBatchTimeout) ->
	parse_options(Rest, Config#config{ cm_out_batch_timeout = CMBatchTimeout });
parse_options([{<<"cm_out_batch_timeout">>, CMBatchTimeout} | _], _) ->
	{error, {bad_type, cm_out_batch_timeout, number}, CMBatchTimeout};

parse_options([{<<"cm_in_batch_timeout">>, _CMBatchTimeout} | Rest], Config) ->
	?LOG_WARNING("Deprecated option found 'cm_in_batch_timeout': "
		" this option has been removed and is a no-op.", []),
	parse_options(Rest, Config);

parse_options([{<<"is_pool_server">>, true} | Rest], Config) ->
	parse_options(Rest, Config#config{ is_pool_server = true });
parse_options([{<<"is_pool_server">>, false} | Rest], Config) ->
	parse_options(Rest, Config);
parse_options([{<<"is_pool_server">>, Opt} | _], _) ->
	{error, {bad_type, is_pool_server, boolean}, Opt};

parse_options([{<<"is_pool_client">>, true} | Rest], Config) ->
	parse_options(Rest, Config#config{ is_pool_client = true });
parse_options([{<<"is_pool_client">>, false} | Rest], Config) ->
	parse_options(Rest, Config);
parse_options([{<<"is_pool_client">>, Opt} | _], _) ->
	{error, {bad_type, is_pool_client, boolean}, Opt};

parse_options([{<<"pool_api_key">>, Key} | Rest], Config) when is_binary(Key) ->
	parse_options(Rest, Config#config{ pool_api_key = Key });
parse_options([{<<"pool_api_key">>, Key} | _], _) ->
	{error, {bad_type, pool_api_key, string}, Key};

parse_options([{<<"pool_server_address">>, Host} | Rest], Config) when is_binary(Host) ->
	parse_options(Rest, Config#config{ pool_server_address = Host });
parse_options([{<<"pool_server_address">>, Host} | _], _) ->
	{error, {bad_type, pool_server_address, string}, Host};

%% Undocumented/unsupported options
parse_options([{<<"chunk_storage_file_size">>, ChunkGroupSize} | Rest], Config)
  		when is_integer(ChunkGroupSize) ->
	parse_options(Rest, Config#config{ chunk_storage_file_size = ChunkGroupSize });
parse_options([{<<"chunk_storage_file_size">>, ChunkGroupSize} | _], _) ->
	{error, {bad_type, chunk_storage_file_size, number}, ChunkGroupSize};

parse_options([Opt | _], _) ->
	{error, unknown, Opt};
parse_options([], Config) ->
	{ok, Config}.

parse_storage_module(RangeNumber, RangeSize, PackingBin) ->
	Packing =
		case PackingBin of
			<<"unpacked">> ->
				unpacked;
			<< MiningAddr:43/binary, ":", PackingDifficultyBin/binary >> ->
				PackingDifficulty = binary_to_integer(PackingDifficultyBin),
				true = PackingDifficulty >= 1
						andalso PackingDifficulty =< ?MAX_PACKING_DIFFICULTY,
				{composite, ar_util:decode(MiningAddr), PackingDifficulty};
			MiningAddr when byte_size(MiningAddr) == 43 ->
				{spora_2_6, ar_util:decode(MiningAddr)}
		end,
	{ok, {RangeSize, RangeNumber, Packing}}.

parse_storage_module(RangeNumber, RangeSize, PackingBin, ToPackingBin) ->
	Packing =
		case PackingBin of
			<<"unpacked">> ->
				unpacked;
			<< MiningAddr:43/binary, ":", PackingDifficultyBin/binary >> ->
				PackingDifficulty = binary_to_integer(PackingDifficultyBin),
				true = PackingDifficulty >= 1
						andalso PackingDifficulty =< ?MAX_PACKING_DIFFICULTY,
				{composite, ar_util:decode(MiningAddr), PackingDifficulty};
			MiningAddr when byte_size(MiningAddr) == 43 ->
				{spora_2_6, ar_util:decode(MiningAddr)}
		end,
	ToPacking =
		case ToPackingBin of
			<<"unpacked">> ->
				unpacked;
			<< ToMiningAddr:43/binary, ":", ToPackingDifficultyBin/binary >> ->
				ToPackingDifficulty = binary_to_integer(ToPackingDifficultyBin),
				true = ToPackingDifficulty >= 1
						andalso ToPackingDifficulty =< ?MAX_PACKING_DIFFICULTY,
				{composite, ar_util:decode(ToMiningAddr), ToPackingDifficulty};
			ToMiningAddr when byte_size(ToMiningAddr) == 43 ->
				{spora_2_6, ar_util:decode(ToMiningAddr)}
		end,
	{repack_in_place, {{RangeSize, RangeNumber, Packing}, ToPacking}}.

safe_map(Fun, List) ->
	try
		{ok, lists:map(Fun, List)}
	catch
		_:_ -> error
	end.

parse_peers([Peer | Rest], ParsedPeers) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ParsedPeer} -> parse_peers(Rest, [ParsedPeer | ParsedPeers]);
		{error, _} -> error
	end;
parse_peers([], ParsedPeers) ->
	{ok, lists:reverse(ParsedPeers)}.

parse_webhooks([{WebhookConfig} | Rest], ParsedWebhookConfigs) when is_list(WebhookConfig) ->
	case parse_webhook(WebhookConfig, #config_webhook{}) of
		{ok, ParsedWebhook} -> parse_webhooks(Rest, [ParsedWebhook | ParsedWebhookConfigs]);
		error -> error
	end;
parse_webhooks([_ | _], _) ->
	error;
parse_webhooks([], ParsedWebhookConfigs) ->
	{ok, lists:reverse(ParsedWebhookConfigs)}.

parse_webhook([{<<"events">>, Events} | Rest], Webhook) when is_list(Events) ->
	case parse_webhook_events(Events, []) of
		{ok, ParsedEvents} ->
			parse_webhook(Rest, Webhook#config_webhook{ events = ParsedEvents });
		error ->
			error
	end;
parse_webhook([{<<"events">>, _} | _], _) ->
	error;
parse_webhook([{<<"url">>, Url} | Rest], Webhook) when is_binary(Url) ->
	parse_webhook(Rest, Webhook#config_webhook{ url = Url });
parse_webhook([{<<"url">>, _} | _], _) ->
	error;
parse_webhook([{<<"headers">>, {Headers}} | Rest], Webhook) when is_list(Headers) ->
	parse_webhook(Rest, Webhook#config_webhook{ headers = Headers });
parse_webhook([{<<"headers">>, _} | _], _) ->
	error;
parse_webhook([], Webhook) ->
	{ok, Webhook}.

parse_webhook_events([Event | Rest], Events) ->
	case Event of
		<<"transaction">> -> parse_webhook_events(Rest, [transaction | Events]);
		<<"transaction_data">> -> parse_webhook_events(Rest, [transaction_data | Events]);
		<<"block">> -> parse_webhook_events(Rest, [block | Events]);
		_ -> error
	end;
parse_webhook_events([], Events) ->
	{ok, lists:reverse(Events)}.

parse_atom_number_map({[Pair | Pairs]}, Parsed) when is_tuple(Pair) ->
	parse_atom_number_map({Pairs}, parse_atom_number(Pair, Parsed));
parse_atom_number_map({[]}, Parsed) ->
	{ok, Parsed};
parse_atom_number_map(_, _) ->
	error.

parse_atom_number({Name, Number}, Parsed) when is_binary(Name), is_number(Number) ->
	maps:put(binary_to_atom(Name), Number, Parsed);
parse_atom_number({Key, Value}, Parsed) ->
	?LOG_WARNING([{event, parse_config_bad_type},
		{key, io_lib:format("~p", [Key])}, {value, io_lib:format("~p", [Value])}]),
	Parsed.

parse_requests_per_minute_limit_by_ip(Input) ->
	parse_requests_per_minute_limit_by_ip(Input, #{}).

parse_requests_per_minute_limit_by_ip({[{IP, Object} | Pairs]}, Parsed) ->
	case ar_util:safe_parse_peer(IP) of
		{error, invalid} ->
			error;
		{ok, {A, B, C, D, _Port}} ->
			case parse_atom_number_map(Object, #{}) of
				error ->
					error;
				{ok, ParsedMap} ->
					parse_requests_per_minute_limit_by_ip({Pairs},
							maps:put({A, B, C, D}, ParsedMap, Parsed))
			end
	end;
parse_requests_per_minute_limit_by_ip({[]}, Parsed) ->
	{ok, Parsed};
parse_requests_per_minute_limit_by_ip(_, _) ->
	error.

parse_vdf_server_trusted_peers([Peer | Rest], Config) ->
	Config2 = parse_vdf_server_trusted_peer(Peer, Config),
	parse_vdf_server_trusted_peers(Rest, Config2);
parse_vdf_server_trusted_peers([], Config) ->
	Config.

parse_vdf_server_trusted_peer(Peer, Config) when is_binary(Peer) ->
	parse_vdf_server_trusted_peer(binary_to_list(Peer), Config);
parse_vdf_server_trusted_peer(Peer, Config) ->
	#config{ nonce_limiter_server_trusted_peers = Peers } = Config,
	Config#config{ nonce_limiter_server_trusted_peers = Peers ++ [Peer] }.

log_config(Config) ->
	Fields = record_info(fields, config),
	?LOG_INFO("=============== Start Config ==============="),
	log_config(Config, Fields, 2, []),
	?LOG_INFO("=============== End Config   ===============").

log_config(_Config, [], _Index, _Acc) ->
	ok;
log_config(Config, [Field | Rest], Index, Acc) ->
	FieldValue = erlang:element(Index, Config),
	%% Wrap formatting in a try/catch just in case - we don't want any issues in formatting
	%% to cause a crash.
	FormattedValue = try
		log_config_value(Field, FieldValue)
	catch _:_ ->
		FieldValue
	end,
	Line = ?LOG_INFO("~s: ~tp", [atom_to_list(Field), FormattedValue]),
	log_config(Config, Rest, Index+1, [Line | Acc]).

log_config_value(peers, FieldValue) ->
	format_peers(FieldValue);
log_config_value(block_gossip_peers, FieldValue) ->
	format_peers(FieldValue);
log_config_value(local_peers, FieldValue) ->
	format_peers(FieldValue);
log_config_value(mining_addr, FieldValue) ->
	format_binary(FieldValue);
log_config_value(start_from_block, FieldValue) ->
	format_binary(FieldValue);
log_config_value(storage_modules, FieldValue) ->
	[format_storage_module(StorageModule) || StorageModule <- FieldValue];
log_config_value(repack_in_place_storage_modules, FieldValue) ->
	[format_storage_module(StorageModule) || {StorageModule, _} <- FieldValue];
log_config_value(_, FieldValue) ->
	FieldValue.

format_peers(Peers) ->
	[ar_util:format_peer(Peer) || Peer <- Peers].
format_binary(Address) ->
	ar_util:encode(Address).
format_storage_module({RangeSize, RangeNumber, {spora_2_6, MiningAddress}}) ->
	{RangeSize, RangeNumber, {spora_2_6, format_binary(MiningAddress)}};
format_storage_module({RangeSize, RangeNumber, {composite, MiningAddress, PackingDiff}}) ->
	{RangeSize, RangeNumber, {composite, format_binary(MiningAddress), PackingDiff}};
format_storage_module(StorageModule) ->
	StorageModule.
