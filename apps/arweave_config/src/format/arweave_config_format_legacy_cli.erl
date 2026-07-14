%%% @doc Legacy command-line argument parser - parses pre-2.9.6 command line
%%% arguments and writes them to the options registry.
-module(arweave_config_format_legacy_cli).
-compile(warnings_as_errors).
-export([
	parse/1,
	find_config_file/1
]).
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_verify_chunks.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

%% @doc Find at most one `config_file PATH' pair in a legacy CLI arg
%% list. Returns `none' if no `config_file' keyword is present,
%% `{ok, Path}' for a single occurrence, or `{error, _}' if the
%% keyword appears more than once or has no value.
-spec find_config_file(Args) -> Return when
	Args :: [string()],
	Return :: none | {ok, string()} | {error, term()}.
find_config_file(Args) ->
	find_config_file(Args, none, 1).

find_config_file([], Found, _Pos) ->
	Found;
find_config_file(["config_file", Path | Rest], Found, Pos) ->
	case Found of
		none -> find_config_file(Rest, {ok, Path}, Pos + 2);
		{ok, _} -> {error, multiple_config_files}
	end;
find_config_file(["config_file"], _Found, Pos) ->
	{error, #{
		reason => <<"missing value">>,
		type => path,
		position => Pos + 1
	}};
find_config_file([_Arg | Rest], Found, Pos) ->
	find_config_file(Rest, Found, Pos + 1).

%% @doc Parse a legacy CLI argument list, writing each parsed option
%% into the options registry.
-spec parse(Args) -> Return when
	Args :: [string()],
	Return :: ok | {error, Actions},
	Actions :: [{M, F, A}],
	M :: atom(),
	F :: atom(),
	A :: [term()].
parse([]) ->
	ok;
parse(["config_file",_|Rest]) ->
	%% Handled separately by arweave_config_format_legacy_json:parse_config_file/1.
	parse(Rest);
parse(["mine" | Rest]) ->
	_ = arweave_config:set([mining, enabled], true),
	parse(Rest);
parse(["verify", "purge" | Rest]) ->
	_ = arweave_config:set([verify, mode], purge),
	parse(Rest);
parse(["verify", "log" | Rest]) ->
	_ = arweave_config:set([verify, mode], log),
	parse(Rest);
parse(["verify", _ | _]) ->
	io:format("Invalid verify mode. Valid modes are 'purge' or 'log'.~n"),
	{error, [
		{timer, sleep, [1000]},
		{init, stop, [1]}
	]};
parse(["verify_samples", "all" | Rest]) ->
	_ = arweave_config:set([verify, samples], all),
	parse(Rest);
parse(["verify_samples", N | Rest]) ->
	Samples = list_to_integer(N),
	_ = arweave_config:set([verify, samples], Samples),
	parse(Rest);
parse(["vdf", Mode | Rest]) ->
	ParsedMode = case Mode of
		"openssl" ->openssl;
		"openssllite" ->openssllite;
		"fused" ->fused;
		"hiopt_m4" ->hiopt_m4;
		_ ->
			io:format("VDF ~p is invalid.~n", [Mode]),
			openssl
	end,
	_ = arweave_config:set([vdf, algorithm], ParsedMode),
	parse(Rest);
parse(["peer", Peer | Rest]) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ValidPeers} when is_list(ValidPeers) ->
			Ps = arweave_config_options_peers:by_role(trusted),
			NewPeers = ValidPeers ++ Ps,
			_ = arweave_config_options_peers:write_legacy_list(trusted, NewPeers),
			parse(Rest);
		{error, _} ->
			io:format("Peer ~p is invalid.~n", [Peer]),
			parse(Rest)
	end;
parse(["block_gossip_peer", Peer | Rest]) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ValidPeer} when is_list(ValidPeer) ->
			Peers = arweave_config_options_peers:by_role(block_gossip),
			NewPeers = ValidPeer ++ Peers,
			_ = arweave_config_options_peers:write_legacy_list(block_gossip, NewPeers),
			parse(Rest);
		{error, _} ->
			io:format("Peer ~p invalid ~n", [Peer]),
			parse(Rest)
	end;
parse(["local_peer", Peer | Rest]) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ValidPeer} when is_list(ValidPeer) ->
			Peers = arweave_config_options_peers:by_role(local),
			NewPeers = ValidPeer ++ Peers,
			_ = arweave_config_options_peers:write_legacy_list(local, NewPeers),
			parse(Rest);
		{error, _} ->
			io:format("Peer ~p is invalid.~n", [Peer]),
			parse(Rest)
	end;
parse(["sync_from_local_peers_only" | Rest]) ->
	_ = arweave_config:set([sync, local_peers_only], true),
	parse(Rest);
parse(["transaction_blacklist", File | Rest]) ->
	Files = get_list([transactions, blocklist, files]),
	NewFiles = [list_to_binary(File) | Files],
	_ = arweave_config:set([transactions, blocklist, files], NewFiles),
	parse(Rest);
parse(["transaction_blacklist_url", URL | Rest]) ->
	URLs = get_list([transactions, blocklist, urls]),
	NewURLs = [list_to_binary(URL) | URLs],
	_ = arweave_config:set([transactions, blocklist, urls], NewURLs),
	parse(Rest);
parse(["transaction_whitelist", File | Rest]) ->
	Files = get_list([transactions, allowlist, files]),
	NewFiles = [list_to_binary(File) | Files],
	_ = arweave_config:set([transactions, allowlist, files], NewFiles),
	parse(Rest);
parse(["transaction_whitelist_url", URL | Rest]) ->
	URLs = get_list([transactions, allowlist, urls]),
	NewURLs = [list_to_binary(URL) | URLs],
	_ = arweave_config:set([transactions, allowlist, urls], NewURLs),
	parse(Rest);
parse(["port", Port | Rest]) ->
	PortInt = list_to_integer(Port),
	_ = arweave_config:set([port], PortInt),
	parse(Rest);
parse(["data_dir", DataDir | Rest]) ->
	_ = arweave_config:set([data_dir], DataDir),
	parse(Rest);
parse(["log_dir", Dir | Rest]) ->
	_ = arweave_config:set([log_dir], Dir),
	parse(Rest);
parse(["storage_module", StorageModuleString | Rest]) ->
	try
		case arweave_config_format_legacy_json:parse_storage_module(StorageModuleString) of
			{ok, StorageModule} ->
				StorageModules = arweave_config_options_storage_modules:legacy_list(),
				NewModules = [StorageModule | StorageModules],
				_ = arweave_config_options_storage_modules:write_legacy_list(NewModules),
				parse(Rest);
			{repack_in_place, StorageModule} ->
				StorageModules = arweave_config_options_repack_modules:legacy_list(),
				NewModules = [StorageModule | StorageModules],
				_ = arweave_config_options_repack_modules:write_legacy_list(NewModules),
				parse(Rest)
		end
	catch _:_ ->
		io:format("~nstorage_module value must be "
				"in the {number},{address}[,repack_in_place,{to_packing}] format.~n~n"),
		{error, [
			{init, stop, [1]}
		]}
	end;
parse(["repack_batch_size", N | Rest]) ->
	V = list_to_integer(N),
	_ = arweave_config:set([packing, repack, batch_size], V),
	parse(Rest);
parse(["repack_cache_size_mb", _N | Rest]) ->
	?LOG_WARNING([{event, deprecated_config_option},
		{option, repack_cache_size_mb}, {action, ignored},
		{reason, <<"replica.2.9 repacks derive their cache size from "
			"[packing, entropy, cache_size]">>}]),
	parse(Rest);
parse(["polling", Frequency | Rest]) ->
	V = list_to_integer(Frequency),
	_ = arweave_config:set([gossip, block, poll_interval], V),
	parse(Rest);
parse(["block_pollers", N | Rest]) ->
	V = list_to_integer(N),
	_ = arweave_config:set([gossip, block, pollers], V),
	parse(Rest);
parse(["no_auto_join" | Rest]) ->
	_ = arweave_config:set([join, auto], false),
	parse(Rest);
parse(["join_workers", N | Rest]) ->
	V = list_to_integer(N),
	_ = arweave_config:set([join, workers], V),
	parse(Rest);
parse(["diff", N | Rest]) ->
	V = list_to_integer(N),
	_ = arweave_config:set([genesis, difficulty], V),
	parse(Rest);
parse(["mining_addr", Addr | Rest]) ->
	Current = case arweave_config:get([mining, address]) of
		undefined -> not_set;
		V -> V
	end,
	case Current of
		not_set ->
			case ar_util:safe_decode(Addr) of
				{ok, DecodedAddr} when byte_size(DecodedAddr) == 32 ->
					_ = arweave_config:set([mining, address], DecodedAddr),
					parse(Rest);
				_ ->
					io:format("~nmining_addr must be a valid Base64Url string, 43"
							" characters long.~n~n"),
					{error, [{init, stop, [1]}]}
			end;
		_ ->
			io:format("~nYou may specify at most one mining_addr.~n~n"),
			{error, [{init, stop, [1]}]}
	end;
parse(["hashing_threads", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([mining, hashing_threads], V),
	parse(Rest);
parse(["data_cache_size_limit", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([sync, cache_size_limit], V),
	parse(Rest);
parse(["packing_cache_size_limit", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([packing, cache_size], V),
	parse(Rest);
parse(["mining_cache_size_mb", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([mining, cache_size], V),
	parse(Rest);
parse(["max_emitters", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([gossip, tx, max_emitters], V),
	parse(Rest);
parse(["disk_space_check_frequency", Frequency | Rest]) ->
	V = list_to_integer(Frequency) * 1000,
	_ = arweave_config:set([disk_space_check_frequency], V),
	parse(Rest);
parse(["start_from_block_index" | Rest]) ->
	_ = arweave_config:set([join, start_from_latest_state], true),
	parse(Rest);
parse(["start_from_state", Folder | Rest]) ->
	_ = arweave_config:set([join, start_from_state], Folder),
	parse(Rest);
parse(["start_from_block", H | Rest]) ->
	case ar_util:safe_decode(H) of
		{ok, Decoded} when byte_size(Decoded) == 48 ->
			_ = arweave_config:set([join, start_from_block], Decoded),
			parse(Rest);
		_ ->
			io:format("Invalid start_from_block.~n", []),
			{error, [
				{timer, sleep, [1000]},
				{init, stop, [1]}
			]}
	end;
parse(["start_from_latest_state" | Rest]) ->
	_ = arweave_config:set([join, start_from_latest_state], true),
	parse(Rest);
parse(["init" | Rest])->
	_ = arweave_config:set([genesis, init], true),
	parse(Rest);
parse(["internal_api_secret", Secret | Rest])
		when length(Secret) >= ?INTERNAL_API_SECRET_MIN_LEN ->
	SecretBin = list_to_binary(Secret),
	_ = arweave_config:set([internal_api_secret], SecretBin),
	parse(Rest);
parse(["internal_api_secret", _ | _]) ->
	io:format("~nThe internal_api_secret must be at least ~B characters long.~n~n",
			[?INTERNAL_API_SECRET_MIN_LEN]),
	{error, [
		{init, stop, [1]}
	]};
parse(["enable", Feature | Rest]) ->
	arweave_config_features:classify_legacy_flag(list_to_atom(Feature), enable),
	parse(Rest);
parse(["disable", Feature | Rest]) ->
	arweave_config_features:classify_legacy_flag(list_to_atom(Feature), disable),
	parse(Rest);
parse(["requests_per_minute_limit", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([requests_per_minute_limit], V),
	parse(Rest);
parse(["max_propagation_peers", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([gossip, tx, max_peers], V),
	parse(Rest);
parse(["max_block_propagation_peers", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([gossip, block, max_peers], V),
	parse(Rest);
parse(["sync_jobs", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([sync, jobs], V),
	parse(Rest);
parse(["header_sync_jobs", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([gossip, header_sync_jobs], V),
	parse(Rest);
parse(["enable_data_roots_syncing", "true" | Rest]) ->
	_ = arweave_config:set([gossip, data_roots, syncing_enabled], true),
	parse(Rest);
parse(["enable_data_roots_syncing", "false" | Rest]) ->
	_ = arweave_config:set([gossip, data_roots, syncing_enabled], false),
	parse(Rest);
parse(["data_sync_request_packed_chunks" | Rest]) ->
	_ = arweave_config:set([sync, request_packed_chunks], true),
	parse(Rest);
parse(["post_tx_timeout", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([gossip, tx, post_timeout], V),
	parse(Rest);
parse(["max_connections", Num | Rest]) ->
	try list_to_integer(Num) of
		N when N >= 1 ->
			_ = arweave_config:set([network, server, tcp, max_connections], N),
			parse(Rest);
		_ ->
			io:format("Invalid max_connections ~p", [Num]),
			parse(Rest)
	catch
		_:_ ->
			io:format("Invalid max_connections ~p", [Num]),
			parse(Rest)

	end;
parse(["disk_pool_data_root_expiration_time", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([disk_pool, data_root_expiration_time], V),
	parse(Rest);
parse(["max_disk_pool_buffer_mb", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([disk_pool, max_buffer_size], V),
	parse(Rest);
parse(["max_disk_pool_data_root_buffer_mb", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([disk_pool, max_data_root_buffer_size], V),
	parse(Rest);
parse(["max_duplicate_data_roots", "infinity" | Rest]) ->
	_ = arweave_config:set([gossip, data_roots, max_duplicates], infinity),
	parse(Rest);
parse(["max_duplicate_data_roots", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([gossip, data_roots, max_duplicates], V),
	parse(Rest);
parse(["disk_cache_size_mb", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([gossip, header_cache_size], V),
	parse(Rest);
parse(["packing_workers", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([packing, workers], V),
	parse(Rest);
parse(["replica_2_9_workers", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([packing, entropy, workers], V),
	parse(Rest);
parse(["disable_replica_2_9_device_limit" | Rest]) ->
	_ = arweave_config:set([disable_device_limit], true),
	parse(Rest);
parse(["replica_2_9_entropy_cache_size_mb", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([packing, entropy, cache_size], V),
	parse(Rest);
parse(["max_vdf_validation_thread_count", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([vdf, max_validation_threads], V),
	parse(Rest);
parse(["max_vdf_last_step_validation_thread_count", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([vdf, max_last_step_validation_threads], V),
	parse(Rest);
parse(["vdf_server_trusted_peer", Peer | Rest]) ->
	Peers = arweave_config_options_peers:by_role(vdf_server),
	NewPeers = [Peer | Peers],
	_ = arweave_config_options_peers:write_legacy_list(vdf_server, NewPeers),
	parse(Rest);
parse(["vdf_client_peer", RawPeer | Rest]) ->
	Peers = arweave_config_options_peers:by_role(vdf_client),
	NewPeers = [RawPeer | Peers],
	_ = arweave_config_options_peers:write_legacy_list(vdf_client, NewPeers),
	parse(Rest);
parse(["debug" | Rest]) ->
	_ = arweave_config:set([debug], true),
	parse(Rest);
parse(["run_defragmentation" | Rest]) ->
	_ = arweave_config:set([defrag, enabled], true),
	parse(Rest);
parse(["defragmentation_trigger_threshold", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([defrag, threshold], V),
	parse(Rest);
parse(["block_throttle_by_ip_interval", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([gossip, block, throttle_by_ip_interval], V),
	parse(Rest);
parse(["block_throttle_by_solution_interval", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([gossip, block, throttle_by_solution_interval], V),
	parse(Rest);
parse(["defragment_module", DefragModuleString | Rest]) ->
	DefragModules = arweave_config_options_storage_modules:legacy_defrags(),
	try
		{ok, DefragModule} = arweave_config_format_legacy_json:parse_storage_module(DefragModuleString),
		DefragModules2 = [DefragModule | DefragModules],
		_ = arweave_config_options_storage_modules:write_legacy_defrags(DefragModules2),
		parse(Rest)
	catch _:_ ->
		io:format("~ndefragment_module value must be in the {number},{address} format.~n~n"),
		{error, [
			{init, stop, [1]}
		]}
	end;
parse(["http_api.tcp.idle_timeout_seconds", Num | Rest]) ->
	V = list_to_integer(Num) * 1000,
	_ = arweave_config:set([network, server, transport, idle_timeout], V),
	parse(Rest);
parse(["coordinated_mining" | Rest]) ->
	_ = arweave_config:set([cm, enabled], true),
	parse(Rest);
parse(["cm_api_secret", CMSecret | Rest])
		when length(CMSecret) >= ?INTERNAL_API_SECRET_MIN_LEN ->
	SecretBin = list_to_binary(CMSecret),
	_ = arweave_config:set([cm, api_secret], SecretBin),
	parse(Rest);
parse(["cm_api_secret", _ | _]) ->
	io:format("~nThe cm_api_secret must be at least ~B characters long.~n~n",
			[?INTERNAL_API_SECRET_MIN_LEN]),
	{error, [
		{init, stop, [1]}
	]};
parse(["cm_poll_interval", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([cm, poll_interval], V),
	parse(Rest);
parse(["cm_peer", Peer | Rest]) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ValidPeer} when is_list(ValidPeer) ->
			Ps = arweave_config_options_peers:by_role(cm_peer),
			NewPeers = ValidPeer ++ Ps,
			_ = arweave_config_options_peers:write_legacy_list(cm_peer, NewPeers),
			parse(Rest);
		{error, _} ->
			io:format("Peer ~p is invalid.~n", [Peer]),
			parse(Rest)
	end;
parse(["cm_exit_peer", Peer | Rest]) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, [ValidPeer|_]} ->
			_ = arweave_config_options_peers:write_legacy_singleton(cm_exit, ValidPeer),
			parse(Rest);
		{error, _} ->
			io:format("Peer ~p is invalid.~n", [Peer]),
			parse(Rest)
	end;
parse(["cm_out_batch_timeout", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([cm, out_batch_timeout], V),
	parse(Rest);
parse(["is_pool_server" | Rest]) ->
	_ = arweave_config:set([pool, is_server], true),
	parse(Rest);
parse(["is_pool_client" | Rest]) ->
	_ = arweave_config:set([pool, is_client], true),
	parse(Rest);
parse(["pool_api_key", Key | Rest]) ->
	V = list_to_binary(Key),
	_ = arweave_config:set([pool, api_key], V),
	parse(Rest);
parse(["pool_server_address", Host | Rest]) ->
	V = list_to_binary(Host),
	_ = arweave_config:set([pool, server_address], V),
	parse(Rest);
parse(["pool_worker_name", Host | Rest]) ->
	V = list_to_binary(Host),
	_ = arweave_config:set([pool, worker_name], V),
	parse(Rest);
parse(["rocksdb_flush_interval", Seconds | Rest]) ->
	V = list_to_integer(Seconds),
	_ = arweave_config:set([rocksdb, flush_interval], V),
	parse(Rest);
parse(["rocksdb_wal_sync_interval", Seconds | Rest]) ->
	V = list_to_integer(Seconds),
	_ = arweave_config:set([rocksdb, wal_sync_interval], V),
	parse(Rest);

%% TCP shutdown procedure.
parse(["network.tcp.connection_timeout", Delay|Rest]) ->
	V = list_to_integer(Delay),
	_ = arweave_config:set([network, server, shutdown_connection_timeout], V),
	parse(Rest);
parse(["network.tcp.shutdown.mode", RawMode|Rest]) ->
	case RawMode of
		"shutdown" ->
			_ = arweave_config:set([network, server, shutdown_mode], shutdown),
			parse(Rest);
		"close" ->
			_ = arweave_config:set([network, server, shutdown_mode], close),
			parse(Rest);
		Mode ->
			io:format("Mode ~p is invalid.~n", [Mode]),
			parse(Rest)
	end;

%% Global socket configuration.
parse(["network.socket.backend", Backend|Rest]) ->
	case Backend of
		"inet" ->
			_ = arweave_config:set([network, server, socket_backend], inet),
			parse(Rest);
		"socket" ->
			_ = arweave_config:set([network, server, socket_backend], socket),
			parse(Rest);
		_ ->
			io:format("Invalid socket.backend ~p.", [Backend]),
			parse(Rest)
	end;

%% Gun HTTP client configuration.
parse(["http_client.http.keepalive", "infinity"|Rest]) ->
	_ = arweave_config:set([network, client, http, keepalive], infinity),
	parse(Rest);
parse(["http_client.http.keepalive", Keepalive|Rest]) ->
	try list_to_integer(Keepalive) of
		K when K >= 0 ->
			_ = arweave_config:set([network, client, http, keepalive], K),
			parse(Rest);
		_ ->
			io:format("Invalid http_client.http.keepalive ~p.", [Keepalive]),
			parse(Rest)
	catch
		_:_ ->
			io:format("Invalid http_client.http.keepalive ~p.", [Keepalive]),
			parse(Rest)
	end;
parse(["http_client.tcp.delay_send", DelaySend|Rest]) ->
	case DelaySend of
		"true" ->
			_ = arweave_config:set([network, client, tcp, delay_send], true),
			parse(Rest);
		"false" ->
			_ = arweave_config:set([network, client, tcp, delay_send], false),
			parse(Rest);
		_ ->
			io:format("Invalid http_client.tcp.delay_send ~p.", [DelaySend]),
			parse(Rest)
	end;
parse(["http_client.tcp.keepalive", Keepalive|Rest]) ->
	case Keepalive of
		"true" ->
			_ = arweave_config:set([network, client, tcp, keepalive], true),
			parse(Rest);
		"false" ->
			_ = arweave_config:set([network, client, tcp, keepalive], false),
			parse(Rest);
		_ ->
			io:format("Invalid http_client.tcp.keepalive ~p.", [Keepalive]),
			parse(Rest)
	end;
parse(["http_client.tcp.linger", Linger|Rest]) ->
	case Linger of
		"true" ->
			_ = arweave_config:set([network, client, tcp, linger], true),
			parse(Rest);
		"false" ->
			_ = arweave_config:set([network, client, tcp, linger], false),
			parse(Rest);
		_ ->
			io:format("Invalid http_client.tcp.linger ~p.", [Linger]),
			parse(Rest)
	end;
parse(["http_client.tcp.linger_timeout", Timeout|Rest]) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			_ = arweave_config:set([network, client, tcp, linger_timeout], T),
			parse(Rest);
		_ ->
			io:format("Invalid http_client.tcp.linger_timeout ~p.", [Timeout]),
			parse(Rest)
	catch
		_:_ ->
			io:format("Invalid http_client.tcp.linger_timeout timeout ~p.", [Timeout]),
			parse(Rest)
	end;
parse(["http_client.tcp.nodelay", Nodelay|Rest]) ->
	case Nodelay of
		"true" ->
			_ = arweave_config:set([network, client, tcp, nodelay], true),
			parse(Rest);
		"false" ->
			_ = arweave_config:set([network, client, tcp, nodelay], false),
			parse(Rest);
		_ ->
			io:format("Invalid http_client.tcp.nodelay ~p.", [Nodelay]),
			parse(Rest)
	end;
parse(["http_client.tcp.send_timeout_close", Value|Rest]) ->
	case Value of
		"true" ->
			_ = arweave_config:set([network, client, tcp, send_timeout_close], true),
			parse(Rest);
		"false" ->
			_ = arweave_config:set([network, client, tcp, send_timeout_close], false),
			parse(Rest);
		_ ->
			io:format("Invalid http_client.tcp.send_timeout_close ~p.", [Value]),
			parse(Rest)
	end;
parse(["http_client.tcp.send_timeout", Timeout|Rest]) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			_ = arweave_config:set([network, client, tcp, send_timeout], T),
			parse(Rest);
		_ ->
			io:format("Invalid http_client.tcp.send_timeout ~p.", [Timeout]),
			parse(Rest)
	catch
		_:_ ->
			io:format("Invalid http_client.tcp.send_timeout ~p.", [Timeout]),
			parse(Rest)
	end;

%% Cowboy HTTP server configuration.
parse(["http_api.http.active_n", Active|Rest]) ->
	try list_to_integer(Active) of
		N when N >= 1 ->
			_ = arweave_config:set([network, server, http, active_n], N),
			parse(Rest);
		_ ->
			io:format("Invalid http_api.http.active_n ~p.", [Active]),
			parse(Rest)
	catch
		_:_ ->
			io:format("Invalid http_api.http.active_n ~p.", [Active]),
			parse(Rest)
	end;
parse(["http_api.http.inactivity_timeout", Timeout|Rest]) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			_ = arweave_config:set([network, server, http, inactivity_timeout], T),
			parse(Rest);
		_ ->
			io:format("Invalid http_api.http.inactivity_timeout ~p.", [Timeout]),
			parse(Rest)
	catch
		_:_ ->
			io:format("Invalid http_api.http.inactivity_timeout ~p.", [Timeout]),
			parse(Rest)
	end;
parse(["http_api.http.linger_timeout", Timeout|Rest]) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			_ = arweave_config:set([network, server, http, linger_timeout], T),
			parse(Rest);
		_ ->
			io:format("Invalid http_api.http.linger_timeout ~p.", [Timeout]),
			parse(Rest)
	catch
		_:_ ->
			io:format("Invalid http_api.http.linger_timeout ~p.", [Timeout]),
			parse(Rest)
	end;
parse(["http_api.http.request_timeout", Timeout|Rest]) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			_ = arweave_config:set([network, server, http, request_timeout], T),
			parse(Rest);
		_ ->
			io:format("Invalid http_api.http.request_timeout ~p.", [Timeout]),
			parse(Rest)
	catch
		_:_ ->
			io:format("Invalid http_api.http.request_timeout ~p.", [Timeout]),
			parse(Rest)
	end;
parse(["http_api.tcp.backlog", Backlog|Rest]) ->
	try list_to_integer(Backlog)of
		B when B >= 1 ->
			_ = arweave_config:set([network, server, tcp, backlog], B),
			parse(Rest);
		_ ->
			io:format("Invalid http_api.tcp.backlog ~p.", [Backlog]),
			parse(Rest)
	catch
		_:_ ->
			io:format("Invalid http_api.tcp.backlog ~p.", [Backlog]),
			parse(Rest)
	end;
parse(["http_api.tcp.delay_send", DelaySend|Rest]) ->
	case DelaySend of
		"true" ->
			_ = arweave_config:set([network, server, tcp, delay_send], true),
			parse(Rest);
		"false" ->
			_ = arweave_config:set([network, server, tcp, delay_send], false),
			parse(Rest);
		_ ->
			io:format("Invalid http_api.tcp.delay_send ~p.", [DelaySend]),
			parse(Rest)
	end;
parse(["http_api.tcp.keepalive", "true"|Rest]) ->
	_ = arweave_config:set([network, server, tcp, keepalive], true),
	parse(Rest);
parse(["http_api.tcp.keepalive", "false"|Rest]) ->
	_ = arweave_config:set([network, server, tcp, keepalive], false),
	parse(Rest);
parse(["http_api.tcp.keepalive", Keepalive|Rest]) ->
	io:format("Invalid http_api.tcp.keepalive ~p.", [Keepalive]),
	parse(Rest);
parse(["http_api.tcp.linger", Linger|Rest]) ->
	case Linger of
		"true" ->
			_ = arweave_config:set([network, server, tcp, linger], true),
			parse(Rest);
		"false" ->
			_ = arweave_config:set([network, server, tcp, linger], false),
			parse(Rest);
		_ ->
			io:format("Invalid http_api.tcp.linger ~p.", [Linger]),
			parse(Rest)
	end;
parse(["http_api.tcp.linger_timeout", Timeout|Rest]) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			_ = arweave_config:set([network, server, tcp, linger_timeout], T),
			parse(Rest);
		_ ->
			io:format("Invalid http_api.tcp.linger_timeout ~p.", [Timeout]),
			parse(Rest)
	catch
		_:_ ->
			io:format("Invalid http_api.tcp.linger_timeout ~p.", [Timeout]),
			parse(Rest)
	end;
parse(["http_api.tcp.listener_shutdown", "brutal_kill"|Rest]) ->
	_ = arweave_config:set([network, server, tcp, listener_shutdown], brutal_kill),
	parse(Rest);
parse(["http_api.tcp.listener_shutdown", "infinity"|Rest]) ->
	_ = arweave_config:set([network, server, tcp, listener_shutdown], infinity),
	parse(Rest);
parse(["http_api.tcp.listener_shutdown", Shutdown|Rest]) ->
	try list_to_integer(Shutdown) of
		S when S >= 0 ->
			_ = arweave_config:set([network, server, tcp, listener_shutdown], S),
			parse(Rest);
		_ ->
			io:format("Invalid http_api.tcp.listener_shutdown ~p.", [Shutdown]),
			parse(Rest)
	catch
		_:_ ->
			io:format("Invalid http_api.tcp.listener_shutdown ~p.", [Shutdown]),
			parse(Rest)
	end;
parse(["http_api.tcp.nodelay", Nodelay|Rest]) ->
	case Nodelay of
		"true" ->
			_ = arweave_config:set([network, server, tcp, nodelay], true),
			parse(Rest);
		"false" ->
			_ = arweave_config:set([network, server, tcp, nodelay], false),
			parse(Rest);
		_ ->
			io:format("Invalid http_api.tcp.nodelay ~p.", [Nodelay]),
			parse(Rest)
	end;
parse(["http_api.tcp.num_acceptors", Acceptors|Rest]) ->
	try list_to_integer(Acceptors) of
		N when N >= 0 ->
			_ = arweave_config:set([network, server, tcp, num_acceptors], N),
			parse(Rest);
		_ ->
			io:format("Invalid http_api.tcp.num_acceptors ~p.", [Acceptors]),
			parse(Rest)
	catch
		_:_ ->
			io:format("Invalid http_api.tcp.num_acceptors ~p.", [Acceptors]),
			parse(Rest)
	end;
parse(["http_api.tcp.send_timeout_close", Value|Rest]) ->
	case Value of
		"true" ->
			_ = arweave_config:set([network, server, tcp, send_timeout_close], true),
			parse(Rest);
		"false" ->
			_ = arweave_config:set([network, server, tcp, send_timeout_close], false),
			parse(Rest);
		_ ->
			io:format("Invalid http_api.tcp.send_timeout_close ~p.", [Value]),
			parse(Rest)
	end;
parse(["http_api.tcp.send_timeout", Timeout|Rest]) ->
	try list_to_integer(Timeout) of
		T when T >= 0 ->
			_ = arweave_config:set([network, server, tcp, send_timeout], T),
			parse(Rest);
		_ ->
			io:format("Invalid http_api.tcp.send_timeout ~p.", [Timeout]),
			parse(Rest)
	catch
		_:_ ->
			io:format("Invalid http_api.tcp.send_timeout ~p.", [Timeout]),
			parse(Rest)
	end;

%% Undocumented or unsupported options.
parse(["chunk_storage_file_size", Num | Rest]) ->
	V = list_to_integer(Num),
	_ = arweave_config:set([chunk_storage_file_size], V),
	parse(Rest);

parse([Arg | _Rest]) ->
	io:format("~nUnknown argument: ~s.~n", [Arg]),
	{error, [
		{arweave_config_help, print, []}
	]}.

%% @doc Read a list-shaped option, defaulting to `[]` when unset.
get_list(Key) ->
	case arweave_config:get(Key) of
		undefined -> [];
		L when is_list(L) -> L;
		_ -> []
	end.

