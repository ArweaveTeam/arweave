%%% @doc Legacy JSON configuration parser. Reads pre-2.9.6 `config.json'
%%% files and writes each option into the options registry via
%%% `arweave_config:set/2'.
-module(arweave_config_format_legacy_json).
-export([
	parse/1,
	parse/2,
	parse_config_file/1,
	parse_config_file/2,
	parse_storage_module/1
]).
-ifdef(AR_TEST).
-export([parse_peers/3]).
-endif.
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @see parse_config_file/2
-spec parse_config_file(Args) -> Return when
	Args :: [string()],
	Return :: ok
		| {error, term(), term()}
		| {error, term()}.
parse_config_file(Args) ->
	parse_config_file(Args, []).

%% @doc Scan a legacy CLI arg list for a `config_file' parameter, then
%% read and parse the named file. Parsed values are written into the
%% options registry; the function returns `ok' on success.
-spec parse_config_file(Args, Skipped) -> Return when
	Args :: [string()],
	Skipped :: [string()],
	Return :: ok
		| {error, term(), term()}
		| {error, term()}.
parse_config_file([], _) ->
	ok;
parse_config_file(["config_file", Path | Rest], Skipped) ->
	case read_config_from_file(Path) of
		{ok, _} ->
			parse_config_file(Rest, Skipped);
		{error, Reason, Item} ->
			io:format("Failed to parse config: ~p: ~p.~n", [Reason, Item]),
			arweave_config_help:print(),
			{error, Reason, Item};
		{error, Reason} ->
			io:format("Failed to parse config: ~p.~n", [Reason]),
			arweave_config_help:print(),
			{error, Reason}
	end;
parse_config_file([Arg | Rest], Skipped) ->
	parse_config_file(Rest, [Arg | Skipped]).

%% @doc Read a config file from disk and feed it to `parse/1'.
-spec read_config_from_file(Path) -> Return when
	Path :: string(),
	Return :: {ok, binary()}
		| {error, file_unreadable, Path}.
read_config_from_file(Path) ->
	case file:read_file(Path) of
		{ok, FileData} ->
			parse(FileData);
		{error, _} ->
			{error, file_unreadable, Path}
	end.

parse(Config) ->
	parse(Config, #{}).

%% @doc Like `parse/1'. `#{raw_peers => true}' keeps peer options as
%% the operator's original strings — validated for shape, but never
%% resolved to IPs or normalized with default ports. Used by the
%% converter so converted files preserve peer spellings and convert
%% needs no DNS.
parse(Config, Opts) when is_binary(Config) ->
	case ar_serialize:json_decode(Config) of
		{ok, JSONValue} ->
			case parse_options(JSONValue, Opts) of
				ok -> {ok, ok};
				{error, _} = E -> E;
				{error, _, _} = E -> E
			end;
		{error, _} -> {error, bad_json, Config}
	end.

parse_storage_module(IOList) ->
	Bin = iolist_to_binary(IOList),
	case binary:split(Bin, <<",">>, [global]) of
		[PartitionNumberBin, PackingBin, <<"repack_in_place">>, ToPackingBin] ->
			PartitionNumber = binary_to_integer(PartitionNumberBin),
			true = PartitionNumber >= 0,
			parse_storage_module(PartitionNumber, ar_block:partition_size(), PackingBin, ToPackingBin);
		[RangeNumberBin, RangeSizeBin, PackingBin, <<"repack_in_place">>, ToPackingBin] ->
			RangeNumber = binary_to_integer(RangeNumberBin),
			true = RangeNumber >= 0,
			RangeSize = binary_to_integer(RangeSizeBin),
			true = RangeSize >= 0,
			parse_storage_module(RangeNumber, RangeSize, PackingBin, ToPackingBin);
		[PartitionNumberBin, PackingBin] ->
			PartitionNumber = binary_to_integer(PartitionNumberBin),
			true = PartitionNumber >= 0,
			parse_storage_module(PartitionNumber, ar_block:partition_size(), PackingBin);
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

%% @doc Parse a decoded JSON object's key-value list. Each clause
%% writes the option into the options registry; returns `ok' on success or
%% `{error, ...}' on the first failure.
parse_options({KVPairs}, Opts) when is_list(KVPairs) ->
	parse_options(KVPairs, Opts);
parse_options(JSONValue, _Opts) when not is_list(JSONValue) ->
	{error, root_not_object, JSONValue};
parse_options([{_, null} | Rest], Opts) ->
	parse_options(Rest, Opts);

parse_options([{<<"config_file">>, _} | _], _Opts) ->
	{error, config_file_set};

parse_options([{<<"peers">>, Peers} | Rest], Opts) when is_list(Peers) ->
	case parse_peers(Peers, [], Opts) of
		{ok, ParsedPeers} ->
			write_peers(trusted, ParsedPeers, Opts),
			parse_options(Rest, Opts);
		error ->
			{error, bad_peers, Peers}
	end;
parse_options([{<<"peers">>, Peers} | _], _Opts) ->
	{error, {bad_type, peers, array}, Peers};

parse_options([{<<"block_gossip_peers">>, Peers} | Rest], Opts) when is_list(Peers) ->
	case parse_peers(Peers, [], Opts) of
		{ok, ParsedPeers} ->
			write_peers(block_gossip, ParsedPeers, Opts),
			parse_options(Rest, Opts);
		error ->
			{error, bad_peers, Peers}
	end;
parse_options([{<<"block_gossip_peers">>, Peers} | _], _Opts) ->
	{error, {bad_type, peers, array}, Peers};

parse_options([{<<"local_peers">>, Peers} | Rest], Opts) when is_list(Peers) ->
	case parse_peers(Peers, [], Opts) of
		{ok, ParsedPeers} ->
			write_peers(local, ParsedPeers, Opts),
			parse_options(Rest, Opts);
		error ->
			{error, bad_local_peers, Peers}
	end;
parse_options([{<<"local_peers">>, Peers} | _], _Opts) ->
	{error, {bad_type, local_peers, array}, Peers};

parse_options([{<<"sync_from_local_peers_only">>, true} | Rest], Opts) ->
	_ = arweave_config:set([sync, local_peers_only], true),
	parse_options(Rest, Opts);
parse_options([{<<"sync_from_local_peers_only">>, false} | Rest], Opts) ->
	_ = arweave_config:set([sync, local_peers_only], false),
	parse_options(Rest, Opts);
parse_options([{<<"sync_from_local_peers_only">>, Opt} | _], _Opts) ->
	{error, {bad_type, sync_from_local_peers_only, boolean}, Opt};

parse_options([{<<"start_from_latest_state">>, true} | Rest], Opts) ->
	_ = arweave_config:set([join, start_from_latest_state], true),
	parse_options(Rest, Opts);
parse_options([{<<"start_from_latest_state">>, false} | Rest], Opts) ->
	_ = arweave_config:set([join, start_from_latest_state], false),
	parse_options(Rest, Opts);
parse_options([{<<"start_from_latest_state">>, Opt} | _], _Opts) ->
	{error, {bad_type, start_from_latest_state, boolean}, Opt};

parse_options([{<<"start_from_state">>, Folder} | Rest], Opts) when is_binary(Folder) ->
	_ = arweave_config:set([join, start_from_state], binary_to_list(Folder)),
	parse_options(Rest, Opts);
parse_options([{<<"start_from_state">>, Folder} | _], _Opts) ->
	{error, {bad_type, start_from_state, string}, Folder};

parse_options([{<<"start_from_block">>, H} | Rest], Opts) when is_binary(H) ->
	case ar_util:safe_decode(H) of
		{ok, Decoded} when byte_size(Decoded) == 48 ->
			_ = arweave_config:set([join, start_from_block], Decoded),
			parse_options(Rest, Opts);
		_ ->
			{error, bad_block, H}
	end;
parse_options([{<<"start_from_block">>, Opt} | _], _Opts) ->
	{error, {bad_type, start_from_block, string}, Opt};

parse_options([{<<"start_from_block_index">>, true} | Rest], Opts) ->
	_ = arweave_config:set([join, start_from_latest_state], true),
	parse_options(Rest, Opts);
parse_options([{<<"start_from_block_index">>, false} | Rest], Opts) ->
	_ = arweave_config:set([join, start_from_latest_state], false),
	parse_options(Rest, Opts);
parse_options([{<<"start_from_block_index">>, Opt} | _], _Opts) ->
	{error, {bad_type, start_from_block_index, boolean}, Opt};

parse_options([{<<"mine">>, true} | Rest], Opts) ->
	_ = arweave_config:set([mining, enabled], true),
	parse_options(Rest, Opts);
parse_options([{<<"mine">>, false} | Rest], Opts) ->
	parse_options(Rest, Opts);
parse_options([{<<"mine">>, Opt} | _], _Opts) ->
	{error, {bad_type, mine, boolean}, Opt};

parse_options([{<<"verify">>, <<"purge">>} | Rest], Opts) ->
	_ = arweave_config:set([verify, mode], purge),
	parse_options(Rest, Opts);
parse_options([{<<"verify">>, <<"log">>} | Rest], Opts) ->
	_ = arweave_config:set([verify, mode], log),
	parse_options(Rest, Opts);
parse_options([{<<"verify">>, Opt} | _], _Opts) ->
	{error, bad_verify_mode, Opt};

parse_options([{<<"verify_samples">>, N} | Rest], Opts) when is_integer(N) ->
	_ = arweave_config:set([verify, samples], N),
	parse_options(Rest, Opts);
parse_options([{<<"verify_samples">>, <<"all">>} | Rest], Opts) ->
	_ = arweave_config:set([verify, samples], all),
	parse_options(Rest, Opts);
parse_options([{<<"verify_samples">>, Opt} | _], _Opts) ->
	{error, {bad_type, verify_samples, number}, Opt};

parse_options([{<<"vdf">>, Mode} | Rest], Opts) ->
	ParsedMode = case Mode of
		<<"openssl">> -> openssl;
		<<"fused">> -> fused;
		<<"hiopt_m4">> -> hiopt_m4;
		_ ->
			io:format("VDF ~p is invalid.~n", [Mode]),
			openssl
	end,
	_ = arweave_config:set([vdf, algorithm], ParsedMode),
	parse_options(Rest, Opts);

parse_options([{<<"port">>, Port} | Rest], Opts) when is_integer(Port) ->
	_ = arweave_config:set([port], Port),
	parse_options(Rest, Opts);
parse_options([{<<"port">>, Port} | _], _Opts) ->
	{error, {bad_type, port, number}, Port};

parse_options([{<<"data_dir">>, DataDir} | Rest], Opts) when is_binary(DataDir) ->
	_ = arweave_config:set([data_dir], binary_to_list(DataDir)),
	parse_options(Rest, Opts);
parse_options([{<<"data_dir">>, DataDir} | _], _Opts) ->
	{error, {bad_type, data_dir, string}, DataDir};

parse_options([{<<"log_dir">>, Dir} | Rest], Opts) when is_binary(Dir) ->
	_ = arweave_config:set([log_dir], binary_to_list(Dir)),
	parse_options(Rest, Opts);
parse_options([{<<"log_dir">>, Dir} | _], _Opts) ->
	{error, {bad_type, log_dir, string}, Dir};

parse_options([{<<"storage_modules">>, L} | Rest], Opts) when is_list(L) ->
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
		_ = arweave_config_options_storage_modules:write_legacy_list(
			StorageModules),
		_ = arweave_config_options_repack_modules:write_legacy_list(
			RepackInPlaceStorageModules),
		parse_options(Rest, Opts)
	catch Error:Reason ->
		?LOG_ERROR([{event, parse_failure}, {option, storage_modules},
			{error, Error}, {reason, Reason}]),
		{error, {bad_format, storage_modules, "an array of "
				"\"{number},{address}[,repack_in_place,{to_packing}]\""}, L}
	end;
parse_options([{<<"storage_modules">>, Bin} | _], _Opts) ->
	{error, {bad_type, storage_modules, array}, Bin};

parse_options([{<<"repack_batch_size">>, N} | Rest], Opts) when is_integer(N) ->
	_ = arweave_config:set([packing, repack, batch_size], N),
	parse_options(Rest, Opts);
parse_options([{<<"repack_batch_size">>, Opt} | _], _Opts) ->
	{error, {bad_type, repack_batch_size, number}, Opt};

parse_options([{<<"repack_cache_size_mb">>, _Opt} | Rest], Opts) ->
	?LOG_WARNING([{event, deprecated_config_option},
		{option, repack_cache_size_mb}, {action, ignored},
		{reason, <<"replica.2.9 repacks derive their cache size from "
			"[packing, entropy, cache_size]">>}]),
	parse_options(Rest, Opts);

parse_options([{<<"polling">>, Frequency} | Rest], Opts) when is_integer(Frequency) ->
	_ = arweave_config:set([gossip, block, poll_interval], Frequency),
	parse_options(Rest, Opts);
parse_options([{<<"polling">>, Opt} | _], _Opts) ->
	{error, {bad_type, polling, number}, Opt};

parse_options([{<<"block_pollers">>, N} | Rest], Opts) when is_integer(N) ->
	_ = arweave_config:set([gossip, block, pollers], N),
	parse_options(Rest, Opts);
parse_options([{<<"block_pollers">>, Opt} | _], _Opts) ->
	{error, {bad_type, block_pollers, number}, Opt};

parse_options([{<<"no_auto_join">>, true} | Rest], Opts) ->
	_ = arweave_config:set([join, auto], false),
	parse_options(Rest, Opts);
parse_options([{<<"no_auto_join">>, false} | Rest], Opts) ->
	parse_options(Rest, Opts);
parse_options([{<<"no_auto_join">>, Opt} | _], _Opts) ->
	{error, {bad_type, no_auto_join, boolean}, Opt};

parse_options([{<<"join_workers">>, N} | Rest], Opts) when is_integer(N)->
	_ = arweave_config:set([join, workers], N),
	parse_options(Rest, Opts);
parse_options([{<<"join_workers">>, Opt} | _], _Opts) ->
	{error, {bad_type, join_workers, number}, Opt};

parse_options([{<<"packing_workers">>, N} | Rest], Opts) when is_integer(N)->
	_ = arweave_config:set([packing, workers], N),
	parse_options(Rest, Opts);
parse_options([{<<"packing_workers">>, Opt} | _], _Opts) ->
	{error, {bad_type, packing_workers, number}, Opt};

parse_options([{<<"replica_2_9_workers">>, N} | Rest], Opts) when is_integer(N)->
	_ = arweave_config:set([packing, entropy, workers], N),
	parse_options(Rest, Opts);
parse_options([{<<"replica_2_9_workers">>, Opt} | _], _Opts) ->
	{error, {bad_type, replica_2_9_workers, number}, Opt};

parse_options([{<<"disable_replica_2_9_device_limit">>, true} | Rest], Opts) ->
	_ = arweave_config:set([disable_device_limit], true),
	parse_options(Rest, Opts);
parse_options([{<<"disable_replica_2_9_device_limit">>, false} | Rest], Opts) ->
	parse_options(Rest, Opts);
parse_options([{<<"disable_replica_2_9_device_limit">>, Opt} | _], _Opts) ->
	{error, {bad_type, disable_replica_2_9_device_limit, boolean}, Opt};

parse_options([{<<"replica_2_9_entropy_cache_size_mb">>, N} | Rest], Opts) when is_integer(N)->
	_ = arweave_config:set([packing, entropy, cache_size], N),
	parse_options(Rest, Opts);
parse_options([{<<"replica_2_9_entropy_cache_size_mb">>, Opt} | _], _Opts) ->
	{error, {bad_type, replica_2_9_entropy_cache_size_mb, number}, Opt};

parse_options([{<<"diff">>, Diff} | Rest], Opts) when is_integer(Diff) ->
	_ = arweave_config:set([genesis, difficulty], Diff),
	parse_options(Rest, Opts);
parse_options([{<<"diff">>, Diff} | _], _Opts) ->
	{error, {bad_type, diff, number}, Diff};

parse_options([{<<"mining_addr">>, Addr} | Rest], Opts) when is_binary(Addr) ->
	case ar_util:safe_decode(Addr) of
		{ok, D} when byte_size(D) == 32 ->
			_ = arweave_config:set([mining, address], D),
			parse_options(Rest, Opts);
		_ -> {error, bad_mining_addr, Addr}
	end;
parse_options([{<<"mining_addr">>, Addr} | _], _Opts) ->
	{error, {bad_type, mining_addr, string}, Addr};

parse_options([{<<"hashing_threads">>, Threads} | Rest], Opts) when is_integer(Threads) ->
	_ = arweave_config:set([mining, hashing_threads], Threads),
	parse_options(Rest, Opts);
parse_options([{<<"hashing_threads">>, Threads} | _], _Opts) ->
	{error, {bad_type, hashing_threads, number}, Threads};

parse_options([{<<"data_cache_size_limit">>, Limit} | Rest], Opts)
		when is_integer(Limit) ->
	_ = arweave_config:set([sync, cache_size_limit], Limit),
	parse_options(Rest, Opts);
parse_options([{<<"data_cache_size_limit">>, Limit} | _], _Opts) ->
	{error, {bad_type, data_cache_size_limit, number}, Limit};

parse_options([{<<"packing_cache_size_limit">>, Limit} | Rest], Opts)
		when is_integer(Limit) ->
	_ = arweave_config:set([packing, cache_size], Limit),
	parse_options(Rest, Opts);
parse_options([{<<"packing_cache_size_limit">>, Limit} | _], _Opts) ->
	{error, {bad_type, packing_cache_size_limit, number}, Limit};

parse_options([{<<"mining_cache_size_mb">>, Limit} | Rest], Opts)
		when is_integer(Limit) ->
	_ = arweave_config:set([mining, cache_size], Limit),
	parse_options(Rest, Opts);
parse_options([{<<"mining_cache_size_mb">>, Limit} | _], _Opts) ->
	{error, {bad_type, mining_cache_size_mb, number}, Limit};

parse_options([{<<"max_emitters">>, Value} | Rest], Opts) when is_integer(Value) ->
	_ = arweave_config:set([gossip, tx, max_emitters], Value),
	parse_options(Rest, Opts);
parse_options([{<<"max_emitters">>, Value} | _], _Opts) ->
	{error, {bad_type, max_emitters, number}, Value};

parse_options([{<<"post_tx_timeout">>, Value} | Rest], Opts)
		when is_integer(Value) ->
	_ = arweave_config:set([gossip, tx, post_timeout], Value),
	parse_options(Rest, Opts);
parse_options([{<<"post_tx_timeout">>, Value} | _], _Opts) ->
	{error, {bad_type, post_tx_timeout, number}, Value};

parse_options([{<<"max_propagation_peers">>, Value} | Rest], Opts)
		when is_integer(Value) ->
	_ = arweave_config:set([gossip, tx, max_peers], Value),
	parse_options(Rest, Opts);
parse_options([{<<"max_propagation_peers">>, Value} | _], _Opts) ->
	{error, {bad_type, max_propagation_peers, number}, Value};

parse_options([{<<"max_block_propagation_peers">>, Value} | Rest], Opts)
		when is_integer(Value) ->
	_ = arweave_config:set([gossip, block, max_peers], Value),
	parse_options(Rest, Opts);
parse_options([{<<"max_block_propagation_peers">>, Value} | _], _Opts) ->
	{error, {bad_type, max_block_propagation_peers, number}, Value};

parse_options([{<<"sync_jobs">>, Value} | Rest], Opts)
		when is_integer(Value) ->
	_ = arweave_config:set([sync, jobs], Value),
	parse_options(Rest, Opts);
parse_options([{<<"sync_jobs">>, Value} | _], _Opts) ->
	{error, {bad_type, sync_jobs, number}, Value};

parse_options([{<<"header_sync_jobs">>, Value} | Rest], Opts)
		when is_integer(Value) ->
	_ = arweave_config:set([gossip, header_sync_jobs], Value),
	parse_options(Rest, Opts);
parse_options([{<<"header_sync_jobs">>, Value} | _], _Opts) ->
	{error, {bad_type, header_sync_jobs, number}, Value};

parse_options([{<<"enable_data_roots_syncing">>, Value} | Rest], Opts)
		when is_boolean(Value) ->
	_ = arweave_config:set([gossip, data_roots, syncing_enabled], Value),
	parse_options(Rest, Opts);
parse_options([{<<"enable_data_roots_syncing">>, Value} | _], _Opts) ->
	{error, {bad_type, enable_data_roots_syncing, boolean}, Value};

parse_options([{<<"disk_pool_jobs">>, Value} | Rest], Opts)
		when is_integer(Value) ->
	_ = arweave_config:set([disk_pool, jobs], Value),
	parse_options(Rest, Opts);
parse_options([{<<"disk_pool_jobs">>, Value} | _], _Opts) ->
	{error, {bad_type, disk_pool, jobs}, Value};

parse_options([{<<"transaction_blacklists">>, TransactionBlacklists} | Rest], Opts)
		when is_list(TransactionBlacklists) ->
	parse_binary_list([transactions, blocklist, files], TransactionBlacklists,
		bad_transaction_blacklists, Rest, Opts);
parse_options([{<<"transaction_blacklists">>, TransactionBlacklists} | _], _Opts) ->
	{error, {bad_type, transaction_blacklists, array}, TransactionBlacklists};

parse_options([{<<"transaction_blacklist_urls">>, TransactionBlacklistURLs} | Rest], Opts)
		when is_list(TransactionBlacklistURLs) ->
	parse_binary_list([transactions, blocklist, urls], TransactionBlacklistURLs,
		bad_transaction_blacklist_urls, Rest, Opts);
parse_options([{<<"transaction_blacklist_urls">>, TransactionBlacklistURLs} | _], _Opts) ->
	{error, {bad_type, transaction_blacklist_urls, array}, TransactionBlacklistURLs};

parse_options([{<<"transaction_whitelists">>, TransactionWhitelists} | Rest], Opts)
		when is_list(TransactionWhitelists) ->
	parse_binary_list([transactions, allowlist, files], TransactionWhitelists,
		bad_transaction_whitelists, Rest, Opts);
parse_options([{<<"transaction_whitelists">>, TransactionWhitelists} | _], _Opts) ->
	{error, {bad_type, transaction_whitelists, array}, TransactionWhitelists};

parse_options([{<<"transaction_whitelist_urls">>, TransactionWhitelistURLs} | Rest], Opts)
		when is_list(TransactionWhitelistURLs) ->
	parse_binary_list([transactions, allowlist, urls], TransactionWhitelistURLs,
		bad_transaction_whitelist_urls, Rest, Opts);
parse_options([{<<"transaction_whitelist_urls">>, TransactionWhitelistURLs} | _], _Opts) ->
	{error, {bad_type, transaction_whitelist_urls, array}, TransactionWhitelistURLs};

parse_options([{<<"disk_space_check_frequency">>, Frequency} | Rest], Opts)
		when is_integer(Frequency) ->
	_ = arweave_config:set([disk_space_check_frequency], Frequency * 1000),
	parse_options(Rest, Opts);
parse_options([{<<"disk_space_check_frequency">>, Frequency} | _], _Opts) ->
	{error, {bad_type, disk_space_check_frequency, number}, Frequency};

parse_options([{<<"init">>, true} | Rest], Opts) ->
	_ = arweave_config:set([genesis, init], true),
	parse_options(Rest, Opts);
parse_options([{<<"init">>, false} | Rest], Opts) ->
	_ = arweave_config:set([genesis, init], false),
	parse_options(Rest, Opts);
parse_options([{<<"init">>, Opt} | _], _Opts) ->
	{error, {bad_type, init, boolean}, Opt};

parse_options([{<<"internal_api_secret">>, Secret} | Rest], Opts)
		when is_binary(Secret), byte_size(Secret) >= ?INTERNAL_API_SECRET_MIN_LEN ->
	_ = arweave_config:set([internal_api_secret], Secret),
	parse_options(Rest, Opts);
parse_options([{<<"internal_api_secret">>, Secret} | _], _Opts) ->
	{error, bad_secret, Secret};

parse_options([{<<"enable">>, Features} | Rest], Opts) when is_list(Features) ->
	case safe_map(fun(Feature) -> binary_to_atom(Feature, latin1) end, Features) of
		{ok, FeatureAtoms} ->
			lists:foreach(
				fun(F) -> arweave_config_features:classify_legacy_flag(F, enable) end,
				FeatureAtoms),
			parse_options(Rest, Opts);
		error ->
			{error, bad_enable}
	end;
parse_options([{<<"enable">>, Features} | _], _Opts) ->
	{error, {bad_type, enable, array}, Features};

parse_options([{<<"disable">>, Features} | Rest], Opts) when is_list(Features) ->
	case safe_map(fun(Feature) -> binary_to_atom(Feature, latin1) end, Features) of
		{ok, FeatureAtoms} ->
			lists:foreach(
				fun(F) -> arweave_config_features:classify_legacy_flag(F, disable) end,
				FeatureAtoms),
			parse_options(Rest, Opts);
		error ->
			{error, bad_disable}
	end;
parse_options([{<<"disable">>, Features} | _], _Opts) ->
	{error, {bad_type, disable, array}, Features};

parse_options([{<<"webhooks">>, WebhookConfigs} | Rest], Opts) when is_list(WebhookConfigs) ->
	case parse_webhooks(WebhookConfigs, []) of
		{ok, ParsedWebhooks} ->
			_ = arweave_config_options_webhooks:write_legacy_list(ParsedWebhooks),
			parse_options(Rest, Opts);
		error ->
			{error, bad_webhooks, WebhookConfigs}
	end;
parse_options([{<<"webhooks">>, Webhooks} | _], _Opts) ->
	{error, {bad_type, webhooks, array}, Webhooks};

parse_options([{<<"semaphores">>, Semaphores} | Rest], Opts) when is_tuple(Semaphores) ->
	%% Seed empty rather than with the full default map: unmentioned
	%% names revert to their compile-time defaults anyway
	%% (write_legacy_map clears first), and only operator-written
	%% entries should become explicit store values (and hence appear in
	%% a converted config).
	case parse_atom_number_map(Semaphores, #{}) of
		{ok, ParsedSemaphores} ->
			_ = arweave_config_options_semaphores:write_legacy_map(ParsedSemaphores),
			parse_options(Rest, Opts);
		error ->
			{error, bad_semaphores, Semaphores}
	end;
parse_options([{<<"semaphores">>, Semaphores} | _], _Opts) ->
	{error, {bad_type, semaphores, object}, Semaphores};

parse_options([{<<"max_connections">>, MaxConnections} | Rest], Opts)
		when is_integer(MaxConnections), MaxConnections >= 1 ->
	_ = arweave_config:set([network, server, tcp, max_connections], MaxConnections),
	parse_options(Rest, Opts);

parse_options([{<<"disk_pool_data_root_expiration_time">>, D} | Rest], Opts)
		when is_integer(D) ->
	_ = arweave_config:set([disk_pool, data_root_expiration_time], D),
	parse_options(Rest, Opts);

parse_options([{<<"max_disk_pool_buffer_mb">>, D} | Rest], Opts) when is_integer(D) ->
	_ = arweave_config:set([disk_pool, max_buffer_size], D),
	parse_options(Rest, Opts);

parse_options([{<<"max_disk_pool_data_root_buffer_mb">>, D} | Rest], Opts)
		when is_integer(D) ->
	_ = arweave_config:set([disk_pool, max_data_root_buffer_size], D),
	parse_options(Rest, Opts);

parse_options([{<<"max_duplicate_data_roots">>, <<"infinity">>} | Rest], Opts) ->
	_ = arweave_config:set([gossip, data_roots, max_duplicates], infinity),
	parse_options(Rest, Opts);
parse_options([{<<"max_duplicate_data_roots">>, D} | Rest], Opts)
		when is_integer(D) ->
	_ = arweave_config:set([gossip, data_roots, max_duplicates], D),
	parse_options(Rest, Opts);

parse_options([{<<"disk_cache_size_mb">>, D} | Rest], Opts) when is_integer(D) ->
	_ = arweave_config:set([gossip, header_cache_size], D),
	parse_options(Rest, Opts);

parse_options([{<<"max_nonce_limiter_validation_thread_count">>, D} | Rest], Opts)
		when is_integer(D) ->
	_ = arweave_config:set([vdf, max_validation_threads], D),
	parse_options(Rest, Opts);

parse_options([{<<"max_nonce_limiter_last_step_validation_thread_count">>, D} | Rest], Opts)
		when is_integer(D) ->
	_ = arweave_config:set([vdf, max_last_step_validation_threads], D),
	parse_options(Rest, Opts);

parse_options([{<<"vdf_server_trusted_peer">>, <<>>} | Rest], Opts) ->
	parse_options(Rest, Opts);
parse_options([{<<"vdf_server_trusted_peer">>, Peer} | Rest], Opts) ->
	add_vdf_server_trusted_peer(Peer, Opts),
	parse_options(Rest, Opts);

parse_options([{<<"vdf_server_trusted_peers">>, Peers} | Rest], Opts) when is_list(Peers) ->
	lists:foreach(fun(Peer) -> add_vdf_server_trusted_peer(Peer, Opts) end, Peers),
	parse_options(Rest, Opts);
parse_options([{<<"vdf_server_trusted_peers">>, Peers} | _], _Opts) ->
	{error, {bad_type, vdf_server_trusted_peers, array}, Peers};

parse_options([{<<"vdf_client_peers">>, Peers} | Rest], Opts) when is_list(Peers) ->
	write_peers(vdf_client, Peers, Opts),
	parse_options(Rest, Opts);
parse_options([{<<"vdf_client_peers">>, Peers} | _], _Opts) ->
	{error, {bad_type, vdf_client_peers, array}, Peers};

parse_options([{<<"debug">>, B} | Rest], Opts) when is_boolean(B) ->
	_ = arweave_config:set([debug], B),
	parse_options(Rest, Opts);

parse_options([{<<"run_defragmentation">>, B} | Rest], Opts) when is_boolean(B) ->
	_ = arweave_config:set([defrag, enabled], B),
	parse_options(Rest, Opts);

parse_options([{<<"defragmentation_trigger_threshold">>, D} | Rest], Opts)
		when is_integer(D) ->
	_ = arweave_config:set([defrag, threshold], D),
	parse_options(Rest, Opts);

parse_options([{<<"block_throttle_by_ip_interval">>, D} | Rest], Opts)
		when is_integer(D) ->
	_ = arweave_config:set([gossip, block, throttle_by_ip_interval], D),
	parse_options(Rest, Opts);

parse_options([{<<"block_throttle_by_solution_interval">>, D} | Rest], Opts)
		when is_integer(D) ->
	_ = arweave_config:set([gossip, block, throttle_by_solution_interval], D),
	parse_options(Rest, Opts);

parse_options([{<<"defragment_modules">>, L} | Rest], Opts) when is_list(L) ->
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
		_ = arweave_config_options_storage_modules:write_legacy_defrags(DefragModules),
		parse_options(Rest, Opts)
	catch _:_ ->
		{error, {bad_format, defragment_modules, "an array of \"{number},{address}\""}, L}
	end;
parse_options([{<<"defragment_modules">>, Bin} | _], _Opts) ->
	{error, {bad_type, defragment_modules, array}, Bin};

parse_options([{<<"http_api.tcp.idle_timeout_seconds">>, D} | Rest], Opts) when is_integer(D) ->
	_ = arweave_config:set([network, server, transport, idle_timeout], D * 1000),
	parse_options(Rest, Opts);

parse_options([{<<"coordinated_mining">>, true} | Rest], Opts) ->
	_ = arweave_config:set([cm, enabled], true),
	parse_options(Rest, Opts);
parse_options([{<<"coordinated_mining">>, false} | Rest], Opts) ->
	parse_options(Rest, Opts);
parse_options([{<<"coordinated_mining">>, Opt} | _], _Opts) ->
	{error, {bad_type, coordinated_mining, boolean}, Opt};

parse_options([{<<"cm_api_secret">>, CMSecret} | Rest], Opts)
		when is_binary(CMSecret), byte_size(CMSecret) >= ?INTERNAL_API_SECRET_MIN_LEN ->
	_ = arweave_config:set([cm, api_secret], CMSecret),
	parse_options(Rest, Opts);
parse_options([{<<"cm_api_secret">>, CMSecret} | _], _Opts) ->
	{error, {bad_type, cm_api_secret, string}, CMSecret};

parse_options([{<<"cm_poll_interval">>, CMPollInterval} | Rest], Opts)
		when is_integer(CMPollInterval) ->
	_ = arweave_config:set([cm, poll_interval], CMPollInterval),
	parse_options(Rest, Opts);
parse_options([{<<"cm_poll_interval">>, CMPollInterval} | _], _Opts) ->
	{error, {bad_type, cm_poll_interval, number}, CMPollInterval};

parse_options([{<<"cm_peers">>, Peers} | Rest], Opts) when is_list(Peers) ->
	case parse_peers(Peers, [], Opts) of
		{ok, ParsedPeers} ->
			write_peers(cm_peer, ParsedPeers, Opts),
			parse_options(Rest, Opts);
		error ->
			{error, bad_peers, Peers}
	end;

parse_options([{<<"cm_exit_peer">>, Peer} | Rest], Opts) ->
	case parse_cm_exit_peer(Peer, Opts) of
		ok -> parse_options(Rest, Opts);
		error -> {error, bad_cm_exit_peer, Peer}
	end;

parse_options([{<<"cm_out_batch_timeout">>, CMBatchTimeout} | Rest], Opts)
		when is_integer(CMBatchTimeout) ->
	_ = arweave_config:set([cm, out_batch_timeout], CMBatchTimeout),
	parse_options(Rest, Opts);
parse_options([{<<"cm_out_batch_timeout">>, CMBatchTimeout} | _], _Opts) ->
	{error, {bad_type, cm_out_batch_timeout, number}, CMBatchTimeout};

parse_options([{<<"is_pool_server">>, true} | Rest], Opts) ->
	_ = arweave_config:set([pool, is_server], true),
	parse_options(Rest, Opts);
parse_options([{<<"is_pool_server">>, false} | Rest], Opts) ->
	parse_options(Rest, Opts);
parse_options([{<<"is_pool_server">>, Opt} | _], _Opts) ->
	{error, {bad_type, is_pool_server, boolean}, Opt};

parse_options([{<<"is_pool_client">>, true} | Rest], Opts) ->
	_ = arweave_config:set([pool, is_client], true),
	parse_options(Rest, Opts);
parse_options([{<<"is_pool_client">>, false} | Rest], Opts) ->
	parse_options(Rest, Opts);
parse_options([{<<"is_pool_client">>, Opt} | _], _Opts) ->
	{error, {bad_type, is_pool_client, boolean}, Opt};

parse_options([{<<"pool_api_key">>, Key} | Rest], Opts) when is_binary(Key) ->
	_ = arweave_config:set([pool, api_key], Key),
	parse_options(Rest, Opts);
parse_options([{<<"pool_api_key">>, Key} | _], _Opts) ->
	{error, {bad_type, pool_api_key, string}, Key};

parse_options([{<<"pool_server_address">>, Host} | Rest], Opts) when is_binary(Host) ->
	_ = arweave_config:set([pool, server_address], Host),
	parse_options(Rest, Opts);
parse_options([{<<"pool_server_address">>, Host} | _], _Opts) ->
	{error, {bad_type, pool_server_address, string}, Host};

parse_options([{<<"pool_worker_name">>, WorkerName} | Rest], Opts) when is_binary(WorkerName) ->
	_ = arweave_config:set([pool, worker_name], WorkerName),
	parse_options(Rest, Opts);
parse_options([{<<"pool_worker_name">>, WorkerName} | _], _Opts) ->
	{error, {bad_type, pool_worker_name, string}, WorkerName};

%% Undocumented or unsupported options.
parse_options([{<<"chunk_storage_file_size">>, ChunkGroupSize} | Rest], Opts)
		when is_integer(ChunkGroupSize) ->
	_ = arweave_config:set([chunk_storage_file_size], ChunkGroupSize),
	parse_options(Rest, Opts);
parse_options([{<<"chunk_storage_file_size">>, ChunkGroupSize} | _], _Opts) ->
	{error, {bad_type, chunk_storage_file_size, number}, ChunkGroupSize};

parse_options([{<<"rocksdb_flush_interval">>, IntervalS} | Rest], Opts)
		when is_integer(IntervalS) ->
	_ = arweave_config:set([rocksdb, flush_interval], IntervalS),
	parse_options(Rest, Opts);
parse_options([{<<"rocksdb_flush_interval">>, IntervalS} | _], _Opts) ->
	{error, {bad_type, rocksdb_flush_interval, number}, IntervalS};

parse_options([{<<"rocksdb_wal_sync_interval">>, IntervalS} | Rest], Opts)
		when is_integer(IntervalS) ->
	_ = arweave_config:set([rocksdb, wal_sync_interval], IntervalS),
	parse_options(Rest, Opts);
parse_options([{<<"rocksdb_wal_sync_interval">>, IntervalS} | _], _Opts) ->
	{error, {bad_type, rocksdb_wal_sync_interval, number}, IntervalS};

parse_options([{<<"data_sync_request_packed_chunks">>, Bool} | Rest], Opts)
		when is_boolean(Bool) ->
	_ = arweave_config:set([sync, request_packed_chunks], Bool),
	parse_options(Rest, Opts);
parse_options([{<<"data_sync_request_packed_chunks">>, InvalidValue} | _Rest], _Opts) ->
	{error, {bad_type, data_sync_request_packed_chunks, boolean}, InvalidValue};

parse_options([{<<"data_discovery_max_concurrent_peer_scans">>, N} | Rest], Opts)
		when is_integer(N), N >= 0 ->
	_ = arweave_config:set([sync, max_concurrent_peer_scans], N),
	parse_options(Rest, Opts);
parse_options([{<<"data_discovery_max_concurrent_peer_scans">>, InvalidValue} | _Rest], _Opts) ->
	{error, {bad_type, data_discovery_max_concurrent_peer_scans, number}, InvalidValue};

%% TCP shutdown procedure.
parse_options([{<<"network.tcp.shutdown.connection_timeout">>, Delay} | Rest], Opts)
	when is_integer(Delay) andalso Delay > 0 ->
		_ = arweave_config:set([network, server, shutdown_connection_timeout], Delay),
		parse_options(Rest, Opts);
parse_options([{<<"network.tcp.shutdown.connection_timeout">>, InvalidValue} | _Rest], _Opts) ->
	{error, {bad_type, shutdown_tcp_connection_timeout, integer}, InvalidValue};
parse_options([{<<"network.tcp.shutdown.mode">>, Mode}|Rest], Opts) ->
	case Mode of
		<<"shutdown">> ->
			_ = arweave_config:set([network, server, shutdown_mode], shutdown),
			parse_options(Rest, Opts);
		<<"close">> ->
			_ = arweave_config:set([network, server, shutdown_mode], close),
			parse_options(Rest, Opts);
		Mode ->
			{error, {bad_value, shutdown_tcp_mode}, Mode}
	end;

%% Global socket configuration.
parse_options([{<<"network.socket.backend">>, Backend}|Rest], Opts) ->
	case Backend of
		<<"inet">> ->
			_ = arweave_config:set([network, server, socket_backend], inet),
			parse_options(Rest, Opts);
		<<"socket">> ->
			_ = arweave_config:set([network, server, socket_backend], socket),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'socket.backend'}, Backend}
	end;

%% Gun HTTP client parameters.
parse_options([{<<"http_client.http.closing_timeout">>, Timeout}|Rest], Opts) ->
	case Timeout of
		_ when is_integer(Timeout), Timeout >= 0 ->
			_ = arweave_config:set([network, client, http, closing_timeout], Timeout),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_client.http.closing_timeout'}, Timeout}
	end;
parse_options([{<<"http_client.http.keepalive">>, Timeout}|Rest], Opts) ->
	case Timeout of
		<<"infinity">> ->
			_ = arweave_config:set([network, client, http, keepalive], infinity),
			parse_options(Rest, Opts);
		_ when is_integer(Timeout), Timeout >= 0 ->
			_ = arweave_config:set([network, client, http, keepalive], Timeout),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_client.http.keepalive'}, Timeout}
	end;
parse_options([{<<"http_client.tcp.delay_send">>, Delay}|Rest], Opts) ->
	case Delay of
		_ when is_boolean(Delay) ->
			_ = arweave_config:set([network, client, tcp, delay_send], Delay),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_client.tcp.delay_send'}, Delay}
	end;
parse_options([{<<"http_client.tcp.keepalive">>, Keepalive}|Rest], Opts) ->
	case Keepalive of
		_ when is_boolean(Keepalive) ->
			_ = arweave_config:set([network, client, tcp, keepalive], Keepalive),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_client.tcp.keepalive'}, Keepalive}
	end;
parse_options([{<<"http_client.tcp.linger">>, Linger}|Rest], Opts) ->
	case Linger of
		_ when is_boolean(Linger) ->
			_ = arweave_config:set([network, client, tcp, linger], Linger),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_client.tcp.linger'}, Linger}
	end;
parse_options([{<<"http_client.tcp.linger_timeout">>, Timeout}|Rest], Opts) ->
	case Timeout of
		_ when is_integer(Timeout), Timeout >= 0 ->
			_ = arweave_config:set([network, client, tcp, linger_timeout], Timeout),
			parse_options(Rest, Opts);

		_ ->
			{error, {bad_value, 'http_client.tcp.linger_timeout'}, Timeout}
	end;
parse_options([{<<"http_client.tcp.nodelay">>, Nodelay}|Rest], Opts) ->
	case Nodelay of
		_ when is_boolean(Nodelay) ->
			_ = arweave_config:set([network, client, tcp, nodelay], Nodelay),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_client.tcp.nodelay'}, Nodelay }
	end;
parse_options([{<<"http_client.tcp.send_timeout_close">>, Value}|Rest], Opts) ->
	case Value of
		_ when is_boolean(Value) ->
			_ = arweave_config:set([network, client, tcp, send_timeout_close], Value),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_client.tcp.send_timeout_close'}, Value}
	end;
parse_options([{<<"http_client.tcp.send_timeout">>, Timeout}|Rest], Opts) ->
	case Timeout of
		_ when is_integer(Timeout), Timeout >= 0 ->
			_ = arweave_config:set([network, client, tcp, send_timeout], Timeout),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_client.tcp.send_timeout'}, Timeout}
	end;

%% Cowboy HTTP server parameters.
parse_options([{<<"http_api.http.active_n">>, Active}|Rest], Opts) ->
	case Active of
		_ when is_integer(Active), Active >= 1 ->
			_ = arweave_config:set([network, server, http, active_n], Active),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_api.http.active_n'}, Active}
	end;
parse_options([{<<"http_api.http.inactivity_timeout">>, Timeout}|Rest], Opts) ->
	case Timeout of
		_ when is_integer(Timeout), Timeout >= 0 ->
			_ = arweave_config:set([network, server, http, inactivity_timeout], Timeout),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_api.http.inactivity_timeout'}, Timeout}
	end;
parse_options([{<<"http_api.http.linger_timeout">>, Timeout}|Rest], Opts) ->
	case Timeout of
		_ when is_integer(Timeout), Timeout >= 0 ->
			_ = arweave_config:set([network, server, http, linger_timeout], Timeout),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_api.http.linger_timeout'}, Timeout}
	end;
parse_options([{<<"http_api.http.request_timeout">>, Timeout}|Rest], Opts) ->
	case Timeout of
		_ when is_integer(Timeout), Timeout >= 0 ->
			_ = arweave_config:set([network, server, http, request_timeout], Timeout),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_api.http.request_timeout'}, Timeout}
	end;
parse_options([{<<"http_api.tcp.backlog">>, Backlog}|Rest], Opts) ->
	case Backlog of
		_ when is_integer(Backlog), Backlog >= 1 ->
			_ = arweave_config:set([network, server, tcp, backlog], Backlog),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_api.tcp.backlog'}, Backlog}
	end;
parse_options([{<<"http_api.tcp.delay_send">>, Delay}|Rest], Opts) ->
	case Delay of
		_ when is_boolean(Delay) ->
			_ = arweave_config:set([network, server, tcp, delay_send], Delay),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_api.tcp.delay_send'}, Delay}
	end;
parse_options([{<<"http_api.tcp.keepalive">>, Keepalive}|Rest], Opts) ->
	case Keepalive of
		_ when is_boolean(Keepalive) ->
			_ = arweave_config:set([network, server, tcp, keepalive], Keepalive),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_api.tcp.keepalive'}, Keepalive}
	end;
parse_options([{<<"http_api.tcp.linger">>, Linger}|Rest], Opts) ->
	case Linger of
		_ when is_boolean(Linger) ->
			_ = arweave_config:set([network, server, tcp, linger], Linger),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_api.tcp.linger'}, Linger}
	end;
parse_options([{<<"http_api.tcp.linger_timeout">>, Timeout}|Rest], Opts) ->
	case Timeout of
		_ when is_integer(Timeout), Timeout >= 0 ->
			_ = arweave_config:set([network, server, tcp, linger_timeout], Timeout),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_api.tcp.linger_timeout'}, Timeout}
	end;
parse_options([{<<"http_api.tcp.listener_shutdown">>, Shutdown}|Rest], Opts) ->
	case Shutdown of
		"brutal_kill" ->
			_ = arweave_config:set([network, server, tcp, listener_shutdown], brutal_kill),
			parse_options(Rest, Opts);
		"infinity" ->
			_ = arweave_config:set([network, server, tcp, listener_shutdown], infinity),
			parse_options(Rest, Opts);
		_ when is_integer(Shutdown), Shutdown >= 0 ->
			_ = arweave_config:set([network, server, tcp, listener_shutdown], Shutdown),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_api.tcp.listener_shutdown'}, Shutdown}
	end;
parse_options([{<<"http_api.tcp.nodelay">>, Nodelay}|Rest], Opts) ->
	case Nodelay of
		_ when is_boolean(Nodelay) ->
			_ = arweave_config:set([network, server, tcp, nodelay], Nodelay),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_api.tcp.nodelay'}, Nodelay }
	end;
parse_options([{<<"http_api.tcp.num_acceptors">>, Acceptors}|Rest], Opts) ->
	case Acceptors of
		_ when is_integer(Acceptors), Acceptors >= 1 ->
			_ = arweave_config:set([network, server, tcp, num_acceptors], Acceptors),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_valud, 'http_api.tcp.num_acceptors'}, Acceptors}
	end;
parse_options([{<<"http_api.tcp.send_timeout_close">>, Value}|Rest], Opts) ->
	case Value of
		_ when is_boolean(Value) ->
			_ = arweave_config:set([network, server, tcp, send_timeout_close], Value),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_api.tcp.send_timeout_close'}, Value}
	end;
parse_options([{<<"http_api.tcp.send_timeout">>, Timeout}|Rest], Opts) ->
	case Timeout of
		_ when is_integer(Timeout), Timeout >= 0 ->
			_ = arweave_config:set([network, server, tcp, send_timeout], Timeout),
			parse_options(Rest, Opts);
		_ ->
			{error, {bad_value, 'http_api.tcp.send_timeout'}, Timeout}
	end;

parse_options([Opt | _], _Opts) ->
	{error, unknown, Opt};
parse_options([], _Opts) ->
	ok.

parse_storage_module(RangeNumber, RangeSize, PackingBin) ->
	Packing =
		case PackingBin of
			<<"unpacked">> ->
				unpacked;
			<< MiningAddr:43/binary, ".replica.2.9" >> ->
				{replica_2_9, ar_util:decode(MiningAddr)};
			MiningAddr when byte_size(MiningAddr) == 43 ->
				{spora_2_6, ar_util:decode(MiningAddr)}
		end,
	{ok, {RangeSize, RangeNumber, Packing}}.

parse_storage_module(RangeNumber, RangeSize, PackingBin, ToPackingBin) ->
	Packing =
		case PackingBin of
			<<"unpacked">> ->
				unpacked;
			<< MiningAddr:43/binary, ".replica.2.9" >> ->
				{replica_2_9, ar_util:decode(MiningAddr)};
			MiningAddr when byte_size(MiningAddr) == 43 ->
				{spora_2_6, ar_util:decode(MiningAddr)}
		end,
	ToPacking =
		case ToPackingBin of
			<<"unpacked">> ->
				unpacked;
			<< ToMiningAddr:43/binary, ".replica.2.9" >> ->
				{replica_2_9, ar_util:decode(ToMiningAddr)};
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

parse_peers([Peer | Rest], ParsedPeers, Opts) ->
	case maps:get(raw_peers, Opts, false) of
		true -> parse_raw_peer(Peer, Rest, ParsedPeers, Opts);
		false -> parse_resolved_peer(Peer, Rest, ParsedPeers, Opts)
	end;
parse_peers([], ParsedPeers, _Opts) ->
	Flatten = lists:flatten(ParsedPeers),
	Reverse = lists:reverse(Flatten),
	{ok, Reverse}.

parse_resolved_peer(Peer, Rest, ParsedPeers, Opts) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ParsedPeer} -> parse_peers(Rest, ParsedPeer ++ ParsedPeers, Opts);
		{error, _} ->
			?LOG_WARNING([{event, invalid_peer_in_config}, {peer, Peer}, {action, ignored}]),
			parse_peers(Rest, ParsedPeers, Opts)
	end.

%% @doc Raw-peers mode: validate the peer's shape without DNS and keep
%% the original string.
parse_raw_peer(Peer, Rest, ParsedPeers, Opts) ->
	case arweave_config_type:peer_id(Peer) of
		{ok, _} -> parse_peers(Rest, [Peer | ParsedPeers], Opts);
		{error, _} ->
			?LOG_WARNING([{event, invalid_peer_in_config}, {peer, Peer}, {action, ignored}]),
			parse_peers(Rest, ParsedPeers, Opts)
	end.

%% @doc Resolve-and-write the `cm_exit_peer' singleton, or keep the
%% original string in raw-peers mode. `error' maps to
%% `bad_cm_exit_peer' at the call site.
parse_cm_exit_peer(Peer, Opts) ->
	case maps:get(raw_peers, Opts, false) of
		true ->
			case arweave_config_type:peer_id(Peer) of
				{ok, _} ->
					%% Direct store write; see write_peers/2.
					_ = arweave_config_store:set([peers, cm_exit], Peer),
					ok;
				{error, _} ->
					error
			end;
		false ->
			case ar_util:safe_parse_peer(Peer) of
				{ok, [ParsedPeer | _]} ->
					_ = arweave_config_options_peers:write_legacy_singleton(
						cm_exit, ParsedPeer),
					ok;
				{error, _} ->
					error
			end
	end.

%% @doc Route a parsed peer list to the normalizing legacy writer, or —
%% in raw-peers mode — write the store directly so the operator's
%% original strings survive. The direct write deliberately bypasses the
%% registry: its set path runs the spec's type function, which would
%% resolve/normalize the peers again (and, unlike the legacy
%% warn-and-skip, drop the whole list when one hostname fails to
%% resolve). Raw entries are already shape-validated by
%% parse_raw_peer, and the converter snapshot-restores the store
%% around the parse.
write_peers(Role, Peers, Opts) ->
	case maps:get(raw_peers, Opts, false) of
		true -> _ = arweave_config_store:set([peers, Role], Peers);
		false -> _ = arweave_config_options_peers:write_legacy_list(Role, Peers)
	end,
	ok.

parse_webhooks([{WebhookConfig} | Rest], ParsedWebhookConfigs) when is_list(WebhookConfig) ->
	case parse_webhook(WebhookConfig, #{}) of
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
			parse_webhook(Rest, Webhook#{events => ParsedEvents});
		error ->
			error
	end;
parse_webhook([{<<"events">>, _} | _], _) ->
	error;
parse_webhook([{<<"url">>, Url} | Rest], Webhook) when is_binary(Url) ->
	parse_webhook(Rest, Webhook#{url => Url});
parse_webhook([{<<"url">>, _} | _], _) ->
	error;
%% Headers become a map, as they are in every other config format.
parse_webhook([{<<"headers">>, {Headers}} | Rest], Webhook) when is_list(Headers) ->
	parse_webhook(Rest, Webhook#{headers => maps:from_list(Headers)});
parse_webhook([{<<"headers">>, _} | _], _) ->
	error;
parse_webhook([], Webhook) ->
	{ok, Webhook}.

%% Entries stay binaries, as they are in every other config format.
parse_binary_list(Key, Values, Error, Rest, Opts) ->
	case lists:all(fun is_binary/1, Values) of
		true ->
			_ = arweave_config:set(Key, Values),
			parse_options(Rest, Opts);
		false ->
			{error, Error}
	end.

%% Event names stay binaries, as they are in every other config format.
%% Which names are legal is enforced by the webhooks validator.
parse_webhook_events([Event | Rest], Events) when is_binary(Event) ->
	parse_webhook_events(Rest, [Event | Events]);
parse_webhook_events([_ | _], _) ->
	error;
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

add_vdf_server_trusted_peer(Peer, Opts) when is_binary(Peer) ->
	%% vdf peers never DNS-resolve in either mode. The flag is consulted
	%% here because raw-peers mode must also skip normalization: the
	%% binary_to_list conversion and, via write_peers, the peer_id
	%% port-stamping/usort in write_legacy_list — keeping the operator's
	%% exact string.
	case maps:get(raw_peers, Opts, false) of
		true -> append_vdf_server_trusted_peer(Peer, Opts);
		false -> add_vdf_server_trusted_peer(binary_to_list(Peer), Opts)
	end;
add_vdf_server_trusted_peer(Peer, Opts) ->
	append_vdf_server_trusted_peer(Peer, Opts).

append_vdf_server_trusted_peer(Peer, Opts) ->
	Peers = arweave_config_options_peers:by_role(vdf_server),
	write_peers(vdf_server, Peers ++ [Peer], Opts),
	ok.
