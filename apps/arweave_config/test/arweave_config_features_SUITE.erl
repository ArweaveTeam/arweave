%%% @doc Tests for `arweave_config_features' — the feature flag
%%% catalog and helpers.
-module(arweave_config_features_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) ->
	ok = arweave_config:start(),
	Config.

end_per_suite(_Config) ->
	ok = arweave_config:stop().

init_per_testcase(_TestCase, Config) -> Config.

end_per_testcase(_TestCase, _Config) -> ok.

all() ->
	[
		catalog_non_empty,
		names_sorted,
		enabled_with_stored_value,
		enabled_fallback_default,
		enabled_unknown_flag,
		tombstone_message_known,
		tombstone_message_unknown,
		classify_legacy_flag_promotes_dedicated_options
	].

%%====================================================================
%% Test cases
%%====================================================================

catalog_non_empty(_Config) ->
	Catalog = arweave_config_features:catalog(),
	?assert(is_list(Catalog)),
	?assert(length(Catalog) > 0),
	lists:foreach(
		fun(Entry) ->
			?assert(is_map(Entry)),
			?assert(maps:is_key(name, Entry)),
			?assert(maps:is_key(default, Entry)),
			?assert(maps:is_key(description, Entry))
		end,
		Catalog),
	ok.

names_sorted(_Config) ->
	Names = arweave_config_features:names(),
	?assert(is_list(Names)),
	?assert(length(Names) > 0),
	lists:foreach(fun(N) -> ?assert(is_atom(N)) end, Names),
	?assertEqual(Names, lists:sort(Names)),
	ok.

enabled_with_stored_value(_Config) ->
	arweave_config:with_test_config(fun() ->
		[Flag | _] = arweave_config_features:names(),
		ok = arweave_config:set([features, Flag], true),
		?assertEqual(true, arweave_config_features:enabled(Flag)),
		ok = arweave_config:set([features, Flag], false),
		?assertEqual(false, arweave_config_features:enabled(Flag))
	end),
	ok.

enabled_fallback_default(_Config) ->
	arweave_config:with_test_config(fun() ->
		%% Walk the catalog and verify every default round-trips.
		lists:foreach(
			fun(#{ name := Name, default := Default }) ->
				?assertEqual(Default,
					arweave_config_features:enabled(Name))
			end,
			arweave_config_features:catalog())
	end),
	ok.

enabled_unknown_flag(_Config) ->
	?assertEqual(false,
		arweave_config_features:enabled(this_is_not_a_real_flag)),
	ok.

tombstone_message_known(_Config) ->
	case arweave_config_features:tombstone_message(tx_poller) of
		{ok, Msg} ->
			?assert(is_binary(Msg)),
			?assert(byte_size(Msg) > 0);
		Other ->
			ct:fail({expected_tombstone_message, Other})
	end,
	ok.

tombstone_message_unknown(_Config) ->
	?assertEqual(not_found,
		arweave_config_features:tombstone_message(no_such_flag)),
	ok.

classify_legacy_flag_promotes_dedicated_options(_Config) ->
	arweave_config:with_test_config(fun() ->
		lists:foreach(
			fun assert_promotion/1,
			[
				{compute_own_vdf, disable, [vdf, compute], false},
				{compute_own_vdf, enable, [vdf, compute], true},
				{public_vdf_server, enable, [vdf, is_public_server], true},
				{vdf_server_pull, disable, [vdf, pull], false},
				{randomx_jit, disable, [randomx, jit], false},
				{randomx_hardware_aes, disable, [randomx, hardware_aes], false},
				{randomx_large_pages, enable, [randomx, large_pages], true},
				{tx_poller, disable, [gossip, tx, polling_enabled], false},
				{tx_poller, enable, [gossip, tx, polling_enabled], true}
			])
	end),
	ok.

%%====================================================================
%% Helpers
%%====================================================================

assert_promotion({Flag, ListName, OptionKey, Value}) ->
	?assertEqual(ok,
		arweave_config_features:classify_legacy_flag(Flag, ListName)),
	?assertEqual(Value, arweave_config:get(OptionKey)).
