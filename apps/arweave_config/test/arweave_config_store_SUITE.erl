%%% @doc
-module(arweave_config_store_SUITE).
-compile([export_all, nowarn_export_all]).
-include("arweave_config.hrl").
-include_lib("common_test/include/ct.hrl").

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
	{ok, Pid} = arweave_config_store:start_link(),
	[{arweave_config_store, Pid}|Config].

end_per_testcase(_TestCase, _Config) ->
	ok = arweave_config_store:stop().

all() ->
	[
		arweave_config_store,
		snapshot_restore_roundtrip,
		items_with_prefix_filter,
		to_map_nested,
		log_smoke
	].

%%====================================================================
%% Test cases
%%====================================================================

arweave_config_store(_Config) ->
	{error, undefined} = arweave_config_store:get("test"),

	{error, undefined} = arweave_config_store:delete("test"),

	default = arweave_config_store:get("test", default),

	{ok, {[test], data}} = arweave_config_store:set("test", data),

	{ok, data} = arweave_config_store:get("test"),

	{ok, {[test], data}} = arweave_config_store:delete("test"),

	{error, undefined} = arweave_config_store:get("test"),

	ok.

snapshot_restore_roundtrip(_Config) ->
	{ok, _} = arweave_config_store:set([snap, a], 1),
	{ok, _} = arweave_config_store:set([snap, b], 2),

	Snap = arweave_config_store:snapshot(),

	{ok, _} = arweave_config_store:set([snap, a], 99),
	{ok, _} = arweave_config_store:set([snap, c], 3),
	{ok, 99} = arweave_config_store:get([snap, a]),
	{ok, 3} = arweave_config_store:get([snap, c]),

	ok = arweave_config_store:restore(Snap),
	{ok, 1} = arweave_config_store:get([snap, a]),
	{ok, 2} = arweave_config_store:get([snap, b]),
	{error, undefined} = arweave_config_store:get([snap, c]),

	ok.

items_with_prefix_filter(_Config) ->
	{ok, _} = arweave_config_store:set([foo, a], 1),
	{ok, _} = arweave_config_store:set([foo, b], 2),
	{ok, _} = arweave_config_store:set([bar, c], 3),

	FooItems = arweave_config_store:items_with_prefix([foo]),
	2 = length(FooItems),
	true = lists:member({[foo, a], 1}, FooItems),
	true = lists:member({[foo, b], 2}, FooItems),
	false = lists:member({[bar, c], 3}, FooItems),

	[] = arweave_config_store:items_with_prefix([baz]),

	ok.

to_map_nested(_Config) ->
	{ok, _} = arweave_config_store:set([deep, x, y], 1),
	{ok, _} = arweave_config_store:set([deep, x, z], 2),

	Map = arweave_config_store:to_map(),
	true = is_map(Map),
	#{deep := #{x := #{y := 1, z := 2}}} = Map,

	ok.

log_smoke(_Config) ->
	{ok, _} = arweave_config_store:set([log_smoke, key], <<"value">>),
	ok = arweave_config_store:log(),
	ok.
