%%% @doc Tests for the `arweave_throttling_router' ETS routing table.
%%% @end
-module(arweave_throttling_router_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
		init_creates_tables/1,
		update_new_unchanged_changed/1,
		lookup_found_and_unknown/1,
		delete_existing_and_missing/1,
		info_reports_counts/1,
		decrement_branches/1,
		errors_without_tables/1,
		cleanup_removes_tables/1
		]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(M, arweave_throttling_router).

suite() -> [{userdata, [description()]}, {timetrap, {seconds, 30}}].

description() ->
	{description, "arweave_throttling_router ETS routing table"}.

init_per_suite(Config) ->
	Config.

end_per_suite(_Config) ->
	ok.

init_per_testcase(_TestCase, Config) ->
	ok = ?M:init(),
	Config.

end_per_testcase(_TestCase, _Config) ->
	ok = ?M:cleanup(),
	ok.

all() ->
	[
	init_creates_tables,
	update_new_unchanged_changed,
	lookup_found_and_unknown,
	delete_existing_and_missing,
	info_reports_counts,
	decrement_branches,
	errors_without_tables,
	cleanup_removes_tables
	].

%%====================================================================
%% Test cases
%%====================================================================

init_creates_tables(_Config) ->
	?assertNotEqual(undefined, ets:info(arweave_throttling_router, size)),
	?assertNotEqual(undefined,
					ets:info(arweave_throttling_router_counters, size)),
	ok.

update_new_unchanged_changed(_Config) ->
	Peer = peer(1),
	?assertEqual({ok, new}, ?M:update_path(Peer, "/chunk/1", chunk)),
	?assertEqual({ok, unchanged}, ?M:update_path(Peer, "/chunk/1", chunk)),
	?assertEqual({ok, changed}, ?M:update_path(Peer, "/chunk/1", general)),
	?assertEqual({ok, general}, ?M:lookup_path(Peer, "/chunk/1")),
	ok.

lookup_found_and_unknown(_Config) ->
	Peer = peer(2),
	?assertEqual({error, unknown_key}, ?M:lookup_path(Peer, "/vdf")),
	{ok, new} = ?M:update_path(Peer, "/vdf", get_vdf),
	?assertEqual({ok, get_vdf}, ?M:lookup_path(Peer, "/vdf")),
	ok.

delete_existing_and_missing(_Config) ->
	Peer = peer(3),
	%% Deleting a missing key is a no-op, not an error.
	?assertEqual(ok, ?M:delete_path(Peer, "/metrics")),
	{ok, new} = ?M:update_path(Peer, "/metrics", metrics),
	?assertEqual(ok, ?M:delete_path(Peer, "/metrics")),
	?assertEqual({error, unknown_key}, ?M:lookup_path(Peer, "/metrics")),
	%% Counter row is gone once the group holds no keys.
	?assertEqual(#{keys_total => 0,
				group_ids_total => 0,
				keys_per_group => #{}},
				?M:info()),
	ok.

info_reports_counts(_Config) ->
	{ok, new} = ?M:update_path(peer(1), "/chunk/1", chunk),
	{ok, new} = ?M:update_path(peer(2), "/chunk/2", chunk),
	{ok, new} = ?M:update_path(peer(3), "/vdf", get_vdf),
	?assertEqual(#{keys_total => 3,
				group_ids_total => 2,
				keys_per_group => #{chunk => 2, get_vdf => 1}},
				?M:info()),
	ok.

%% @doc A group's counter is decremented on both change and delete, and
%% its row disappears only when the last key leaves it.
decrement_branches(_Config) ->
	{ok, new} = ?M:update_path(peer(1), "/a", chunk),
	{ok, new} = ?M:update_path(peer(2), "/b", chunk),
	%% chunk now holds 2 keys.
	?assertEqual(#{chunk => 2}, keys_per_group()),
	%% Re-route one key: chunk 2 -> 1 (row stays).
	{ok, changed} = ?M:update_path(peer(1), "/a", general),
	?assertEqual(#{chunk => 1, general => 1}, keys_per_group()),
	%% Delete the last chunk key: chunk 1 -> 0 (row removed).
	ok = ?M:delete_path(peer(2), "/b"),
	?assertEqual(#{general => 1}, keys_per_group()),
	ok.

errors_without_tables(_Config) ->
	ok = ?M:cleanup(),
	?assertMatch({error, _}, ?M:update_path(peer(1), "/x", general)),
	?assertMatch({error, _}, ?M:lookup_path(peer(1), "/x")),
	?assertMatch({error, _}, ?M:delete_path(peer(1), "/x")),
	ok.

cleanup_removes_tables(_Config) ->
	{ok, new} = ?M:update_path(peer(1), "/x", general),
	ok = ?M:cleanup(),
	?assertEqual(undefined, ets:info(arweave_throttling_router, size)),
	?assertEqual(undefined,
				ets:info(arweave_throttling_router_counters, size)),
	%% cleanup/0 is idempotent - safe to call when tables are gone.
	?assertEqual(ok, ?M:cleanup()),
	ok.

%%====================================================================
%% Helpers
%%====================================================================

peer(N) -> {N, N, N, N, 1984}.

keys_per_group() ->
	maps:get(keys_per_group, ?M:info()).
