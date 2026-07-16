%%% @doc Tests for the `arweave_throttling_peer_groups' ETS store.
%%% @end
-module(arweave_throttling_distinct_group_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
		init_creates_tables/1,
		insert_new_and_duplicate/1,
		is_stored_reflects_membership/1,
		distinct_count_counts_distinct_group_ids/1,
		distinct_count_unknown_peer_is_zero/1,
		peers_counted_independently/1,
		errors_without_tables/1,
		cleanup_removes_tables/1
		]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(M, arweave_throttling_distinct_group).

suite() -> [{userdata, [description()]}, {timetrap, {seconds, 30}}].

description() ->
	{description, "arweave_throttling_peer_groups ETS store"}.

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
	insert_new_and_duplicate,
	is_stored_reflects_membership,
	distinct_count_counts_distinct_group_ids,
	distinct_count_unknown_peer_is_zero,
	peers_counted_independently,
	errors_without_tables,
	cleanup_removes_tables
	].

%%====================================================================
%% Test cases
%%====================================================================

init_creates_tables(_Config) ->
	?assertNotEqual(undefined,
			ets:info(arweave_throttling_distinct_group)),
	?assertNotEqual(undefined,
			ets:info(arweave_throttling_distinct_group_counts, size)),
	ok.

insert_new_and_duplicate(_Config) ->
	Peer = peer(1),
	?assertEqual({ok, new}, ?M:insert(Peer, <<"chunk">>)),
	%% Re-inserting the same pair does not raise the distinct count.
	?assertEqual({ok, duplicate}, ?M:insert(Peer, <<"chunk">>)),
	?assertEqual({ok, 1}, ?M:distinct_count(Peer)),
	ok.

%% @doc is_stored/2 reports membership without inserting.
is_stored_reflects_membership(_Config) ->
	Peer = peer(1),
	?assertEqual({ok, false}, ?M:is_stored(Peer, <<"chunk">>)),
	%% A membership check must not create the entry.
	?assertEqual({ok, 0}, ?M:distinct_count(Peer)),
	{ok, new} = ?M:insert(Peer, <<"chunk">>),
	?assertEqual({ok, true}, ?M:is_stored(Peer, <<"chunk">>)),
	%% A different group id for the same peer is not stored.
	?assertEqual({ok, false}, ?M:is_stored(Peer, <<"general">>)),
	%% The same group id for a different peer is not stored.
	?assertEqual({ok, false}, ?M:is_stored(peer(2), <<"chunk">>)),
	ok.

distinct_count_counts_distinct_group_ids(_Config) ->
	Peer = peer(2),
	{ok, new} = ?M:insert(Peer, <<"chunk">>),
	{ok, new} = ?M:insert(Peer, <<"general">>),
	{ok, new} = ?M:insert(Peer, <<"get_vdf">>),
	{ok, duplicate} = ?M:insert(Peer, <<"general">>),
	?assertEqual({ok, 3}, ?M:distinct_count(Peer)),
	ok.

distinct_count_unknown_peer_is_zero(_Config) ->
	?assertEqual({ok, 0}, ?M:distinct_count(peer(9))),
	ok.

peers_counted_independently(_Config) ->
	{ok, new} = ?M:insert(peer(1), <<"chunk">>),
	{ok, new} = ?M:insert(peer(1), <<"general">>),
	{ok, new} = ?M:insert(peer(2), <<"chunk">>),
	?assertEqual({ok, 2}, ?M:distinct_count(peer(1))),
	?assertEqual({ok, 1}, ?M:distinct_count(peer(2))),
	ok.

errors_without_tables(_Config) ->
	ok = ?M:cleanup(),
	?assertMatch({error, _}, ?M:insert(peer(1), <<"chunk">>)),
	?assertMatch({error, _}, ?M:distinct_count(peer(1))),
	ok.

cleanup_removes_tables(_Config) ->
	{ok, new} = ?M:insert(peer(1), <<"chunk">>),
	ok = ?M:cleanup(),
	?assertEqual(undefined,
		        ets:info(arweave_throttling_peer_groups, size)),
	?assertEqual(undefined,
			ets:info(arweave_throttling_peer_groups_counts, size)),
	%% cleanup/0 is idempotent - safe to call when tables are gone.
	?assertEqual(ok, ?M:cleanup()),
	ok.

%%====================================================================
%% Helpers
%%====================================================================

peer(N) -> {N, N, N, N, 1984}.
