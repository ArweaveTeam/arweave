%%% @doc End-to-end tests for the peer role-list config model.
%%%
%%% Exercises:
%%%   - Round-trip: legacy peer fields → role-list entries →
%%%     legacy aggregator view.
%%%   - Cross-instance validators (D3 invariants).
-module(arweave_config_peers_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

init_per_testcase(hostname_peer_round_trip, Config) ->
	ok = arweave_config:start(),
	%% Resolve example.com deterministically so the eager-role test stays
	%% hermetic (no DNS).
	meck:new(ar_util, [passthrough]),
	meck:expect(ar_util, safe_parse_peer, fun(Peer) ->
		case iolist_to_binary([Peer]) of
			<<"example.com:1984">> -> {ok, [{1, 2, 3, 4, 1984}]};
			_ -> meck:passthrough([Peer])
		end
	end),
	Config;
init_per_testcase(_TestCase, Config) ->
	ok = arweave_config:start(),
	Config.

end_per_testcase(hostname_peer_round_trip, _Config) ->
	meck:unload(ar_util),
	ok = arweave_config:stop();
end_per_testcase(_TestCase, _Config) ->
	ok = arweave_config:stop().

all() ->
		[
		roundtrip_peers_list,
		roundtrip_cm_exit_peer,
		multi_role_peer_lists_correctly,
		set_rejects_cm_exit_list,
		runtime_transition_rejects_vdf_client_and_server_same_peer,
		runtime_transition_allows_vdf_client_and_server_different_peers,
		runtime_transition_allows_cm_exit,
		write_legacy_list_replaces_stale_entries,
		write_legacy_list_accepts_binary_and_string_peers,
		write_legacy_list_promotes_default_port_for_bare_ipv4,
		write_legacy_list_silently_skips_malformed_entries,
		write_legacy_singleton_not_set_clears_role,
		hostname_peer_round_trip,
		vdf_server_peer_hostname_preserved,
		by_role_and_singleton_accept_binary_role,
		by_role_unknown_role_binary_crashes,
		clear_and_replace_pre_runtime,
		clear_and_replace_rejected_at_runtime
	].

%%====================================================================
%% Test cases
%%====================================================================

roundtrip_peers_list(_Config) ->
	Input = [{1, 2, 3, 4, 1984}, {5, 6, 7, 8, 9999}],
	ok = arweave_config_options_peers:write_legacy_list(trusted, Input),

	?assertEqual(
		[{1,2,3,4,1984}, {5,6,7,8,9999}],
		arweave_config:get([peers, trusted])),

	AggList = arweave_config:get([peers, trusted]),
	?assertEqual(
		[{1,2,3,4,1984}, {5,6,7,8,9999}],
		lists:sort(AggList)),

	ok.

roundtrip_cm_exit_peer(_Config) ->
	Peer = {1, 2, 3, 4, 1984},
	ok = arweave_config_options_peers:write_legacy_singleton(cm_exit, Peer),

	?assertEqual({1,2,3,4,1984},
		arweave_config:get([peers, cm_exit])),

	Singleton = arweave_config:get([peers, cm_exit]),
	?assertEqual({1,2,3,4,1984}, Singleton),

	ok.

multi_role_peer_lists_correctly(_Config) ->
	Peer = {1, 2, 3, 4, 1984},
	ok = arweave_config_options_peers:write_legacy_list(trusted, [Peer]),
	ok = arweave_config_options_peers:write_legacy_list(cm_peer, [Peer]),
	ok = arweave_config_options_peers:write_legacy_singleton(cm_exit, Peer),

	?assertEqual([{1,2,3,4,1984}],
		arweave_config:get([peers, trusted])),
	?assertEqual([{1,2,3,4,1984}],
		arweave_config:get([peers, cm_peer])),
	?assertEqual({1,2,3,4,1984},
		arweave_config:get([peers, cm_exit])),

	?assertEqual([{1,2,3,4,1984}], arweave_config:get([peers, trusted])),
	?assertEqual([{1,2,3,4,1984}], arweave_config:get([peers, cm_peer])),
	?assertEqual({1,2,3,4,1984}, arweave_config:get([peers, cm_exit])),

	ok.

set_rejects_cm_exit_list(_Config) ->
	?assertMatch({error, #{reason := type_check_failed}},
		arweave_config:set([peers, cm_exit],
			[{1,2,3,4,1984}, {5,6,7,8,1984}])),
	?assertEqual(false, arweave_config:is_runtime()),
	ok.

runtime_transition_rejects_vdf_client_and_server_same_peer(_Config) ->
	ok = arweave_config:set([peers, vdf_client], [{1,2,3,4,1984}]),
	ok = arweave_config:set([peers, vdf_server], [{1,2,3,4,1984}]),
	?assertMatch(
		{error, {vdf_client_and_server_on_same_peer, _}},
		arweave_config:runtime()),
	?assertEqual(false, arweave_config:is_runtime()),
	ok.

runtime_transition_allows_vdf_client_and_server_different_peers(_Config) ->
	ok = arweave_config:set([peers, vdf_client], [{1,2,3,4,1984}]),
	ok = arweave_config:set([peers, vdf_server], [{5,6,7,8,1984}]),
	?assertEqual(ok, arweave_config:runtime()),
	?assertEqual(true, arweave_config:is_runtime()),
	ok.

%% Positive complement to `set_rejects_cm_exit_list': cm_exit is a scalar peer.
runtime_transition_allows_cm_exit(_Config) ->
	ok = arweave_config:set([peers, cm_exit], {1,2,3,4,1984}),
	?assertEqual(ok, arweave_config:runtime()),
	?assertEqual(true, arweave_config:is_runtime()),
	ok.

write_legacy_list_replaces_stale_entries(_Config) ->
	P1 = {1, 2, 3, 4, 1984},
	P2 = {5, 6, 7, 8, 9999},
	ok = arweave_config_options_peers:write_legacy_list(trusted, [P1, P2]),
	?assertEqual(
		[{1,2,3,4,1984}, {5,6,7,8,9999}],
		arweave_config:get([peers, trusted])),

	%% Rewrite with only P2; P1 must be removed from the role list.
	ok = arweave_config_options_peers:write_legacy_list(trusted, [P2]),
	?assertEqual([{5,6,7,8,9999}],
		arweave_config:get([peers, trusted])),
	?assertEqual([{5,6,7,8,9999}], arweave_config:get([peers, trusted])),
	ok.

write_legacy_singleton_not_set_clears_role(_Config) ->
	Peer = {1, 2, 3, 4, 1984},
	ok = arweave_config_options_peers:write_legacy_singleton(cm_exit, Peer),
	?assertEqual({1,2,3,4,1984},
		arweave_config:get([peers, cm_exit])),

	ok = arweave_config_options_peers:write_legacy_singleton(cm_exit, not_set),
	?assertEqual(not_set, arweave_config:get([peers, cm_exit])),
	ok.

%% `write_legacy_list/2' accepts tuple, binary, and string peer
%% representations. All three normalize to the same on-disk peer_id.
write_legacy_list_accepts_binary_and_string_peers(_Config) ->
	ok = arweave_config_options_peers:write_legacy_list(trusted,
		[<<"1.2.3.4:1984">>, "5.6.7.8:9999"]),
	?assertEqual(
		[{1,2,3,4,1984}, {5,6,7,8,9999}],
		arweave_config:get([peers, trusted])),
	?assertEqual(
		[{1,2,3,4,1984}, {5,6,7,8,9999}],
		lists:sort(arweave_config:get([peers, trusted]))),
	ok.

%% Port-less IPv4 input is normalized to the default port (1984).
write_legacy_list_promotes_default_port_for_bare_ipv4(_Config) ->
	ok = arweave_config_options_peers:write_legacy_list(trusted,
		[{1, 2, 3, 4}]),
	?assertEqual([{1,2,3,4,1984}],
		arweave_config:get([peers, trusted])),
	?assertEqual([{1,2,3,4,1984}], arweave_config:get([peers, trusted])),
	ok.

%% `write_one/2' silently drops entries that fail `peer_id/1'
%% validation rather than aborting the whole batch.
write_legacy_list_silently_skips_malformed_entries(_Config) ->
	Good1 = {1, 2, 3, 4, 1984},
	Good2 = {5, 6, 7, 8, 1984},
	%% `<<"##bad##">>' fails `validate_host/1' because `#' is outside
	%% the allowed character set.
	ok = arweave_config_options_peers:write_legacy_list(trusted,
		[Good1, <<"##bad##">>, Good2]),
	?assertEqual(
		[{1,2,3,4,1984}, {5,6,7,8,1984}],
		lists:sort(arweave_config:get([peers, trusted]))),
	?assertEqual(
		[{1,2,3,4,1984}, {5,6,7,8,1984}],
		arweave_config:get([peers, trusted])),
	ok.

%% Eager roles resolve hostnames to IPv4 peers at write time, matching
%% the legacy CLI/JSON parsers (example.com is mocked above).
hostname_peer_round_trip(_Config) ->
	ok = arweave_config_options_peers:write_legacy_list(trusted,
		[<<"example.com:1984">>]),
	?assertEqual([{1, 2, 3, 4, 1984}],
		arweave_config:get([peers, trusted])),
	ok.

%% VDF peers must round-trip as hostnames so the runtime resolver can
%% re-resolve them after DNS changes (see `ar_peers:resolve_and_cache_peer/2').
vdf_server_peer_hostname_preserved(_Config) ->
	ok = arweave_config_options_peers:write_legacy_list(vdf_server,
		[<<"vdf.example.com">>]),
	?assertEqual([<<"vdf.example.com:1984">>],
		arweave_config:get([peers, vdf_server])),
	ok.

%% Both readers accept the role as a binary, routed via
%% `binary_to_existing_atom/1'.
by_role_and_singleton_accept_binary_role(_Config) ->
	ok = arweave_config_options_peers:write_legacy_list(trusted,
		[{1, 2, 3, 4, 1984}]),
	ok = arweave_config_options_peers:write_legacy_singleton(cm_exit,
		{5, 6, 7, 8, 1984}),
	?assertEqual([{1, 2, 3, 4, 1984}],
		arweave_config_options_peers:by_role(<<"trusted">>)),
	?assertEqual({5, 6, 7, 8, 1984},
		arweave_config_options_peers:singleton_by_role(<<"cm_exit">>)),
	ok.

%% Unknown role binaries fail fast via `binary_to_existing_atom/1'.
%% The role name below is deliberately implausible to avoid an
%% accidental atom-table hit.
by_role_unknown_role_binary_crashes(_Config) ->
	?assertError(badarg,
		arweave_config_options_peers:by_role(
			<<"zzz_no_such_peer_role_zzz">>)),
	?assertError(badarg,
		arweave_config_options_peers:singleton_by_role(
			<<"zzz_no_such_peer_role_zzz">>)),
	ok.

clear_and_replace_pre_runtime(_Config) ->
	?assertEqual(false, arweave_config:is_runtime()),
	P1 = {1,2,3,4,1984},
	P2 = {5,6,7,8,1984},

	ok = arweave_config:set([peers, trusted], [P1, P2]),
	?assertEqual(lists:sort([P1, P2]),
		lists:sort(arweave_config:get([peers, trusted]))),

	ok = arweave_config:set([peers, trusted], [P2]),
	?assertEqual([P2], arweave_config:get([peers, trusted])),

	ok = arweave_config:set([peers, trusted], []),
	?assertEqual([], arweave_config:get([peers, trusted])),

	ok.

clear_and_replace_rejected_at_runtime(_Config) ->
	?assertEqual(false, arweave_config:is_runtime()),
	P1 = {1,2,3,4,1984},
	P2 = {5,6,7,8,1984},

	%% Seed state before flipping runtime.
	ok = arweave_config:set([peers, trusted], [P1]),
	?assertEqual([P1], arweave_config:get([peers, trusted])),

	ok = arweave_config:runtime(),
	?assertEqual(true, arweave_config:is_runtime()),

	?assertMatch(
		{error, #{reason := parameter_not_runtime_writable}},
		arweave_config:set([peers, trusted], [P1, P2])),
	?assertEqual([P1], arweave_config:get([peers, trusted]),
		"set must not write at runtime"),

	?assertMatch(
		{error, #{reason := parameter_not_runtime_writable}},
		arweave_config:set([peers, trusted], [])),
	?assertEqual([P1], arweave_config:get([peers, trusted]),
		"clear-via-set must not delete entries at runtime"),

	ok.
