%%% @doc End-to-end tests for the indexed-peer config model.
%%%
%%% Exercises:
%%%   - Round-trip: legacy peer fields → per-instance entries →
%%%     legacy aggregator view.
%%%   - Cross-instance validators (D3 invariants).
%%%   - Spec resolution for wildcard peer keys.
-module(arweave_config_peers_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
	ok = arweave_config:start(),
	Config.

end_per_testcase(_TestCase, _Config) ->
	ok = arweave_config:stop().

all() ->
	[
		roundtrip_peers_list,
		roundtrip_cm_exit_peer_singleton,
		multi_role_peer_aggregates_correctly,
		wildcard_resolve_returns_bindings,
		runtime_transition_rejects_two_cm_exit,
		runtime_transition_rejects_vdf_client_and_server_same_peer,
		runtime_transition_allows_vdf_client_and_server_different_peers,
		runtime_transition_allows_single_cm_exit,
		write_legacy_list_replaces_stale_entries,
		write_legacy_list_accepts_binary_and_string_peers,
		write_legacy_list_promotes_default_port_for_bare_ipv4,
		write_legacy_list_silently_skips_malformed_entries,
		write_legacy_singleton_not_set_clears_role,
		hostname_peer_round_trip,
		by_role_and_singleton_accept_binary_role,
		by_role_unknown_role_binary_crashes,
		validator_rejects_peer_with_no_active_roles,
		clear_and_replace_pre_runtime,
		clear_and_replace_rejected_at_runtime
	].

%%====================================================================
%% Test cases
%%====================================================================

roundtrip_peers_list(_Config) ->
	Input = [{1, 2, 3, 4, 1984}, {5, 6, 7, 8, 9999}],
	ok = arweave_config_options_peers:write_legacy_list(trusted, Input),

	%% Per-instance entries should exist.
	?assertEqual(true,
		arweave_config:get([peers, <<"1.2.3.4:1984">>, trusted])),
	?assertEqual(true,
		arweave_config:get([peers, <<"5.6.7.8:9999">>, trusted])),

	%% Typed reader returns the original list.
	AggList = arweave_config:get_peers(trusted),
	?assertEqual(lists:sort(Input), lists:sort(AggList)),

	ok.

roundtrip_cm_exit_peer_singleton(_Config) ->
	Peer = {1, 2, 3, 4, 1984},
	ok = arweave_config_options_peers:write_legacy_singleton(cm_exit, Peer),

	?assertEqual(true,
		arweave_config:get([peers, <<"1.2.3.4:1984">>, cm_exit])),

	Singleton = arweave_config:get_peer(cm_exit),
	?assertEqual(Peer, Singleton),

	ok.

multi_role_peer_aggregates_correctly(_Config) ->
	Peer = {1, 2, 3, 4, 1984},
	ok = arweave_config_options_peers:write_legacy_list(trusted, [Peer]),
	ok = arweave_config_options_peers:write_legacy_list(cm_peer, [Peer]),
	ok = arweave_config_options_peers:write_legacy_singleton(cm_exit, Peer),

	%% Three boolean leaves under the same peer_id.
	?assertEqual(true,
		arweave_config:get([peers, <<"1.2.3.4:1984">>, trusted])),
	?assertEqual(true,
		arweave_config:get([peers, <<"1.2.3.4:1984">>, cm_peer])),
	?assertEqual(true,
		arweave_config:get([peers, <<"1.2.3.4:1984">>, cm_exit])),

	%% Three typed readers each reflect the peer.
	?assertEqual([Peer], arweave_config:get_peers(trusted)),
	?assertEqual([Peer], arweave_config:get_peers(cm_peer)),
	?assertEqual(Peer, arweave_config:get_peer(cm_exit)),

	ok.

wildcard_resolve_returns_bindings(_Config) ->
	Result = arweave_config_options_registry:resolve(
		[peers, <<"1.2.3.4:1984">>, cm_peer]),
	?assertMatch(
		{ok, [peers, <<"1.2.3.4:1984">>, cm_peer], _Spec,
			#{peer_id := <<"1.2.3.4:1984">>}},
		Result),
	ok.

runtime_transition_rejects_two_cm_exit(_Config) ->
	{ok, _} =
		arweave_config:set([peers, <<"1.2.3.4:1984">>, cm_exit], true),
	{ok, _} =
		arweave_config:set([peers, <<"5.6.7.8:1984">>, cm_exit], true),
	?assertMatch(
		{error, {multiple_cm_exit_peers, _}},
		arweave_config:runtime()),
	?assertEqual(false, arweave_config:is_runtime()),
	ok.

runtime_transition_rejects_vdf_client_and_server_same_peer(_Config) ->
	{ok, _} =
		arweave_config:set([peers, <<"1.2.3.4:1984">>, vdf_client], true),
	{ok, _} =
		arweave_config:set([peers, <<"1.2.3.4:1984">>, vdf_server], true),
	?assertMatch(
		{error, {vdf_client_and_server_on_same_peer, _}},
		arweave_config:runtime()),
	?assertEqual(false, arweave_config:is_runtime()),
	ok.

runtime_transition_allows_vdf_client_and_server_different_peers(_Config) ->
	{ok, _} =
		arweave_config:set([peers, <<"1.2.3.4:1984">>, vdf_client], true),
	{ok, _} =
		arweave_config:set([peers, <<"5.6.7.8:1984">>, vdf_server], true),
	?assertEqual(ok, arweave_config:runtime()),
	?assertEqual(true, arweave_config:is_runtime()),
	ok.

%% Positive complement to `runtime_transition_rejects_two_cm_exit': a
%% single peer carrying `cm_exit' must pass the singleton validator.
runtime_transition_allows_single_cm_exit(_Config) ->
	{ok, _} =
		arweave_config:set([peers, <<"1.2.3.4:1984">>, cm_exit], true),
	?assertEqual(ok, arweave_config:runtime()),
	?assertEqual(true, arweave_config:is_runtime()),
	ok.

write_legacy_list_replaces_stale_entries(_Config) ->
	P1 = {1, 2, 3, 4, 1984},
	P2 = {5, 6, 7, 8, 9999},
	ok = arweave_config_options_peers:write_legacy_list(trusted, [P1, P2]),
	?assertEqual(true,
		arweave_config:get([peers, <<"1.2.3.4:1984">>, trusted])),
	?assertEqual(true,
		arweave_config:get([peers, <<"5.6.7.8:9999">>, trusted])),

	%% Rewrite with only P2; P1's leaf must be cleared.
	ok = arweave_config_options_peers:write_legacy_list(trusted, [P2]),
	?assertEqual(false,
		arweave_config:get([peers, <<"1.2.3.4:1984">>, trusted])),
	?assertEqual(true,
		arweave_config:get([peers, <<"5.6.7.8:9999">>, trusted])),
	?assertEqual([P2], arweave_config:get_peers(trusted)),
	ok.

write_legacy_singleton_not_set_clears_role(_Config) ->
	Peer = {1, 2, 3, 4, 1984},
	ok = arweave_config_options_peers:write_legacy_singleton(cm_exit, Peer),
	?assertEqual(true,
		arweave_config:get([peers, <<"1.2.3.4:1984">>, cm_exit])),

	ok = arweave_config_options_peers:write_legacy_singleton(cm_exit, not_set),
	?assertEqual(false,
		arweave_config:get([peers, <<"1.2.3.4:1984">>, cm_exit])),
	?assertEqual(not_set, arweave_config:get_peer(cm_exit)),
	ok.

%% `write_legacy_list/2' accepts tuple, binary, and string peer
%% representations. All three normalize to the same on-disk peer_id.
write_legacy_list_accepts_binary_and_string_peers(_Config) ->
	ok = arweave_config_options_peers:write_legacy_list(trusted,
		[<<"1.2.3.4:1984">>, "5.6.7.8:9999"]),
	?assertEqual(true,
		arweave_config:get([peers, <<"1.2.3.4:1984">>, trusted])),
	?assertEqual(true,
		arweave_config:get([peers, <<"5.6.7.8:9999">>, trusted])),
	%% Round-trip back through `get_peers/1' returns the legacy tuple
	%% form, regardless of input shape.
	?assertEqual(
		lists:sort([{1, 2, 3, 4, 1984}, {5, 6, 7, 8, 9999}]),
		lists:sort(arweave_config:get_peers(trusted))),
	ok.

%% Port-less IPv4 input is normalized to the default port (1984).
write_legacy_list_promotes_default_port_for_bare_ipv4(_Config) ->
	ok = arweave_config_options_peers:write_legacy_list(trusted,
		[{1, 2, 3, 4}]),
	?assertEqual(true,
		arweave_config:get([peers, <<"1.2.3.4:1984">>, trusted])),
	?assertEqual([{1, 2, 3, 4, 1984}], arweave_config:get_peers(trusted)),
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
		lists:sort([Good1, Good2]),
		lists:sort(arweave_config:get_peers(trusted))),
	?assertEqual(false,
		arweave_config:get([peers, <<"##bad##">>, trusted])),
	ok.

%% Non-IPv4 hosts stay as binaries through the round-trip rather than
%% being coerced into the legacy IPv4 tuple shape.
hostname_peer_round_trip(_Config) ->
	ok = arweave_config_options_peers:write_legacy_list(trusted,
		[<<"example.com:1984">>]),
	?assertEqual(true,
		arweave_config:get([peers, <<"example.com:1984">>, trusted])),
	?assertEqual([<<"example.com:1984">>],
		arweave_config:get_peers(trusted)),
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

%% A peer that appears in the store with every role set to `false' is
%% rejected by `validate_at_least_one_role/2' at runtime transition.
validator_rejects_peer_with_no_active_roles(_Config) ->
	PeerId = <<"1.2.3.4:1984">>,
	{ok, _} = arweave_config:set([peers, PeerId, trusted], false),
	?assertMatch(
		{error, {peer_with_no_roles, [PeerId]}},
		arweave_config:runtime()),
	?assertEqual(false, arweave_config:is_runtime()),
	ok.

clear_and_replace_pre_runtime(_Config) ->
	?assertEqual(false, arweave_config:is_runtime()),
	P1 = {1, 2, 3, 4, 1984},
	P2 = {5, 6, 7, 8, 1984},

	ok = arweave_config:replace_peers(trusted, [P1, P2]),
	?assertEqual(lists:sort([P1, P2]),
		lists:sort(arweave_config:get_peers(trusted))),

	ok = arweave_config:replace_peers(trusted, [P2]),
	?assertEqual([P2], arweave_config:get_peers(trusted)),

	ok = arweave_config:clear_peers(trusted),
	?assertEqual([], arweave_config:get_peers(trusted)),

	ok.

clear_and_replace_rejected_at_runtime(_Config) ->
	?assertEqual(false, arweave_config:is_runtime()),
	P1 = {1, 2, 3, 4, 1984},
	P2 = {5, 6, 7, 8, 1984},

	%% Seed state before flipping runtime.
	ok = arweave_config:replace_peers(trusted, [P1]),
	?assertEqual([P1], arweave_config:get_peers(trusted)),

	ok = arweave_config:runtime(),
	?assertEqual(true, arweave_config:is_runtime()),

	?assertMatch(
		{error, #{reason := parameter_not_runtime_writable, role := trusted}},
		arweave_config:replace_peers(trusted, [P2])),
	?assertEqual([P1], arweave_config:get_peers(trusted),
		"replace must not clear or write at runtime"),

	?assertMatch(
		{error, #{reason := parameter_not_runtime_writable, role := trusted}},
		arweave_config:clear_peers(trusted)),
	?assertEqual([P1], arweave_config:get_peers(trusted),
		"clear must not delete entries at runtime"),

	ok.
