%%% @doc Behavioural tests for the indexed-webhook config model.
%%%
%%% Verifies legacy round-trip, validator semantics, and that
%%% disabled webhooks vanish from the legacy list view.
-module(arweave_config_options_webhooks_SUITE).
-compile([export_all, nowarn_export_all]).
-include("arweave_config.hrl").
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
		roundtrip_single_webhook,
		roundtrip_multiple_webhooks_with_shared_url,
		disabled_webhook_omitted_from_legacy_view,
		runtime_transition_rejects_webhook_missing_url,
		runtime_transition_rejects_webhook_with_no_events,
		runtime_transition_skips_disabled_webhook,
		reload_clears_prior_legacy_instances
	].

%%====================================================================
%% Test cases
%%====================================================================

roundtrip_single_webhook(_Config) ->
	Hook = #{
		events => [block, transaction],
		url => <<"https://example.com/hook">>,
		headers => [{<<"Authorization">>, <<"Bearer 123">>}]
	},
	ok = arweave_config_options_webhooks:write_legacy_list([Hook]),

	%% Per-instance leaves exist under legacy_1.
	?assertEqual(true,
		arweave_config:get([webhooks, legacy_1, enabled])),
	?assertEqual(<<"https://example.com/hook">>,
		arweave_config:get([webhooks, legacy_1, url])),
	?assertEqual([block, transaction],
		arweave_config:get([webhooks, legacy_1, events])),
	?assertEqual([{<<"Authorization">>, <<"Bearer 123">>}],
		arweave_config:get([webhooks, legacy_1, headers])),

	%% Public aggregate reader returns the original map.
	?assertEqual([Hook], arweave_config:webhooks()),
	ok.

roundtrip_multiple_webhooks_with_shared_url(_Config) ->
	Url = <<"https://example.com/hook">>,
	A = #{ events => [block], url => Url, headers => [] },
	B = #{ events => [transaction], url => Url,
		headers => [{<<"X-Auth">>, <<"v1">>}] },
	ok = arweave_config_options_webhooks:write_legacy_list([A, B]),

	?assertEqual([A, B], arweave_config:webhooks()),
	ok.

disabled_webhook_omitted_from_legacy_view(_Config) ->
	Hook = #{
		events => [block],
		url => <<"https://example.com/hook">>,
		headers => []
	},
	ok = arweave_config_options_webhooks:write_legacy_list([Hook]),
	%% Flip enabled to false directly.
	{ok, _} =
		arweave_config:set([webhooks, legacy_1, enabled], false),

	?assertEqual([], arweave_config:webhooks()),
	%% The leaves still exist if the operator wants to re-enable.
	?assertEqual(false,
		arweave_config:get([webhooks, legacy_1, enabled])),
	?assertEqual(<<"https://example.com/hook">>,
		arweave_config:get([webhooks, legacy_1, url])),
	ok.

runtime_transition_rejects_webhook_missing_url(_Config) ->
	%% Set the leaves directly to bypass the legacy bridge's URL
	%% requirement (a legacy hook always has a URL because the
	%% legacy parser requires one). This simulates a hand-edited
	%% YAML config that forgot the URL.
	{ok, _} = arweave_config:set([webhooks, my_hook, enabled], true),
	{ok, _} = arweave_config:set([webhooks, my_hook, events], [block]),
	?assertMatch(
		{error, _},
		arweave_config:runtime()),
	?assertEqual(false, arweave_config:is_runtime()),
	ok.

runtime_transition_rejects_webhook_with_no_events(_Config) ->
	{ok, _} = arweave_config:set([webhooks, my_hook, enabled], true),
	{ok, _} =
		arweave_config:set([webhooks, my_hook, url],
			<<"https://example.com/hook">>),
	?assertMatch(
		{error, _},
		arweave_config:runtime()),
	?assertEqual(false, arweave_config:is_runtime()),
	ok.

runtime_transition_skips_disabled_webhook(_Config) ->
	{ok, _} = arweave_config:set([webhooks, my_hook, enabled], false),
	?assertEqual(ok, arweave_config:runtime()),
	?assertEqual(true, arweave_config:is_runtime()),
	ok.

reload_clears_prior_legacy_instances(_Config) ->
	A = #{ events => [block],
		url => <<"https://a.example/hook">>, headers => [] },
	B = #{ events => [transaction],
		url => <<"https://b.example/hook">>, headers => [] },
	ok = arweave_config_options_webhooks:write_legacy_list([A]),
	ok = arweave_config_options_webhooks:write_legacy_list([B]),
	%% Only B survives.
	?assertEqual([B], arweave_config:webhooks()),
	?assertEqual(<<"https://b.example/hook">>,
		arweave_config:get([webhooks, legacy_1, url])),
	ok.
