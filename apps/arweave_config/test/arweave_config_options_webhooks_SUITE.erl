%%% @doc Behavioural tests for the webhook list config model.
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
		runtime_transition_rejects_webhook_with_unknown_event,
		runtime_transition_skips_disabled_webhook,
		reload_replaces_prior_webhook_list
	].

%%====================================================================
%% Test cases
%%====================================================================

roundtrip_single_webhook(_Config) ->
	Hook = #{
		events => [<<"block">>, <<"transaction">>],
		url => <<"https://example.com/hook">>,
		headers => #{<<"Authorization">> => <<"Bearer 123">>}
	},
	ok = arweave_config_options_webhooks:write_legacy_list([Hook]),

	?assertEqual([Hook#{enabled => true}], arweave_config:get([webhooks])),
	?assertEqual([Hook], arweave_config_options_webhooks:legacy_list()),
	ok.

roundtrip_multiple_webhooks_with_shared_url(_Config) ->
	Url = <<"https://example.com/hook">>,
	A = #{ events => [<<"block">>], url => Url, headers => #{} },
	B = #{ events => [<<"transaction">>], url => Url,
		headers => #{<<"X-Auth">> => <<"v1">>} },
	ok = arweave_config_options_webhooks:write_legacy_list([A, B]),

	?assertEqual([A, B], arweave_config_options_webhooks:legacy_list()),
	ok.

disabled_webhook_omitted_from_legacy_view(_Config) ->
	Hook = #{
		events => [<<"block">>],
		url => <<"https://example.com/hook">>,
		headers => #{},
		enabled => false
	},
	ok = arweave_config:set([webhooks], [Hook]),
	?assertEqual([], arweave_config_options_webhooks:legacy_list()),
	?assertEqual([Hook], arweave_config:get([webhooks])),
	ok.

runtime_transition_rejects_webhook_missing_url(_Config) ->
	ok = arweave_config:set([webhooks], [#{events => [<<"block">>]}]),
	?assertMatch(
		{error, _},
		arweave_config:runtime()),
	?assertEqual(false, arweave_config:is_runtime()),
	ok.

runtime_transition_rejects_webhook_with_no_events(_Config) ->
	ok = arweave_config:set([webhooks], [
		#{url => <<"https://example.com/hook">>}
	]),
	?assertMatch(
		{error, _},
		arweave_config:runtime()),
	?assertEqual(false, arweave_config:is_runtime()),
	ok.

%% An unknown event would otherwise subscribe to nothing and never fire.
runtime_transition_rejects_webhook_with_unknown_event(_Config) ->
	ok = arweave_config:set([webhooks], [
		#{url => <<"https://example.com/hook">>, events => [<<"nonsense">>]}
	]),
	?assertMatch(
		{error, _},
		arweave_config:runtime()),
	?assertEqual(false, arweave_config:is_runtime()),
	ok.

runtime_transition_skips_disabled_webhook(_Config) ->
	ok = arweave_config:set([webhooks], [#{enabled => false}]),
	?assertEqual(ok, arweave_config:runtime()),
	?assertEqual(true, arweave_config:is_runtime()),
	ok.

reload_replaces_prior_webhook_list(_Config) ->
	A = #{ events => [<<"block">>],
		url => <<"https://a.example/hook">>, headers => #{} },
	B = #{ events => [<<"transaction">>],
		url => <<"https://b.example/hook">>, headers => #{} },
	ok = arweave_config_options_webhooks:write_legacy_list([A]),
	ok = arweave_config_options_webhooks:write_legacy_list([B]),
	%% Only B survives.
	?assertEqual([B], arweave_config_options_webhooks:legacy_list()),
	?assertEqual([B#{enabled => true}], arweave_config:get([webhooks])),
	ok.
