%%% @doc Tests for `arweave_config_validate:run/0' — the fail-fast
%%% walker that runs each option module's `validate/0' callback.
-module(arweave_config_validate_SUITE).
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
		walker_passes_on_clean_config,
		walker_returns_first_error,
		walker_catches_exceptions,
		validator_cm_requires_secret,
		validator_storage_modules_dedup,
		validator_repack_allows_differing_addresses,
		validator_repack_rejects_mixed_archetypes
	].

%%====================================================================
%% Test cases
%%====================================================================

walker_passes_on_clean_config(_Config) ->
	arweave_config:with_test_config(fun() ->
		?assertEqual(ok, arweave_config_validate:run())
	end),
	ok.

walker_returns_first_error(_Config) ->
	arweave_config:with_test_config(fun() ->
		ok = arweave_config:set([verify, mode], purge),
		ok = arweave_config:set([mining, enabled], true),
		case arweave_config_validate:run() of
			{error, _} -> ok;
			Other ->
				ct:fail({expected_error, Other})
		end
	end),
	ok.

walker_catches_exceptions(_Config) ->
	arweave_config:with_test_config(fun() ->
		Target = arweave_config_options_misc,
		ok = meck:new(Target, [passthrough]),
		try
			meck:expect(Target, validate, fun() -> erlang:error(boom) end),
			Result = arweave_config_validate:run(),
			?assertMatch({error, {validator_crash, Target, error, boom}}, Result)
		after
			meck:unload(Target)
		end
	end),
	ok.

validator_cm_requires_secret(_Config) ->
	arweave_config:with_test_config(fun() ->
		ok = arweave_config:set([cm, enabled], true),
		case arweave_config_validate:run() of
			{error, _} -> ok;
			Other ->
				ct:fail({expected_cm_secret_error, Other})
		end
	end),
	ok.

validator_storage_modules_dedup(_Config) ->
	arweave_config:with_test_config(fun() ->
		Module = #{
			partition => 0,
			packing_format => unpacked,
			defrag => false
		},
		ok = arweave_config:set([storage_modules], [Module, Module]),
		case arweave_config_validate:run() of
			{error, _} -> ok;
			Else ->
				ct:fail({expected_storage_modules_error, Else})
		end
	end),
	ok.

%% Same archetype (replica.2.9 -> replica.2.9) on every module, but with different source
%% and target addresses per module - this must be allowed.
validator_repack_allows_differing_addresses(_Config) ->
	arweave_config:with_test_config(fun() ->
		AddrA = crypto:strong_rand_bytes(32),
		AddrB = crypto:strong_rand_bytes(32),
		AddrC = crypto:strong_rand_bytes(32),
		AddrD = crypto:strong_rand_bytes(32),
		Module0 = #{
			partition => 0,
			from_format => replica_2_9, from_address => AddrA,
			to_format => replica_2_9, to_address => AddrB
		},
		Module1 = #{
			partition => 1,
			from_format => replica_2_9, from_address => AddrC,
			to_format => replica_2_9, to_address => AddrD
		},
		ok = arweave_config:set([repack_modules], [Module0, Module1]),
		?assertEqual(ok, arweave_config_validate:run())
	end),
	ok.

%% Mixing archetypes (replica.2.9 -> unpacked vs unpacked -> replica.2.9) must be rejected.
validator_repack_rejects_mixed_archetypes(_Config) ->
	arweave_config:with_test_config(fun() ->
		AddrA = crypto:strong_rand_bytes(32),
		AddrB = crypto:strong_rand_bytes(32),
		Module0 = #{
			partition => 0,
			from_format => replica_2_9, from_address => AddrA,
			to_format => unpacked
		},
		Module1 = #{
			partition => 1,
			from_format => unpacked,
			to_format => replica_2_9, to_address => AddrB
		},
		ok = arweave_config:set([repack_modules], [Module0, Module1]),
		case arweave_config_validate:run() of
			{error, _} -> ok;
			Else ->
				ct:fail({expected_repack_archetype_error, Else})
		end
	end),
	ok.
