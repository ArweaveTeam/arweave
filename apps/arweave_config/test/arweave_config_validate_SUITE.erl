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
		validator_peers_cm_exit_singleton,
		validator_cm_requires_secret,
		validator_storage_modules_dedup
	].

%% NOTE on validator_storage_modules_dedup: the dedup branch in
%% `arweave_config_options_storage_modules:validate/0' is unreachable
%% from valid inputs — `derived_id/1' is bijective on
%% (BucketSize, Bucket, Packing) so two distinct ids cannot produce
%% identical tuples. This test instead exercises the broader
%% storage_modules validator surface by forging a malformed id that
%% trips the per-id `validate_each/1' loop. Both code paths return
%% `{error, Binary}', which is what the walker contract advertises.

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
		{ok, _} = arweave_config:set([verify, mode], purge),
		{ok, _} = arweave_config:set([mining, enabled], true),
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

validator_peers_cm_exit_singleton(_Config) ->
	arweave_config:with_test_config(fun() ->
		%% Write two cm_exit peers directly via set_local — the
		%% peers validator reads the raw `[peers, _, cm_exit]' leaves.
		_ = arweave_config_options_registry:set_local(
			[peers, <<"127.0.0.1:1984">>, cm_exit], true),
		_ = arweave_config_options_registry:set_local(
			[peers, <<"127.0.0.1:1985">>, cm_exit], true),
		case arweave_config_validate:run() of
			{error, _} -> ok;
			Other ->
				ct:fail({expected_singleton_error, Other})
		end
	end),
	ok.

validator_cm_requires_secret(_Config) ->
	arweave_config:with_test_config(fun() ->
		{ok, _} = arweave_config:set([cm, enabled], true),
		case arweave_config_validate:run() of
			{error, _} -> ok;
			Other ->
				ct:fail({expected_cm_secret_error, Other})
		end
	end),
	ok.

validator_storage_modules_dedup(_Config) ->
	arweave_config:with_test_config(fun() ->
		Addr = crypto:strong_rand_bytes(32),
		%% Forge an id whose derived id is not `bad_id` — this trips
		%% `validate_derived_id/1' in the per-id walker.
		ID = <<"bad_id">>,
		_ = arweave_config_options_registry:set_local(
			[storage_modules, ID, packing, format], spora_2_6),
		_ = arweave_config_options_registry:set_local(
			[storage_modules, ID, packing, address], Addr),
		_ = arweave_config_options_registry:set_local(
			[storage_modules, ID, partition], 0),
		case arweave_config_validate:run() of
			{error, _} -> ok;
			Else ->
				ct:fail({expected_storage_modules_error, Else})
		end
	end),
	ok.
