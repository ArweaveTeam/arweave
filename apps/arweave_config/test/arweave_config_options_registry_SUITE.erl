%%% @doc Unit tests for `arweave_config_options_registry'. Each
%%% testcase boots a minimal registry with a hand-rolled spec list via
%%% `specs/1' so behavior can be observed in isolation from the real
%%% spec contributors.
-module(arweave_config_options_registry_SUITE).
-compile([export_all, nowarn_export_all]).

-include("arweave_config.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(TestCase, Config) ->
	{ok, PIDStore} = arweave_config_store:start_link(),
	{ok, PIDSpec} = arweave_config_options_registry:start_link(specs(TestCase)),
	[
		{arweave_config_store, PIDStore},
		{arweave_config_options_registry, PIDSpec}
		| Config
	].

end_per_testcase(_TestCase, _Config) ->
	ok = arweave_config_options_registry:stop(),
	ok = arweave_config_store:stop().

all() ->
	[
		default,
		default_value,
		default_type,
		default_get,
		default_set,
		default_set_state,
		default_multi,
		default_runtime,
		default_multi_types,
		default_multi_types_miss_returns_error,
		handle_set_exception_returns_error_tuple,
		handle_set_ignore_returns_current_value,
		handle_get_failure_with_default_falls_back,
		handle_get_failure_without_default_returns_error,
		unknown_call_returns_error,
		default_environment,
		default_enabled_and_deprecated,
		default_wildcard_option_long_argument,
		default_binary_option_key,
		duplicate_option_keys_silently_overwrite,
		default_legacy_ok,
		duplicate_legacy_aliases_are_ambiguous,
		resolve_exact_option,
		resolve_not_found,
		set_rejected_by_handler_preserves_store,
		validation_rollback_restores_old_value,
		validation_rollback_deletes_when_no_previous_value,
		get_local_set_local_basic
	].

%%====================================================================
%% Test cases
%%====================================================================

default(_Config) ->
	{error, undefined} = arweave_config_options_registry:get([default]),
	{ok, test} = arweave_config_options_registry:set([default], test),
	{ok, test} = arweave_config_store:get([default]).

default_value(_Config) ->
	{ok, true} = arweave_config_options_registry:get([default_value]),
	{error, undefined} = arweave_config_store:get([default_value]),
	{ok, false} = arweave_config_options_registry:set([default_value], false),
	{ok, false} = arweave_config_store:get([default_value]).

default_type(_Config) ->
	{ok, true} =
		arweave_config_options_registry:set([default_type], true),
	{error, #{ reason := type_check_failed }} =
		arweave_config_options_registry:set([default_type], "not a boolean").

default_get(_Config) ->
	{ok, valid} =
		arweave_config_options_registry:get([default_get]).

default_set(_Config) ->
	{ok, ok} =
		arweave_config_options_registry:set([default_set], self()),
	ok = receive
		ok -> ok
	after
		10 -> error
	end.

default_set_state(_Config) ->
	{ok, empty} =
		arweave_config_options_registry:set([default_set_state], ok),
	{ok, full} =
		arweave_config_options_registry:set([default_set_state], ok),
	ok.

default_multi(_Config) ->
	{ok, 1} = arweave_config_options_registry:get([one]),
	{ok, one} = arweave_config_options_registry:set([one], one),
	%% `three' always returns 3 regardless of what's set.
	{ok, 3} = arweave_config_options_registry:get([three]),
	{ok, 3} = arweave_config_options_registry:set([three], any),
	ok.

default_runtime(_Config) ->
	false = arweave_config:is_runtime(),
	{ok, 1} = arweave_config_options_registry:set([default], 1),
	{ok, 1} = arweave_config_options_registry:set([dynamic], 1),
	{ok, 1} = arweave_config_options_registry:set([explicitly_static], 1),
	%% Flip the lifecycle directly so we don't have to satisfy the
	%% real validators against this minimal hand-rolled spec set.
	ok = arweave_config_options_registry:set_runtime(true),
	true = arweave_config:is_runtime(),
	%% Each runtime set re-runs validation; stub it to a no-op so the
	%% real validators don't fire against the minimal spec set.
	ok = meck:new(arweave_config_validate, [passthrough]),
	try
		meck:expect(arweave_config_validate, run, fun() -> ok end),
		%% Default `runtime' is false, so sets are rejected after the
		%% lifecycle transition.
		{error, _} = arweave_config_options_registry:set([default], 2),
		{ok, 2} = arweave_config_options_registry:set([dynamic], 2),
		{error, _} = arweave_config_options_registry:set([explicitly_static], 2)
	after
		meck:unload(arweave_config_validate)
	end.

default_multi_types(_Config) ->
	{ok, true} =
		arweave_config_options_registry:set([default], <<"true">>),
	{ok, 1} =
		arweave_config_options_registry:set([default], 1),
	{ok, <<"127.0.0.1">>} =
		arweave_config_options_registry:set([default], <<"127.0.0.1">>),
	ok.

%% None of the declared types accept this value, so the multi-type
%% path falls through to the same `type_check_failed' error as the
%% single-type miss in `default_type'.
default_multi_types_miss_returns_error(_Config) ->
	{error, #{ reason := type_check_failed }} =
		arweave_config_options_registry:set([default], <<"definitely not">>).

handle_set_exception_returns_error_tuple(_Config) ->
	?assertMatch({error, {error, bad_set}},
		arweave_config_options_registry:set([raises], value)),
	{error, undefined} = arweave_config_options_registry:get([raises]).

%% `ignore' tells the registry to return the current value without
%% touching the store. With no prior set, the current value is the
%% spec default.
handle_set_ignore_returns_current_value(_Config) ->
	?assertEqual({ok, initial},
		arweave_config_options_registry:set([ignored], anything_else)),
	?assertEqual({error, undefined}, arweave_config_store:get([ignored])).

%% A `handle_get' that returns anything other than `{ok, _}' silently
%% falls back to the spec default when one is declared.
handle_get_failure_with_default_falls_back(_Config) ->
	?assertEqual({ok, fallback},
		arweave_config_options_registry:get([failing_get_with_default])).

%% A `handle_get' that returns anything other than `{ok, _}' surfaces
%% as `{error, Whatever}' when no default is declared.
handle_get_failure_without_default_returns_error(_Config) ->
	?assertEqual({error, some_error},
		arweave_config_options_registry:get([failing_get_no_default])).

unknown_call_returns_error(_Config) ->
	?assertMatch({error, {unknown_call, ping}},
		gen_server:call(arweave_config_options_registry, ping)).

default_environment(_Config) ->
	Env = arweave_config_options_registry:get_environments(),
	{value, {<<"AR_ENVIRONMENT_ENABLED">>, [environment,enabled]}} =
		lists:search(fun
			({_,[environment,enabled]}) -> true;
			(_) -> false
		end,
		Env
	),
	ok.

default_enabled_and_deprecated(_Config) ->
	[] = ets:lookup(arweave_config_options_registry, [enabled,disabled]),
	[{_, #{ deprecated := true }}] =
		ets:lookup(arweave_config_options_registry, [deprecated,plain]),
	[{_, #{ deprecated := true }}] =
		ets:lookup(arweave_config_options_registry, [deprecated,with_message]),
	ok.

default_wildcard_option_long_argument(_Config) ->
	?assertEqual({error, not_found},
		arweave_config_options_registry:resolve([webhooks, <<"hook_a">>, url])),

	[{<<"--webhooks.[list_item].url">>, _}] =
		arweave_config_options_registry:get_long_argument(
			<<"--webhooks.[list_item].url">>),
	ok.

default_binary_option_key(_Config) ->
	[{_, #{ option_key := [foo, bar] }}] =
		ets:lookup(arweave_config_options_registry, [foo, bar]),
	ok.

duplicate_option_keys_silently_overwrite(_Config) ->
	[{_, Spec}] =
		ets:lookup(arweave_config_options_registry, [duplicate, key]),
	?assertEqual(second, maps:get(default, Spec)),
	?assertEqual(pos_integer, maps:get(type, Spec)),
	?assertEqual({ok, second},
		arweave_config_options_registry:get([duplicate, key])),
	ok.

default_legacy_ok(_Config) ->
	{ok, [legacy, bridge]} = arweave_config_options_registry:get_legacy(init),
	ok.

duplicate_legacy_aliases_are_ambiguous(_Config) ->
	LegacyMap = arweave_config_options_registry:get_legacy(),
	?assertEqual(1, maps:size(LegacyMap)),
	?assert(lists:member(
		maps:get(duplicate_alias, LegacyMap),
		[[legacy, first], [legacy, second]])),
	?assertEqual({error, undefined},
		arweave_config_options_registry:get_legacy(duplicate_alias)),
	ok.

resolve_exact_option(_Config) ->
	?assertMatch(
		{ok, [a, b], _Spec, Bindings} when map_size(Bindings) =:= 0,
		arweave_config_options_registry:resolve([a, b])),
	ok.

resolve_not_found(_Config) ->
	?assertEqual({error, not_found},
		arweave_config_options_registry:resolve(
			[this, key, does, 'not', exist])),
	ok.

%% Originally named `validation_rollback_on_set_failure', but the
%% spec's `handle_set' returns `{error, _}' before the store is
%% touched, so no rollback is involved — what's actually under test
%% is that a handler rejection leaves the previously stored value
%% intact.
set_rejected_by_handler_preserves_store(_Config) ->
	{ok, <<"first">>} =
		arweave_config_options_registry:set([guarded], <<"first">>),
	{ok, <<"first">>} = arweave_config_store:get([guarded]),
	{error, rejected_by_handler} =
		arweave_config_options_registry:set([guarded], <<"second">>),
	?assertEqual({ok, <<"first">>},
		arweave_config_store:get([guarded])),
	ok.

%% Real rollback path: store succeeds, validator (mocked) fails,
%% rollback restores the previous value.
validation_rollback_restores_old_value(_Config) ->
	%% Seed in load mode — post-store validation only fires in
	%% runtime mode, so the initial set must succeed unconditionally.
	{ok, <<"first">>} =
		arweave_config_options_registry:set([rolled_back], <<"first">>),
	{ok, <<"first">>} = arweave_config_store:get([rolled_back]),

	%% Flip the lifecycle flag without going through `runtime/0' so we
	%% don't have to satisfy the real validators.
	ok = arweave_config_options_registry:set_runtime(true),
	true = arweave_config:is_runtime(),

	ok = meck:new(arweave_config_validate, [passthrough]),
	try
		meck:expect(arweave_config_validate, run,
			fun() -> {error, validator_rejected_for_test} end),
		?assertEqual({error, validator_rejected_for_test},
			arweave_config_options_registry:set(
				[rolled_back], <<"second">>)),
		%% Rollback re-stored the previous value.
		?assertEqual({ok, <<"first">>},
			arweave_config_store:get([rolled_back]))
	after
		meck:unload(arweave_config_validate),
		ok = arweave_config_options_registry:set_runtime(false)
	end,
	ok.

%% Rollback's `OldValue =:= undefined' branch: when nothing was
%% previously stored, the rollback deletes the just-written key
%% rather than restoring anything.
validation_rollback_deletes_when_no_previous_value(_Config) ->
	ok = arweave_config_options_registry:set_runtime(true),
	true = arweave_config:is_runtime(),

	ok = meck:new(arweave_config_validate, [passthrough]),
	try
		meck:expect(arweave_config_validate, run,
			fun() -> {error, validator_rejected_for_test} end),
		?assertEqual({error, validator_rejected_for_test},
			arweave_config_options_registry:set(
				[rolled_back_no_default], <<"first">>)),
		%% Key never existed before; rollback removed it.
		?assertEqual({error, undefined},
			arweave_config_store:get([rolled_back_no_default]))
	after
		meck:unload(arweave_config_validate),
		ok = arweave_config_options_registry:set_runtime(false)
	end,
	ok.

get_local_set_local_basic(_Config) ->
	?assertMatch({ok, hello},
		arweave_config_options_registry:set_local([local_key], hello)),
	?assertEqual({ok, hello},
		arweave_config_options_registry:get_local([local_key])),
	ok.

%%====================================================================
%% Helpers
%%====================================================================

specs(default) ->
	[
		#{ option_key => [default] }
	];
specs(default_value) ->
	[
		#{
			option_key => [default_value],
			default => true
		 }
	];
specs(default_type) ->
	[
		#{
			option_key => [default_type],
			type => boolean
		}
	];
specs(default_get) ->
	[
		#{
			option_key => [default_get],
			handle_get => fun
				(_K, _S) ->
					{ok, valid}
			end
		}
	];
specs(default_set) ->
	[
		#{
			option_key => [default_set],
			handle_set => fun
				(_K, V, _S, _) ->
					V ! ok,
					{ok, ok}
			end
		}
	];
specs(default_set_state) ->
	[
		#{
			option_key => [default_set_state],
			handle_set => fun
				(_K, _V, #{ config := Config }, _) ->
					case Config of
						#{ default_set_state := empty } ->
							{store, full};
						_ ->
							{store, empty}
					end
			end
		}
	];
specs(default_multi) ->
	[
		#{
			option_key => [one],
			default => 1
		},
		#{
			option_key => [three],
			handle_get => fun
				(_K, _S) ->
					{ok, 3}
			end,
			handle_set => fun
				(_K, _V, _S, _) ->
					{ok, 3}
			end
		}
	];
specs(default_runtime) ->
	[
		#{
			option_key => [default]
		},
		#{
			option_key => [dynamic],
			runtime => true
		},
		#{
			option_key => [explicitly_static],
			runtime => false
		}
	];
specs(default_multi_types) ->
	[
		#{
			option_key => [default],
			type => [boolean, integer, ipv4]
		}
	];
specs(default_multi_types_miss_returns_error) ->
	[
		#{
			option_key => [default],
			type => [boolean, integer]
		}
	];
specs(handle_set_exception_returns_error_tuple) ->
	[
		#{
			option_key => [raises],
			handle_set => fun
				(_K, _V, _S, _) ->
					error(bad_set)
			end
		}
	];
specs(handle_set_ignore_returns_current_value) ->
	[
		#{
			option_key => [ignored],
			default => initial,
			handle_set => fun(_K, _V, _S, _) -> ignore end
		}
	];
specs(handle_get_failure_with_default_falls_back) ->
	[
		#{
			option_key => [failing_get_with_default],
			default => fallback,
			handle_get => fun(_K, _S) -> not_an_ok_tuple end
		}
	];
specs(handle_get_failure_without_default_returns_error) ->
	[
		#{
			option_key => [failing_get_no_default],
			handle_get => fun(_K, _S) -> some_error end
		}
	];
specs(unknown_call_returns_error) ->
	[
		#{ option_key => [default] }
	];
specs(default_environment) ->
	[
		#{ option_key => [environment,enabled] }
	];
specs(default_enabled_and_deprecated) ->
	[
		#{
			option_key => [enabled,disabled],
			enabled => false
		},
		#{
			option_key => [deprecated,plain],
			deprecated => true
		},
		#{
			option_key => [deprecated,with_message],
			deprecated => {true, <<"use something else">>}
		}
	];
specs(default_wildcard_option_long_argument) ->
	[
		#{ option_key => [webhooks, {list_item}, url] }
	];
specs(default_binary_option_key) ->
	[
		#{ option_key => <<"foo.bar">> }
	];
specs(duplicate_option_keys_silently_overwrite) ->
	[
		#{
			option_key => [duplicate, key],
			default => first,
			type => atom
		},
		#{
			option_key => [duplicate, key],
			default => second,
			type => pos_integer
		}
	];
specs(default_legacy_ok) ->
	[
		#{
			option_key => [legacy, bridge],
			legacy => init
		}
	];
specs(duplicate_legacy_aliases_are_ambiguous) ->
	[
		#{
			option_key => [legacy, first],
			legacy => duplicate_alias
		},
		#{
			option_key => [legacy, second],
			legacy => duplicate_alias
		}
	];
specs(resolve_exact_option) ->
	[
		#{ option_key => [a, b] }
	];
specs(resolve_not_found) ->
	[
		#{ option_key => [some, real, key] }
	];
specs(set_rejected_by_handler_preserves_store) ->
	[
		#{
			option_key => [guarded],
			%% First write stores the value; subsequent writes are
			%% rejected. The test verifies the rejected write does not
			%% overwrite what is already in the store.
			handle_set => fun
				(_K, _V2, #{ config := #{ guarded := _ } }, _) ->
					{error, rejected_by_handler};
				(_K, V, _S, _) ->
					{store, V}
			end
		}
	];
specs(validation_rollback_restores_old_value) ->
	[
		#{
			option_key => [rolled_back],
			runtime => true,
			handle_set => fun(_K, V, _S, _) -> {store, V} end
		}
	];
specs(validation_rollback_deletes_when_no_previous_value) ->
	[
		#{
			option_key => [rolled_back_no_default],
			runtime => true,
			handle_set => fun(_K, V, _S, _) -> {store, V} end
		}
	];
specs(get_local_set_local_basic) ->
	[
		#{ option_key => [local_key] }
	].
