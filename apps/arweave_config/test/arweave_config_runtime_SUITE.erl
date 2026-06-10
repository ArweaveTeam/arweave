%%% @doc Runtime-writability guard tests.
%%%
%%% `arweave_config' has a one-way lifecycle flag: during *load* mode
%%% every spec accepts writes; once `arweave_config:runtime/0' has
%%% flipped the flag only specs declared `runtime => true' continue to
%%% accept writes. This SUITE pins down that contract end-to-end:
%%% load-mode writes succeed for every spec, the runtime transition
%%% itself behaves correctly, and post-flip writes are accepted or
%%% rejected per the spec's `runtime' field across the relevant value
%%% types.
%%%
%%% Tests run against the full app (every spec contributor loaded), so
%%% the option_keys exercised below are real production specs.
-module(arweave_config_runtime_SUITE).
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
		load_mode_accepts_every_spec,
		runtime_flip_is_idempotent,
		runtime_writable_boolean,
		runtime_writable_pos_integer,
		non_runtime_scalar_rejected,
		non_runtime_address_rejected,
		non_runtime_list_replace_rejected
	].

%%====================================================================
%% Test cases
%%====================================================================

%% Load mode.
%% Before `runtime/0' is called, every spec accepts writes regardless
%% of its `runtime' annotation. Spot-check one of each polarity.
load_mode_accepts_every_spec(_Config) ->
	arweave_config:with_test_config(fun() ->
		false = arweave_config:is_runtime(),
		%% runtime => false (default for [data_dir]).
		ok = arweave_config:set([data_dir], "/tmp/load-mode"),
		%% runtime => true (default for [debug]).
		ok = arweave_config:set([debug], true),
		?assertEqual("/tmp/load-mode", arweave_config:get([data_dir])),
		?assertEqual(true, arweave_config:get([debug]))
	end),
	ok.

%% Lifecycle transition.
%% Second call to `runtime/0' is a no-op — the flag is already set
%% and the validator pass re-runs cleanly.
runtime_flip_is_idempotent(_Config) ->
	arweave_config:with_test_config(fun() ->
		false = arweave_config:is_runtime(),
		ok = arweave_config:runtime(),
		true = arweave_config:is_runtime(),
		ok = arweave_config:runtime(),
		true = arweave_config:is_runtime()
	end),
	ok.

%% Post-runtime: writes accepted on `runtime => true' specs.
%% Booleans declared `runtime => true' accept writes after the flip.
runtime_writable_boolean(_Config) ->
	arweave_config:with_test_config(fun() ->
		ok = arweave_config:set([debug], false),
		ok = arweave_config:runtime(),
		ok = arweave_config:set([debug], true),
		?assertEqual(true, arweave_config:get([debug])),
		ok = arweave_config:set([debug], false),
		?assertEqual(false, arweave_config:get([debug]))
	end),
	ok.

%% Integers declared `runtime => true' accept writes after the flip
%% (and the `pos_integer' type validator coerces binary input).
runtime_writable_pos_integer(_Config) ->
	arweave_config:with_test_config(fun() ->
		Key = [logging, formatter, max_size],
		ok = arweave_config:set(Key, 4096),
		ok = arweave_config:runtime(),
		ok = arweave_config:set(Key, 8192),
		?assertEqual(8192, arweave_config:get(Key)),
		%% Binary input still goes through the type validator.
		ok = arweave_config:set(Key, <<"1024">>),
		?assertEqual(1024, arweave_config:get(Key))
	end),
	ok.

%% Post-runtime: writes rejected on `runtime => false' specs.
%% Scalar `runtime => false' (string-typed). The store keeps the
%% load-mode value untouched.
non_runtime_scalar_rejected(_Config) ->
	arweave_config:with_test_config(fun() ->
		ok = arweave_config:set([data_dir], "/tmp/load-mode"),
		ok = arweave_config:runtime(),
		Result = arweave_config:set([data_dir], "/tmp/runtime-change"),
		?assertMatch(
			{error, #{reason := parameter_not_runtime_writable}},
			Result),
		?assertEqual("/tmp/load-mode", arweave_config:get([data_dir]))
	end),
	ok.

%% Address-typed `runtime => false' spec — the runtime guard fires
%% before the type validator, so even a syntactically valid value is
%% rejected.
non_runtime_address_rejected(_Config) ->
	arweave_config:with_test_config(fun() ->
		ok = arweave_config:runtime(),
		Result = arweave_config:set(
			[mining, address],
			<<"LKC84RnISouGUw4uMQGCpPS9yDC-tIoqM2UVbUIt-Sw">>),
		?assertMatch(
			{error, #{reason := parameter_not_runtime_writable}},
			Result)
	end),
	ok.

%% Static peer options reject normal `set/2` writes after runtime.
non_runtime_list_replace_rejected(_Config) ->
	arweave_config:with_test_config(fun() ->
		ok = arweave_config:set([peers, trusted], [{1,2,3,4,1984}]),
		ok = arweave_config:runtime(),
		?assertMatch(
			{error, #{
				reason := parameter_not_runtime_writable
			}},
			arweave_config:set([peers, trusted],
				[{1,2,3,4,1984}, {5,6,7,8,1984}])),
		?assertEqual([{1,2,3,4,1984}], arweave_config:get([peers, trusted]))
	end),
	ok.
