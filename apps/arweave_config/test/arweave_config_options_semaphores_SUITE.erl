%%% @doc Behavioural tests for the per-semaphore config model.
%%%
%%% Verifies legacy round-trip, default surfacing, partial-override
%%% semantics, and that a re-load replaces rather than appends.
-module(arweave_config_options_semaphores_SUITE).
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
		per_name_get_returns_compile_time_default,
		partial_overrides_keep_other_defaults,
		reload_clears_prior_entries,
		unknown_semaphore_name_rejected_at_set
	].

%%====================================================================
%% Test cases
%%====================================================================

per_name_get_returns_compile_time_default(_Config) ->
	?assertEqual(?MAX_PARALLEL_GET_CHUNK_REQUESTS,
		arweave_config:get([semaphores, get_chunk, limit])),
	?assertEqual(?MAX_PARALLEL_POST_TX_REQUESTS,
		arweave_config:get([semaphores, post_tx, limit])),
	ok.

partial_overrides_keep_other_defaults(_Config) ->
	ok = arweave_config_options_semaphores:write_legacy_map(
		#{ get_chunk => 999 }),

	%% Per-leaf get sees the override.
	?assertEqual(999,
		arweave_config:get([semaphores, get_chunk, limit])),

	%% Aggregate read fills in defaults for unmentioned names.
	Map = arweave_config:semaphores(),
	?assertEqual(999, maps:get(get_chunk, Map)),
	?assertEqual(?MAX_PARALLEL_POST_CHUNK_REQUESTS,
		maps:get(post_chunk, Map)),
	?assertEqual(?MAX_PARALLEL_GET_TX_REQUESTS,
		maps:get(get_tx, Map)),
	ok.

reload_clears_prior_entries(_Config) ->
	ok = arweave_config_options_semaphores:write_legacy_map(
		#{ get_chunk => 999 }),
	ok = arweave_config_options_semaphores:write_legacy_map(
		#{ post_tx => 777 }),

	%% get_chunk is back to its default; post_tx has the new value.
	?assertEqual(?MAX_PARALLEL_GET_CHUNK_REQUESTS,
		arweave_config:get([semaphores, get_chunk, limit])),
	?assertEqual(777,
		arweave_config:get([semaphores, post_tx, limit])),
	ok.

%% Names are a closed set (default_limits/0). The registry rejects
%% writes to any other name because no spec matches the option_key.
unknown_semaphore_name_rejected_at_set(_Config) ->
	?assertMatch({error, _},
		arweave_config:set([semaphores, made_up_name, limit], 5)),
	?assertEqual({error, undefined},
		arweave_config_store:get([semaphores, made_up_name, limit])),
	ok.
