%%% @doc CT coverage for the `limiter` option group. Verifies that
%%% every default group id is registered, that spec defaults are
%%% retrievable via `arweave_config:get/1', and that the `local_peers'
%%% bypass default holds.
-module(arweave_config_options_limiter_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
    ok = arweave_config:start(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = arweave_config:stop().

all() ->
    [
        known_group_ids_are_registered,
        spec_default_round_trip,
        local_peers_no_limit_default_is_true,
        override_only_touches_target_field
    ].

%%====================================================================
%% Test cases
%%====================================================================

known_group_ids_are_registered(_Config) ->
    IDs = arweave_config_options_limiter:group_ids(),
    true = length(IDs) > 0,
    true = lists:member(general, IDs),
    true = lists:member(chunk, IDs),
    true = lists:member(local_peers, IDs).

%% Every registered group must have a defined `concurrency_limit'
%% reachable via the canonical key. Picks one field as a smoke check
%% for the round-trip; per-field validation is the spec module's job.
%% Bypass groups (`no_limit => true') sentinel their per-limit fields
%% with `infinity', so the round-trip accepts either a non-negative
%% integer or the `infinity' atom.
spec_default_round_trip(_Config) ->
    [begin
        Value = arweave_config:get([limiter, ID, concurrency_limit]),
        ?assert(Value =:= infinity orelse is_integer(Value))
     end || ID <- arweave_config_options_limiter:group_ids()].

local_peers_no_limit_default_is_true(_Config) ->
    true = arweave_config:get([limiter, local_peers, no_limit]),
    %% Every other registered group defaults to no_limit = false.
    [?assertEqual(
        {ID, false},
        {ID, arweave_config:get([limiter, ID, no_limit])})
     || ID <- arweave_config_options_limiter:group_ids(), ID =/= local_peers].

override_only_touches_target_field(_Config) ->
    DefaultSliding =
        arweave_config:get([limiter, chunk, sliding_window_limit]),
    DefaultGeneralConcurrency =
        arweave_config:get([limiter, general, concurrency_limit]),
    ok = arweave_config:set([limiter, chunk, concurrency_limit], 999),
    999 = arweave_config:get([limiter, chunk, concurrency_limit]),
    %% Sibling field on the same group is untouched.
    DefaultSliding =
        arweave_config:get([limiter, chunk, sliding_window_limit]),
    %% Different group is untouched.
    DefaultGeneralConcurrency =
        arweave_config:get([limiter, general, concurrency_limit]).
