%%% @doc Tests for the `arweave_throttling_peer_groups' ETS store.
%%% @end
-module(arweave_throttling_peer_compatibility_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(M, arweave_throttling_peer_compatibility).

suite() -> [{userdata, [description()]}, {timetrap, {seconds, 30}}].

description() ->
    {description, "arweave_throttling_peer_compatibility_register ETS store"}.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = ?M:init(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = ?M:cleanup(),
    ok.

all() ->
    [
     happy_path
    ].

happy_path(_Config) ->
    Peer = {1,2,3,4},
    Peer2 = {2,3,4,5},
    
    %% Default is compatible
    ?assert(?M:is_peer_marked_compatible(Peer)),

    %% No header -> incompatible
    ?assertEqual(ok, ?M:mark_incompatible(Peer)),
    ?assertNot(?M:is_peer_marked_compatible(Peer)),
    %% Idempotence
    ?assertEqual(ok, ?M:mark_incompatible(Peer)),
    ?assertNot(?M:is_peer_marked_compatible(Peer)),

    %% Header -> compatible
    ?assertEqual(ok, ?M:mark_compatible(Peer)),
    ?assert(?M:is_peer_marked_compatible(Peer)),
    %% Idempotence
    ?assertEqual(ok, ?M:mark_compatible(Peer)),
    ?assert(?M:is_peer_marked_compatible(Peer)),

    %% Can be cycled any number of times.
    ?assertEqual(ok, ?M:mark_incompatible(Peer)),
    ?assertNot(?M:is_peer_marked_compatible(Peer)),

    %% State independence
    ?assert(?M:is_peer_marked_compatible(Peer2)),

    ok.
