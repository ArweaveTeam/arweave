%%% @doc Tests for `arweave_config_normalize:run/0' — the walker that
%%% invokes each option module's optional `normalize/0' callback.
-module(arweave_config_normalize_SUITE).
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
        walker_smoke,
        join_normalize_promotes_latest_state,
        verify_normalize_noop_when_disabled,
        verify_normalize_forces_flags_purge,
        verify_normalize_forces_flags_log,
        classify_legacy_flag_promotes_catalog_entries,
        classify_legacy_flag_promotes_dedicated_fields
    ].

%%====================================================================
%% Test cases
%%====================================================================

walker_smoke(_Config) ->
    arweave_config:with_test_config(fun() ->
        ?assertEqual(ok, arweave_config_normalize:run())
    end),
    ok.

join_normalize_promotes_latest_state(_Config) ->
    arweave_config:with_test_config(fun() ->
        %% Sanity: the latest-state flag defaults to false.
        ?assertEqual(false,
            arweave_config:get([join, start_from_latest_state])),
        %% Write a non-default value for start_from_state.
        ok = arweave_config:set([join, start_from_state], "/tmp/state"),
        ?assertEqual(ok, arweave_config_normalize:run()),
        ?assertEqual(true,
            arweave_config:get([join, start_from_latest_state]))
    end),
    ok.

%% When verify.mode is off (default `false`), normalize must leave
%% every flag the operator set alone.
verify_normalize_noop_when_disabled(_Config) ->
    arweave_config:with_test_config(fun() ->
        %% Pre-set a couple of flags to non-default values that the
        %% active-verify path would otherwise clobber.
        ok = arweave_config:set([join, auto], false),
        ok = arweave_config:set([sync, max_download_rate], 7),
        ok = arweave_config:set([cm, enabled], true),
        ?assertEqual(ok, arweave_config_normalize:run()),
        ?assertEqual(false, arweave_config:get([join, auto])),
        ?assertEqual(7, arweave_config:get([sync, max_download_rate])),
        ?assertEqual(true, arweave_config:get([cm, enabled]))
    end),
    ok.

verify_normalize_forces_flags_purge(_Config) ->
    assert_verify_normalize_forces_all_flags(purge).

verify_normalize_forces_flags_log(_Config) ->
    assert_verify_normalize_forces_all_flags(log).

%% Catalog flags: `enable foo` / `disable foo` for any flag declared in
%% the feature catalog should land at `[features, foo] = true | false`
%% directly (no staging keys).
classify_legacy_flag_promotes_catalog_entries(_Config) ->
    arweave_config:with_test_config(fun() ->
        [Flag | _] = arweave_config_features:names(),
        discard_io(fun() ->
            arweave_config_features:classify_legacy_flag(Flag, enable)
        end),
        ?assertEqual(true, arweave_config:get([features, Flag])),
        discard_io(fun() ->
            arweave_config_features:classify_legacy_flag(Flag, disable)
        end),
        ?assertEqual(false, arweave_config:get([features, Flag]))
    end),
    ok.

%% Promotion-table flags: a few `enable` / `disable` keywords map to
%% dedicated option_keys (e.g. `randomx_jit` -> `[randomx, jit]`).
classify_legacy_flag_promotes_dedicated_fields(_Config) ->
    arweave_config:with_test_config(fun() ->
        discard_io(fun() ->
            arweave_config_features:classify_legacy_flag(randomx_jit, disable)
        end),
        ?assertEqual(false, arweave_config:get([randomx, jit])),
        discard_io(fun() ->
            arweave_config_features:classify_legacy_flag(randomx_large_pages, enable)
        end),
        ?assertEqual(true, arweave_config:get([randomx, large_pages]))
    end),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% Pre-seeds every leaf and list value to the opposite of the forced
%% value, runs normalize with `[verify, mode]' set to `Mode', and
%% asserts every write took effect. `purge' and `log' share this
%% fan-out.
assert_verify_normalize_forces_all_flags(Mode) ->
    arweave_config:with_test_config(fun() ->
        %% Leaf pre-seeds: each value is chosen so the asserted
        %% forced value differs, ensuring every assertion proves a
        %% write happened.
        ok = arweave_config:set([join, auto], true),
        ok = arweave_config:set([join, start_from_latest_state], false),
        ok = arweave_config:set([sync, max_download_rate], 7),
        ok = arweave_config:set([gossip, block, pollers], 7),
        ok = arweave_config:set([gossip, header, workers], 7),
        ok = arweave_config:set([gossip, tx, polling_enabled], true),
        ok = arweave_config:set([packing, entropy, workers], 7),
        ok = arweave_config:set([cm, enabled], true),
        ok = arweave_config:set([gossip, tx, max_peers], 7),
        ok = arweave_config:set([gossip, block, max_peers], 7),
        ok = arweave_config:set([vdf, compute], true),
        ok = arweave_config:set([vdf, is_public_server], true),
        %% List pre-seeds: cm_peer, cm_exit, vdf_client,
        %% vdf_server. Distinct peers so cm_exit doesn't collide
        %% with cm_peer.
        ok = arweave_config:set([peers, cm_peer], [<<"127.0.0.1:1984">>]),
        ok = arweave_config:set([peers, cm_exit], <<"127.0.0.2:1984">>),
        ok = arweave_config:set([peers, vdf_client], [<<"127.0.0.3:1984">>]),
        ok = arweave_config:set([peers, vdf_server], [<<"127.0.0.4:1984">>]),

        %% Flip verify mode on and run normalize. The fan-out emits
        %% WARNING lines via io:format/1; sink them so CT logs stay
        %% clean.
        ok = arweave_config:set([verify, mode], Mode),
        discard_io(fun() ->
            ?assertEqual(ok, arweave_config_normalize:run())
        end),

        %% Every leaf write took effect.
        ?assertEqual(false, arweave_config:get([join, auto])),
        ?assertEqual(true, arweave_config:get([join, start_from_latest_state])),
        ?assertEqual(0, arweave_config:get([sync, max_download_rate])),
        ?assertEqual(0, arweave_config:get([gossip, block, pollers])),
        ?assertEqual(0, arweave_config:get([gossip, header, workers])),
        ?assertEqual(false, arweave_config:get([gossip, tx, polling_enabled])),
        ?assertEqual(0, arweave_config:get([packing, entropy, workers])),
        ?assertEqual(false, arweave_config:get([cm, enabled])),
        ?assertEqual(0, arweave_config:get([gossip, tx, max_peers])),
        ?assertEqual(0, arweave_config:get([gossip, block, max_peers])),
        ?assertEqual(false, arweave_config:get([vdf, compute])),
        ?assertEqual(false, arweave_config:get([vdf, is_public_server])),

        %% Every list replace took effect.
        ?assertEqual([], arweave_config_options_peers:by_role(cm_peer)),
            ?assertEqual(not_set, arweave_config:get([peers, cm_exit])),
        ?assertEqual([], arweave_config_options_peers:by_role(vdf_client)),
        ?assertEqual([], arweave_config_options_peers:by_role(vdf_server))
    end),
    ok.

%% Simple group_leader sink. Discards every io_request until it
%% receives `stop'.
capture_io(_Parent) ->
    receive
        {io_request, From, ReplyAs, _Req} ->
            From ! {io_reply, ReplyAs, ok},
            capture_io(_Parent);
        stop ->
            ok
    end.

%% Run `Fun' with stdout sunk into a discard process so deprecation
%% warnings from the classifier don't pollute CT logs.
discard_io(Fun) ->
    Self = self(),
    IOPid = spawn(fun() -> capture_io(Self) end),
    OldGL = group_leader(),
    group_leader(IOPid, self()),
    try
        Fun()
    after
        group_leader(OldGL, self()),
        IOPid ! stop
    end.
