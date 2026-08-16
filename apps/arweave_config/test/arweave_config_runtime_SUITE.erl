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
        load_mode_accepts_every_spec,
        runtime_flip_is_idempotent,
        runtime_writable_boolean,
        runtime_writable_pos_integer,
        runtime_writable_options_accept_set,
        non_runtime_scalar_rejected,
        non_runtime_address_rejected,
        non_runtime_list_replace_rejected
    ].

%% A value for each runtime-writable option group, exercising every
%% distinct `handle_set' path at least once. Identical per-field siblings
%% (the gun client opts, the cowboy protocol opts behind `set_protocol_opt',
%% the per-field network / limiter / transactions knobs) are represented by
%% a single entry rather than enumerated; logging options have their own
%% suite. A bad value type, a stale runtime flag, or a `handle_set' that
%% crashes the registry surfaces here: the consumer-side setters are
%% load-safe no-ops in this context (no node booted), so a successful `set'
%% then `get' returning the value confirms the spec is runtime-writable end
%% to end.
runtime_writable_cases() ->
    [
        %% Tier 1 — pure runtime flag.
        {[packing, repack, batch_size], 32},
        {[network, client, http, keepalive], 30000},
        {[network, server, shutdown, mode], close},
        {[gossip, tx, post_timeout], 25},
        {[gossip, data_roots, syncing_enabled], false},
        {[gossip, data_roots, max_duplicates], 5},
        {[sync, local_peers_only], true},
        {[disk_pool, max_buffer_size], 200},
        {[randomx, hardware_aes], false},
        {[vdf, pull], false},
        {[vdf, max_validation_threads], 4},
        {[vdf, algorithm], openssl},
        {[cm, poll_interval], 30000},
        {[pool, api_key], <<"a-pool-api-key">>},
        {[peers, block_gossip], [{1,2,3,4,1984}]},
        {[peers, local], [{1,2,3,4,1984}]},
        {[peers, cm_peer], [{1,2,3,4,1984}]},
        {[transactions, blocklist, files], [<<"/tmp/blocklist">>]},
        {[sync, request_packed_chunks], true},
        %% Tier 2 — runtime flag + handle_set / live-read conversion.
        {[gossip, tx, max_peers], 10},
        {[gossip, block, throttle_by_ip_interval], 500},
        {[gossip, header, cache_size], 100},
        {[sync, cache_size], 500},
        {[packing, cache_size], 1000},
        {[packing, entropy, cache_size], 2000},
        {[packing, entropy, workers], 4},
        {[mining, cache_size], 1000},
        {[disable_device_limit], true},
        {[disk_space_check_frequency], 5000},
        {[rocksdb, flush_interval], 600},
        {[rocksdb, wal_sync_interval], 30},
        {[cm, out_batch_timeout], 50},
        {[features, http_logging], true},
        %% New reconfigure APIs.
        {[limiter, chunk, concurrency_limit], 100},
        {[network, server, http, max_connections], 1000},
        {[network, server, http, request_timeout], 6000}
    ].

runtime_writable_options_accept_set(_Config) ->
    arweave_config:with_test_config(fun() ->
        ok = arweave_config:runtime(),
        true = arweave_config:is_runtime(),
        lists:foreach(
            fun({Key, Value}) ->
                ?assertEqual(ok, arweave_config:set(Key, Value),
                    lists:flatten(io_lib:format("set ~p", [Key]))),
                ?assertEqual(Value, arweave_config:get(Key),
                    lists:flatten(io_lib:format("get ~p", [Key])))
            end,
            runtime_writable_cases())
    end),
    ok.

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
