%%% @doc Specs for the `limiter` option group. Each list entry
%%% corresponds to one HTTP API rate-limiter group started by
%%% `arweave_limiter_sup`. A group combines a sliding-window limiter,
%%% a leaky-bucket limiter, and a concurrency cap.
%%%
%%% `default_groups/0` is the single source of truth: it lists every
%%% known group along with the per-field defaults. `specs/0` walks the
%%% map and emits one spec per `[limiter, GroupID, Field]` leaf, so
%%% adding a group is a one-line edit. `groups/0` reverses the
%%% process — it reconstructs the legacy `[#{id => …, ...}]` shape
%%% that `arweave_limiter_sup` consumes.
-module(arweave_config_options_limiter).
-behaviour(arweave_config_options).
-export([
    specs/0,
    group_description/0,
    validate/0,
    group_ids/0
]).

%% Per-group compile-time defaults. The `-ifdef(AR_TEST)` blocks raise
%% the leaky-bucket caps for groups whose production limit is too
%% tight for some test scenarios.
-define(LIMITER_TIMESTAMP_CLEANUP_INTERVAL, 120000).
-define(LIMITER_IS_EXTERNAL_REDUCTION_ENABLED, false).

%% Upper bound, in milliseconds, for every timer interval: one day.
-define(LIMITER_MAX_INTERVAL_MS, 86_400_000).

%% general
-ifdef(AR_TEST).
-define(LIMITER_GENERAL_SLIDING_WINDOW_LIMIT, 0).
-define(LIMITER_GENERAL_LEAKY_LIMIT, 45000).
-else.
-define(LIMITER_GENERAL_SLIDING_WINDOW_LIMIT, 3).
-define(LIMITER_GENERAL_LEAKY_LIMIT, 450).
-endif.
-define(LIMITER_GENERAL_SLIDING_WINDOW_DURATION, 2000).
-define(LIMITER_GENERAL_LEAKY_TICK_INTERVAL, 30000).
-define(LIMITER_GENERAL_LEAKY_TICK_REDUCTION, 450).
-define(LIMITER_GENERAL_CONCURRENCY_LIMIT, 150).

%% chunk
-define(LIMITER_CHUNK_SLIDING_WINDOW_LIMIT, 1000).
-define(LIMITER_CHUNK_SLIDING_WINDOW_DURATION, 1000).
-define(LIMITER_CHUNK_LEAKY_LIMIT, 6000).
-define(LIMITER_CHUNK_LEAKY_TICK_INTERVAL, 30000).
-define(LIMITER_CHUNK_LEAKY_TICK_REDUCTION, 6000).
-define(LIMITER_CHUNK_CONCURRENCY_LIMIT, 200).

%% data_sync_record
-define(LIMITER_DATA_SYNC_RECORD_SLIDING_WINDOW_LIMIT, 0).
-define(LIMITER_DATA_SYNC_RECORD_SLIDING_WINDOW_DURATION, 1000).
%% Test build runs unevenly high request rates against this group:
%% e2e suites poll `/data_sync_record' from both the test driver and
%% from the in-cluster sync workers. Upstream commit a7f43d57e also
%% made concurrency group-wide rather than per-peer, so what used to
%% be effectively 40-per-peer is now 40 total. Raise both caps under
%% AR_TEST so the bucket and concurrency budget don't bottleneck.
-ifdef(AR_TEST).
-define(LIMITER_DATA_SYNC_RECORD_LEAKY_LIMIT, 10000).
-define(LIMITER_DATA_SYNC_RECORD_CONCURRENCY_LIMIT, 200).
-define(LIMITER_DATA_SYNC_RECORD_LEAKY_TICK_REDUCTION, 10000).
-else.
-define(LIMITER_DATA_SYNC_RECORD_LEAKY_LIMIT, 20).
-define(LIMITER_DATA_SYNC_RECORD_CONCURRENCY_LIMIT, 40).
-define(LIMITER_DATA_SYNC_RECORD_LEAKY_TICK_REDUCTION, 20).
-endif.
-define(LIMITER_DATA_SYNC_RECORD_LEAKY_TICK_INTERVAL, 30000).

%% recent_hash_list_diff
-define(LIMITER_RECENT_HASH_LIST_DIFF_SLIDING_WINDOW_LIMIT, 0).
-define(LIMITER_RECENT_HASH_LIST_DIFF_SLIDING_WINDOW_DURATION, 1000).
-define(LIMITER_RECENT_HASH_LIST_DIFF_LEAKY_LIMIT, 120).
-define(LIMITER_RECENT_HASH_LIST_DIFF_LEAKY_TICK_INTERVAL, 30000).
-define(LIMITER_RECENT_HASH_LIST_DIFF_LEAKY_TICK_REDUCTION, 120).
-define(LIMITER_RECENT_HASH_LIST_DIFF_CONCURRENCY_LIMIT, 240).

%% block_index
-define(LIMITER_BLOCK_INDEX_SLIDING_WINDOW_LIMIT, 0).
-define(LIMITER_BLOCK_INDEX_SLIDING_WINDOW_DURATION, 1000).
-ifdef(AR_TEST).
-define(LIMITER_BLOCK_INDEX_LEAKY_LIMIT, 10).
-else.
-define(LIMITER_BLOCK_INDEX_LEAKY_LIMIT, 1).
-endif.
-define(LIMITER_BLOCK_INDEX_LEAKY_TICK_INTERVAL, 30000).
-define(LIMITER_BLOCK_INDEX_LEAKY_TICK_REDUCTION, 1).
-define(LIMITER_BLOCK_INDEX_CONCURRENCY_LIMIT, 2).

%% wallet_list
-define(LIMITER_WALLET_LIST_SLIDING_WINDOW_LIMIT, 0).
-define(LIMITER_WALLET_LIST_SLIDING_WINDOW_DURATION, 1000).
-ifdef(AR_TEST).
-define(LIMITER_WALLET_LIST_LEAKY_LIMIT, 10).
-else.
-define(LIMITER_WALLET_LIST_LEAKY_LIMIT, 1).
-endif.
-define(LIMITER_WALLET_LIST_LEAKY_TICK_INTERVAL, 30000).
-define(LIMITER_WALLET_LIST_LEAKY_TICK_REDUCTION, 1).
-define(LIMITER_WALLET_LIST_CONCURRENCY_LIMIT, 2).

%% get_vdf
-define(LIMITER_GET_VDF_SLIDING_WINDOW_LIMIT, 0).
-define(LIMITER_GET_VDF_SLIDING_WINDOW_DURATION, 1000).
-ifdef(AR_TEST).
-define(LIMITER_GET_VDF_LEAKY_LIMIT, 4500).
-else.
-define(LIMITER_GET_VDF_LEAKY_LIMIT, 180).
-endif.
-define(LIMITER_GET_VDF_LEAKY_TICK_INTERVAL, 30000).
-define(LIMITER_GET_VDF_LEAKY_TICK_REDUCTION, 180).
-define(LIMITER_GET_VDF_CONCURRENCY_LIMIT, 90).

%% get_vdf_session
-define(LIMITER_GET_VDF_SESSION_SLIDING_WINDOW_LIMIT, 0).
-define(LIMITER_GET_VDF_SESSION_SLIDING_WINDOW_DURATION, 1000).
-ifdef(AR_TEST).
-define(LIMITER_GET_VDF_SESSION_LEAKY_LIMIT, 50000).
-else.
-define(LIMITER_GET_VDF_SESSION_LEAKY_LIMIT, 30).
-endif.
-define(LIMITER_GET_VDF_SESSION_LEAKY_TICK_INTERVAL, 30000).
-define(LIMITER_GET_VDF_SESSION_LEAKY_TICK_REDUCTION, 30).
-define(LIMITER_GET_VDF_SESSION_CONCURRENCY_LIMIT, 30).

%% get_previous_vdf_session
-define(LIMITER_GET_PREVIOUS_VDF_SESSION_SLIDING_WINDOW_LIMIT, 0).
-define(LIMITER_GET_PREVIOUS_VDF_SESSION_SLIDING_WINDOW_DURATION, 1000).
-ifdef(AR_TEST).
-define(LIMITER_GET_PREVIOUS_VDF_SESSION_LEAKY_LIMIT, 50000).
-else.
-define(LIMITER_GET_PREVIOUS_VDF_SESSION_LEAKY_LIMIT, 30).
-endif.
-define(LIMITER_GET_PREVIOUS_VDF_SESSION_LEAKY_TICK_INTERVAL, 30000).
-define(LIMITER_GET_PREVIOUS_VDF_SESSION_LEAKY_TICK_REDUCTION, 30).
-define(LIMITER_GET_PREVIOUS_VDF_SESSION_CONCURRENCY_LIMIT, 30).

%% metrics
-define(LIMITER_METRICS_SLIDING_WINDOW_LIMIT, 0).
-define(LIMITER_METRICS_SLIDING_WINDOW_DURATION, 1000).
-define(LIMITER_METRICS_LEAKY_LIMIT, 2).
-define(LIMITER_METRICS_LEAKY_TICK_INTERVAL, 1000).
-define(LIMITER_METRICS_LEAKY_TICK_REDUCTION, 2).
-define(LIMITER_METRICS_CONCURRENCY_LIMIT, 2).

%% Number of worker gen_servers per group. Peers are sharded across
%% workers by `arweave_limiter_util:worker_ref/3'. local_peers and
%% metrics are low-traffic static endpoints and don't benefit from
%% sharding.
-define(LIMITER_DEFAULT_WORKERS, 5).
-define(LIMITER_LOCAL_PEERS_WORKERS, 1).
-define(LIMITER_METRICS_WORKERS, 1).

specs() ->
    [spec_for(GroupID, Field, Default) ||
        {GroupID, Fields} <- maps:to_list(default_groups()),
        {Field, Default} <- maps:to_list(Fields)].

%% Fields that are not runtime-reconfigurable yet: `number_of_workers'
%% sizes the worker pool at sup init, and the timer-driving fields need a
%% timer cancel/re-arm rather than a plain field write. Every other field
%% is read live from worker state, so it can be set at runtime.
non_runtime_fields() ->
    [number_of_workers, no_limit, leaky_tick_ms, timestamp_cleanup_tick_ms].

spec_for(GroupID, Field, Default) ->
    Runtime = not lists:member(Field, non_runtime_fields()),
    #{
        enabled => true,
        option_key => [limiter, GroupID, Field],
        type => type_for(Field),
        default => Default,
        runtime => Runtime,
        handle_set => fun(_K, V, _S, _A) ->
            set_field(GroupID, Field, V, Runtime)
        end,
        short_description => short_description_for(Field),
        long_description => group_coverage_for(GroupID)
    }.

%% @doc Validate an operator-supplied value for `Field' and, for a
%% runtime-writable field, push it to the group's workers before it
%% is stored. The cross-field invariants are checked here only once
%% the registry is in runtime mode; during load `validate/0' checks
%% them after every source has been applied.
set_field(GroupID, Field, V, Runtime) ->
    maybe
        ok ?= validate_field(GroupID, Field, V),
        ok ?= validate_group_at_runtime(GroupID, Field, V),
        ok = push_to_workers(GroupID, Field, V, Runtime),
        {store, V}
    end.

push_to_workers(GroupID, Field, V, true) ->
    arweave_limiter_group:set_config(GroupID, Field, V);
push_to_workers(_GroupID, _Field, _V, false) ->
    ok.

validate_group_at_runtime(GroupID, Field, V) ->
    case arweave_config:is_runtime() of
        true ->
            validate_group(GroupID, (group_fields(GroupID))#{Field => V});
        false ->
            ok
    end.

type_for(no_limit) -> boolean;
type_for(is_external_reduction_enabled) -> boolean;
type_for(_) -> non_neg_integer.

group_coverage_for(chunk) ->
    <<"Group covers: /chunk, /chunk2.">>;
group_coverage_for(data_sync_record) ->
    <<"Group covers: /data_sync_record.">>;
group_coverage_for(recent_hash_list_diff) ->
    <<"Group covers: /recent_hash_list_diff.">>;
group_coverage_for(block_index) ->
    <<"Group covers: /hash_list, /hash_list2, /block_index, "
      "/block_index2, /block/{type}/{id}/hash_list.">>;
group_coverage_for(wallet_list) ->
    <<"Group covers: /wallet_list, "
      "/block/{type}/{id}/wallet_list.">>;
group_coverage_for(get_vdf) ->
    <<"Group covers: /vdf, /vdf2.">>;
group_coverage_for(get_vdf_session) ->
    <<"Group covers: /vdf/session, /vdf2/session, "
      "/vdf3/session, /vdf4/session.">>;
group_coverage_for(get_previous_vdf_session) ->
    <<"Group covers: /vdf/previous_session, "
      "/vdf2/previous_session, /vdf4/previous_session.">>;
group_coverage_for(metrics) ->
    <<"Group covers: /metrics and its sub-paths.">>;
group_coverage_for(general) ->
    <<"Catch-all group for HTTP endpoints not routed to any other "
      "limiter group.">>;
group_coverage_for(local_peers) ->
    <<"Applies to every request from a peer listed under "
      "[peers, <peer>, local], regardless of path. With "
      "`no_limit` true (the default) these peers bypass rate "
      "limiting entirely.">>;
group_coverage_for(test_limiter) ->
    <<"Test-only group registered under -ifdef(AR_TEST). Provides "
      "a sandbox namespace so eunit tests can mutate limiter "
      "config without disturbing the production groups.">>;
group_coverage_for(test_limiter_2) ->
    <<"Second test-only group. Used by metrics-collector tests "
      "that exercise multi-group / multi-worker behaviour.">>.

short_description_for(sliding_window_limit) ->
    <<"Per-peer request budget within the sliding window; traffic "
      "beyond this falls through to the leaky bucket.">>;
short_description_for(sliding_window_duration) ->
    <<"Sliding window width in milliseconds.">>;
short_description_for(leaky_rate_limit) ->
    <<"Per-peer leaky-bucket capacity; once the sliding window is "
      "exhausted, requests beyond this are rejected.">>;
short_description_for(leaky_tick_ms) ->
    <<"Interval in milliseconds between leaky-bucket drains.">>;
short_description_for(tick_reduction) ->
    <<"Tokens drained from each peer's leaky bucket on every "
      "drain tick.">>;
short_description_for(concurrency_limit) ->
    <<"Maximum simultaneous in-flight requests per peer.">>;
short_description_for(timestamp_cleanup_tick_ms) ->
    <<"Interval in milliseconds between sweeps that drop idle "
      "peers from the sliding-window map.">>;
short_description_for(is_external_reduction_enabled) ->
    <<"Skip the extra leaky-bucket reduction performed after "
      "each accepted request.">>;
short_description_for(no_limit) ->
    <<"Bypass all rate limiting.">>;
short_description_for(number_of_workers) ->
    <<"Number of worker gen_servers per group; peers are sharded "
      "across them by the last octet of their IP.">>.

group_description() ->
    <<"HTTP API rate-limiter groups — sliding window + leaky bucket "
      "+ concurrency caps.">>.

%% @doc Cross-cutting: every group's timer and limit fields must hold
%% integers within range once the group actually limits. Bypass
%% groups (`no_limit => true') keep their `infinity' sentinels: they
%% never arm a timer, so nothing reads those values.
validate() ->
    validate_groups(lists:sort(group_ids())).

validate_groups([]) ->
    ok;
validate_groups([GroupID | Rest]) ->
    case validate_group(GroupID, group_fields(GroupID)) of
        ok -> validate_groups(Rest);
        {error, _} = Err -> Err
    end.

%% Fields whose values are checked by `validate_group/2'. `no_limit'
%% is read alongside them to detect bypass groups.
validated_fields() ->
    [
        sliding_window_limit,
        sliding_window_duration,
        leaky_rate_limit,
        leaky_tick_ms,
        tick_reduction,
        concurrency_limit,
        timestamp_cleanup_tick_ms
    ].

group_fields(GroupID) ->
    Fields = [no_limit | validated_fields()],
    maps:from_list([
        {Field, arweave_config:get([limiter, GroupID, Field])}
        || Field <- Fields
    ]).

validate_group(_GroupID, #{no_limit := true}) ->
    ok;
validate_group(GroupID, #{
        sliding_window_limit := SlidingLimit,
        leaky_rate_limit := LeakyLimit} = Fields) ->
    maybe
        ok ?= validate_combined_limits(GroupID, SlidingLimit, LeakyLimit),
        ok ?= validate_fields(GroupID, validated_fields(), Fields)
    end.

%% We can't have 0 limit in total, so let's validate that.
%% However, at startup we can have 'infinity'.
validate_combined_limits(_GroupID, infinity, infinity) ->
    ok;
validate_combined_limits(_GroupID, SlidingLimit, LeakyLimit)
    when (SlidingLimit + LeakyLimit) > 0 ->
    ok;
validate_combined_limits(GroupID, _, _) ->
    %% Both SlidingLimit, and LeakyLimit is positive integers,
    %% so if one were negative, it would have failed before.
    {error, iolist_to_binary(
        io_lib:format("Both (sliding window and leaky bucket)"
                      "rate limits can't be zero. (~B)", [GroupID]))}.


validate_fields(_GroupID, [], _Fields) ->
    ok;
validate_fields(GroupID, [Field | Rest], Fields) ->
    case validate_field(GroupID, Field, maps:get(Field, Fields)) of
        ok -> validate_fields(GroupID, Rest, Fields);
        {error, _} = Err -> Err
    end.

%% @doc Range check for one field. `infinity' is never accepted from
%% an operator: it is only meaningful as the code default of a bypass
%% group, and those never reach here (their defaults are not `set').
validate_field(GroupID, Field, infinity) ->
    field_error(GroupID, Field, "must be an integer, not infinity");
validate_field(GroupID, Field, V) ->
    case field_bounds(Field) of
        none ->
            ok;
        {min, Min} when V >= Min ->
            ok;
        {min, Min} ->
            field_error(GroupID, Field,
                io_lib:format("must be minimum ~B", [Min]));
        {Min, Max} when V >= Min, V =< Max ->
            ok;
        {Min, Max} ->
            field_error(GroupID, Field,
                io_lib:format("must be between ~B and ~B", [Min, Max]))
    end.

%% Inclusive bounds for the timer-driven fields. Every one of them is
%% handed to `timer:send_interval/3' or compared against the
%% monotonic clock, so 0 would spin and anything past a day is a
%% misconfiguration. The plain limits keep the `non_neg_integer' type's
%% non-negative check.
field_bounds(concurrency_limit) -> {min, 1};
field_bounds(sliding_window_duration) -> {1, ?LIMITER_MAX_INTERVAL_MS};
field_bounds(leaky_tick_ms) -> {1, ?LIMITER_MAX_INTERVAL_MS};
field_bounds(tick_reduction) -> {min, 1};
field_bounds(timestamp_cleanup_tick_ms) -> {1, ?LIMITER_MAX_INTERVAL_MS};
field_bounds(_Field) -> none.

field_error(GroupID, Field, Message) ->
    {error, iolist_to_binary(
        io_lib:format("limiter.~s.~s ~s", [GroupID, Field, Message]))}.

%% @doc Return the list of registered limiter group IDs. Per-field
%% values for a given group are read via
%% `arweave_config:get([limiter, GroupID, Field])'.
-spec group_ids() -> [atom()].
group_ids() ->
    maps:keys(default_groups()).

%% @doc Per-group default config. Fields shared by every group come
%% from `common/0`; per-group overrides live in each entry. Adding a
%% new group means one new key here; adding a new field means one new
%% key in `common/0` (or a per-group override).
default_groups() ->
    maps:merge(production_groups(), test_only_groups()).

-ifdef(AR_TEST).
test_only_groups() ->
    %% Dedicated sandboxes so eunit tests can mutate limiter config
    %% without affecting production groups. Defaults mirror `general'
    %% with `number_of_workers => 1' (`test_limiter') or the default count
    %% (`test_limiter_2', used by `arweave_limiter_metrics_collector_tests'
    %% to exercise sharded behaviour).
    #{
        test_limiter => (standard(
            ?LIMITER_GENERAL_SLIDING_WINDOW_LIMIT,
            ?LIMITER_GENERAL_SLIDING_WINDOW_DURATION,
            ?LIMITER_GENERAL_LEAKY_LIMIT,
            ?LIMITER_GENERAL_LEAKY_TICK_INTERVAL,
            ?LIMITER_GENERAL_LEAKY_TICK_REDUCTION,
            ?LIMITER_GENERAL_CONCURRENCY_LIMIT))#{
            number_of_workers => 1,
            is_external_reduction_enabled => true
        },
        test_limiter_2 => standard(
            ?LIMITER_GENERAL_SLIDING_WINDOW_LIMIT,
            ?LIMITER_GENERAL_SLIDING_WINDOW_DURATION,
            ?LIMITER_GENERAL_LEAKY_LIMIT,
            ?LIMITER_GENERAL_LEAKY_TICK_INTERVAL,
            ?LIMITER_GENERAL_LEAKY_TICK_REDUCTION,
            ?LIMITER_GENERAL_CONCURRENCY_LIMIT)
    }.
-else.
test_only_groups() -> #{}.
-endif.

production_groups() ->
    #{
        chunk => standard(
            ?LIMITER_CHUNK_SLIDING_WINDOW_LIMIT,
            ?LIMITER_CHUNK_SLIDING_WINDOW_DURATION,
            ?LIMITER_CHUNK_LEAKY_LIMIT,
            ?LIMITER_CHUNK_LEAKY_TICK_INTERVAL,
            ?LIMITER_CHUNK_LEAKY_TICK_REDUCTION,
            ?LIMITER_CHUNK_CONCURRENCY_LIMIT),
        data_sync_record => standard(
            ?LIMITER_DATA_SYNC_RECORD_SLIDING_WINDOW_LIMIT,
            ?LIMITER_DATA_SYNC_RECORD_SLIDING_WINDOW_DURATION,
            ?LIMITER_DATA_SYNC_RECORD_LEAKY_LIMIT,
            ?LIMITER_DATA_SYNC_RECORD_LEAKY_TICK_INTERVAL,
            ?LIMITER_DATA_SYNC_RECORD_LEAKY_TICK_REDUCTION,
            ?LIMITER_DATA_SYNC_RECORD_CONCURRENCY_LIMIT),
        recent_hash_list_diff => standard(
            ?LIMITER_RECENT_HASH_LIST_DIFF_SLIDING_WINDOW_LIMIT,
            ?LIMITER_RECENT_HASH_LIST_DIFF_SLIDING_WINDOW_DURATION,
            ?LIMITER_RECENT_HASH_LIST_DIFF_LEAKY_LIMIT,
            ?LIMITER_RECENT_HASH_LIST_DIFF_LEAKY_TICK_INTERVAL,
            ?LIMITER_RECENT_HASH_LIST_DIFF_LEAKY_TICK_REDUCTION,
            ?LIMITER_RECENT_HASH_LIST_DIFF_CONCURRENCY_LIMIT),
        block_index => standard(
            ?LIMITER_BLOCK_INDEX_SLIDING_WINDOW_LIMIT,
            ?LIMITER_BLOCK_INDEX_SLIDING_WINDOW_DURATION,
            ?LIMITER_BLOCK_INDEX_LEAKY_LIMIT,
            ?LIMITER_BLOCK_INDEX_LEAKY_TICK_INTERVAL,
            ?LIMITER_BLOCK_INDEX_LEAKY_TICK_REDUCTION,
            ?LIMITER_BLOCK_INDEX_CONCURRENCY_LIMIT),
        wallet_list => standard(
            ?LIMITER_WALLET_LIST_SLIDING_WINDOW_LIMIT,
            ?LIMITER_WALLET_LIST_SLIDING_WINDOW_DURATION,
            ?LIMITER_WALLET_LIST_LEAKY_LIMIT,
            ?LIMITER_WALLET_LIST_LEAKY_TICK_INTERVAL,
            ?LIMITER_WALLET_LIST_LEAKY_TICK_REDUCTION,
            ?LIMITER_WALLET_LIST_CONCURRENCY_LIMIT),
        get_vdf => standard(
            ?LIMITER_GET_VDF_SLIDING_WINDOW_LIMIT,
            ?LIMITER_GET_VDF_SLIDING_WINDOW_DURATION,
            ?LIMITER_GET_VDF_LEAKY_LIMIT,
            ?LIMITER_GET_VDF_LEAKY_TICK_INTERVAL,
            ?LIMITER_GET_VDF_LEAKY_TICK_REDUCTION,
            ?LIMITER_GET_VDF_CONCURRENCY_LIMIT),
        get_vdf_session => standard(
            ?LIMITER_GET_VDF_SESSION_SLIDING_WINDOW_LIMIT,
            ?LIMITER_GET_VDF_SESSION_SLIDING_WINDOW_DURATION,
            ?LIMITER_GET_VDF_SESSION_LEAKY_LIMIT,
            ?LIMITER_GET_VDF_SESSION_LEAKY_TICK_INTERVAL,
            ?LIMITER_GET_VDF_SESSION_LEAKY_TICK_REDUCTION,
            ?LIMITER_GET_VDF_SESSION_CONCURRENCY_LIMIT),
        get_previous_vdf_session => standard(
            ?LIMITER_GET_PREVIOUS_VDF_SESSION_SLIDING_WINDOW_LIMIT,
            ?LIMITER_GET_PREVIOUS_VDF_SESSION_SLIDING_WINDOW_DURATION,
            ?LIMITER_GET_PREVIOUS_VDF_SESSION_LEAKY_LIMIT,
            ?LIMITER_GET_PREVIOUS_VDF_SESSION_LEAKY_TICK_INTERVAL,
            ?LIMITER_GET_PREVIOUS_VDF_SESSION_LEAKY_TICK_REDUCTION,
            ?LIMITER_GET_PREVIOUS_VDF_SESSION_CONCURRENCY_LIMIT),
        general => (standard(
            ?LIMITER_GENERAL_SLIDING_WINDOW_LIMIT,
            ?LIMITER_GENERAL_SLIDING_WINDOW_DURATION,
            ?LIMITER_GENERAL_LEAKY_LIMIT,
            ?LIMITER_GENERAL_LEAKY_TICK_INTERVAL,
            ?LIMITER_GENERAL_LEAKY_TICK_REDUCTION,
            ?LIMITER_GENERAL_CONCURRENCY_LIMIT))#{
            is_external_reduction_enabled => true
        },
        %% Metrics is a static low-traffic endpoint; one worker is enough.
        metrics => (standard(
            ?LIMITER_METRICS_SLIDING_WINDOW_LIMIT,
            ?LIMITER_METRICS_SLIDING_WINDOW_DURATION,
            ?LIMITER_METRICS_LEAKY_LIMIT,
            ?LIMITER_METRICS_LEAKY_TICK_INTERVAL,
            ?LIMITER_METRICS_LEAKY_TICK_REDUCTION,
            ?LIMITER_METRICS_CONCURRENCY_LIMIT))#{
            number_of_workers => ?LIMITER_METRICS_WORKERS
        },
        %% Bypass group: every limit field is moot because no_limit is
        %% true. One worker is plenty for the static low-traffic load.
        local_peers => no_limit(?LIMITER_LOCAL_PEERS_WORKERS)
    }.

standard(SlidingLimit, SlidingDuration, LeakyLimit, LeakyTickMs,
        TickReduction, ConcurrencyLimit) ->
    (common())#{
        sliding_window_limit => SlidingLimit,
        sliding_window_duration => SlidingDuration,
        leaky_rate_limit => LeakyLimit,
        leaky_tick_ms => LeakyTickMs,
        tick_reduction => TickReduction,
        concurrency_limit => ConcurrencyLimit
    }.

%% Default set for groups that bypass rate limiting (`no_limit => true').
%% Every per-limit field is set to `infinity' to signal "ignored"; only
%% `number_of_workers' and `no_limit' carry meaningful values.
%% `arweave_limiter_group:init/1' skips timer creation when `no_limit'
%% is true, so the `infinity' values are never passed to
%% `timer:send_interval/3'.
no_limit(NumberOfWorkers) ->
    (common())#{
        no_limit => true,
        number_of_workers => NumberOfWorkers,
        sliding_window_limit => infinity,
        sliding_window_duration => infinity,
        leaky_rate_limit => infinity,
        leaky_tick_ms => infinity,
        tick_reduction => infinity,
        concurrency_limit => infinity,
        timestamp_cleanup_tick_ms => infinity
    }.

common() ->
    #{
        timestamp_cleanup_tick_ms =>
            ?LIMITER_TIMESTAMP_CLEANUP_INTERVAL,
        is_external_reduction_enabled =>
            ?LIMITER_IS_EXTERNAL_REDUCTION_ENABLED,
        no_limit => false,
        number_of_workers => ?LIMITER_DEFAULT_WORKERS
    }.
