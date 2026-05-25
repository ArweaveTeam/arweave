%%%
%%% @doc Leaky bucket token rate limiter based on
%%%      https://gist.github.com/humaite/21a84c3b3afac07fcebe476580f3a40b
%%%      combined with a concurrency limiter similar to Ranch's connection pool.
%%%      The leaky bucket limiter sits on top of a sliding window limiter.
%%%
%%%      Concurrency is validated first, then sliding window, followed by leaky
%%%      bucket. If sliding windows passes, the call is accepted, otherwise it
%%%      burns leaky tokens, if those are exhausted as well, the call will be
%%%      marked as rejected.
%%%      It only stores data in process memory.
%%%
-module(arweave_limiter_group).

-behaviour(gen_server).

%% API
-export([
         start_link/2,
         info/1,
         register_or_reject_call/2,
         reduce_for_peer/2,
         reset_all/1,
         set_config/3,
         stop/1
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).

-ifdef(AR_TEST).
-export([
         expire_and_get_requests/4,
         drop_expired/3,
         add_and_order_timestamps/2,
         cleanup_expired_sliding_peers/3,
         generate_policy/1,
         build_headers_info_sliding/4]).
-endif.

-include_lib("arweave/include/ar.hrl").

-define(UNEXPECTED_ERROR_STR, "unexpected").

%% Wall-clock limit on a `register_or_reject_call/2' gen_server hop.
%% Calls that exceed it are treated as `{reject, error, _}'.
-define(CALL_TIMEOUT, 1000).

%%% API
%% `LimiterRef' is the registered worker name (one of several per
%% group when sharded); `GroupID' is the limiter group atom (e.g.
%% `chunk', `general'). Both are needed because the worker reads its
%% config via `arweave_config:get([limiter, GroupID, _])'.
start_link(LimiterRef, GroupID) when is_atom(GroupID) ->
    gen_server:start_link({local, LimiterRef}, ?MODULE, [GroupID], []).

info(LimiterRef) ->
    WorkersNum = arweave_config:get([limiter, LimiterRef, number_of_workers]),
    lists:foldl(fun(N, Acc) -> merge_info_maps(LimiterRef, N, Acc) end,
                #{sliding_timestamps => #{},
                  leaky_tokens => #{},
                  concurrent_monitors => #{}},
                lists:seq(0, WorkersNum - 1)).

register_or_reject_call(LimiterRef, Peer) ->
    {Time, Value} = timer:tc(fun do_register_or_reject_call/2, [LimiterRef, Peer]),
    prometheus_histogram:observe(ar_limiter_response_time_microseconds, [atom_to_list(LimiterRef)], Time),
    Value.

do_register_or_reject_call(LimiterRef, Peer) ->
    prometheus_counter:inc(ar_limiter_requests_total, [atom_to_list(LimiterRef)]),
    LimiterWorkerRef = ref_to_worker_ref(LimiterRef, Peer),
    try gen_server:call(LimiterWorkerRef,
                        {register_or_reject, Peer},
                        ?CALL_TIMEOUT) of
        {reject, Reason, _HeadersInfo} = Rejection ->
            prometheus_counter:inc(ar_limiter_rejected_total,
                                   [atom_to_list(LimiterRef), atom_to_list(Reason)]),
            Rejection;
        Accept ->
            Accept
    catch E:R ->
            ReasonStr = reason_to_list(R),
            prometheus_counter:inc(ar_limiter_requests_error, [atom_to_list(LimiterRef), ReasonStr]),
            %% Only log unexpected errors, others can be read from metrics.
            if ReasonStr == ?UNEXPECTED_ERROR_STR ->
                    ?LOG_ERROR([{event, rate_limiter_group_error},
                                  {limiter_ref, LimiterRef},
                                  {peer, Peer},
                                  {class, E},
                                  {reason, R}]);
               true ->
                    ok
            end,
            {reject, error, #{}}
    end.

%% This function is called when a transaction is accepted. This is how the previous
%% solution dealt with high loads. This will perform double reduction. (as the periodic
%% reduction is still occurring).
reduce_for_peer(LimiterRef, Peer) ->
    LimiterWorkerRef = ref_to_worker_ref(LimiterRef, Peer),
    Result = gen_server:call(LimiterWorkerRef, {reduce_for_peer, Peer}),
    Result == ok andalso prometheus_counter:inc(ar_limiter_reduce_requests_total,
                                                [atom_to_list(LimiterRef)]),
    Result.

reset_all(LimiterRef) ->
    whereis(LimiterRef) == undefined orelse gen_server:call(LimiterRef, reset_all).

%% @doc Push a live config update for Field into every worker of GroupID's
%% state. Only the timer-free fields are marked `runtime' in the spec, so the
%% timer-interval / no_limit fields (which would need a timer re-arm) never
%% reach here. A no-op for workers not yet started (boot/load phase).
set_config(GroupID, Field, V) when is_atom(GroupID) ->
    WorkersNum = arweave_config:get([limiter, GroupID, number_of_workers]),
    lists:foreach(
        fun(N) ->
            WorkerName = arweave_limiter_util:worker_name(GroupID, N),
            gen_server:cast(WorkerName, {set_config, Field, V})
        end,
        lists:seq(0, WorkersNum - 1)),
    ok.

stop(LimiterRef) ->
    gen_server:stop(LimiterRef).

%% gen_server callbacks
%%
%% Every field is read from `arweave_config:get/1' against the canonical
%% `[limiter, GroupID, Field]' key. Defaults live exclusively in
%% the arweave_config app; tests that need non-default values
%% call `arweave_config:set/2' on the same keys before `start_link/2'.
init([GroupID]) when is_atom(GroupID) ->
    process_flag(priority, high),

    ID = atom_to_list(GroupID),

    IsDisabled = arweave_config:get([limiter, GroupID, no_limit]),
    IsManualReductionDisabled = arweave_config:get([limiter, GroupID, is_manual_reduction_disabled]),
    LeakyTickMs = arweave_config:get([limiter, GroupID, leaky_tick_ms]),
    TimestampCleanupTickMs = arweave_config:get([limiter, GroupID, timestamp_cleanup_tick_ms]),
    TimestampCleanupExpiry = arweave_config:get([limiter, GroupID, timestamp_cleanup_expiry]),
    LeakyRateLimit = arweave_config:get([limiter, GroupID, leaky_rate_limit]),
    ConcurrencyLimit = arweave_config:get([limiter, GroupID, concurrency_limit]),
    TickReduction = arweave_config:get([limiter, GroupID, tick_reduction]),
    SlidingWindowDuration = arweave_config:get([limiter, GroupID, sliding_window_duration]),
    SlidingWindowLimit = arweave_config:get([limiter, GroupID, sliding_window_limit]),

    Now = arweave_limiter_time:ts_now(),
    %% Bypass groups (`no_limit => true') carry `infinity' for every
    %% timer interval and never need ticks; skip timer creation so
    %% `timer:send_interval/3' isn't handed a non-integer.
    {LeakyRef, TSRef, NextLBTickTS} = start_tick_timers(
        IsDisabled, LeakyTickMs, TimestampCleanupTickMs, Now),
    {ok, #{
           id => ID,
           is_disabled => IsDisabled,
           is_manual_reduction_disabled => IsManualReductionDisabled,
           leaky_tick_timer_ref => LeakyRef,
           timestamp_cleanup_timer_ref => TSRef,
           leaky_tick_ms => LeakyTickMs,
           next_leaky_tick_ts => NextLBTickTS,
           timestamp_cleanup_tick_ms => TimestampCleanupTickMs,
           timestamp_cleanup_expiry => TimestampCleanupExpiry,
           tick_reduction => TickReduction,
           leaky_rate_limit => LeakyRateLimit,
           concurrency_limit => ConcurrencyLimit,
           concurrent_monitors => #{}, %% MonitorRef -> Peer
           leaky_tokens => #{}, %% Peer -> Leaky Bucket tokens
           sliding_window_duration => SlidingWindowDuration,
           sliding_window_limit => SlidingWindowLimit,
           sliding_timestamps => #{} %% Peer -> Ordered list of timestamps
          }}.

start_tick_timers(true, _LeakyTickMs, _TimestampCleanupTickMs, _Now) ->
    {undefined, undefined, infinity};
start_tick_timers(false, LeakyTickMs, TimestampCleanupTickMs, Now) ->
    {ok, LeakyRef} = timer:send_interval(LeakyTickMs, self(),
                                         {tick, leaky_bucket_reduction}),
    {ok, TSRef} = timer:send_interval(TimestampCleanupTickMs, self(),
                                      {tick, sliding_window_timestamp_cleanup}),
    {LeakyRef, TSRef, Now + LeakyTickMs}.

handle_call(reset_all, _From, State) ->
    {reply, ok, State#{concurrent_monitors => #{},
                       leaky_tokens => #{},
                       sliding_timestamps => #{}}};
handle_call({register_or_reject, _Peer}, {_FromPid, _},
            State = #{id := _ID, is_disabled := true}) ->
    LimiterHeaders = #{policies => generate_policy(State)},
    {reply, {register, no_limiting_applied, LimiterHeaders}, State};
handle_call({register_or_reject, _Peer}, {_FromPid, _},
            State = #{concurrency_limit := ConcurrencyLimit,
                      concurrent_monitors := ConcurrentMonitors})
  when map_size(ConcurrentMonitors) >= ConcurrencyLimit ->
    %% Concurrency Hard Limit — group-wide, not per-peer.
    Policies = generate_policy(State),
    HeadersInfo = #{expiring_limit => ConcurrencyLimit,
                    remaining      => 0,
                    reset_seconds  => 1,
                    policies       => Policies},
    {reply, {reject, concurrency, HeadersInfo}, State};
handle_call({register_or_reject, Peer}, {FromPid, _},
            State = #{id := ID,
                      is_disabled := false,
                      leaky_rate_limit := LeakyRateLimit,
                      leaky_tokens := LeakyTokens,
                      next_leaky_tick_ts := NextLeakyTickTS,
                      concurrent_monitors := ConcurrentMonitors,
                      sliding_window_duration := SlidingWindowDuration,
                      sliding_window_limit := SlidingWindowLimit,
                      sliding_timestamps := SlidingTimestamps
                     }) ->
    Now = arweave_limiter_time:ts_now(),
    Tokens = maps:get(Peer, LeakyTokens, 0) + 1,

    SlidingTimestampsForPeer0 =
        expire_and_get_requests(Peer, SlidingTimestamps, SlidingWindowDuration, Now),

    Policies = generate_policy(State),

    case length(SlidingTimestampsForPeer0) + 1 > SlidingWindowLimit of
        true ->
            %% Sliding Window limited, check Leaky Bucket Tokens
            case Tokens > LeakyRateLimit of
                true ->
                    %% Burst exhausted with the Leaky Tokens
                    ?LOG_DEBUG([{event, ar_limiter_reject}, {reason, rate_limit},
                                {sliding_window_limit, SlidingWindowLimit},
                                {leaky_rate_limit, LeakyRateLimit},
                                {peer, Peer}, {id, ID}]),
                    HeadersInfo = build_headers_info_leaky(
                                    0, LeakyRateLimit, NextLeakyTickTS,
                                    SlidingTimestampsForPeer0, Now,
                                    Policies),
                    {reply, {reject, rate_limit, HeadersInfo}, State};
                false ->
                    NewLeakyTokens = update_token(Peer, Tokens, LeakyTokens),
                    NewMonitors = register_concurrent(FromPid, ConcurrentMonitors),
                    LbRemaining = LeakyRateLimit - Tokens,
                    HeadersInfo = build_headers_info_leaky(
                                    LbRemaining, LeakyRateLimit, NextLeakyTickTS,
                                    SlidingTimestampsForPeer0, Now,
                                    Policies),
                    {reply, {register, leaky, HeadersInfo},
                     State#{leaky_tokens => NewLeakyTokens,
                            concurrent_monitors => NewMonitors}}
            end;
        _ ->
            NewMonitors = register_concurrent(FromPid, ConcurrentMonitors),
            SlidingTimestampsForPeer1 = add_and_order_timestamps(Now, SlidingTimestampsForPeer0),
            NewSlidingTimestamps = SlidingTimestamps#{Peer => SlidingTimestampsForPeer1},
            SWRemaining = max(0, SlidingWindowLimit
                              - length(SlidingTimestampsForPeer1)),
            HeadersInfo = build_headers_info_sliding(SWRemaining, SlidingTimestampsForPeer1,
                                                     Now, Policies),
            {reply, {register, sliding, HeadersInfo},
             State#{sliding_timestamps => NewSlidingTimestamps,
                    concurrent_monitors => NewMonitors}}
    end;
handle_call({reduce_for_peer, Peer}, _From, State =
                #{is_manual_reduction_disabled := false,
                  leaky_tokens := LeakyTokens}) ->
    NewLeakyTokens = do_reduce_for_peer(Peer, LeakyTokens),
    {reply, ok, State#{leaky_tokens => NewLeakyTokens}};
handle_call({reduce_for_peer, _Peer}, _From, State =
                #{is_manual_reduction_disabled := true}) ->
    {reply, disabled, State};
handle_call(get_info, _From, State =
                #{sliding_timestamps := SlidingTimestamps,
                  leaky_tokens := LeakyTokens,
                  concurrent_monitors := ConcurrentMonitors}) ->
    {reply, #{sliding_timestamps => SlidingTimestamps,
              leaky_tokens => LeakyTokens,
              concurrent_monitors => ConcurrentMonitors}, State};
handle_call(Request, From, State = #{id := ID}) ->
    ?LOG_WARNING([{event, unhandled_call}, {id, ID}, {module, ?MODULE},
                  {request, Request}, {from, From},
                  {config, filter_state_for_config(State)}]),
    {reply, ok, State}.

handle_cast({set_config, Field, V}, State) ->
    {noreply, State#{Field => V}};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({tick, sliding_window_timestamp_cleanup},
            State = #{id := ID, sliding_timestamps := SlidingTimestamps,
                      timestamp_cleanup_tick_ms := CleanupTickMs,
                      timestamp_cleanup_expiry := CleanupExpiry}) ->
    Now = arweave_limiter_time:ts_now(),
    NextSWTickTS = Now + CleanupTickMs,
    NewSlidingTimestamps = cleanup_expired_sliding_peers(SlidingTimestamps, CleanupExpiry, Now),
    Deleted = maps:size(SlidingTimestamps) - maps:size(NewSlidingTimestamps),
    prometheus_counter:inc(ar_limiter_cleanup_tick_expired_sliding_peers_deleted_total, [ID], Deleted),
    {noreply, State#{sliding_timestamps => NewSlidingTimestamps, next_timestamp_clean_ts => NextSWTickTS}};
handle_info({tick, leaky_bucket_reduction},
            State = #{id := ID,
                      tick_reduction := TickReduction,
                      leaky_tick_ms := LeakyTickMs,
                      leaky_tokens := LeakyTokens}) ->
    Now = arweave_limiter_time:ts_now(),
    %% NextLBTickTS is an approximate value for rate-limiting headers to use.
    %%
    NextLBTickTS = Now + LeakyTickMs,
    prometheus_counter:inc(ar_limiter_leaky_ticks, [ID]),
    SizeBefore = maps:size(LeakyTokens),
    %% This is going to be more precise than ar_limiter_leaky_ticks*ar_limiter_peers
    prometheus_counter:inc(ar_limiter_leaky_tick_reductions_peer, [ID], SizeBefore),
    NewTokens =
        maps:fold(fun(Key, Value, AccIn) ->
                          fold_decrease_rate(ID, Key, Value, AccIn, TickReduction)
                  end, #{}, LeakyTokens),
    prometheus_counter:inc(
      ar_limiter_leaky_tick_delete_peer_total, [ID], SizeBefore - maps:size(NewTokens)),
    {noreply, State#{leaky_tokens => NewTokens, next_leaky_tick_ts => NextLBTickTS}};
handle_info({'DOWN', MonitorRef, process, Pid, Reason},
            State = #{concurrent_monitors := ConcurrentMonitors}) ->
    NewConcurrentMonitors =
        remove_concurrent(MonitorRef, Pid, Reason, ConcurrentMonitors),
    {noreply, State#{concurrent_monitors => NewConcurrentMonitors}};
handle_info(Info, State = #{id := ID}) ->
    ?LOG_WARNING([{event, unhandled_info}, {id, ID}, {module, ?MODULE}, {info, Info}]),
    {noreply, State}.

terminate(_Reason, #{id := _ID,
                     leaky_tick_timer_ref := _LeakyRef,
                     timestamp_cleanup_timer_ref := _TSRef} = _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, Status) ->
    Status.

%%% Internal functions

%% Sliding window manipulation
expire_and_get_requests(Peer, SlidingTimestamps, SlidingWindowDuration, Now) ->
    Timestamps = maps:get(Peer, SlidingTimestamps, []),
    drop_expired(Timestamps, SlidingWindowDuration, Now).

drop_expired([TS|Timestamps], WindowDuration, Now) when TS + WindowDuration =< Now ->
    drop_expired(Timestamps, WindowDuration, Now);
drop_expired(Timestamps, _WindowDuration, _Now) ->
    Timestamps.

%% There is no idomatic way of adding an element to the end of a list in Erlang.
%% So, we reverse the list add it to the beginning and reverse it again.
add_and_order_timestamps(TS, Timestamps) ->
    lists:reverse(do_add_and_order_timestamps(TS, lists:reverse(Timestamps))).

do_add_and_order_timestamps(TS, []) ->
    [TS];
do_add_and_order_timestamps(TS, [Head | _Rest] = Timestamps) when TS >= Head ->
    [TS | Timestamps];
do_add_and_order_timestamps(TS, [Head | Rest])  ->
    %% This clause shouldn't really reached, because we use monotonic time
    %% for timestamps.
    [Head | do_add_and_order_timestamps(TS, Rest)].

cleanup_expired_sliding_peers(SlidingTimestamps, WindowDuration, Now) ->
    maps:fold(fun(Peer, TSList, AccIn) ->
                      case drop_expired(TSList, WindowDuration, Now) of
                          [] ->
                              AccIn;
                          ValidTimestamps ->
                              AccIn#{Peer => ValidTimestamps}
                      end
              end, #{}, SlidingTimestamps).

%% Token manipulation
update_token(Peer, Token, LeakyToken) ->
    maps:put(Peer, Token, LeakyToken).

do_reduce_for_peer(Peer, LeakyTokens) ->
    case maps:get(Peer, LeakyTokens, 0) of
        0 ->
            LeakyTokens;
        Tokens ->
            LeakyTokens#{Peer => Tokens - 1}
    end.

fold_decrease_rate(_ID, _Key, Counter, Acc, _TickReduction)
  when is_integer(Counter), Counter =< 0 ->
    Acc;
fold_decrease_rate(ID, Key, Counter, Acc, TickReduction) when Counter < TickReduction ->
    prometheus_counter:inc(ar_limiter_leaky_tick_token_reductions_total, [ID], Counter),
    maps:put(Key, 0, Acc);
fold_decrease_rate(ID, Key, Counter, Acc, TickReduction) ->
    prometheus_counter:inc(ar_limiter_leaky_tick_token_reductions_total, [ID], TickReduction),
    maps:put(Key, Counter-TickReduction, Acc).

%% Concurrency tracking — group-wide (one slot per in-flight call,
%% regardless of peer). The map's key is the monitor ref, value is
%% unused; we keep it as a map so concurrency size is `map_size/1'.
register_concurrent(Pid, ConcurrentMonitors) ->
    MonitorRef = erlang:monitor(process, Pid),
    maps:put(MonitorRef, true, ConcurrentMonitors).

remove_concurrent(MonitorRef, _Pid, _Reason, ConcurrentMonitors) ->
    maps:remove(MonitorRef, ConcurrentMonitors).

filter_state_for_config(#{id := ID,
                          is_disabled := IsDisabled,
                          is_manual_reduction_disabled := IsManualReductionDisabled,
                          leaky_tick_ms := LeakyTickMs,
                          timestamp_cleanup_tick_ms := TimestampCleanupTickMs,
                          timestamp_cleanup_expiry := TimestampCleanupExpiry,
                          tick_reduction := TickReduction,
                          leaky_rate_limit := LeakyRateLimit,
                          concurrency_limit := ConcurrencyLimit,
                          sliding_window_duration := SlidingWindowDuration,
                          sliding_window_limit := SlidingWindowLimit}) ->
    #{id => ID,
      is_disabled => IsDisabled,
      is_manual_reduction_disabled => IsManualReductionDisabled,
      leaky_tick_ms => LeakyTickMs,
      timestamp_cleanup_tick_ms => TimestampCleanupTickMs,
      timestamp_cleanup_expiry => TimestampCleanupExpiry,
      tick_reduction => TickReduction,
      leaky_rate_limit => LeakyRateLimit,
      concurrency_limit => ConcurrencyLimit,
      sliding_window_duration => SlidingWindowDuration,
      sliding_window_limit => SlidingWindowLimit}.

%% Convert reasons into strings/Prometheus labels.
%% We don't really expect anything other than timeouts sometime.
reason_to_list({timeout, _}) ->
    "timeout";
reason_to_list({noproc, _}) ->
    "noproc";
reason_to_list(_) ->
    ?UNEXPECTED_ERROR_STR.

merge_info_maps(LimiterRef, N, #{concurrent_monitors := AccMon,
                                 leaky_tokens := AccTokens,
                                 sliding_timestamps := AccInfoTS} = AccIn) ->
    LimiterWorkerRef = arweave_limiter_util:worker_name(LimiterRef, N),
    %% We could do a pmap to do the gen_server calls, but the latency of this
    %% function is irrevelant, and perhaps we don't need to lock all workers at the same time.
    try gen_server:call(LimiterWorkerRef, get_info) of
        #{concurrent_monitors := InfoMon,
          leaky_tokens := InfoTokens,
          sliding_timestamps := InfoTS} ->

            %% The keys are either: process disjoint sets of monitor references,
            %% or disjoint sets of peers.
            #{concurrent_monitors => maps:merge(InfoMon, AccMon),
              leaky_tokens => maps:merge(InfoTokens, AccTokens),
              sliding_timestamps => maps:merge(InfoTS, AccInfoTS)}
    catch E:R ->
            ?LOG_ERROR([{event, couldnt_scan_limiter_group_info},
                        {limiter_ref, LimiterRef},
                        {n, N},
                        {class, E},
                        {reason, R}]),
            AccIn
    end.

ref_to_worker_ref(LimiterRef, Peer) ->
    WorkersNum = arweave_config:get([limiter, LimiterRef, number_of_workers]),
    arweave_limiter_util:worker_ref(LimiterRef, Peer, WorkersNum).

generate_policy(#{id := GroupID,
                  concurrency_limit := ConcurrencyLimit,
                  sliding_window_duration := SlidingWindowDuration,
                  sliding_window_limit := SlidingWindowLimit,
                  leaky_rate_limit := LeakyRateLimit,
                  leaky_tick_ms := LeakyTickMs,
                  tick_reduction := TickReduction}) ->
    #{id => GroupID,
      concurrency => #{limit => ConcurrencyLimit},
      sliding_window => #{limit => SlidingWindowLimit,
                          window_seconds => SlidingWindowDuration div 1000},
      leaky_bucket => #{burst => LeakyRateLimit,
                        tick_ms => LeakyTickMs,
                        tick_reduction => TickReduction}
     }.

build_headers_info_sliding(Remaining, SWTimestamps, Now, Policies) ->
    SWReset = sliding_window_reset_seconds(SWTimestamps, Now),
    SW = maps:get(sliding_window, Policies),
    ExpiringLimit = maps:get(limit, SW),
    #{expiring_limit => ExpiringLimit,
      remaining      => Remaining,
      reset_seconds  => SWReset,
      policies       => Policies}.

build_headers_info_leaky(Remaining, LbCapacity, NextLeakyTickTS, SWTimestamps, Now, Policies) ->
    LbReset = max(1, (NextLeakyTickTS - Now) div 1000), %% This should be never negative really
    SWReset = sliding_window_reset_seconds(SWTimestamps, Now),

    %% This is pretty much a short circuit for
    Reset = if SWReset > 0 -> min(SWReset, LbReset);
               true -> LbReset
            end,

    ExpiringLimit = LbCapacity,
    #{expiring_limit => ExpiringLimit,
      remaining      => Remaining,
      reset_seconds  => Reset,
      policies       => Policies}.

%% Seconds until the oldest in-window timestamp ages out. 0 when the window
%% has spare capacity (no meaningful "reset" to advertise).
sliding_window_reset_seconds([], _Now) -> 0;
sliding_window_reset_seconds([Oldest | _], Now) when Oldest >= Now -> 0;
sliding_window_reset_seconds([Oldest | _], Now) ->
    %% Timestamps are monotonic ms
    max(1, (Now - Oldest) div 1000).
