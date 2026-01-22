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
         config/1,
         register_or_reject_call/2,
         reduce_for_peer/2,
         reset_all/1,
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
         cleanup_expired_sliding_peers/3]).
-endif.


-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

%%% API
start_link(LimiterRef, Config) ->
    gen_server:start_link({local, LimiterRef}, ?MODULE, [Config], []).

info(LimiterRef) ->
    gen_server:call(LimiterRef, get_info).

config(LimiterRef) ->
    gen_server:call(LimiterRef, get_config).

register_or_reject_call(LimiterRef, Peer) ->
    {Time, Value} = timer:tc(fun do_register_or_reject_call/2, [LimiterRef, Peer]),
    prometheus_histogram:observe(ar_limiter_response_time_microseconds, [atom_to_list(LimiterRef)], Time),
    Value.

do_register_or_reject_call(LimiterRef, Peer) ->
    prometheus_counter:inc(ar_limiter_requests_total,
                           [atom_to_list(LimiterRef)]),
    case gen_server:call(LimiterRef, {register_or_reject, Peer}) of
        {reject, Reason, _Data} = Rejection ->
            prometheus_counter:inc(ar_limiter_rejected_total,
                                   [atom_to_list(LimiterRef), atom_to_list(Reason)]),
            Rejection;
        Accept ->
            Accept
    end.

%% This function is called when a transaction is accepted. This is how the previous
%% solution dealt with high loads. This will perform double reduction. (as the periodic
%% reduction is still occurring).
reduce_for_peer(LimiterRef, Peer) ->
    Result = gen_server:call(LimiterRef, {reduce_for_peer, Peer}),
    Result == ok andalso prometheus_counter:inc(ar_limiter_reduce_requests_total,
                                                [atom_to_list(LimiterRef)]),
    Result.

reset_all(LimiterRef) ->
    whereis(LimiterRef) == undefined orelse gen_server:call(LimiterRef, reset_all).

stop(LimiterRef) ->
    gen_server:stop(LimiterRef).

%% gen_server callbacks
init([Config] = _Args) ->
    Id = maps:get(id, Config),

    IsDisabled = maps:get(no_limit, Config, false),
    IsManualReductionDisabled = maps:get(is_manual_reduction_disabled, Config, false),

    LeakyTickMs = maps:get(leaky_tick_ms, Config, ?DEFAULT_HTTP_API_LIMITER_GENERAL_LEAKY_TICK_INTERVAL),
    TimestampCleanupTickMs = maps:get(timestamp_cleanup_tick_ms, Config,
                                      ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_INTERVAL),
    TimestampCleanupExpiry = maps:get(timestamp_cleanup_expiry, Config,
                                      ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_EXPIRY),
    LeakyRateLimit = maps:get(leaky_rate_limit, Config, ?DEFAULT_HTTP_API_LIMITER_GENERAL_LEAKY_LIMIT),
    ConcurrencyLimit = maps:get(concurrency_limit, Config, ?DEFAULT_HTTP_API_LIMITER_GENERAL_CONCURRENCY_LIMIT),
    TickReduction = maps:get(tick_reduction, Config,
                             ?DEFAULT_HTTP_API_LIMITER_GENERAL_LEAKY_TICK_REDUCTION),
    SlidingWindowDuration = maps:get(sliding_window_duration, Config,
                                     ?DEFAULT_HTTP_API_LIMITER_GENERAL_SLIDING_WINDOW_DURATION),
    SlidingWindowLimit = maps:get(sliding_window_limit, Config,
                                  ?DEFAULT_HTTP_API_LIMITER_GENERAL_SLIDING_WINDOW_LIMIT),

    {ok, LeakyRef} = timer:send_interval(LeakyTickMs, self(), {tick, leaky_bucket_reduction}),
    {ok, TsRef} = timer:send_interval(TimestampCleanupTickMs, self(), {tick, sliding_window_timestamp_cleanup}),
    {ok, #{
           id => atom_to_list(Id),
           is_disabled => IsDisabled,
           is_manual_reduction_disabled => IsManualReductionDisabled,
           leaky_tick_timer_ref => LeakyRef,
           timestamp_cleanup_timer_ref => TsRef,
           leaky_tick_ms => LeakyTickMs,
           timestamp_cleanup_tick_ms => TimestampCleanupTickMs,
           timestamp_cleanup_expiry => TimestampCleanupExpiry,
           tick_reduction => TickReduction,
           leaky_rate_limit => LeakyRateLimit,
           concurrency_limit => ConcurrencyLimit,
           concurrent_requests => #{}, %% Peer -> List of {MonitorRef, Pid}
           concurrent_monitors => #{}, %% MonitorRef -> Peer
           leaky_tokens => #{}, %% Peer -> Leaky Bucket tokens
           sliding_window_duration => SlidingWindowDuration,
           sliding_window_limit => SlidingWindowLimit,
           sliding_timestamps => #{} %% Peer -> Ordered list of timestamps
          }}.

handle_call(reset_all, _From, State) ->
    {reply, ok, State#{concurrent_requests => #{},
                       concurrent_monitors => #{},
                       leaky_tokens => #{},
                       sliding_timestamps => #{}}};
handle_call({register_or_reject, Peer}, {FromPid, _},
            State = #{id := Id,
                      is_disabled := IsDisabled,
                      leaky_rate_limit := LeakyRateLimit,
                      leaky_tokens := LeakyTokens,
                      concurrency_limit := ConcurrencyLimit,
                      concurrent_requests := ConcurrentRequests,
                      concurrent_monitors := ConcurrentMonitors,
                      sliding_window_duration := SlidingWindowDuration,
                      sliding_window_limit := SlidingWindowLimit,
                      sliding_timestamps := SlidingTimestamps
                     }) ->
    Now = arweave_limiter_time:ts_now(),
    Tokens = maps:get(Peer, LeakyTokens, 0) + 1,
    Concurrency = length(maps:get(Peer, ConcurrentRequests, [])) + 1,

    SlidingTimestampsForPeer0 =
        expire_and_get_requests(Peer, SlidingTimestamps, SlidingWindowDuration, Now),
    case IsDisabled of
        true ->
            {reply, {register, no_limiting_applied}, State};
        _ ->
            case Concurrency > ConcurrencyLimit of
                true ->
                    %% Concurrency Hard Limit
                    ?LOG_DEBUG([{event, ar_limiter_reject}, {reason, concurrency},
                                  {peer, Peer}, {id, Id}]),
                    {reply, {reject, concurrency, data}, State};
                _ ->
                    case length(SlidingTimestampsForPeer0) + 1 > SlidingWindowLimit of
                        true ->
                            %% Sliding Window limited, check Leaky Bucket Tokens
                            case Tokens > LeakyRateLimit of
                                true ->
                                    %% Burst exhausted with the Leaky Tokens
                                    ?LOG_DEBUG([{event, ar_limiter_reject}, {reason, rate_limit},
                                                  {sliding_window_limit, SlidingWindowLimit},
                                                  {leaky_rate_limit, LeakyRateLimit},
                                                  {peer, Peer}, {id, Id}]),
                                    {reply, {reject, rate_limit, data}, State};
                                false ->
                                    NewLeakyTokens = update_token(Peer, Tokens, LeakyTokens),
                                    {NewRequests, NewMonitors} =
                                        register_concurrent(
                                          Peer, FromPid, ConcurrentRequests, ConcurrentMonitors),
                                    {reply, {register, leaky},
                                     State#{leaky_tokens => NewLeakyTokens,
                                            concurrent_requests => NewRequests,
                                            concurrent_monitors => NewMonitors}}
                            end;
                        _ ->
                            {NewRequests, NewMonitors} =
                                register_concurrent(
                                  Peer, FromPid, ConcurrentRequests, ConcurrentMonitors),
                            SlidingTimestampsForPeer1 = add_and_order_timestamps(Now, SlidingTimestampsForPeer0),
                            NewSlidingTimestamps = SlidingTimestamps#{Peer => SlidingTimestampsForPeer1},
                            {reply, {register, sliding}, State#{sliding_timestamps => NewSlidingTimestamps,
                                                                concurrent_requests => NewRequests,
                                                                concurrent_monitors => NewMonitors}}
                    end
            end
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
                  concurrent_requests := ConcurrentRequests,
                  concurrent_monitors := ConcurrentMonitors}) ->
    {reply, #{sliding_timestamps => SlidingTimestamps,
              leaky_tokens => LeakyTokens,
              concurrent_requests => ConcurrentRequests,
              concurrent_monitors => ConcurrentMonitors}, State};
handle_call(get_config, _From, State) ->
    {reply, filter_state_for_config(State), State};
handle_call(Request, From, State = #{id := Id}) ->
    ?LOG_WARNING([{event, unhandled_call}, {id, Id}, {module, ?MODULE},
                  {request, Request}, {from, From},
                  {config, filter_state_for_config(State)}]),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({tick, sliding_window_timestamp_cleanup},
            State = #{id := Id, sliding_timestamps := SlidingTimestamps,
                      timestamp_cleanup_expiry := CleanupExpiry}) ->
    Now = arweave_limiter_time:ts_now(),
    NewSlidingTimestamps = cleanup_expired_sliding_peers(SlidingTimestamps, CleanupExpiry, Now),
    Deleted = maps:size(SlidingTimestamps) - maps:size(NewSlidingTimestamps),
    prometheus_counter:inc(ar_limiter_cleanup_tick_expired_sliding_peers_deleted_total, [Id], Deleted),
    {noreply, State#{sliding_timestamps => NewSlidingTimestamps}};
handle_info({tick, leaky_bucket_reduction},
            State = #{id := Id, tick_reduction := TickReduction, leaky_tokens := LeakyTokens}) ->
    %% This is going to be more precise than ar_limiter_leaky_ticks*ar_limiter_peers
    prometheus_counter:inc(ar_limiter_leaky_ticks, [Id]),
    SizeBefore = maps:size(LeakyTokens),
    prometheus_counter:inc(ar_limiter_leaky_tick_reductions_peer, [Id], SizeBefore),
    NewTokens =
        maps:fold(fun(Key, Value, AccIn) ->
                          fold_decrease_rate(Id, Key, Value, AccIn, TickReduction)
                  end, #{}, LeakyTokens),
    prometheus_counter:inc(
      ar_limiter_leaky_tick_delete_peer_total, [Id], SizeBefore - maps:size(NewTokens)),
    {noreply, State#{leaky_tokens => NewTokens}};
handle_info({'DOWN', MonitorRef, process, Pid, Reason},
            State = #{concurrent_requests := ConcurrentRequests,
                      concurrent_monitors := ConcurrentMonitors}) ->
    {NewConcurrentRequests, NewConcurrentMonitors} =
        remove_concurrent(
          MonitorRef, Pid, Reason, ConcurrentRequests, ConcurrentMonitors),
    {noreply, State#{concurrent_requests => NewConcurrentRequests,
                     concurrent_monitors => NewConcurrentMonitors}};
handle_info(Info, State = #{id := Id}) ->
    ?LOG_WARNING([{event, unhandled_info}, {id, Id}, {module, ?MODULE}, {info, Info}]),
    {noreply, State}.

terminate(_Reason, #{leaky_tick_timer_ref := _LeakyRef,
                     timestamp_cleanup_timer_ref := _TsRef} = _State) ->
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
add_and_order_timestamps(Ts, Timestamps) ->
    lists:reverse(do_add_and_order_timestamps(Ts, lists:reverse(Timestamps))).

do_add_and_order_timestamps(Ts, []) ->
    [Ts];
do_add_and_order_timestamps(Ts, [Head | _Rest] = Timestamps) when Ts >= Head ->
    [Ts | Timestamps];
do_add_and_order_timestamps(Ts, [Head | Rest])  ->
    %% This clause shouldn't really reached, because we use monotonic time
    %% for timestamps.
    [Head | do_add_and_order_timestamps(Ts, Rest)].

cleanup_expired_sliding_peers(SlidingTimestamps, WindowDuration, Now) ->
    maps:fold(fun(Peer, TsList, AccIn) ->
                      case drop_expired(TsList, WindowDuration, Now) of
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

fold_decrease_rate(_Id, _Key, Counter, Acc, _TickReduction)
  when is_integer(Counter), Counter =< 0 ->
    Acc;
fold_decrease_rate(Id, Key, Counter, Acc, TickReduction) when Counter < TickReduction ->
    prometheus_counter:inc(ar_limiter_leaky_tick_token_reductions_total, [Id], Counter),
    maps:put(Key, 0, Acc);
fold_decrease_rate(Id, Key, Counter, Acc, TickReduction) ->
    prometheus_counter:inc(ar_limiter_leaky_tick_token_reductions_total, [Id], TickReduction),
    maps:put(Key, Counter-TickReduction, Acc).

%% Concurrency magic
register_concurrent(Peer, Pid, ConcurrentRequests, ConcurrentMonitors) ->
    MonitorRef = erlang:monitor(process, Pid),
    Processes = maps:get(Peer, ConcurrentRequests, []),
    NewConcurrentRequests = maps:put(Peer, [{MonitorRef, Pid} | Processes], ConcurrentRequests),
    NewConcurrentMonitors = maps:put(MonitorRef, Peer, ConcurrentMonitors),
    {NewConcurrentRequests, NewConcurrentMonitors}.

remove_concurrent(MonitorRef, _Pid, _Reason, ConcurrentRequests, ConcurrentMonitors) ->
    %% Peer for a MonitorRef shouldn't be undefined, because we started to
    %% monitor the process as a first thing when register was called.
    case maps:get(MonitorRef, ConcurrentMonitors, not_found) of
        not_found ->
            %% MonitorRef not found. This happens when we reset all the peers
            %% manually. This also means everything else has been deleted as well.
            %% Nothing to do, just return the current state.
            {ConcurrentRequests, ConcurrentMonitors};
        Peer ->
            ConcurrentForPeer = maps:get(Peer, ConcurrentRequests),
            NewConcurrentForPeer = proplists:delete(MonitorRef, ConcurrentForPeer),
            NewConcurrentRequests =
                case NewConcurrentForPeer of
                    [] ->
                        maps:remove(Peer, ConcurrentRequests);
                    _ ->
                        ConcurrentRequests#{Peer => NewConcurrentForPeer}
                end,
            NewConcurrentMonitors = maps:remove(MonitorRef, ConcurrentMonitors),
            {NewConcurrentRequests, NewConcurrentMonitors}
    end.

filter_state_for_config(#{id := Id,
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
    #{id => Id,
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
