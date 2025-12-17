%%%
%%% @doc Leaky bucket token rate limiter based on
%%%      https://gist.github.com/humaite/21a84c3b3afac07fcebe476580f3a40b
%%%      combined with a concurrency limiter similar to Ranch's connection pool.
%%%      It only stores data in process memory.
%%%
-module(ar_limiter).

-behaviour(gen_server).

%% API
-export([
         start_link/2,
         info/1,
         register_or_reject_call/2,
         stop/1
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).

-define(SERVER, ?MODULE).

%% TODO: determine sensible defaults based on desired load profile,
%%       and where to store these macros what config are they a part of,
%%       semantically?
-define(DEFAULT_TICK_INTERVAL_MS, 1000).
-define(DEFAULT_LEAKY_RATE_LIMIT, 5).
-define(DEFAULT_CONCURRENCY_LIMIT, 2).
-define(DEFAULT_TICK_REDUCTION, 1).

-define(DEFAULT_SLIDING_WINDOW_DURATION, 1000).
-define(DEFAULT_SLIDING_WINDOW_LIMIT, 5).

-include_lib("arweave/include/ar.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_LIMITER, test_limiter).
-define(assertHandlerRegisterOrRejectCall(LimiterRef, Pattern, Peer, Now),
 	((fun () ->
                  spawn_link(fun() ->
                                     ?assertMatch(
                                        Pattern,
                                        register_or_reject_call(LimiterRef, Peer, Now)),
                                     receive
                                         done -> ok
                                     end
                             end)
	  end)())).
-endif.

%%% API
start_link(LimiterRef, Args) ->
    gen_server:start_link({local, LimiterRef}, ?MODULE, [Args], []).

info(LimiterRef) ->
    gen_server:call(LimiterRef, get_info).

register_or_reject_call(LimiterRef, Peer) ->
    %% FIXME?
    %% Ideally, we would like to take this timestamp in the process, so
    %% we don't have timestamps out of order. This would bring additional
    %% opportunities for optimisations as well.
    %% If inaccuraccy due to providing the timestamp from the caller process
    %% or performance becomes an issue, we'll change this. For now, testability
    %% is more important.
    Now = erlang:monotonic_time(millisecond),
    register_or_reject_call(LimiterRef, Peer, Now).

register_or_reject_call(LimiterRef, Peer, Now) ->
    gen_server:call(LimiterRef, {register_or_reject, Peer, Now}).

stop(LimiterRef) ->
    gen_server:stop(LimiterRef).


%% gen_server callbacks
init([Args]) ->
    process_flag(trap_exit, true),
    TickMs = maps:get(tick_interval_ms, Args, ?DEFAULT_TICK_INTERVAL_MS),
    LeakyRateLimit = maps:get(leaky_rate_limit, Args, ?DEFAULT_LEAKY_RATE_LIMIT),
    ConcurrencyLimit = maps:get(concurrency_limit, Args, ?DEFAULT_CONCURRENCY_LIMIT),
    TickReduction = maps:get(tick_reduction, Args, ?DEFAULT_TICK_REDUCTION),
    SlidingWindowDuration = maps:get(sliding_window_duration, Args, ?DEFAULT_SLIDING_WINDOW_DURATION),
    SlidingWindowLimit = maps:get(sliding_window_limit, Args, ?DEFAULT_SLIDING_WINDOW_LIMIT),

    {ok, Ref} = timer:send_interval(TickMs, self(), {tick, rate_limit}),
    {ok, #{
           timer_ref => Ref,
           tick_ms => TickMs,
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

handle_call({register_or_reject, Peer, Now}, {FromPid, _},
            State = #{leaky_rate_limit := LeakyRateLimit,
                      leaky_tokens := LeakyTokens,
                      concurrency_limit := ConcurrencyLimit,
                      concurrent_requests := ConcurrentRequests,
                      concurrent_monitors := ConcurrentMonitors,
                      sliding_window_duration := SlidingWindowDuration,
                      sliding_window_limit := SlidingWindowLimit,
                      sliding_timestamps := SlidingTimestamps
                     }) ->
    Tokens = maps:get(Peer, LeakyTokens, 0) + 1,
    Concurrency = length(maps:get(Peer, ConcurrentRequests, [])) + 1,

    SlidingTimestampsForPeer0 = 
        expire_and_get_requests(Peer, SlidingTimestamps, SlidingWindowDuration, Now),

    case Concurrency > ConcurrencyLimit of
        true ->
            %% Concurrency Hard Limit
            {reply, {reject, concurrency, data}, State};
        _ ->
            case length(SlidingTimestampsForPeer0) + 1 > SlidingWindowLimit of
                true ->
                    %% Sliding Window limited, check Leaky Bucket Tokens
                    case Tokens > LeakyRateLimit of
                        true ->
                            %% Burst exhausted with the Leaky Tokens
                            {reply, {reject, rate_limit, data}, State};
                        false ->
                            NewLeakyTokens = update_token(Peer, Tokens, LeakyTokens),
                            {NewRequests, NewMonitors} =
                                register_concurrent(
                                  Peer, FromPid, ConcurrentRequests, ConcurrentMonitors),
                            %% QUESTION: AT THIS POINT, DO WE WANT TO ADD THE REQUEST
                            %%           TIMESTAMP TO THE SLIDING WINDOW TO PENALISE THE BURST?
                            {reply, register,
                             State#{leaky_tokens => NewLeakyTokens,
                                    concurrent_requests => NewRequests,
                                    concurrent_monitors := NewMonitors}}
                    end;
                _ ->
                    {NewRequests, NewMonitors} =
                        register_concurrent(
                          Peer, FromPid, ConcurrentRequests, ConcurrentMonitors),
                    SlidingTimestampsForPeer1 = add_and_order_timestamps(Now, SlidingTimestampsForPeer0),
                    NewSlidingTimestamps = SlidingTimestamps#{Peer => SlidingTimestampsForPeer1},
                    {reply, register, State#{sliding_timestamps := NewSlidingTimestamps,
                                             concurrent_requests => NewRequests,
                                             concurrent_monitors := NewMonitors}}
            end
    end;
handle_call(get_info, _From, State =
                #{sliding_timestamps := SlidingTimestamps,
                  leaky_tokens := LeakyTokens,
                  concurrent_requests := ConcurrentRequests,
                  concurrent_monitors := ConcurrentMonitors}) ->
    {reply, #{sliding_timestamps => SlidingTimestamps,
              leaky_tokens => LeakyTokens,
              concurrent_requests => ConcurrentRequests,
              concurrent_monitors => ConcurrentMonitors}, State};
handle_call(Request, From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}, {from, From}]),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({tick, rate_limit},
            State = #{tick_reduction := TickReduction, leaky_tokens := LeakyTokens}) ->
    NewTokens =
        maps:fold(fun(Key, Value, AccIn) ->
                          fold_decrease_rate(Key, Value, AccIn, TickReduction)
                  end, #{}, LeakyTokens),
    {noreply, State#{leaky_tokens => NewTokens}};
handle_info({'DOWN', MonitorRef, process, Pid, Reason},
            State = #{concurrent_requests := ConcurrentRequests,
                      concurrent_monitors := ConcurrentMonitors}) ->
    {NewConcurrentRequests, NewConcurrentMonitors} =
        remove_concurrent(
          MonitorRef, Pid, Reason, ConcurrentRequests, ConcurrentMonitors),
    {noreply, State#{concurrent_requests => NewConcurrentRequests,
                     concurrent_monitors => NewConcurrentMonitors}};
handle_info(Info, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
    {noreply, State}.

terminate(_Reason, _State) ->
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

add_and_order_timestamps(Ts, Timestamps) ->
    lists:reverse(do_add_and_order_timestamps(Ts, lists:reverse(Timestamps))).

do_add_and_order_timestamps(Ts, []) ->
    [Ts];
do_add_and_order_timestamps(Ts, [Head | _Rest] = Timestamps) when Ts >= Head ->
    [Ts | Timestamps];
do_add_and_order_timestamps(Ts, [Head | Rest])  ->
    [Head | do_add_and_order_timestamps(Ts, Rest)].  

%% Token manipulation
update_token(Peer, Token, LeakyToken) ->
    maps:put(Peer, Token, LeakyToken).

fold_decrease_rate(_Key, Counter, Acc, _TickReduction)
  when is_integer(Counter), Counter =< 0 ->
    Acc;
fold_decrease_rate(Key, Counter, Acc, TickReduction) ->
    maps:put(Key, Counter - TickReduction, Acc).

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
    Peer = maps:get(MonitorRef, ConcurrentMonitors),
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
    {NewConcurrentRequests, NewConcurrentMonitors}.

%% TESTS
-ifdef(TEST).

expire_test() ->
    IP = {1,2,3,4},
    ?assertEqual([], expire_and_get_requests(IP, #{}, 1000, 1)),
    ?assertEqual([1], drop_expired([1], 1000, 500)),
    ?assertEqual([1], expire_and_get_requests(IP, #{IP => [1]}, 1000, 500)),
    ?assertEqual([1, 500], expire_and_get_requests(IP, #{IP => [1, 500]}, 1000, 501)),
    ?assertEqual([500, 501], expire_and_get_requests(IP, #{IP => [1, 500, 501]}, 1000, 1100)),
    ?assertEqual([500, 501], expire_and_get_requests(IP, #{IP => [1, 500, 501]}, 1000, 1499)),
    ?assertEqual([501], expire_and_get_requests(IP, #{IP => [1, 500, 501]}, 1000, 1500)),
    ?assertEqual([], expire_and_get_requests(IP, #{IP => [1, 500, 501]}, 1000, 1501)),
    ok.

add_and_order_test() ->
    ?assertEqual([5], add_and_order_timestamps(5, [])),
    ?assertEqual([1,2,3,4,5], add_and_order_timestamps(5, [1,2,3,4])),
    ?assertEqual([1,2,3,4,5,6,7], add_and_order_timestamps(5, [1,2,3,4,6,7])),
    ?assertEqual([5,7,8], add_and_order_timestamps(5, [7,8])),
    ok.

rate_limiter_happy_path_0_leaky_tokens_test() ->
    %% Start with 0 leaky_rate, reject when sliding window is exhausted.
    IP = {1,2,3,4},
    {ok, LimiterPid} = ?MODULE:start_link(?TEST_LIMITER,
                                          #{leaky_rate_limit => 0,
                                            concurrency_limit => 5,
                                            sliding_window_limit => 2,
                                            sliding_window_duration => 1000,
                                            tick_interval_ms => 100000}),
    
    Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 1),
    Caller1 ! done,

    timer:sleep(100),
    Info1 = info(?TEST_LIMITER),
    ?assertMatch(#{sliding_timestamps := #{IP := [1]}}, Info1),
    #{concurrent_requests := ConcurrentReqs1} = Info1,
    ?assertEqual(0, maps:size(ConcurrentReqs1)),

    Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 500),
    Caller2 ! done,

    timer:sleep(100),
    Info2 = info(?TEST_LIMITER),
    ?assertMatch(#{sliding_timestamps := #{IP := [1,500]}}, Info2),
    #{concurrent_requests := ConcurrentReqs2} = Info2,
    ?assertEqual(0, maps:size(ConcurrentReqs2)),

    Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 2000),
    Caller3 ! done,
    timer:sleep(100),
    %% 2 previous ts expired due to the time elapsed.
    ?assertMatch(#{sliding_timestamps := #{IP := [2000]}}, info(?TEST_LIMITER)),
    
    Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 2001),
    Caller4 ! done,
    timer:sleep(100),
    ?assertMatch(#{sliding_timestamps := #{IP := [2000, 2001]}}, info(?TEST_LIMITER)),
    
    Caller5 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {reject, rate_limit, _} , IP, 2002),
    Caller5 ! done,
    timer:sleep(100),
    %% Wait a bit for surely have request processed, and observe, no new timestamp
    ?assertMatch(#{sliding_timestamps := #{IP := [2000, 2001]}}, info(?TEST_LIMITER)),

    ?MODULE:stop(?TEST_LIMITER).

rate_limiter_happy_path_0_sliding_window_test() ->
    %% Start with low limits, and extremely high interval, so we can control
    %% ticks manually in the test.
    %% Sliding window limit is 0, so we test the extremes.
    {ok, LimiterPid} = ?MODULE:start_link(?TEST_LIMITER,
                                          #{leaky_rate_limit => 5,
                                            concurrency_limit => 2,
                                            sliding_window_limit => 0,
                                            tick_interval_ms => 100000}),

    % init state, the ip is not blocked
    IP = {1,2,3,4},

    %% Spawn link will crash the test process and make the test fail if
    %% assertion fails.
    %% FIXME: needs proper cleanup, in case of a test failure as well.
    Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),
    Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),

    %% wait a bit so they are surely started.
    timer:sleep(100),
    ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                   leaky_tokens := #{IP := 2}}, info(?TEST_LIMITER)),


    Caller1 ! done,
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_requests := #{IP := [_]},
                   leaky_tokens := #{IP := 2}}, info(?TEST_LIMITER)),

    Caller2 ! done,
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    %% Keys deleted
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{IP := 2}}, info(?TEST_LIMITER)),

    %% manually trigger a tick.
    LimiterPid ! {tick, rate_limit},
    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{IP := 1}}, info(?TEST_LIMITER)),

    %% manually trigger a tick.
    LimiterPid ! {tick, rate_limit},
    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{IP := 0}}, info(?TEST_LIMITER)),

    %% manually trigger a tick.
    LimiterPid ! {tick, rate_limit},
    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{}}, info(?TEST_LIMITER)),

    % stop the service
    ?MODULE:stop(?TEST_LIMITER).

rate_limiter_rejected_due_concurrency_test() ->
    %% Start with low limits, and extremely high interval, so we can control
    %% ticks manually in the test.
    %% Sliding window limit is 0, so we test the extremes.
    {ok, LimiterPid} = ?MODULE:start_link(?TEST_LIMITER,
                                          #{leaky_rate_limit=> 5,
                                            concurrency_limit => 2,
                                            sliding_window_limit => 0,
                                            tick_interval_ms => 100000}),

    % init state, the ip is not blocked
    IP = {1,2,3,4},

    %% Spawn link will crash the test process and make the test fail if
    %% assertion fails.
    %% FIXME: needs proper cleanup, in case of a failure as well.
    Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),
    Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),
    Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {reject, concurrency, _Data}, IP, 0),

    %% wait a bit so they are surely started.
    timer:sleep(100),

    ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                   leaky_tokens := #{IP := 2}}, info(?TEST_LIMITER)),


    Caller1 ! done,
    Caller2 ! done,
    Caller3 ! done,
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    %% Keys deleted
    %% NOTE: concurrent_requests := #{} matches to any map, so we don't what's in there.
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{IP := 2}}, info(?TEST_LIMITER)),

    %% manually trigger a tick.
    LimiterPid ! {tick, rate_limit},
    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{IP := 1}}, info(?TEST_LIMITER)),

    %% Concurrency reduced, one handler terminated, will register again
    Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    Caller4 ! done,
    %% Keys deleted
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{IP := 2}}, info(?TEST_LIMITER)),


    %% manually trigger two ticks.
    LimiterPid ! {tick, rate_limit},
    LimiterPid ! {tick, rate_limit},
    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{IP := 0}}, info(?TEST_LIMITER)),

    %% manually trigger a tick.
    LimiterPid ! {tick, rate_limit},
    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{}}, info(?TEST_LIMITER)),

    % stop the service
    ?MODULE:stop(?TEST_LIMITER).

rate_limiter_rejected_due_leaky_rate_test() ->
    %% Start with low limits, and extremely high interval, so we can control
    %% ticks manually in the test.
    %% Sliding window limit is 0, so we test the extremes.
    {ok, LimiterPid} = ?MODULE:start_link(?TEST_LIMITER,
                                          #{leaky_rate_limit=> 2,
                                            concurrency_limit => 5,
                                            sliding_window_limit => 0,
                                            tick_interval_ms => 100000}),

    % init state, the ip is not blocked
    IP = {1,2,3,4},

    %% Spawn link will crash the test process and make the test fail if
    %% assertion fails.
    %% FIXME: needs proper cleanup, in case of a failure as well.
    Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),
    Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),
    Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {reject, rate_limit, _Data}, IP, 0),

    %% wait a bit so they are surely started.
    timer:sleep(100),
    %% 2 concurrent, 2 token
    ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                   leaky_tokens := #{IP := 2}}, info(?TEST_LIMITER)),

    %% Simulate a tick
    LimiterPid ! {tick, rate_limit},
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    %% 2 concurrent, but tokens reduced.
    ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                   leaky_tokens := #{IP := 1}}, info(?TEST_LIMITER)),


    %% Tokens reduced, will register again
    Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    %% 3 concurrent, 2 tokens
    ?assertMatch(#{concurrent_requests := #{IP := [_,_,_]},
                   leaky_tokens := #{IP := 2}}, info(?TEST_LIMITER)),


    %% manually trigger two ticks.
    LimiterPid ! {tick, rate_limit},
    LimiterPid ! {tick, rate_limit},

    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_requests := #{IP := [_,_,_]},
                   leaky_tokens := #{IP := 0}}, info(?TEST_LIMITER)),

    %% Clean up
    Caller1 ! done,
    Caller2 ! done,
    Caller3 ! done,
    Caller4 ! done,

    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{}}, info(?TEST_LIMITER)),


    % stop the service
    ?MODULE:stop(?TEST_LIMITER).

-endif.
