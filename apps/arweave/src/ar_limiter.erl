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

-include_lib("arweave/include/ar.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_LIMITER, test_limiter).
-define(assertHandlerRegisterOrRejectCall(LimiterRef, Pattern, Peer),
 	((fun () ->
                  spawn_link(fun() ->
                                     ?assertEqual(
                                        Pattern,
                                        register_or_reject_call(LimiterRef, Peer)),
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
    gen_server:call(LimiterRef, {register_or_reject, Peer}).

stop(LimiterRef) ->
    gen_server:stop(LimiterRef).


%% gen_server callbacks
init([Args]) ->
    process_flag(trap_exit, true),
    TickMs = maps:get(tick_interval_ms, Args, ?DEFAULT_TICK_INTERVAL_MS),
    LeakyRateLimit = maps:get(leaky_rate_limit, Args, ?DEFAULT_LEAKY_RATE_LIMIT),
    ConcurrencyLimit = maps:get(concurrency_limit, Args, ?DEFAULT_CONCURRENCY_LIMIT),
    TickReduction = maps:get(tick_reduction, Args, ?DEFAULT_TICK_REDUCTION),

    {ok, Ref} = timer:send_interval(TickMs, self(), {tick, rate_limit}),
    {ok, #{
           timer_ref => Ref,
           tick_ms => TickMs,
           tick_reduction => TickReduction,
           leaky_rate_limit => LeakyRateLimit,
           concurrency_limit => ConcurrencyLimit,
           concurrent_requests => #{}, %% Peer -> List of {MonitorRef, Pid}
           concurrent_monitors => #{}, %% MonitorRef -> Peer
           leaky_tokens => #{} %% Peer -> Leaky Bucket tokens
           }}.

handle_call({register_or_reject, Peer}, {FromPid, _},
            State = #{leaky_rate_limit := LeakyRateLimit,
                      leaky_tokens := LeakyTokens,
                      concurrency_limit := ConcurrencyLimit,
                      concurrent_requests := ConcurrentRequests,
                      concurrent_monitors := ConcurrentMonitors
                     }) ->
    Tokens = maps:get(Peer, LeakyTokens, 0) + 1,
    Concurrency = length(maps:get(Peer, ConcurrentRequests, [])) + 1,

    case Tokens > LeakyRateLimit orelse Concurrency > ConcurrencyLimit of
        true ->
            {reply, reject, State};
        false ->
            NewLeakyTokens = update_token(Peer, Tokens, LeakyTokens),
            {NewRequests, NewMonitors} = register_concurrent(
                                         Peer, FromPid, ConcurrentRequests, ConcurrentMonitors),
            {reply, register,
             State#{leaky_tokens => NewLeakyTokens,
                    concurrent_requests => NewRequests,
                    concurrent_monitors := NewMonitors}}
    end;
handle_call(get_info, _From, State =
                #{leaky_tokens := LeakyTokens,
                  concurrent_requests := ConcurrentRequests,
                  concurrent_monitors := ConcurrentMonitors}) ->
    {reply, #{leaky_tokens => LeakyTokens,
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

rate_limiter_happy_path_test() ->
    %% Start with low limits, and extremely high interval, so we can control
    %% ticks manually in the test.
    {ok, LimiterPid} = ?MODULE:start_link(?TEST_LIMITER,
                                          #{leaky_rate_limit => 5,
                                            concurrency_limit => 2,
                                            tick_interval_ms => 100000}),

    % init state, the ip is not blocked
    IP = {1,2,3,4},

    %% Spawn link will crash the test process and make the test fail if
    %% assertion fails.
    %% FIXME: needs proper cleanup, in case of a test failure as well.
    Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP),
    Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP),

    %% wait a bit so they are surely started.
    timer:sleep(100),
    ?assertMatch(#{concurrent_requests := #{{1,2,3,4} := [_,_]},
                   leaky_tokens := #{{1,2,3,4} := 2}}, info(?TEST_LIMITER)),


    Caller1 ! done,
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_requests := #{{1,2,3,4} := [_]},
                   leaky_tokens := #{{1,2,3,4} := 2}}, info(?TEST_LIMITER)),

    Caller2 ! done,
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    %% Keys deleted
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{{1,2,3,4} := 2}}, info(?TEST_LIMITER)),

    %% manually trigger a tick.
    LimiterPid ! {tick, rate_limit},
    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{{1,2,3,4} := 1}}, info(?TEST_LIMITER)),

    %% manually trigger a tick.
    LimiterPid ! {tick, rate_limit},
    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{{1,2,3,4} := 0}}, info(?TEST_LIMITER)),

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
    {ok, LimiterPid} = ?MODULE:start_link(?TEST_LIMITER,
                                          #{leaky_rate_limit=> 5,
                                            concurrency_limit => 2,
                                            tick_interval_ms => 100000}),

    % init state, the ip is not blocked
    IP = {1,2,3,4},

    %% Spawn link will crash the test process and make the test fail if
    %% assertion fails.
    %% FIXME: needs proper cleanup, in case of a failure as well.
    Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP),
    Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP),
    Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, reject, IP),

    %% wait a bit so they are surely started.
    timer:sleep(100),

    ?assertMatch(#{concurrent_requests := #{{1,2,3,4} := [_,_]},
                   leaky_tokens := #{{1,2,3,4} := 2}}, info(?TEST_LIMITER)),


    Caller1 ! done,
    Caller2 ! done,
    Caller3 ! done,
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    %% Keys deleted
    %% NOTE: concurrent_requests := #{} matches to any map, so we don't what's in there.
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{{1,2,3,4} := 2}}, info(?TEST_LIMITER)),

    %% manually trigger a tick.
    LimiterPid ! {tick, rate_limit},
    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{{1,2,3,4} := 1}}, info(?TEST_LIMITER)),

    %% Concurrency reduced, one handler terminated, will register again
    Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP),
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    Caller4 ! done,
    %% Keys deleted
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{{1,2,3,4} := 2}}, info(?TEST_LIMITER)),


    %% manually trigger two ticks.
    LimiterPid ! {tick, rate_limit},
    LimiterPid ! {tick, rate_limit},
    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_requests := #{},
                   leaky_tokens := #{{1,2,3,4} := 0}}, info(?TEST_LIMITER)),

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
    {ok, LimiterPid} = ?MODULE:start_link(?TEST_LIMITER,
                                          #{leaky_rate_limit=> 2,
                                            concurrency_limit => 5,
                                            tick_interval_ms => 100000}),

    % init state, the ip is not blocked
    IP = {1,2,3,4},

    %% Spawn link will crash the test process and make the test fail if
    %% assertion fails.
    %% FIXME: needs proper cleanup, in case of a failure as well.
    Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP),
    Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP),
    Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, reject, IP),

    %% wait a bit so they are surely started.
    timer:sleep(100),
    %% 2 concurrent, 2 token
    ?assertMatch(#{concurrent_requests := #{{1,2,3,4} := [_,_]},
                   leaky_tokens := #{{1,2,3,4} := 2}}, info(?TEST_LIMITER)),

    %% Simulate a tick
    LimiterPid ! {tick, rate_limit},
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    %% 2 concurrent, but tokens reduced.
    ?assertMatch(#{concurrent_requests := #{{1,2,3,4} := [_,_]},
                   leaky_tokens := #{{1,2,3,4} := 1}}, info(?TEST_LIMITER)),


    %% Tokens reduced, will register again
    Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP),
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    %% 3 concurrent, 2 tokens
    ?assertMatch(#{concurrent_requests := #{{1,2,3,4} := [_,_,_]},
                   leaky_tokens := #{{1,2,3,4} := 2}}, info(?TEST_LIMITER)),


    %% manually trigger two ticks.
    LimiterPid ! {tick, rate_limit},
    LimiterPid ! {tick, rate_limit},

    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_requests := #{{1,2,3,4} := [_,_,_]},
                   leaky_tokens := #{{1,2,3,4} := 0}}, info(?TEST_LIMITER)),

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
