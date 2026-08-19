%%% @doc Global download-rate limiter for chunk sync.
%%%
%%% The limiter is a one-second token bucket. A dispatch may start one task
%%% while the remaining balance is positive, then consume that task in full;
%%% this permits at most one task of overdraft and preserves the configured
%%% long-run rate. Bytes reserved for a failed fetch are restored because they
%%% were not delivered.
-module(ar_sync_download_limit).
-test_category([fast]).

-export([new/0, new/1, enabled/0, refill/1, refill/2, has_capacity/1,
        consume/2, restore/2, maybe_schedule_wakeup/3, wakeup_fired/1]).
-export_type([t/0]).

-include_lib("arweave/include/ar.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(rate, {
    balance = infinity,
    refill_ms,
    wakeup_scheduled = false
}).

-opaque t() :: #rate{}.

%% @doc Create a limiter using the current monotonic time.
new() ->
    new(ar_timer:monotonic_ms()).

%% @doc Create a limiter using an explicit monotonic timestamp.
new(NowMs) ->
    #rate{ refill_ms = NowMs }.

%% @doc Return whether chunk syncing is enabled by its download-rate setting.
enabled() ->
    max_download_rate() =/= 0.

%% @doc Add capacity earned since the previous refill.
refill(Rate) ->
    refill(Rate, ar_timer:monotonic_ms()).

refill(Rate, NowMs) ->
    do_refill(Rate, NowMs, max_download_rate()).

%% @doc Return whether another fetch may start.
has_capacity(#rate{ balance = infinity }) ->
    true;
has_capacity(#rate{ balance = Balance }) ->
    Balance > 0.

%% @doc Reserve Bytes of the current download allowance.
consume(#rate{ balance = infinity } = Rate, _Bytes) ->
    Rate;
consume(#rate{ balance = Balance } = Rate, Bytes) ->
    Rate#rate{ balance = Balance - Bytes }.

%% @doc Restore reserved bytes when a fetch did not deliver them.
restore(Rate, 0) ->
    Rate;
restore(Rate, Bytes) ->
    do_restore(Rate, Bytes, max_download_rate()).

do_restore(#rate{ balance = infinity } = Rate, _Bytes, _Limit) ->
    Rate;
do_restore(#rate{} = Rate, _Bytes, infinity) ->
    Rate;
do_restore(#rate{ balance = Balance } = Rate, Bytes, Limit) ->
    Rate#rate{ balance = min(Limit, Balance + Bytes) }.

%% @doc Schedule a retry when download capacity is the only reason queued work
%% cannot start. At most one retry timer may be pending.
maybe_schedule_wakeup(#rate{ wakeup_scheduled = true } = Rate,
        _HasQueuedWork, _MaxDelayMs) ->
    Rate;
maybe_schedule_wakeup(Rate, HasQueuedWork, MaxDelayMs) ->
    case not has_capacity(Rate) andalso HasQueuedWork of
        true ->
            DelayMs = wakeup_delay(Rate, max_download_rate(), MaxDelayMs),
            {ok, _} = ar_timer:send_after(
                DelayMs, self(), {ar_sync_download_limit, wakeup}),
            Rate#rate{ wakeup_scheduled = true };
        false ->
            Rate
    end.

%% @doc Mark the pending retry as delivered.
wakeup_fired(Rate) ->
    Rate#rate{ wakeup_scheduled = false }.

do_refill(Rate, NowMs, Limit) ->
    #rate{ balance = Balance, refill_ms = RefillMs } = Rate,
    Balance2 = case {Limit, Balance} of
        {infinity, _} ->
            infinity;
        {Limit, infinity} ->
            %% A newly configured limiter starts with one full second.
            Limit;
        {Limit, _} ->
            %% Clamp elapsed time if the injected clock source moves backward.
            ElapsedMs = max(0, NowMs - RefillMs),
            min(Limit, Balance + ElapsedMs * Limit div 1000)
    end,
    Rate#rate{ balance = Balance2, refill_ms = NowMs }.

wakeup_delay(#rate{ balance = Balance }, Limit, MaxDelayMs) ->
    Deficit = ?DATA_CHUNK_SIZE - Balance,
    min(MaxDelayMs, max(100, Deficit * 1000 div max(1, Limit))).

max_download_rate() ->
    arweave_config:get([sync, max_download_rate]).

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).

rate_limit_test() ->
    Started = do_refill(new(0), 1000, 1000),
    ?assertEqual(1000, Started#rate.balance),
    ?assertEqual(1000, Started#rate.refill_ms),
    HalfRefilled = do_refill(
        Started#rate{ balance = 200 }, 1500, 1000),
    ?assertEqual(700, HalfRefilled#rate.balance),
    ?assertEqual(1000,
        (do_refill(HalfRefilled, 2500, 1000))#rate.balance),
    %% A clock moving backward adds no capacity.
    ?assertEqual(700,
        (do_refill(HalfRefilled, 500, 1000))#rate.balance),
    ?assertEqual(infinity,
        (do_refill(HalfRefilled, 2000, infinity))#rate.balance),

    Exhausted = consume(#rate{ balance = 1 }, ?DATA_CHUNK_SIZE),
    ?assertNot(has_capacity(Exhausted)),
    ?assertEqual(250,
        (do_restore(#rate{ balance = 100 }, 200, 250))#rate.balance),
    ?assertEqual(infinity,
        (do_restore(#rate{ balance = infinity }, 200, 250))#rate.balance),

    ?assertEqual(1000,
        wakeup_delay(#rate{ balance = 0 }, ?DATA_CHUNK_SIZE, 10_000)),
    ?assertEqual(2000,
        wakeup_delay(#rate{ balance = -?DATA_CHUNK_SIZE },
            ?DATA_CHUNK_SIZE, 10_000)),
    ?assertEqual(100,
        wakeup_delay(#rate{ balance = 0 },
            100 * ?DATA_CHUNK_SIZE, 10_000)),
    ?assertEqual(10_000,
        wakeup_delay(#rate{ balance = 0 }, 0, 10_000)).

-endif.
