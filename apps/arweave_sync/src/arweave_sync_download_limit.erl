%%% @doc The node-wide download limit for chunk sync. The limit keeps a byte
%%% balance that grows at sync.max_download_rate bytes per second, up to one
%%% second's worth. arweave_sync_scheduler drives the limit:
%%% 1. Refill (refill/1), at the start of each dispatch pass: add to the
%%%    balance for the time since the last refill.
%%% 2. Start (has_capacity/1, consume/2), for each fetch the pass starts: a
%%%    fetch can start while the balance is at least one byte, and then
%%%    subtracts a whole chunk. The balance can go below zero by at most one
%%%    chunk, so the average rate still matches the limit.
%%% 3. Wakeup (maybe_schedule_wakeup/3), at the end of each pass: if queued
%%%    work is waiting for the balance and no timer is pending, start a timer
%%%    for when the balance should reach one chunk. The scheduler runs a pass
%%%    when the timer fires (wakeup_fired/1).
%%% 4. Fetch results (restore/2), after each fetch: give back the bytes the
%%%    fetch did not deliver.
%%%
%%% A sync.max_download_rate of 0 turns chunk sync off (enabled/0).
-module(arweave_sync_download_limit).

-ifdef(AR_TEST).
-export([
    do_refill/3,
    do_restore/3,
    wakeup_delay/3
]).
-endif.

-export([
    new/0, new/1,
    enabled/0,
    refill/1, refill/2,
    has_capacity/1,
    consume/2,
    restore/2,
    maybe_schedule_wakeup/3,
    wakeup_fired/1
]).
-export_type([t/0]).

-include_lib("arweave/include/ar.hrl").

-include("arweave_sync_download_limit.hrl").
-include_lib("arweave_sync/include/arweave_sync_deps.hrl").

-opaque t() :: #rate{}.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Create a limiter using the current monotonic time.
new() ->
    new(?DEP(clock):monotonic_ms()).

%% @doc Create a limiter using an explicit monotonic timestamp.
new(NowMs) ->
    #rate{refill_ms = NowMs}.

%% @doc Return whether the download-rate setting enables chunk syncing.
enabled() ->
    max_download_rate() =/= 0.

%%%===================================================================
%%% Refill.
%%%===================================================================

%% @doc Add to the balance for the time since the previous refill.
refill(Rate) ->
    refill(Rate, ?DEP(clock):monotonic_ms()).

refill(Rate, NowMs) ->
    do_refill(Rate, NowMs, max_download_rate()).

do_refill(Rate, NowMs, Limit) ->
    #rate{balance = Balance, refill_ms = RefillMs} = Rate,
    Balance2 =
        case {Limit, Balance} of
            {infinity, _} ->
                infinity;
            {Limit, infinity} ->
                %% A new limit starts with a full second's worth of bytes.
                float(Limit);
            {Limit, _} ->
                %% Tests can replace the clock with one that moves backward;
                %% treat that as no time elapsed.
                ElapsedMs = max(0, NowMs - RefillMs),
                min(float(Limit), Balance + ElapsedMs * Limit / 1000)
        end,
    Rate#rate{balance = Balance2, refill_ms = NowMs}.

%%%===================================================================
%%% Start.
%%%===================================================================

%% @doc Return whether another fetch may start.
has_capacity(#rate{balance = infinity}) ->
    true;
has_capacity(#rate{balance = Balance}) ->
    Balance >= 1.0.

%% @doc Reserve Bytes from the balance.
consume(#rate{balance = infinity} = Rate, _Bytes) ->
    Rate;
consume(#rate{balance = Balance} = Rate, Bytes) ->
    Rate#rate{balance = Balance - Bytes}.

%%%===================================================================
%%% Wakeup.
%%%===================================================================

%% @doc Start a wakeup timer when queued work is waiting for the balance to
%% refill, unless a timer is already pending.
maybe_schedule_wakeup(
    #rate{wakeup_scheduled = true} = Rate,
    _HasQueuedWork,
    _MaxDelayMs
) ->
    Rate;
maybe_schedule_wakeup(Rate, HasQueuedWork, MaxDelayMs) ->
    case not has_capacity(Rate) andalso HasQueuedWork of
        true ->
            DelayMs = wakeup_delay(Rate, max_download_rate(), MaxDelayMs),
            {ok, _} = ?DEP(clock):send_after(
                DelayMs, self(), {arweave_sync_download_limit, wakeup}
            ),
            Rate#rate{wakeup_scheduled = true};
        false ->
            Rate
    end.

%% @doc Record that the wakeup timer has fired.
wakeup_fired(Rate) ->
    Rate#rate{wakeup_scheduled = false}.

wakeup_delay(#rate{balance = Balance}, Limit, MaxDelayMs) ->
    Deficit = ?DATA_CHUNK_SIZE - Balance,
    min(MaxDelayMs, max(100, ceil(Deficit * 1000 / max(1, Limit)))).

%%%===================================================================
%%% Fetch results.
%%%===================================================================

%% @doc Restore reserved bytes when a fetch did not deliver them.
restore(Rate, 0) ->
    Rate;
restore(Rate, Bytes) ->
    do_restore(Rate, Bytes, max_download_rate()).

do_restore(#rate{balance = infinity} = Rate, _Bytes, _Limit) ->
    Rate;
do_restore(#rate{} = Rate, _Bytes, infinity) ->
    Rate;
do_restore(#rate{balance = Balance} = Rate, Bytes, Limit) ->
    Rate#rate{balance = min(float(Limit), Balance + Bytes)}.

%%%===================================================================
%%% Shared helpers.
%%%===================================================================

max_download_rate() ->
    ?DEP(config):get([sync, max_download_rate]).
