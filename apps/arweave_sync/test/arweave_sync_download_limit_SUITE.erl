-module(arweave_sync_download_limit_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("arweave/include/ar.hrl").
-include("arweave_sync_download_limit.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        rate_limit,
        fractional_refills,
        fractional_capacity_and_wakeup,
        full_bucket_discards_fractional_credit
    ].

init_per_testcase(_Case, Config) ->
    arweave_sync_test_deps:setup(),
    Config.

end_per_testcase(_Case, _Config) ->
    arweave_sync_test_deps:cleanup().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc The byte budget refills, consumes and restores capacity with bounded
%% wakeup delays.
rate_limit(_Config) ->
    Started = arweave_sync_download_limit:do_refill(
        arweave_sync_download_limit:new(0), 1000, 1000
    ),
    ?assertEqual(1000.0, Started#rate.balance),
    ?assertEqual(1000, Started#rate.refill_ms),
    HalfRefilled = arweave_sync_download_limit:do_refill(
        Started#rate{balance = 200.0}, 1500, 1000
    ),
    ?assertEqual(700.0, HalfRefilled#rate.balance),
    ?assertEqual(
        1000.0,
        (arweave_sync_download_limit:do_refill(
            HalfRefilled, 2500, 1000
        ))#rate.balance
    ),
    %% A clock moving backward adds no capacity.
    ?assertEqual(
        700.0,
        (arweave_sync_download_limit:do_refill(
            HalfRefilled, 500, 1000
        ))#rate.balance
    ),
    ?assertEqual(
        infinity,
        (arweave_sync_download_limit:do_refill(
            HalfRefilled, 2000, infinity
        ))#rate.balance
    ),

    Exhausted = arweave_sync_download_limit:consume(
        #rate{balance = 1.0}, ?DATA_CHUNK_SIZE
    ),
    ?assertNot(arweave_sync_download_limit:has_capacity(Exhausted)),
    ?assertEqual(
        250.0,
        (arweave_sync_download_limit:do_restore(
            #rate{balance = 100.0}, 200, 250
        ))#rate.balance
    ),
    ?assertEqual(
        infinity,
        (arweave_sync_download_limit:do_restore(
            #rate{balance = infinity}, 200, 250
        ))#rate.balance
    ),

    ?assertEqual(
        1000,
        arweave_sync_download_limit:wakeup_delay(
            #rate{balance = 0.0}, ?DATA_CHUNK_SIZE, 10_000
        )
    ),
    ?assertEqual(
        2000,
        arweave_sync_download_limit:wakeup_delay(
            #rate{balance = -float(?DATA_CHUNK_SIZE)},
            ?DATA_CHUNK_SIZE,
            10_000
        )
    ),
    ?assertEqual(
        100,
        arweave_sync_download_limit:wakeup_delay(
            #rate{balance = 0.0},
            100 * ?DATA_CHUNK_SIZE,
            10_000
        )
    ),
    ?assertEqual(
        10_000,
        arweave_sync_download_limit:wakeup_delay(
            #rate{balance = 0.0}, 0, 10_000
        )
    ).

fractional_refills(_Config) ->
    %% One thousand 1-ms refills must earn the same capacity as one second,
    %% including at 1 B/s and rates with a fractional byte per millisecond.
    lists:foreach(fun(Limit) ->
        Started = arweave_sync_download_limit:do_refill(
            arweave_sync_download_limit:new(0), 0, Limit
        ),
        Empty = arweave_sync_download_limit:consume(Started, Limit),
        Frequent = lists:foldl(fun(NowMs, Rate) ->
            arweave_sync_download_limit:do_refill(Rate, NowMs, Limit)
        end, Empty, lists:seq(1, 1000)),
        Single = arweave_sync_download_limit:do_refill(Empty, 1000, Limit),
        %% One millionth of a byte permits accumulated floating-point roundoff.
        ?assert(abs(Single#rate.balance - Frequent#rate.balance) < 1.0e-6),
        ?assertEqual(Single#rate.refill_ms, Frequent#rate.refill_ms)
    end, [1, 99, 1001, ?MiB + 1]).

fractional_capacity_and_wakeup(_Config) ->
    Empty = #rate{balance = 0.0, refill_ms = 0},
    %% At 1 B/s, 500 ms earns half a byte and 1000 ms earns a whole byte.
    Half = arweave_sync_download_limit:do_refill(Empty, 500, 1),
    ?assertEqual(0.5, Half#rate.balance),
    ?assertNot(arweave_sync_download_limit:has_capacity(Half)),
    Whole = arweave_sync_download_limit:do_refill(Half, 1000, 1),
    ?assert(arweave_sync_download_limit:has_capacity(Whole)),
    %% A half-byte overdraft takes just over one second at one chunk/s;
    %% round up to 1001 integer milliseconds for the timer.
    ?assertEqual(1001, arweave_sync_download_limit:wakeup_delay(
        #rate{balance = -0.5}, ?DATA_CHUNK_SIZE, 10_000
    )).

full_bucket_discards_fractional_credit(_Config) ->
    Empty = #rate{balance = 0.0, refill_ms = 0},
    %% At 1 B/s, 1750 ms fills the bucket and discards the extra 3/4 byte.
    Full = arweave_sync_download_limit:do_refill(Empty, 1750, 1),
    Spent = arweave_sync_download_limit:consume(Full, 1),
    Next = arweave_sync_download_limit:do_refill(Spent, 2000, 1),
    ?assertEqual(0.25, Next#rate.balance),
    ?assertNot(arweave_sync_download_limit:has_capacity(Next)),
    %% Refunding a full byte after half a second must also discard the
    %% accumulated half byte; otherwise spending the refund earns it twice.
    Half = arweave_sync_download_limit:do_refill(Empty, 500, 1),
    Refunded = arweave_sync_download_limit:do_restore(Half, 1, 1),
    RefundedSpent = arweave_sync_download_limit:consume(Refunded, 1),
    AfterRefund = arweave_sync_download_limit:do_refill(
        RefundedSpent, 1000, 1
    ),
    ?assertEqual(0.5, AfterRefund#rate.balance),
    ?assertNot(arweave_sync_download_limit:has_capacity(AfterRefund)).
