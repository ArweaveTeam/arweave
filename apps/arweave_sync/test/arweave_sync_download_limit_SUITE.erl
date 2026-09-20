-module(arweave_sync_download_limit_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("arweave/include/ar.hrl").
-include("arweave_sync_download_limit.hrl").
-import(arweave_sync_download_limit, [
    consume/2,
    do_refill/3,
    do_restore/3,
    has_capacity/1,
    new/1,
    wakeup_delay/3
]).

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        rate_limit
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
    Started = do_refill(new(0), 1000, 1000),
    ?assertEqual(1000, Started#rate.balance),
    ?assertEqual(1000, Started#rate.refill_ms),
    HalfRefilled = do_refill(
        Started#rate{balance = 200}, 1500, 1000
    ),
    ?assertEqual(700, HalfRefilled#rate.balance),
    ?assertEqual(
        1000,
        (do_refill(HalfRefilled, 2500, 1000))#rate.balance
    ),
    %% A clock moving backward adds no capacity.
    ?assertEqual(
        700,
        (do_refill(HalfRefilled, 500, 1000))#rate.balance
    ),
    ?assertEqual(
        infinity,
        (do_refill(HalfRefilled, 2000, infinity))#rate.balance
    ),

    Exhausted = consume(#rate{balance = 1}, ?DATA_CHUNK_SIZE),
    ?assertNot(has_capacity(Exhausted)),
    ?assertEqual(
        250,
        (do_restore(#rate{balance = 100}, 200, 250))#rate.balance
    ),
    ?assertEqual(
        infinity,
        (do_restore(#rate{balance = infinity}, 200, 250))#rate.balance
    ),

    ?assertEqual(
        1000,
        wakeup_delay(#rate{balance = 0}, ?DATA_CHUNK_SIZE, 10_000)
    ),
    ?assertEqual(
        2000,
        wakeup_delay(
            #rate{balance = -?DATA_CHUNK_SIZE},
            ?DATA_CHUNK_SIZE,
            10_000
        )
    ),
    ?assertEqual(
        100,
        wakeup_delay(
            #rate{balance = 0},
            100 * ?DATA_CHUNK_SIZE,
            10_000
        )
    ),
    ?assertEqual(
        10_000,
        wakeup_delay(#rate{balance = 0}, 0, 10_000)
    ).
