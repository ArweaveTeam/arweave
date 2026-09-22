%%% @doc Fixture and deterministic-time runner for chunk-sync simulations.
%%%
%%% The runner installs arweave_sync_deps_sim behind the real sync pipeline,
%%% drives the pipeline on arweave_sim's clock, and exposes snapshots
%%% used by simulation scenarios.
-module(arweave_sync_sim).
-export([setup_fixture/0, teardown_fixture/0]).
-export([start_sim/1, update_world/1, run_for/1, advance_tick/0, flush_pipeline/0]).
-export([metric/2]).
-export([complete_write/3]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_sim/include/arweave_sim.hrl").

%% Workers and cache mirror the s1 test-rig config; the store count just
%% needs to be comfortably plural (sweep cost is per store).
%% Capacity for 7200 chunks, each reserving three 256 KiB representations.
-define(SIM_CACHE_SIZE_MIB, 5400).
%% The arweave_sync_scheduler tick, shrunk from the production 10s: every per-tick
%% dynamic (cap growth, EWMA folds, timeout rotations) is tick-denominated,
%% so a shorter tick only cuts the simulated seconds each tick drags in.
-define(SIM_TICK_MS, 1000).
%% Sub-steps per simulated second -- equivalently, the round trips a
%% fully-busy worker completes per second (one per sub-step).
-define(SUBSTEPS_PER_SECOND, (1000 div ?SIM_SUBSTEP_MS)).
%% Generous deadman against genuinely lost sleepers. Mainnet-sized footprints
%% can spend several wall-clock seconds computing between sleeps.
-define(WORKERS_ASLEEP_TIMEOUT_MS, 60_000).
%% Complete admitted writes at the next simulated substep.
-define(WRITE_LATENCY_MS, 1).

%% @doc Own the simulator's tables without starting the host node.
setup_fixture() ->
    %% Ingestion is internal; replace it only in the simulator fixture.
    %% Do not retain a mock history for every simulated chunk.
    ok = meck:new(arweave_sync_ingest, [passthrough, no_link, no_history]),
    ok = meck:expect(
        arweave_sync_ingest, store_fetched_chunk, fun store_fetched_chunk/6
    ),
    arweave_sim:create_world(),
    arweave_sync_deps:override_module(arweave_sync_deps_sim),
    %% Production-scale sweep windows and the short simulation scheduler
    %% tick are hot-path persistent_term overrides, not mocks.
    arweave_sync_cursor:override_query_range_step_size(?QUERY_RANGE_STEP_SIZE),
    arweave_sync_scheduler:override_tick_interval_ms(?SIM_TICK_MS),
    ok.

%% @doc Stop sync workers before dropping the clock and testcase-owned state.
teardown_fixture() ->
    stop_pipeline(),
    meck:unload(arweave_sync_ingest),
    arweave_sim:stop_clock(),
    arweave_sync_cursor:reset_all_overrides(),
    arweave_sync_scheduler:reset_all_overrides(),
    arweave_sync_deps:reset_all_overrides(),
    arweave_constants:internal_reset_partition_size_override(),
    arweave_constants:internal_reset_replica_2_9_overrides(),
    arweave_sim:delete_world(),
    arweave_entropy:internal_clear_cache(),
    ok.

%% @doc Configure one initial world and start the real sync pipeline.
start_sim(#sim_world{} = World) ->
    stop_pipeline(),
    ok = arweave_entropy:internal_clear_cache(),
    NodeConfig0 = maps:merge(default_node_config(), World#sim_world.node_config),
    %% Store count and size are fixed for every scenario, and the world may
    %% shift them off the partition grid; node_config controls all other
    %% production settings used by the simulation.
    NodeConfig = NodeConfig0#{
        [storage_modules] => arweave_sim:storage_modules(World)},
    ok = arweave_config:internal_force_config(NodeConfig),
    arweave_sim:start_clock(),
    CacheBytes = arweave_config:get([packing, cache_size]) * ?MiB,
    CacheLimit = max(1, CacheBytes div ar_chunk_cache:reservation_bytes()),
    arweave_sim:reset_world(World, CacheLimit,
        arweave_sync_scheduler:tick_interval_ms(),
        arweave_sync_cursor:query_range_step_size()),
    arweave_sync_discovery:reset_all_caches(),
    start_pipeline(),
    arweave_sim:start_scenario(),
    ok.

%% @doc Apply a runtime world change and expose its current peers to discovery.
update_world(#sim_world{} = World) ->
    PreviousWorld = arweave_sim:world_value(world),
    arweave_sim:update_world(World),
    maybe_collect_peers(PreviousWorld, World).

maybe_collect_peers(#sim_world{ peers = Peers }, #sim_world{ peers = Peers }) ->
    ok;
maybe_collect_peers(_PreviousWorld, _World) ->
    collect_peers().

collect_peers() ->
    arweave_sync_discovery:collect_peers().

start_pipeline() ->
    ok = arweave_sync:activate(),
    WeaveSize = arweave_sim:weave_size(),
    [begin
        ok = arweave_sync:set_weave_size(StoreID, WeaveSize),
        drain_sweeper(StoreID)
    end || StoreID <- arweave_sim:store_ids()],
    collect_peers(),
    [begin
        ok = arweave_sync:start_store(StoreID),
        drain_sweeper(StoreID)
    end || StoreID <- arweave_sim:store_ids()],
    %% Start simulated time from a stable mailbox boundary. This only waits for
    %% already-started workers to park at time zero; metadata remains unresolved
    %% and the sweepers discover it through their normal polling loop.
    wait_until_workers_sleep(),
    ok.

drain_sweeper(StoreID) ->
    pong = gen_server:call(arweave_sync_store_sweeper:name(StoreID), ping),
    ok.

%% @doc Model ingestion latency and writes through the real scheduler completion.
store_fetched_chunk(StoreID, Peer, Byte, _Proof, TaskRef, _CacheRef) ->
    arweave_sim:increment_chunk_cache_size(StoreID),
    ok = arweave_sim:wait_for_entropy(Peer, Byte),
    %% As in arweave_sync_ingest, the chunk is unpacked once its entropy is
    %% available; only the write remains.
    arweave_sync_scheduler:report_unpacked(TaskRef),
    {ok, _} = arweave_sim:apply_after(
        ?WRITE_LATENCY_MS,
        ?MODULE,
        complete_write,
        [StoreID, Byte, TaskRef]
    ),
    ok.

complete_write(StoreID, Byte, TaskRef) ->
    Second = arweave_sim:monotonic_ms() div 1000,
    case
        arweave_sim:admit_store_write(
            StoreID, arweave_sim:tick_index(), Second
        )
    of
        exhausted ->
            {ok, _} = arweave_sim:apply_after(
                1000,
                ?MODULE,
                complete_write,
                [StoreID, Byte, TaskRef]
            ),
            ok;
        available ->
            arweave_sim:write_completed(StoreID, Byte),
            arweave_sync_scheduler:report_write_completed(TaskRef),
            ok
    end.

stop_pipeline() ->
    arweave_sync:deactivate().

default_node_config() ->
    #{
        [packing, cache_size] => ?SIM_CACHE_SIZE_MIB,
        [disable_device_limit] => true
    }.

%% @doc Return the number of simulated seconds in one scheduler tick.
seconds_per_tick() ->
    arweave_sync_scheduler:tick_interval_ms() div 1000.

%% @doc Advance the simulation by whole scheduler ticks and return the measured change.
run_for(Ticks) when is_integer(Ticks), Ticks > 0 ->
    Before = arweave_sim:snapshot(),
    do_run_for(Ticks),
    #sim_measurement{
        duration_seconds = Ticks * seconds_per_tick(),
        before_snapshot = Before,
        after_snapshot = arweave_sim:snapshot()
    }.

do_run_for(0) ->
    ok;
do_run_for(Ticks) ->
    advance_tick(),
    do_run_for(Ticks - 1).

%% @doc Advance one scheduler tick and settle each worker wave.
advance_tick() ->
    lists:foreach(fun(_) -> advance_second() end,
        lists:seq(1, seconds_per_tick())).

advance_second() ->
    lists:foreach(
        fun(_) ->
            ok = arweave_sim:advance(?SIM_SUBSTEP_MS),
            wait_until_workers_sleep()
        end,
        lists:seq(1, ?SUBSTEPS_PER_SECOND)).

wait_until_workers_sleep() ->
    wait_until_workers_sleep(erlang:monotonic_time(millisecond)
        + ?WORKERS_ASLEEP_TIMEOUT_MS).

wait_until_workers_sleep(Deadline) ->
    flush_pipeline(),
    FetchInflight = arweave_sync_scheduler:inflight_count(),
    Sleeping = arweave_sim:sleeping(),
    DiscoveryInflight = case catch arweave_sync_discovery:inflight_count() of
        N when is_integer(N) -> N;
        _ -> 0
    end,
    case FetchInflight + DiscoveryInflight =:= Sleeping of
        true ->
            ok;
        false ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true ->
                    Rows = arweave_sim:pending_timers(),
                    Pids = [element(2, Msg) || {_, Msg, _} <- Rows,
                        is_tuple(Msg), element(1, Msg) =:= msg],
                    Stacks = [{Pid,
                        catch erlang:process_info(Pid, current_stacktrace)}
                        || Pid <- Pids, is_pid(Pid)],
                    error({workers_still_awake, FetchInflight, DiscoveryInflight,
                        Sleeping, Stacks});
                false ->
                    erlang:yield(),
                    wait_until_workers_sleep(Deadline)
            end
    end.

%% @doc Flush casts queued in the scheduler, generators, and discovery.
flush_pipeline() ->
    [catch gen_server:call(arweave_sync_store_sweeper:name(StoreID), ping)
        || StoreID <- arweave_sim:store_ids()],
    catch gen_server:call(arweave_sync_scheduler, ping),
    %% Force DOWN processing so metadata job membership converges with the
    %% simulated clock's sleeper count.
    catch arweave_sync_discovery:inflight_count(),
    catch gen_server:call(arweave_sync_scheduler, ping),
    [catch gen_server:call(arweave_sync_store_sweeper:name(StoreID), ping)
        || StoreID <- arweave_sim:store_ids()],
    catch gen_server:call(arweave_sync_scheduler, ping),
    ok.

%% @doc Return an interval metric, or one value from a map-valued metric.
metric({Metric, Key}, Measurement) ->
    maps:get(Key, metric(Metric, Measurement), 0);
metric(duration_seconds, #sim_measurement{ duration_seconds = Seconds }) ->
    Seconds;
metric(cps_by_peer, Measurement) ->
    Seconds = metric(duration_seconds, Measurement),
    maps:map(fun(_Peer, Count) -> Count / Seconds end,
        metric(served_by_peer, Measurement));
metric(served_by_peer, Measurement) ->
    map_counter_difference(#sim_snapshot.served_by_peer, Measurement);
metric(rejected_by_peer, Measurement) ->
    map_counter_difference(#sim_snapshot.rejected_by_peer, Measurement);
metric(timed_out_by_peer, Measurement) ->
    map_counter_difference(#sim_snapshot.timed_out_by_peer, Measurement);
metric(client_errors_by_peer, Measurement) ->
    map_counter_difference(#sim_snapshot.client_errors_by_peer, Measurement);
metric(chunk_interval_requests_by_peer, Measurement) ->
    map_counter_difference(
        #sim_snapshot.chunk_interval_requests_by_peer, Measurement);
metric(chunks_stored_by_store, Measurement) ->
    map_counter_difference(#sim_snapshot.chunks_stored_by_store, Measurement);
metric(chunks_stored_total, Measurement) ->
    lists:sum(maps:values(metric(chunks_stored_by_store, Measurement)));
metric(stored_cps, Measurement) ->
    metric(chunks_stored_total, Measurement)
        / metric(duration_seconds, Measurement);
metric(final_http_inflight_by_peer, #sim_measurement{
        after_snapshot = #sim_snapshot{ http_inflight_by_peer = HTTPInflight }
    }) ->
    HTTPInflight;
metric(final_chunk_interval_requests_by_peer, #sim_measurement{
        after_snapshot = #sim_snapshot{
            chunk_interval_requests_by_peer = ChunkIntervalRequests
        }
    }) ->
    ChunkIntervalRequests;
metric(final_chunk_interval_inflight_by_peer, #sim_measurement{
        after_snapshot = #sim_snapshot{
            chunk_interval_inflight_by_peer = ChunkIntervalInflight
        }
    }) ->
    ChunkIntervalInflight.

map_counter_difference(FieldIndex, #sim_measurement{
        before_snapshot = Before,
        after_snapshot = After
    }) ->
    map_difference(element(FieldIndex, Before), element(FieldIndex, After)).

map_difference(Before, After) ->
    maps:fold(
        fun(Key, BeforeValue, Difference) ->
            maps:update_with(Key,
                fun(AfterValue) -> AfterValue - BeforeValue end,
                -BeforeValue,
                Difference)
        end,
        After,
        Before).
