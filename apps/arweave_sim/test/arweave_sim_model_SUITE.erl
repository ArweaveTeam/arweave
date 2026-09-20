-module(arweave_sim_model_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sim/include/arweave_sim.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        queued_rejected_and_rollover_admission,
        get_chunk_binary_outcome_precedence_and_cleanup,
        cache_write_and_snapshot,
        local_data_layout,
        discovery_configuration,
        driver_controls_timing_and_ranges
    ].

init_per_suite(Config) ->
    {ok, Started} = application:ensure_all_started(arweave_storage),
    [{started_apps, Started} | Config].

end_per_suite(Config) ->
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Peer, link and remote-store budgets queue or reject excess work and
%% reset each second.
queued_rejected_and_rollover_admission(_Config) ->
    with_world(fun() ->
        %% A one-request budget makes the first admission and first overage exact.
        Capacity = 1,
        StoreSize = ?SIM_STORE_SIZE,
        LimitedPeer = limited_peer,
        QueuedPeer = queued_peer,
        World = #sim_world{
            peers = #{
                LimitedPeer => #sim_peer{max_serve_cps = Capacity, limited = true},
                QueuedPeer => #sim_peer{max_serve_cps = Capacity}
            },
            link_capacity_cps = Capacity,
            remote_store_cps = Capacity
        },
        reset_world(World, 1),
        %% Simulated time begins in second zero; the next value tests rollover.
        Second = 0,
        NextSecond = Second + 1,
        ?assertEqual(admitted, arweave_sim_world:admit_peer(LimitedPeer, Second)),
        ?assertEqual(reject, arweave_sim_world:admit_peer(LimitedPeer, Second)),
        ?assertEqual(admitted, arweave_sim_world:admit_peer(LimitedPeer, NextSecond)),
        ?assertEqual(admitted, arweave_sim_world:admit_peer(QueuedPeer, Second)),
        ?assertEqual(wait, arweave_sim_world:admit_peer(QueuedPeer, Second)),
        ?assertEqual(admitted, arweave_sim_world:admit_peer(QueuedPeer, NextSecond)),
        ?assertEqual(admitted, arweave_sim_world:admit_link(Second)),
        ?assertEqual(wait, arweave_sim_world:admit_link(Second)),
        ?assertEqual(admitted, arweave_sim_world:admit_link(NextSecond)),
        ?assertEqual(admitted, arweave_sim_world:admit_remote_store(LimitedPeer, 0, Second)),
        ?assertEqual(wait, arweave_sim_world:admit_remote_store(LimitedPeer, 0, Second)),
        ?assertEqual(
            admitted,
            arweave_sim_world:admit_remote_store(LimitedPeer, StoreSize, Second)
        ),
        ?assertEqual(
            admitted,
            arweave_sim_world:admit_remote_store(LimitedPeer, 0, NextSecond)
        ),
        Snapshot = arweave_sim_world:snapshot(),
        ?assertEqual(
            Capacity,
            maps:get(LimitedPeer, Snapshot#sim_snapshot.rejected_by_peer)
        )
    end).

%% @doc Connection limits take precedence over timeouts and both outcomes clear
%% inflight counts.
get_chunk_binary_outcome_precedence_and_cleanup(_Config) ->
    with_world(fun() ->
        Name = peer,
        %% Zero allowed connections makes the first in-flight fetch overflow.
        ConnectionDepth = 0,
        AlwaysTimeout = fun(_Tick, _RequestSequence) -> timeout end,
        reset_world(
            #sim_world{
                peers = #{
                    Name => #sim_peer{
                        max_serve_cps = 1,
                        %% One substep gives each request a deterministic wakeup.
                        latency_ms = ?SIM_SUBSTEP_MS,
                        http_inflight_limit = ConnectionDepth,
                        failure_policy = AlwaysTimeout
                    }
                }
            },
            1
        ),
        %% One second exceeds the one-substep modeled response.
        TimeoutMS = 1000,
        ?assertEqual(
            {error, client_error},
            fetch_chunk(Name, 0, TimeoutMS)
        ),
        Snapshot1 = arweave_sim_world:snapshot(),
        ?assertEqual(
            1,
            maps:get(Name, Snapshot1#sim_snapshot.client_errors_by_peer)
        ),
        ?assertEqual(
            0,
            maps:get(Name, Snapshot1#sim_snapshot.timed_out_by_peer)
        ),
        ?assertEqual(
            0,
            maps:get(Name, Snapshot1#sim_snapshot.http_inflight_by_peer)
        ),
        Peer = arweave_sim_world:get({peer, Name}),
        arweave_sim_world:set_peer(Name, Peer#sim_peer{http_inflight_limit = infinity}),
        ?assertEqual(
            {error, timeout},
            fetch_chunk(Name, 0, TimeoutMS)
        ),
        Snapshot2 = arweave_sim_world:snapshot(),
        ?assertEqual(
            1,
            maps:get(Name, Snapshot2#sim_snapshot.timed_out_by_peer)
        ),
        ?assertEqual(
            0,
            maps:get(Name, Snapshot2#sim_snapshot.http_inflight_by_peer)
        )
    end).

%% @doc Completed simulated writes release cache capacity and update stored
%% ranges and counters.
cache_write_and_snapshot(_Config) ->
    with_world(fun() ->
        Store = store,
        %% A one-chunk cache reaches full exactly after one admission.
        CacheLimit = 1,
        World = #sim_world{},
        reset_world(World, CacheLimit),
        ?assertEqual(CacheLimit, arweave_sim_world:increment_chunk_cache_size(Store)),
        ?assertEqual(CacheLimit, arweave_sim_world:chunk_cache_size()),
        ?assertEqual(CacheLimit, arweave_sim_world:chunk_cache_size(Store)),
        ?assert(arweave_sim_world:is_chunk_cache_full()),
        arweave_sim_world:update_world(World#sim_world{store_write_cps = 0}),
        ?assertEqual(exhausted, arweave_sim_world:admit_store_write(Store, 0, 0)),
        arweave_sim_world:update_world(World#sim_world{store_write_cps = 1}),
        %% Consumption resets on the next simulated second, not on reconfiguration.
        NextSecond = 1,
        ?assertEqual(available, arweave_sim_world:admit_store_write(Store, 0, NextSecond)),
        ?assertEqual(exhausted, arweave_sim_world:admit_store_write(Store, 0, NextSecond)),
        arweave_sim_world:write_completed(Store, 0),
        ?assertEqual(0, arweave_sim_world:chunk_cache_size()),
        ?assertEqual(0, arweave_sim_world:chunk_cache_size(Store)),
        ?assertNot(arweave_sim_world:is_chunk_cache_full()),
        Intervals = ar_intervals:from_list([
            {2 * ?DATA_CHUNK_SIZE, 0}
        ]),
        ?assertEqual(
            [{2 * ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE}],
            ar_intervals:to_list(
                arweave_sim_world:exclude_stored_chunks(byte, Store, Intervals)
            )
        ),
        FootprintSize = arweave_constants:get_sub_chunks_per_replica_2_9_entropy(),
        ?assertEqual(
            [{FootprintSize, 1}],
            ar_intervals:to_list(
                arweave_sim_world:unsynced_footprint_intervals(0, 0, Store)
            )
        ),
        ?assertEqual(
            {?DATA_CHUNK_SIZE, 0},
            arweave_sim_world:get_next_synced_interval(Store, 0, 2 * ?DATA_CHUNK_SIZE)
        ),
        ?assertEqual(
            not_found,
            arweave_sim_world:get_next_synced_interval(
                Store, ?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE
            )
        ),
        %% Add a non-adjacent chunk after the first exclusion. A second query
        %% must observe the new write while preserving the exact one-chunk hole
        %% between the two writes.
        ?assertEqual(CacheLimit, arweave_sim_world:increment_chunk_cache_size(Store)),
        arweave_sim_world:write_completed(Store, 2 * ?DATA_CHUNK_SIZE),
        ThreeChunkIntervals = ar_intervals:from_list([
            {3 * ?DATA_CHUNK_SIZE, 0}
        ]),
        ?assertEqual(
            [{2 * ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE}],
            ar_intervals:to_list(
                arweave_sim_world:exclude_stored_chunks(byte, Store, ThreeChunkIntervals)
            )
        ),
        ?assertEqual(
            [{FootprintSize, 2}],
            ar_intervals:to_list(
                arweave_sim_world:unsynced_footprint_intervals(0, 0, Store)
            )
        ),
        Snapshot = arweave_sim_world:snapshot(),
        %% The original and non-adjacent writes are the two unique completions.
        ExpectedStoredChunks = 2,
        ?assertEqual(
            ExpectedStoredChunks,
            maps:get(Store, Snapshot#sim_snapshot.chunks_stored_by_store)
        )
    end).

%% @doc Missing-range queries reflect fresh and fragmented local data layouts.
local_data_layout(_Config) ->
    with_world(fun() ->
        World = #sim_world{},
        reset_world(World, 1),
        %% A five-chunk query is one need for a fresh store. The fragmented
        %% layout retains every other chunk: the first, third, and fifth.
        Start = 2 * ?DATA_CHUNK_SIZE,
        End = Start + 5 * ?DATA_CHUNK_SIZE,
        ?assertEqual(
            [{End, Start}],
            ar_intervals:to_list(
                arweave_sim_world:unsynced_intervals(Start, End, store)
            )
        ),
        arweave_sim_world:update_world(World#sim_world{local_data_layout = fragmented}),
        ?assertEqual(
            [
                {Start + ?DATA_CHUNK_SIZE, Start},
                {Start + 3 * ?DATA_CHUNK_SIZE, Start + 2 * ?DATA_CHUNK_SIZE},
                {End, End - ?DATA_CHUNK_SIZE}
            ],
            ar_intervals:to_list(
                arweave_sim_world:unsynced_intervals(Start, End, store)
            )
        )
    end).

%% @doc Peer advertisements honor configured sync modes, ranges and non-serving
%% membership.
discovery_configuration(_Config) ->
    with_world(fun() ->
        Peer = peer,
        %% Two known non-serving peers distinguish peer membership from service.
        GossipPeers = [gossip_peer_1, gossip_peer_2],
        WorldIntervals = [{?DATA_CHUNK_SIZE, 0}],
        Peers = maps:merge(
            #{
                Peer => #sim_peer{
                    max_serve_cps = 1,
                    release = 101,
                    sync_kinds = [footprint],
                    sync_availability = {intervals, WorldIntervals}
                }
            },
            maps:from_list([
                {P, #sim_peer{sync_kinds = []}}
             || P <- GossipPeers
            ])
        ),
        reset_world(
            #sim_world{
                peers = Peers
            },
            1
        ),
        ?assertNot(arweave_sim_world:peer_sync_kind_enabled(Peer, byte)),
        ?assert(arweave_sim_world:peer_sync_kind_enabled(Peer, footprint)),
        ?assertEqual(
            WorldIntervals,
            ar_intervals:to_list(arweave_sim_world:peer_sync_intervals(Peer))
        ),
        ?assertEqual(length(GossipPeers) + 1, length(arweave_sim_world:get(peers))),
        ?assertEqual(101, (arweave_sim_world:get({peer, Peer}))#sim_peer.release),
        [GossipPeer | _] = arweave_sim_world:get(peers) -- [Peer],
        ?assertEqual([], (arweave_sim_world:get({peer, GossipPeer}))#sim_peer.sync_kinds)
    end).

%% @doc The simulation driver controls tick cadence, query ranges and storage
%% metadata.
driver_controls_timing_and_ranges(_Config) ->
    with_world(fun() ->
        [{StoreID, StoreRange} | _] = arweave_sim:store_ranges(),
        [Module | _] = arweave_sim:storage_modules(),
        #store_info{id = ExpectedID, effective_range = ExpectedRange} =
            arweave_storage:store_info(Module),
        ?assertEqual(
            {ExpectedID, ExpectedRange},
            {StoreID, StoreRange}
        ),
        ?assertEqual(?SIM_STORES, length(arweave_sim:store_ranges())),
        %% Two-second ticks distinguish the driver's cadence from one second.
        TickMS = 2000,
        %% Four chunks make the model's eight-range advertisement exact.
        QueryRangeBytes = 4 * ?DATA_CHUNK_SIZE,
        Peer = peer,
        World = #sim_world{
            peers = #{
                Peer => #sim_peer{
                    sync_availability = {stores, [StoreID]}
                }
            }
        },
        %% Metadata and clock checks need no chunk-cache capacity.
        arweave_sim:reset_world(World, 0, TickMS, QueryRangeBytes),
        arweave_sim:start_scenario(),
        arweave_sim:advance(TickMS - 1),
        ?assertEqual(0, arweave_sim:tick_index()),
        arweave_sim:advance(1),
        ?assertEqual(1, arweave_sim:tick_index()),
        {Start, _End} = StoreRange,
        ?assertEqual(
            [{Start + 8 * QueryRangeBytes, Start}],
            ar_intervals:to_list(arweave_sim:peer_sync_intervals(Peer))
        )
    end).

%%====================================================================
%% Helpers
%%====================================================================

with_world(Fun) ->
    arweave_sim_world:create(),
    arweave_sim:start_clock(),
    try
        Fun()
    after
        arweave_sim:stop_clock(),
        arweave_sim_world:delete()
    end.

%% @doc Complete a one-substep fetch without relying on wall-clock latency.
fetch_chunk(Peer, Offset, TimeoutMS) ->
    Parent = self(),
    Worker = spawn_link(fun() ->
        Result = arweave_sim_world:get_chunk_binary(Peer, Offset, TimeoutMS),
        Parent ! {self(), Result}
    end),
    try
        case
            ar_test_await:until(simulated_fetch_waiting, fun() ->
                arweave_sim:sleeping() =:= 1
            end)
        of
            ok ->
                arweave_sim:advance(?SIM_SUBSTEP_MS),
                receive
                    {Worker, Result} -> Result
                end;
            Error ->
                Error
        end
    after
        unlink(Worker),
        exit(Worker, kill)
    end.

%% @doc Reset a unit-test world with one-second ticks and 1 GB query ranges.
reset_world(World, CacheLimit) ->
    arweave_sim_world:reset(World, CacheLimit, 1000, 1_000_000_000).
