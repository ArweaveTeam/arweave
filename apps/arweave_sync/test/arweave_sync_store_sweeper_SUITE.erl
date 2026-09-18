-module(arweave_sync_store_sweeper_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("stdlib/include/assert.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave_sync/include/arweave_sync_store_sweeper.hrl").
-import(arweave_sync_store_sweeper, [
    byte_range_end/2,
    can_sweep/1,
    completed_range_delay/2,
    do_enqueue_sweep_range/3,
    frontier_offset/2,
    handle_cast/2,
    initialize_sweep/1,
    process_next_sweep_range/2,
    process_sweep_queues/1,
    process_sweep_range/2,
    sweep_queue/2
]).

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        frontier_offset,
        byte_range_end_aligns_padded_store_start,
        fresh_sweep_range_waits,
        warmed_sweep_range_proceeds,
        empty_sweep_range_has_no_metadata_wait,
        completed_nonempty_range_uses_cadence,
        waiting_metadata_allows_other_mode_progress,
        sync_bounds_decrease_keeps_sweep,
        sweep_readiness
    ].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_sync),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

init_per_testcase(_, Config) ->
    arweave_sync_deps:override_module(arweave_sync_deps_mainnet),
    Config.

end_per_testcase(_, _) ->
    arweave_sync_deps:reset_all_overrides().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc The sweep frontier skips synced prefixes without moving behind its
%% current offset.
frontier_offset(_Config) ->
    ?assertEqual(100, frontier_offset(100, ar_intervals:new())),
    Intervals = ar_intervals:from_list([{300, 250}, {500, 400}]),
    ?assertEqual(250, frontier_offset(100, Intervals)),
    ?assertEqual(260, frontier_offset(260, Intervals)),
    ?assertEqual(450, frontier_offset(450, Intervals)).

%% @doc Byte queries align to the global range grid even when the store starts
%% off-grid.
byte_range_end_aligns_padded_store_start(_Config) ->
    Step = arweave_sync_cursor:query_range_step_size(),
    %% A start 122,880 bytes before the grid must stop at the next boundary,
    %% not one full step after the unaligned start.
    Offset = 10 * Step - 122_880,
    ?assertEqual(10 * Step, byte_range_end(Offset, 12 * Step)),
    ?assertEqual(11 * Step, byte_range_end(10 * Step, 12 * Step)).

%% @doc A newly requested range waits for its metadata warmup window.
fresh_sweep_range_waits(_Config) ->
    SweepRange = #sweep_range{requested_at = ar_timer:monotonic_ms()},
    State = #state{},
    {{blocked, WarmDelay}, State} = process_sweep_range(SweepRange, State),
    ?assert(WarmDelay > 0),
    ?assert(WarmDelay =< ?SWEEP_RANGE_WARM_WAIT_MS).

%% @doc An aged range can complete and advance the cursor once no missing chunks
%% remain.
warmed_sweep_range_proceeds(_Config) ->
    arweave_sync_test_util:with_mocks(
        [
            {arweave_sync_discovery, get_peers_for_offset, fun(0) -> [] end},
            {arweave_sync_discovery, warm_peer_ranges, fun(test_store, [], 0) ->
                ok
            end},
            {arweave_sync_discovery, cached_peer_ranges, fun(
                test_store, [], 0, 0, 100
            ) ->
                {[], ok}
            end},
            {ar_peers, pick_peers, fun([], _Limit) -> [] end},
            {arweave_storage, get_intervals, fun(
                unsynced, 0, 100, any_packing, {ar_data_sync, byte}, test_store
            ) ->
                ar_intervals:new()
            end},
            {ar_tx_blacklist, get_blacklisted_intervals, fun(0, 100) ->
                ar_intervals:new()
            end},
            {ar_footprint_limit, get, fun(test_store) ->
                arweave_constants:get_replica_2_9_footprints_per_partition()
            end}
        ],
        fun() ->
            Cursor = arweave_sync_cursor:new(0, 100),
            UnsyncedRange = #unsynced_range{
                kind = byte,
                query_offset = 0,
                intervals = ar_intervals:from_list([{100, 0}]),
                range_start = 0,
                range_end = 100,
                advance = 100
            },
            %% Ten seconds is a minimum age, independent of metadata completeness.
            RequestedAt = ar_timer:monotonic_ms() - ?SWEEP_RANGE_WARM_WAIT_MS,
            SweepRange = #sweep_range{
                mode = byte,
                offset = 0,
                next_offset = 100,
                unsynced_range = UnsyncedRange,
                requested_at = RequestedAt
            },
            State = #state{
                store_id = test_store,
                weave_size = 100,
                disk_pool_threshold = 100,
                cursor = Cursor,
                readahead_cursor = Cursor,
                sweep_queues = #{
                    byte => queue:in(SweepRange, queue:new()),
                    footprint => queue:new()
                }
            },
            {ok, State2} = process_sweep_range(SweepRange, State),
            ?assert(queue:is_empty(sweep_queue(byte, State2))),
            ?assertEqual(
                100, arweave_sync_cursor:current(byte, State2#state.cursor)
            )
        end
    ).

%% @doc An empty range advances immediately without waiting for metadata warmup.
empty_sweep_range_has_no_metadata_wait(_Config) ->
    Now = ar_timer:monotonic_ms(),
    Cursor = arweave_sync_cursor:new(0, ?DATA_CHUNK_SIZE),
    Empty = #sweep_range{
        mode = byte,
        next_offset = ?DATA_CHUNK_SIZE,
        requested_at = Now
    },
    State = #state{cursor = Cursor, readahead_cursor = Cursor},
    State2 = do_enqueue_sweep_range(Empty, Cursor, State),
    {ok, State3} = process_next_sweep_range(byte, State2),
    ?assert(queue:is_empty(sweep_queue(byte, State3))).

%% @doc Completing a nonempty range applies metadata cadence while empty ranges
%% skip the delay.
completed_nonempty_range_uses_cadence(_Config) ->
    arweave_sync_test_util:with_mocks(
        [
            {arweave_sync_discovery, get_peers_for_offset, fun(100) ->
                [test_peer]
            end},
            {arweave_sync_discovery, cached_peer_ranges, fun(
                test_store, [test_peer], 100, 100, 200
            ) ->
                {[], cache_miss}
            end},
            {arweave_throttling, is_throttled, fun(test_peer, ?CHUNK_PATH) ->
                false
            end},
            {ar_peers, pick_peers, fun([test_peer], _Limit) -> [test_peer] end}
        ],
        fun() ->
            CompletedRange = #sweep_range{
                mode = footprint,
                offset = 0,
                unsynced_range = occupied
            },
            PendingUnsyncedRange = #unsynced_range{
                query_offset = 100,
                range_start = 100,
                range_end = 200
            },
            PendingRange = #sweep_range{
                mode = footprint,
                offset = 100,
                unsynced_range = PendingUnsyncedRange
            },
            Before = #state{
                store_id = test_store,
                sweep_queues = #{
                    byte => queue:new(),
                    footprint => queue:from_list([CompletedRange, PendingRange])
                }
            },
            After = Before#state{
                sweep_queues = #{
                    byte => queue:new(),
                    footprint => queue:from_list([PendingRange])
                }
            },
            %% One second lets the pending serial metadata request progress.
            ?assertEqual(
                ?SWEEP_RANGE_CADENCE_MS,
                completed_range_delay(Before, After)
            ),
            EmptyBefore = Before#state{
                sweep_queues = #{
                    byte => queue:new(),
                    footprint => queue:from_list([
                        CompletedRange#sweep_range{unsynced_range = none},
                        PendingRange
                    ])
                }
            },
            ?assertEqual(0, completed_range_delay(EmptyBefore, After))
        end
    ).

%% @doc A byte range waiting for metadata does not stop the footprint cursor
%% from advancing.
waiting_metadata_allows_other_mode_progress(_Config) ->
    arweave_sync_test_util:with_mocks(
        [
            {arweave_sync_discovery, get_peers_for_offset, fun(0) ->
                [test_peer]
            end},
            {arweave_sync_discovery, warm_peer_ranges, fun(
                test_store, [test_peer], 0
            ) ->
                ok
            end},
            {arweave_sync_discovery, cached_peer_ranges, fun(
                test_store, [test_peer], 0, 0, 100
            ) ->
                {[], cache_miss}
            end},
            {arweave_throttling, is_throttled, fun(test_peer, ?CHUNK_PATH) ->
                false
            end},
            {ar_peers, pick_peers, fun([test_peer], _Limit) -> [test_peer] end}
        ],
        fun() ->
            Cursor = arweave_sync_cursor:new(0, 100),
            UnsyncedRange = #unsynced_range{
                kind = byte,
                query_offset = 0,
                intervals = ar_intervals:from_list([{100, 0}]),
                range_start = 0,
                range_end = 100,
                advance = 100
            },
            ByteSweepRange = #sweep_range{
                mode = byte,
                offset = 0,
                next_offset = 100,
                unsynced_range = UnsyncedRange,
                requested_at = ar_timer:monotonic_ms()
            },
            FootprintSweepRange = #sweep_range{
                mode = footprint,
                offset = 0,
                next_offset = 100,
                requested_at = ar_timer:monotonic_ms()
            },
            State = #state{
                store_id = test_store,
                weave_size = 100,
                disk_pool_threshold = 100,
                cursor = Cursor,
                readahead_cursor = set_test_cursors(Cursor, 100, 100),
                sweep_queues = #{
                    byte => queue:in(ByteSweepRange, queue:new()),
                    footprint => queue:in(FootprintSweepRange, queue:new())
                }
            },
            {ok, State2} = process_sweep_queues(State),
            ?assertEqual(
                0, arweave_sync_cursor:current(byte, State2#state.cursor)
            ),
            ?assertEqual(
                100,
                arweave_sync_cursor:current(footprint, State2#state.cursor)
            ),
            ?assertEqual(1, queue:len(sweep_queue(byte, State2))),
            ?assert(queue:is_empty(sweep_queue(footprint, State2)))
        end
    ).

%% @doc Reduced live bounds update sweep limits without resetting existing
%% cursors.
sync_bounds_decrease_keeps_sweep(_Config) ->
    %% Half a chunk makes the footprint bound observably distinct from the
    %% one-chunk weave bound supplied by the cast.
    DiskPoolThreshold = ?DATA_CHUNK_SIZE div 2,
    arweave_sync_test_util:with_mocks(
        [
            {ar_disk_pool, get_threshold, fun() -> DiskPoolThreshold end}
        ],
        fun() ->
            Cursor = arweave_sync_cursor:set(
                footprint,
                ?DATA_CHUNK_SIZE,
                arweave_sync_cursor:set(
                    byte,
                    ?DATA_CHUNK_SIZE,
                    arweave_sync_cursor:new(0, 2 * ?DATA_CHUNK_SIZE)
                )
            ),
            State = #state{
                store_id = test_store,
                weave_size = 2 * ?DATA_CHUNK_SIZE,
                disk_pool_threshold = 2 * ?DATA_CHUNK_SIZE,
                cursor = Cursor
            },
            {noreply, State2} = handle_cast(
                {set_weave_size, ?DATA_CHUNK_SIZE}, State
            ),
            ?assertEqual(?DATA_CHUNK_SIZE, State2#state.weave_size),
            ?assertEqual(DiskPoolThreshold, State2#state.disk_pool_threshold),
            ?assertEqual(Cursor, State2#state.cursor),
            ?assertEqual(
                ?DATA_CHUNK_SIZE,
                arweave_sync_cursor:live_end(
                    byte,
                    State2#state.cursor,
                    State2#state.weave_size,
                    State2#state.disk_pool_threshold
                )
            ),
            ?assertEqual(
                DiskPoolThreshold,
                arweave_sync_cursor:live_end(
                    footprint,
                    State2#state.cursor,
                    State2#state.weave_size,
                    State2#state.disk_pool_threshold
                )
            )
        end
    ).

%% can_sweep gates range generation on disk space and the tip.
sweep_readiness(_Config) ->
    arweave_sync_test_util:with_mocks(
        [
            {ar_node, is_joined, fun() -> true end},
            {ar_data_sync, is_footprint_record_initialized, fun(_) -> true end},
            {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end}
        ],
        fun() ->
            InitialState = #state{
                store_id = store1,
                range_start = 0,
                range_end = 1000,
                weave_size = 1000,
                disk_pool_threshold = 1000
            },
            %% Cursor initialization starts both traversals at the module start.
            StartedState = initialize_sweep(InitialState),
            Cursor = StartedState#state.cursor,
            ?assertEqual(0, arweave_sync_cursor:current(byte, Cursor)),
            ?assertEqual(0, arweave_sync_cursor:current(footprint, Cursor)),
            ?assertEqual(Cursor, StartedState#state.readahead_cursor),
            ?assertEqual(ready, can_sweep(StartedState)),
            %% A module starting at the live end has no work in this sweep.
            ?assertEqual(
                complete,
                can_sweep(
                    initialize_sweep(
                        InitialState#state{range_start = 1000}
                    )
                )
            ),
            %% Both live bounds must be known before a sweep is ready.
            ?assertEqual(
                {blocked, ?NODE_JOIN_RETRY_DELAY_MS},
                can_sweep(StartedState#state{weave_size = undefined})
            ),
            ?assertEqual(
                {blocked, ?NODE_JOIN_RETRY_DELAY_MS},
                can_sweep(StartedState#state{disk_pool_threshold = undefined})
            ),
            %% Joining and footprint migration completion gate the initialized sweep.
            meck:expect(ar_node, is_joined, fun() -> false end),
            ?assertEqual(
                {blocked, ?NODE_JOIN_RETRY_DELAY_MS}, can_sweep(StartedState)
            ),
            meck:expect(ar_node, is_joined, fun() -> true end),
            meck:expect(
                ar_data_sync,
                is_footprint_record_initialized,
                fun(_) -> false end
            ),
            ?assertEqual(
                {blocked, ?NODE_JOIN_RETRY_DELAY_MS}, can_sweep(StartedState)
            ),
            meck:expect(
                ar_data_sync,
                is_footprint_record_initialized,
                fun(_) -> true end
            ),

            State = StartedState,
            %% Disk ok and at least one cursor below the tip -> ready.
            ?assertEqual(ready, can_sweep(State)),
            %% Both cursors reached their live bounds -> sweep done.
            CompleteState = State#state{
                cursor = set_test_cursors(Cursor, 1000, 1000)
            },
            ?assertEqual(complete, can_sweep(CompleteState)),
            %% One cursor can finish early while the other keeps the sweep active.
            ActiveState = State#state{
                cursor = set_test_cursors(Cursor, 1000, 0)
            },
            ?assertEqual(ready, can_sweep(ActiveState)),
            %% The footprint cursor is complete at the disk-pool threshold even when the
            %% storage module range extends further.
            ThresholdState = State#state{
                disk_pool_threshold = 500,
                cursor = set_test_cursors(Cursor, 1000, 500)
            },
            ?assertEqual(complete, can_sweep(ThresholdState)),
            %% Both live bounds below their cursors -> sweep done.
            EmptyState = State#state{weave_size = 0, disk_pool_threshold = 0},
            ?assertEqual(complete, can_sweep(EmptyState)),
            %% Disk state gates need identification.
            meck:expect(ar_data_sync, is_disk_space_sufficient, fun(_) ->
                false
            end),
            ?assertEqual({blocked, 30_000}, can_sweep(State)),
            meck:expect(ar_data_sync, is_disk_space_sufficient, fun(_) ->
                not_initialized
            end),
            ?assertEqual({blocked, 1_000}, can_sweep(State))
        end
    ).

%%====================================================================
%% Helpers
%%====================================================================

set_test_cursors(Cursor, Byte, Footprint) ->
    arweave_sync_cursor:set(
        footprint,
        Footprint,
        arweave_sync_cursor:set(byte, Byte, Cursor)
    ).
