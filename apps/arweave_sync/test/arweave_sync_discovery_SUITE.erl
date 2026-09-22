-module(arweave_sync_discovery_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("stdlib/include/assert.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include_lib("arweave_sync/include/arweave_sync_discovery.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        enqueue_job_deduplicates_pending_and_inflight,
        enqueue_job_rejects_untracked_peer,
        enqueue_job_keeps_distinct_pending_locations,
        enqueue_job_discards_when_pending_limit_reached,
        enqueue_job_guarantees_late_store_without_eviction,
        enqueue_job_uses_global_capacity_after_store_guarantee,
        finish_job_updates_matching_kind,
        add_peer_enqueues_one_sync_bucket_job,
        enqueue_sync_bucket_jobs_deduplicates_active_peers,
        remove_jobs_keeps_other_peers,
        removed_peer_job_results_are_ignored,
        take_next_job_prefers_less_loaded_peer,
        take_next_job_limits_byte_peer_store_mode,
        take_next_job_allows_bounded_footprint_concurrency,
        take_next_job_prefers_recently_requested_location,
        start_jobs_respects_inflight_limit,
        get_peers_for_offset_unions_sync_bucket_sources,
        cached_peer_ranges_includes_byte_and_footprint_ranges,
        get_chunk_intervals_limits_byte_metadata_to_requested_range,
        irrelevant_peer_has_no_cache_miss_without_detail_metadata,
        cached_empty_metadata_is_ok,
        warming_schedules_miss_but_cached_read_has_no_side_effect,
        warming_calls_are_backpressured,
        stale_interval_rows_remain_usable,
        warming_stale_metadata_requests_refresh,
        byte_share_change_marks_chunk_intervals_stale,
        footprint_share_change_marks_chunk_intervals_stale,
        expired_sync_buckets_delete_chunk_intervals,
        complete_chunk_interval_range_is_paginated,
        failed_chunk_interval_page_aborts_fetch,
        legacy_chunk_interval_pages_are_cut_to_query_range,
        chunk_interval_pagination_is_bounded,
        refresh_chunk_intervals_reuses_cache
    ].

init_per_suite(Config) ->
    {ok, Started} = application:ensure_all_started(arweave_sync),
    [{started_apps, Started} | Config].

end_per_suite(Config) ->
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ),
    ok.

init_per_testcase(_Case, Config) ->
    arweave_sync_deps:override_module(?MODULE),
    arweave_sync_discovery:reset_all_caches(),
    {ok, PID} = arweave_sync_discovery:start_link(),
    [{discovery, PID} | Config].

end_per_testcase(_Case, Config) ->
    gen_server:stop(proplists:get_value(discovery, Config)),
    arweave_sync_deps:reset_all_overrides().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Repeated requests refresh pending demand without duplicating queued or
%% inflight jobs.
enqueue_job_deduplicates_pending_and_inflight(_Config) ->
    Job = chunk_interval_job_for_test(peer, store, 0),
    TrackedPeers = sets:from_list([Job#discovery_job.peer]),
    State = #state{tracked_peers = TrackedPeers},
    State2 = arweave_sync_discovery:enqueue_job(Job, State),
    PendingJobs = maps:get(chunk_interval, State2#state.jobs),
    ?assertEqual(1, map_size(PendingJobs#discovery_jobs.pending)),
    State3 = arweave_sync_discovery:enqueue_job(Job, State2),
    RefreshedJobs = maps:get(chunk_interval, State3#state.jobs),
    ?assertEqual(1, map_size(RefreshedJobs#discovery_jobs.pending)),
    RefreshedJob = maps:get(
        Job#discovery_job.key,
        RefreshedJobs#discovery_jobs.pending
    ),
    ?assertNotEqual(undefined, RefreshedJob#discovery_job.requested_at),
    {ok, RefreshedJob, TakenJobs} = arweave_sync_discovery:take_next_job(
        RefreshedJobs,
        #{},
        #{},
        arweave_sync_discovery:inflight_peer_store_mode_counts(RefreshedJobs)
    ),
    ?assertEqual(0, map_size(TakenJobs#discovery_jobs.pending)),
    InflightJob = RefreshedJob#discovery_job{pid = job_pid},
    InflightJobs = TakenJobs#discovery_jobs{
        inflight = #{Job#discovery_job.key => InflightJob}
    },
    State4 = State3#state{
        jobs = (State3#state.jobs)#{
            chunk_interval := InflightJobs
        }
    },
    ?assertEqual(State4, arweave_sync_discovery:enqueue_job(Job, State4)).

%% @doc Discovery ignores metadata jobs for peers outside the tracked set.
enqueue_job_rejects_untracked_peer(_Config) ->
    Job = chunk_interval_job_for_test(peer, store, 0),
    State = #state{},
    ?assertEqual(State, arweave_sync_discovery:enqueue_job(Job, State)).

%% @doc Distinct readahead locations retain independent pending metadata jobs.
enqueue_job_keeps_distinct_pending_locations(_Config) ->
    Step = arweave_sync_cursor:query_range_step_size(),
    Later = chunk_interval_job_for_test(peer, store, Step),
    Earlier = chunk_interval_job_for_test(peer, store, 0),
    Latest = chunk_interval_job_for_test(peer, store, 2 * Step),
    TrackedPeers = sets:from_list([peer]),
    State = #state{tracked_peers = TrackedPeers},
    JobsByKind = State#state.jobs,
    %% Distinct active readahead locations remain independent while the global
    %% pending ceiling bounds their aggregate queue.
    State2 = State#state{
        jobs = JobsByKind#{
            chunk_interval := #discovery_jobs{}
        }
    },
    State3 = arweave_sync_discovery:enqueue_job(Later, State2),
    State4 = arweave_sync_discovery:enqueue_job(Earlier, State3),
    State5 = arweave_sync_discovery:enqueue_job(Latest, State4),
    Jobs = maps:get(chunk_interval, State5#state.jobs),
    ?assertEqual(3, map_size(Jobs#discovery_jobs.pending)),
    ?assert(arweave_sync_discovery:job_exists(Earlier#discovery_job.key, State5)),
    ?assert(arweave_sync_discovery:job_exists(Later#discovery_job.key, State5)),
    ?assert(arweave_sync_discovery:job_exists(Latest#discovery_job.key, State5)).

%% @doc New jobs are rejected once both global capacity and the store guarantee
%% are exhausted.
enqueue_job_discards_when_pending_limit_reached(_Config) ->
    Step = arweave_sync_cursor:query_range_step_size(),
    %% Sixty jobs consume the store guarantee. The sixty-first is rejected
    %% because the one-job opportunistic limit was exceeded long ago.
    Existing = [
        chunk_interval_job_for_test({peer, N}, store, N * Step)
     || N <- lists:seq(
            1,
            ?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE
        )
    ],
    Incoming = chunk_interval_job_for_test(
        peer,
        store,
        (?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE + 1) * Step
    ),
    TrackedPeers = sets:from_list(
        [
            Incoming#discovery_job.peer
            | [Job#discovery_job.peer || Job <- Existing]
        ]
    ),
    State = #state{tracked_peers = TrackedPeers},
    JobsByKind = State#state.jobs,
    %% A one-job opportunistic ceiling leaves only the store guarantee.
    ChunkIntervalJobs = #discovery_jobs{max_pending = 1},
    State2 = State#state{
        jobs = JobsByKind#{
            chunk_interval := ChunkIntervalJobs
        }
    },
    FullStoreState = lists:foldl(
        fun arweave_sync_discovery:enqueue_job/2, State2, Existing
    ),
    ?assertEqual(FullStoreState, arweave_sync_discovery:enqueue_job(Incoming, FullStoreState)).

%% @doc A late store receives its guaranteed queue capacity without evicting
%% existing work.
enqueue_job_guarantees_late_store_without_eviction(_Config) ->
    Step = arweave_sync_cursor:query_range_step_size(),
    Existing = chunk_interval_job_for_test(peer1, store1, 0),
    %% A late store may add sixty jobs after the one-job global limit is full:
    %% one byte and one footprint request for each of thirty candidate peers.
    Guaranteed = [
        chunk_interval_job_for_test({peer2, N}, store2, N * Step)
     || N <- lists:seq(
            1,
            ?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE
        )
    ],
    Extra = chunk_interval_job_for_test(
        peer2,
        store2,
        (?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE + 1) * Step
    ),
    TrackedPeers = sets:from_list(
        [
            Existing#discovery_job.peer,
            Extra#discovery_job.peer
            | [Job#discovery_job.peer || Job <- Guaranteed]
        ]
    ),
    State = #state{tracked_peers = TrackedPeers},
    JobsByKind = State#state.jobs,
    ChunkIntervalJobs = #discovery_jobs{max_pending = 1},
    State2 = State#state{
        jobs = JobsByKind#{
            chunk_interval := ChunkIntervalJobs
        }
    },
    FullState = arweave_sync_discovery:enqueue_job(Existing, State2),
    GuaranteedState = lists:foldl(
        fun arweave_sync_discovery:enqueue_job/2, FullState, Guaranteed
    ),
    Jobs = maps:get(chunk_interval, GuaranteedState#state.jobs),
    Pending = Jobs#discovery_jobs.pending,
    ?assertEqual(
        1 + ?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE,
        map_size(Pending)
    ),
    ?assert(maps:is_key(Existing#discovery_job.key, Pending)),
    ?assert(
        lists:all(
            fun(Job) -> maps:is_key(Job#discovery_job.key, Pending) end,
            Guaranteed
        )
    ),
    %% Once both the global limit and store guarantee are full, another job is
    %% dropped without moving any existing work.
    ?assertEqual(GuaranteedState, arweave_sync_discovery:enqueue_job(Extra, GuaranteedState)).

%% @doc A store can use spare global queue capacity after exhausting its
%% guaranteed share.
enqueue_job_uses_global_capacity_after_store_guarantee(_Config) ->
    Step = arweave_sync_cursor:query_range_step_size(),
    %% Sixty jobs consume the store guarantee, but the sixty-first still uses
    %% the final slot under a sixty-one-job opportunistic limit.
    Existing = [
        chunk_interval_job_for_test({peer, N}, store, N * Step)
     || N <- lists:seq(
            1,
            ?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE
        )
    ],
    Incoming = chunk_interval_job_for_test(
        peer,
        store,
        (?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE + 1) * Step
    ),
    TrackedPeers = sets:from_list(
        [
            Incoming#discovery_job.peer
            | [Job#discovery_job.peer || Job <- Existing]
        ]
    ),
    State = #state{tracked_peers = TrackedPeers},
    JobsByKind = State#state.jobs,
    ChunkIntervalJobs = #discovery_jobs{
        max_pending = ?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE + 1
    },
    State2 = State#state{
        jobs = JobsByKind#{
            chunk_interval := ChunkIntervalJobs
        }
    },
    State3 = lists:foldl(
        fun arweave_sync_discovery:enqueue_job/2, State2, Existing
    ),
    State4 = arweave_sync_discovery:enqueue_job(Incoming, State3),
    Jobs = maps:get(chunk_interval, State3#state.jobs),
    ?assertEqual(
        ?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE,
        map_size(Jobs#discovery_jobs.pending)
    ),
    Jobs2 = maps:get(chunk_interval, State4#state.jobs),
    ?assertEqual(
        ?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE + 1,
        map_size(Jobs2#discovery_jobs.pending)
    ),
    ?assert(
        maps:is_key(
            Incoming#discovery_job.key,
            Jobs2#discovery_jobs.pending
        )
    ).

%% @doc Completing a chunk interval job leaves sync bucket job state unchanged.
finish_job_updates_matching_kind(_Config) ->
    Peer = peer,
    Job = (chunk_interval_job_for_test(Peer, store, 0))#discovery_job{
        pid = self()
    },
    State0 = #state{tracked_peers = sets:from_list([Peer])},
    SyncBucketJobs = maps:get(sync_bucket, State0#state.jobs),
    ChunkIntervalJobs = #discovery_jobs{
        inflight = #{Job#discovery_job.key => Job}
    },
    State = State0#state{
        jobs = (State0#state.jobs)#{
            chunk_interval := ChunkIntervalJobs
        }
    },
    State2 = arweave_sync_discovery:finish_job(self(), State),
    ?assertEqual(
        SyncBucketJobs, maps:get(sync_bucket, State2#state.jobs)
    ),
    ?assertEqual(
        #{},
        (maps:get(chunk_interval, State2#state.jobs))#discovery_jobs.inflight
    ).

%% @doc Adding a peer schedules one bucket request, including repeated
%% additions.
add_peer_enqueues_one_sync_bucket_job(_Config) ->
    Peer = peer,
    State = arweave_sync_discovery:add_peer(Peer, #state{}),
    Jobs = maps:get(sync_bucket, State#state.jobs),
    ?assert(sets:is_element(Peer, State#state.tracked_peers)),
    ?assertEqual(1, map_size(Jobs#discovery_jobs.pending)),
    ?assert(arweave_sync_discovery:job_exists({sync_bucket, Peer}, State)),
    %% Re-collecting an already tracked peer must not duplicate its job.
    ?assertEqual(State, arweave_sync_discovery:add_peer(Peer, State)).

%% @doc Bucket refreshes preserve inflight jobs and enqueue only peers without
%% active requests.
enqueue_sync_bucket_jobs_deduplicates_active_peers(_Config) ->
    Peer1 = peer1,
    Peer2 = peer2,
    Job1 = (arweave_sync_discovery:sync_bucket_job(Peer1))#discovery_job{pid = job_pid},
    State0 = #state{tracked_peers = sets:from_list([Peer1, Peer2])},
    State = State0#state{
        jobs = (State0#state.jobs)#{
            sync_bucket := #discovery_jobs{
                inflight = #{Job1#discovery_job.key => Job1}
            }
        }
    },
    State2 = arweave_sync_discovery:enqueue_sync_bucket_jobs(State),
    Jobs = maps:get(sync_bucket, State2#state.jobs),
    ?assertEqual(
        #{Job1#discovery_job.key => Job1},
        Jobs#discovery_jobs.inflight
    ),
    ?assertEqual(
        #{
            {sync_bucket, Peer2} => arweave_sync_discovery:sync_bucket_job(Peer2)
        },
        Jobs#discovery_jobs.pending
    ).

%% @doc Removing a peer stops its inflight jobs and preserves other peers'
%% pending work.
remove_jobs_keeps_other_peers(_Config) ->
    RemovedPeer = removed_peer,
    KeptPeer = kept_peer,
    RemovedInflightJob = chunk_interval_job_for_test(
        RemovedPeer, inflight_store, 0
    ),
    RemovedPendingJob = chunk_interval_job_for_test(
        RemovedPeer, pending_store, 1
    ),
    KeptPendingJob = chunk_interval_job_for_test(KeptPeer, kept_store, 2),
    RemovedJobPID = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    RemovedInflightJob2 =
        RemovedInflightJob#discovery_job{pid = RemovedJobPID},
    Jobs = #discovery_jobs{
        pending = #{
            RemovedPendingJob#discovery_job.key => RemovedPendingJob,
            KeptPendingJob#discovery_job.key => KeptPendingJob
        },
        inflight = #{
            RemovedInflightJob2#discovery_job.key => RemovedInflightJob2
        }
    },
    Jobs2 = arweave_sync_discovery:remove_jobs(RemovedPeer, Jobs),
    State0 = #state{},
    State = State0#state{
        jobs = (State0#state.jobs)#{
            chunk_interval := Jobs2
        }
    },
    ?assertNot(is_process_alive(RemovedJobPID)),
    ?assertNot(arweave_sync_discovery:job_exists(RemovedPendingJob#discovery_job.key, State)),
    ?assertNot(arweave_sync_discovery:job_exists(RemovedInflightJob2#discovery_job.key, State)),
    ?assert(arweave_sync_discovery:job_exists(KeptPendingJob#discovery_job.key, State)).

%% @doc Late results from a removed peer cannot repopulate its cleared metadata
%% cache.
removed_peer_job_results_are_ignored(_Config) ->
    arweave_sync_discovery:reset_all_caches(),
    Peer = {10, 0, 0, 1, 1984},
    Offset = 0,
    Intervals = ar_intervals:from_list([{?DATA_CHUNK_SIZE, 0}]),
    TrackedState = arweave_sync_discovery:add_peer(Peer, #state{}),
    _ = arweave_sync_discovery:handle_cast(
        {job_result, Peer,
            {chunk_intervals, ?DEFAULT_MODULE, Offset, byte, {ok, Intervals}}},
        TrackedState
    ),
    ?assertEqual({hit, Intervals}, arweave_sync_discovery:chunk_interval_lookup(byte, Peer, Offset)),
    arweave_sync_discovery:store_row(sync_bucket, byte, 0, Peer, 1.0),
    {noreply, RemovedState} =
        arweave_sync_discovery:do_remove_peer(Peer, test, TrackedState),
    ?assertEqual(miss, arweave_sync_discovery:chunk_interval_lookup(byte, Peer, Offset)),
    ?assertNot(
        ets:member(
            ?SYNC_BUCKET_CACHE_TABLE,
            arweave_sync_discovery:sync_bucket_key(byte, 0, Peer)
        )
    ),
    _ = arweave_sync_discovery:handle_cast(
        {job_result, Peer,
            {chunk_intervals, test_store, Offset, byte, {ok, Intervals}}},
        RemovedState
    ),
    ?assertEqual(miss, arweave_sync_discovery:chunk_interval_lookup(byte, Peer, Offset)),
    SyncBuckets = ar_sync_buckets:from_intervals(Intervals),
    _ = arweave_sync_discovery:handle_cast(
        {job_result, Peer, {sync_buckets, byte, SyncBuckets}},
        RemovedState
    ),
    ?assertNot(
        ets:member(
            ?SYNC_BUCKET_CACHE_TABLE,
            arweave_sync_discovery:sync_bucket_key(byte, 0, Peer)
        )
    ).

%% @doc Selection prefers the least-loaded peer before considering store load.
take_next_job_prefers_less_loaded_peer(_Config) ->
    Step = arweave_sync_cursor:query_range_step_size(),
    Inflight1 = chunk_interval_job_for_test(p1, s1, 0),
    Inflight2 = chunk_interval_job_for_test(p1, s2, Step),
    Inflight3 = chunk_interval_job_for_test(p2, s1, 2 * Step),
    Q1 = chunk_interval_job_for_test(p1, s3, 3 * Step),
    Q2 = chunk_interval_job_for_test(p2, s2, 4 * Step),
    Jobs = #discovery_jobs{
        pending = #{
            Q1#discovery_job.key => Q1,
            Q2#discovery_job.key => Q2
        },
        inflight = #{
            Inflight1#discovery_job.key =>
                Inflight1#discovery_job{pid = self()},
            Inflight2#discovery_job.key =>
                Inflight2#discovery_job{pid = self()},
            Inflight3#discovery_job.key =>
                Inflight3#discovery_job{pid = self()}
        }
    },
    {PeerLoad, StoreLoad} = arweave_sync_discovery:compute_job_load(Jobs),
    ?assertEqual(#{p1 => 2, p2 => 1}, PeerLoad),
    ?assertEqual(#{s1 => 2, s2 => 1}, StoreLoad),
    {ok, Selected, _Jobs2} = arweave_sync_discovery:take_next_job(
        Jobs,
        PeerLoad,
        StoreLoad,
        arweave_sync_discovery:inflight_peer_store_mode_counts(Jobs)
    ),
    ?assertEqual(p2, Selected#discovery_job.peer).

%% @doc Byte metadata stays serialized per peer and store while unrelated
%% stores continue using discovery capacity.
take_next_job_limits_byte_peer_store_mode(_Config) ->
    Step = arweave_sync_cursor:query_range_step_size(),
    Inflight = chunk_interval_job_for_test(peer, store1, 0),
    SamePendingKey = chunk_interval_job_for_test(peer, store1, Step),
    OtherStore = chunk_interval_job_for_test(peer, store2, Step),
    Jobs = #discovery_jobs{
        pending = #{
            SamePendingKey#discovery_job.key => SamePendingKey,
            OtherStore#discovery_job.key => OtherStore
        },
        inflight = #{
            Inflight#discovery_job.key =>
                Inflight#discovery_job{pid = self()}
        }
    },
    {PeerLoad, StoreLoad} = arweave_sync_discovery:compute_job_load(Jobs),
    Counts = arweave_sync_discovery:inflight_peer_store_mode_counts(Jobs),
    SameCombinationJobs = Jobs#discovery_jobs{
        pending = #{
            SamePendingKey#discovery_job.key => SamePendingKey
        }
    },
    ?assertEqual(
        none,
        arweave_sync_discovery:take_next_job(
            SameCombinationJobs, PeerLoad, StoreLoad, Counts
        )
    ),
    {ok, Selected, Jobs2} = arweave_sync_discovery:take_next_job(
        Jobs, PeerLoad, StoreLoad, Counts
    ),
    ?assertEqual(store2, Selected#discovery_job.store_id),
    ?assertEqual(
        #{SamePendingKey#discovery_job.key => SamePendingKey},
        Jobs2#discovery_jobs.pending
    ).

%% @doc Independent footprint locations use bounded concurrent searches.
take_next_job_allows_bounded_footprint_concurrency(_Config) ->
    Limit = ?MAX_FOOTPRINT_JOBS_PER_PEER_STORE,
    InflightJobs = [
        chunk_interval_job_for_test(peer, store, footprint, N)
     || N <- lists:seq(0, Limit - 1)
    ],
    Pending = chunk_interval_job_for_test(
        peer, store, footprint, Limit
    ),
    Inflight = maps:from_list([
        {Job#discovery_job.key, Job#discovery_job{pid = self()}}
     || Job <- InflightJobs
    ]),
    FullJobs = #discovery_jobs{
        pending = #{Pending#discovery_job.key => Pending},
        inflight = Inflight
    },
    {PeerLoad, StoreLoad} = arweave_sync_discovery:compute_job_load(FullJobs),
    ?assertEqual(
        none,
        arweave_sync_discovery:take_next_job(
            FullJobs,
            PeerLoad,
            StoreLoad,
            arweave_sync_discovery:inflight_peer_store_mode_counts(FullJobs)
        )
    ),
    [Released | _] = InflightJobs,
    JobsWithCapacity = FullJobs#discovery_jobs{
        inflight = maps:remove(Released#discovery_job.key, Inflight)
    },
    {PeerLoad2, StoreLoad2} = arweave_sync_discovery:compute_job_load(JobsWithCapacity),
    {ok, Selected, _Jobs2} = arweave_sync_discovery:take_next_job(
        JobsWithCapacity,
        PeerLoad2,
        StoreLoad2,
        arweave_sync_discovery:inflight_peer_store_mode_counts(JobsWithCapacity)
    ),
    ?assertEqual(Pending#discovery_job.key, Selected#discovery_job.key).

%% @doc Recent frontier demand wins over older requests, with offset order
%% breaking age ties.
take_next_job_prefers_recently_requested_location(_Config) ->
    Step = arweave_sync_cursor:query_range_step_size(),
    Old = chunk_interval_job_for_test(peer, store, 0),
    New = chunk_interval_job_for_test(peer, store, Step),
    Jobs = #discovery_jobs{
        pending = #{
            Old#discovery_job.key => Old,
            New#discovery_job.key => New
        }
    },
    {ok, Selected, _Jobs2} = arweave_sync_discovery:take_next_job(Jobs, #{}, #{}, #{}),
    %% Equal-age readahead keeps the earlier queue location first.
    ?assertEqual(Old#discovery_job.key, Selected#discovery_job.key),
    TouchedNew = New#discovery_job{requested_at = 1},
    TouchedJobs = Jobs#discovery_jobs{
        pending = #{
            Old#discovery_job.key => Old,
            TouchedNew#discovery_job.key => TouchedNew
        }
    },
    {ok, Selected2, _Jobs3} = arweave_sync_discovery:take_next_job(
        TouchedJobs, #{}, #{}, #{}
    ),
    %% Re-requesting the later location makes it the active frontier demand.
    ?assertEqual(TouchedNew#discovery_job.key, Selected2#discovery_job.key).

%% At the job limit, start_jobs/2 leaves pending and inflight work unchanged.
start_jobs_respects_inflight_limit(_Config) ->
    Limit = ?MAX_DISCOVERY_JOBS_PER_KIND,
    InflightJobs = [
        chunk_interval_job_for_test({p, N}, {s, N}, N)
     || N <- lists:seq(1, Limit)
    ],
    Inflight = maps:from_list([
        {Job#discovery_job.key, Job#discovery_job{pid = self()}}
     || Job <- InflightJobs
    ]),
    PendingJob = chunk_interval_job_for_test(
        {p, 1}, pending_store, Limit + 1
    ),
    DefaultJobsByKind = (#state{})#state.jobs,
    DefaultSyncBucketJobs = maps:get(sync_bucket, DefaultJobsByKind),
    ?assertEqual(Limit, DefaultSyncBucketJobs#discovery_jobs.max_inflight),
    ?assertEqual(
        ?MAX_DISCOVERY_PEERS,
        DefaultSyncBucketJobs#discovery_jobs.max_pending
    ),
    DefaultJobs = maps:get(chunk_interval, DefaultJobsByKind),
    ?assertEqual(Limit, DefaultJobs#discovery_jobs.max_inflight),
    ?assertEqual(
        ?MAX_PENDING_CHUNK_INTERVAL_JOBS,
        DefaultJobs#discovery_jobs.max_pending
    ),
    Jobs = DefaultJobs#discovery_jobs{
        inflight = Inflight,
        pending = #{PendingJob#discovery_job.key => PendingJob}
    },
    ?assertEqual(Jobs, arweave_sync_discovery:start_jobs(Jobs)).

%% @doc Peer lookup combines byte and footprint bucket advertisements for the
%% requested offset.
get_peers_for_offset_unions_sync_bucket_sources(_Config) ->
    arweave_sync_discovery:reset_all_caches(),
    Offset = ?DATA_CHUNK_SIZE,
    BytePeer = {10, 0, 0, 4, 1984},
    FootprintPeer = {10, 0, 0, 5, 1984},
    arweave_sync_discovery:store_row(
        sync_bucket,
        byte,
        Offset div ?NETWORK_DATA_BUCKET_SIZE,
        BytePeer,
        1.0
    ),
    arweave_sync_discovery:store_row(
        sync_bucket,
        footprint,
        arweave_storage:get_footprint_bucket(Offset + ?DATA_CHUNK_SIZE),
        FootprintPeer,
        1.0
    ),
    ?assertEqual(
        lists:sort([BytePeer, FootprintPeer]),
        lists:sort(arweave_sync_discovery:get_peers_for_offset(Offset))
    ).

%% @doc Cached range lookup preserves both byte and footprint sources for the
%% same peer.
cached_peer_ranges_includes_byte_and_footprint_ranges(_Config) ->
    with_mocks(
        [
            {arweave_sync_test_deps, get_peer_release, fun(_) ->
                ?GET_FOOTPRINT_SUPPORT_RELEASE
            end}
        ],
        fun() ->
            arweave_sync_discovery:reset_all_caches(),
            Peer = {10, 0, 0, 6, 1984},
            Offset = ?DATA_CHUNK_SIZE,
            RangeStart = 0,
            RangeEnd = 4 * ?DATA_CHUNK_SIZE,
            ByteIntervals = ar_intervals:from_list([
                {2 * ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE}
            ]),
            FootprintIntervals = ar_intervals:from_list([{4, 0}]),
            {Partition, Footprint} = arweave_storage:get_footprint_location(
                Offset + ?DATA_CHUNK_SIZE
            ),
            FootprintKey = #footprint{
                store_id = test_store,
                partition = Partition,
                footprint = Footprint
            },
            arweave_sync_discovery:store_row(
                sync_bucket,
                byte,
                Offset div ?NETWORK_DATA_BUCKET_SIZE,
                Peer,
                1.0
            ),
            arweave_sync_discovery:store_row(
                sync_bucket,
                footprint,
                arweave_storage:get_footprint_bucket(
                    Offset + ?DATA_CHUNK_SIZE
                ),
                Peer,
                1.0
            ),
            arweave_sync_discovery:store_row(chunk_interval, byte, 0, Peer, ByteIntervals),
            arweave_sync_discovery:store_row(
                chunk_interval, footprint, Offset, Peer, FootprintIntervals
            ),
            ExpectedFootprintIntervals =
                arweave_storage:footprint_intervals_to_byte_intervals(
                    FootprintIntervals
                ),
            ?assertEqual(
                {
                    [
                        #peer_range{
                            store_id = test_store,
                            offset = Offset,
                            peer = Peer,
                            intervals = ByteIntervals,
                            footprint = none
                        },
                        #peer_range{
                            store_id = test_store,
                            offset = Offset,
                            peer = Peer,
                            intervals = ExpectedFootprintIntervals,
                            footprint = FootprintKey
                        }
                    ],
                    ok
                },
                arweave_sync_discovery:cached_peer_ranges(
                    test_store, [Peer], Offset, RangeStart, RangeEnd
                )
            )
        end
    ).

%% @doc Cached byte metadata is clipped to the requested range.
get_chunk_intervals_limits_byte_metadata_to_requested_range(_Config) ->
    arweave_sync_discovery:reset_all_caches(),
    Peer = {10, 0, 0, 1, 1984},
    Chunk = ?DATA_CHUNK_SIZE,
    %% The cached range spans four chunks while the query selects its middle two.
    Intervals = ar_intervals:from_list([{4 * Chunk, 0}]),
    Expected = ar_intervals:from_list([{3 * Chunk, Chunk}]),
    arweave_sync_discovery:store_row(chunk_interval, byte, Chunk, Peer, Intervals),
    ?assertEqual(
        {ok, Expected},
        arweave_sync_discovery:get_chunk_intervals(
            byte, Peer, Chunk, Chunk, 3 * Chunk
        )
    ).

%% @doc A peer with no relevant coarse advertisement does not create a metadata
%% cache miss.
irrelevant_peer_has_no_cache_miss_without_detail_metadata(_Config) ->
    arweave_sync_discovery:reset_all_caches(),
    Peer = {10, 0, 0, 7, 1984},
    ?assertEqual(
        {[], ok},
        arweave_sync_discovery:cached_peer_ranges(
            ?DEFAULT_MODULE, [Peer], ?DATA_CHUNK_SIZE, 0, ?DATA_CHUNK_SIZE
        )
    ).

%% @doc A cached empty interval set is a successful lookup, not a cache miss.
cached_empty_metadata_is_ok(_Config) ->
    arweave_sync_discovery:reset_all_caches(),
    Peer = {10, 0, 0, 7, 1984},
    arweave_sync_discovery:store_row(sync_bucket, byte, 0, Peer, 1.0),
    arweave_sync_discovery:store_row(chunk_interval, byte, 0, Peer, ar_intervals:new()),
    ?assertEqual(
        {[], ok},
        arweave_sync_discovery:cached_peer_ranges(
            ?DEFAULT_MODULE, [Peer], 0, 0, ?DATA_CHUNK_SIZE
        )
    ).

%% @doc Only explicit warming requests metadata; cached reads do not enqueue
%% discovery work.
warming_schedules_miss_but_cached_read_has_no_side_effect(_Config) ->
    arweave_sync_discovery:reset_all_caches(),
    Peer = {10, 0, 0, 8, 1984},
    arweave_sync_discovery:store_row(sync_bucket, byte, 0, Peer, 1.0),
    DiscoveryPid = whereis(arweave_sync_discovery),
    1 = erlang:trace(DiscoveryPid, true, ['receive']),
    try
        ?assertEqual(
            {[], cache_miss},
            arweave_sync_discovery:cached_peer_ranges(
                ?DEFAULT_MODULE, [Peer], 0, 0, ?DATA_CHUNK_SIZE
            )
        ),
        _ = sys:get_state(DiscoveryPid),
        receive
            {trace, DiscoveryPid, 'receive',
                {'$gen_call', _, {warm_peer_ranges, _, _, _}}} ->
                ?assert(false)
        after 0 ->
            ok
        end,
        ?assertEqual(
            ok,
            arweave_sync_discovery:warm_peer_ranges(?DEFAULT_MODULE, [Peer], 0)
        ),
        RefreshRequest =
            receive
                {trace, DiscoveryPid, 'receive',
                    {'$gen_call', _,
                        {warm_peer_ranges, StoreID, Peers, Offset}}} ->
                    {StoreID, Peers, Offset}
            after 1000 ->
                none
            end,
        ?assertEqual({?DEFAULT_MODULE, [Peer], 0}, RefreshRequest)
    after
        1 = erlang:trace(DiscoveryPid, false, ['receive'])
    end.

%% @doc Synchronous warming limits each caller to one pending request while
%% discovery is blocked.
warming_calls_are_backpressured(_Config) ->
    DiscoveryPid = whereis(arweave_sync_discovery),
    %% Twenty callers making fifty requests model one thousand repeated store
    %% requests. A synchronous batch permits only one queued call per caller.
    CallerCount = 20,
    RequestsPerCaller = 50,
    ok = sys:suspend(DiscoveryPid),
    Callers = [
        spawn_monitor(fun() ->
            lists:foreach(
                fun(_) ->
                    ok = arweave_sync_discovery:warm_peer_ranges(test_store, [], 0)
                end,
                lists:seq(1, RequestsPerCaller)
            )
        end)
     || _ <- lists:seq(1, CallerCount)
    ],
    try
        ok = ar_test_await:until(discovery_warm_calls_blocked, fun() ->
            warm_call_count(DiscoveryPid) =:= CallerCount
        end),
        ?assertEqual(CallerCount, warm_call_count(DiscoveryPid))
    after
        ok = sys:resume(DiscoveryPid)
    end,
    %% Empty peer batches complete well within the normal thirty-second test
    %% timeout; matching monitor references also verifies no caller crashed.
    Reasons = [
        receive
            {'DOWN', Ref, process, Pid, Reason} -> Reason
        after 30_000 ->
            timeout
        end
     || {Pid, Ref} <- Callers
    ],
    ?assertEqual(lists:duplicate(CallerCount, normal), Reasons).

%% @doc An aged row is served as {stale, Intervals}: the data is still used while the
%% demand path requests a chunk interval job. A fresh row stays a plain hit.
stale_interval_rows_remain_usable(_Config) ->
    arweave_sync_discovery:reset_all_caches(),
    Peer = {10, 0, 0, 1, 1984},
    RangeEnd = ?DATA_CHUNK_SIZE,
    Intervals = ar_intervals:from_list([{RangeEnd, 0}]),
    ByteKey = arweave_sync_discovery:chunk_interval_key(byte, Peer, 0),
    arweave_sync_discovery:store_row(chunk_interval, byte, 0, Peer, Intervals),
    ?assertEqual(
        {ok, Intervals},
        arweave_sync_discovery:get_chunk_intervals(byte, Peer, 0, 0, RangeEnd)
    ),
    StaleMs =
        arweave_sync_test_deps:monotonic_ms() -
            ?CHUNK_INTERVAL_CACHE_TTL_MS - 1,
    ets:insert(?CHUNK_INTERVAL_CACHE_TABLE, {ByteKey, Intervals, StaleMs}),
    ?assertEqual(
        {stale, Intervals},
        arweave_sync_discovery:get_chunk_intervals(byte, Peer, 0, 0, RangeEnd)
    ),
    %% A stale byte row remains usable without making the cached read impure.
    arweave_sync_discovery:store_row(sync_bucket, byte, 0, Peer, 1.0),
    ?assertEqual(
        {
            [
                #peer_range{
                    store_id = ?DEFAULT_MODULE,
                    offset = 0,
                    peer = Peer,
                    intervals = Intervals,
                    footprint = none
                }
            ],
            ok
        },
        arweave_sync_discovery:cached_peer_ranges(?DEFAULT_MODULE, [Peer], 0, 0, RangeEnd)
    ),
    %% Footprint rows age against the same safety floor.
    FootprintOffset = 0,
    FootprintIntervals = ar_intervals:from_list([{1, 0}]),
    ExpectedFootprintIntervals =
        arweave_storage:footprint_intervals_to_byte_intervals(
            FootprintIntervals
        ),
    FpKey = arweave_sync_discovery:chunk_interval_key(footprint, Peer, FootprintOffset),
    arweave_sync_discovery:store_row(
        chunk_interval,
        footprint,
        FootprintOffset,
        Peer,
        FootprintIntervals
    ),
    ?assertEqual(
        {ok, ExpectedFootprintIntervals},
        arweave_sync_discovery:get_chunk_intervals(
            footprint, Peer, FootprintOffset, 0, RangeEnd
        )
    ),
    FpStaleMs =
        arweave_sync_test_deps:monotonic_ms() -
            ?CHUNK_INTERVAL_CACHE_TTL_MS - 1,
    ets:insert(
        ?CHUNK_INTERVAL_CACHE_TABLE,
        {FpKey, FootprintIntervals, FpStaleMs}
    ),
    ?assertEqual(
        {stale, ExpectedFootprintIntervals},
        arweave_sync_discovery:get_chunk_intervals(
            footprint, Peer, FootprintOffset, 0, RangeEnd
        )
    ).

%% @doc Warming stale metadata sends a refresh request to discovery.
warming_stale_metadata_requests_refresh(_Config) ->
    arweave_sync_discovery:reset_all_caches(),
    Peer = {10, 0, 0, 9, 1984},
    Intervals = ar_intervals:from_list([{?DATA_CHUNK_SIZE, 0}]),
    StaleMs =
        arweave_sync_test_deps:monotonic_ms() - ?CHUNK_INTERVAL_CACHE_TTL_MS -
            1,
    arweave_sync_discovery:store_row(sync_bucket, byte, 0, Peer, 1.0),
    ets:insert(
        ?CHUNK_INTERVAL_CACHE_TABLE,
        {arweave_sync_discovery:chunk_interval_key(byte, Peer, 0), Intervals, StaleMs}
    ),
    DiscoveryPid = whereis(arweave_sync_discovery),
    1 = erlang:trace(DiscoveryPid, true, ['receive']),
    try
        ?assertEqual(ok, arweave_sync_discovery:warm_peer_ranges(?DEFAULT_MODULE, [Peer], 0)),
        receive
            {trace, DiscoveryPid, 'receive',
                {'$gen_call', _,
                    {warm_peer_ranges, ?DEFAULT_MODULE, [Peer], 0}}} ->
                ok
        after 1000 ->
            ?assert(false)
        end
    after
        1 = erlang:trace(DiscoveryPid, false, ['receive'])
    end.

%% @doc A changed byte sync-bucket share marks the covered chunk intervals stale.
%% Unchanged shares, chunk intervals in other sync buckets, and other peers'
%% rows in the same bucket remain fresh.
byte_share_change_marks_chunk_intervals_stale(_Config) ->
    arweave_sync_discovery:reset_all_caches(),
    %% Ten interval locations per sync bucket, so a bucket has a last location
    %% distinct from its first.
    Step = ?NETWORK_DATA_BUCKET_SIZE div 10,
    arweave_sync_cursor:override_query_range_step_size(Step),
    try
        Peer = {10, 0, 0, 1, 1984},
        OtherPeer = {10, 0, 0, 2, 1984},
        Intervals = ar_intervals:from_list([{100, 0}]),
        LastInBucket = ?NETWORK_DATA_BUCKET_SIZE - Step,
        arweave_sync_discovery:store_row(chunk_interval, byte, 0, Peer, Intervals),
        arweave_sync_discovery:store_row(chunk_interval, byte, LastInBucket, Peer, Intervals),
        %% The first location of bucket one is outside bucket zero.
        arweave_sync_discovery:store_row(chunk_interval, byte, ?NETWORK_DATA_BUCKET_SIZE, Peer, Intervals),
        %% Another peer shares bucket zero's locations.
        arweave_sync_discovery:store_row(chunk_interval, byte, 0, OtherPeer, Intervals),
        arweave_sync_discovery:store_row(chunk_interval, byte, LastInBucket, OtherPeer, Intervals),
        arweave_sync_discovery:store_row(sync_bucket, byte, 0, Peer, 0.5),
        arweave_sync_discovery:mark_chunk_intervals_stale_on_share_change(byte, Peer, 0, 0.5),
        ?assertMatch({hit, _}, arweave_sync_discovery:chunk_interval_lookup(byte, Peer, 0)),
        arweave_sync_discovery:mark_chunk_intervals_stale_on_share_change(byte, Peer, 0, 0.7),
        ?assertMatch({stale, _}, arweave_sync_discovery:chunk_interval_lookup(byte, Peer, 0)),
        ?assertMatch({stale, _}, arweave_sync_discovery:chunk_interval_lookup(byte, Peer, LastInBucket)),
        ?assertMatch(
            {hit, _},
            arweave_sync_discovery:chunk_interval_lookup(byte, Peer, ?NETWORK_DATA_BUCKET_SIZE)
        ),
        ?assertMatch({hit, _}, arweave_sync_discovery:chunk_interval_lookup(byte, OtherPeer, 0)),
        ?assertMatch(
            {hit, _}, arweave_sync_discovery:chunk_interval_lookup(byte, OtherPeer, LastInBucket)
        )
    after
        arweave_sync_cursor:reset_all_overrides()
    end.

%% @doc A changed footprint sync-bucket share has the same stale-marking behavior.
footprint_share_change_marks_chunk_intervals_stale(_Config) ->
    arweave_sync_discovery:reset_all_caches(),
    Peer = {10, 0, 0, 1, 1984},
    Intervals = ar_intervals:from_list([{100, 0}]),
    InSyncBucketOffset = 0,
    %% Two buckets away is beyond the one-footprint boundary superset marked
    %% stale with bucket zero.
    FarAwayChunkEnd =
        arweave_storage:get_padded_offset_from_footprint_offset(
            2 * ?NETWORK_FOOTPRINT_BUCKET_SIZE + 1
        ),
    FarAwayOffset = FarAwayChunkEnd - ?DATA_CHUNK_SIZE,
    SyncBucket = arweave_sync_discovery:sync_bucket(footprint, InSyncBucketOffset),
    arweave_sync_discovery:store_row(
        chunk_interval,
        footprint,
        InSyncBucketOffset,
        Peer,
        Intervals
    ),
    arweave_sync_discovery:store_row(chunk_interval, footprint, FarAwayOffset, Peer, Intervals),
    arweave_sync_discovery:store_row(sync_bucket, footprint, SyncBucket, Peer, 0.5),
    arweave_sync_discovery:mark_chunk_intervals_stale_on_share_change(
        footprint, Peer, SyncBucket, 0.5
    ),
    ?assertMatch(
        {hit, _},
        arweave_sync_discovery:chunk_interval_lookup(footprint, Peer, InSyncBucketOffset)
    ),
    arweave_sync_discovery:mark_chunk_intervals_stale_on_share_change(
        footprint, Peer, SyncBucket, 0.7
    ),
    ?assertMatch(
        {stale, _},
        arweave_sync_discovery:chunk_interval_lookup(footprint, Peer, InSyncBucketOffset)
    ),
    ?assertMatch(
        {hit, _},
        arweave_sync_discovery:chunk_interval_lookup(footprint, Peer, FarAwayOffset)
    ).

%% @doc Sync buckets not updated within ?SYNC_BUCKET_CACHE_TTL_MS are retired
%% together with their cached chunk intervals; recently updated buckets remain.
expired_sync_buckets_delete_chunk_intervals(_Config) ->
    arweave_sync_discovery:reset_all_caches(),
    Peer = {10, 0, 0, 9, 1984},
    Intervals = ar_intervals:from_list([{100, 0}]),
    Now = arweave_sync_test_deps:monotonic_ms(),
    Old = Now - ?SYNC_BUCKET_CACHE_TTL_MS - 1,
    ets:insert(
        ?SYNC_BUCKET_CACHE_TABLE,
        {arweave_sync_discovery:sync_bucket_key(byte, 0, Peer), 0.5, Old}
    ),
    ets:insert(
        ?SYNC_BUCKET_CACHE_TABLE,
        {arweave_sync_discovery:sync_bucket_key(byte, 1, Peer), 0.5, Now}
    ),
    arweave_sync_discovery:store_row(chunk_interval, byte, 0, Peer, Intervals),
    arweave_sync_discovery:store_row(
        chunk_interval,
        byte,
        ?NETWORK_DATA_BUCKET_SIZE,
        Peer,
        Intervals
    ),
    arweave_sync_discovery:delete_expired_sync_buckets(),
    ?assertNot(
        ets:member(
            ?SYNC_BUCKET_CACHE_TABLE,
            arweave_sync_discovery:sync_bucket_key(byte, 0, Peer)
        )
    ),
    ?assertEqual(miss, arweave_sync_discovery:chunk_interval_lookup(byte, Peer, 0)),
    ?assert(
        ets:member(
            ?SYNC_BUCKET_CACHE_TABLE,
            arweave_sync_discovery:sync_bucket_key(byte, 1, Peer)
        )
    ),
    ?assertMatch(
        {hit, _},
        arweave_sync_discovery:chunk_interval_lookup(byte, Peer, ?NETWORK_DATA_BUCKET_SIZE)
    ).

%% @doc Full metadata pages are combined until the requested range is complete.
complete_chunk_interval_range_is_paginated(_Config) ->
    Peer = {10, 0, 0, 8, 1984},
    Page1 = interval_page(1, ?QUERY_SYNC_INTERVALS_COUNT_LIMIT),
    Page1End = element(1, ar_intervals:largest(Page1)),
    NextStart = Page1End + 1,
    Page2 = interval_page(?QUERY_SYNC_INTERVALS_COUNT_LIMIT + 1, 1),
    Expected = ar_intervals:union(Page1, Page2),
    with_mocked_chunk_interval_pages(
        Peer,
        ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE,
        fun
            (1) -> {ok, Page1};
            (Start) when Start =:= NextStart -> {ok, Page2}
        end,
        fun() ->
            ?assertEqual(
                {ok, Expected},
                arweave_sync_discovery:fetch_chunk_intervals(byte, Peer, 0)
            )
        end
    ).

%% @doc A failed metadata page aborts the fetch instead of returning incomplete
%% success.
failed_chunk_interval_page_aborts_fetch(_Config) ->
    Peer = {10, 0, 0, 9, 1984},
    Page1 = interval_page(1, ?QUERY_SYNC_INTERVALS_COUNT_LIMIT),
    Page1End = element(1, ar_intervals:largest(Page1)),
    NextStart = Page1End + 1,
    with_mocked_chunk_interval_pages(
        Peer,
        ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE,
        fun
            (1) -> {ok, Page1};
            (Start) when Start =:= NextStart -> {error, timeout}
        end,
        fun() ->
            ?assertEqual(
                {error, timeout},
                arweave_sync_discovery:fetch_chunk_intervals(byte, Peer, 0)
            )
        end
    ).

%% @doc Legacy metadata responses are clipped to the requested right boundary.
legacy_chunk_interval_pages_are_cut_to_query_range(_Config) ->
    Peer = {10, 0, 0, 10, 1984},
    Page1 = interval_page(1, ?QUERY_SYNC_INTERVALS_COUNT_LIMIT),
    Page1End = element(1, ar_intervals:largest(Page1)),
    NextStart = Page1End + 1,
    Right = arweave_sync_cursor:query_range_step_size(),
    Page2 = ar_intervals:from_list([{Right + 3, Right - 1}]),
    Expected = ar_intervals:cut(ar_intervals:union(Page1, Page2), Right),
    with_mocked_chunk_interval_pages(
        Peer,
        ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE - 1,
        fun
            (1) -> {ok, Page1};
            (Start) when Start =:= NextStart -> {ok, Page2}
        end,
        fun() ->
            ?assertEqual(
                {ok, Expected},
                arweave_sync_discovery:fetch_chunk_intervals(byte, Peer, 0)
            )
        end
    ).

%% @doc Metadata pagination stops at its page limit even when every response is
%% full.
chunk_interval_pagination_is_bounded(_Config) ->
    Peer = {10, 0, 0, 11, 1984},
    with_mocked_chunk_interval_pages(
        Peer,
        ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE,
        fun(Start) ->
            Left = Start - 1,
            {ok,
                ar_intervals:from_list([
                    {Left + 2 * I, Left + 2 * I - 1}
                 || I <- lists:seq(1, ?QUERY_SYNC_INTERVALS_COUNT_LIMIT)
                ])}
        end,
        fun() ->
            ?assertEqual(
                {error, interval_page_limit},
                arweave_sync_discovery:fetch_chunk_intervals(byte, Peer, 0)
            ),
            ?assertEqual(
                ?MAX_CHUNK_INTERVAL_PAGES,
                meck:num_calls(ar_http_iface_client, get_sync_record, 4)
            )
        end
    ).

%% @doc Fresh footprint metadata is reused without another HTTP request while
%% its bucket share is unchanged.
refresh_chunk_intervals_reuses_cache(_Config) ->
    arweave_sync_discovery:reset_all_caches(),
    Peer = {10, 0, 0, 1, 1984},
    Offset = 0,
    Intervals = ar_intervals:from_list([{1, 0}]),
    Expected = arweave_storage:footprint_intervals_to_byte_intervals(
        Intervals
    ),
    arweave_sync_discovery:store_row(
        sync_bucket,
        footprint,
        arweave_sync_discovery:sync_bucket(footprint, Offset),
        Peer,
        1.0
    ),
    arweave_sync_discovery:store_row(chunk_interval, footprint, Offset, Peer, Intervals),
    meck:new(ar_http_iface_client, [passthrough]),
    meck:expect(
        ar_http_iface_client,
        get_footprints,
        fun(_, _, _) -> {error, timeout} end
    ),
    try
        Job = #discovery_job{
            kind = chunk_interval,
            peer = Peer,
            store_id = test_store,
            mode = footprint,
            start = Offset
        },
        ?assertEqual(
            ok,
            arweave_sync_discovery:refresh_chunk_intervals(Job)
        ),
        ?assertEqual(
            0,
            meck:num_calls(ar_http_iface_client, get_footprints, 3)
        ),
        ?assertEqual(
            {ok, Expected},
            arweave_sync_discovery:get_chunk_intervals(
                footprint, Peer, Offset, 0, ?DATA_CHUNK_SIZE
            )
        )
    after
        meck:unload(ar_http_iface_client)
    end.

%%====================================================================
%% Helpers
%%====================================================================

interval_page(First, Count) ->
    ar_intervals:from_list([
        {2 * Index, 2 * Index - 1}
     || Index <- lists:seq(First, First + Count - 1)
    ]).

with_mocked_chunk_interval_pages(Peer, Release, PageFun, TestFun) ->
    Right = arweave_sync_cursor:query_range_step_size(),
    FetchMock =
        case Release >= ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE of
            true ->
                {ar_http_iface_client, get_sync_record, fun(
                    P, Start, QueryRight, Limit
                ) when
                    P =:= Peer,
                    QueryRight =:= Right,
                    Limit =:= ?QUERY_SYNC_INTERVALS_COUNT_LIMIT
                ->
                    PageFun(Start)
                end};
            false ->
                {ar_http_iface_client, get_sync_record, fun(
                    P, Start, Limit
                ) when
                    P =:= Peer,
                    Limit =:= ?QUERY_SYNC_INTERVALS_COUNT_LIMIT
                ->
                    PageFun(Start)
                end}
        end,
    with_mocks(
        [
            {arweave_sync_test_deps, get_peer_release, fun(P) when P =:= Peer ->
                Release
            end},
            FetchMock
        ],
        TestFun
    ).

chunk_interval_job_for_test(Peer, StoreID, Start) ->
    chunk_interval_job_for_test(Peer, StoreID, byte, Start).

chunk_interval_job_for_test(Peer, StoreID, Mode, Start) ->
    AlignedStart =
        case Mode of
            byte -> arweave_sync_discovery:interval_location(byte, Start);
            footprint -> Start
        end,
    #discovery_job{
        key = {chunk_interval, Peer, StoreID, Mode, AlignedStart},
        kind = chunk_interval,
        peer = Peer,
        store_id = StoreID,
        mode = Mode,
        start = AlignedStart
    }.

warm_call_count(DiscoveryPid) ->
    {messages, Messages} = process_info(DiscoveryPid, messages),
    length([
        Message
     || Message = {'$gen_call', _, {warm_peer_ranges, _, _, _}} <- Messages
    ]).

with_mocks(Mocks, Test) ->
    Modules = lists:usort([Module || {Module, _, _} <- Mocks]),
    lists:foreach(fun(Module) -> meck:new(Module, [passthrough]) end, Modules),
    try
        lists:foreach(
            fun({Module, Function, Implementation}) ->
                meck:expect(Module, Function, Implementation)
            end,
            Mocks
        ),
        Test()
    after
        lists:foreach(fun meck:unload/1, Modules)
    end.

clock() -> arweave_sync_test_deps.
events() -> arweave_sync_test_deps.
node() -> arweave_sync_test_deps.
peers() -> arweave_sync_test_deps.
http() -> ar_http_iface_client.
chunk_cache() -> ar_chunk_cache.
constants() -> arweave_constants.
sync_buckets() -> ar_sync_buckets.
