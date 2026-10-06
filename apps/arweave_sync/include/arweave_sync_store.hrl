%% The claim limit is the number of chunks the store writes in this time. Five
%% seconds of work keeps the store busy through short swings in write speed,
%% and the cache limit, which matches it, stops a slow store from filling the
%% chunk cache.
-define(WRITE_QUEUE_TARGET_DURATION_MS, 5000).

%% The pipeline limit is the number of chunks the store writes in this time.
%% That covers a chunk's whole trip: about five seconds for a slow fetch, up to
%% WRITE_QUEUE_TARGET_DURATION_MS waiting to be written, and one second of
%% slack.
-define(PIPELINE_TARGET_DURATION_MS, 11_000).

%% Smoothing factor for the write rate, so that bursts of completed writes are
%% averaged over about two write samples.
-define(WRITE_RATE_ALPHA, 0.5).

%% Each write sample lasts at least one production tick, so that writes
%% completed in batches don't show up as write samples that alternate between
%% zero and a burst.
-define(WRITE_SAMPLE_MIN_MS, ?TICK_INTERVAL_MS).

%% The write rate never sets a store's limits below this many chunks, so a
%% store with a zero or very low write rate still fetches a few. That is
%% enough to notice when a stalled store resumes writing, at little cost.
-define(MIN_CLAIM_LIMIT, 25).

-record(store_state, {
    store_id,
    %% Claimed tasks and footprint reservations that are not yet bound to a
    %% peer, in priority order.
    work_queue = gb_sets:new(),
    claimed = arweave_lib_intervals:new(),
    %% Chunks claimed one at a time as tasks, from admission until the task
    %% finishes.
    claimed_chunks = 0,
    %% Chunks claimed by footprint reservations that are not yet bound.
    reservation_chunks = 0,
    queued_peer_counts = #{},
    %% The store's tasks that are queued or bound to a peer but not yet
    %% fetching.
    queued_task_count = 0,
    completed_writes_since_sample = 0,
    observed_write_count = undefined,
    sample_started_ms = undefined,
    %% True while fetched chunks have been waiting for the store to write them
    %% for the whole current write sample.
    sample_driven = true,
    write_rate = undefined
}).

%% One store's part of a dispatch plan.
%%
%% Each chunk of a store's work moves through four stages:
%% - queued: claimed for the store and waiting in its work queue;
%% - bound: in a peer queue, waiting to be fetched from that peer;
%% - fetching: a fetch worker is requesting it from the peer;
%% - writing: fetched and held in the chunk cache until the store writes it.
%%
%% Three limits bound those stages. arweave_sync_store sets each limit from an
%% estimate of how long it will take to process the chunks in that stage:
%% - claim_limit/1 limits queued and bound chunks - how many of the store's
%%   unsynced chunks the sweeper claims.
%% - cache_limit limits the store's chunks in the chunk cache, mostly its
%%   writing chunks. When the store is at its cache_limit it takes no new work
%%   and starts no fetches for its bound chunks.
%% - pipeline_limit limits bound and fetching chunks plus those in the chunk
%%   cache. When the store is at its pipeline_limit it takes no new work.
-record(store_plan, {
    state,
    bound_count = 0,
    fetching_count = 0,
    writing_count = 0,
    %% The store's chunks in the shared chunk cache, from sync and from other
    %% producers such as repacking and the disk pool.
    cached_chunk_count = 0,
    cache_limit = 0,
    pipeline_limit = 0,
    disk_ready = false,
    %% The units of work this pass may still try to bind for the store, in
    %% priority order: the queued tasks and footprints from
    %% #store_state.work_queue, plus footprints already bound to a peer that
    %% still have chunks to hand to that peer. The pass removes each unit as it
    %% tries it, so each unit is tried only once per round of binding.
    %% #store_state.work_queue changes only when the pass binds a unit.
    work_queue = gb_sets:new(),
    %% The source peers of the units in #store_state.work_queue, plus the peer
    %% of each bound footprint that still has chunks to hand out. The pass
    %% splits each peer's cap and queue length evenly among the stores that
    %% list the peer and are below their cache and pipeline limits.
    peers = sets:new()
}).

%% The rank of a store for binding its next unit of work; the lowest rank
%% wins. Stores compare field by field, in order:
%% - their chunks bound or fetching (network_count), fewest first;
%% - their chunks bound, fetching or writing (active_count), fewest first;
%% - the store ID, to break ties.
-record(store_priority, {
    network_count,
    active_count,
    store_id
}).

%% The rank of a unit of work in a store's work queue; the lowest rank is taken
%% first. Units compare field by field, in order:
%% - their position (position): the footprint within the partition, or for a
%%   task without a footprint, the matching slice of its partition;
%% - their kind (kind_rank): single-chunk tasks, then bound footprints, then
%%   queued footprints.
-record(work_priority, {
    position,
    kind_rank
}).
