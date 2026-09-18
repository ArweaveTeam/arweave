%%% State shared with focused tests.

%% Cadence for the periodic re-dispatch tick (a cheap re-dispatch of freed
%% capacity, and the metric-emit cadence).
-define(TICK_INTERVAL_MS, 10_000).

-record(state, {
    %% Opaque indexed store state.
    stores = arweave_sync_store:new(),
    %% TaskRef => #task{}: dispatched tasks in the fetching, writing, or
    %% write_complete state. A task leaves when its failed fetch or one async
    %% storage handoff reaches a terminal state.
    tasks = #{},
    %% MonitorRef => {TaskRef, WorkerPID}: routes the worker monitor's 'DOWN' to
    %% its task entry and lets the scheduler terminate the workers it owns.
    %% map_size is the inflight worker count.
    monitor_index = #{},
    %% Opaque footprint state.
    footprints = #{},
    %% Peer => FIFO of peer-bound tasks waiting for an active fetch slot.
    peer_queues = #{},
    %% Peer => true once any dispatch in the current scheduler interval sees
    %% runnable work held behind that peer's full active cap.
    driven_peers = #{},
    %% arweave_sync_peer's opaque state, evolved on the scheduler tick.
    peer_state = arweave_sync_peer:new(),
    %% Debounce flag: a dispatch self-cast is already queued. enqueue and
    %% worker-DOWN only mark that a dispatch is needed rather than each running a
    %% full dispatch pass; the single queued dispatch then drains all freed capacity
    %% in one pass. Without this, a burst of DOWN/enqueue messages (multi-peer churn)
    %% runs one expensive selection pass per message, the gen_server falls behind, and
    %% the task map goes stale so the dispatcher wrongly believes it is full.
    dispatch_scheduled = false,
    %% Opaque global download-rate limiter, including its refill and wakeup
    %% state. arweave_sync_download_limit owns the token-bucket behavior.
    download_limit = arweave_sync_download_limit:new()
}).

%% Mutable snapshot built while selecting one dispatch batch;
%% stores, peers, and footprints are updated as queued tasks are selected.
-record(dispatch, {
    %% Opaque indexed store dispatches.
    stores,
    %% live plus selected worker count for this pass
    worker_count,
    %% arweave_sync_peer:dispatch()
    peers,
    %% arweave_sync_footprint:dispatch()
    footprints,
    %% Peer => queue of bound runnable #task{}
    peer_queues,
    %% [#task{}] selected this pass (task_ref minted at spawn)
    tasks_to_start = []
}).

%% A resolved dispatch candidate. Item retains the task or footprint's
%% lifecycle state while the remaining fields present one common selection
%% contract to the scheduler.
-record(work, {
    item,
    store_id,
    sources = []
}).
