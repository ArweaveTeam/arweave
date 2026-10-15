-record(state, {
    %% Each store's work queue, claims, write rate and the limits derived from
    %% the write rate (arweave_sync_store).
    stores = arweave_sync_store:new(),
    %% TaskRef => #task{} for each started task, from the start of its fetch
    %% until its write finishes.
    tasks = #{},
    %% MonitorRef => {TaskRef, WorkerPID} for each running fetch worker.
    monitor_index = #{},
    %% The footprint reservations and the entropy slots they hold
    %% (arweave_sync_footprint).
    footprints = arweave_sync_footprint:new(),
    %% Each peer's queue, cap and queue length (arweave_sync_peer).
    peer_state = arweave_sync_peer:new(),
    %% True while a dispatch message is waiting in the mailbox. Events that add
    %% work or free up room (new tasks, fetch results, writes, worker exits)
    %% queue a dispatch only when none is waiting, so a burst of events leads
    %% to a single pass.
    dispatch_scheduled = false,
    %% The download byte balance (arweave_sync_download_limit).
    download_limit = arweave_sync_download_limit:new()
}).

%% The plan of one dispatch pass: copies of the stores, footprints and peers
%% that the pass changes, written back when the pass commits the plan.
-record(dispatch_plan, {
    stores,
    %% Running workers plus the fetches this pass has started.
    worker_count,
    peers,
    footprints,
    %% The tasks whose fetches this pass starts.
    tasks_to_start = []
}).

%% A unit of work to bind to a peer: a #task{} (one chunk) or a
%% #footprint_reservation{} (the chunks of one footprint, which share
%% entropy), with its store and the peers that can serve it.
-record(work, {
    unit,
    store_id,
    sources = []
}).
