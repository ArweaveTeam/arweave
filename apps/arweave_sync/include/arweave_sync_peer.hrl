-include("arweave_sync_peer_cap.hrl").

%% The peer queue holds about four seconds of the peer's goodput.
-define(QUEUE_TARGET_DURATION_MS, 4000).

%%%===================================================================
%%% Records.
%%%===================================================================

%% All of this node's state for one peer. The control and queue fields are
%% kept for every peer seen; the other fields cover the time since the last
%% tick.
-record(peer, {
    control = #cap_control{},
    %% The peer queue: tasks bound to the peer, oldest first, waiting to be
    %% fetched.
    queue = queue:new(),
    %% Updated by add_fetch_result/4: the total chunk bytes fetched from the
    %% peer, and the fetch time since the last tick.
    fetched_bytes = 0,
    fetch_timing = #fetch_timing{},
    %% {FetchedBytes, TimeMs} at the last tick; undefined before the first
    %% counter snapshot.
    last_tick = undefined,
    %% Assigned work remembered for the next tick, including completed work.
    active = false,
    %% Updated by update_activity/3: whether the peer was driven in any
    %% dispatch pass since the last tick, and its latest in-flight count.
    driven = false,
    fetching_count = 0,
    %% The goodput of the peer's most recent goodput samples, newest first, up
    %% to GOODPUT_WINDOW_SAMPLES of them; their mean sizes the peer queue.
    recent_goodputs = []
}).

-record(state, {
    %% Peer => #peer{}, with one entry for each peer seen.
    peers = #{},
    %% Latest tick time in milliseconds, used as the baseline for new peers.
    last_tick_ms = undefined
}).

%% One local store's tasks on a peer during a dispatch pass: the tasks it has in
%% the peer queue or fetching, and its share of the peer's task limit.
-record(store_load, {
    task_count = 0,
    target_task_count = 0
}).

%% One peer's part of a plan.
-record(peer_plan, {
    %% True if this peer was assigned tasks or footprint work during this
    %% dispatch pass. update_activity/3 sets #peer.active when this is true,
    %% preserving activity from earlier passes until the next tick.
    active = false,
    queue = queue:new(),
    fetching_count = 0,
    %% Tasks in the peer queue or fetching.
    task_count = 0,
    concurrency_cap = ?CONCURRENCY_CAP_INITIAL,
    queue_max_length = ?CONCURRENCY_CAP_INITIAL,
    %% StoreID => #store_load{}.
    stores = #{}
}).

%% This module's part of a dispatch plan, from snapshot/3 to commit_plan/2.
-record(plan, {
    %% Peer => #peer_plan{} with retained limits and assigned tasks.
    peers = #{}
}).

%% The rank of a source for binding a task; the lowest rank wins. Sources
%% compare field by field, in order:
%% - their peer's load (peer_load), lowest first;
%% - the store's load on that peer (store_load), lowest first;
%% - the peer, to break ties.
-record(source_priority, {
    peer_load,
    store_load,
    peer
}).
