%%% @doc Global scheduler for the network-sync subsystem.
%%%
%%% Each per-StoreID `ar_peer_sync' gen_server discovers work and pushes
%%% #sync_task{} records here (gated by `ready_for_work/0', which bounds this
%%% process's task queue). This server owns admission and dispatch: it
%%% spawn_monitors one transient `ar_data_sync_worker' per task and tracks it by
%%% monitor ref.
%%%
%%% Process liveness is the accounting: inflight count is `map_size(monitors)',
%%% per-peer concurrency is a filter over the same map, and a footprint's global
%%% entropy slot is held exactly while it has an inflight or queued chunk.
%%% A single `'DOWN'' handler releases the slot, the per-peer count, and the
%%% `ar_peer_sync' inflight intervals — so nothing can be leaked or stranded.
-module(ar_sync_dispatcher).
-test_category([fast]).

-behaviour(gen_server).

-export([start_link/0, is_syncing_enabled/0, sync_jobs/0, register_workers/0,
        ready_for_work/0, enqueue/1, default_inflight_limit/0,
        set_entropy_cache_size/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave/include/ar_peers.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

%% Cadence for refreshing the cached network target latency that drives the
%% dynamic per-peer cap. Also doubles as a cheap re-dispatch of freed capacity.
-define(REFRESH_TARGETS_MS, 10_000).
%% Queued + inflight tasks per worker before ar_peer_sync stops pushing.
-define(TASKS_PER_WORKER, 50).
%% Floor for the dynamic per-peer inflight cap.
-define(MIN_INFLIGHT_LIMIT, 8).
%% sync_jobs divisor for scaling the per-peer cap floor above ?MIN_INFLIGHT_LIMIT.
-define(INFLIGHT_LIMIT_DIVISOR, 50).

-record(state, {
    %% Ordered task queue pushed by ar_peer_sync. gb_set of
    %% {FootprintKey, Start, End, Peer, StoreID}; ordered FK-first so footprint
    %% groups are contiguous and normal (FK = none) tasks sort first.
    task_queue = gb_sets:new(),
    %% Ref => #sync_task{} for inflight workers. The source of truth for inflight
    %% work: inflight count is map_size, per-peer concurrency and per-footprint
    %% inflight are filters over it. Runtime-cleaned via the worker monitor's 'DOWN'.
    monitors = #{},
    %% Set of FootprintKeys currently holding an entropy slot (sets:size =<
    %% max_footprints). Membership IS the footprint-budget decision — which
    %% footprints were admitted — and cannot be derived from a
    %% task_queue/monitors snapshot. Added on admission; removed on 'DOWN' once
    %% the footprint has neither an inflight worker nor a queued chunk (both
    %% derived from the structures above), so the slot is held first-chunk..last
    %% yet can't be leaked by a stale counter.
    active_footprints = sets:new(),
    max_footprints,  %% max distinct active footprints (entropy-cache slots)
    max_inflight,    %% max total inflight workers
    target_latency = 0.0
}).

%% Context threaded through one dispatch_fetches pass while admitting a batch of fetches.
-record(admission, {
    inflight,            %% running inflight count (map_size(monitors), ++ per admit)
    peer_counts,         %% Peer => inflight count, ++ per admit
    slots,               %% sets:size(active_footprints), ++ per fresh footprint
    active_footprints,   %% the set, gains a key per fresh footprint
    to_spawn = [],       %% [{SortKey, Concurrency}] selected to spawn this pass
    peer_caps = #{},     %% Peer => cap, memoized for this pass
    disk_ok = #{},       %% StoreID => boolean, memoized for this pass
    max_inflight,        %% constants copied from #state for the pass
    max_footprints,
    target_latency
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Returns true if syncing is enabled (sync_jobs > 0). Pure config read,
%% safe before the server starts (used by supervisors to gate the subtree).
is_syncing_enabled() ->
    sync_jobs() > 0.

sync_jobs() ->
    arweave_config:get([sync, jobs]).

%% @doc Supervisor child spec for the dispatcher. Caller gates on
%% is_syncing_enabled/0.
register_workers() ->
    [?CHILD(?MODULE, worker)].

%% @doc True if the dispatcher can absorb more pushed tasks. ar_peer_sync gates
%% its enqueue loop on this so the task queue stays bounded. Cheap ETS read.
ready_for_work() ->
    try
        ets:lookup_element(?MODULE, active_task_count, 2) < max_tasks()
    catch _:_ ->
        false
    end.

%% @doc Push an ar_peer_sync step's discovered tasks into the dispatch task queue
%% (one cast per step so a step triggers a single dispatch pass, not one per task).
enqueue([]) ->
    ok;
enqueue(SyncTasks) when is_list(SyncTasks) ->
    gen_server:cast(?MODULE, {enqueue, SyncTasks}).

%% @doc Recompute the footprint-slot ceiling after [packing, entropy,
%% cache_size] changes at runtime.
set_entropy_cache_size(_V) ->
    gen_server:cast(?MODULE, recompute_max_footprints).

default_inflight_limit() ->
    max(?MIN_INFLIGHT_LIMIT, sync_jobs() div ?INFLIGHT_LIMIT_DIVISOR).

calculate_max_footprints() ->
    EntropyCacheSizeMiB = arweave_config:get([packing, entropy, cache_size]),
    FootprintSize = ar_block:get_replica_2_9_footprint_size(),
    max(1, (EntropyCacheSizeMiB * ?MiB) div FootprintSize).

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init([]) ->
    ?LOG_INFO([{event, init}, {module, ?MODULE}]),
    ets:insert(?MODULE, {active_task_count, 0}),
    %% Recover from a possible prior crash of this server: any ranges its
    %% previous incarnation had in flight are lost, so clear every ar_peer_sync's
    %% inflight intervals to make them re-discoverable. No-op on first boot
    %% (ar_peer_sync instances start after us, with empty state).
    ar_peer_sync:reset_inflight(),
    erlang:send_after(?REFRESH_TARGETS_MS, self(), refresh_targets),
    {ok, refresh_target_latency(#state{
        max_footprints = calculate_max_footprints(),
        max_inflight = max(1, sync_jobs())
    })}.

handle_call(get_state, _From, State) ->
    {reply, {ok, State}, State};
handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, {error, unhandled}, State}.

handle_cast({enqueue, SyncTasks}, State) ->
    {noreply, dispatch_fetches(lists:foldl(fun queue_task/2, State, SyncTasks))};

handle_cast(recompute_max_footprints, State) ->
    {noreply, State#state{ max_footprints = calculate_max_footprints() }};

handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info({'DOWN', Ref, process, _Pid, Reason}, State) ->
    {noreply, on_worker_down(Ref, Reason, State)};

handle_info(refresh_targets, State) ->
    erlang:send_after(?REFRESH_TARGETS_MS, self(), refresh_targets),
    {noreply, dispatch_fetches(refresh_target_latency(State))};

handle_info(Info, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
    {noreply, State}.

terminate(Reason, _State) ->
    ?LOG_INFO([{event, terminate}, {module, ?MODULE},
        {reason, io_lib:format("~p", [Reason])}]),
    ok.

%%%===================================================================
%%% Admission + dispatch.
%%%===================================================================

%% @doc Add a pushed task to the ordered task queue (deduped by full key).
queue_task(Task, State) ->
    Key = sort_key(Task),
    case gb_sets:is_element(Key, State#state.task_queue) of
        true ->
            State;
        false ->
            State#state{
                task_queue = gb_sets:add_element(Key, State#state.task_queue) }
    end.

%% @doc Spawn workers for as many queued tasks as admission allows, then update
%% the active-task count. A single ordered walk from the front admits normal and
%% already-active-footprint tasks first and opens a fresh footprint only when an
%% entropy slot is free; it stops the moment global capacity is exhausted, so in
%% the steady state (inflight already at the cap) the gate below short-circuits
%% and a completion re-dispatches just the freed slot.
dispatch_fetches(State) ->
    #state{ task_queue = TaskQueue, monitors = Monitors, active_footprints = ActiveFootprints,
            max_inflight = MaxInflight, max_footprints = MaxFootprints,
            target_latency = Target } = State,
    %% Gate before spawning so we never start a worker into a full chunk cache.
    Stalled = gb_sets:is_empty(TaskQueue)
        orelse map_size(Monitors) >= MaxInflight
        orelse ar_data_sync:is_chunk_cache_full(),
    case Stalled of
        true ->
            update_active_task_count(State);
        false ->
            Ctx0 = #admission{ inflight = map_size(Monitors),
                    peer_counts = peer_counts(Monitors),
                    slots = sets:size(ActiveFootprints),
                    active_footprints = ActiveFootprints,
                    max_inflight = MaxInflight, max_footprints = MaxFootprints,
                    target_latency = Target },
            Ctx = find_next_tasks(gb_sets:iterator(TaskQueue), Ctx0),
            #admission{ to_spawn = ToSpawn, active_footprints = ActiveFootprints2 } = Ctx,
            TaskQueue2 = lists:foldl(
                fun({Key, _Concurrency}, Acc) -> gb_sets:delete(Key, Acc) end,
                TaskQueue, ToSpawn),
            Monitors2 = spawn_all(lists:reverse(ToSpawn), Monitors),
            update_active_task_count(State#state{ task_queue = TaskQueue2,
                monitors = Monitors2, active_footprints = ActiveFootprints2 })
    end.

%% Walk the ordered task queue, admitting each task, until global capacity is hit.
find_next_tasks(Iter, #admission{ inflight = Inflight, max_inflight = MaxInflight } = Ctx) ->
    case Inflight >= MaxInflight of
        true ->
            Ctx;
        false ->
            case gb_sets:next(Iter) of
                none -> Ctx;
                {Key, Iter2} -> find_next_tasks(Iter2, admit_task(Key, Ctx))
            end
    end.

%% Decide one queued task: record it for spawning (and bump the running
%% tallies) or leave it queued. Per-peer cap and disk sufficiency are computed
%% once per peer/store and cached in the admission context.
admit_task({FK, _S, _E, Peer, StoreID} = Key, Ctx) ->
    #admission{ peer_counts = PC, slots = Slots, active_footprints = ActiveFootprints,
        to_spawn = ToSpawn, inflight = Inflight, max_footprints = MaxFootprints,
        target_latency = Target } = Ctx,
    {DiskSufficient, Ctx1} = disk_ok_for(StoreID, Ctx),
    case DiskSufficient of
        false ->
            Ctx1;
        true ->
            {Cap, Ctx2} = peer_cap_for(Peer, Target, Ctx1),
            PeerCount = maps:get(Peer, PC, 0),
            Admit = case PeerCount < Cap of
                false -> false;
                true -> admit_footprint(FK, ActiveFootprints, Slots, MaxFootprints)
            end,
            case Admit of
                false ->
                    Ctx2;
                {ok, SlotDelta} ->
                    ActiveFootprints2 = case FK of
                        none -> ActiveFootprints;
                        _ -> sets:add_element(FK, ActiveFootprints)
                    end,
                    Ctx2#admission{ inflight = Inflight + 1,
                        peer_counts = maps:put(Peer, PeerCount + 1, PC),
                        slots = Slots + SlotDelta, active_footprints = ActiveFootprints2,
                        to_spawn = [{Key, PeerCount + 1} | ToSpawn] }
            end
    end.

disk_ok_for(StoreID, #admission{ disk_ok = Cache } = Ctx) ->
    case Cache of
        #{ StoreID := OK } ->
            {OK, Ctx};
        _ ->
            OK = ar_data_sync:is_disk_space_sufficient(StoreID) =:= true,
            {OK, Ctx#admission{ disk_ok = Cache#{ StoreID => OK } }}
    end.

peer_cap_for(Peer, Target, #admission{ peer_caps = Cache } = Ctx) ->
    case Cache of
        #{ Peer := Cap } ->
            {Cap, Ctx};
        _ ->
            Cap = case ar_peers:get_peer_performances([Peer]) of
                #{ Peer := Perf } -> peer_cap(Perf, Target);
                _ -> default_inflight_limit()
            end,
            {Cap, Ctx#admission{ peer_caps = Cache#{ Peer => Cap } }}
    end.

%% @doc Cap = floor scaled up for peers faster than the network target latency,
%% clamped to [floor, ceiling]. A pure function of the peer's live ar_peers
%% rating — no stored per-peer integer, so it responds on the next admission and
%% cannot leak. Bad peers sit at the floor; the rate limiter / not-being-offered
%% path handles genuinely unhealthy peers upstream (see ar_peer_sync).
peer_cap(_Perf, Target) when Target =< 0.0 ->
    default_inflight_limit();
peer_cap(#performance{ average_latency = Latency }, _Target) when Latency =< 0.0 ->
    default_inflight_limit();
peer_cap(#performance{ average_latency = Latency }, Target) ->
    Floor = default_inflight_limit(),
    Ceiling = max(Floor, sync_jobs()),
    arweave_util:between(round(Floor * (Target / Latency)), Floor, Ceiling).

%% Footprint admission: normal tasks need no slot; an active footprint
%% piggybacks; a fresh footprint needs a free slot.
admit_footprint(none, _ActiveFootprints, _Slots, _MaxFootprints) ->
    {ok, 0};
admit_footprint(FK, ActiveFootprints, Slots, MaxFootprints) ->
    case sets:is_element(FK, ActiveFootprints) of
        true -> {ok, 0};
        false when Slots < MaxFootprints -> {ok, 1};
        false -> false
    end.

spawn_all([], Monitors) ->
    Monitors;
spawn_all([{{FK, Start, End, Peer, StoreID}, Concurrency} | Rest], Monitors) ->
    Task = #sync_task{ start_offset = Start, end_offset = End, peer = Peer,
        store_id = StoreID, footprint_key = FK },
    {_Pid, Ref} = spawn_monitor(ar_data_sync_worker, run, [Task, Concurrency]),
    spawn_all(Rest, maps:put(Ref, Task, Monitors)).

%% @doc A worker exited (done, failed, or crashed). Release its overlay range
%% (the single release path), its footprint slot if the footprint is now
%% drained, and refill the freed capacity.
on_worker_down(Ref, Reason, State) ->
    case maps:take(Ref, State#state.monitors) of
        error ->
            State;
        {#sync_task{ peer = Peer, store_id = StoreID, footprint_key = FK,
                start_offset = Start, end_offset = End }, Monitors2} ->
            log_if_crash(Peer, Start, End, Reason),
            ar_peer_sync:release_task_range(StoreID, Start, End),
            ActiveFootprints2 = release_footprint(
                FK, Monitors2, State#state.task_queue, State#state.active_footprints),
            dispatch_fetches(State#state{ monitors = Monitors2, active_footprints = ActiveFootprints2 })
    end.

%% Drop a footprint's entropy slot only once it has neither an inflight worker
%% (in Monitors) nor a queued chunk (in TaskQueue); otherwise keep it (sticky)
%% so the entropy stays amortized across the footprint's whole lifetime. Both
%% conditions are derived from the source-of-truth structures, so the slot is
%% held exactly while the footprint has work and can't be leaked by a stale
%% counter. Monitors excludes the just-removed worker; TaskQueue never holds the
%% inflight task (it was deleted at spawn), so this sees only sibling chunks.
release_footprint(none, _Monitors, _TaskQueue, ActiveFootprints) ->
    ActiveFootprints;
release_footprint(FK, Monitors, TaskQueue, ActiveFootprints) ->
    case footprint_inflight(FK, Monitors)
            orelse footprint_queued(FK, TaskQueue) of
        true -> ActiveFootprints;
        false -> sets:del_element(FK, ActiveFootprints)
    end.

footprint_inflight(FK, Monitors) ->
    lists:any(fun(#sync_task{ footprint_key = F }) -> F =:= FK end,
        maps:values(Monitors)).

%% The task queue is ordered FK-first, so seek to just below this footprint's
%% group and check whether the next element belongs to it (O(log n)). The
%% lower bound uses -1 in the start_offset slot, which is below any real
%% (non-negative) offset, so it lands at the head of exactly this FK's group.
footprint_queued(FK, TaskQueue) ->
    case gb_sets:next(gb_sets:iterator_from({FK, -1, 0, 0, 0}, TaskQueue)) of
        {{FK, _, _, _, _}, _} -> true;
        _ -> false
    end.

%%%===================================================================
%%% Network target latency (drives the dynamic per-peer cap).
%%%===================================================================

refresh_target_latency(State) ->
    Peers = distinct_peers(gb_sets:to_list(State#state.task_queue),
        State#state.monitors),
    Perfs = ar_peers:get_peer_performances(Peers),
    {Sum, Count} = maps:fold(
        fun(_Peer, #performance{ average_latency = L }, {S, C}) ->
            {S + L, C + 1}
        end, {0.0, 0}, Perfs),
    Target = case Count of 0 -> 0.0; _ -> Sum / Count end,
    State#state{ target_latency = Target }.

%%%===================================================================
%%% Helpers.
%%%===================================================================

max_tasks() ->
    sync_jobs() * ?TASKS_PER_WORKER.

%% @doc The gb_set ordering key for a task: FK-first so footprint groups are
%% contiguous (normal tasks, FK = none, sort first), then by byte offset.
sort_key(#sync_task{ footprint_key = FK, start_offset = Start, end_offset = End,
        peer = Peer, store_id = StoreID }) ->
    {FK, Start, End, Peer, StoreID}.

peer_counts(Monitors) ->
    maps:fold(fun(_Ref, #sync_task{ peer = Peer }, Acc) ->
        maps:update_with(Peer, fun(N) -> N + 1 end, 1, Acc)
    end, #{}, Monitors).

%% @doc Distinct peers across queued (sort keys) + in-flight (#sync_task)
%% tasks — the population whose mean latency sets the per-peer cap. Both
%% sources matter: dropping the queued peers would bias the mean toward
%% whoever is in-flight and shrink fast peers' caps over time.
distinct_peers(QueuedKeys, Monitors) ->
    FromQueue = [Peer || {_FK, _S, _E, Peer, _Sid} <- QueuedKeys],
    FromMonitors = [Peer || #sync_task{ peer = Peer } <- maps:values(Monitors)],
    lists:usort(FromQueue ++ FromMonitors).

update_active_task_count(State) ->
    Load = gb_sets:size(State#state.task_queue) + map_size(State#state.monitors),
    catch ets:insert(?MODULE, {active_task_count, Load}),
    State.

log_if_crash(_Peer, _Start, _End, normal) ->
    ok;
log_if_crash(Peer, Start, End, Reason) ->
    ?LOG_WARNING([{event, sync_worker_crash}, {module, ?MODULE},
        {peer, arweave_util:format_peer(Peer)}, {start_offset, Start},
        {end_offset, End}, {reason, io_lib:format("~p", [Reason])}]).

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").

%% Admission decisions are pure (find_next_tasks/admit_task produce a spawn list with no side
%% effects), so the footprint-budget / per-peer-cap / footprint invariants are tested
%% directly, without the gen_server or real workers.
decision_test_() ->
    ar_test_util:with_mocked([
        {arweave_config, get,
            fun([sync, jobs]) -> 100; (K) -> meck:passthrough([K]) end},
        {ar_data_sync, is_disk_space_sufficient,
            fun(no_disk) -> false; (_) -> true end},
        {ar_peers, get_peer_performances, fun mock_perfs/1}
    ], fun() ->
        test_global_limit(),
        test_footprint_budget(),
        test_footprint_piggyback(),
        test_per_peer_cap(),
        test_disk_gate(),
        test_release_footprint()
    end, 30).

%% Each peer's average latency is its port, so the cap is deterministic.
mock_perfs(Peers) ->
    maps:from_list([{P, #performance{ average_latency = element(5, P) }}
        || P <- Peers]).

%% Build a queued task in the dispatcher's sort-key form — the
%% {FootprintKey, Start, End, Peer, StoreID} tuple find_next_tasks walks (not a
%% #sync_task{}; admit_task operates on these keys, spawn_all reconstructs the
%% record afterwards).
normal_task(Peer, Offset) ->
    {none, Offset, Offset + 100, Peer, store1}.

footprint_task(FootprintKey, Peer, Offset) ->
    {FootprintKey, Offset, Offset + 100, Peer, store1}.

base_admission() ->
    #admission{ inflight = 0, peer_counts = #{}, slots = 0,
        active_footprints = sets:new(), max_inflight = 1000, max_footprints = 1000,
        target_latency = 0.0 }.

queue(Tasks) ->
    gb_sets:iterator(gb_sets:from_list(Tasks)).

%% Global inflight cap binds even when per-peer/footprint admission would allow more.
test_global_limit() ->
    P = {1, 1, 1, 1, 9},
    Tasks = [normal_task(P, I * 100) || I <- lists:seq(1, 10)],
    Ctx = find_next_tasks(queue(Tasks), (base_admission())#admission{ max_inflight = 3 }),
    ?assertEqual(3, length(Ctx#admission.to_spawn)).

%% At most `max_footprints' distinct footprints are admitted (I1).
test_footprint_budget() ->
    P = {1, 1, 1, 1, 9},
    Tasks = [footprint_task({fp, I, P}, P, I * 1000) || I <- lists:seq(1, 5)],
    Ctx = find_next_tasks(queue(Tasks),
        (base_admission())#admission{ max_footprints = 2, max_inflight = 100 }),
    ?assertEqual(2, length(Ctx#admission.to_spawn)),
    ?assertEqual(2, sets:size(Ctx#admission.active_footprints)).

%% Tasks of an already-admitted footprint piggyback on its slot (no extra slot).
test_footprint_piggyback() ->
    P = {1, 1, 1, 1, 9},
    FK = {fp, 1, P},
    Tasks = [footprint_task(FK, P, I * 100) || I <- lists:seq(1, 3)],
    Ctx = find_next_tasks(queue(Tasks),
        (base_admission())#admission{ max_footprints = 1, max_inflight = 100 }),
    ?assertEqual(3, length(Ctx#admission.to_spawn)),
    ?assertEqual([FK], sets:to_list(Ctx#admission.active_footprints)).

%% A faster peer (lower latency) gets a higher dynamic cap than a slow one.
test_per_peer_cap() ->
    Good = {1, 1, 1, 1, 25},   %% latency 25, 4x faster than target -> cap 32
    Bad = {2, 2, 2, 2, 100},   %% latency 100 == target -> cap floor 8
    Tasks = [normal_task(Good, I * 100) || I <- lists:seq(1, 50)]
        ++ [normal_task(Bad, 1000000 + I * 100) || I <- lists:seq(1, 50)],
    Ctx = find_next_tasks(queue(Tasks),
        (base_admission())#admission{ target_latency = 100.0, max_inflight = 1000 }),
    ToSpawn = Ctx#admission.to_spawn,
    GoodCount = length([ok || {{_, _, _, Pk, _}, _} <- ToSpawn, Pk == Good]),
    BadCount = length([ok || {{_, _, _, Pk, _}, _} <- ToSpawn, Pk == Bad]),
    ?assertEqual(32, GoodCount),
    ?assertEqual(8, BadCount).

%% A store without sufficient disk space admits nothing.
test_disk_gate() ->
    P = {1, 1, 1, 1, 9},
    %% store_id = no_disk -> is_disk_space_sufficient/1 returns false (see mock).
    Tasks = [{none, I * 100, I * 100 + 100, P, no_disk} || I <- lists:seq(1, 5)],
    Ctx = find_next_tasks(queue(Tasks), base_admission()),
    ?assertEqual([], Ctx#admission.to_spawn).

%% A footprint's slot is released only when it has neither an inflight worker
%% nor a queued chunk; otherwise it is held (sticky) — I2. Both conditions are
%% derived from monitors / task_queue, so there is no count to drift.
test_release_footprint() ->
    FK = {fp, 1},
    ActiveFootprints = sets:from_list([FK]),
    Inflight = #{ make_ref() => #sync_task{ peer = peer, store_id = store1,
        footprint_key = FK, start_offset = 0, end_offset = 100 } },
    Queued = gb_sets:from_list([{FK, 0, 100, peer, store1}]),
    Empty = gb_sets:new(),
    %% In-flight worker remains -> slot kept.
    ?assert(sets:is_element(FK, release_footprint(FK, Inflight, Empty, ActiveFootprints))),
    %% No inflight, but a sibling chunk is still queued -> slot kept (sticky).
    ?assert(sets:is_element(FK, release_footprint(FK, #{}, Queued, ActiveFootprints))),
    %% Neither inflight nor queued -> slot released.
    ?assertNot(sets:is_element(FK, release_footprint(FK, #{}, Empty, ActiveFootprints))),
    %% Normal (none) tasks never touch the active_footprints set.
    ?assertEqual(ActiveFootprints, release_footprint(none, #{}, Empty, ActiveFootprints)).

%% peer_cap value: floor for no-target / degenerate / slow peers, scaled up for
%% faster-than-target peers, clamped to [floor, sync_jobs].
peer_cap_test_() ->
    ar_test_util:with_mocked([
        {arweave_config, get,
            fun([sync, jobs]) -> 100; (K) -> meck:passthrough([K]) end}
    ], fun() ->
        Perf = fun(L) -> #performance{ average_latency = L } end,
        ?assertEqual(8, default_inflight_limit()),       %% floor sanity
        ?assertEqual(8, peer_cap(Perf(50.0), 0.0)),      %% no target yet -> floor
        ?assertEqual(8, peer_cap(Perf(0.0), 100.0)),     %% degenerate latency -> floor
        ?assertEqual(8, peer_cap(Perf(200.0), 100.0)),   %% slower than target -> floor
        ?assertEqual(16, peer_cap(Perf(50.0), 100.0)),   %% 2x faster -> 8 * 100/50
        ?assertEqual(100, peer_cap(Perf(1.0), 100.0))    %% far faster -> clamp to sync_jobs
    end, 30).

%% footprint_queued finds a footprint's chunks via the FK-first ordered seek,
%% even with a leading normal task and an adjacent footprint group present.
footprint_queued_test() ->
    FK1 = {fp, 1},
    FK2 = {fp, 2},
    Q = gb_sets:from_list([
        {none, 0, 100, p, s},
        {FK1, 0, 100, p, s},
        {FK1, 100, 200, p, s},
        {FK2, 0, 100, p, s}
    ]),
    ?assert(footprint_queued(FK1, Q)),
    ?assert(footprint_queued(FK2, Q)),
    ?assertNot(footprint_queued({fp, 3}, Q)),
    ?assertNot(footprint_queued(FK1, gb_sets:new())).

%% End-to-end: enqueue a mix of normal + footprint tasks; immediate-exit workers
%% drive the spawn -> 'DOWN' -> release -> refill cycle to completion. Asserts no
%% residue (no leak): every structure is empty once the work drains.
no_leak_test_() ->
    ar_test_util:with_mocked([
        {arweave_config, get,
            fun([sync, jobs]) -> 100;
                ([packing, entropy, cache_size]) -> 1000000;
                (K) -> meck:passthrough([K]) end},
        {ar_data_sync, is_chunk_cache_full, fun() -> false end},
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end},
        {ar_peers, get_peer_performances, fun mock_perfs/1},
        {ar_data_sync_worker, run, fun(_Task, _Concurrency) -> ok end},
        {ar_peer_sync, reset_inflight, fun() -> ok end},
        {ar_peer_sync, release_task_range, fun(_, _, _) -> ok end}
    ], fun test_no_leak/0, 30).

test_no_leak() ->
    drains_clean().

%% Same no-residue guarantee when workers CRASH (exit abnormally) rather than
%% complete: the monitor 'DOWN' must still release the slot, the per-peer count,
%% and the byte range. This is the core premise of the monitor-based design.
crash_no_leak_test_() ->
    ar_test_util:with_mocked([
        {arweave_config, get,
            fun([sync, jobs]) -> 100;
                ([packing, entropy, cache_size]) -> 1000000;
                (K) -> meck:passthrough([K]) end},
        {ar_data_sync, is_chunk_cache_full, fun() -> false end},
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end},
        {ar_peers, get_peer_performances, fun mock_perfs/1},
        {ar_data_sync_worker, run, fun(_Task, _Concurrency) -> exit(simulated_crash) end},
        {ar_peer_sync, reset_inflight, fun() -> ok end},
        {ar_peer_sync, release_task_range, fun(_, _, _) -> ok end}
    ], fun drains_clean/0, 30).

%% Drive a mix of normal + footprint tasks through the gen_server (worker
%% behaviour set by the caller's mock) and assert no residue once work drains.
drains_clean() ->
    ensure_table(),
    %% Unregistered (the node under test already owns the registered name);
    %% drive it directly by pid.
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    try
        Peers = [{1, 1, 1, 1, 9}, {2, 2, 2, 2, 9}],
        Tasks = [#sync_task{
                start_offset = I * 100, end_offset = I * 100 + 100,
                peer = lists:nth((I rem 2) + 1, Peers), store_id = store1,
                footprint_key = case I rem 3 of
                    0 -> none;
                    R -> {fp, R, store1}
                end }
            || I <- lists:seq(1, 30)],
        gen_server:cast(Pid, {enqueue, Tasks}),
        ok = ar_test_await:until(dispatcher_drained, fun() ->
            {ok, S} = gen_server:call(Pid, get_state),
            gb_sets:is_empty(S#state.task_queue)
                andalso map_size(S#state.monitors) == 0
        end),
        {ok, State} = gen_server:call(Pid, get_state),
        ?assertEqual(0, map_size(State#state.monitors)),
        ?assertEqual(0, sets:size(State#state.active_footprints)),
        ?assert(gb_sets:is_empty(State#state.task_queue))
    after
        gen_server:stop(Pid)
    end.

%% A full chunk cache stalls dispatch (nothing spawned, tasks stay queued), and
%% re-pushing the same tasks is deduped rather than doubling the queue.
cache_full_test_() ->
    ar_test_util:with_mocked([
        {arweave_config, get,
            fun([sync, jobs]) -> 100;
                ([packing, entropy, cache_size]) -> 1000000;
                (K) -> meck:passthrough([K]) end},
        {ar_data_sync, is_chunk_cache_full, fun() -> true end},
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end},
        {ar_peers, get_peer_performances, fun mock_perfs/1},
        {ar_peer_sync, reset_inflight, fun() -> ok end}
    ], fun test_cache_full/0, 30).

test_cache_full() ->
    ensure_table(),
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    try
        P = {1, 1, 1, 1, 9},
        Tasks = [#sync_task{ start_offset = I * 100, end_offset = I * 100 + 100,
                peer = P, store_id = store1, footprint_key = none }
            || I <- lists:seq(1, 5)],
        gen_server:cast(Pid, {enqueue, Tasks}),
        gen_server:cast(Pid, {enqueue, Tasks}),   %% duplicate push
        {ok, State} = gen_server:call(Pid, get_state),   %% syncs both casts
        ?assertEqual(0, map_size(State#state.monitors)),       %% cache full -> nothing spawned
        ?assertEqual(5, gb_sets:size(State#state.task_queue))  %% deduped, not 10
    after
        gen_server:stop(Pid)
    end.

ensure_table() ->
    case ets:info(?MODULE) of
        undefined -> ets:new(?MODULE, [named_table, public, set]);
        _ -> ok
    end.

-endif.
