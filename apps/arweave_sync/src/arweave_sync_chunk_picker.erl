%%% @doc Pure(ish) chunk-fetch selection logic for the network sync subsystem.
%%%
%%% Given a store's locally-needed unsynced ranges and cached peer
%%% advertisements, this module decides WHAT is fetchable from WHOM. It
%%% intersects each peer's intervals with local need to produce #task_source{}s
%%% and spends the per-step chunk budget over them.
%%%
%%% It holds no process state: arweave_sync_store_sweeper owns the sweep cursors, and
%%% arweave_sync_scheduler owns claims, admission, and peer selection.
-module(arweave_sync_chunk_picker).

-ifdef(AR_TEST).
%% Focused tests live outside the production module.
-export([
    add_task_source/2,
    build_task_source/2,
    build_tasks/3
]).
-endif.

-export([claim_ranges/3]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").

%%%===================================================================
%%% Per-peer fetchable-interval computation.
%%%===================================================================

%% @doc Resolve a sweep step's unsynced ranges into work the scheduler has
%% admitted, returning how many chunks were claimed.
%% A successful partial batch also returns the next claim's starting offset.
%%
%% `blocked' means the store has no admission headroom, so no later range in the
%% store can be admitted either. PeerRanges is an immutable discovery-cache
%% snapshot supplied by the sweeper.
%%
%% The headroom query is advisory; it only bounds how many candidates are worth
%% building. Exact claims and final admission are one atomic scheduler
%% operation, so the scheduler stays authoritative if capacity moved in
%% between. Candidates are built out here rather than inside the scheduler to
%% keep every store's interval work off that single process.
claim_ranges(_StoreID, [], _PeerRanges) ->
    {ok, 0};
claim_ranges(StoreID, UnsyncedRanges, PeerRanges) ->
    case arweave_sync_scheduler:admission_headroom(StoreID) of
        0 ->
            blocked;
        Headroom ->
            TaskSources = find_task_sources(UnsyncedRanges, PeerRanges),
            {Tasks, NextClaimOffset} = build_tasks(
                StoreID, TaskSources, Headroom),
            case arweave_sync_scheduler:claim_and_enqueue(StoreID, Tasks) of
                blocked ->
                    blocked;
                {ok, ClaimedChunks} when is_integer(NextClaimOffset) ->
                    {ok, ClaimedChunks, NextClaimOffset};
                {ok, ClaimedChunks} ->
                    {ok, ClaimedChunks}
            end
    end.

find_task_sources(UnsyncedRanges, PeerRanges) ->
    TaskSourcesByKey = lists:foldl(
        fun(UnsyncedRange, Acc) ->
            build_task_sources(UnsyncedRange, PeerRanges, Acc)
        end,
        #{},
        UnsyncedRanges),
    maps:values(TaskSourcesByKey).

%% @doc Build task sources by intersecting peer advertisements with the chunks
%% the local store still needs. Sources from the same peer and footprint are
%% consolidated by unioning their intervals in TaskSourcesByKey.
build_task_sources(UnsyncedRange, PeerRanges, TaskSourcesByKey) ->
    lists:foldl(
        fun(PeerRange, Acc) ->
            TaskSource = build_task_source(UnsyncedRange, PeerRange),
            add_task_source(TaskSource, Acc)
        end,
        TaskSourcesByKey,
        PeerRanges).

build_task_source(UnsyncedRange, PeerRange) ->
    #peer_range{ peer = Peer,
        intervals = AdvertisedIntervals, footprint = Footprint } = PeerRange,
    #unsynced_range{ intervals = UnsyncedIntervals } = UnsyncedRange,
    Intervals = ar_intervals:intersection(
        AdvertisedIntervals, UnsyncedIntervals),
    #task_source{
        peer = Peer,
        footprint = Footprint,
        intervals = Intervals
    }.

%% @doc Add a task source keyed by peer and footprint, merging intervals when
%% multiple unsynced ranges produce the same key. Empty sources are ignored.
add_task_source(TaskSource, TaskSourcesByKey) ->
    #task_source{ intervals = Intervals } = TaskSource,
    case ar_intervals:is_empty(Intervals) of
        true -> TaskSourcesByKey;
        false -> do_add_task_source(TaskSource, TaskSourcesByKey)
    end.

do_add_task_source(TaskSource, TaskSourcesByKey) ->
    Key = {TaskSource#task_source.peer, TaskSource#task_source.footprint},
    maps:update_with(
        Key,
        fun(ExistingSource) ->
            ExistingSource#task_source{
                intervals = ar_intervals:union(
                    ExistingSource#task_source.intervals,
                    TaskSource#task_source.intervals)
            }
        end,
        TaskSource,
        TaskSourcesByKey).

%% @doc Build byte-source tasks and one reservation per footprint group.
%%
%% Byte advertisements produce ordinary tasks. Footprint advertisements produce
%% one reservation per footprint; the scheduler selects one footprint source
%% and only then expands its intervals into tasks. Both may describe the same
%% chunks because the scheduler's exact store claims resolve that overlap.
build_tasks(_StoreID, _TaskSources, MaxChunks) when MaxChunks =< 0 ->
    {[], none};
build_tasks(StoreID, TaskSources, MaxChunks) ->
    Groups = sorted_task_groups(TaskSources),
    build_task_groups(StoreID, Groups, MaxChunks, []).

build_task_groups(_StoreID, [], _Remaining, TaskGroups) ->
    {flatten_task_groups(TaskGroups), none};
build_task_groups(StoreID,
        [{#footprint{} = Footprint, _Intervals, Sources} | Rest],
        Remaining, TaskGroups) ->
    Reservation = arweave_sync_footprint:new_reservation(
        StoreID, Footprint, Sources),
    build_task_groups(StoreID, Rest, Remaining,
        [[Reservation] | TaskGroups]);
build_task_groups(StoreID, [{none, Intervals, Sources} | Rest],
        Remaining, TaskGroups) ->
    ByteTasks = build_byte_tasks(Intervals, Remaining, StoreID, Sources),
    case Remaining - length(ByteTasks) of
        0 ->
            NextClaimOffset =
                (lists:last(ByteTasks))#task.offset + ?DATA_CHUNK_SIZE,
            {flatten_task_groups([ByteTasks | TaskGroups]), NextClaimOffset};
        Remaining2 ->
            build_task_groups(StoreID, Rest, Remaining2,
                [ByteTasks | TaskGroups])
    end.

flatten_task_groups(TaskGroups) ->
    lists:append(lists:reverse(TaskGroups)).

%% @doc Organize source intervals by footprint while retaining their peer
%% information. The `none' group holds byte sources independently of footprint
%% work that advertises the same chunks.
sorted_task_groups(TaskSources) ->
    Groups = group_by_footprint(TaskSources),
    GroupList = [
        {Group, Intervals, lists:sort(Sources)}
        || {Group, {Intervals, Sources}} <- maps:to_list(Groups)
    ],
    lists:sort(
        fun(Left, Right) -> interval_sort_key(Left) =< interval_sort_key(Right)
        end,
        GroupList).

interval_sort_key({Group, Intervals, _Sources}) ->
    {_, Start} = ar_intervals:smallest(Intervals),
    KindRank = case Group of
        #footprint{} -> 0;
        none -> 1
    end,
    {Start, KindRank}.

group_by_footprint(TaskSources) ->
    lists:foldl(
        fun(TaskSource, Acc) ->
            #task_source{ footprint = Footprint,
                intervals = Intervals } = TaskSource,
            maps:update_with(
                Footprint,
                fun({ExistingIntervals, ExistingSources}) ->
                    {ar_intervals:union(ExistingIntervals, Intervals),
                        [TaskSource | ExistingSources]}
                end,
                {Intervals, [TaskSource]},
                Acc)
        end,
        #{},
        TaskSources).

%% @doc Build up to MaxChunks byte tasks from an interval set in ascending
%% offset order.
build_byte_tasks(Intervals, MaxChunks, StoreID, TaskSources) ->
    {_, Tasks} = ar_intervals:fold(
        fun(_, {Remaining, TasksAcc}) when Remaining =< 0 ->
                {Remaining, TasksAcc};
            ({End, Start}, {Remaining, TasksAcc}) ->
                RangeEnd = min(End, Start + (Remaining * ?DATA_CHUNK_SIZE)),
                IntervalTasks = build_tasks_for_interval(
                    Start, RangeEnd, StoreID, TaskSources),
                {Remaining - length(IntervalTasks),
                    lists:reverse(IntervalTasks, TasksAcc)}
        end,
        {MaxChunks, []},
        Intervals),
    lists:reverse(Tasks).

%% @doc Expand one contiguous byte interval into one task per request offset.
%% This is separate from build_byte_tasks/4 because ar_intervals:fold/3 iterates
%% intervals, while the scheduler contract requires one task for each chunk.
build_tasks_for_interval(Start, RangeEnd, _StoreID, _TaskSources)
        when Start >= RangeEnd ->
    [];
build_tasks_for_interval(Start, RangeEnd, StoreID, TaskSources) ->
    End = min(Start + ?DATA_CHUNK_SIZE, RangeEnd),
    Task = #task{
        offset = Start,
        sources = sources(Start, TaskSources),
        store_id = StoreID
    },
    [Task | build_tasks_for_interval(End, RangeEnd, StoreID, TaskSources)].

%% @doc Collect every byte peer that advertised the chunk beginning at Start.
%% Footprint peers remain attached to their reservation instead.
sources(Start, TaskSources) ->
    lists:filtermap(
        fun(#task_source{ intervals = Intervals } = TaskSource) ->
            case ar_intervals:is_inside(Intervals, Start + 1) of
                true ->
                    {true, TaskSource#task_source{ intervals = undefined }};
                false ->
                    false
            end
        end,
        TaskSources).
