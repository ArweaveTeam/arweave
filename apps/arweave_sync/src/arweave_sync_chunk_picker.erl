%%% @doc Builds the work for one sweep step of a store by matching the chunks
%%% the store needs against the intervals peers serve. arweave_sync_sweeper
%%% calls claim_ranges/3 with the step's unsynced ranges and a snapshot of peer
%%% ranges from arweave_sync_discovery. claim_ranges/3 then:
%%% 1. Asks arweave_sync_scheduler how many more chunks the store may claim
%%%    under its claim limit (the admission headroom), and returns `blocked'
%%%    when the answer is 0.
%%% 2. Intersects each peer range with the unsynced ranges to build task
%%%    sources (build_task_sources/3): one per peer and footprint, where a
%%%    byte source has footprint none.
%%% 3. Builds work from the task sources in offset order (build_tasks/3): a
%%%    footprint reservation for each footprint, holding the footprint's
%%%    sources, and, up to the headroom, a task for each chunk the byte
%%%    sources serve, holding the byte sources of that chunk.
%%% 4. Offers the work to arweave_sync_scheduler:claim_and_enqueue/2, which
%%%    claims the chunks and adds the work to the store's work queue.
%%%
%%% When the tasks reach the headroom, claim_ranges/3 also returns the offset
%%% where the next claim for the step resumes. The module keeps no state.
-module(arweave_sync_chunk_picker).

-ifdef(AR_TEST).
-export([
    add_task_source/2,
    build_task_sources/3,
    build_tasks/3
]).
-endif.

-export([claim_ranges/3]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Turn a sweep step's unsynced ranges into work for the scheduler,
%% returning the number of chunks claimed (and, after a partial claim, the
%% offset to resume from), or `blocked' when the store can claim no more.
claim_ranges(_StoreID, [], _PeerRanges) ->
    {ok, 0};
claim_ranges(StoreID, UnsyncedRanges, PeerRanges) ->
    %% The headroom only limits how much work is worth building. The scheduler
    %% makes the exact claims in one atomic step, so it stays correct if the
    %% headroom changed in the meantime. Building the work here rather than in
    %% the scheduler keeps every store's interval work off that single process.
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

%%%===================================================================
%%% Task sources.
%%%===================================================================

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
    UnsyncedFootprintIntervals = unsynced_footprint_intervals(
        UnsyncedRange, PeerRanges),
    lists:foldl(
        fun(PeerRange, Acc) ->
            TaskSource = build_task_source(
                UnsyncedRange, PeerRange, UnsyncedFootprintIntervals),
            add_task_source(TaskSource, Acc)
        end,
        TaskSourcesByKey,
        PeerRanges).

%% @doc Map each advertised footprint to its unsynced intervals in
%% footprint-record space, where a run of chunks is one interval.
unsynced_footprint_intervals(UnsyncedRange, PeerRanges) ->
    #unsynced_range{ intervals = UnsyncedIntervals } = UnsyncedRange,
    Footprints = lists:usort([PeerRange#peer_range.footprint
        || PeerRange <- PeerRanges,
            PeerRange#peer_range.footprint =/= none]),
    lists:foldl(
        fun(Footprint, Acc) ->
            #footprint{ partition = Partition, footprint = Number } = Footprint,
            Intervals =
                arweave_lib_footprint:byte_intervals_to_footprint_intervals(
                    UnsyncedIntervals, Partition, Number),
            maps:put(Footprint, Intervals, Acc)
        end,
        #{},
        Footprints).

build_task_source(UnsyncedRange,
        #peer_range{ footprint = none } = PeerRange,
        _UnsyncedFootprintIntervals) ->
    #peer_range{ peer = Peer, intervals = AdvertisedIntervals } = PeerRange,
    #unsynced_range{ intervals = UnsyncedIntervals } = UnsyncedRange,
    Intervals = arweave_lib_intervals:intersection(
        AdvertisedIntervals, UnsyncedIntervals),
    #task_source{
        peer = Peer,
        footprint = none,
        intervals = Intervals
    };
build_task_source(UnsyncedRange, PeerRange, UnsyncedFootprintIntervals) ->
    #peer_range{ peer = Peer,
        intervals = AdvertisedIntervals, footprint = Footprint } = PeerRange,
    #unsynced_range{ intervals = UnsyncedIntervals } = UnsyncedRange,
    %% Intersect in footprint-record space first, then convert only the
    %% fetchable chunks to byte intervals. Converting the whole advertisement
    %% would produce one byte interval per chunk.
    FetchableFootprintIntervals = arweave_lib_intervals:intersection(
        AdvertisedIntervals, maps:get(Footprint, UnsyncedFootprintIntervals)),
    Intervals = arweave_lib_intervals:intersection(
        arweave_lib_footprint:footprint_intervals_to_byte_intervals(
            FetchableFootprintIntervals),
        UnsyncedIntervals),
    #task_source{
        peer = Peer,
        footprint = Footprint,
        intervals = Intervals
    }.

%% @doc Add a task source keyed by peer and footprint, merging intervals when
%% multiple unsynced ranges produce the same key. Empty sources are ignored.
add_task_source(TaskSource, TaskSourcesByKey) ->
    #task_source{ intervals = Intervals } = TaskSource,
    case arweave_lib_intervals:is_empty(Intervals) of
        true -> TaskSourcesByKey;
        false -> do_add_task_source(TaskSource, TaskSourcesByKey)
    end.

do_add_task_source(TaskSource, TaskSourcesByKey) ->
    Key = {TaskSource#task_source.peer, TaskSource#task_source.footprint},
    maps:update_with(
        Key,
        fun(ExistingSource) ->
            ExistingSource#task_source{
                intervals = arweave_lib_intervals:union(
                    ExistingSource#task_source.intervals,
                    TaskSource#task_source.intervals)
            }
        end,
        TaskSource,
        TaskSourcesByKey).

%%%===================================================================
%%% Work.
%%%===================================================================

%% @doc Build byte-source tasks and one reservation per footprint group.
%%
%% Byte advertisements produce ordinary tasks. Footprint advertisements produce
%% one reservation per footprint; the scheduler selects one footprint source
%% and only then expands its intervals into tasks. Both may describe the same
%% chunks because the scheduler's exact store claims resolve that overlap.
build_tasks(_StoreID, _TaskSources, MaxChunks) when MaxChunks =< 0 ->
    {[], none};
build_tasks(StoreID, TaskSources, MaxChunks) ->
    %% Byte tasks and reservations may cover the same chunks; the scheduler's
    %% claims on the store resolve the overlap.
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
    {_, Start} = arweave_lib_intervals:smallest(Intervals),
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
                    {arweave_lib_intervals:union(ExistingIntervals, Intervals),
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
    {_, Tasks} = arweave_lib_intervals:fold(
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
%% This is separate from build_byte_tasks/4 because
%% arweave_lib_intervals:fold/3 iterates intervals, while the scheduler
%% contract requires one task for each chunk.
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

%% @doc Return the byte sources that advertise the chunk starting at Start.
sources(Start, TaskSources) ->
    lists:filtermap(
        fun(#task_source{ intervals = Intervals } = TaskSource) ->
            case arweave_lib_intervals:is_inside(Intervals, Start + 1) of
                true ->
                    {true, TaskSource#task_source{ intervals = undefined }};
                false ->
                    false
            end
        end,
        TaskSources).
