%%% @doc Pure(ish) chunk-fetch selection logic for the network sync subsystem.
%%%
%%% Given a storage module's locally-needed unsynced ranges and cached peer
%%% advertisements, this module decides WHAT is fetchable from WHOM. It
%%% intersects each peer's intervals with local need to produce #task_source{}s
%%% and spends the per-step chunk budget over them.
%%%
%%% It holds no process state: ar_sync_store_sweeper owns the sweep cursors, and
%%% ar_sync_scheduler owns claims, admission, and peer selection.
-module(ar_sync_chunk_picker).

-export([claim_ranges/3]).

-include("ar.hrl").
-include("ar_sync.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
    case ar_sync_scheduler:admission_headroom(StoreID) of
        0 ->
            blocked;
        Headroom ->
            TaskSources = find_task_sources(UnsyncedRanges, PeerRanges),
            {Tasks, NextClaimOffset} = build_tasks(
                StoreID, TaskSources, Headroom),
            case ar_sync_scheduler:claim_and_enqueue(StoreID, Tasks) of
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
    Reservation = ar_sync_footprint:new_reservation(
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

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).

build_byte_task_source_test() ->
    Peer = {1, 2, 3, 4, 1984},
    UnsyncedRange = #unsynced_range{
        kind = byte,
        query_offset = 2,
        intervals = ar_intervals:from_list([{6, 2}]),
        range_start = 2,
        range_end = 8,
        advance = done
    },
    PeerRange = #peer_range{
        peer = Peer,
        offset = 2,
        intervals = ar_intervals:from_list([{8, 4}]),
        footprint = none
    },
    #task_source{ peer = Peer, footprint = none, intervals = Intervals } =
        build_task_source(UnsyncedRange, PeerRange),
    ?assertEqual([{6, 4}], ar_intervals:to_list(Intervals)).

add_task_source_merges_overlapping_intervals_test() ->
    Peer = {1, 2, 3, 4, 1984},
    Chunk = ?DATA_CHUNK_SIZE,
    First = #task_source{
        peer = Peer,
        intervals = ar_intervals:from_list([{2 * Chunk, 0}]),
        footprint = none
    },
    Second = #task_source{
        peer = Peer,
        intervals = ar_intervals:from_list([{3 * Chunk, Chunk}]),
        footprint = none
    },
    TaskSourcesByKey = add_task_source(
        Second, add_task_source(First, #{})),
    [Consolidated] = maps:values(TaskSourcesByKey),
    %% Two overlapping two-chunk ranges form one contiguous three-chunk range.
    ?assertEqual([{3 * Chunk, 0}],
        ar_intervals:to_list(Consolidated#task_source.intervals)).

add_task_source_ignores_empty_intervals_test() ->
    TaskSource = #task_source{ intervals = ar_intervals:new() },
    ?assertEqual(#{}, add_task_source(TaskSource, #{})).

build_footprint_task_source_test() ->
    Peer = {1, 2, 3, 4, 1984},
    Footprint = #footprint{ store_id = test_store, partition = 0,
        footprint = 1 },
    UnsyncedRange = #unsynced_range{
        kind = byte,
        query_offset = 2,
        intervals = ar_intervals:from_list([{6, 3}]),
        range_start = 2,
        range_end = 8,
        advance = done
    },
    PeerRange = #peer_range{
        peer = Peer,
        offset = 2,
        intervals = ar_intervals:from_list([{7, 2}]),
        footprint = Footprint
    },
    #task_source{ peer = Peer, footprint = Footprint,
        intervals = Intervals } =
        build_task_source(UnsyncedRange, PeerRange),
    ?assertEqual([{6, 3}], ar_intervals:to_list(Intervals)).

build_tasks_keeps_source_representations_independent_test() ->
    Chunk = ?DATA_CHUNK_SIZE,
    BytePeer = {1, 1, 1, 1, 1984},
    FootprintPeer = {2, 2, 2, 2, 1984},
    Footprint = #footprint{ store_id = test_store, partition = 0,
        footprint = 1 },
    Intervals = ar_intervals:from_list([{2 * Chunk, 0}]),
    TaskSources = [
        #task_source{ peer = BytePeer, intervals = Intervals,
            footprint = none },
        #task_source{ peer = FootprintPeer, intervals = Intervals,
            footprint = Footprint }
    ],
    %% A two-task budget still retains the footprint reservation because it is
    %% speculative work, not an executable chunk task.
    {Tasks, _NextClaimOffset} = build_tasks(test_store, TaskSources, 2),
    [Reservation, FirstTask, SecondTask] = Tasks,
    ?assertMatch(#footprint_reservation{}, Reservation),
    ?assertEqual(Footprint, ar_sync_footprint:key(Reservation)),
    ?assertEqual(
        [#task_source{ peer = FootprintPeer, footprint = Footprint,
            intervals = Intervals }],
        ar_sync_footprint:sources(Reservation)),
    ?assertEqual([#task_source{ peer = BytePeer }], FirstTask#task.sources),
    ?assertEqual([#task_source{ peer = BytePeer }], SecondTask#task.sources).

build_tasks_keeps_each_footprint_group_test() ->
    Chunk = ?DATA_CHUNK_SIZE,
    Peer = {1, 1, 1, 1, 1984},
    FirstFootprint = #footprint{ store_id = test_store, partition = 0,
        footprint = 1 },
    SecondFootprint = #footprint{ store_id = test_store, partition = 0,
        footprint = 2 },
    TaskSources = [
        #task_source{ peer = Peer, footprint = FirstFootprint,
            intervals = ar_intervals:from_list([
                {Chunk, 0}, {3 * Chunk, 2 * Chunk}]) },
        #task_source{ peer = Peer, footprint = SecondFootprint,
            intervals = ar_intervals:from_list([
                {2 * Chunk, Chunk}, {4 * Chunk, 3 * Chunk}]) }
    ],
    %% The positive task budget permits discovery work; reservations do not
    %% consume its two executable-task slots.
    {Tasks, _NextClaimOffset} = build_tasks(test_store, TaskSources, 2),
    [FirstReservation, SecondReservation] = Tasks,
    ?assertEqual(FirstFootprint,
        ar_sync_footprint:key(FirstReservation)),
    ?assertEqual(SecondFootprint,
        ar_sync_footprint:key(SecondReservation)).

build_tasks_returns_next_claim_offset_test() ->
    Chunk = ?DATA_CHUNK_SIZE,
    Peer = {1, 1, 1, 1, 1984},
    %% Three advertised chunks with a two-task budget leave the third chunk
    %% for the next admission pass.
    Intervals = ar_intervals:from_list([{3 * Chunk, 0}]),
    TaskSources = [#task_source{
        peer = Peer,
        footprint = none,
        intervals = Intervals
    }],
    {Tasks, NextClaimOffset} = build_tasks(test_store, TaskSources, 2),
    ?assertEqual(2, length(Tasks)),
    ?assertEqual(2 * Chunk, NextClaimOffset).

-endif.
