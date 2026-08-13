%%% @doc Pure(ish) chunk-fetch selection logic for the network sync subsystem.
%%%
%%% Given a storage module's locally-needed unsynced ranges and the peers that
%%% advertise them, this module decides WHAT is fetchable from WHOM: it picks the
%%% hot (non-throttled) peers, intersects each peer's cached intervals with the
%%% unsynced ranges to produce #fetchable_range{}s, and spends the per-step
%%% chunk budget over them.
%%%
%%% It holds no process state: ar_sync_store_sweeper owns the sweep cursors, and
%%% ar_sync_scheduler owns claims, admission, and peer selection.
-module(ar_sync_chunk_picker).

-export([claim_ranges/2]).

-include("ar.hrl").
-include("ar_sync_buckets.hrl").
-include("ar_sync.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Peer-advertised ranges that overlap a locally needed range and can be
%% claimed for chunk fetching.
-record(fetchable_range, {
    peer :: term(),
    query_offset :: non_neg_integer(),
    intervals :: ar_intervals:intervals(),
    footprint :: none | #footprint{}
}).

%% Chunks are fetched from /chunk2, so peer selection for chunk fetching gates on the
%% chunk2 throttle, not the chunk interval metadata throttle
%% (data_sync_record / footprints). Gating on the metadata path wrongly dropped
%% peers after a background job hit its rate limit, even though they had cached
%% intervals and were fully available for chunks. This collapsed the fetch peer
%% set to roughly half.
-define(CHUNK_PATH, "/chunk2").

%%%===================================================================
%%% Per-peer fetchable-interval computation from the ar_sync_discovery cache.
%%%===================================================================

%% @doc Resolve a sweep step's unsynced ranges into work the scheduler has
%% admitted, returning how many chunks were claimed.
%%
%% `blocked' means the store has no admission headroom, so no later range in the
%% store can be admitted either. Missing peer metadata does not block the sweep;
%% discovery notifies the sweeper when newly fetched intervals should be
%% revisited.
%%
%% The headroom query is advisory; it only bounds how many candidates are worth
%% building. Exact claims and final admission are one atomic scheduler
%% operation, so the scheduler stays authoritative if capacity moved in
%% between. Candidates are built out here rather than inside the scheduler to
%% keep every store's interval work off that single process.
claim_ranges(_StoreID, []) ->
    {ok, 0};
claim_ranges(StoreID, UnsyncedRanges) ->
    case ar_sync_scheduler:admission_headroom(StoreID) of
        0 ->
            %% No later range in this store can be admitted either. Hold the
            %% sweep here until existing work releases claim headroom.
            blocked;
        Headroom ->
            Tasks = pick_tasks(StoreID, UnsyncedRanges, Headroom),
            ar_sync_scheduler:claim_and_enqueue(StoreID, Tasks)
    end.

%% @doc Turn a sweep step's unsynced ranges into candidate fetch tasks.
%%
%% Availability only. The picker does not rate peers or split a budget between
%% them - those depend on live capacity the scheduler owns, and deciding them
%% here meant deciding them from a stale snapshot the scheduler then had to work
%% around.
%%
%% Each chunk lists every peer and source representation advertising it. The
%% scheduler chooses one source and binds footprint work to one peer.
%%
%% MaxChunks is the scheduler's advisory admission headroom. Exact claims are
%% still made by the scheduler when the batch is submitted.
pick_tasks(StoreID, UnsyncedRanges, MaxChunks) when is_list(UnsyncedRanges) ->
    FetchableRangeGroups = [
        find_fetchable_ranges(StoreID, UnsyncedRange)
        || UnsyncedRange <- UnsyncedRanges
    ],
    FetchableRanges = lists:append(FetchableRangeGroups),
    ConsolidatedFetchableRanges = consolidate_fetchable_ranges(FetchableRanges),
    build_tasks(StoreID, ConsolidatedFetchableRanges, MaxChunks).

find_fetchable_ranges(StoreID, UnsyncedRange) ->
    #unsynced_range{
        query_offset = Left,
        range_start = RangeStart,
        range_end = RangeEnd
    } = UnsyncedRange,
    Peers = get_hot_peers(Left),
    PeerRanges = ar_sync_deps:get_peer_ranges_for_peers(
        StoreID, Peers, Left, RangeStart, RangeEnd),
    build_fetchable_ranges(UnsyncedRange, PeerRanges).

%% @doc Build fetchable ranges by intersecting each peer's advertised intervals
%% with the chunks the local store still needs.
build_fetchable_ranges(UnsyncedRange, PeerRanges) ->
    lists:filtermap(
        fun(PeerRange) ->
            build_fetchable_range(UnsyncedRange, PeerRange)
        end,
        PeerRanges).

build_fetchable_range(UnsyncedRange, PeerRange) ->
    #peer_range{ peer = Peer, offset = QueryOffset,
        intervals = AdvertisedIntervals, footprint = Footprint } = PeerRange,
    #unsynced_range{ intervals = UnsyncedIntervals } = UnsyncedRange,
    FetchableIntervals = ar_intervals:intersection(
        AdvertisedIntervals, UnsyncedIntervals),
    case ar_intervals:is_empty(FetchableIntervals) of
        true -> false;
        false ->
            {true, #fetchable_range{
                peer = Peer,
                query_offset = QueryOffset,
                intervals = FetchableIntervals,
                footprint = Footprint
            }}
    end.

%% @doc Consolidate #fetchable_ranges{} by peer and footprint. This merges
%% the multiple #fetchable_ranges{} we may have for a given peer. Duplicates
%% can exist because we query a #peer_range for each #unsynced_range, which
%% can result in multiple #peer_ranges for a given peer.
consolidate_fetchable_ranges(FetchableRanges) ->
    {Keys, RangesByKey} = lists:foldl(
        fun consolidate_fetchable_range/2,
        {[], #{}},
        FetchableRanges),
    [maps:get(Key, RangesByKey) || Key <- lists:reverse(Keys)].

consolidate_fetchable_range(FetchableRange, {Keys, RangesByKey}) ->
    Key = {FetchableRange#fetchable_range.peer,
        FetchableRange#fetchable_range.footprint},
    case maps:find(Key, RangesByKey) of
        error ->
            {[Key | Keys], maps:put(Key, FetchableRange, RangesByKey)};
        {ok, ExistingRange} ->
            ConsolidatedRange = ExistingRange#fetchable_range{
                query_offset = min(ExistingRange#fetchable_range.query_offset,
                    FetchableRange#fetchable_range.query_offset),
                intervals = ar_intervals:union(
                    ExistingRange#fetchable_range.intervals,
                    FetchableRange#fetchable_range.intervals)
            },
            {Keys, maps:put(Key, ConsolidatedRange, RangesByKey)}
    end.

%% @doc Build byte-source tasks and one reservation per footprint group.
%%
%% The interval groups prevent overlapping byte and footprint advertisements
%% from producing duplicate work. A footprint reservation carries each
%% source's immutable advertised intervals; the scheduler selects one source
%% and only then expands its intervals into tasks.
build_tasks(_StoreID, _FetchableRanges, MaxChunks) when MaxChunks =< 0 ->
    [];
build_tasks(StoreID, FetchableRanges, MaxChunks) ->
    IntervalGroups = group_intervals(FetchableRanges),
    {_, Tasks} = lists:foldl(
        fun({_Group, _Intervals}, {Remaining, TasksAcc}) when Remaining =< 0 ->
                {Remaining, TasksAcc};
            ({none, Intervals}, {Remaining, TasksAcc}) ->
                build_tasks_for_group(
                    none, Intervals, Remaining, TasksAcc, StoreID,
                    FetchableRanges);
            ({#footprint{} = Footprint, Intervals}, {Remaining, TasksAcc}) ->
                Reservation = ar_sync_footprint:new_reservation(
                    StoreID, Footprint,
                    footprint_sources(Intervals, FetchableRanges)),
                {Remaining - ar_sync_store:chunks_in_claim(Reservation),
                    [Reservation | TasksAcc]}
        end,
        {MaxChunks, []},
        IntervalGroups),
    lists:reverse(Tasks).

footprint_sources(Group, FetchableRanges) ->
    lists:filtermap(
        fun(FetchableRange) ->
            #fetchable_range{
                peer = Peer,
                footprint = Footprint,
                intervals = Intervals
            } = FetchableRange,
            SourceIntervals = ar_intervals:intersection(Group, Intervals),
            case ar_intervals:is_empty(SourceIntervals) of
                true -> false;
                false -> {true, #task_source{
                    peer = Peer,
                    footprint = Footprint,
                    intervals = SourceIntervals
                }}
            end
        end,
        FetchableRanges).

%% @doc Organize fetchable intervals into non-overlapping task-construction
%% groups. Each footprint remains a distinct group because its chunks should be
%% assigned together. Byte intervals covered by any footprint group are removed
%% only from the byte group; byte sources remain attached later by sources/2.
group_intervals(FetchableRanges) ->
    Groups = extract_groups(FetchableRanges),
    NonOverlappingGroups = exclude_footprints_from_byte_group(Groups),
    lists:sort(
        fun({_LeftGroup, LeftIntervals}, {_RightGroup, RightIntervals}) ->
            {_, LeftStart} = ar_intervals:smallest(LeftIntervals),
            {_, RightStart} = ar_intervals:smallest(RightIntervals),
            LeftStart =< RightStart
        end,
        maps:to_list(NonOverlappingGroups)).

extract_groups(FetchableRanges) ->
    lists:foldl(
        fun(FetchableRange, Groups) ->
            #fetchable_range{
                footprint = Footprint,
                intervals = Intervals
            } = FetchableRange,
            maps:update_with(Footprint,
                fun(Existing) -> ar_intervals:union(Existing, Intervals) end,
                Intervals, Groups)
        end,
        #{none => ar_intervals:new()},
        FetchableRanges).

exclude_footprints_from_byte_group(Groups) ->
    {ByteIntervals, FootprintGroups} = maps:take(none, Groups),
    FootprintIntervals = maps:fold(
        fun(_Footprint, Intervals, Acc) ->
            ar_intervals:union(Intervals, Acc)
        end,
        ar_intervals:new(),
        FootprintGroups),
    ByteOnly = ar_intervals:outerjoin(FootprintIntervals, ByteIntervals),
    case ar_intervals:is_empty(ByteOnly) of
        true -> FootprintGroups;
        false -> maps:put(none, ByteOnly, FootprintGroups)
    end.

%% @doc Build up to MaxChunks tasks from an interval set in ascending offset
%% order. Returning the unspent count lets build_tasks/3 apply one limit across
%% all groups while retaining the task accumulator between groups.
build_tasks_for_group(Footprint, Group, MaxChunks, Tasks, StoreID,
        FetchableRanges) ->
    {RemainingChunks, Tasks2} = ar_intervals:fold(
        fun(_, {Remaining, TasksAcc}) when Remaining =< 0 ->
                {Remaining, TasksAcc};
            ({End, Start}, {Remaining, TasksAcc}) ->
                RangeEnd = min(End, Start + (Remaining * ?DATA_CHUNK_SIZE)),
                build_tasks_for_interval(Start, RangeEnd, Remaining, TasksAcc,
                    StoreID, Footprint, FetchableRanges)
        end,
        {MaxChunks, Tasks},
        Group),
    {RemainingChunks, Tasks2}.

%% @doc Expand one contiguous byte interval into one task per request offset.
%% This is
%% separate from build_tasks_for_group/6 because ar_intervals:fold/3 iterates
%% intervals, while the scheduler contract requires one task for each chunk.
build_tasks_for_interval(Start, RangeEnd, Remaining, TasksAcc,
        _StoreID, _Footprint, _FetchableRanges)
        when Start >= RangeEnd ->
    {Remaining, TasksAcc};
build_tasks_for_interval(Start, RangeEnd, Remaining, TasksAcc,
        StoreID, Footprint, FetchableRanges) ->
    End = min(Start + ?DATA_CHUNK_SIZE, RangeEnd),
    Task = #task{
        offset = Start,
        sources = sources(Start, FetchableRanges),
        store_id = StoreID,
        footprint = Footprint
    },
    build_tasks_for_interval(End, RangeEnd, Remaining - 1, [Task | TasksAcc],
        StoreID, Footprint, FetchableRanges).

%% @doc Collect every peer that advertised the chunk beginning at Start.
%% One source is retained per peer: byte availability is represented by `none',
%% while footprint availability carries the footprint that constrains affinity.
sources(Start, FetchableRanges) ->
    ByPeer = lists:foldl(
        fun(FetchableRange, Acc) ->
            #fetchable_range{
                peer = Peer,
                footprint = Footprint,
                intervals = Intervals
            } = FetchableRange,
            case ar_intervals:is_inside(Intervals, Start + 1) of
                false ->
                    Acc;
                true ->
                    maps:update_with(Peer,
                        fun(Existing) ->
                            preferred_representation(Existing, Footprint)
                        end,
                        Footprint, Acc)
            end
        end,
        #{},
        FetchableRanges),
    [#task_source{ peer = Peer, footprint = Footprint }
        || {Peer, Footprint} <- lists:sort(maps:to_list(ByPeer))].

%% @doc Choose one representation when the same peer advertises a chunk through
%% both byte and footprint intervals. Byte availability wins because it fetches
%% the same chunk without consuming a source-entropy slot.
preferred_representation(none, _Footprint) ->
    none;
preferred_representation(_Footprint, none) ->
    none;
preferred_representation(Footprint, Footprint) ->
    Footprint.

%%%===================================================================
%%% Per-peer budget / share allocation.
%%%===================================================================

get_hot_peers(Offset) ->
    LocalOnly = arweave_config:get([sync, local_peers_only]),
    AllPeers =
        case LocalOnly of
            true -> arweave_config:get([peers, local]);
        false -> ar_sync_deps:get_peers_for_offset(Offset)
        end,
    %% Gate on the chunk2 throttle - the path chunk fetching actually uses - not the
    %% chunk interval metadata throttle. A peer throttled only for metadata still
    %% serves chunks from its cached intervals.
    UnthrottledPeers = lists:filter(
        fun(Peer) ->
            not ar_sync_deps:is_throttled(Peer, ?CHUNK_PATH)
        end,
        AllPeers),
    HotPeers = case UnthrottledPeers of
        [] -> AllPeers;
        _ -> UnthrottledPeers
    end,
    ar_sync_deps:pick_peers(HotPeers, ?QUERY_BEST_PEERS_COUNT).

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).

build_fetchable_byte_ranges_test() ->
    Peer = {1, 2, 3, 4, 1984},
    UnsyncedRange = #unsynced_range{
        kind = byte,
        query_offset = 2,
        intervals = ar_intervals:from_list([{6, 2}]),
        range_start = 2,
        range_end = 8,
        advance = done
    },
    [#fetchable_range{ intervals = Intervals }] =
        build_fetchable_ranges(UnsyncedRange, [
            #peer_range{
                peer = Peer,
                offset = 2,
                intervals = ar_intervals:from_list([{8, 4}]),
                footprint = none
            }
        ]),
    ?assertEqual([{6, 4}], ar_intervals:to_list(Intervals)).

consolidate_overlapping_fetchable_ranges_test() ->
    Peer = {1, 2, 3, 4, 1984},
    Chunk = ?DATA_CHUNK_SIZE,
    First = #fetchable_range{
        peer = Peer,
        query_offset = 0,
        intervals = ar_intervals:from_list([{2 * Chunk, 0}]),
        footprint = none
    },
    Second = #fetchable_range{
        peer = Peer,
        query_offset = Chunk,
        intervals = ar_intervals:from_list([{3 * Chunk, Chunk}]),
        footprint = none
    },
    [Consolidated] = consolidate_fetchable_ranges([First, Second]),
    ?assertEqual(0, Consolidated#fetchable_range.query_offset),
    %% Two overlapping two-chunk ranges form one contiguous three-chunk range.
    ?assertEqual([{3 * Chunk, 0}],
        ar_intervals:to_list(Consolidated#fetchable_range.intervals)).

build_fetchable_footprint_ranges_test() ->
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
    [#fetchable_range{ footprint = Footprint,
        intervals = Intervals }] =
        build_fetchable_ranges(UnsyncedRange, [PeerRange]),
    ?assertEqual([{6, 3}], ar_intervals:to_list(Intervals)).

build_tasks_combines_source_representations_test() ->
    Chunk = ?DATA_CHUNK_SIZE,
    BytePeer = {1, 1, 1, 1, 1984},
    FootprintPeer = {2, 2, 2, 2, 1984},
    Footprint = #footprint{ store_id = test_store, partition = 0,
        footprint = 1 },
    Intervals = ar_intervals:from_list([{2 * Chunk, 0}]),
    FetchableRanges = [
        #fetchable_range{ peer = BytePeer, intervals = Intervals,
            footprint = none },
        #fetchable_range{ peer = FootprintPeer, intervals = Intervals,
            footprint = Footprint }
    ],
    %% Both source representations describe one footprint, so the picker emits
    %% one reservation and leaves task creation to the scheduler.
    Tasks = build_tasks(test_store, FetchableRanges, 2),
    [Reservation] = Tasks,
    ?assertMatch(#footprint_reservation{}, Reservation),
    ?assertEqual(Footprint, ar_sync_footprint:key(Reservation)),
    Sources = ar_sync_footprint:sources(Reservation),
    ?assertEqual(
        [#task_source{ peer = BytePeer, intervals = Intervals },
            #task_source{ peer = FootprintPeer, footprint = Footprint,
                intervals = Intervals }],
        Sources).

build_tasks_finishes_footprint_before_next_group_test() ->
    Chunk = ?DATA_CHUNK_SIZE,
    Peer = {1, 1, 1, 1, 1984},
    FirstFootprint = #footprint{ store_id = test_store, partition = 0,
        footprint = 1 },
    SecondFootprint = #footprint{ store_id = test_store, partition = 0,
        footprint = 2 },
    FetchableRanges = [
        #fetchable_range{ peer = Peer, footprint = FirstFootprint,
            intervals = ar_intervals:from_list([
                {Chunk, 0}, {3 * Chunk, 2 * Chunk}]) },
        #fetchable_range{ peer = Peer, footprint = SecondFootprint,
            intervals = ar_intervals:from_list([
                {2 * Chunk, Chunk}, {4 * Chunk, 3 * Chunk}]) }
    ],
    %% Any positive headroom admits one whole footprint reservation. Its
    %% footprint-sized cost leaves no room for the second group.
    Tasks = build_tasks(test_store, FetchableRanges, 2),
    [Reservation] = Tasks,
    ?assertMatch(#footprint_reservation{}, Reservation),
    ?assertEqual(FirstFootprint, ar_sync_footprint:key(Reservation)).

-endif.
