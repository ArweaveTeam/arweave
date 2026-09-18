-module(arweave_sync_chunk_picker_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-import(arweave_sync_chunk_picker, [
    add_task_source/2,
    build_task_source/2,
    build_tasks/3
]).

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        build_byte_task_source,
        add_task_source_merges_overlapping_intervals,
        add_task_source_ignores_empty_intervals,
        build_footprint_task_source,
        build_tasks_keeps_source_representations_independent,
        build_tasks_keeps_each_footprint_group,
        build_tasks_returns_next_claim_offset
    ].

init_per_testcase(_Case, Config) ->
    arweave_sync_test_deps:setup(),
    Config.

end_per_testcase(_Case, _Config) ->
    arweave_sync_test_deps:cleanup().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Byte task sources contain only the intersection of missing and
%% advertised data.
build_byte_task_source(_Config) ->
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
    #task_source{peer = Peer, footprint = none, intervals = Intervals} =
        build_task_source(UnsyncedRange, PeerRange),
    ?assertEqual([{6, 4}], ar_intervals:to_list(Intervals)).

%% @doc Overlapping ranges from the same source merge into one contiguous range.
add_task_source_merges_overlapping_intervals(_Config) ->
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
        Second, add_task_source(First, #{})
    ),
    [Consolidated] = maps:values(TaskSourcesByKey),
    %% Two overlapping two-chunk ranges form one contiguous three-chunk range.
    ?assertEqual(
        [{3 * Chunk, 0}],
        ar_intervals:to_list(Consolidated#task_source.intervals)
    ).

%% @doc Empty source ranges do not create task-source entries.
add_task_source_ignores_empty_intervals(_Config) ->
    TaskSource = #task_source{intervals = ar_intervals:new()},
    ?assertEqual(#{}, add_task_source(TaskSource, #{})).

%% @doc Footprint sources preserve their identity while intersecting missing and
%% advertised data.
build_footprint_task_source(_Config) ->
    Peer = {1, 2, 3, 4, 1984},
    Footprint = #footprint{
        store_id = test_store,
        partition = 0,
        footprint = 1
    },
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
    #task_source{
        peer = Peer,
        footprint = Footprint,
        intervals = Intervals
    } =
        build_task_source(UnsyncedRange, PeerRange),
    ?assertEqual([{6, 3}], ar_intervals:to_list(Intervals)).

%% @doc Byte tasks and footprint reservations remain independent even when their
%% ranges overlap.
build_tasks_keeps_source_representations_independent(_Config) ->
    Chunk = ?DATA_CHUNK_SIZE,
    BytePeer = {1, 1, 1, 1, 1984},
    FootprintPeer = {2, 2, 2, 2, 1984},
    Footprint = #footprint{
        store_id = test_store,
        partition = 0,
        footprint = 1
    },
    Intervals = ar_intervals:from_list([{2 * Chunk, 0}]),
    TaskSources = [
        #task_source{
            peer = BytePeer,
            intervals = Intervals,
            footprint = none
        },
        #task_source{
            peer = FootprintPeer,
            intervals = Intervals,
            footprint = Footprint
        }
    ],
    %% A two-task budget still retains the footprint reservation because it is
    %% speculative work, not an executable chunk task.
    {Tasks, _NextClaimOffset} = build_tasks(test_store, TaskSources, 2),
    [Reservation, FirstTask, SecondTask] = Tasks,
    ?assertMatch(#footprint_reservation{}, Reservation),
    ?assertEqual(Footprint, arweave_sync_footprint:key(Reservation)),
    ?assertEqual(
        [
            #task_source{
                peer = FootprintPeer,
                footprint = Footprint,
                intervals = Intervals
            }
        ],
        arweave_sync_footprint:sources(Reservation)
    ),
    ?assertEqual([#task_source{peer = BytePeer}], FirstTask#task.sources),
    ?assertEqual([#task_source{peer = BytePeer}], SecondTask#task.sources).

%% @doc Interleaved ranges from different footprints produce separate
%% reservations.
build_tasks_keeps_each_footprint_group(_Config) ->
    Chunk = ?DATA_CHUNK_SIZE,
    Peer = {1, 1, 1, 1, 1984},
    FirstFootprint = #footprint{
        store_id = test_store,
        partition = 0,
        footprint = 1
    },
    SecondFootprint = #footprint{
        store_id = test_store,
        partition = 0,
        footprint = 2
    },
    TaskSources = [
        #task_source{
            peer = Peer,
            footprint = FirstFootprint,
            intervals = ar_intervals:from_list([
                {Chunk, 0}, {3 * Chunk, 2 * Chunk}
            ])
        },
        #task_source{
            peer = Peer,
            footprint = SecondFootprint,
            intervals = ar_intervals:from_list([
                {2 * Chunk, Chunk}, {4 * Chunk, 3 * Chunk}
            ])
        }
    ],
    %% The positive task budget permits discovery work; reservations do not
    %% consume its two executable-task slots.
    {Tasks, _NextClaimOffset} = build_tasks(test_store, TaskSources, 2),
    [FirstReservation, SecondReservation] = Tasks,
    ?assertEqual(
        FirstFootprint,
        arweave_sync_footprint:key(FirstReservation)
    ),
    ?assertEqual(
        SecondFootprint,
        arweave_sync_footprint:key(SecondReservation)
    ).

%% @doc A limited task batch returns the offset where the next admission should
%% resume.
build_tasks_returns_next_claim_offset(_Config) ->
    Chunk = ?DATA_CHUNK_SIZE,
    Peer = {1, 1, 1, 1, 1984},
    %% Three advertised chunks with a two-task budget leave the third chunk
    %% for the next admission pass.
    Intervals = ar_intervals:from_list([{3 * Chunk, 0}]),
    TaskSources = [
        #task_source{
            peer = Peer,
            footprint = none,
            intervals = Intervals
        }
    ],
    {Tasks, NextClaimOffset} = build_tasks(test_store, TaskSources, 2),
    ?assertEqual(2, length(Tasks)),
    ?assertEqual(2 * Chunk, NextClaimOffset).
