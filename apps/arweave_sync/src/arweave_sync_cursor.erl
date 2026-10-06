%%% @doc The sweeper's position in a store range: how far the byte sweep and
%%% the footprint sweep have each got. The end of the range moves up with the
%%% weave size, and the sweep is complete when both have reached it. The
%%% footprint sweep walks only the first sector of each partition
%%% (next_footprint_offset/3), which covers every footprint in the partition.
-module(arweave_sync_cursor).

-export([
    new/2,
    current/2,
    start/2,
    set/3,
    live_end/4,
    is_complete/3,
    advance/2,
    next_footprint_offset/3,
    query_range_step_size/0
]).
-export_type([t/0]).

-include_lib("arweave_lib/include/arweave_lib_constants.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include_lib("arweave_sync/include/arweave_sync_deps.hrl").

-ifdef(AR_TEST).
-export([override_query_range_step_size/1, reset_all_overrides/0]).
-endif.

-record(state, {
    start :: {integer(), integer()},
    %% The fixed end offset of each cursor, before live_end/4 caps it.
    limit :: {integer(), integer()},
    current :: {integer(), integer()}
}).

-opaque t() :: #state{}.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return new byte and footprint cursors that run from StartOffset to
%% EndOffset and both start at StartOffset.
new(StartOffset, EndOffset) ->
    Start = {StartOffset, StartOffset},
    Limit = {EndOffset, EndOffset},
    #state{
        start = Start,
        limit = Limit,
        current = Start
    }.

current(Kind, #state{current = Current}) ->
    value(Kind, Current).

start(Kind, #state{start = Start}) ->
    value(Kind, Start).

set(byte, Offset, #state{current = {_Byte, Footprint}} = State) ->
    State#state{current = {Offset, Footprint}};
set(footprint, Offset, #state{current = {Byte, _Footprint}} = State) ->
    State#state{current = {Byte, Offset}}.

%% @doc Return the end offset of the Kind cursor, capping the offset at
%% WeaveSize and, for the footprint cursor, at DiskPoolThreshold.
live_end(byte, #state{limit = Limit}, WeaveSize, _DiskPoolThreshold) ->
    min(value(byte, Limit), WeaveSize);
live_end(footprint, #state{limit = Limit}, WeaveSize, DiskPoolThreshold) ->
    min(min(value(footprint, Limit), WeaveSize), DiskPoolThreshold).

%% @doc Return true once both cursors reach their live ends.
is_complete(State, WeaveSize, DiskPoolThreshold) ->
    lists:all(
        fun(Kind) ->
            current(Kind, State) >= live_end(Kind, State, WeaveSize, DiskPoolThreshold)
        end,
        [byte, footprint]
    ).

%% @doc Move the cursor of the range's kind past the range: to the advance
%% offset of a byte range, or to next_footprint_offset/3 of a footprint range.
advance(#unsynced_range{kind = byte, advance = End}, State) ->
    set(byte, End, State);
advance(
    #unsynced_range{
        kind = footprint,
        advance = {Offset, Start, End}
    },
    State
) ->
    set(footprint, next_footprint_offset(Offset, Start, End), State).

%% @doc Return the footprint cursor's next offset after Offset, within
%% [Start, End). Each chunk in a sector has a different entropy, and so a
%% different footprint, so one sector covers every footprint in its partition.
%% The cursor moves one chunk at a time through the partition's first sector,
%% then jumps to the next partition.
next_footprint_offset(Offset, Start, End) ->
    SectorSize = arweave_lib_constants:get_replica_2_9_entropy_sector_size(),
    Partition = arweave_lib_replica_2_9:get_entropy_partition(
        Offset + ?DATA_CHUNK_SIZE),
    {PartitionStart, PartitionEnd} =
        arweave_lib_replica_2_9:get_entropy_partition_range(Partition),
    SectorStart = max(Start, PartitionStart),
    SectorEnd = min(PartitionEnd, SectorStart + SectorSize),
    NextOffset =
        case Offset + 2 * ?DATA_CHUNK_SIZE > SectorEnd of
            true -> PartitionEnd;
            false -> Offset + ?DATA_CHUNK_SIZE
        end,
    min(NextOffset, End).

%% @doc Return the byte sweep step size: QUERY_RANGE_STEP_SIZE in production
%% and an overridable 10 MB in test builds.
-ifdef(AR_TEST).
query_range_step_size() ->
    persistent_term:get({?MODULE, query_range_step_size}, 10_000_000).

override_query_range_step_size(Bytes) ->
    persistent_term:put({?MODULE, query_range_step_size}, Bytes).

reset_all_overrides() ->
    persistent_term:erase({?MODULE, query_range_step_size}),
    ok.
-else.
query_range_step_size() ->
    ?QUERY_RANGE_STEP_SIZE.
-endif.

%%%===================================================================
%%% Private functions.
%%%===================================================================

value(byte, {Byte, _Footprint}) ->
    Byte;
value(footprint, {_Byte, Footprint}) ->
    Footprint.
