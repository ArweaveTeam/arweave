%% @doc Sweep traversal and cursor helpers for ar_sync_store_sweeper.
-module(ar_sync_cursor).

-export([kinds/0, new/2, current/2, start/2, set/3,
        live_end/4, is_complete/3, advance/2, query_range_step_size/0]).
-export_type([t/0]).

-include("ar_sync.hrl").

-ifdef(AR_TEST).
-export([override_query_range_step_size/1, reset_all_overrides/0]).
-endif.

-record(state, {
    %% Left bounds for each sync cursor.
    start :: {integer(), integer()},
    %% Static right bounds for each sync cursor. Live bounds are applied at
    %% enqueue time so a sweep follows tip and disk-pool-threshold changes without
    %% being rebuilt.
    limit :: {integer(), integer()},
    %% Current byte-window and footprint-cadence cursor offsets.
    current :: {integer(), integer()}
}).

-opaque t() :: #state{}.

kinds() ->
    [byte, footprint].

-ifdef(AR_TEST).
query_range_step_size() ->
    persistent_term:get({?MODULE, query_range_step_size}, 10_000_000). % 10 MB

override_query_range_step_size(Bytes) ->
    persistent_term:put({?MODULE, query_range_step_size}, Bytes).

reset_all_overrides() ->
    persistent_term:erase({?MODULE, query_range_step_size}),
    ok.
-else.
query_range_step_size() ->
    ?QUERY_RANGE_STEP_SIZE.
-endif.

new(StartOffset, EndOffset) ->
    Start = {StartOffset, StartOffset},
    Limit = {EndOffset, EndOffset},
    #state{
        start = Start,
        limit = Limit,
        current = Start
    }.

current(Kind, #state{ current = Current }) ->
    value(Kind, Current).

start(Kind, #state{ start = Start }) ->
    value(Kind, Start).

set(byte, Offset, #state{ current = {_Byte, Footprint} } = State) ->
    State#state{ current = {Offset, Footprint} };
set(footprint, Offset, #state{ current = {Byte, _Footprint} } = State) ->
    State#state{ current = {Byte, Offset} }.

live_end(byte, #state{ limit = Limit }, WeaveSize, _DiskPoolThreshold) ->
    min(value(byte, Limit), WeaveSize);
live_end(footprint, #state{ limit = Limit }, WeaveSize, DiskPoolThreshold) ->
    min(min(value(footprint, Limit), WeaveSize), DiskPoolThreshold).

is_complete(State, WeaveSize, DiskPoolThreshold) ->
    lists:all(
        fun(Kind) ->
            current(Kind, State) >= live_end(Kind, State, WeaveSize, DiskPoolThreshold)
        end,
        kinds()).

advance(#unsynced_range{ kind = byte, advance = End }, State) ->
    set(byte, End, State);
advance(#unsynced_range{ kind = footprint,
        advance = {Offset, Start, End} }, State) ->
    set(footprint,
        ar_replica_2_9:get_next_fetch_offset(Offset, Start, End), State).

value(byte, {Byte, _Footprint}) ->
    Byte;
value(footprint, {_Byte, Footprint}) ->
    Footprint.
