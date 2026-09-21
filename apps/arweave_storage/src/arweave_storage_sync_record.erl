%%% Availability queries and persisted byte/footprint interval records.
-module(arweave_storage_sync_record).
-export([get_data_sizes/0, sync_record_exists/3]).

-behaviour(gen_server).

-export([
    start_link/2,
    get/3,
    add/5,
    add_async/5, add_async/6,
    delete/4,
    cut/3,
    is_recorded/4,
    is_recorded_any/3,
    get_next_interval/6,
    get_intervals/6,
    get_interval/4,
    get_intersection_size/5,
    get_packings/2,
    name/1
]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

%% The kv storage key to the sync records.
-define(SYNC_RECORDS_KEY, <<"sync_records">>).

%% The kv key of the write ahead log counter.
-define(WAL_COUNT_KEY, <<"wal">>).

%% The frequency of dumping sync records on disk. A store snapshots no more
%% often than this, and only when its write-ahead log is not empty.
-ifdef(AR_TEST).
-define(STORE_SYNC_RECORD_FREQUENCY_MS, 1000).
-else.
-define(STORE_SYNC_RECORD_FREQUENCY_MS, 60 * 1000).
-endif.

%% Building a snapshot materializes every interval of the store on the heap,
%% ~150 bytes per interval, for seconds. Stores take turns through the
%% ?SNAPSHOT_TOKEN_KEY row of the sync_records registry so the node pays that
%% transient for one store at a time; a store that finds the token taken
%% asks again after this delay.
-define(SNAPSHOT_TOKEN_KEY, snapshot_token).
-ifdef(AR_TEST).
-define(SNAPSHOT_TOKEN_RETRY_MS, 100).
-else.
-define(SNAPSHOT_TOKEN_RETRY_MS, 1000).
-endif.

%% The intervals themselves are NOT kept in the server state. The single
%% source of truth is a set of ar_ets_intervals tables, one per record
%% (keyed {ID, StoreID} and {ID, Packing, StoreID} in the `sync_records'
%% registry), each holding non-overlapping intervals {End, Start} in the
%% record's native coordinates: byte offsets or footprint-index offsets.
%% Intervals exclude Start and include End.
%%
%% Each set serves as a compact map of what is synced by the node.
%% No matter how big the weave is or how much of it the node stores,
%% this record can remain very small compared to the space taken by
%% chunk identifiers, whose number grows unlimited with time.
%%
%% The tables are owned by this gen_server (they die with it) and are
%% only mutated by it; other processes read them lock-free through the
%% registry. The state below keeps the persistence bookkeeping only.
-record(state, {
    %% The name of the WAL store.
    state_db,
    %% The identifier of the storage module.
    store_id,
    %% The storage module.
    storage_module,
    %% The partition covered by the storage module.
    partition_number,
    %% The number of entries in the write-ahead log.
    wal,
    %% Whether the sync record is in memory only.
    in_memory = false
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, StoreID) ->
    gen_server:start_link({local, Name}, ?MODULE, StoreID, []).

%% @doc Return the union of matching records, or an empty set if none exist.
get(Packing, Record, StoreID) ->
    Tables = record_tables(Packing, record_id(Record), StoreID),
    do_get(Tables).

do_get([TID]) ->
    ar_ets_intervals:to_gb_set(TID);
do_get(Tables) ->
    lists:foldl(
        fun(TID, Acc) ->
            ar_intervals:union(Acc, ar_ets_intervals:to_gb_set(TID))
        end,
        ar_intervals:new(),
        Tables
    ).

%% @doc Persist a typed interval, or an untyped interval with any_packing.
add(End, Start, Packing, Record, StoreID) when StoreID =/= any_store ->
    ID = record_id(Record),
    GenServerID = name(StoreID),
    Request =
        case Packing of
            any_packing -> {add, End, Start, ID};
            _ -> {add, End, Start, Packing, ID}
        end,
    case catch gen_server:call(GenServerID, Request, 120000) of
        {'EXIT', {timeout, {gen_server, call, _}}} ->
            {error, timeout};
        Reply ->
            Reply
    end.

%% @doc Queue an untyped interval update.
add_async(Event, End, Start, Record, StoreID) ->
    ID = record_id(Record),
    GenServerID = name(StoreID),
    gen_server:cast(GenServerID, {add_async, Event, End, Start, ID}).

%% @doc Queue a packing-specific interval update.
add_async(Event, End, Start, Packing, Record, StoreID) ->
    ID = record_id(Record),
    GenServerID = name(StoreID),
    gen_server:cast(GenServerID, {add_async, Event, End, Start, Packing, ID}).

%% @doc Remove the given interval from the record
%% with the given ID. Store the changes on disk before
%% returning ok.
delete(End, Start, Record, StoreID) ->
    ID = record_id(Record),
    GenServerID = name(StoreID),
    case catch gen_server:call(GenServerID, {delete, End, Start, ID}, 120000) of
        {'EXIT', {timeout, {gen_server, call, _}}} ->
            {error, timeout};
        Reply ->
            Reply
    end.

%% @doc Remove everything strictly above the given
%% Offset from the record. Store the changes on disk
%% before returning ok.
cut(Offset, Record, StoreID) ->
    ID = record_id(Record),
    GenServerID = name(StoreID),
    case catch gen_server:call(GenServerID, {cut, Offset, ID}, 120000) of
        {'EXIT', {timeout, {gen_server, call, _}}} ->
            {error, timeout};
        Reply ->
            Reply
    end.

%% @doc Check an index offset with concrete or wildcard packing and store selectors.
is_recorded(Offset, Packing, {_, footprint} = Record, any_store) ->
    %% Footprint offsets cannot select stores by their physical byte ranges.
    ID = record_id(Record),
    StoreIDs = ets:select(
        sync_records,
        [{{{ID, '$1'}, '_'}, [], ['$1']}]
    ),
    do_is_recorded_in_stores(
        Offset,
        Packing,
        ID,
        [?DEFAULT_MODULE | lists:delete(?DEFAULT_MODULE, StoreIDs)]
    );
is_recorded(Offset, Packing, Record, StoreID) ->
    do_is_recorded(Offset, Packing, record_id(Record), StoreID).

%% @doc Search only the supplied ordered list of storage modules.
is_recorded_any(Offset, Record, StorageModules) ->
    do_is_recorded_any(Offset, record_id(Record), StorageModules).

%% @doc Return {true, StoreID} or {{true, Packing}, StoreID} if a chunk containing
%% the given Offset is found in the record with the given ID, false otherwise.
%% If several types are recorded for the chunk, only one of them is returned,
%% the choice is not defined. If the chunk is stored in the default storage module,
%% return the type found there. If not, search for a configured storage
%% module covering the given Offset. If there are multiple
%% storage modules with the chunk, the choice is not defined.
%% The offset is 1-based - if a chunk consists of a single
%% byte that is the first byte of the weave, is_recorded(0, ID)
%% returns false and is_recorded(1, ID) returns true.
do_is_recorded(Offset, {ID, Packing}) ->
    case do_is_recorded(Offset, Packing, ID, ?DEFAULT_MODULE) of
        true ->
            {{true, Packing}, ?DEFAULT_MODULE};
        false ->
            StorageModules = lists:filter(
                fun({_, _, ModulePacking}) -> ModulePacking == Packing end,
                arweave_storage:covering_stores(Offset, any_packing)
            ),
            is_recorded_any_by_type(Offset, ID, StorageModules)
    end;
do_is_recorded(Offset, ID) ->
    case do_is_recorded(Offset, ID, ?DEFAULT_MODULE) of
        false ->
            StorageModules = arweave_storage:covering_stores(Offset, any_packing),
            do_is_recorded_any(Offset, ID, StorageModules);
        Reply ->
            {Reply, ?DEFAULT_MODULE}
    end.

%% @doc Return true or {true, Packing} if a chunk containing
%% the given Offset is found in the record with the given ID
%% in the storage module identified by StoreID, false otherwise.
do_is_recorded(Offset, ID, StoreID) ->
    case ets:lookup(sync_records, {ID, StoreID}) of
        [] ->
            false;
        [{_, TID}] ->
            case ar_ets_intervals:is_inside(TID, Offset) of
                false ->
                    false;
                true ->
                    case is_recorded2(Offset, ets:first(sync_records), ID, StoreID) of
                        false ->
                            true;
                        {true, Packing} ->
                            {true, Packing}
                    end
            end
    end.

%% @doc Check a byte with concrete or wildcard packing and store selectors.
do_is_recorded(Offset, any_packing, ID, any_store) ->
    do_is_recorded(Offset, ID);
do_is_recorded(Offset, Packing, ID, any_store) ->
    do_is_recorded(Offset, {ID, Packing});
do_is_recorded(Offset, any_packing, ID, StoreID) ->
    do_is_recorded(Offset, ID, StoreID);
do_is_recorded(Offset, Packing, ID, StoreID) ->
    case ets:lookup(sync_records, {ID, Packing, StoreID}) of
        [] ->
            false;
        [{_, TID}] ->
            ar_ets_intervals:is_inside(TID, Offset)
    end.

%% @doc Return the packings the store's record has a table for.
get_packings(Record, StoreID) ->
    ID = record_id(Record),
    ets:select(sync_records, [{{{ID, '$1', StoreID}, '_'}, [], ['$1']}]).

%% @doc Read the next interval; any_store uses the union of matching records.
get_next_interval(Status, Offset, End, Packing, Record, StoreID) when
    Status =:= synced; Status =:= unsynced
->
    do_get_next_interval(Status, Offset, End, Packing, record_id(Record), StoreID).

%% @doc Collect intervals clipped to (Start, End] in the selected index.
get_intervals(Status, Start, End, Packing, Record, StoreID) when
    Status =:= synced; Status =:= unsynced
->
    ID = record_id(Record),
    collect_intervals(
        Start,
        End,
        fun(Offset) ->
            do_get_next_interval(Status, Offset, End, Packing, ID, StoreID)
        end
    ).

do_get_next_interval(Status, Offset, End, Packing, ID, StoreID) ->
    case Offset >= End of
        true ->
            not_found;
        false ->
            Tables = record_tables(Packing, ID, StoreID),
            do_get_next_interval(Status, Offset, End, Tables)
    end.

record_tables(any_packing, ID, any_store) ->
    ets:select(sync_records, [{{{ID, '_'}, '$1'}, [], ['$1']}]);
record_tables(Packing, ID, any_store) ->
    ets:select(sync_records, [{{{ID, Packing, '_'}, '$1'}, [], ['$1']}]);
record_tables(any_packing, ID, StoreID) ->
    [TID || {_, TID} <- ets:lookup(sync_records, {ID, StoreID})];
record_tables(Packing, ID, StoreID) ->
    [TID || {_, TID} <- ets:lookup(sync_records, {ID, Packing, StoreID})].

do_get_next_interval(_Status, Offset, End, _Tables) when Offset >= End ->
    not_found;
do_get_next_interval(synced, _Offset, _End, []) ->
    not_found;
do_get_next_interval(unsynced, Offset, End, []) ->
    {End, Offset};
do_get_next_interval(synced, Offset, End, [TID]) ->
    ar_ets_intervals:get_next_interval(TID, Offset, End);
do_get_next_interval(unsynced, Offset, End, [TID]) ->
    ar_ets_intervals:get_next_interval_outside(TID, Offset, End);
do_get_next_interval(synced, Offset, End, Tables) ->
    Intervals = [
        Interval
     || TID <- Tables,
        Interval <- [ar_ets_intervals:get_next_interval(TID, Offset, End)],
        Interval =/= not_found
    ],
    case lists:keysort(2, Intervals) of
        [] -> not_found;
        [First | _] -> merge_interval(First, End, Tables)
    end;
do_get_next_interval(unsynced, Offset, End, Tables) ->
    case do_get_next_interval(synced, Offset, End, Tables) of
        not_found -> {End, Offset};
        {_, Start} when Start > Offset -> {Start, Offset};
        {SyncedEnd, _} -> do_get_next_interval(unsynced, SyncedEnd, End, Tables)
    end.

%% @doc Extend a cross-store interval without materializing entire sync records.
merge_interval(Interval, UpperBound, Tables) ->
    Merged = lists:foldl(
        fun(TID, {End, Start}) ->
            Start2 =
                case ar_ets_intervals:get_interval_with_byte(TID, Start) of
                    not_found -> Start;
                    {_, LeftStart} -> min(Start, LeftStart)
                end,
            End2 =
                case ar_ets_intervals:get_next_interval(TID, End, UpperBound) of
                    {RightEnd, RightStart} when RightStart =< End -> RightEnd;
                    _ -> End
                end,
            {End2, Start2}
        end,
        Interval,
        Tables
    ),
    case Merged of
        Interval -> Interval;
        _ -> merge_interval(Merged, UpperBound, Tables)
    end.

%% @doc Return the interval containing the given Offset, including the right bound,
%% excluding the left bound. Return not_found if the given offset does not belong to
%% any interval.
get_interval(Offset, Packing, Record, StoreID) ->
    Tables = record_tables(Packing, record_id(Record), StoreID),
    do_get_interval(Offset, Tables, Tables).

do_get_interval(_Offset, [], _Tables) ->
    not_found;
do_get_interval(Offset, [TID], [TID]) ->
    ar_ets_intervals:get_interval_with_byte(TID, Offset);
do_get_interval(Offset, [TID | Rest], Tables) ->
    case ar_ets_intervals:get_interval_with_byte(TID, Offset) of
        not_found -> do_get_interval(Offset, Rest, Tables);
        Interval -> merge_interval(Interval, infinity, Tables)
    end.

%% @doc Count the selected union within (Start, End], without double counting.
get_intersection_size(End, Start, Packing, Record, StoreID) ->
    Tables = record_tables(Packing, record_id(Record), StoreID),
    case Tables of
        [] ->
            0;
        [TID] ->
            ar_ets_intervals:get_intersection_size(TID, End, Start);
        _ ->
            do_get_intersection_size(Start, End, Tables, 0)
    end.

do_get_intersection_size(Start, End, _Tables, Size) when Start >= End ->
    Size;
do_get_intersection_size(Start, End, Tables, Size) ->
    case do_get_next_interval(synced, Start, End, Tables) of
        not_found ->
            Size;
        {Right, Left} ->
            do_get_intersection_size(
                Right, End, Tables, Size + Right - max(Start, Left)
            )
    end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(StoreID) ->
    ?LOG_INFO([{event, ar_sync_record_start}, {store_id, StoreID}]),
    process_flag(trap_exit, true),
    StorageModule = arweave_storage_module:get_by_id(StoreID),
    DataDir = arweave_config:get([data_dir]),
    {Dir, PartitionNumber} =
        case StorageModule of
            ?DEFAULT_MODULE ->
                {filename:join([DataDir, ?ROCKS_DB_DIR, "ar_sync_record_db"]), undefined};
            Atom when is_atom(Atom) ->
                %% A module without a storage, to use in tests.
                {undefined, undefined};
            {Start, _End, _Packing} ->
                #store_info{path = Path} = arweave_storage:store_info(StoreID),
                {
                    filename:join([
                        Path,
                        ?ROCKS_DB_DIR,
                        "ar_sync_record_db"
                    ]),
                    Start div arweave_constants:partition_size()
                }
        end,
    StateDB = {sync_record, StoreID},
    State = #state{
        state_db = StateDB,
        store_id = StoreID,
        storage_module = StorageModule,
        partition_number = PartitionNumber,
        wal = undefined,
        in_memory = Dir == undefined
    },
    %% The interval tables of a previous incarnation died with it; drop the
    %% registry entries pointing at them before creating fresh tables.
    clear_sync_records_ets(StoreID),
    case Dir of
        undefined ->
            {ok, State};
        _ ->
            ok = arweave_storage_deps:open_database(#{path => Dir, name => StateDB}),
            {WAL, Sizes} = load_sync_records(StateDB, StoreID),
            publish_data_sizes(Sizes, State),
            %% Spread the stores' first snapshots over a period instead of
            %% lining them all up at boot.
            schedule_store_state(
                ?STORE_SYNC_RECORD_FREQUENCY_MS +
                    rand:uniform(?STORE_SYNC_RECORD_FREQUENCY_MS)
            ),
            ?LOG_INFO([{event, ar_sync_record_initialized}, {store_id, StoreID}]),
            {ok, State#state{wal = WAL}}
    end.

handle_call({add, End, Start, ID}, _From, State) ->
    {Reply, State2} = do_add(End, Start, ID, State),
    {reply, Reply, State2};
handle_call({add, End, Start, Packing, ID}, _From, State) ->
    {Reply, State2} = do_add(End, Start, Packing, ID, State),
    {reply, Reply, State2};
handle_call({delete, End, Start, ID}, _From, State) ->
    {Reply, State2} = do_delete(End, Start, ID, State),
    {reply, Reply, State2};
handle_call({cut, Offset, ID}, _From, State) ->
    {Reply, State2} = do_cut(Offset, ID, State),
    {reply, Reply, State2};
handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, ok, State}.

handle_cast(store_state, #state{wal = 0} = State) ->
    %% Nothing changed since the last snapshot.
    schedule_store_state(?STORE_SYNC_RECORD_FREQUENCY_MS),
    {noreply, State};
handle_cast(store_state, State) ->
    case acquire_snapshot_token() of
        false ->
            schedule_store_state(?SNAPSHOT_TOKEN_RETRY_MS),
            {noreply, State};
        true ->
            {_, State2} =
                try
                    store_state(State)
                after
                    release_snapshot_token()
                end,
            %% Snapshot construction materializes every fragmented interval
            %% table. Reclaim that temporary heap before this long-lived
            %% server goes idle.
            erlang:garbage_collect(),
            schedule_store_state(?STORE_SYNC_RECORD_FREQUENCY_MS),
            {noreply, State2}
    end;
handle_cast({add_async, Event, End, Start, ID}, State) ->
    {Reply, State2} = do_add(End, Start, ID, State),
    case Reply of
        ok ->
            ok;
        Error ->
            ?LOG_ERROR([
                {event, Event},
                {operation, add_async},
                {status, failed},
                {sync_record_id, ID},
                {offset, End},
                {error, io_lib:format("~p", [Error])}
            ])
    end,
    {noreply, State2};
handle_cast({add_async, Event, End, Start, Packing, ID}, State) ->
    {Reply, State2} = do_add(End, Start, Packing, ID, State),
    case Reply of
        ok ->
            ok;
        Error ->
            ?LOG_ERROR([
                {event, Event},
                {operation, add_async},
                {status, failed},
                {sync_record_id, ID},
                {offset, End},
                {packing, arweave_storage_deps:encode_packing(Packing, true)},
                {error, io_lib:format("~p", [Error])}
            ])
    end,
    {noreply, State2};
handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info(Message, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
    {noreply, State}.

terminate(Reason, State) ->
    ?LOG_INFO([{event, terminate}, {module, ?MODULE}, {reason, io_lib:format("~p", [Reason])}]),
    store_state(State).

%%%===================================================================
%%% Private functions.
%%%===================================================================

name(StoreID) when is_atom(StoreID) ->
    list_to_atom("ar_sync_record_" ++ atom_to_list(StoreID));
name(StoreID) ->
    list_to_atom("ar_sync_record_" ++ arweave_storage_module:label(StoreID)).

do_add(End, Start, ID, State) ->
    #state{state_db = StateDB, store_id = StoreID, storage_module = Module} = State,
    record_add(End, Start, ID, StoreID),
    {Reply, State2} = update_write_ahead_log({add, {End, Start, ID}}, StateDB, State),
    case Reply of
        ok ->
            emit_add_range(Start, End, ID, #{module => Module});
        _ ->
            ok
    end,
    {Reply, State2}.

do_add(End, Start, Packing, ID, State) ->
    #state{state_db = StateDB, store_id = StoreID, storage_module = Module} = State,
    record_add(End, Start, Packing, ID, StoreID),
    {Reply, State2} = update_write_ahead_log(
        {{add, Packing}, {End, Start, ID}}, StateDB, State
    ),
    case Reply of
        ok ->
            emit_add_range(Start, End, ID, #{module => Module, packing => Packing});
        _ ->
            ok
    end,
    {Reply, State2}.

do_delete(End, Start, ID, State) ->
    #state{state_db = StateDB, store_id = StoreID} = State,
    record_delete(End, Start, ID, StoreID),
    {Reply, State2} = update_write_ahead_log({delete, {End, Start, ID}}, StateDB, State),
    case Reply of
        ok ->
            emit_remove_range(Start, End, StoreID);
        _ ->
            ok
    end,
    {Reply, State2}.

do_cut(Offset, ID, State) ->
    #state{state_db = StateDB, store_id = StoreID} = State,
    record_cut(Offset, ID, StoreID),
    {Reply, State2} = update_write_ahead_log({cut, {Offset, ID}}, StateDB, State),
    case Reply of
        ok ->
            emit_cut(Offset, StoreID);
        _ ->
            ok
    end,
    {Reply, State2}.

%% @doc Add the interval to the {ID, StoreID} ETS interval table.
record_add(End, Start, ID, StoreID) ->
    TID = get_or_create_tid({ID, StoreID}),
    ar_ets_intervals:add(TID, End, Start).

%% @doc Add the interval to the {ID, Packing, StoreID} and {ID, StoreID}
%% ETS interval tables.
record_add(End, Start, Packing, ID, StoreID) ->
    TypeTID = get_or_create_tid({ID, Packing, StoreID}),
    ar_ets_intervals:add(TypeTID, End, Start),
    record_add(End, Start, ID, StoreID).

%% @doc Remove the interval from the {ID, StoreID} ETS interval table and
%% from every {ID, Packing, StoreID} table of the store.
record_delete(End, Start, ID, StoreID) ->
    TID = get_or_create_tid({ID, StoreID}),
    ar_ets_intervals:delete(TID, End, Start),
    ets:foldl(
        fun
            ({{ID2, _, SID}, TypeTID}, _) when ID2 == ID, SID == StoreID ->
                ar_ets_intervals:delete(TypeTID, End, Start);
            (_, _) ->
                ok
        end,
        ok,
        sync_records
    ).

%% @doc Cut the {ID, StoreID} ETS interval table and every
%% {ID, Packing, StoreID} table of the store at the given Offset.
record_cut(Offset, ID, StoreID) ->
    TID = get_or_create_tid({ID, StoreID}),
    ar_ets_intervals:cut(TID, Offset),
    ets:foldl(
        fun
            ({{ID2, _, SID}, TypeTID}, _) when ID2 == ID, SID == StoreID ->
                ar_ets_intervals:cut(TypeTID, Offset);
            (_, _) ->
                ok
        end,
        ok,
        sync_records
    ).

collect_intervals(Start, End, NextFun) ->
    do_collect_intervals(Start, End, NextFun, ar_intervals:new()).

do_collect_intervals(Start, End, _NextFun, Intervals) when Start >= End ->
    Intervals;
do_collect_intervals(Start, End, NextFun, Intervals) ->
    case NextFun(Start) of
        not_found ->
            Intervals;
        {End2, Start2} ->
            End3 = min(End2, End),
            Start3 = max(Start2, Start),
            do_collect_intervals(
                End3,
                End,
                NextFun,
                ar_intervals:add(Intervals, End3, Start3)
            )
    end.

%% @doc Resolve an index without changing any persisted record identifier.
record_id({ar_data_sync, footprint}) -> ar_data_sync_footprints;
record_id({ID, byte}) when is_atom(ID), ID /= ar_data_sync_footprints -> ID.

do_is_recorded_in_stores(_Offset, _Packing, _ID, []) ->
    false;
do_is_recorded_in_stores(Offset, Packing, ID, [StoreID | Rest]) ->
    case do_is_recorded(Offset, Packing, ID, StoreID) of
        false -> do_is_recorded_in_stores(Offset, Packing, ID, Rest);
        true when Packing /= any_packing -> {{true, Packing}, StoreID};
        Reply -> {Reply, StoreID}
    end.

is_recorded_any_by_type(Offset, ID, [StorageModule | StorageModules]) ->
    StoreID = arweave_storage_module:id(StorageModule),
    {_, _, Packing} = StorageModule,
    case do_is_recorded(Offset, Packing, ID, StoreID) of
        true ->
            {{true, Packing}, StoreID};
        false ->
            is_recorded_any_by_type(Offset, ID, StorageModules)
    end;
is_recorded_any_by_type(_Offset, _ID, []) ->
    false.

do_is_recorded_any(Offset, ID, [StorageModule | StorageModules]) ->
    StoreID = arweave_storage_module:id(StorageModule),
    case do_is_recorded(Offset, ID, StoreID) of
        false ->
            do_is_recorded_any(Offset, ID, StorageModules);
        Reply ->
            {Reply, StoreID}
    end;
do_is_recorded_any(_Offset, _ID, []) ->
    false.

is_recorded2(_Offset, '$end_of_table', _ID, _StoreID) ->
    false;
is_recorded2(Offset, {ID, Packing, StoreID}, ID, StoreID) ->
    case ets:lookup(sync_records, {ID, Packing, StoreID}) of
        [{_, TID}] ->
            case ar_ets_intervals:is_inside(TID, Offset) of
                true ->
                    {true, Packing};
                false ->
                    is_recorded2(
                        Offset,
                        ets:next(sync_records, {ID, Packing, StoreID}),
                        ID,
                        StoreID
                    )
            end;
        [] ->
            %% Very unlucky timing.
            false
    end;
is_recorded2(Offset, Key, ID, StoreID) ->
    is_recorded2(Offset, ets:next(sync_records, Key), ID, StoreID).

%% @doc Populate the ETS interval tables from the on-disk snapshot and replay
%% the write-ahead log on top of them. Return the number of WAL entries and
%% the snapshot's data sizes by packing.
load_sync_records(StateDB, StoreID) ->
    {SyncRecordByID, SyncRecordByIDType} =
        case arweave_storage_deps:get_database(StateDB, ?SYNC_RECORDS_KEY) of
            not_found ->
                {#{}, #{}};
            {ok, V} ->
                binary_to_term(V, [safe])
        end,
    initialize_sync_record_by_id_type_ets(SyncRecordByIDType, StoreID),
    initialize_sync_record_by_id_ets(SyncRecordByID, StoreID),
    {replay_write_ahead_log(StateDB, StoreID), data_sizes(SyncRecordByIDType)}.

%% @doc Return [{Packing, Size}] for the ar_data_sync records of a snapshot.
data_sizes(SyncRecordByIDType) ->
    [
        {Packing, ar_intervals:sum(TypeRecord)}
     || {{ar_data_sync, Packing}, TypeRecord} <-
            maps:to_list(SyncRecordByIDType)
    ].

replay_write_ahead_log(StateDB, StoreID) ->
    WAL =
        case arweave_storage_deps:get_database(StateDB, ?WAL_COUNT_KEY) of
            not_found ->
                0;
            {ok, V} ->
                binary:decode_unsigned(V)
        end,
    Module = arweave_storage_module:get_by_id(StoreID),
    replay_write_ahead_log(1, WAL, StateDB, StoreID, Module).

replay_write_ahead_log(N, WAL, _StateDB, _StoreID, _Module) when N > WAL ->
    WAL;
replay_write_ahead_log(N, WAL, StateDB, StoreID, Module) ->
    case arweave_storage_deps:get_database(StateDB, binary:encode_unsigned(N)) of
        not_found ->
            %% The VM crashed after recording the number.
            WAL;
        {ok, V} ->
            {Op, Params} = binary_to_term(V, [safe]),
            apply_write_ahead_log_op(Op, Params, StoreID, Module),
            replay_write_ahead_log(N + 1, WAL, StateDB, StoreID, Module)
    end.

%% @doc Apply a replayed WAL operation to the ETS interval tables and emit
%% the corresponding sync_record event.
apply_write_ahead_log_op(add, {End, Start, ID}, StoreID, Module) ->
    record_add(End, Start, ID, StoreID),
    emit_add_range(Start, End, ID, #{module => Module});
apply_write_ahead_log_op({add, Packing}, {End, Start, ID}, StoreID, Module) ->
    record_add(End, Start, Packing, ID, StoreID),
    emit_add_range(Start, End, ID, #{module => Module, packing => Packing});
apply_write_ahead_log_op(delete, {End, Start, ID}, StoreID, Module) ->
    record_delete(End, Start, ID, StoreID),
    emit_remove_range(Start, End, Module);
apply_write_ahead_log_op(cut, {Offset, ID}, StoreID, Module) ->
    record_cut(Offset, ID, StoreID),
    emit_cut(Offset, Module).

emit_add_range(Start, End, ar_data_sync, Options) ->
    arweave_storage_deps:send_event(sync_record, {add_range, Start, End, ar_data_sync, Options});
emit_add_range(Start, End, ar_data_sync_footprints, Options) ->
    arweave_storage_deps:send_event(
        sync_record, {add_range, Start, End, ar_data_sync_footprints, Options}
    );
emit_add_range(_Start, _End, _ID, _Options) ->
    ok.

emit_remove_range(Start, End, Module) ->
    arweave_storage_deps:send_event(sync_record, {remove_range, Start, End, Module}).

emit_cut(Offset, Module) ->
    arweave_storage_deps:send_event(sync_record, {cut, Offset, Module}).

%% @doc Remove the registry entries of the given store from the sync_records
%% ETS table.
clear_sync_records_ets(StoreID) ->
    ets:match_delete(sync_records, {{'_', StoreID}, '_'}),
    ets:match_delete(sync_records, {{'_', '_', StoreID}, '_'}).

initialize_sync_record_by_id_ets(SyncRecordByID, StoreID) ->
    Iterator = maps:iterator(SyncRecordByID),
    initialize_sync_record_by_id_ets2(maps:next(Iterator), StoreID).

initialize_sync_record_by_id_ets2(none, _StoreID) ->
    ok;
initialize_sync_record_by_id_ets2({ID, SyncRecord, Iterator}, StoreID) ->
    TID = ets:new(sync_record_type, [ordered_set, public, {read_concurrency, true}]),
    ar_ets_intervals:init_from_gb_set(TID, SyncRecord),
    ets:insert(sync_records, {{ID, StoreID}, TID}),
    initialize_sync_record_by_id_ets2(maps:next(Iterator), StoreID).

initialize_sync_record_by_id_type_ets(SyncRecordByIDType, StoreID) ->
    Iterator = maps:iterator(SyncRecordByIDType),
    initialize_sync_record_by_id_type_ets2(maps:next(Iterator), StoreID).

initialize_sync_record_by_id_type_ets2(none, _StoreID) ->
    ok;
initialize_sync_record_by_id_type_ets2({{ID, Packing}, SyncRecord, Iterator}, StoreID) ->
    TID = ets:new(sync_record_type, [ordered_set, public, {read_concurrency, true}]),
    ar_ets_intervals:init_from_gb_set(TID, SyncRecord),
    ets:insert(sync_records, {{ID, Packing, StoreID}, TID}),
    initialize_sync_record_by_id_type_ets2(maps:next(Iterator), StoreID).

%% @doc Collect the intervals of every record of the given store from the
%% ETS tables into the {SyncRecordByID, SyncRecordByIDType} maps used as the
%% on-disk snapshot format.
read_sync_records_from_ets(StoreID) ->
    ets:foldl(
        fun
            ({{ID, SID}, TID}, {ByID, ByIDType}) when SID == StoreID ->
                {maps:put(ID, ar_ets_intervals:to_gb_set(TID), ByID), ByIDType};
            ({{ID, Packing, SID}, TID}, {ByID, ByIDType}) when SID == StoreID ->
                {ByID, maps:put({ID, Packing}, ar_ets_intervals:to_gb_set(TID), ByIDType)};
            (_, Acc) ->
                Acc
        end,
        {#{}, #{}},
        sync_records
    ).

store_state(#state{in_memory = true}) ->
    ok;
store_state(#state{wal = 0} = State) ->
    {ok, State};
store_state(State) ->
    #state{state_db = StateDB, store_id = StoreID} = State,
    {SyncRecordByID, SyncRecordByIDType} = read_sync_records_from_ets(StoreID),
    StoreSyncRecords =
        arweave_storage_deps:put_database(
            StateDB,
            ?SYNC_RECORDS_KEY,
            %% Fragmented records can serialize to hundreds of MB. Keep the
            %% single RocksDB value small enough to avoid long write stalls.
            term_to_binary(
                {SyncRecordByID, SyncRecordByIDType}, [compressed]
            )
        ),
    ResetWAL =
        case StoreSyncRecords of
            {error, _} = Error ->
                Error;
            ok ->
                arweave_storage_deps:put_database(
                    StateDB, ?WAL_COUNT_KEY, binary:encode_unsigned(0)
                )
        end,
    case ResetWAL of
        {error, Reason} = Error2 ->
            ?LOG_WARNING([
                {event, failed_to_store_state},
                {reason, io_lib:format("~p", [Reason])}
            ]),
            {Error2, State};
        ok ->
            publish_data_sizes(data_sizes(SyncRecordByIDType), State),
            {ok, State#state{wal = 0}}
    end.

%% @doc Schedule the next snapshot attempt of this store.
schedule_store_state(Delay) ->
    {ok, _} = arweave_storage_deps:apply_after(
        Delay,
        gen_server,
        cast,
        [self(), store_state],
        #{skip_on_shutdown => false}
    ),
    ok.

%% @doc Take the node-wide snapshot token, unless a live store holds it. A
%% holder that died mid-snapshot forfeits it. Only the atomic insert_new grants
%% the token: the liveness check merely clears a dead holder's row, so of two
%% stores that both find it dead, exactly one wins the retry.
acquire_snapshot_token() ->
    case ets:insert_new(sync_records, {?SNAPSHOT_TOKEN_KEY, self()}) of
        true ->
            true;
        false ->
            case ets:lookup(sync_records, ?SNAPSHOT_TOKEN_KEY) of
                [{_, Holder}] when Holder == self() ->
                    true;
                [{_, Holder}] ->
                    case is_process_alive(Holder) of
                        true ->
                            false;
                        false ->
                            ets:delete_object(
                                sync_records, {?SNAPSHOT_TOKEN_KEY, Holder}
                            ),
                            acquire_snapshot_token()
                    end;
                [] ->
                    acquire_snapshot_token()
            end
    end.

release_snapshot_token() ->
    ets:delete_object(sync_records, {?SNAPSHOT_TOKEN_KEY, self()}),
    ok.

publish_data_sizes(Sizes, State) ->
    #state{
        storage_module = StorageModule,
        partition_number = PartitionNumber
    } = State,
    lists:foreach(
        fun({Packing, Size}) ->
            publish_data_size(StorageModule, Packing, PartitionNumber, Size)
        end,
        Sizes
    ).

%% @doc Return the last persisted data sizes for late subscribers.
get_data_sizes() ->
    [DataSize || {_, DataSize} <- ets:tab2list(arweave_storage_data_sizes)].

%% @doc Check whether a record is registered without reading its intervals.
sync_record_exists(Packing, Record, StoreID) ->
    record_tables(Packing, record_id(Record), StoreID) =/= [].

publish_data_size(StorageModule, Packing, PartitionNumber, Size) ->
    StoreID = arweave_storage_module:id(StorageModule),
    DataSize = {StorageModule, Packing, PartitionNumber, Size},
    ets:insert(arweave_storage_data_sizes, {{StoreID, Packing}, DataSize}),
    arweave_storage_deps:send_event(chunk_storage, {data_size, DataSize}).

get_or_create_tid(Key) ->
    case ets:lookup(sync_records, Key) of
        [] ->
            TID = ets:new(sync_record_type, [ordered_set, public, {read_concurrency, true}]),
            ets:insert(sync_records, {Key, TID}),
            TID;
        [{_, TID2}] ->
            TID2
    end.

update_write_ahead_log(_OpParams, _StateDB, #state{in_memory = true} = State) ->
    {ok, State};
update_write_ahead_log(OpParams, StateDB, State) ->
    #state{
        wal = WAL
    } = State,
    case
        arweave_storage_deps:put_database(
            StateDB, binary:encode_unsigned(WAL + 1), term_to_binary(OpParams)
        )
    of
        {error, _Reason} = Error ->
            {Error, State};
        ok ->
            case
                arweave_storage_deps:put_database(
                    StateDB, ?WAL_COUNT_KEY, binary:encode_unsigned(WAL + 1)
                )
            of
                ok ->
                    {ok, State#state{wal = WAL + 1}};
                Error2 ->
                    {Error2, State}
            end
    end.
