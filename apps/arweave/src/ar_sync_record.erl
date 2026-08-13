-module(ar_sync_record).

-behaviour(gen_server).

-export([start_link/2, get/2, get/3, add/4, add/5, add_async/5, add_async/6, delete/4, cut/3,
        is_recorded/2, is_recorded/3, is_recorded/4, is_recorded_any/3,
        get_next_synced_interval/4, get_next_synced_interval/5,
        get_next_unsynced_interval/4, get_next_unsynced_interval/5,
        collect_unsynced_intervals/4, collect_synced_intervals/4, collect_synced_intervals/5,
        get_interval/3, get_intersection_size/4, name/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include("ar.hrl").

-include_lib("eunit/include/eunit.hrl").

%% The kv storage key to the sync records.
-define(SYNC_RECORDS_KEY, <<"sync_records">>).

%% The kv key of the write ahead log counter.
-define(WAL_COUNT_KEY, <<"wal">>).

%% The frequency of dumping sync records on disk.
-ifdef(AR_TEST).
-define(STORE_SYNC_RECORD_FREQUENCY_MS, 1000).
-else.
-define(STORE_SYNC_RECORD_FREQUENCY_MS, 60 * 1000).
-endif.

%% The intervals themselves are NOT kept in the server state. The single
%% source of truth is a set of ar_ets_intervals tables, one per record
%% (keyed {ID, StoreID} and {ID, Packing, StoreID} in the `sync_records'
%% registry), each holding non-overlapping intervals of global byte
%% offsets {End, Start} denoting some synced data. End offsets are defined
%% on [1, WeaveSize], start offsets are defined on [0, WeaveSize).
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

%% @doc Return the set of intervals. Reads the ETS interval table directly;
%% returns an empty set when the record does not exist.
get(ID, StoreID) ->
    read_intervals({ID, StoreID}).

%% @doc Return the set of intervals. Reads the ETS interval table directly;
%% returns an empty set when the record does not exist.
get(ID, Packing, StoreID) ->
    read_intervals({ID, Packing, StoreID}).

%% @doc Add the given interval to the record with the
%% given ID. Store the changes on disk before returning ok.
add(End, Start, ID, StoreID) ->
    GenServerID = name(StoreID),
    case catch gen_server:call(GenServerID, {add, End, Start, ID}, 120000) of
        {'EXIT', {timeout, {gen_server, call, _}}} ->
            {error, timeout};
        Reply ->
            Reply
    end.

%% @doc Add the given interval to the record with the
%% given ID and Packing. Store the changes on disk before
%% returning ok.
add(End, Start, Packing, ID, StoreID) ->
    GenServerID = name(StoreID),
    case catch gen_server:call(GenServerID, {add, End, Start, Packing, ID}, 120000) of
        {'EXIT', {timeout, {gen_server, call, _}}} ->
            {error, timeout};
        Reply ->
            Reply
    end.

%% @doc Special case of add/4.
add_async(Event, End, Start, ID, StoreID) ->
    GenServerID = name(StoreID),
    gen_server:cast(GenServerID, {add_async, Event, End, Start, ID}).

%% @doc Special case of add/5 for repacked chunks. When repacking the ar_sync_record add
%% happens at the end so we don't need to block on it to complete.
add_async(Event, End, Start, Packing, ID, StoreID) ->
    GenServerID = name(StoreID),
    gen_server:cast(GenServerID, {add_async, Event, End, Start, Packing, ID}).

%% @doc Remove the given interval from the record
%% with the given ID. Store the changes on disk before
%% returning ok.
delete(End, Start, ID, StoreID) ->
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
cut(Offset, ID, StoreID) ->
    GenServerID = name(StoreID),
    case catch gen_server:call(GenServerID, {cut, Offset, ID}, 120000) of
        {'EXIT', {timeout, {gen_server, call, _}}} ->
            {error, timeout};
        Reply ->
            Reply
    end.

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
is_recorded(Offset, {ID, Packing}) ->
    case is_recorded(Offset, Packing, ID, ?DEFAULT_MODULE) of
        true ->
            {{true, Packing}, ?DEFAULT_MODULE};
        false ->
            StorageModules = lists:filter(
                fun({_, _, ModulePacking}) -> ModulePacking == Packing end,
                ar_storage_module:get_all(Offset)),
            is_recorded_any_by_type(Offset, ID, StorageModules)
    end;
is_recorded(Offset, ID) ->
    case is_recorded(Offset, ID, ?DEFAULT_MODULE) of
        false ->
            StorageModules = ar_storage_module:get_all(Offset),
            is_recorded_any(Offset, ID, StorageModules);
        Reply ->
            {Reply, ?DEFAULT_MODULE}
    end.

%% @doc Return true or {true, Packing} if a chunk containing
%% the given Offset is found in the record with the given ID
%% in the storage module identified by StoreID, false otherwise.
is_recorded(Offset, ID, StoreID) ->
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

%% @doc Return true if a chunk containing the given Offset and Packing
%% is found in the record in the storage module identified by StoreID,
%% false otherwise.
is_recorded(Offset, Packing, ID, StoreID) ->
    case ets:lookup(sync_records, {ID, Packing, StoreID}) of
        [] ->
            false;
        [{_, TID}] ->
            ar_ets_intervals:is_inside(TID, Offset)
    end.

%% @doc Return the lowest synced interval with the end offset strictly above the given Offset
%% and at most EndOffsetUpperBound.
%% Return not_found if there are no such intervals.
get_next_synced_interval(Offset, EndOffsetUpperBound, ID, StoreID) ->
    case ets:lookup(sync_records, {ID, StoreID}) of
        [] ->
            not_found;
        [{_, TID}] ->
            ar_ets_intervals:get_next_interval(TID, Offset, EndOffsetUpperBound)
    end.

%% @doc Return the lowest unsynced interval with the end offset strictly above the given Offset
%% and at most EndOffsetUpperBound.
%% Return not_found when Offset >= EndOffsetUpperBound.
%% Return {EndOffsetUpperBound, Offset} when no records are found.
get_next_unsynced_interval(Offset, EndOffsetUpperBound, _ID, _StoreID)
        when Offset >= EndOffsetUpperBound ->
    not_found;
get_next_unsynced_interval(Offset, EndOffsetUpperBound, ID, StoreID) ->
    case ets:lookup(sync_records, {ID, StoreID}) of
        [] ->
            {EndOffsetUpperBound, Offset};
        [{_, TID}] ->
            ar_ets_intervals:get_next_interval_outside(TID, Offset, EndOffsetUpperBound)
    end.

%% @doc Return every unsynced interval in [Start, End) for ID and StoreID.
collect_unsynced_intervals(Start, End, ID, StoreID) ->
    collect_intervals(Start, End,
        fun(Offset) -> get_next_unsynced_interval(Offset, End, ID, StoreID) end).

%% @doc Return every synced interval in [Start, End) for ID and StoreID.
collect_synced_intervals(Start, End, ID, StoreID) ->
    collect_intervals(Start, End,
        fun(Offset) -> get_next_synced_interval(Offset, End, ID, StoreID) end).

%% @doc Return every synced interval with the given Packing in [Start, End)
%% for ID and StoreID.
collect_synced_intervals(Start, End, Packing, ID, StoreID) ->
    collect_intervals(Start, End,
        fun(Offset) -> get_next_synced_interval(Offset, End, Packing, ID, StoreID) end).

%% @doc Return the lowest synced interval with the end offset strictly above the given Offset
%% and at most EndOffsetUpperBound.
%% Return not_found if there are no such intervals.
get_next_synced_interval(Offset, EndOffsetUpperBound, Packing, ID, StoreID) ->
    case ets:lookup(sync_records, {ID, Packing, StoreID}) of
        [] ->
            not_found;
        [{_, TID}] ->
            ar_ets_intervals:get_next_interval(TID, Offset, EndOffsetUpperBound)
    end.

%% @doc Return the lowest unsynced interval with the end offset strictly above the given Offset
%% and at most EndOffsetUpperBound.
%% Return not_found when Offset >= EndOffsetUpperBound.
%% Return {EndOffsetUpperBound, Offset} when no records are found.
get_next_unsynced_interval(Offset, EndOffsetUpperBound, _Packing, _ID, _StoreID)
        when Offset >= EndOffsetUpperBound ->
    not_found;
get_next_unsynced_interval(Offset, EndOffsetUpperBound, Packing, ID, StoreID) ->
    case ets:lookup(sync_records, {ID, Packing, StoreID}) of
        [] ->
            {EndOffsetUpperBound, Offset};
        [{_, TID}] ->
            ar_ets_intervals:get_next_interval_outside(TID, Offset, EndOffsetUpperBound)
    end.

%% @doc Return the interval containing the given Offset, including the right bound,
%% excluding the left bound. Return not_found if the given offset does not belong to
%% any interval.
get_interval(Offset, ID, StoreID) ->
    case ets:lookup(sync_records, {ID, StoreID}) of
        [] ->
            not_found;
        [{_, TID}] ->
            ar_ets_intervals:get_interval_with_byte(TID, Offset)
    end.

%% @doc Return the size of the intersection between the intervals and the given range.
%% Return 0 if the given ID and StoreID are not found.
get_intersection_size(End, Start, ID, StoreID) ->
    case ets:lookup(sync_records, {ID, StoreID}) of
        [] ->
            0;
        [{_, TID}] ->
            ar_ets_intervals:get_intersection_size(TID, End, Start)
    end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(StoreID) ->
    ?LOG_INFO([{event, ar_sync_record_start}, {store_id, StoreID}]),
    process_flag(trap_exit, true),
    StorageModule = ar_storage_module:get_by_id(StoreID),
    DataDir = arweave_config:get([data_dir]),
    {Dir, PartitionNumber} =
        case StorageModule of
            ?DEFAULT_MODULE ->
                {filename:join([DataDir, ?ROCKS_DB_DIR, "ar_sync_record_db"]),
                    undefined};
            Atom when is_atom(Atom) ->
                %% A module without a storage, to use in tests.
                {undefined, undefined};
            {Start, _End, _Packing} ->
                {filename:join([ar_chunk_storage:storage_module_path(
                            DataDir, StoreID), ?ROCKS_DB_DIR,
                        "ar_sync_record_db"]),
                    ar_node:get_partition_number(Start)}
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
            ok = ar_kv:open(#{ path => Dir, name => StateDB }),
            gen_server:cast(self(), store_state),
            WAL = load_sync_records(StateDB, StoreID),
            ?LOG_INFO([{event, ar_sync_record_initialized}, {store_id, StoreID}]),
            {ok, State#state{ wal = WAL }}
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

handle_cast(store_state, State) ->
    {_, State2} = store_state(State),
    {ok, _} = ar_timer:apply_after(
        ?STORE_SYNC_RECORD_FREQUENCY_MS,
        gen_server,
        cast,
        [self(), store_state],
        #{ skip_on_shutdown => false }
    ),
    {noreply, State2};

handle_cast({add_async, Event, End, Start, ID}, State) ->
    {Reply, State2} = do_add(End, Start, ID, State),
    case Reply of
        ok ->
            ok;
        Error ->
            ?LOG_ERROR([{event, Event},
                    {operation, add_async},
                    {status, failed},
                    {sync_record_id, ID},
                    {offset, End},
                    {error, io_lib:format("~p", [Error])}])
    end,
    {noreply, State2};

handle_cast({add_async, Event, End, Start, Packing, ID}, State) ->
    {Reply, State2} = do_add(End, Start, Packing, ID, State),
    case Reply of
        ok ->
            ok;
        Error ->
            ?LOG_ERROR([{event, Event},
                    {operation, add_async},
                    {status, failed},
                    {sync_record_id, ID},
                    {offset, End},
                    {packing, ar_serialize:encode_packing(Packing, true)},
                    {error, io_lib:format("~p", [Error])}])
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
    list_to_atom("ar_sync_record_" ++ ar_storage_module:label(StoreID)).

do_add(End, Start, ID, State) ->
    #state{ state_db = StateDB, store_id = StoreID, storage_module = Module } = State,
    record_add(End, Start, ID, StoreID),
    {Reply, State2} = update_write_ahead_log({add, {End, Start, ID}}, StateDB, State),
    case Reply of
        ok ->
            emit_add_range(Start, End, ID, #{ module => Module });
        _ ->
            ok
    end,
    {Reply, State2}.

do_add(End, Start, Packing, ID, State) ->
    #state{ state_db = StateDB, store_id = StoreID, storage_module = Module } = State,
    record_add(End, Start, Packing, ID, StoreID),
    {Reply, State2} = update_write_ahead_log(
        {{add, Packing}, {End, Start, ID}}, StateDB, State),
    case Reply of
        ok ->
            emit_add_range(Start, End, ID, #{ module => Module, packing => Packing });
        _ ->
            ok
    end,
    {Reply, State2}.

do_delete(End, Start, ID, State) ->
    #state{ state_db = StateDB, store_id = StoreID } = State,
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
    #state{ state_db = StateDB, store_id = StoreID } = State,
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
        fun    ({{ID2, _, SID}, TypeTID}, _) when ID2 == ID, SID == StoreID ->
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
        fun    ({{ID2, _, SID}, TypeTID}, _) when ID2 == ID, SID == StoreID ->
                ar_ets_intervals:cut(TypeTID, Offset);
            (_, _) ->
                ok
        end,
        ok,
        sync_records
    ).

%% @doc Return the intervals stored in the ETS table registered under Key,
%% an empty set when the record does not exist.
read_intervals(Key) ->
    case ets:lookup(sync_records, Key) of
        [] ->
            ar_intervals:new();
        [{_, TID}] ->
            ar_ets_intervals:to_gb_set(TID)
    end.

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
            do_collect_intervals(End3, End, NextFun,
                ar_intervals:add(Intervals, End3, Start3))
    end.

is_recorded_any_by_type(Offset, ID, [StorageModule | StorageModules]) ->
    StoreID = ar_storage_module:id(StorageModule),
    {_, _, Packing} = StorageModule,
    case is_recorded(Offset, Packing, ID, StoreID) of
        true ->
            {{true, Packing}, StoreID};
        false ->
            is_recorded_any_by_type(Offset, ID, StorageModules)
    end;
is_recorded_any_by_type(_Offset, _ID, []) ->
    false.

is_recorded_any(Offset, ID, [StorageModule | StorageModules]) ->
    StoreID = ar_storage_module:id(StorageModule),
    case is_recorded(Offset, ID, StoreID) of
        false ->
            is_recorded_any(Offset, ID, StorageModules);
        Reply ->
            {Reply, StoreID}
    end;
is_recorded_any(_Offset, _ID, []) ->
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
                    is_recorded2(Offset, ets:next(sync_records, {ID, Packing, StoreID}), ID,
                            StoreID)
            end;
        [] ->
            %% Very unlucky timing.
            false
    end;
is_recorded2(Offset, Key, ID, StoreID) ->
    is_recorded2(Offset, ets:next(sync_records, Key), ID, StoreID).

%% @doc Populate the ETS interval tables from the on-disk snapshot and replay
%% the write-ahead log on top of them. Return the number of WAL entries.
load_sync_records(StateDB, StoreID) ->
    {SyncRecordByID, SyncRecordByIDType} =
        case ar_kv:get(StateDB, ?SYNC_RECORDS_KEY) of
            not_found ->
                {#{}, #{}};
            {ok, V} ->
                binary_to_term(V, [safe])
        end,
    initialize_sync_record_by_id_type_ets(SyncRecordByIDType, StoreID),
    initialize_sync_record_by_id_ets(SyncRecordByID, StoreID),
    replay_write_ahead_log(StateDB, StoreID).

replay_write_ahead_log(StateDB, StoreID) ->
    WAL =
        case ar_kv:get(StateDB, ?WAL_COUNT_KEY) of
            not_found ->
                0;
            {ok, V} ->
                binary:decode_unsigned(V)
        end,
    Module = ar_storage_module:get_by_id(StoreID),
    replay_write_ahead_log(1, WAL, StateDB, StoreID, Module).

replay_write_ahead_log(N, WAL, _StateDB, _StoreID, _Module) when N > WAL ->
    WAL;
replay_write_ahead_log(N, WAL, StateDB, StoreID, Module) ->
    case ar_kv:get(StateDB, binary:encode_unsigned(N)) of
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
    emit_add_range(Start, End, ID, #{ module => Module });
apply_write_ahead_log_op({add, Packing}, {End, Start, ID}, StoreID, Module) ->
    record_add(End, Start, Packing, ID, StoreID),
    emit_add_range(Start, End, ID, #{ module => Module, packing => Packing });
apply_write_ahead_log_op(delete, {End, Start, ID}, StoreID, Module) ->
    record_delete(End, Start, ID, StoreID),
    emit_remove_range(Start, End, Module);
apply_write_ahead_log_op(cut, {Offset, ID}, StoreID, Module) ->
    record_cut(Offset, ID, StoreID),
    emit_cut(Offset, Module).

emit_add_range(Start, End, ar_data_sync, Options) ->
    ar_events:send(sync_record, {add_range, Start, End, ar_data_sync, Options});
emit_add_range(Start, End, ar_data_sync_footprints, Options) ->
    ar_events:send(sync_record, {add_range, Start, End, ar_data_sync_footprints, Options});
emit_add_range(_Start, _End, _ID, _Options) ->
    ok.

emit_remove_range(Start, End, Module) ->
    ar_events:send(sync_record, {remove_range, Start, End, Module}).

emit_cut(Offset, Module) ->
    ar_events:send(sync_record, {cut, Offset, Module}).

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
        fun    ({{ID, SID}, TID}, {ByID, ByIDType}) when SID == StoreID ->
                {maps:put(ID, ar_ets_intervals:to_gb_set(TID), ByID), ByIDType};
            ({{ID, Packing, SID}, TID}, {ByID, ByIDType}) when SID == StoreID ->
                {ByID, maps:put({ID, Packing}, ar_ets_intervals:to_gb_set(TID), ByIDType)};
            (_, Acc) ->
                Acc
        end,
        {#{}, #{}},
        sync_records
    ).

store_state(#state{ in_memory = true }) ->
    ok;
store_state(State) ->
    #state{ state_db = StateDB, store_id = StoreID,
            storage_module = StorageModule,
            partition_number = PartitionNumber } = State,
    {SyncRecordByID, SyncRecordByIDType} = read_sync_records_from_ets(StoreID),
    StoreSyncRecords =
        ar_kv:put(
            StateDB,
            ?SYNC_RECORDS_KEY,
            term_to_binary({SyncRecordByID, SyncRecordByIDType})
        ),
    ResetWAL =
        case StoreSyncRecords of
            {error, _} = Error ->
                Error;
            ok ->
                ar_kv:put(StateDB, ?WAL_COUNT_KEY, binary:encode_unsigned(0))
        end,
    case ResetWAL of
        {error, Reason} = Error2 ->
            ?LOG_WARNING([
                {event, failed_to_store_state},
                {reason, io_lib:format("~p", [Reason])}
            ]),
            {Error2, State};
        ok ->
            maps:map(
                fun    ({ar_data_sync, Packing}, TypeRecord) ->
                        ar_mining_stats:set_storage_module_data_size(
                            StorageModule, Packing, PartitionNumber,
                            ar_intervals:sum(TypeRecord));
                    (_, _) ->
                        ok
                end,
                SyncRecordByIDType
            ),
            {ok, State#state{ wal = 0 }}
    end.

get_or_create_tid(Key) ->
    case ets:lookup(sync_records, Key) of
        [] ->
            TID = ets:new(sync_record_type, [ordered_set, public, {read_concurrency, true}]),
            ets:insert(sync_records, {Key, TID}),
            TID;
        [{_, TID2}] ->
            TID2
    end.

update_write_ahead_log(_OpParams, _StateDB, #state{ in_memory = true } = State) ->
    {ok, State};
update_write_ahead_log(OpParams, StateDB, State) ->
    #state{
        wal = WAL
    } = State,
    case ar_kv:put(StateDB, binary:encode_unsigned(WAL + 1), term_to_binary(OpParams)) of
        {error, _Reason} = Error ->
            {Error, State};
        ok ->
            case ar_kv:put(StateDB, ?WAL_COUNT_KEY, binary:encode_unsigned(WAL + 1)) of
                ok ->
                    {ok, State#state{ wal = WAL + 1 }};
                Error2 ->
                    {Error2, State}
            end
    end.

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).

get_after_add_test_() ->
    ar_test_util:with_mocked([
        {ar_storage_module, get_by_id, fun
            (test_sync_record_get) -> test_sync_record_get;
            (StoreID) -> meck:passthrough([StoreID])
        end}
    ], fun test_get_after_add/0, 30).

%% @doc Exercise the direct-ETS read path of get/2,3: reads reflect adds,
%% deletes, and cuts without going through the server, and a record that was
%% never created reads as the empty set.
test_get_after_add() ->
    StoreID = test_sync_record_get,
    ID = test_sync_record_get_id,
    {ok, Pid} = start_link(name(StoreID), StoreID),
    ?assertEqual([], ar_intervals:to_list(get(ID, StoreID))),
    ?assertEqual([], ar_intervals:to_list(get(ID, unpacked, StoreID))),
    ok = add(2, 0, unpacked, ID, StoreID),
    ?assertEqual([{2, 0}], ar_intervals:to_list(get(ID, StoreID))),
    ?assertEqual([{2, 0}], ar_intervals:to_list(get(ID, unpacked, StoreID))),
    ok = add(4, 3, ID, StoreID),
    ?assertEqual([{2, 0}, {4, 3}], ar_intervals:to_list(get(ID, StoreID))),
    ?assertEqual([{2, 0}], ar_intervals:to_list(get(ID, unpacked, StoreID))),
    ok = delete(2, 1, ID, StoreID),
    ?assertEqual([{1, 0}, {4, 3}], ar_intervals:to_list(get(ID, StoreID))),
    ?assertEqual([{1, 0}], ar_intervals:to_list(get(ID, unpacked, StoreID))),
    ok = cut(1, ID, StoreID),
    ?assertEqual([{1, 0}], ar_intervals:to_list(get(ID, StoreID))),
    kill(Pid).

persistence_roundtrip_test_() ->
    ar_test_util:with_mocked([
        {ar_storage_module, get_by_id, fun
            (test_sync_record_persist) -> {ar_block:partition_size(), 0, unpacked};
            (StoreID) -> meck:passthrough([StoreID])
        end}
    ], fun test_persistence_roundtrip/0, 60).

test_persistence_roundtrip() ->
    StoreID = test_sync_record_persist,
    ID = test_sync_record_persist_id,
    StateDB = {sync_record, StoreID},
    %% Wipe state left over from a previous run — the test data dir
    %% persists across runs.
    DataDir = arweave_config:get([data_dir]),
    _ = file:del_dir_r(filename:join([DataDir, "storage_modules", StoreID])),
    {ok, Pid} = start_link(name(StoreID), StoreID),
    %% The operations below land in the WAL; kill the server before the next
    %% snapshot so init has to replay them into the ETS tables.
    ok = add(2, 1, ID, StoreID),
    ok = add(4, 3, unpacked, ID, StoreID),
    ok = cut(3, ID, StoreID),
    kill(Pid),
    {ok, Pid2} = start_link(name(StoreID), StoreID),
    ?assertEqual([{2, 1}], ar_intervals:to_list(get(ID, StoreID))),
    ?assertEqual([], ar_intervals:to_list(get(ID, unpacked, StoreID))),
    ?assertEqual(true, is_recorded(2, ID, StoreID)),
    %% Mutate again and wait for the periodic snapshot (it resets the WAL
    %% counter) so the next restart loads the state from the snapshot.
    ok = add(6, 5, unpacked, ID, StoreID),
    ok = delete(2, 1, ID, StoreID),
    ok = ar_test_await:until(sync_record_snapshot_stored, fun() ->
        case ar_kv:get(StateDB, ?WAL_COUNT_KEY) of
            {ok, V} -> binary:decode_unsigned(V) == 0;
            _ -> false
        end
    end),
    kill(Pid2),
    {ok, Pid3} = start_link(name(StoreID), StoreID),
    ?assertEqual([{6, 5}], ar_intervals:to_list(get(ID, StoreID))),
    ?assertEqual([{6, 5}], ar_intervals:to_list(get(ID, unpacked, StoreID))),
    ?assertEqual(false, is_recorded(2, ID, StoreID)),
    kill(Pid3).

kill(Pid) ->
    unlink(Pid),
    MonitorRef = monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', MonitorRef, process, Pid, _} -> ok end.

-endif.
