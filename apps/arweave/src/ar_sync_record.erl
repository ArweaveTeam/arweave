-module(ar_sync_record).

-behaviour(gen_server).

-export([start_link/2, get/2, get/3, add/4, add/5, delete/4, cut/3, is_recorded/2,
		is_recorded/3, is_recorded/4, get_next_synced_interval/4, get_next_synced_interval/5,
		get_interval/3, get_intersection_size/4]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_data_discovery.hrl").

%% The kv storage key to the sync records.
-define(SYNC_RECORDS_KEY, <<"sync_records">>).

%% The kv key of the write ahead log counter.
-define(WAL_COUNT_KEY, <<"wal">>).

%% The frequency of dumping sync records on disk.
-ifdef(DEBUG).
-define(STORE_SYNC_RECORD_FREQUENCY_MS, 1000).
-else.
-define(STORE_SYNC_RECORD_FREQUENCY_MS, 60 * 1000).
-endif.

-record(state, {
	%% A map ID => Intervals
	%% where Intervals is a set of non-overlapping intervals
	%% of global byte offsets {End, Start} denoting some synced
	%% data. End offsets are defined on [1, WeaveSize], start
	%% offsets are defined on [0, WeaveSize).
	%%
	%% Each set serves as a compact map of what is synced by the node.
	%% No matter how big the weave is or how much of it the node stores,
	%% this record can remain very small compared to the space taken by
	%% chunk identifiers, whose number grows unlimited with time.
	sync_record_by_id,
	%% A map {ID, Type} => Intervals.
	sync_record_by_id_type,
	%% The name of the WAL store.
	state_db,
	%% The identifier of the storage module.
	store_id,
	%% The partition covered by the storage module.
	partition_number,
	%% The size in bytes of the storage module; undefined for the "default" storage.
	storage_module_size,
	%% The index of the storage module; undefined for the "default" storage.
	storage_module_index,
	%% The number of entries in the write-ahead log.
	wal
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, StoreID) ->
	gen_server:start_link({local, Name}, ?MODULE, StoreID, []).

%% @doc Return the set of intervals.
get(ID, StoreID) ->
	GenServerID = list_to_atom("ar_sync_record_" ++ StoreID),
	case catch gen_server:call(GenServerID, {get, ID}, 20000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Return the set of intervals.
get(ID, Type, StoreID) ->
	GenServerID = list_to_atom("ar_sync_record_" ++ StoreID),
	case catch gen_server:call(GenServerID, {get, Type, ID}, 20000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Add the given interval to the record with the
%% given ID. Store the changes on disk before returning ok.
add(End, Start, ID, StoreID) ->
	GenServerID = list_to_atom("ar_sync_record_" ++ StoreID),
	case catch gen_server:call(GenServerID, {add, End, Start, ID}, 120000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Add the given interval to the record with the
%% given ID and Type. Store the changes on disk before
%% returning ok.
add(End, Start, Type, ID, StoreID) ->
	GenServerID = list_to_atom("ar_sync_record_" ++ StoreID),
	case catch gen_server:call(GenServerID, {add, End, Start, Type, ID}, 120000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Remove the given interval from the record
%% with the given ID. Store the changes on disk before
%% returning ok.
delete(End, Start, ID, StoreID) ->
	GenServerID = list_to_atom("ar_sync_record_" ++ StoreID),
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
	GenServerID = list_to_atom("ar_sync_record_" ++ StoreID),
	case catch gen_server:call(GenServerID, {cut, Offset, ID}, 120000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Return {true, StoreID} or {{true, Type}, StoreID} if a chunk containing
%% the given Offset is found in the record with the given ID, false otherwise.
%% If several types are recorded for the chunk, only one of them is returned,
%% the choice is not defined. If the chunk is stored in the default storage module,
%% return the type found there. If not, search for a configured storage
%% module covering the given Offset. If there are multiple
%% storage modules with the chunk, the choice is not defined.
%% The offset is 1-based - if a chunk consists of a single
%% byte that is the first byte of the weave, is_recorded(0, ID)
%% returns false and is_recorded(1, ID) returns true.
is_recorded(Offset, {ID, Type}) ->
	case is_recorded(Offset, Type, ID, "default") of
		true ->
			{{true, Type}, "default"};
		false ->
			StorageModules = [Module
					|| {_, _, Packing} = Module <- ar_storage_module:get_all(Offset),
					Packing == Type],
			is_recorded_any_by_type(Offset, ID, StorageModules)
	end;
is_recorded(Offset, ID) ->
	case is_recorded(Offset, ID, "default") of
		false ->
			StorageModules = ar_storage_module:get_all(Offset),
			is_recorded_any(Offset, ID, StorageModules);
		Reply ->
			{Reply, "default"}
	end.

%% @doc Return true or {true, Type} if a chunk containing
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
						{true, Type} ->
							{true, Type}
					end
			end
	end.

%% @doc Return true if a chunk containing the given Offset and Type
%% is found in the record in the storage module identified by StoreID,
%% false otherwise.
is_recorded(Offset, Type, ID, StoreID) ->
	case ets:lookup(sync_records, {ID, Type, StoreID}) of
		[] ->
			false;
		[{_, TID}] ->
			ar_ets_intervals:is_inside(TID, Offset)
	end.

%% @doc Return the lowest synced interval with the end offset strictly above the given Offset
%% and with the right bound at most RightBound.
%% Return not_found if there are no such intervals.
get_next_synced_interval(Offset, RightBound, ID, StoreID) ->
	case ets:lookup(sync_records, {ID, StoreID}) of
		[] ->
			not_found;
		[{_, TID}] ->
			ar_ets_intervals:get_next_interval(TID, Offset, RightBound)
	end.

%% @doc Return the lowest synced interval with the end offset strictly above the given Offset
%% and with the right bound at most RightBound.
%% Return not_found if there are no such intervals.
get_next_synced_interval(Offset, RightBound, Type, ID, StoreID) ->
	case ets:lookup(sync_records, {ID, Type, StoreID}) of
		[] ->
			not_found;
		[{_, TID}] ->
			ar_ets_intervals:get_next_interval(TID, Offset, RightBound)
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
	process_flag(trap_exit, true),
	{Dir, StorageModuleSize, StorageModuleIndex, PartitionNumber} =
		case StoreID of
			"default" ->
				{filename:join(?ROCKS_DB_DIR, "ar_sync_record_db"),
					undefined, undefined, undefined};
			_ ->
				{Size, Index, _Packing} = ar_storage_module:get_by_id(StoreID),
				{filename:join(["storage_modules", StoreID, ?ROCKS_DB_DIR,
						"ar_sync_record_db"]), Size, Index, ?PARTITION_NUMBER(Size * Index)}
		end,
	StateDB = {sync_record, StoreID},
	ok = ar_kv:open(Dir, StateDB),
	{SyncRecordByID, SyncRecordByIDType, WAL} = read_sync_records(StateDB),
	initialize_sync_record_by_id_type_ets(SyncRecordByIDType, StoreID),
	initialize_sync_record_by_id_ets(SyncRecordByID, StoreID),
	gen_server:cast(self(), store_state),
	{ok, #state{
		state_db = StateDB,
		store_id = StoreID,
		partition_number = PartitionNumber,
		storage_module_size = StorageModuleSize,
		storage_module_index = StorageModuleIndex,
		sync_record_by_id = SyncRecordByID,
		sync_record_by_id_type = SyncRecordByIDType,
		wal = WAL
	}}.

handle_call({get, ID}, _From, State) ->
	#state{ sync_record_by_id = SyncRecordByID } = State,
	{reply, maps:get(ID, SyncRecordByID, ar_intervals:new()), State};

handle_call({get, Type, ID}, _From, State) ->
	#state{ sync_record_by_id_type = SyncRecordByIDType } = State,
	{reply, maps:get({ID, Type}, SyncRecordByIDType, ar_intervals:new()), State};

handle_call({add, End, Start, ID}, _From, State) ->
	#state{ sync_record_by_id = SyncRecordByID, state_db = StateDB,
			store_id = StoreID } = State,
	SyncRecord = maps:get(ID, SyncRecordByID, ar_intervals:new()),
	SyncRecord2 = ar_intervals:add(SyncRecord, End, Start),
	SyncRecordByID2 = maps:put(ID, SyncRecord2, SyncRecordByID),
	TID = get_or_create_type_tid({ID, StoreID}),
	ar_ets_intervals:add(TID, End, Start),
	State2 = State#state{ sync_record_by_id = SyncRecordByID2 },
	{Reply, State3} = update_write_ahead_log({add, {End, Start, ID}}, StateDB, State2),
	{reply, Reply, State3};

handle_call({add, End, Start, Type, ID}, _From, State) ->
	#state{ sync_record_by_id = SyncRecordByID, sync_record_by_id_type = SyncRecordByIDType,
			state_db = StateDB, store_id = StoreID } = State,
	ByType = maps:get({ID, Type}, SyncRecordByIDType, ar_intervals:new()),
	ByType2 = ar_intervals:add(ByType, End, Start),
	SyncRecordByIDType2 = maps:put({ID, Type}, ByType2, SyncRecordByIDType),
	TypeTID = get_or_create_type_tid({ID, Type, StoreID}),
	ar_ets_intervals:add(TypeTID, End, Start),
	SyncRecord = maps:get(ID, SyncRecordByID, ar_intervals:new()),
	SyncRecord2 = ar_intervals:add(SyncRecord, End, Start),
	SyncRecordByID2 = maps:put(ID, SyncRecord2, SyncRecordByID),
	TID = get_or_create_type_tid({ID, StoreID}),
	ar_ets_intervals:add(TID, End, Start),
	State2 = State#state{ sync_record_by_id = SyncRecordByID2,
			sync_record_by_id_type = SyncRecordByIDType2 },
	{Reply, State3} = update_write_ahead_log({{add, Type}, {End, Start, ID}}, StateDB, State2),
	{reply, Reply, State3};

handle_call({delete, End, Start, ID}, _From, State) ->
	#state{ sync_record_by_id = SyncRecordByID, sync_record_by_id_type = SyncRecordByIDType,
			state_db = StateDB, store_id = StoreID } = State,
	SyncRecord = maps:get(ID, SyncRecordByID, ar_intervals:new()),
	SyncRecord2 = ar_intervals:delete(SyncRecord, End, Start),
	SyncRecordByID2 = maps:put(ID, SyncRecord2, SyncRecordByID),
	TID = get_or_create_type_tid({ID, StoreID}),
	ar_ets_intervals:delete(TID, End, Start),
	SyncRecordByIDType2 =
		maps:map(
			fun
				({ID2, _}, ByType) when ID2 == ID ->
					ar_intervals:delete(ByType, End, Start);
				(_, ByType) ->
					ByType
			end,
			SyncRecordByIDType
		),
	ets:foldl(
		fun
			({{ID2, _, SID}, TypeTID}, _) when ID2 == ID, SID == StoreID ->
				ar_ets_intervals:delete(TypeTID, End, Start);
			(_, _) ->
				ok
		end,
		ok,
		sync_records
	),
	State2 = State#state{ sync_record_by_id = SyncRecordByID2,
			sync_record_by_id_type = SyncRecordByIDType2 },
	{Reply, State3} = update_write_ahead_log({delete, {End, Start, ID}}, StateDB, State2),
	{reply, Reply, State3};

handle_call({cut, Offset, ID}, _From, State) ->
	#state{ sync_record_by_id = SyncRecordByID, sync_record_by_id_type = SyncRecordByIDType,
			state_db = StateDB, store_id = StoreID } = State,
	SyncRecord = maps:get(ID, SyncRecordByID, ar_intervals:new()),
	SyncRecord2 = ar_intervals:cut(SyncRecord, Offset),
	SyncRecordByID2 = maps:put(ID, SyncRecord2, SyncRecordByID),
	TID = get_or_create_type_tid({ID, StoreID}),
	ar_ets_intervals:cut(TID, Offset),
	SyncRecordByIDType2 =
		maps:map(
			fun
				({ID2, _}, ByType) when ID2 == ID ->
					ar_intervals:cut(ByType, Offset);
				(_, ByType) ->
					ByType
			end,
			SyncRecordByIDType
		),
	ets:foldl(
		fun
			({{ID2, _, SID}, TypeTID}, _) when ID2 == ID, SID == StoreID ->
				ar_ets_intervals:cut(TypeTID, Offset);
			(_, _) ->
				ok
		end,
		ok,
		sync_records
	),
	State2 = State#state{ sync_record_by_id = SyncRecordByID2,
			sync_record_by_id_type = SyncRecordByIDType2 },
	{Reply, State3} = update_write_ahead_log({cut, {Offset, ID}}, StateDB, State2),
	{reply, Reply, State3};

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(store_state, State) ->
	{_, State2} = store_state(State),
	timer:apply_after(
		?STORE_SYNC_RECORD_FREQUENCY_MS, gen_server, cast, [self(), store_state]),
	{noreply, State2};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(Reason, State) ->
	?LOG_INFO([{event, terminate}, {reason, io_lib:format("~p", [Reason])}]),
	store_state(State).

%%%===================================================================
%%% Private functions.
%%%===================================================================


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
is_recorded2(Offset, {ID, Type, StoreID}, ID, StoreID) ->
	case ets:lookup(sync_records, {ID, Type, StoreID}) of
		[{_, TID}] ->
			case ar_ets_intervals:is_inside(TID, Offset) of
				true ->
					{true, Type};
				false ->
					is_recorded2(Offset, ets:next(sync_records, {ID, Type, StoreID}), ID,
							StoreID)
			end;
		[] ->
			%% Very unlucky timing.
			false
	end;
is_recorded2(Offset, Key, ID, StoreID) ->
	is_recorded2(Offset, ets:next(sync_records, Key), ID, StoreID).

read_sync_records(StateDB) ->
	{SyncRecordByID, SyncRecordByIDType} =
		case ar_kv:get(StateDB, ?SYNC_RECORDS_KEY) of
			not_found ->
				{#{}, #{}};
			{ok, V} ->
				binary_to_term(V)
		end,
	{SyncRecordByID2, SyncRecordByIDType2, WAL} =
		replay_write_ahead_log(SyncRecordByID, SyncRecordByIDType, StateDB),
	{SyncRecordByID2, SyncRecordByIDType2, WAL}.

replay_write_ahead_log(SyncRecordByID, SyncRecordByIDType, StateDB) ->
	WAL =
		case ar_kv:get(StateDB, ?WAL_COUNT_KEY) of
			not_found ->
				0;
			{ok, V} ->
				binary:decode_unsigned(V)
		end,
	replay_write_ahead_log(SyncRecordByID, SyncRecordByIDType, WAL, StateDB).

replay_write_ahead_log(SyncRecordByID, SyncRecordByIDType, WAL, StateDB) ->
	replay_write_ahead_log(SyncRecordByID, SyncRecordByIDType, 1, WAL, StateDB).

replay_write_ahead_log(SyncRecordByID, SyncRecordByIDType, N, WAL, _StateDB) when N > WAL ->
	{SyncRecordByID, SyncRecordByIDType, WAL};
replay_write_ahead_log(SyncRecordByID, SyncRecordByIDType, N, WAL, StateDB) ->
	case ar_kv:get(StateDB, binary:encode_unsigned(N)) of
		not_found ->
			%% The VM crashed after recording the number.
			{SyncRecordByID, SyncRecordByIDType, WAL};
		{ok, V} ->
			{Op, Params} = binary_to_term(V),
			case Op of
				add ->
					{End, Start, ID} = Params,
					SyncRecord = maps:get(ID, SyncRecordByID, ar_intervals:new()),
					SyncRecord2 = ar_intervals:add(SyncRecord, End, Start),
					SyncRecordByID2 = maps:put(ID, SyncRecord2, SyncRecordByID),
					replay_write_ahead_log(
						SyncRecordByID2, SyncRecordByIDType, N + 1, WAL, StateDB);
				{add, Type} ->
					{End, Start, ID} = Params,
					SyncRecord = maps:get(ID, SyncRecordByID, ar_intervals:new()),
					SyncRecord2 = ar_intervals:add(SyncRecord, End, Start),
					SyncRecordByID2 = maps:put(ID, SyncRecord2, SyncRecordByID),
					ByType = maps:get({ID, Type}, SyncRecordByIDType, ar_intervals:new()),
					ByType2 = ar_intervals:add(ByType, End, Start),
					SyncRecordByIDType2 = maps:put({ID, Type}, ByType2, SyncRecordByIDType),
					replay_write_ahead_log(
						SyncRecordByID2, SyncRecordByIDType2, N + 1, WAL, StateDB);
				delete ->
					{End, Start, ID} = Params,
					SyncRecord = maps:get(ID, SyncRecordByID, ar_intervals:new()),
					SyncRecord2 = ar_intervals:delete(SyncRecord, End, Start),
					SyncRecordByID2 = maps:put(ID, SyncRecord2, SyncRecordByID),
					SyncRecordByIDType2 =
						maps:map(
							fun
								({ID2, _}, ByType) when ID2 == ID ->
									ar_intervals:delete(ByType, End, Start);
								(_, ByType) ->
									ByType
							end,
							SyncRecordByIDType
						),
					replay_write_ahead_log(
						SyncRecordByID2, SyncRecordByIDType2, N + 1, WAL, StateDB);
				cut ->
					{Offset, ID} = Params,
					SyncRecord = maps:get(ID, SyncRecordByID, ar_intervals:new()),
					SyncRecord2 = ar_intervals:cut(SyncRecord, Offset),
					SyncRecordByID2 = maps:put(ID, SyncRecord2, SyncRecordByID),
					SyncRecordByIDType2 =
						maps:map(
							fun
								({ID2, _}, ByType) when ID2 == ID ->
									ar_intervals:cut(ByType, Offset);
								(_, ByType) ->
									ByType
							end,
							SyncRecordByIDType
						),
					replay_write_ahead_log(
						SyncRecordByID2, SyncRecordByIDType2, N + 1, WAL, StateDB)
			end
	end.

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
initialize_sync_record_by_id_type_ets2({{ID, Type}, SyncRecord, Iterator}, StoreID) ->
	TID = ets:new(sync_record_type, [ordered_set, public, {read_concurrency, true}]),
	ar_ets_intervals:init_from_gb_set(TID, SyncRecord),
	ets:insert(sync_records, {{ID, Type, StoreID}, TID}),
	initialize_sync_record_by_id_type_ets2(maps:next(Iterator), StoreID).

store_state(State) ->
	#state{ state_db = StateDB, sync_record_by_id = SyncRecordByID,
			sync_record_by_id_type = SyncRecordByIDType, store_id = StoreID,
			partition_number = PartitionNumber,
			storage_module_size = StorageModuleSize,
			storage_module_index = StorageModuleIndex } = State,
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
				fun	({ar_data_sync, Type}, TypeRecord) ->
						Type2 =
							case Type of
								{spora_2_6, _Addr} ->
									spora_2_6;
								_ ->
									Type
							end,
						ar_mining_stats:set_storage_module_data_size(
							StoreID, Type2, PartitionNumber, StorageModuleSize, StorageModuleIndex,
							ar_intervals:sum(TypeRecord));
					(_, _) ->
						ok
				end,
				SyncRecordByIDType
			),
			{ok, State#state{ wal = 0 }}
	end.

get_or_create_type_tid(IDType) ->
	case ets:lookup(sync_records, IDType) of
		[] ->
			TID = ets:new(sync_record_type, [ordered_set, public, {read_concurrency, true}]),
			ets:insert(sync_records, {IDType, TID}),
			TID;
		[{_, TID2}] ->
			TID2
	end.

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
