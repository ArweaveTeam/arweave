-module(ar_sync_record).

-behaviour(gen_server).

-export([start_link/0, set/2, set/3, add/3, add/4, delete/3, cut/2,
		is_recorded/2, is_recorded/3, get_record/2,
		get_serialized_sync_buckets/1,
		get_next_synced_interval/4, get_interval/2]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_data_discovery.hrl").

%% The kv storage key to the sync records.
-define(SYNC_RECORDS_KEY, <<"sync_records">>).

%% The kv key of the write ahead log counter.
-define(WAL_COUNT_KEY, <<"wal">>).

%% The frequency in seconds of updating serialized sync buckets.
-ifdef(DEBUG).
-define(UPDATE_SERIALIZED_SYNC_BUCKETS_FREQUENCY_S, 2).
-else.
-define(UPDATE_SERIALIZED_SYNC_BUCKETS_FREQUENCY_S, 300).
-endif.

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
	%% Compact representations of synced intervals.
	sync_buckets_by_id,
	%% A reference to the on-disk key-value storage holding sync records
	%% and a write ahead log.
	state_db,
	%% The number of entries in the write-ahead log.
	wal
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Add the given set of intervals to the record
%% with the given ID. Store the changes on disk before
%% returning ok.
set(SyncRecord, ID) ->
	case catch gen_server:call(?MODULE, {set, SyncRecord, ID}, infinity) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Add the given set of intervals to the record
%% with the given Type and ID. Store the changes on disk
%% before, returning ok.
set(SyncRecord, Type, ID) ->
	case catch gen_server:call(?MODULE, {set, SyncRecord, Type, ID}, infinity) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Add the given interval to the record with the
%% given ID. Store the changes on disk before returning ok.
add(End, Start, ID) ->
	case catch gen_server:call(?MODULE, {add, End, Start, ID}, 10000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Add the given interval to the record with the
%% given ID and Type. Store the changes on disk before
%% returning ok.
add(End, Start, Type, ID) ->
	case catch gen_server:call(?MODULE, {add, End, Start, Type, ID}, 10000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Remove the given interval from the record
%% with the given ID. Store the changes on disk before
%% returning ok.
delete(End, Start, ID) ->
	case catch gen_server:call(?MODULE, {delete, End, Start, ID}, 10000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Remove everything strictly above the given
%% Offset from the record. Store the changes on disk
%% before returning ok.
cut(Offset, ID) ->
	case catch gen_server:call(?MODULE, {cut, Offset, ID}, 10000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Return true or {true, Type} if a chunk containing
%% the given Offset is found in the record with the given ID,
%% false otherwise. If several types are recorded for the chunk,
%% only one of them is returned, the choice is not defined.
%% The offset is 1-based - if a chunk consists of a single
%% byte that is the first byte of the weave, is_recorded(0, ID)
%% returns false and is_recorded(1, ID) returns true.
is_recorded(Offset, ID) ->
	case ar_ets_intervals:is_inside(ID, Offset) of
		false ->
			false;
		true ->
			case is_recorded2(Offset, ets:first(sync_records), ID) of
				false ->
					true;
				{true, Type} ->
					{true, Type}
			end
	end.

%% @doc Return true if a chunk containing the given Offset and Type
%% is found in the record, false otherwise.
is_recorded(Offset, Type, ID) ->
	case ets:lookup(sync_records, {ID, Type}) of
		[] ->
			false;
		[{_, TID}] ->
			ar_ets_intervals:is_inside(TID, Offset)
	end.

%% @doc Return a set of intervals of synced data ranges.
%%
%% Args is a map with the following keys
%%
%% format			required	etf or json	or raw	serialize in Erlang Term Format or JSON
%% random_subset	optional	any()				pick a random subset if the key is present
%% start			optional	integer()			pick intervals with right bound >= start
%% limit			optional	integer()			the number of intervals to pick
%%
%% ?MAX_SHARED_SYNCED_INTERVALS_COUNT is both the default and the maximum value for limit.
%% If random_subset key is present, a random subset of intervals is picked, the start key is
%% ignored. If random_subset key is not present, the start key must be provided.
%% @end
get_record(Args, ID) ->
	case catch gen_server:call(?MODULE, {get_record, Args, ID}, 10000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Return an ETF-serialized compact but imprecise representation of the synced data -
%% a bucket size and a map where every key is the sequence number of the bucket, every value -
%% the percentage of data synced in the reported bucket.
get_serialized_sync_buckets(ID) ->
	case ets:lookup(?MODULE, {serialized_sync_buckets, ID}) of
		[] ->
			{error, not_initialized};
		[{_, SerializedSyncBuckets}] ->
			{ok, SerializedSyncBuckets}
	end.

%% @doc Return the lowest synced interval with the end offset strictly above the given Offset
%% and with the right bound at most RightBound.
%% Return not_found if there are no such intervals.
get_next_synced_interval(Offset, RightBound, Type, ID) ->
	case ets:lookup(sync_records, {ID, Type}) of
		[] ->
			not_found;
		[{_, TID}] ->
			ar_ets_intervals:get_next_interval(TID, Offset, RightBound)
	end.

%% @doc Return the interval containing the given Offset, including the right bound,
%% excluding the left bound. Return not_found if the given offset does not belong to
%% any interval.
get_interval(Offset, ID) ->
	ar_ets_intervals:get_interval_with_byte(ID, Offset).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, StateDB} = ar_kv:open_without_column_families("ar_sync_record_db", []),
	{SyncRecordByID, SyncRecordByIDType, WAL} = read_sync_records(StateDB),
	SyncBucketsByID = maps:map(
		fun(ID, SyncRecord) ->
			ar_ets_intervals:init_from_gb_set(ID, SyncRecord),
			SyncBuckets = ar_sync_buckets:from_intervals(SyncRecord),
			{SyncBuckets2, SerializedSyncBuckets} = ar_sync_buckets:serialize(SyncBuckets,
					?MAX_SYNC_BUCKETS_SIZE),
			ets:insert(?MODULE, {{serialized_sync_buckets, ID}, SerializedSyncBuckets}),
			SyncBuckets2
		end,
		SyncRecordByID
	),
	initialize_sync_record_by_id_type_ets(SyncRecordByIDType),
	gen_server:cast(?MODULE, {update_sync_buckets, []}),
	gen_server:cast(?MODULE, store_state),
	{ok, #state{
		state_db = StateDB,
		sync_record_by_id = SyncRecordByID,
		sync_record_by_id_type = SyncRecordByIDType,
		sync_buckets_by_id = SyncBucketsByID,
		wal = WAL
	}}.

handle_call({set, SyncRecord, ID}, _From, State) ->
	#state{ sync_record_by_id = SyncRecordByID,
			sync_buckets_by_id = SyncBucketsByID } = State,
	SyncRecord2 = maps:get(ID, SyncRecordByID, ar_intervals:new()),
	SyncRecord3 = ar_intervals:union(SyncRecord2, SyncRecord),
	SyncBuckets = ar_sync_buckets:from_intervals(SyncRecord3),
	{SyncBuckets2, SerializedSyncBuckets} = ar_sync_buckets:serialize(SyncBuckets,
			?MAX_SYNC_BUCKETS_SIZE),
	ets:insert(?MODULE, {{serialized_sync_buckets, ID}, SerializedSyncBuckets}),
	SyncRecordByID2 = maps:put(ID, SyncRecord3, SyncRecordByID),
	SyncBucketsByID2 = maps:put(ID, SyncBuckets2, SyncBucketsByID),
	ar_ets_intervals:init_from_gb_set(ID, SyncRecord3),
	State2 = State#state{ sync_record_by_id = SyncRecordByID2,
			sync_buckets_by_id = SyncBucketsByID2 },
	{Reply, State3} = store_state(State2),
	{reply, Reply, State3};

handle_call({set, SyncRecord, Type, ID}, _From, State) ->
	#state{ sync_record_by_id = SyncRecordByID,
			sync_record_by_id_type = SyncRecordByIDType,
			sync_buckets_by_id = SyncBucketsByID } = State,
	SyncRecord2 = maps:get(ID, SyncRecordByID, ar_intervals:new()),
	SyncRecord3 = ar_intervals:union(SyncRecord2, SyncRecord),
	SyncBuckets = ar_sync_buckets:from_intervals(SyncRecord3),
	{SyncBuckets2, SerializedSyncBuckets} = ar_sync_buckets:serialize(SyncBuckets,
			?MAX_SYNC_BUCKETS_SIZE),
	ets:insert(?MODULE, {{serialized_sync_buckets, ID}, SerializedSyncBuckets}),
	SyncRecordByID2 = maps:put(ID, SyncRecord3, SyncRecordByID),
	SyncBucketsByID2 = maps:put(ID, SyncBuckets2, SyncBucketsByID),
	ar_ets_intervals:init_from_gb_set(ID, SyncRecord3),
	ByType = maps:get({ID, Type}, SyncRecordByIDType, ar_intervals:new()),
	ByType2 = ar_intervals:union(ByType, SyncRecord),
	SyncRecordByIDType2 = maps:put({ID, Type}, ByType2, SyncRecordByIDType),
	TID = get_or_create_type_tid({ID, Type}),
	ar_ets_intervals:init_from_gb_set(TID, ByType2),
	State2 = State#state{ sync_record_by_id = SyncRecordByID2,
			sync_record_by_id_type = SyncRecordByIDType2,
			sync_buckets_by_id = SyncBucketsByID2 },
	{Reply, State3} = store_state(State2),
	{reply, Reply, State3};

handle_call({add, End, Start, ID}, _From, State) ->
	#state{ sync_record_by_id = SyncRecordByID, state_db = StateDB,
			sync_buckets_by_id = SyncBucketsByID } = State,
	SyncRecord = maps:get(ID, SyncRecordByID, ar_intervals:new()),
	SyncBuckets = maps:get(ID, SyncBucketsByID, ar_sync_buckets:new()),
	SyncRecord2 = ar_intervals:add(SyncRecord, End, Start),
	SyncRecordByID2 = maps:put(ID, SyncRecord2, SyncRecordByID),
	SyncBuckets2 = ar_sync_buckets:add(End, Start, SyncBuckets),
	SyncBucketsByID2 = maps:put(ID, SyncBuckets2, SyncBucketsByID),
	ar_ets_intervals:add(ID, End, Start),
	State2 = State#state{ sync_record_by_id = SyncRecordByID2,
			sync_buckets_by_id = SyncBucketsByID2 },
	{Reply, State3} = update_write_ahead_log({add, {End, Start, ID}}, StateDB, State2),
	{reply, Reply, State3};

handle_call({add, End, Start, Type, ID}, _From, State) ->
	#state{ sync_record_by_id = SyncRecordByID, sync_record_by_id_type = SyncRecordByIDType,
			sync_buckets_by_id = SyncBucketsByID, state_db = StateDB } = State,
	SyncRecord = maps:get(ID, SyncRecordByID, ar_intervals:new()),
	SyncRecord2 = ar_intervals:add(SyncRecord, End, Start),
	SyncRecordByID2 = maps:put(ID, SyncRecord2, SyncRecordByID),
	ar_ets_intervals:add(ID, End, Start),
	SyncBuckets = maps:get(ID, SyncBucketsByID, ar_sync_buckets:new()),
	SyncBuckets2 = ar_sync_buckets:add(End, Start, SyncBuckets),
	SyncBucketsByID2 = maps:put(ID, SyncBuckets2, SyncBucketsByID),
	ByType = maps:get({ID, Type}, SyncRecordByIDType, ar_intervals:new()),
	ByType2 = ar_intervals:add(ByType, End, Start),
	SyncRecordByIDType2 = maps:put({ID, Type}, ByType2, SyncRecordByIDType),
	TID = get_or_create_type_tid({ID, Type}),
	ar_ets_intervals:add(TID, End, Start),
	State2 = State#state{ sync_record_by_id = SyncRecordByID2,
			sync_record_by_id_type = SyncRecordByIDType2,
			sync_buckets_by_id = SyncBucketsByID2 },
	{Reply, State3} = update_write_ahead_log({{add, Type}, {End, Start, ID}}, StateDB, State2),
	{reply, Reply, State3};

handle_call({delete, End, Start, ID}, _From, State) ->
	#state{ sync_record_by_id = SyncRecordByID, sync_record_by_id_type = SyncRecordByIDType,
			sync_buckets_by_id = SyncBucketsByID, state_db = StateDB } = State,
	SyncRecord = maps:get(ID, SyncRecordByID, ar_intervals:new()),
	SyncRecord2 = ar_intervals:delete(SyncRecord, End, Start),
	SyncRecordByID2 = maps:put(ID, SyncRecord2, SyncRecordByID),
	ar_ets_intervals:delete(ID, End, Start),
	SyncBuckets = maps:get(ID, SyncBucketsByID, ar_sync_buckets:new()),
	SyncBuckets2 = ar_sync_buckets:delete(End, Start, SyncBuckets),
	SyncBucketsByID2 = maps:put(ID, SyncBuckets2, SyncBucketsByID),
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
			({{ID2, _}, TID}, _) when ID2 == ID ->
				ar_ets_intervals:delete(TID, End, Start);
			(_, _) ->
				ok
		end,
		ok,
		sync_records
	),
	State2 = State#state{ sync_record_by_id = SyncRecordByID2,
			sync_record_by_id_type = SyncRecordByIDType2,
			sync_buckets_by_id = SyncBucketsByID2 },
	{Reply, State3} = update_write_ahead_log({delete, {End, Start, ID}}, StateDB, State2),
	{reply, Reply, State3};

handle_call({cut, Offset, ID}, _From, State) ->
	#state{ sync_record_by_id = SyncRecordByID, sync_record_by_id_type = SyncRecordByIDType,
			sync_buckets_by_id = SyncBucketsByID, state_db = StateDB } = State,
	SyncRecord = maps:get(ID, SyncRecordByID, ar_intervals:new()),
	SyncRecord2 = ar_intervals:cut(SyncRecord, Offset),
	SyncRecordByID2 = maps:put(ID, SyncRecord2, SyncRecordByID),
	ar_ets_intervals:cut(ID, Offset),
	SyncBuckets = maps:get(ID, SyncBucketsByID, ar_sync_buckets:new()),
	SyncBuckets2 = ar_sync_buckets:cut(Offset, SyncBuckets),
	SyncBucketsByID2 = maps:put(ID, SyncBuckets2, SyncBucketsByID),
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
			({{ID2, _}, TID}, _) when ID2 == ID ->
				ar_ets_intervals:cut(TID, Offset);
			(_, _) ->
				ok
		end,
		ok,
		sync_records
	),
	State2 = State#state{ sync_record_by_id = SyncRecordByID2,
			sync_record_by_id_type = SyncRecordByIDType2,
			sync_buckets_by_id = SyncBucketsByID2 },
	{Reply, State3} = update_write_ahead_log({cut, {Offset, ID}}, StateDB, State2),
	{reply, Reply, State3};

handle_call({get_record, #{ format := raw } = Args, ID}, _From, State) ->
	case map_size(Args) /= 1 of
		true ->
			throw("extra arguments are not supported for format=raw");
		false ->
			ok
	end,
	#state{ sync_record_by_id = SyncRecordByID } = State,
	SyncRecord = maps:get(ID, SyncRecordByID, ar_intervals:new()),
	{reply, {ok, SyncRecord}, State};

handle_call({get_record, Args, ID}, _From, State) ->
	#state{ sync_record_by_id = SyncRecordByID } = State,
	Limit =
		min(
			maps:get(limit, Args, ?MAX_SHARED_SYNCED_INTERVALS_COUNT),
			?MAX_SHARED_SYNCED_INTERVALS_COUNT
		),
	SyncRecord = maps:get(ID, SyncRecordByID, ar_intervals:new()),
	{reply, {ok, ar_intervals:serialize(Args#{ limit => Limit }, SyncRecord)}, State};

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(update_sync_buckets, State) ->
	#state{ sync_buckets_by_id = SyncBucketsByID } = State,
	Keys = maps:keys(SyncBucketsByID),
	gen_server:cast(?MODULE, {update_sync_buckets, Keys}),
	{noreply, State};

handle_cast({update_sync_buckets, []}, State) ->
	ar_util:cast_after(?UPDATE_SERIALIZED_SYNC_BUCKETS_FREQUENCY_S, ?MODULE,
			update_sync_buckets),
	{noreply, State};
handle_cast({update_sync_buckets, [ID | Keys]}, State) ->
	#state{ sync_buckets_by_id = SyncBucketsByID } = State,
	SyncBuckets = maps:get(ID, SyncBucketsByID, ar_sync_buckets:new()),
	{SyncBuckets2, SerializedSyncBuckets} = ar_sync_buckets:serialize(SyncBuckets,
			?MAX_SYNC_BUCKETS_SIZE),
	ets:insert(?MODULE, [{{serialized_sync_buckets, ID}, SerializedSyncBuckets}]),
	gen_server:cast(?MODULE, {update_sync_buckets, Keys}),
	{noreply, State#state{ sync_buckets_by_id = maps:put(ID, SyncBuckets2, SyncBucketsByID) }};

handle_cast(store_state, State) ->
	{_, State2} = store_state(State),
	timer:apply_after(
		?STORE_SYNC_RECORD_FREQUENCY_MS, gen_server, cast, [?MODULE, store_state]),
	{noreply, State2};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(Reason, #state{ state_db = StateDB } = State) ->
	?LOG_INFO([{event, terminate}, {reason, io_lib:format("~p", [Reason])}]),
	store_state(State),
	ar_kv:close(StateDB).

%%%===================================================================
%%% Private functions.
%%%===================================================================

is_recorded2(_Offset, '$end_of_table', _ID) ->
	false;
is_recorded2(Offset, {ID, Type}, ID) ->
	case ets:lookup(sync_records, {ID, Type}) of
		[{_, TID}] ->
			case ar_ets_intervals:is_inside(TID, Offset) of
				true ->
					{true, Type};
				false ->
					is_recorded2(Offset, ets:next(sync_records, {ID, Type}), ID)
			end;
		[] ->
			%% Very unlucky timing.
			false
	end;
is_recorded2(Offset, {ID2, Type}, ID) ->
	is_recorded2(Offset, ets:next(sync_records, {ID2, Type}), ID).

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

initialize_sync_record_by_id_type_ets(SyncRecordByIDType) ->
	Iterator = maps:iterator(SyncRecordByIDType),
	initialize_sync_record_by_id_type_ets2(maps:next(Iterator)).

initialize_sync_record_by_id_type_ets2(none) ->
	ok;
initialize_sync_record_by_id_type_ets2({{ID, Type}, SyncRecord, Iterator}) ->
	TID = ets:new(sync_record_type, [ordered_set, public, {read_concurrency, true}]),
	ar_ets_intervals:init_from_gb_set(TID, SyncRecord),
	ets:insert(sync_records, {{ID, Type}, TID}),
	initialize_sync_record_by_id_type_ets2(maps:next(Iterator)).

store_state(State) ->
	#state{
		state_db = StateDB,
		sync_record_by_id = SyncRecordByID,
		sync_record_by_id_type = SyncRecordByIDType
	} = State,
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
			SyncRecord = maps:get(ar_data_sync, SyncRecordByID, ar_intervals:new()),
			prometheus_gauge:set(v2_index_data_size, ar_intervals:sum(SyncRecord)),
			maps:map(
				fun	({ar_data_sync, Type}, TypeRecord) ->
						prometheus_gauge:set(v2_index_data_size_by_packing, [Type],
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
			Error;
		ok ->
			case ar_kv:put(StateDB, ?WAL_COUNT_KEY, binary:encode_unsigned(WAL + 1)) of
				ok ->
					{ok, State#state{ wal = WAL + 1 }};
				Error2 ->
					{Error2, State}
			end
	end.
