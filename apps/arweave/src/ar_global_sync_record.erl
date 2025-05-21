-module(ar_global_sync_record).

-behaviour(gen_server).

-include("ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include("ar_data_discovery.hrl").
-include("ar_sync_buckets.hrl").

-export([start_link/0, get_serialized_sync_record/1, get_serialized_sync_buckets/0,
		get_serialized_footprint_buckets/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

%% The frequency in seconds of updating serialized sync buckets.
-ifdef(AR_TEST).
-define(UPDATE_SERIALIZED_SYNC_BUCKETS_FREQUENCY_S, 2).
-else.
-define(UPDATE_SERIALIZED_SYNC_BUCKETS_FREQUENCY_S, 300).
-endif.

-record(state, {
	sync_record,
	sync_buckets,
	footprint_record,
	footprint_buckets
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Return a set of data intervals from all configured storage modules.
%%
%% Args is a map with the following keys
%%
%% format			required	etf or json		serialize in Erlang Term Format or JSON
%% random_subset	optional	any()			pick a random subset if the key is present
%% start			optional	integer()		pick intervals with right bound >= start
%% right_bound		optional	integer()		pick intervals with right bound <= right_bound
%% limit			optional	integer()		the number of intervals to pick
%%
%% ?MAX_SHARED_SYNCED_INTERVALS_COUNT is both the default and the maximum value for limit.
%% If random_subset key is present, a random subset of intervals is picked, the start key is
%% ignored. If random_subset key is not present, the start key must be provided.
get_serialized_sync_record(Args) ->
	case catch gen_server:call(?MODULE, {get_serialized_sync_record, Args}, 10000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Return an ETF-serialized compact but imprecise representation of the synced data -
%% a bucket size and a map where every key is the sequence number of the bucket, every value -
%% the percentage of data synced in the reported bucket.
get_serialized_sync_buckets() ->
	case ets:lookup(?MODULE, serialized_sync_buckets) of
		[] ->
			{error, not_initialized};
		[{_, SerializedSyncBuckets}] ->
			{ok, SerializedSyncBuckets}
	end.

%% @doc Return an ETF-serialized compact but imprecise representation of the synced footprints -
%% a bucket size and a map where every key is the sequence number of the bucket, every value -
%% the percentage of data synced in the reported bucket. Every bucket contains one or more
%% footprints. Note that while footprints in the buckets are adjacent in the sense that
%% the first chunk in the first footprint is adjacent to the first chunk in the second footprint,
%% chunks are generally not adjacent because of how the logic of 2.9 footprints goes.
get_serialized_footprint_buckets() ->
	case ets:lookup(?MODULE, serialized_footprint_buckets) of
		[] ->
			{error, not_initialized};
		[{_, SerializedFootprintBuckets}] ->
			{ok, SerializedFootprintBuckets}
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	ok = ar_events:subscribe(sync_record),
	SyncRecord = get_sync_record(),
	SyncBuckets = cache_and_get_sync_buckets(SyncRecord, serialized_sync_buckets,
			ar_sync_buckets:new()),
	FootprintRecord = get_footprint_record(),
	FootprintBuckets = cache_and_get_sync_buckets(FootprintRecord,
			serialized_footprint_buckets,
			ar_sync_buckets:new(?NETWORK_FOOTPRINT_BUCKET_SIZE)),
	{ok, #state{ sync_record = SyncRecord, sync_buckets = SyncBuckets,
			footprint_record = FootprintRecord, footprint_buckets = FootprintBuckets }}.

handle_call({get_serialized_sync_record, Args}, _From, State) ->
	#state{ sync_record = SyncRecord } = State,
	Limit = min(maps:get(limit, Args, ?MAX_SHARED_SYNCED_INTERVALS_COUNT),
			?MAX_SHARED_SYNCED_INTERVALS_COUNT),
	{reply, {ok, ar_intervals:serialize(Args#{ limit => Limit }, SyncRecord)}, State};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({update_serialized_sync_buckets, serialized_sync_buckets = Key}, State) ->
	#state{ sync_buckets = SyncBuckets } = State,
	{SyncBuckets2, SerializedSyncBuckets} = ar_sync_buckets:serialize(SyncBuckets,
			?MAX_SYNC_BUCKETS_SIZE),
	ets:insert(?MODULE, {Key, SerializedSyncBuckets}),
	ar_util:cast_after(?UPDATE_SERIALIZED_SYNC_BUCKETS_FREQUENCY_S * 1000,
			?MODULE, {update_serialized_sync_buckets, Key}),
	{noreply, State#state{ sync_buckets = SyncBuckets2 }};
handle_cast({update_serialized_sync_buckets, serialized_footprint_buckets = Key}, State) ->
	#state{ footprint_buckets = FootprintBuckets } = State,
	{FootprintBuckets2, SerializedFootprintBuckets} = ar_sync_buckets:serialize(
			FootprintBuckets, ?MAX_SYNC_BUCKETS_SIZE),
	ets:insert(?MODULE, {Key, SerializedFootprintBuckets}),
	ar_util:cast_after(?UPDATE_SERIALIZED_SYNC_BUCKETS_FREQUENCY_S * 1000,
			?MODULE, {update_serialized_sync_buckets, Key}),
	{noreply, State#state{ footprint_buckets = FootprintBuckets2 }};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({event, sync_record, {add_range, Start, End, ar_data_sync, Module}}, State) ->
	#state{ sync_record = SyncRecord, sync_buckets = SyncBuckets } = State,
	Packing = ar_storage_module:get_packing(Module),
	case Packing of
		{replica_2_9, _} ->
			%% Replica 2.9 data is recorded in the footprint record. It is synced
			%% footprint by footprint (not left to right).
			{noreply, State};
		_ ->
			SyncRecord2 = ar_intervals:add(SyncRecord, End, Start),
			SyncBuckets2 = ar_sync_buckets:add(End, Start, SyncBuckets),
			{noreply, State#state{ sync_record = SyncRecord2, sync_buckets = SyncBuckets2 }}
	end;

handle_info({event, sync_record, {add_range, Start, End, ar_data_sync_footprints, _Module}}, State) ->
	State2 = update_footprint_data(Start, End, State),
	{noreply, State2};

handle_info({event, sync_record, {global_cut, Offset}}, State) ->
	#state{ sync_record = SyncRecord, sync_buckets = SyncBuckets } = State,
	SyncRecord2 = ar_intervals:cut(SyncRecord, Offset),
	SyncBuckets2 = ar_sync_buckets:cut(Offset, SyncBuckets),
	{noreply, State#state{ sync_record = SyncRecord2, sync_buckets = SyncBuckets2 }};

handle_info({event, sync_record, {global_remove_range, Start, End}}, State) ->
	#state{ sync_record = SyncRecord, sync_buckets = SyncBuckets } = State,
	SyncRecord2 = ar_intervals:delete(SyncRecord, End, Start),
	SyncBuckets2 = ar_sync_buckets:delete(End, Start, SyncBuckets),
	State2 = remove_footprint_data(Start, End, State),
	{noreply, State2#state{ sync_record = SyncRecord2, sync_buckets = SyncBuckets2 }};

handle_info({event, sync_record, _}, State) ->
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{event, terminate}, {module, ?MODULE}, {reason, io_lib:format("~p", [Reason])}]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_sync_record() ->
	{ok, Config} = arweave_config:get_env(),
	lists:foldl(
		fun(Module, Acc) ->
			case Module of
				{_, _, {replica_2_9, _}} ->
					%% Replica 2.9 data is recorded in the footprint record. It is synced
					%% footprint by footprint (not left to right).
					Acc;
				_ ->
					StoreID = ar_storage_module:id(Module),
					ar_intervals:union(ar_sync_record:get(ar_data_sync, StoreID), Acc)
			end
		end,
		ar_intervals:new(),
		[?DEFAULT_MODULE | Config#config.storage_modules]
	).

get_footprint_record() ->
	{ok, Config} = arweave_config:get_env(),
	lists:foldl(
		fun(Module, Acc) ->
			StoreID = ar_storage_module:id(Module),
			ar_intervals:union(ar_sync_record:get(ar_data_sync_footprints, StoreID), Acc)
		end,
		ar_intervals:new(),
		Config#config.storage_modules
	).

cache_and_get_sync_buckets(SyncRecord, Key, SyncBuckets) ->
	SyncBuckets2 = ar_sync_buckets:from_intervals(SyncRecord, SyncBuckets),
	{SyncBuckets3, SerializedSyncBuckets} = ar_sync_buckets:serialize(SyncBuckets2,
					?MAX_SYNC_BUCKETS_SIZE),
	ets:insert(?MODULE, {Key, SerializedSyncBuckets}),
	ar_util:cast_after(?UPDATE_SERIALIZED_SYNC_BUCKETS_FREQUENCY_S * 1000,
			?MODULE, {update_serialized_sync_buckets, Key}),
	SyncBuckets3.

update_footprint_data(Start, End, State) when Start >= End ->
	State;
update_footprint_data(Start, End, State) ->
	#state{ footprint_record = FootprintRecord,
			footprint_buckets = FootprintBuckets } = State,
	FootprintRecord2 = ar_intervals:add(FootprintRecord, Start + 1, Start),
	FootprintBuckets2 = ar_sync_buckets:add(Start + 1, Start, FootprintBuckets),
	State2 = State#state{ footprint_record = FootprintRecord2,
		footprint_buckets = FootprintBuckets2 },
	update_footprint_data(Start + 1, End, State2).

remove_footprint_data(Start, End, State) when Start >= End ->
	State;
remove_footprint_data(Start, End, State) ->
	#state{ footprint_record = FootprintRecord,
			footprint_buckets = FootprintBuckets } = State,
	Offset = ar_footprint_record:get_offset(Start + ?DATA_CHUNK_SIZE),
	FootprintRecord2 = ar_intervals:delete(FootprintRecord, Offset, Offset - 1),
	FootprintBuckets2 = ar_sync_buckets:delete(Offset, Offset - 1, FootprintBuckets),
	State2 = State#state{ footprint_record = FootprintRecord2,
		footprint_buckets = FootprintBuckets2 },
	remove_footprint_data(Start + ?DATA_CHUNK_SIZE, End, State2).