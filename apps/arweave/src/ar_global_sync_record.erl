-module(ar_global_sync_record).

-behaviour(gen_server).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_discovery.hrl").

-export([start_link/0, get_serialized_sync_record/1, get_serialized_sync_buckets/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

%% The frequency in seconds of updating serialized sync buckets.
-ifdef(DEBUG).
-define(UPDATE_SERIALIZED_SYNC_BUCKETS_FREQUENCY_S, 2).
-else.
-define(UPDATE_SERIALIZED_SYNC_BUCKETS_FREQUENCY_S, 300).
-endif.

%% The frequency of recalculating the total size of the stored unique data.
-define(UPDATE_SIZE_METRIC_FREQUENCY_MS, 10000).

-record(state, {
	sync_record,
	sync_buckets
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

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(data_sync),
	{ok, Config} = application:get_env(arweave, config),
	SyncRecord =
		lists:foldl(
			fun(Module, Acc) ->
				StoreID =
					case Module of
						"default" ->
							"default";
						_ ->
							ar_storage_module:id(Module)
					end,
				R = ar_sync_record:get(ar_data_sync, StoreID),
				ar_intervals:union(R, Acc)
			end,
			ar_intervals:new(),
			["default" | Config#config.storage_modules]
		),
	SyncBuckets = ar_sync_buckets:from_intervals(SyncRecord),
	{SyncBuckets2, SerializedSyncBuckets} = ar_sync_buckets:serialize(SyncBuckets,
					?MAX_SYNC_BUCKETS_SIZE),
	ets:insert(?MODULE, {serialized_sync_buckets, SerializedSyncBuckets}),
	ar_util:cast_after(?UPDATE_SERIALIZED_SYNC_BUCKETS_FREQUENCY_S * 1000,
			?MODULE, update_serialized_sync_buckets),
	gen_server:cast(?MODULE, record_v2_index_data_size_metric),
	{ok, #state{ sync_record = SyncRecord, sync_buckets = SyncBuckets2 }}.

handle_call({get_serialized_sync_record, Args}, _From, State) ->
	#state{ sync_record = SyncRecord } = State,
	Limit = min(maps:get(limit, Args, ?MAX_SHARED_SYNCED_INTERVALS_COUNT),
			?MAX_SHARED_SYNCED_INTERVALS_COUNT),
	{reply, {ok, ar_intervals:serialize(Args#{ limit => Limit }, SyncRecord)}, State};

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(update_serialized_sync_buckets, State) ->
	#state{ sync_buckets = SyncBuckets } = State,
	{SyncBuckets2, SerializedSyncBuckets} = ar_sync_buckets:serialize(SyncBuckets,
			?MAX_SYNC_BUCKETS_SIZE),
	ets:insert(?MODULE, {serialized_sync_buckets, SerializedSyncBuckets}),
	ar_util:cast_after(?UPDATE_SERIALIZED_SYNC_BUCKETS_FREQUENCY_S * 1000,
			?MODULE, update_serialized_sync_buckets),
	{noreply, State#state{ sync_buckets = SyncBuckets2 }};

handle_cast(record_v2_index_data_size_metric, State) ->
	#state{ sync_record = SyncRecord } = State,
	ar_mining_stats:set_total_data_size(ar_intervals:sum(SyncRecord)),
	ar_util:cast_after(?UPDATE_SIZE_METRIC_FREQUENCY_MS, ?MODULE,
			record_v2_index_data_size_metric),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info({event, data_sync, {add_range, Start, End, _StoreID}}, State) ->
	#state{ sync_record = SyncRecord, sync_buckets = SyncBuckets } = State,
	SyncRecord2 = ar_intervals:add(SyncRecord, End, Start),
	SyncBuckets2 = ar_sync_buckets:add(End, Start, SyncBuckets),
	{noreply, State#state{ sync_record = SyncRecord2, sync_buckets = SyncBuckets2 }};

handle_info({event, data_sync, {cut, Offset}}, State) ->
	#state{ sync_record = SyncRecord, sync_buckets = SyncBuckets } = State,
	SyncRecord2 = ar_intervals:cut(SyncRecord, Offset),
	SyncBuckets2 = ar_sync_buckets:cut(Offset, SyncBuckets),
	{noreply, State#state{ sync_record = SyncRecord2, sync_buckets = SyncBuckets2 }};

handle_info({event, data_sync, {remove_range, Start, End}}, State) ->
	#state{ sync_record = SyncRecord, sync_buckets = SyncBuckets } = State,
	SyncRecord2 = ar_intervals:delete(SyncRecord, End, Start),
	SyncBuckets2 = ar_sync_buckets:delete(End, Start, SyncBuckets),
	{noreply, State#state{ sync_record = SyncRecord2, sync_buckets = SyncBuckets2 }};

handle_info({event, data_sync, _}, State) ->
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{event, terminate}, {reason, io_lib:format("~p", [Reason])}]).
