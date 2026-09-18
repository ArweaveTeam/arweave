-module(arweave_storage_global_sync_record).

-behaviour(gen_server).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include_lib("arweave/include/ar_sync_buckets.hrl").

-export([
    start_link/0,
    get_serialized_sync_record/1,
    get_serialized_buckets/1
]).

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
%% format           required    etf or json     serialize in Erlang Term Format or JSON
%% random_subset    optional    any()           pick a random subset if the key is present
%% start            optional    integer()       pick intervals with right bound >= start
%% right_bound      optional    integer()       pick intervals with right bound <= right_bound
%% limit            optional    integer()       the number of intervals to pick
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
get_serialized_buckets(byte) ->
    case ets:lookup(?MODULE, serialized_sync_buckets) of
        [] ->
            {error, not_initialized};
        [{_, SerializedSyncBuckets}] ->
            {ok, SerializedSyncBuckets}
    end;
get_serialized_buckets(footprint) ->
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
    ok = arweave_storage_deps:subscribe(sync_record),
    SyncRecord = init_sync_record(),
    SyncBuckets = cache_and_get_sync_buckets(
        SyncRecord,
        serialized_sync_buckets,
        arweave_storage_deps:new_buckets()
    ),
    FootprintRecord = init_footprint_record(),
    FootprintBuckets = cache_and_get_sync_buckets(
        FootprintRecord,
        serialized_footprint_buckets,
        arweave_storage_deps:new_buckets(?NETWORK_FOOTPRINT_BUCKET_SIZE)
    ),
    ?LOG_INFO([{event, ar_global_sync_record_initialized}]),
    {ok, #state{
        sync_record = SyncRecord,
        sync_buckets = SyncBuckets,
        footprint_record = FootprintRecord,
        footprint_buckets = FootprintBuckets
    }}.

handle_call({get_serialized_sync_record, Args}, _From, State) ->
    #state{sync_record = SyncRecord} = State,
    Limit = min(
        maps:get(limit, Args, ?MAX_SHARED_SYNCED_INTERVALS_COUNT),
        ?MAX_SHARED_SYNCED_INTERVALS_COUNT
    ),
    {reply, {ok, ar_intervals:serialize(Args#{limit => Limit}, SyncRecord)}, State};
handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, ok, State}.

handle_cast({update_serialized_sync_buckets, serialized_sync_buckets = Key}, State) ->
    #state{sync_buckets = SyncBuckets} = State,
    {SyncBuckets2, SerializedSyncBuckets} = arweave_storage_deps:serialize_buckets(
        SyncBuckets,
        ?MAX_SYNC_BUCKETS_SIZE
    ),
    ets:insert(?MODULE, {Key, SerializedSyncBuckets}),
    arweave_util:cast_after(
        ?UPDATE_SERIALIZED_SYNC_BUCKETS_FREQUENCY_S * 1000,
        ?MODULE,
        {update_serialized_sync_buckets, Key}
    ),
    {noreply, State#state{sync_buckets = SyncBuckets2}};
handle_cast({update_serialized_sync_buckets, serialized_footprint_buckets = Key}, State) ->
    #state{footprint_buckets = FootprintBuckets} = State,
    {FootprintBuckets2, SerializedFootprintBuckets} = arweave_storage_deps:serialize_buckets(
        FootprintBuckets, ?MAX_SYNC_BUCKETS_SIZE
    ),
    ets:insert(?MODULE, {Key, SerializedFootprintBuckets}),
    arweave_util:cast_after(
        ?UPDATE_SERIALIZED_SYNC_BUCKETS_FREQUENCY_S * 1000,
        ?MODULE,
        {update_serialized_sync_buckets, Key}
    ),
    {noreply, State#state{footprint_buckets = FootprintBuckets2}};
handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info(
    {event, sync_record, {add_range, Start, End, ar_data_sync, #{packing := Packing}}}, State
) ->
    #state{sync_record = SyncRecord, sync_buckets = SyncBuckets} = State,
    case Packing of
        {replica_2_9, _} ->
            %% Replica 2.9 data is recorded in the footprint record. It is synced
            %% footprint by footprint (not left to right).
            {noreply, State};
        _ ->
            SyncRecord2 = ar_intervals:add(SyncRecord, End, Start),
            SyncBuckets2 = arweave_storage_deps:add_bucket_range(End, Start, SyncBuckets),
            {noreply, State#state{sync_record = SyncRecord2, sync_buckets = SyncBuckets2}}
    end;
handle_info(
    {event, sync_record, {add_range, Start, End, ar_data_sync_footprints, _Options}}, State
) ->
    State2 = update_footprint_data(Start, End, State),
    {noreply, State2};
handle_info({event, sync_record, {global_cut, Offset}}, State) ->
    #state{sync_record = SyncRecord, sync_buckets = SyncBuckets} = State,
    SyncRecord2 = ar_intervals:cut(SyncRecord, Offset),
    SyncBuckets2 = arweave_storage_deps:cut_buckets(Offset, SyncBuckets),
    {noreply, State#state{sync_record = SyncRecord2, sync_buckets = SyncBuckets2}};
handle_info({event, sync_record, {global_remove_range, Start, End}}, State) ->
    #state{sync_record = SyncRecord, sync_buckets = SyncBuckets} = State,
    SyncRecord2 = ar_intervals:delete(SyncRecord, End, Start),
    SyncBuckets2 = arweave_storage_deps:delete_bucket_range(End, Start, SyncBuckets),
    State2 = remove_footprint_data(Start, End, State),
    {noreply, State2#state{sync_record = SyncRecord2, sync_buckets = SyncBuckets2}};
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

init_sync_record() ->
    Modules = [
        M
     || M <- [?DEFAULT_MODULE | arweave_config:storage_modules()],
        not is_replica_2_9(M)
    ],
    get_records_wait({ar_data_sync, byte}, Modules, ar_intervals:new()).

init_footprint_record() ->
    get_records_wait(
        {ar_data_sync, footprint},
        arweave_config:storage_modules(),
        ar_intervals:new()
    ).

%% @doc Combine local records, retrying any timed-out record lookup.
get_records_wait(Record, Modules, Acc) ->
    get_records_wait(Record, Modules, Acc, 600).

get_records_wait(_Record, [], Acc, _Retries) ->
    Acc;
get_records_wait(Record, [Module | Rest], Acc, Retries) ->
    StoreID = arweave_storage_module:id(Module),
    case arweave_storage_sync_record:get(any_packing, Record, StoreID) of
        {error, timeout} when Retries > 0 ->
            ?LOG_INFO([
                {event, waiting_for_sync_record},
                {record, Record},
                {store_id, StoreID},
                {retries_remaining, Retries}
            ]),
            timer:sleep(1000),
            get_records_wait(Record, [Module | Rest], Acc, Retries - 1);
        {error, timeout} ->
            error({sync_record_timeout, Record, StoreID});
        SyncRecord ->
            get_records_wait(Record, Rest, ar_intervals:union(SyncRecord, Acc), Retries)
    end.

is_replica_2_9({_, _, {replica_2_9, _}}) -> true;
is_replica_2_9(_) -> false.

cache_and_get_sync_buckets(SyncRecord, Key, SyncBuckets) ->
    SyncBuckets2 = arweave_storage_deps:buckets_from_intervals(SyncRecord, SyncBuckets),
    {SyncBuckets3, SerializedSyncBuckets} = arweave_storage_deps:serialize_buckets(
        SyncBuckets2,
        ?MAX_SYNC_BUCKETS_SIZE
    ),
    ets:insert(?MODULE, {Key, SerializedSyncBuckets}),
    arweave_util:cast_after(
        ?UPDATE_SERIALIZED_SYNC_BUCKETS_FREQUENCY_S * 1000,
        ?MODULE,
        {update_serialized_sync_buckets, Key}
    ),
    SyncBuckets3.

update_footprint_data(Start, End, State) when Start >= End ->
    State;
update_footprint_data(Start, End, State) ->
    #state{
        footprint_record = FootprintRecord,
        footprint_buckets = FootprintBuckets
    } = State,
    FootprintRecord2 = ar_intervals:add(FootprintRecord, Start + 1, Start),
    FootprintBuckets2 = arweave_storage_deps:add_bucket_range(Start + 1, Start, FootprintBuckets),
    State2 = State#state{
        footprint_record = FootprintRecord2,
        footprint_buckets = FootprintBuckets2
    },
    update_footprint_data(Start + 1, End, State2).

remove_footprint_data(Start, End, State) when Start >= End ->
    State;
remove_footprint_data(Start, End, State) ->
    #state{
        footprint_record = FootprintRecord,
        footprint_buckets = FootprintBuckets
    } = State,
    Offset = arweave_storage:get_footprint_offset(Start + ?DATA_CHUNK_SIZE),
    FootprintRecord2 = ar_intervals:delete(FootprintRecord, Offset, Offset - 1),
    FootprintBuckets2 = arweave_storage_deps:delete_bucket_range(
        Offset, Offset - 1, FootprintBuckets
    ),
    State2 = State#state{
        footprint_record = FootprintRecord2,
        footprint_buckets = FootprintBuckets2
    },
    remove_footprint_data(Start + ?DATA_CHUNK_SIZE, End, State2).
