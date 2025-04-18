-module(ar_partition_snapshot).

-behaviour(gen_server).

-export([start_link/2, register_workers/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("ar_sup.hrl").
-include_lib("ar_config.hrl").

%% The frequency of generating snapshots in milliseconds
-define(SNAPSHOT_INTERVAL, 10 * 60 * 1000). % 10 minutes in milliseconds

-record(state, {
    store_id,
    data_dir
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

name(StoreID) ->
	list_to_atom("ar_partition_snapshot_" ++ ar_storage_module:label(StoreID)).

start_link(Name, StoreID) ->
    gen_server:start_link({local, Name}, ?MODULE, StoreID, []).

register_workers() ->
    {ok, Config} = application:get_env(arweave, config),
    case lists:member(partition_snapshot, Config#config.enable) of
        true ->
            StorageModuleWorkers = lists:map(
                fun(StorageModule) ->
                    StoreID = ar_storage_module:id(StorageModule),
                    Name = name(StoreID),
                    ?CHILD_WITH_ARGS(ar_partition_snapshot, worker, Name, [Name, StoreID])
                end,
                Config#config.storage_modules
            ),
            RepackInPlaceWorkers = lists:map(
                fun({StorageModule, _TargetPacking}) ->
                    StoreID = ar_storage_module:id(StorageModule),
                    Name = name(StoreID),
                    ?CHILD_WITH_ARGS(ar_partition_snapshot, worker, Name, [Name, StoreID])
                end,
                Config#config.repack_in_place_storage_modules
            ),
            StorageModuleWorkers ++ RepackInPlaceWorkers;
        false ->
            []
    end.

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init(StoreID) ->
    {ok, Config} = application:get_env(arweave, config),
    DataDir = Config#config.data_dir,
    State = #state{
        store_id = StoreID,
        data_dir = DataDir
    },
    ?LOG_INFO([{event, partition_snapshot_init},
        {store_id, StoreID},
        {data_dir, DataDir}]),

    %% Schedule the first snapshot
    ar_util:cast_after(?SNAPSHOT_INTERVAL, self(), snapshot),
    {ok, State}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(snapshot, State) ->
    #state{store_id = StoreID, data_dir = DataDir} = State,
    {ModuleStart, ModuleEnd} = ar_storage_module:get_range(StoreID),
    ChunkPackings = ar_chunk_visualization:get_chunk_packings(ModuleStart, ModuleEnd, StoreID),
    Bitmap = ar_chunk_visualization:generate_bitmap(ChunkPackings),
    Timestamp = erlang:system_time(second),
    Filename = io_lib:format("partition_snapshot_~s_~p.ppm", [StoreID, Timestamp]),
    Filepath = filename:join([DataDir, "snapshots", Filename]),
    ok = filelib:ensure_dir(Filepath),
    ok = file:write_file(Filepath, ar_chunk_visualization:bitmap_to_binary(Bitmap)),
    ?LOG_INFO([{event, partition_snapshot},
        {store_id, StoreID}, {timestamp, Timestamp}, {filepath, Filepath}]),
    %% Schedule the next snapshot
    ar_util:cast_after(?SNAPSHOT_INTERVAL, self(), snapshot),
    {noreply, State};

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.


