%%% Offline storage-module inspection commands.
-module(arweave_tools_doctor_inspect).

-export([main/1, help/0]).

-include_lib("arweave_storage/include/arweave_storage.hrl").

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% main/1 expects ["bitmap", DataDir, StorageModule] for generating a bitmap
%% of chunk states.
main(Args) ->
    case Args of
        ["bitmap", DataDir, StorageModuleConfig] ->
            bitmap(DataDir, StorageModuleConfig);
        _ ->
            false
    end.

help() ->
    ar:console("Usage: inspect bitmap <data_dir> <storage_module>~n"),
    ar:console("storage_module is a JSON storage_modules entry, e.g.~n"),
    ar:console("'{\"partition\": 0, \"packing_format\": \"replica_2_9\", \"packing_address\": \"<addr>\"}'~n"),
    ar:console("or with range_start/range_end.~n").

%%--------------------------------------------------------------------
%% Inspect Bitmap
%%--------------------------------------------------------------------

%% @doc Generates a bitmap of the provided storage module. Each pixel is a chunk where
%% the color is determined by the packing format of the chunk. Each row of the bitmap
%% is a replica.2.9 sector (so the bitmap is 1024 rows high).
bitmap(DataDir, StorageModuleConfig) ->
    {ok, Entry} =
        arweave_config:parse_storage_module_arg(StorageModuleConfig),
    ok = arweave_config:set([storage_modules], [Entry]),
    ok = arweave_config:load(#{ [data_dir] => DataDir }),

    [StorageModule] = arweave_config:storage_modules(),
    #store_info{id = StoreID} = arweave_storage:store_info(StorageModule),

    case arweave_tools_doctor:check_module_dir(StoreID) of
        false ->
            false;
        true ->
            do_bitmap(StorageModule, StoreID)
    end.

do_bitmap(StorageModule, StoreID) ->
    ar_kv_sup:start_link(),
    ar_storage_sup:start_link(),
    {ok, _} = application:ensure_all_started(arweave_storage),
    {ok, _} = arweave_storage:activate(standalone),
    ar_data_sync:open_store_dbs(StoreID),

    #store_info{effective_range = {ModuleStart, ModuleEnd}} =
        arweave_storage:store_info(StorageModule),
    ChunkPackings = arweave_tools_chunk_visualization:get_chunk_packings(
                      ModuleStart, ModuleEnd, StoreID, true),
    arweave_tools_chunk_visualization:print_chunk_stats(ChunkPackings),
    Bitmap = arweave_tools_chunk_visualization:generate_bitmap(ChunkPackings),

    Filename = "bitmap_" ++ StoreID ++ ".ppm",
    file:write_file(
        Filename, arweave_tools_chunk_visualization:bitmap_to_binary(Bitmap)),
    ar:console("Bitmap written to ~s~n", [Filename]),
    true.
