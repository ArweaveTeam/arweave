%%% Storage-module metadata shared by storage, sync and mining.
-module(arweave_storage_module).
-ifdef(AR_TEST).
-export([has_any/2, has_range/3, get_unique_sorted_intervals/1,
    sort_storage_modules_by_left_bound/2, get_cover2/3]).
-endif.

-export_type([storage_module/0]).

-export([
    info/1,
    get_overlap/1,
    id/1,
    label/1,
    address_label/2,
    module_address/1,
    disk_dir_name/1,
    path/2,
    module_packing_difficulty/1,
    packing_label/1,
    get_by_id/1,
    get_range/1,
    get_range_safe/1,
    get_padded_range/1,
    module_range/1, module_range/2,
    get_packing/1,
    covering_store/2,
    covering_stores/2,
    intersecting_stores/3,
    covers_offset/3,
    covers_range/4,
    covering_ranges/3,
    is_repack_in_place/1
]).

-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").

%% The overlap makes sure a 100 MiB recall range can always be fetched
%% from a single storage module.
-ifdef(AR_TEST).
-define(OVERLAP, 262144).
-else.
-define(OVERLAP, (?LEGACY_RECALL_RANGE_SIZE)).
-endif.

-ifdef(AR_TEST).
-define(REPLICA_2_9_OVERLAP, 262144).
-else.
-define(REPLICA_2_9_OVERLAP, (262144 * 10)).
-endif.

%% A storage module is `{RangeStart, RangeEnd, Packing}`: the
%% (inclusive, exclusive) byte range of the weave it covers, plus the
%% packing. Any range with `RangeEnd > RangeStart` is valid - no
%% alignment is required. Read from the config via
%% `arweave_config:storage_modules/0`.
-type storage_module() ::
    {integer(), integer(), atom()}
    | {integer(), integer(), {atom(), binary()}}
    | {integer(), integer(), {atom(), binary(), integer()}}.

%%%===================================================================
%%% Public interface.
%%%===================================================================

get_overlap({replica_2_9, _Addr}) ->
    ?REPLICA_2_9_OVERLAP;
get_overlap(_Packing) ->
    ?OVERLAP.

%% @doc Return metadata for a module tuple or configured ID, or not_found.
info(?DEFAULT_MODULE) ->
    Path = arweave_config:get([data_dir]),
    #store_info{
        id = ?DEFAULT_MODULE,
        label = ?DEFAULT_MODULE,
        configured_range = {0, infinity},
        effective_range = {0, infinity},
        padded_range = {-1, -1},
        disk_dir_name = ?DEFAULT_MODULE,
        path = Path,
        chunk_storage_path = filename:join(Path, ?CHUNK_DIR),
        repack_in_place = false,
        packing = unpacked,
        mining_address = undefined,
        packing_difficulty = 0
    };
info({Start, End, Packing} = Module) ->
    StoreID = id(Module),
    Range = module_range(Module),
    DiskDirName = do_disk_dir_name(Module, StoreID),
    Path = path(arweave_config:get([data_dir]), DiskDirName),
    #store_info{
        id = StoreID,
        label =
            case ets:whereis(arweave_storage) of
                undefined -> undefined;
                _ -> label(Module)
            end,
        configured_range = {Start, End},
        effective_range = Range,
        padded_range = padded_range(Range),
        disk_dir_name = DiskDirName,
        path = Path,
        chunk_storage_path = filename:join(Path, ?CHUNK_DIR),
        repack_in_place = lists:member(
            Module, arweave_config:repack_modules(module_only)
        ),
        packing = Packing,
        mining_address = module_address(Module),
        packing_difficulty = module_packing_difficulty(Module)
    };
info(StoreID) ->
    case get_by_id(StoreID) of
        {_, _, _} = Module -> info(Module);
        _ -> not_found
    end.

%% @doc Build a storage module path from its resolved on-disk directory name.
path(DataDir, ?DEFAULT_MODULE) ->
    DataDir;
path(DataDir, DiskDirName) ->
    filename:join([DataDir, "storage_modules", DiskDirName]).

%% @doc Return the storage module identifier.
id(?DEFAULT_MODULE) -> ?DEFAULT_MODULE;
id({Start, End, Packing}) -> id(Start, End, packing_string(Packing)).

packing_string({spora_2_6, Addr}) ->
    arweave_util:encode(Addr);
packing_string({replica_2_9, Addr}) ->
    <<(arweave_util:encode(Addr))/binary, ".replica.2.9">>;
packing_string(Packing) ->
    atom_to_list(Packing).

%% @doc Return the obscure unique label for the given storage module.
label(?DEFAULT_MODULE) ->
    ?DEFAULT_MODULE;
label({Start, End, Packing} = Module) ->
    StoreID = id(Module),
    case ets:lookup(arweave_storage, {label, StoreID}) of
        [] ->
            Label = id(Start, End, packing_label(Packing)),
            ets:insert(arweave_storage, {{label, StoreID}, Label}),
            Label;
        [{_, Label}] ->
            Label
    end;
label(StoreID) ->
    case ets:lookup(arweave_storage, {label, StoreID}) of
        [] ->
            {_, _, _} = Module = get_by_id(StoreID),
            label(Module);
        [{_, Label}] ->
            Label
    end.

%% @doc Return the obscure unique label for the given
%% replica owner address + replica type pair.
address_label(Addr, ReplicaType) ->
    Key = {Addr, ReplicaType},
    case ets:lookup(arweave_storage, {address_label, Key}) of
        [] ->
            Label = ets:update_counter(
                arweave_storage,
                last_address_label,
                1,
                {last_address_label, 0}
            ),
            case ets:insert_new(arweave_storage, {{address_label, Key}, Label}) of
                true -> integer_to_list(Label);
                false -> address_label(Addr, ReplicaType)
            end;
        [{_, Label}] ->
            integer_to_list(Label)
    end.

module_address({_, _, {spora_2_6, Addr}}) ->
    Addr;
module_address({_, _, {replica_2_9, Addr}}) ->
    Addr;
module_address(_StorageModule) ->
    undefined.

module_packing_difficulty({_, _, {replica_2_9, _Addr}}) ->
    ?REPLICA_2_9_PACKING_DIFFICULTY;
module_packing_difficulty(_StorageModule) ->
    0.

packing_label({spora_2_6, Addr}) ->
    AddrLabel = arweave_storage_module:address_label(Addr, spora_2_6),
    list_to_atom("spora_2_6_" ++ AddrLabel);
packing_label({replica_2_9, Addr}) ->
    AddrLabel = arweave_storage_module:address_label(Addr, replica_2_9),
    list_to_atom("replica_2_9_" ++ AddrLabel);
packing_label(Packing) ->
    Packing.

%% @doc Return the storage module with the given identifier or not_found.
%% Search across both attached modules and repacked in-place modules.
get_by_id(?DEFAULT_MODULE) ->
    ?DEFAULT_MODULE;
get_by_id(Atom) when is_atom(Atom) ->
    %% May be 'default' or an atom from the unit tests.
    Atom;
get_by_id(ID) ->
    get_by_id(
        ID,
        arweave_config:storage_modules() ++
            arweave_config:repack_modules(module_only)
    ).

get_by_id(_ID, []) ->
    not_found;
get_by_id(ID, [Module | Modules]) ->
    case id(Module) == ID of
        true ->
            Module;
        false ->
            get_by_id(ID, Modules)
    end.

%% @doc Return the range including overlap for a storage module tuple or ID.
get_range(?DEFAULT_MODULE) ->
    {0, infinity};
get_range({_, _, _} = Module) ->
    module_range(Module);
get_range(ID) ->
    Module = get_by_id(ID),
    case Module of
        not_found ->
            not_found;
        _ ->
            module_range(Module)
    end.

%% @doc Return {StartOffset, EndOffset} or not_found if the module is absent or
%% the storage-module registry is not ready yet.
get_range_safe(ID) ->
    case catch get_range(ID) of
        {'EXIT', _} -> not_found;
        not_found -> not_found;
        {Start, End} -> {Start, End}
    end.

%% @doc Return the chunk-padded finite range used by sync producers.
get_padded_range(?DEFAULT_MODULE) ->
    {-1, -1};
get_padded_range(ID) ->
    case get_range_safe(ID) of
        {RangeStart, RangeEnd} when is_integer(RangeStart), is_integer(RangeEnd) ->
            padded_range({RangeStart, RangeEnd});
        not_found ->
            {-1, -1}
    end.

padded_range({RangeStart, RangeEnd}) ->
    {
        max(0, arweave_constants:get_chunk_padded_offset(RangeStart) - ?DATA_CHUNK_SIZE),
        arweave_constants:get_chunk_padded_offset(RangeEnd)
    }.

module_range(Module) ->
    Packing = get_packing(Module),
    module_range(Module, arweave_storage:get_overlap(Packing)).

module_range(Module, Overlap) ->
    {Start, End, _Packing} = Module,
    {Start, End + Overlap}.

%% @doc Return the packing configured for the given module.
get_packing(?DEFAULT_MODULE) ->
    unpacked;
get_packing({_Start, _End, Packing}) ->
    Packing;
get_packing(ID) ->
    Module = get_by_id(ID),
    case Module of
        not_found ->
            not_found;
        _ ->
            get_packing(Module)
    end.

%% @doc Return a configured storage module covering the given Offset, preferably
%% with the given Packing. Return not_found if none is found.
covering_store(Offset, PreferredPacking) ->
    get(Offset, PreferredPacking, arweave_config:storage_modules(), not_found).

%% @doc Return the list of all configured storage modules covering the given Offset.
covering_stores(Offset, Packing) ->
    get_all2(Offset, selected_modules(Packing, any_store), []).

%% @doc Return the list of configured storage modules whose ranges intersect
%% the given interval.
intersecting_stores(Start, End, Packing) ->
    get_all2(Start, End, selected_modules(Packing, any_store), []).

%% @doc Check configured coverage of an offset, including packing overlap.
covers_offset(Offset, Packing, StoreID) ->
    has_any(Offset, selected_modules(Packing, StoreID)).

%% @doc Check complete configured range coverage, excluding packing overlap.
covers_range(Start, End, Packing, StoreID) ->
    %% This cache survives host restarts, which can load different modules.
    StorageModules = selected_modules(Packing, StoreID),
    case ets:lookup(arweave_storage, unique_sorted_intervals) of
        [{_, StorageModules, Intervals}] ->
            has_range(Start, End, Intervals);
        _ ->
            Intervals = get_unique_sorted_intervals(StorageModules),
            ets:insert(
                arweave_storage,
                {unique_sorted_intervals, StorageModules, Intervals}
            ),
            has_range(Start, End, Intervals)
    end.

%% @doc Return the list of at least one {Start, End, StoreID} covering the given range
%% or not_found. The given StoreID (may be none) has a higher chance to be picked in case
%% there are several storage modules covering the same range.
%%
%%                            0     6     10    14      20          30
%%                            |--- sm_1 ---|--- sm_2 ---|--- sm_3 ---|
%%                                         |----sm_4----|
%%
%% 1. covering_ranges(2, 8, none):       2<--->8
%% 2. covering_ranges(7, 13, none):          7<--------->13
%% 3. covering_ranges(7, 25, none):          7<-------------------->25
%% 4. covering_ranges(7, 25, sm4):           7<-------------------->25
%%
%% 1. returns [{2, 8, sm_1}]
%% 2. returns [{7, 10, sm1}, {10, 13, sm_2}]
%% 3. returns [{7, 10, sm1}, {10, 20, sm_2}, {20, 25, sm_3}]
%% 4. returns [{7, 10, sm1}, {10, 20, sm_4}, {20, 25, sm_3}]
covering_ranges(Start, End, MaybeModule) ->
    SortedStorageModules = sort_storage_modules_by_left_bound(
        arweave_config:storage_modules(), MaybeModule
    ),
    case get_cover2(Start, End, SortedStorageModules) of
        [] ->
            not_found;
        not_found ->
            not_found;
        Cover ->
            Cover
    end.

%% @doc Resolve the on-disk directory name for the given store id.
%% Normally the id itself. On a node configured with the legacy
%% notation (the only place bucket sizes can be written), a module
%% whose range is expressible in the legacy bucket notation keeps the
%% legacy directory name (storage_module_<size>_<index>_<packing>) -
%% so legacy nodes keep using their existing disks. Every legacy
%% module is bucket-aligned by construction, so the check is exact
%% there. Migrating a custom-bucket config to the current notation
%% means renaming those directories to the range-form names.
disk_dir_name(StoreID) ->
    do_disk_dir_name(get_by_id(StoreID), StoreID).

do_disk_dir_name(Module, StoreID) ->
    case arweave_config:is_legacy_launch() of
        false ->
            StoreID;
        true ->
            case Module of
                {Start, End, Packing} ->
                    case legacy_bucket_dir_name(Start, End, Packing) of
                        none -> StoreID;
                        LegacyName -> LegacyName
                    end;
                _ ->
                    %% not_found or a test atom - use the id as-is.
                    StoreID
            end
    end.

%% The legacy bucket-notation directory name for a bucket-aligned
%% custom range; none when the range is not expressible in bucket
%% notation. A whole aligned partition needs no translation - its id
%% already matches the legacy partition-form directory name.
legacy_bucket_dir_name(Start, End, Packing) ->
    Len = End - Start,
    case Len =/= arweave_constants:partition_size() andalso Start rem Len == 0 of
        true ->
            binary_to_list(
                iolist_to_binary(
                    io_lib:format(
                        "storage_module_~B_~B_~s",
                        [Len, Start div Len, packing_string(Packing)]
                    )
                )
            );
        false ->
            none
    end.

is_repack_in_place(ID) ->
    lists:any(
        fun(Module) ->
            id(Module) == ID
        end,
        arweave_config:repack_modules(module_only)
    ).

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Select configured modules without including the unbounded default store.
selected_modules(any_packing, any_store) ->
    arweave_config:storage_modules();
selected_modules(Packing, StoreID) ->
    lists:filter(
        fun({_, _, ModulePacking} = Module) ->
            (Packing =:= any_packing orelse Packing =:= ModulePacking) andalso
                (StoreID =:= any_store orelse StoreID =:= id(Module))
        end,
        arweave_config:storage_modules()
    ).

%% Build the identifier from the module's range. Two forms:
%%
%% - one whole aligned partition: storage_module_<partition>_<packing>
%% - any other range:             storage_module_<start>_<end>_<packing>
%%
%% The id is a pure function of the range. The legacy bucket-notation
%% directory names are a disk-level compatibility concern handled by
%% disk_dir_name/1, not part of the module's identity.
%%
%% The guard excludes invalid (empty or reversed) ranges: config
%% validation rejects them at load, so hitting one here is a bug and
%% should crash rather than silently name a directory.
id(Start, End, PackingString) when End > Start ->
    Len = End - Start,
    IsWholePartition =
        Len == arweave_constants:partition_size() andalso
            Start rem Len == 0,
    case IsWholePartition of
        true ->
            binary_to_list(
                iolist_to_binary(
                    io_lib:format(
                        "storage_module_~B_~s",
                        [Start div Len, PackingString]
                    )
                )
            );
        false ->
            binary_to_list(
                iolist_to_binary(
                    io_lib:format(
                        "storage_module_~B_~B_~s",
                        [Start, End, PackingString]
                    )
                )
            )
    end.

get(Offset, Packing, [{ModuleStart, ModuleEnd, Packing2} | StorageModules], StorageModule) ->
    case
        Offset =< ModuleStart orelse
            Offset > ModuleEnd + arweave_storage:get_overlap(Packing2)
    of
        true ->
            get(Offset, Packing, StorageModules, StorageModule);
        false ->
            case Packing == Packing2 of
                true ->
                    {ModuleStart, ModuleEnd, Packing};
                false ->
                    get(Offset, Packing, StorageModules, {ModuleStart, ModuleEnd, Packing})
            end
    end;
get(_Offset, _Packing, [], StorageModule) ->
    StorageModule.

get_all2(
    Offset, [{ModuleStart, ModuleEnd, Packing} = StorageModule | StorageModules], FoundModules
) ->
    case
        Offset =< ModuleStart orelse
            Offset > ModuleEnd + arweave_storage:get_overlap(Packing)
    of
        true ->
            get_all2(Offset, StorageModules, FoundModules);
        false ->
            get_all2(Offset, StorageModules, [StorageModule | FoundModules])
    end;
get_all2(_Offset, [], FoundModules) ->
    FoundModules.

get_all2(
    Start, End, [{ModuleStart, ModuleEnd, Packing} = StorageModule | StorageModules], FoundModules
) ->
    case
        End =< ModuleStart orelse
            Start >= ModuleEnd + arweave_storage:get_overlap(Packing)
    of
        true ->
            get_all2(Start, End, StorageModules, FoundModules);
        false ->
            get_all2(Start, End, StorageModules, [StorageModule | FoundModules])
    end;
get_all2(_Start, _End, [], FoundModules) ->
    FoundModules.

has_any(_Offset, []) ->
    false;
has_any(Offset, [{ModuleStart, ModuleEnd, Packing} | StorageModules]) ->
    case
        Offset > ModuleStart andalso
            Offset =< ModuleEnd + arweave_storage:get_overlap(Packing)
    of
        true ->
            true;
        false ->
            has_any(Offset, StorageModules)
    end.

get_unique_sorted_intervals(StorageModules) ->
    get_unique_sorted_intervals(StorageModules, ar_intervals:new()).

get_unique_sorted_intervals([], Intervals) ->
    [{Start, End} || {End, Start} <- ar_intervals:to_list(Intervals)];
get_unique_sorted_intervals([{Start, End, _Packing} | StorageModules], Intervals) ->
    get_unique_sorted_intervals(StorageModules, ar_intervals:add(Intervals, End, Start)).

has_range(PartitionStart, PartitionEnd, _Intervals) when
    PartitionStart >= PartitionEnd
->
    true;
has_range(_PartitionStart, _PartitionEnd, []) ->
    false;
has_range(PartitionStart, _PartitionEnd, [{Start, _End} | _Intervals]) when
    PartitionStart < Start
->
    %% The given intervals are unique and sorted.
    false;
has_range(PartitionStart, PartitionEnd, [{_Start, End} | Intervals]) when
    PartitionStart >= End
->
    has_range(PartitionStart, PartitionEnd, Intervals);
has_range(_PartitionStart, PartitionEnd, [{_Start, End} | Intervals]) ->
    has_range(End, PartitionEnd, Intervals).

sort_storage_modules_by_left_bound(StorageModules, MaybeModule) ->
    lists:sort(
        fun({Start1, _End1, _} = M1, {Start2, _End2, _} = M2) ->
            case Start1 =< Start2 of
                false ->
                    false;
                true ->
                    case Start1 == Start2 of
                        true ->
                            M1 == MaybeModule orelse M2 /= MaybeModule;
                        false ->
                            true
                    end
            end
        end,
        StorageModules
    ).

get_cover2(Start, End, _StorageModules) when
    Start >= End
->
    [];
get_cover2(_Start, _End, []) ->
    not_found;
get_cover2(Start, _End, [{ModuleStart, _ModuleEnd, _Packing} | _StorageModules]) when
    ModuleStart > Start
->
    not_found;
get_cover2(Start, End, [{_ModuleStart, ModuleEnd, _Packing} | StorageModules]) when
    ModuleEnd =< Start
->
    get_cover2(Start, End, StorageModules);
get_cover2(Start, End, [{_ModuleStart, ModuleEnd, _Packing} = StorageModule | StorageModules]) ->
    End3 = min(End, ModuleEnd),
    StoreID = id(StorageModule),
    case get_cover2(End3, End, StorageModules) of
        not_found ->
            not_found;
        List ->
            [{Start, End3, StoreID} | List]
    end.
