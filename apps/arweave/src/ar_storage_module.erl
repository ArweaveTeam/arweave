-module(ar_storage_module).
-test_category([fast]).

-export([get_overlap/1, id/1, label/1, address_label/2, module_address/1,
        disk_dir_name/1,
        module_packing_difficulty/1, packing_label/1, get_by_id/1,
        get_range/1, get_range_safe/1, get_padded_range/1,
        module_range/1, module_range/2, get_packing/1,
        get/2, get_all/1, get_all/2, get_all/3,
        has_any/1, has_range/2, get_cover/3, is_repack_in_place/1]).

-include_lib("arweave_config/include/arweave_config.hrl").

-include("ar.hrl").
-include("ar_consensus.hrl").

-include_lib("eunit/include/eunit.hrl").

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
-type storage_module() :: {integer(), integer(), {atom(), binary()}}
                        | {integer(), integer(), {atom(), binary(), integer()}}.

%%%===================================================================
%%% Public interface.
%%%===================================================================

get_overlap({replica_2_9, _Addr}) ->
    ?REPLICA_2_9_OVERLAP;
get_overlap(_Packing) ->
    ?OVERLAP.

%% @doc Return the storage module identifier.
id(?DEFAULT_MODULE) -> ?DEFAULT_MODULE;
id({Start, End, Packing}) ->
    id(Start, End, packing_string(Packing)).

packing_string({spora_2_6, Addr}) ->
    arweave_util:encode(Addr);
packing_string({replica_2_9, Addr}) ->
    << (arweave_util:encode(Addr))/binary, ".replica.2.9" >>;
packing_string(Packing) ->
    atom_to_list(Packing).

%% @doc Return the obscure unique label for the given storage module.
label(?DEFAULT_MODULE) ->
    ?DEFAULT_MODULE;
label(StoreID) ->
    case ets:lookup(?MODULE, {label, StoreID}) of
        [] ->
            StorageModule = get_by_id(StoreID),
            {Start, End, Packing} = StorageModule,
            PackingLabel = packing_label(Packing),
            Label = id(Start, End, PackingLabel),
            ets:insert(?MODULE, {{label, StoreID}, Label}),
            Label;
        [{_, Label}] ->
            Label
    end.

%% @doc Return the obscure unique label for the given
%% replica owner address + replica type pair.
address_label(Addr, ReplicaType) ->
    Key = {Addr, ReplicaType},
    case ets:lookup(?MODULE, {address_label, Key}) of
        [] ->
            Label =
                case ets:lookup(?MODULE, last_address_label) of
                    [] ->
                        1;
                    [{_, Counter}] ->
                        Counter + 1
                end,
            ets:insert(?MODULE, {{address_label, Key}, Label}),
            ets:insert(?MODULE, {last_address_label, Label}),
            integer_to_list(Label);
        [{_, Label}] ->
            integer_to_list(Label)
    end.

-spec module_address(ar_storage_module:storage_module()) -> binary() | undefined.
module_address({_, _, {spora_2_6, Addr}}) ->
    Addr;
module_address({_, _, {replica_2_9, Addr}}) ->
    Addr;
module_address(_StorageModule) ->
    undefined.

-spec module_packing_difficulty(ar_storage_module:storage_module()) -> integer().
module_packing_difficulty({_, _, {replica_2_9, _Addr}}) ->
    ?REPLICA_2_9_PACKING_DIFFICULTY;
module_packing_difficulty(_StorageModule) ->
    0.

packing_label({spora_2_6, Addr}) ->
    AddrLabel = ar_storage_module:address_label(Addr, spora_2_6),
    list_to_atom("spora_2_6_" ++ AddrLabel);
packing_label({replica_2_9, Addr}) ->
    AddrLabel = ar_storage_module:address_label(Addr, replica_2_9),
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
    get_by_id(ID, arweave_config:storage_modules()
        ++ arweave_config:repack_modules(module_only)).

get_by_id(_ID, []) ->
    not_found;
get_by_id(ID, [Module | Modules]) ->
    case ar_storage_module:id(Module) == ID of
        true ->
            Module;
        false ->
            get_by_id(ID, Modules)
    end.

%% @doc Return {StartOffset, EndOffset} the given module is responsible for.
get_range(?DEFAULT_MODULE) ->
    {0, infinity};
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
-spec get_range_safe(term()) -> {non_neg_integer(), non_neg_integer() | infinity} | not_found.
get_range_safe(ID) ->
    case catch get_range(ID) of
        {'EXIT', _} -> not_found;
        not_found -> not_found;
        {Start, End} -> {Start, End}
    end.

%% @doc Return the chunk-padded finite range used by sync producers.
-spec get_padded_range(term()) -> {integer(), integer()}.
get_padded_range(?DEFAULT_MODULE) ->
    {-1, -1};
get_padded_range(ID) ->
    case get_range_safe(ID) of
        {RangeStart, RangeEnd} when is_integer(RangeStart), is_integer(RangeEnd) ->
            {max(0, ar_block:get_chunk_padded_offset(RangeStart) - ?DATA_CHUNK_SIZE),
                ar_block:get_chunk_padded_offset(RangeEnd)};
        not_found ->
            {-1, -1}
    end.

-spec module_range(ar_storage_module:storage_module()) ->
    {non_neg_integer(), non_neg_integer()}.
module_range(Module) ->
    Packing = get_packing(Module),
    module_range(Module, ar_storage_module:get_overlap(Packing)).

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
get(Offset, Packing) ->
    get(Offset, Packing, arweave_config:storage_modules(), not_found).

%% @doc Return the list of all configured storage modules covering the given Offset.
get_all(Offset) ->
    get_all2(Offset, arweave_config:storage_modules(), []).

%% @doc Return the list of configured storage modules whose ranges intersect
%% the given interval.
get_all(Start, End) ->
    get_all(Start, End, arweave_config:storage_modules()).

%% @doc Return the list of storage modules chosen from the given list
%% whose ranges intersect the given interval.
get_all(Start, End, StorageModules) ->
    get_all2(Start, End, StorageModules, []).

%% @doc Return true if the given Offset belongs to at least one storage module.
has_any(Offset) ->
    has_any(Offset, arweave_config:storage_modules()).

%% @doc Return true if the given range is covered by the configured storage modules.
has_range(Start, End) ->
    case ets:lookup(?MODULE, unique_sorted_intervals) of
        [] ->
            Intervals = get_unique_sorted_intervals(
                arweave_config:storage_modules()),
            ets:insert(?MODULE, {unique_sorted_intervals, Intervals}),
            has_range(Start, End, Intervals);
        [{_, Intervals}] ->
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
%% 1. get_cover(2, 8, none):       2<--->8
%% 2. get_cover(7, 13, none):          7<--------->13
%% 3. get_cover(7, 25, none):          7<-------------------->25
%% 4. get_cover(7, 25, sm4):           7<-------------------->25
%%
%% 1. returns [{2, 8, sm_1}]
%% 2. returns [{7, 10, sm1}, {10, 13, sm_2}]
%% 3. returns [{7, 10, sm1}, {10, 20, sm_2}, {20, 25, sm_3}]
%% 4. returns [{7, 10, sm1}, {10, 20, sm_4}, {20, 25, sm_3}]
get_cover(Start, End, MaybeModule) ->
    SortedStorageModules = sort_storage_modules_by_left_bound(
            arweave_config:storage_modules(), MaybeModule),
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
    case arweave_config:is_legacy_launch() of
        false ->
            StoreID;
        true ->
            case get_by_id(StoreID) of
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
    case Len =/= ar_block:partition_size() andalso Start rem Len == 0 of
        true ->
            binary_to_list(iolist_to_binary(io_lib:format(
                "storage_module_~B_~B_~s",
                [Len, Start div Len, packing_string(Packing)])));
        false ->
            none
    end.

is_repack_in_place(ID) ->
    lists:any(
        fun(Module) ->
            ar_storage_module:id(Module) == ID
        end,
        arweave_config:repack_modules(module_only)).

%%%===================================================================
%%% Private functions.
%%%===================================================================

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
    IsWholePartition = Len == ar_block:partition_size()
        andalso Start rem Len == 0,
    case IsWholePartition of
        true ->
            binary_to_list(iolist_to_binary(io_lib:format("storage_module_~B_~s",
                    [Start div Len, PackingString])));
        false ->
            binary_to_list(iolist_to_binary(io_lib:format("storage_module_~B_~B_~s",
                    [Start, End, PackingString])))
    end.

get(Offset, Packing, [{ModuleStart, ModuleEnd, Packing2} | StorageModules], StorageModule) ->
    case Offset =< ModuleStart
            orelse Offset > ModuleEnd + ar_storage_module:get_overlap(Packing2) of
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

get_all2(Offset, [{ModuleStart, ModuleEnd, Packing} = StorageModule | StorageModules], FoundModules) ->
    case Offset =< ModuleStart
            orelse Offset > ModuleEnd + ar_storage_module:get_overlap(Packing) of
        true ->
            get_all2(Offset, StorageModules, FoundModules);
        false ->
            get_all2(Offset, StorageModules, [StorageModule | FoundModules])
    end;
get_all2(_Offset, [], FoundModules) ->
    FoundModules.

get_all2(Start, End, [{ModuleStart, ModuleEnd, Packing} = StorageModule | StorageModules], FoundModules) ->
    case End =< ModuleStart
            orelse Start >= ModuleEnd + ar_storage_module:get_overlap(Packing) of
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
    case Offset > ModuleStart
            andalso Offset =< ModuleEnd + ar_storage_module:get_overlap(Packing) of
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

has_range(PartitionStart, PartitionEnd, _Intervals)
        when PartitionStart >= PartitionEnd ->
    true;
has_range(_PartitionStart, _PartitionEnd, []) ->
    false;
has_range(PartitionStart, _PartitionEnd, [{Start, _End} | _Intervals])
        when PartitionStart < Start ->
    %% The given intervals are unique and sorted.
    false;
has_range(PartitionStart, PartitionEnd, [{_Start, End} | Intervals])
        when PartitionStart >= End ->
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

get_cover2(Start, End, _StorageModules)
        when Start >= End ->
    [];
get_cover2(_Start, _End, []) ->
    not_found;
get_cover2(Start, _End, [{ModuleStart, _ModuleEnd, _Packing} | _StorageModules])
        when ModuleStart > Start ->
    not_found;
get_cover2(Start, End, [{_ModuleStart, ModuleEnd, _Packing} | StorageModules])
        when ModuleEnd =< Start ->
    get_cover2(Start, End, StorageModules);
get_cover2(Start, End, [{_ModuleStart, ModuleEnd, _Packing} = StorageModule | StorageModules]) ->
    End3 = min(End, ModuleEnd),
    StoreID = ar_storage_module:id(StorageModule),
    case get_cover2(End3, End, StorageModules) of
        not_found ->
            not_found;
        List ->
            [{Start, End3, StoreID} | List]
    end.

%%%===================================================================
%%% Tests.
%%%===================================================================

-define(LABEL_TEST_ADDR_A, <<"label-test-address-a-aaaaaaaaaaa">>).
-define(LABEL_TEST_ADDR_B, <<"label-test-address-b-bbbbbbbbbbb">>).
-define(LABEL_TEST_ADDR_C, <<255, 255, 255, "label-test-c-cccccccccccccccc">>).

label_test() ->
    OldLabels = ets:match_object(?MODULE, {{label, '_'}, '_'}),
    OldAddrLabels = ets:match_object(?MODULE, {{address_label, '_'}, '_'}),
    OldLastLabel = ets:lookup(?MODULE, last_address_label),
    ets:match_delete(?MODULE, {{label, '_'}, '_'}),
    ets:match_delete(?MODULE, {{address_label, '_'}, '_'}),
    ets:delete(?MODULE, last_address_label),
    try
        arweave_config:with_test_config(fun() ->
            P0 = ar_block:partition_size(),
            StorageModules = [
                {0, P0, {spora_2_6, ?LABEL_TEST_ADDR_A}},
                {2 * P0, 3 * P0, {spora_2_6, ?LABEL_TEST_ADDR_A}},
                {0, P0, {spora_2_6, ?LABEL_TEST_ADDR_B}},
                {3 * 524288, 4 * 524288, {spora_2_6, ?LABEL_TEST_ADDR_B}},
                {2 * P0, 3 * P0, unpacked},
                {2 * P0, 3 * P0, {spora_2_6, ?LABEL_TEST_ADDR_C}},
                {2 * 524288, 3 * 524288, {spora_2_6, ?LABEL_TEST_ADDR_C}}
            ],
            ok = arweave_config:force_config(#{
                [storage_modules] => StorageModules
            }),
            P = ar_block:partition_size(),
            ?assertEqual("storage_module_0_spora_2_6_1",
                label(id({0, P, {spora_2_6, ?LABEL_TEST_ADDR_A}}))),
            ?assertEqual("storage_module_2_spora_2_6_1",
                label(id({2 * P, 3 * P, {spora_2_6, ?LABEL_TEST_ADDR_A}}))),
            ?assertEqual("storage_module_0_spora_2_6_2",
                label(id({0, P, {spora_2_6, ?LABEL_TEST_ADDR_B}}))),
            ?assertEqual("storage_module_1572864_2097152_spora_2_6_2",
                label(id({3 * 524288, 4 * 524288, {spora_2_6, ?LABEL_TEST_ADDR_B}}))),
            ?assertEqual("storage_module_2_unpacked",
                label(id({2 * P, 3 * P, unpacked}))),
            %% force a _ in the encoded address
            ?assertEqual("storage_module_2_spora_2_6_3",
                label(id({2 * P, 3 * P, {spora_2_6, ?LABEL_TEST_ADDR_C}}))),
            ?assertEqual("storage_module_1048576_1572864_spora_2_6_3",
                label(id({2 * 524288, 3 * 524288, {spora_2_6, ?LABEL_TEST_ADDR_C}})))
        end)
    after
        ets:match_delete(?MODULE, {{label, '_'}, '_'}),
        ets:match_delete(?MODULE, {{address_label, '_'}, '_'}),
        ets:delete(?MODULE, last_address_label),
        ets:insert(?MODULE, OldLabels),
        ets:insert(?MODULE, OldAddrLabels),
        ets:insert(?MODULE, OldLastLabel)
    end.

disk_dir_name_test() ->
    arweave_config:with_test_config(fun() ->
        P = ar_block:partition_size(),
        BucketModule = {2 * 524288, 3 * 524288, unpacked},
        PartitionModule = {0, P, unpacked},
        ok = arweave_config:force_config(
            #{[storage_modules] => [BucketModule, PartitionModule]}),
        BucketStoreID = id(BucketModule),
        PartitionStoreID = id(PartitionModule),
        ?assertEqual("storage_module_1048576_1572864_unpacked",
            BucketStoreID),
        %% Current-notation launch: the id names the directory.
        ?assertEqual(BucketStoreID, disk_dir_name(BucketStoreID)),
        %% Legacy-notation launch: bucket-expressible modules keep the
        %% legacy bucket-notation directory name... (force_config: the
        %% eunit node is in runtime mode, where the load-only
        %% config_dialect option rejects plain sets.)
        ok = arweave_config:force_config(#{[config_dialect] => legacy}),
        ?assertEqual("storage_module_524288_2_unpacked",
            disk_dir_name(BucketStoreID)),
        %% ...and whole partitions keep their (identical) name.
        ?assertEqual(PartitionStoreID, disk_dir_name(PartitionStoreID))
    end).

range_helpers_test() ->
    arweave_config:with_test_config(fun() ->
        Module = {1_000_000, 1, unpacked},
        StoreID = id(Module),
        ok = arweave_config:force_config(#{
            [storage_modules] => [arweave_config:storage_module_to_config(Module)]
        }),
        RawRange = module_range(Module),
        ?assertEqual(RawRange, get_range_safe(StoreID)),
        {RangeStart, RangeEnd} = RawRange,
        ?assertEqual(
            {max(0, ar_block:get_chunk_padded_offset(RangeStart) - ?DATA_CHUNK_SIZE),
                ar_block:get_chunk_padded_offset(RangeEnd)},
            get_padded_range(StoreID)),
        ?assertEqual(not_found, get_range_safe(missing_store)),
        ?assertEqual({-1, -1}, get_padded_range(missing_store)),
        ?assertEqual({0, infinity}, get_range_safe(?DEFAULT_MODULE)),
        ?assertEqual({-1, -1}, get_padded_range(?DEFAULT_MODULE))
    end).

has_any_test() ->
    ?assertEqual(false, has_any(0, [])),
    ?assertEqual(false, has_any(0, [{10, 20, p}])),
    ?assertEqual(false, has_any(10, [{10, 20, p}])),
    ?assertEqual(true, has_any(11, [{10, 20, p}])),
    ?assertEqual(true, has_any(11, [{10, 20, {replica_2_9, a}}])),
    ?assertEqual(true, has_any(20 + ?OVERLAP, [{10, 20, p}])),
    ?assertEqual(true, has_any(20 + ?OVERLAP, [{10, 20, {replica_2_9, a}}])),
    %% Unaligned range.
    ?assertEqual(false, has_any(7, [{7, 15, p}])),
    ?assertEqual(true, has_any(8, [{7, 15, p}])),
    ?assertEqual(true, has_any(15 + ?OVERLAP, [{7, 15, p}])),
    ?assertEqual(false, has_any(16 + ?OVERLAP, [{7, 15, p}])).

get_unique_sorted_intervals_test() ->
    ?assertEqual([{0, 24}, {90, 120}],
            get_unique_sorted_intervals([{0, 10, p}, {90, 120, p}, {0, 20, p}, {12, 24, p}])),
    %% Unaligned ranges merge the same way.
    ?assertEqual([{3, 17}],
            get_unique_sorted_intervals([{3, 11, p}, {7, 17, p}])).

has_range_test() ->
    ?assertEqual(false, has_range(0, 10, [])),
    ?assertEqual(false, has_range(0, 10, [{0, 9}])),
    ?assertEqual(true, has_range(0, 10, [{0, 10}])),
    ?assertEqual(true, has_range(0, 10, [{0, 11}])),
    ?assertEqual(true, has_range(0, 10, [{0, 9}, {9, 10}])),
    ?assertEqual(true, has_range(5, 10, [{0, 9}, {9, 10}])),
    ?assertEqual(true, has_range(5, 10, [{0, 2}, {2, 9}, {9, 10}])).

sort_storage_modules_by_left_bound_test() ->
    ?assertEqual([], sort_storage_modules_by_left_bound([], none)),
    ?assertEqual([{0, 1, p}], sort_storage_modules_by_left_bound([{0, 1, p}], none)),
    ?assertEqual([{0, 10, p}, {10, 20, p}, {20, 30, p}],
            sort_storage_modules_by_left_bound([{10, 20, p}, {0, 10, p}, {20, 30, p}], none)),
    ?assertEqual([{0, 10, p}, {7, 14, p}, {10, 20, p}, {20, 30, p}],
            sort_storage_modules_by_left_bound([{10, 20, p}, {0, 10, p}, {20, 30, p},
                    {7, 14, p}], none)),
    ?assertEqual([{0, 10, p}, {10, 20, p}, {10, 20, p2}],
            sort_storage_modules_by_left_bound([{10, 20, p}, {0, 10, p}, {10, 20, p2}], none)),
    ?assertEqual([{0, 10, p}, {10, 20, p2}, {10, 20, p}],
            sort_storage_modules_by_left_bound([{10, 20, p}, {0, 10, p}, {10, 20, p2}],
                    {10, 20, p2})).

get_cover2_test() ->
    ?assertEqual(not_found, get_cover2(0, 1, [])),
    ?assertEqual([{0, 1, "storage_module_0_1_p"}], get_cover2(0, 1, [{0, 1, p}])),
    ?assertEqual([{0, 1, "storage_module_0_1_p"}, {1, 2, "storage_module_1_2_p"}],
            get_cover2(0, 2, [{0, 1, p}, {1, 2, p}])),
    ?assertEqual(not_found, get_cover2(0, 2, [{0, 1, p}, {2, 3, p}])),
    ?assertEqual([{0, 2, "storage_module_0_2_p"}],
            get_cover2(0, 2, [{0, 2, p}, {0, 1, p}])),
    ?assertEqual([{0, 2, "storage_module_0_2_p"}, {2, 3, "storage_module_0_3_p"}],
            get_cover2(0, 3, [{0, 2, p}, {0, 3, p}])),
    ?assertEqual([{0, 1, "storage_module_0_1_p"}, {1, 3, "storage_module_1_3_p"}],
            get_cover2(0, 3, [{0, 1, p}, {1, 3, p}])).
