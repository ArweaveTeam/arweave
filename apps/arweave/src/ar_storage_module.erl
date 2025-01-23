-module(ar_storage_module).

-export([id/1, label/1, address_label/2, module_address/1,
		module_packing_difficulty/1, packing_label/1, label_by_id/1, get_by_id/1,
		get_range/1, module_range/1, module_range/2, get_packing/1, get_size/1,
		get/2, get_strict/2, get_all/1, get_all/2, get_all_packed/3, get_all_module_ranges/0,
		has_any/1, has_range/2, get_cover/3, get_overlap/1]).

-include("../include/ar.hrl").
-include("../include/ar_consensus.hrl").
-include("../include/ar_config.hrl").

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

-type storage_module() :: {integer(), integer(), {atom(), binary()}}
						| {integer(), integer(), {atom(), binary(), integer()}}.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return the storage module identifier.
id({BucketSize, Bucket, Packing}) ->
	PackingString =
		case Packing of
			{spora_2_6, Addr} ->
				ar_util:encode(Addr);
			{composite, Addr, PackingDiff} ->
				<< (ar_util:encode(Addr))/binary, ".",
						(integer_to_binary(PackingDiff))/binary >>;
			{replica_2_9, Addr} ->
				<< (ar_util:encode(Addr))/binary, ".replica.2.9" >>;
			_ ->
				atom_to_list(Packing)
		end,
	id(BucketSize, Bucket, PackingString).

%% @doc Return the obscure unique label for the given storage module.
label({BucketSize, Bucket, Packing} = StorageModule) ->
	StoreID = ar_storage_module:id(StorageModule),
	case ets:lookup(?MODULE, {label, StoreID}) of
		[] ->
			PackingLabel =
				case Packing of
					{spora_2_6, Addr} ->
						ar_storage_module:address_label(Addr, spora_2_6);
					{composite, Addr, PackingDifficulty} ->
						ar_storage_module:address_label(Addr, {composite, PackingDifficulty});
					{replica_2_9, Addr} ->
						ar_storage_module:address_label(Addr, replica_2_9);
					_ ->
						atom_to_list(Packing)
				end,
			Label = id(BucketSize, Bucket, PackingLabel),
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
module_address({_, _, {composite, Addr, _PackingDifficulty}}) ->
	Addr;
module_address({_, _, {replica_2_9, Addr}}) ->
	Addr;
module_address(_StorageModule) ->
	undefined.

-spec module_packing_difficulty(ar_storage_module:storage_module()) -> integer().
module_packing_difficulty({_, _, {composite, _Addr, PackingDifficulty}}) ->
	true = PackingDifficulty /= ?REPLICA_2_9_PACKING_DIFFICULTY,
	PackingDifficulty;
module_packing_difficulty({_, _, {replica_2_9, _Addr}}) ->
	?REPLICA_2_9_PACKING_DIFFICULTY;
module_packing_difficulty(_StorageModule) ->
	0.

packing_label({spora_2_6, Addr}) ->
	AddrLabel = ar_storage_module:address_label(Addr, spora_2_6),
	list_to_atom("spora_2_6_" ++ AddrLabel);
packing_label({composite, Addr, PackingDifficulty}) ->
	AddrLabel = ar_storage_module:address_label(Addr, {composite, PackingDifficulty}),
	list_to_atom("composite_" ++ AddrLabel);
packing_label({replica_2_9, Addr}) ->
	AddrLabel = ar_storage_module:address_label(Addr, replica_2_9),
	list_to_atom("replica_2_9_" ++ AddrLabel);
packing_label(Packing) ->
	Packing.

%% @doc Return the obscure unique label for the given store ID.
label_by_id("default") ->
	"default";
label_by_id(StoreID) ->
	case get_by_id(StoreID) of
		not_found ->
			%% Occurs in tests on application shutdown.
			?LOG_WARNING([{event, store_id_for_label_not_found},
					{store_id, StoreID}]),
			"error";
		M ->
			label(M)
	end.

%% @doc Return the storage module with the given identifier or not_found.
%% Search across both attached modules and repacked in-place modules.
get_by_id(ID) ->
	{ok, Config} = application:get_env(arweave, config),
	RepackInPlaceModules = [element(1, El)
			|| El <- Config#config.repack_in_place_storage_modules],
	get_by_id(ID, Config#config.storage_modules ++ RepackInPlaceModules).

get_by_id(_ID, []) ->
	not_found;
get_by_id(ID, [Module | Modules]) ->
	case ar_storage_module:id(Module) == ID of
		true ->
			Module;
		false ->
			get_by_id(ID, Modules)
	end.

get_all_module_ranges() ->
	{ok, Config} = application:get_env(arweave, config),
	RepackInPlaceModulesStoreIDs = [
			{{BucketSize, Bucket, TargetPacking}, ar_storage_module:id(Module)}
		|| {{BucketSize, Bucket, _Packing} = Module, TargetPacking} <- Config#config.repack_in_place_storage_modules],
	ModuleStoreIDs = [{Module, ar_storage_module:id(Module)}
			|| Module <- Config#config.storage_modules],

	[{module_range(Module), Packing, StoreID} || {{_, _, Packing} = Module, StoreID} <-
		ModuleStoreIDs ++ RepackInPlaceModulesStoreIDs].

%% @doc Return {StartOffset, EndOffset} the given module is responsible for.
get_range("default") ->
	{0, infinity};
get_range(ID) ->
	Module = get_by_id(ID),
	case Module of
		not_found ->
			not_found;
		_ ->
			module_range(Module)
	end.

-spec module_range(ar_storage_module:storage_module()) ->
	{non_neg_integer(), non_neg_integer()}.
module_range(Module) ->	
	{_BucketSize, _Bucket, Packing} = Module,
	module_range(Module, get_overlap(Packing)).

module_range(Module, Overlap) ->	
	{BucketSize, Bucket, _Packing} = Module,
	{BucketSize * Bucket, (Bucket + 1) * BucketSize + Overlap}.

%% @doc Return the packing configured for the given module.
get_packing("default") ->
	unpacked;
get_packing(ID) ->
	Module = get_by_id(ID),
	case Module of
		not_found ->
			not_found;
		_ ->
			{_BucketSize, _Bucket, Packing} = Module,
			Packing
	end.

%% @doc Return the bucket size configured for the given module.
get_size(ID) ->
	Module = get_by_id(ID),
	case Module of
		not_found ->
			not_found;
		_ ->
			{BucketSize, _Bucket, _Packing} = Module,
			BucketSize
	end.

%% @doc Return a configured storage module covering the given Offset, preferably
%% with the given Packing. Return not_found if none is found.
get(Offset, Packing) ->
	{ok, Config} = application:get_env(arweave, config),
	get(Offset, Packing, Config#config.storage_modules, not_found).

%% @doc Return a configured storage module with the given Packing covering the given Offset.
%% Return not_found if none is found. If a module is configured with in-place repacking,
%% pick the target packing (the one we are repacking to.)
get_strict(Offset, Packing) ->
	get_strict(Offset, Packing, get_all_module_ranges()).

%% @doc Return the list of all configured storage modules covering the given Offset.
get_all(Offset) ->
	{ok, Config} = application:get_env(arweave, config),
	get_all(Offset, Config#config.storage_modules, []).

%% @doc Return the list of identifiers of all configured storage modules
%% covering the given Offset and Packing. If a module is configured with
%% in-place repacking, pick the target packing (the one we are repacking to.)
get_all_packed(Offset, Packing) ->
	get_all_packed(Offset, Packing, get_all_module_ranges()).

%% @doc Return the list of configured storage modules whose ranges intersect
%% the given interval.
get_all(Start, End) ->
	{ok, Config} = application:get_env(arweave, config),
	get_all(Start, End, Config#config.storage_modules, []).

%% @doc Return true if the given Offset belongs to at least one storage module.
has_any(Offset) ->
	{ok, Config} = application:get_env(arweave, config),
	has_any(Offset, Config#config.storage_modules).

%% @doc Return true if the given range is covered by the configured storage modules.
has_range(Start, End) ->
	{ok, Config} = application:get_env(arweave, config),
	case ets:lookup(?MODULE, unique_sorted_intervals) of
		[] ->
			Intervals = get_unique_sorted_intervals(Config#config.storage_modules),
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
get_cover(Start, End, MaybeStoreID) ->
	{ok, Config} = application:get_env(arweave, config),
	SortedStorageModules = sort_storage_modules_by_left_bound(
			Config#config.storage_modules, MaybeStoreID),
	case get_cover2(Start, End, SortedStorageModules) of
		[] ->
			not_found;
		not_found ->
			not_found;
		Cover ->
			Cover
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

id(BucketSize, Bucket, PackingString) when BucketSize == ?PARTITION_SIZE ->
	binary_to_list(iolist_to_binary(io_lib:format("storage_module_~B_~s",
			[Bucket, PackingString])));
id(BucketSize, Bucket, PackingString) ->
	binary_to_list(iolist_to_binary(io_lib:format("storage_module_~B_~B_~s",
			[BucketSize, Bucket, PackingString]))).

get(Offset, Packing, [{BucketSize, Bucket, Packing2} | StorageModules], StorageModule) ->
	case Offset =< BucketSize * Bucket
			orelse Offset > BucketSize * (Bucket + 1) + get_overlap(Packing2) of
		true ->
			get(Offset, Packing, StorageModules, StorageModule);
		false ->
			case Packing == Packing2 of
				true ->
					{BucketSize, Bucket, Packing};
				false ->
					get(Offset, Packing, StorageModules, {BucketSize, Bucket, Packing})
			end
	end;
get(_Offset, _Packing, [], StorageModule) ->
	StorageModule.

get_strict(Offset, Packing,
		[{{RangeStart, RangeEnd}, ModulePacking, StoreID} | StorageModules]) ->
	case Offset =< RangeStart orelse Offset > RangeEnd of
		true ->
			get_strict(Offset, Packing, StorageModules);
		false ->
			case Packing == ModulePacking of
				true ->
					{ok, StoreID};
				false ->
					get_strict(Offset, Packing, StorageModules)
			end
	end;
get_strict(_Offset, _Packing, []) ->
	not_found.

get_overlap({replica_2_9, _Addr}) ->
	?REPLICA_2_9_OVERLAP;
get_overlap(_Packing) ->
	?OVERLAP.

get_all(Offset, [{BucketSize, Bucket, Packing} = StorageModule | StorageModules], FoundModules) ->
	case Offset =< BucketSize * Bucket
			orelse Offset > BucketSize * (Bucket + 1) + get_overlap(Packing) of
		true ->
			get_all(Offset, StorageModules, FoundModules);
		false ->
			get_all(Offset, StorageModules, [StorageModule | FoundModules])
	end;
get_all(_Offset, [], FoundModules) ->
	FoundModules.

get_all_packed(Offset, Packing,
		[{{RangeStart, RangeEnd}, Packing, StoreID} | StorageModules]) ->
	case Offset =< RangeStart orelse Offset > RangeEnd of
		true ->
			get_all_packed(Offset, Packing, StorageModules);
		false ->
			[StoreID | get_all_packed(Offset, Packing, StorageModules)]
	end;
get_all_packed(Offset, Packing, [_Element | StorageModules]) ->
	get_all_packed(Offset, Packing, StorageModules);
get_all_packed(_Offset, _Packing, []) ->
	[].

get_all(Start, End, [{BucketSize, Bucket, Packing} = StorageModule | StorageModules], FoundModules) ->
	case End =< BucketSize * Bucket
			orelse Start >= BucketSize * (Bucket + 1) + get_overlap(Packing) of
		true ->
			get_all(Start, End, StorageModules, FoundModules);
		false ->
			get_all(Start, End, StorageModules, [StorageModule | FoundModules])
	end;
get_all(_Start, _End, [], FoundModules) ->
	FoundModules.

has_any(_Offset, []) ->
	false;
has_any(Offset, [{BucketSize, Bucket, Packing} | StorageModules]) ->
	case Offset > Bucket * BucketSize
			andalso Offset =< (Bucket + 1) * BucketSize + get_overlap(Packing) of
		true ->
			true;
		false ->
			has_any(Offset, StorageModules)
	end.

get_unique_sorted_intervals(StorageModules) ->
	get_unique_sorted_intervals(StorageModules, ar_intervals:new()).

get_unique_sorted_intervals([], Intervals) ->
	[{Start, End} || {End, Start} <- ar_intervals:to_list(Intervals)];
get_unique_sorted_intervals([{BucketSize, Bucket, _Packing} | StorageModules], Intervals) ->
	End = (Bucket + 1) * BucketSize,
	Start = Bucket * BucketSize,
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

sort_storage_modules_by_left_bound(StorageModules, MaybeStoreID) ->
	lists:sort(
		fun({BucketSize1, Bucket1, _} = M1, {BucketSize2, Bucket2, _} = M2) ->
			Start1 = BucketSize1 * Bucket1,
			Start2 = BucketSize2 * Bucket2,
			case Start1 =< Start2 of
				false ->
					false;
				true ->
					case Start1 == Start2 of
						true ->
							StoreID1 = ar_storage_module:id(M1),
							StoreID2 = ar_storage_module:id(M2),
							StoreID1 == MaybeStoreID orelse StoreID2 /= MaybeStoreID;
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
get_cover2(Start, _End, [{BucketSize, Bucket, _Packing} | _StorageModules])
		when BucketSize * Bucket > Start ->
	not_found;
get_cover2(Start, End, [{BucketSize, Bucket, _Packing} | StorageModules])
		when BucketSize * Bucket + BucketSize =< Start ->
	get_cover2(Start, End, StorageModules);
get_cover2(Start, End, [{BucketSize, Bucket, _Packing} = StorageModule | StorageModules]) ->
	Start2 = BucketSize * Bucket,
	End2 = Start2 + BucketSize,
	End3 = min(End, End2),
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

label_test() ->
	?assertEqual("storage_module_0_1",
		label({?PARTITION_SIZE, 0, {spora_2_6, <<"a">>}})),
	?assertEqual("storage_module_0_1",
		label({?PARTITION_SIZE, 0, {spora_2_6, <<"a">>}})),
	?assertEqual("storage_module_2_1",
		label({?PARTITION_SIZE, 2, {spora_2_6, <<"a">>}})),
	?assertEqual("storage_module_0_2",
		label({?PARTITION_SIZE, 0, {spora_2_6, <<"b">>}})),
	?assertEqual("storage_module_524288_3_2",
		label({524288, 3, {spora_2_6, <<"b">>}})),
	?assertEqual("storage_module_2_unpacked",
		label({?PARTITION_SIZE, 2, unpacked})),
	%% force a _ in the encoded address
	?assertEqual("storage_module_2_3",
		label({?PARTITION_SIZE, 2, {spora_2_6, <<"s÷">>}})),
	?assertEqual("storage_module_524288_2_3",
		label({524288, 2, {spora_2_6, <<"s÷">>}})),
	?assertEqual("storage_module_524288_3_4",
		label({524288, 3, {composite, <<"b">>, 1}})),
	?assertEqual("storage_module_524288_3_4",
		label({524288, 3, {composite, <<"b">>, 1}})),
	?assertEqual("storage_module_524288_3_5",
		label({524288, 3, {composite, <<"b">>, 2}})).

has_any_test() ->
	?assertEqual(false, has_any(0, [])),
	?assertEqual(false, has_any(0, [{10, 1, p}])),
	?assertEqual(false, has_any(10, [{10, 1, p}])),
	?assertEqual(true, has_any(11, [{10, 1, p}])),
	?assertEqual(true, has_any(11, [{10, 1, {replica_2_9, a}}])),
	?assertEqual(true, has_any(20 + ?OVERLAP, [{10, 1, p}])),
	?assertEqual(true, has_any(20 + ?OVERLAP, [{10, 1, {replica_2_9, a}}])).

get_unique_sorted_intervals_test() ->
	?assertEqual([{0, 24}, {90, 120}],
			get_unique_sorted_intervals([{10, 0, p}, {30, 3, p}, {20, 0, p}, {12, 1, p}])).

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
	?assertEqual([{1, 0, p}], sort_storage_modules_by_left_bound([{1, 0, p}], none)),
	?assertEqual([{10, 0, p}, {10, 1, p}, {10, 2, p}],
			sort_storage_modules_by_left_bound([{10, 1, p}, {10, 0, p}, {10, 2, p}], none)),
	?assertEqual([{10, 0, p}, {7, 1, p}, {10, 1, p}, {10, 2, p}],
			sort_storage_modules_by_left_bound([{10, 1, p}, {10, 0, p}, {10, 2, p},
					{7, 1, p}], none)),
	?assertEqual([{10, 0, p}, {10, 1, p}, {10, 1, p2}],
			sort_storage_modules_by_left_bound([{10, 1, p}, {10, 0, p}, {10, 1, p2}], none)),
	?assertEqual([{10, 0, p}, {10, 1, p2}, {10, 1, p}],
			sort_storage_modules_by_left_bound([{10, 1, p}, {10, 0, p}, {10, 1, p2}],
					"storage_module_10_1_p2")).

get_cover2_test() ->
	?assertEqual(not_found, get_cover2(0, 1, [])),
	?assertEqual([{0, 1, "storage_module_1_0_p"}], get_cover2(0, 1, [{1, 0, p}])),
	?assertEqual([{0, 1, "storage_module_1_0_p"}, {1, 2, "storage_module_1_1_p"}],
			get_cover2(0, 2, [{1, 0, p}, {1, 1, p}])),
	?assertEqual(not_found, get_cover2(0, 2, [{1, 0, p}, {1, 2, p}])),
	?assertEqual([{0, 2, "storage_module_2_0_p"}],
			get_cover2(0, 2, [{2, 0, p}, {1, 0, p}])),
	?assertEqual([{0, 2, "storage_module_2_0_p"}, {2, 3, "storage_module_3_0_p"}],
			get_cover2(0, 3, [{2, 0, p}, {3, 0, p}])).
