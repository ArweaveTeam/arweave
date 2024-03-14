-module(ar_storage_module).

-export([id/1, label/1, address_label/1, label_by_id/1,
		get_by_id/1, get_range/1, get_packing/1, get_size/1, get/2, get_all/1, get_all/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").

%% The overlap makes sure a 100 MiB recall range can always be fetched
%% from a single storage module.
-ifdef(DEBUG).
-define(OVERLAP, (1024 * 1024)).
-else.
-define(OVERLAP, (?RECALL_RANGE_SIZE)).
-endif.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return the storage module identifier.
id({BucketSize, Bucket, Packing}) when BucketSize == ?PARTITION_SIZE ->
	PackingString =
		case Packing of
			{spora_2_6, Addr} ->
				ar_util:encode(Addr);
			_ ->
				atom_to_list(Packing)
		end,
	binary_to_list(iolist_to_binary(io_lib:format("storage_module_~B_~s",
			[Bucket, PackingString])));
id({BucketSize, Bucket, Packing}) ->
	PackingString =
		case Packing of
			{spora_2_6, Addr} ->
				ar_util:encode(Addr);
			_ ->
				atom_to_list(Packing)
		end,
	binary_to_list(iolist_to_binary(io_lib:format("storage_module_~B_~B_~s",
			[BucketSize, Bucket, PackingString]))).

%% @doc Return the obscure unique label for the given storage module.
label({_BucketSize, _Bucket, Packing} = StorageModule) ->
	StoreID = ar_storage_module:id(StorageModule),
	case ets:lookup(?MODULE, {label, StoreID}) of
		[] ->
			PackingLabel =
				case Packing of
					{spora_2_6, Addr} ->
						ar_storage_module:address_label(Addr);
					_ ->
						atom_to_list(Packing)
				end,
			Label =
				case StoreID of
					"default" ->
						"default";
					_ ->
						Parts = binary:split(list_to_binary(StoreID), <<"_">>, [global]),
						Tail = tl(lists:reverse(Parts)),
						Parts2 = lists:reverse([list_to_binary(PackingLabel) | Tail]),
						Parts3 = [binary_to_list(El) || El <- Parts2],
						string:join(Parts3, "_")
				end,
			ets:insert(?MODULE, {{label, StoreID}, Label}),
			Label;
		[{_, Label}] ->
			Label
	end.

%% @doc Return the obscure unique label for the given packing address.
address_label(Addr) ->
	case ets:lookup(?MODULE, {address_label, Addr}) of
		[] ->
			Label =
				case ets:lookup(?MODULE, last_address_label) of
					[] ->
						1;
					[{_, Counter}] ->
						Counter + 1
				end,
			ets:insert(?MODULE, {{address_label, Addr}, Label}),
			ets:insert(?MODULE, {last_address_label, Label}),
			integer_to_list(Label);
		[{_, Label}] ->
			integer_to_list(Label)
	end.

%% @doc Return the obscure unique label for the given store ID.
label_by_id("default") ->
	"default";
label_by_id(StoreID) ->
	M = get_by_id(StoreID),
	label(M).

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

%% @doc Return {StartOffset, EndOffset} the given module is responsible for.
get_range(ID) ->
	{ok, Config} = application:get_env(arweave, config),
	get_range(ID, Config#config.storage_modules).

get_range(ID, [Module | Modules]) ->
	case ar_storage_module:id(Module) == ID of
		true ->
			{BucketSize, Bucket, _Packing} = Module,
			{BucketSize * Bucket, (Bucket + 1) * BucketSize + ?OVERLAP};
		false ->
			get_range(ID, Modules)
	end.

%% @doc Return the packing configured for the given module.
get_packing(ID) ->
	{ok, Config} = application:get_env(arweave, config),
	get_packing(ID, Config#config.storage_modules).

get_packing(ID, [Module | Modules]) ->
	case ar_storage_module:id(Module) == ID of
		true ->
			{_BucketSize, _Bucket, Packing} = Module,
			Packing;
		false ->
			get_packing(ID, Modules)
	end.

%% @doc Return the bucket size configured for the given module.
get_size(ID) ->
	{ok, Config} = application:get_env(arweave, config),
	get_size(ID, Config#config.storage_modules).

get_size(ID, [Module | Modules]) ->
	case ar_storage_module:id(Module) == ID of
		true ->
			{BucketSize, _Bucket, _Packing} = Module,
			BucketSize;
		false ->
			get_size(ID, Modules)
	end.

%% @doc Return a configured storage module covering the given Offset, preferably
%% with the given Packing. Return not_found if none is found.
get(Offset, Packing) ->
	{ok, Config} = application:get_env(arweave, config),
	get(Offset, Packing, Config#config.storage_modules, not_found).

%% @doc Return the list of all configured storage modules covering the given Offset.
get_all(Offset) ->
	{ok, Config} = application:get_env(arweave, config),
	get_all(Offset, Config#config.storage_modules, []).

%% @doc Return the list of configured storage modules whose ranges intersect
%% the given interval.
get_all(Start, End) ->
	{ok, Config} = application:get_env(arweave, config),
	get_all(Start, End, Config#config.storage_modules, []).

%%%===================================================================
%%% Private functions.
%%%===================================================================

get(Offset, Packing, [{BucketSize, Bucket, _Packing} | StorageModules], StorageModule)
		when Offset =< BucketSize * Bucket
				orelse Offset > BucketSize * (Bucket + 1) + ?OVERLAP ->
	get(Offset, Packing, StorageModules, StorageModule);
get(_Offset, Packing, [{BucketSize, Bucket, Packing} | _StorageModules], _StorageModule) ->
	{BucketSize, Bucket, Packing};
get(Offset, _Packing, [{BucketSize, Bucket, Packing} | StorageModules], _StorageModule) ->
	get(Offset, Packing, StorageModules, {BucketSize, Bucket, Packing});
get(_Offset, _Packing, [], StorageModule) ->
	StorageModule.

get_all(Offset, [{BucketSize, Bucket, _Packing} | StorageModules], FoundModules)
		when Offset =< BucketSize * Bucket
				orelse Offset > BucketSize * (Bucket + 1) + ?OVERLAP ->
	get_all(Offset, StorageModules, FoundModules);
get_all(Offset, [StorageModule | StorageModules], FoundModules) ->
	get_all(Offset, StorageModules, [StorageModule | FoundModules]);
get_all(_Offset, [], FoundModules) ->
	FoundModules.

get_all(Start, End, [{BucketSize, Bucket, _Packing} | StorageModules], FoundModules)
		when End =< BucketSize * Bucket
			orelse Start >= BucketSize * (Bucket + 1) + ?OVERLAP ->
	get_all(Start, End, StorageModules, FoundModules);
get_all(Start, End, [StorageModule | StorageModules], FoundModules) ->
	get_all(Start, End, StorageModules, [StorageModule | FoundModules]);
get_all(_Start, _End, [], FoundModules) ->
	FoundModules.
