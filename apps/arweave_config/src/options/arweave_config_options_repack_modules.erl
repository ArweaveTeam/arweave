%%% @doc Specs for the `repack_modules` option group. Options for
%%% declaring storage ranges that should be repacked in place.
%%%
%%% Repack modules are canonically stored as `[repack_modules]`, a
%%% list of maps. Leaf specs use `{list_item}` to declare the fields
%%% available inside each list element.
%%%
%%% The module-level validator enforces each module's shape and rejects
%%% modules that overlap with regular `[storage_modules]` entries.
%%%
%%% Shape per module:
%%%
%%%   [repack_modules, {list_item}, partition] :: non_neg_integer
%%%   [repack_modules, {list_item}, range_start] :: non_neg_integer
%%%   [repack_modules, {list_item}, range_end] :: pos_integer
%%%   [repack_modules, {list_item}, from_format] :: unpacked | spora_2_6 | replica_2_9
%%%   [repack_modules, {list_item}, from_address] :: 32-byte binary
%%%   [repack_modules, {list_item}, to_format] :: unpacked | spora_2_6 | replica_2_9
%%%   [repack_modules, {list_item}, to_address] :: 32-byte binary
%%%
%%% A module has either `partition` or an explicit `range.{start, end}`.
%%% Both `from` and `to` packings are required.
%%%
%%% Only `unpacked`, `spora_2_6`, and `replica_2_9` formats are accepted.
-module(arweave_config_options_repack_modules).
-behaviour(arweave_config_options).
-export([
	specs/0,
	group_description/0,
	repack_module_to_config/1,
	config_to_repack_module/1,
	legacy_list/0,
	write_legacy_repack_module/1,
	write_legacy_list/1,
	validate/0
]).
-include("arweave_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

specs() ->
	[
		#{
			enabled => true,
			option_key => [repack_modules],
			type => list_map,
			default => [],
			short_description =>
				<<"Repack-in-place module declarations.">>
		},
		#{
			enabled => true,
			option_key => [repack_modules, {list_item}, partition],
			type => pos_integer,
			short_description =>
				<<"Partition number this repack module covers.">>,
			long_description =>
				<<"The repack-in-place module operates on the same "
				  "on-disk slot as the source storage module. Use "
				  "`partition` for the default partition size; for a "
				  "custom-sized range, set `range_start` and `range_end` "
				  "instead.">>
		},
		#{
			enabled => true,
			option_key => [repack_modules, {list_item}, range_start],
			type => pos_integer,
			short_description =>
				<<"Inclusive start byte offset of the range to repack.">>,
			long_description =>
				<<"Required when `partition' is not set. The range must "
				  "match the source storage module's on-disk layout.">>
		},
		#{
			enabled => true,
			option_key => [repack_modules, {list_item}, range_end],
			type => pos_integer,
			short_description =>
				<<"Exclusive end byte offset of the range to repack.">>,
			long_description =>
				<<"Required when `partition' is not set.">>
		},
		#{
			enabled => true,
			option_key => [repack_modules, {list_item}, from_format],
			type => atom,
			short_description =>
				<<"Source packing format: unpacked, spora_2_6, or "
				  "replica_2_9.">>
		},
		#{
			enabled => true,
			option_key => [repack_modules, {list_item}, from_address],
			type => address,
			short_description =>
				<<"Source mining address. Required unless "
				  "`from_format = unpacked'.">>
		},
		#{
			enabled => true,
			option_key => [repack_modules, {list_item}, to_format],
			type => atom,
			short_description =>
				<<"Target packing format: unpacked, spora_2_6, or "
				  "replica_2_9.">>,
			long_description =>
				<<"As of 2.9.1 only repack to replica_2_9 is "
				  "supported.">>
		},
		#{
			enabled => true,
			option_key => [repack_modules, {list_item}, to_address],
			type => address,
			short_description =>
				<<"Target mining address. Required unless "
				  "`to_format = unpacked'.">>
		}
	].

group_description() ->
	<<"Define and run repack-in-place modules.">>.

%% @doc Convert the canonical list of maps into the legacy
%% repack-in-place tuple list.
-spec legacy_list() -> [term()].
legacy_list() ->
	case arweave_config_store:get([repack_modules]) of
		{ok, Modules} when is_list(Modules) ->
			[config_to_repack_module(Module) || Module <- Modules];
		_ ->
			[]
	end.

%% @doc Take the legacy repack-in-place list and write each entry's
%% attributes — both the source `from` and the target `to` — into
%% the canonical `[repack_modules]' list of maps.
-spec write_legacy_list([term()]) -> ok.
write_legacy_list(L) when is_list(L) ->
	write_modules(L),
	ok.

write_legacy_repack_module({{_BucketSize, _Bucket, _Packing}, _ToPacking} = Tuple) ->
	write_modules(lists:usort([Tuple | legacy_list()])),
	ok.

set_local(Key, Value) ->
	_ = arweave_config_options_registry:set_local(Key, Value),
	ok.

%% Internal read helpers.

write_modules(Modules) ->
	write_module_maps([repack_module_to_config(Module) || Module <- Modules]).

write_module_maps(Modules) ->
	_ = arweave_config_store:delete_prefix([repack_modules]),
	set_local([repack_modules], Modules),
	ok.

-spec repack_module_to_config(
	{{pos_integer(), non_neg_integer(), term()}, term()}) -> map().
repack_module_to_config({{BucketSize, Bucket, FromPacking}, ToPacking}) ->
	RangeAttrs = case BucketSize =:= ?PARTITION_SIZE of
		true ->
			#{partition => Bucket};
		false ->
			Start = Bucket * BucketSize,
			#{range_start => Start, range_end => Start + BucketSize}
	end,
	maps:merge(
		RangeAttrs,
		maps:merge(
			packing_map(from, FromPacking),
			packing_map(to, ToPacking)
		)
	).

-spec config_to_repack_module(map()) ->
	{{pos_integer(), non_neg_integer(), term()}, term()}.
config_to_repack_module(Module) ->
	{BucketSize, Bucket} = range_from_map(Module),
	From = packing_from_map(from, Module),
	To = packing_from_map(to, Module),
	{{BucketSize, Bucket, From}, To}.

range_from_map(#{partition := Bucket}) ->
	{?PARTITION_SIZE, Bucket};
range_from_map(#{range_start := Start, range_end := End}) ->
	BucketSize = End - Start,
	Bucket = case BucketSize of
		0 -> 0;
		_ -> Start div BucketSize
	end,
	{BucketSize, Bucket}.

packing_map(Prefix, unpacked) ->
	#{
		format_field(Prefix) => unpacked
	};
packing_map(Prefix, {Format, Addr})
		when Format =:= spora_2_6; Format =:= replica_2_9 ->
	#{
		format_field(Prefix) => Format,
		address_field(Prefix) => Addr
	}.

packing_from_map(Prefix, Module) ->
	FormatField = format_field(Prefix),
	AddressField = address_field(Prefix),
	packing_from_map2(maps:get(FormatField, Module), AddressField, Module).

packing_from_map2(unpacked, _AddressField, _Module) ->
	unpacked;
packing_from_map2(Format, AddressField, Module)
		when Format =:= spora_2_6; Format =:= replica_2_9 ->
	Addr = maps:get(AddressField, Module),
	{Format, Addr}.

format_field(from) -> from_format;
format_field(to) -> to_format.

address_field(from) -> from_address;
address_field(to) -> to_address.

%% Module-level validator. Validates each repack module's shape and
%% additionally checks that no repack module overlaps a regular
%% storage module.

-spec validate() -> ok | {error, binary()}.
validate() ->
	case validate_shape() of
		ok -> validate_no_in_place_overlap();
		{error, _} = Err ->
			Err
	end.

validate_shape() ->
	case arweave_config_store:get([repack_modules]) of
		{ok, Modules} when is_list(Modules) ->
			validate_modules(Modules);
		_ ->
			validate_modules([])
	end.

validate_modules([]) ->
	ok;
validate_modules([Module | Rest]) ->
	case validate_module(Module) of
		ok -> validate_modules(Rest);
		{error, _} = Err -> Err
	end.

validate_module(Module) ->
	try
		Tuple = config_to_repack_module(Module),
		validate_tuple_shape(Tuple)
	catch
		_:_ ->
			{error, <<"repack_modules: invalid module shape">>}
	end.

validate_tuple_shape({{BucketSize, _Bucket, From}, To})
		when is_integer(BucketSize), BucketSize >= 0 ->
	case validate_tuple_packing(From) of
		ok -> validate_tuple_packing(To);
		{error, _} = Err -> Err
	end;
validate_tuple_shape(_) ->
	{error, <<"repack_modules: invalid module range">>}.

validate_tuple_packing(unpacked) ->
	ok;
validate_tuple_packing({Format, Addr})
		when (Format =:= spora_2_6 orelse Format =:= replica_2_9),
		     is_binary(Addr), byte_size(Addr) =:= 32 ->
	ok;
validate_tuple_packing(_) ->
	{error, <<"repack_modules: invalid packing">>}.

%% @doc A repack-in-place module must not also be a regular storage
%% module; cross-checks against
%% arweave_config_options_storage_modules:legacy_list/0.
validate_no_in_place_overlap() ->
	StorageIDs =
		[ar_storage_module:id(M)
			|| M <- arweave_config_options_storage_modules:legacy_list()],
	validate_no_in_place_overlap(legacy_list(), StorageIDs).

validate_no_in_place_overlap([], _Modules) ->
	ok;
validate_no_in_place_overlap([{Module, _ToPacking} | L], Modules) ->
	ID = ar_storage_module:id(Module),
	case lists:member(ID, Modules) of
		true ->
			Msg = iolist_to_binary(
				io_lib:format(
					"Cannot use the storage module ~s "
					"while it is being repacked in place.",
					[ID])),
			{error, Msg};
		false ->
			validate_no_in_place_overlap(L, Modules)
	end.
