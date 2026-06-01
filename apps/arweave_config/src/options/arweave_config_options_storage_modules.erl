%%% @doc Specs for the `storage_modules` option group. Options for
%%% declaring what weave data the node stores and how it is encoded.
%%%
%%% Storage modules are canonically stored as `[storage_modules]`, a
%%% list of maps. Leaf specs use `{list_item}` to declare the fields
%%% available inside each list element.
%%%
%%% Shape per module:
%%%
%%%   [storage_modules, {list_item}, partition] :: non_neg_integer
%%%   [storage_modules, {list_item}, range_start] :: non_neg_integer
%%%   [storage_modules, {list_item}, range_end] :: pos_integer
%%%   [storage_modules, {list_item}, packing_format] :: unpacked | spora_2_6 | replica_2_9
%%%   [storage_modules, {list_item}, packing_address] :: 32-byte binary
%%%   [storage_modules, {list_item}, defrag] :: boolean
%%%
%%% A module has either `partition` or explicit `range_start` and
%%% `range_end`.
%%% Repack-in-place modules live in their own `[repack_modules]`
%%% list value.
%%%
%%% Accepted packing formats: `unpacked`, `spora_2_6`, and `replica_2_9`.
-module(arweave_config_options_storage_modules).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-export([
	storage_module_to_config/1,
	config_to_storage_module/1,
	legacy_list/0,
	legacy_defrags/0,
	write_legacy_storage_module/1,
	write_legacy_list/1,
	write_legacy_defrags/1
]).
-include("arweave_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

specs() ->
	[
		#{
			enabled => true,
			option_key => [storage_modules],
			type => list_map,
			default => [],
			short_description =>
				<<"Storage module declarations.">>
		},
		#{
			enabled => true,
			option_key => [storage_modules, {list_item}, partition],
			type => pos_integer,
			short_description =>
				<<"Partition number this storage module covers.">>,
			long_description =>
				<<"A storage module is responsible for syncing and "
				  "storing a particular data range. The partition shorthand "
				  "uses the default partition size; for a custom-sized "
				  "range, set `range_start` and `range_end` explicitly "
				  "instead. Mutually exclusive with those range fields.">>
		},
		#{
			enabled => true,
			option_key => [storage_modules, {list_item}, range_start],
			type => pos_integer,
			short_description =>
				<<"Inclusive start byte offset of the storage module's "
				  "range.">>,
			long_description =>
				<<"Use range to size a storage module to a specific byte "
				  "range. Make sure the corresponding disk has about 10% "
				  "extra space for proofs and other metadata. Required "
				  "when `partition` is not set.">>
		},
		#{
			enabled => true,
			option_key => [storage_modules, {list_item}, range_end],
			type => pos_integer,
			short_description =>
				<<"Exclusive end byte offset of the storage module's "
				  "range.">>,
			long_description =>
				<<"Required when `partition` is not set. See "
				  "`range_start` for sizing guidance.">>
		},
		#{
			enabled => true,
			option_key => [storage_modules, {list_item}, packing_format],
			type => atom,
			short_description =>
				<<"Packing format used to pack the stored data: "
				  "unpacked, spora_2_6, or replica_2_9.">>
		},
		#{
			enabled => true,
			option_key => [storage_modules, {list_item}, packing_address],
			type => address,
			short_description =>
				<<"Mining address the data is packed for.">>,
			long_description =>
				<<"Required unless `packing_format` is `unpacked`. "
				  "Data already packed with different addresses is not "
				  "repacked automatically.">>
		},
		#{
			enabled => true,
			option_key => [storage_modules, {list_item}, defrag],
			default => false,
			type => boolean,
			short_description =>
				<<"Run defragmentation of this module's chunk storage "
				  "files at startup.">>,
			long_description =>
				<<"Defragmentation rewrites the chunk storage files "
				  "more contiguously. Requires `run_defragmentation = "
				  "true` as the master switch. After defragmentation "
				  "completes, the node continues normally with the "
				  "module declared as a regular storage module.">>
		}
	].

group_description() ->
	<<"Define and manage storage modules.">>.

%% @doc Convert the canonical list of maps back into the legacy tuple
%% list shape.
-spec legacy_list() -> [term()].
legacy_list() ->
	case arweave_config_store:get([storage_modules]) of
		{ok, Modules} when is_list(Modules) ->
			[config_to_storage_module(Module) || Module <- Modules];
		_ ->
			[]
	end.

%% @doc Convert canonical maps that have `defrag => true` back into
%% the legacy defragmentation tuple list.
-spec legacy_defrags() -> [term()].
legacy_defrags() ->
	case arweave_config_store:get([storage_modules]) of
		{ok, Modules} when is_list(Modules) ->
			[config_to_storage_module(Module) || Module <- Modules,
				maps:get(defrag, Module, false) =:= true];
		_ ->
			[]
	end.

%% @doc Take the legacy tuple list and write the canonical
%% `[storage_modules]' list of maps.
-spec write_legacy_list([term()]) -> ok.
write_legacy_list(L) when is_list(L) ->
	ExistingDefrags = legacy_defrags(),
	write_modules(lists:usort(L ++ ExistingDefrags)),
	ok.

write_legacy_storage_module({_BucketSize, _Bucket, _Packing} = Tuple) ->
	write_modules(lists:usort([Tuple | legacy_list()])),
	ok.

%% @doc Take the legacy defragmentation_modules list and set
%% `defrag => true` on matching storage module maps.
%% entries. Modules that aren't already present (defrag-only modules
%% in legacy parlance) are synthesized with `packing.*` and the
%% appropriate range attribute populated from the legacy tuple.
-spec write_legacy_defrags([term()]) -> ok.
write_legacy_defrags(L) when is_list(L) ->
	DefragSet = sets:from_list(L),
	Existing = legacy_list(),
	Combined = lists:usort(Existing ++ L),
	write_module_maps([
		Map#{defrag => sets:is_element(Tuple, DefragSet)}
		|| Tuple <- Combined,
		   Map <- [storage_module_to_config(Tuple)]
	]),
	ok.

write_modules(Modules) ->
	write_module_maps([storage_module_to_config(Module) || Module <- Modules]).

write_module_maps(Modules) ->
	_ = arweave_config_store:delete_prefix([storage_modules]),
	set_local([storage_modules], Modules),
	ok.

set_local(Key, Value) ->
	_ = arweave_config_options_registry:set_local(Key, Value),
	ok.

%% Internal read helpers.

-spec storage_module_to_config(map() | {pos_integer(), non_neg_integer(), term()}) ->
	map().
storage_module_to_config(Module) when is_map(Module) ->
	Module;
storage_module_to_config({BucketSize, Bucket, Packing}) ->
	RangeAttrs = case BucketSize =:= ?PARTITION_SIZE of
		true ->
			#{partition => Bucket};
		false ->
			Start = Bucket * BucketSize,
			#{range_start => Start, range_end => Start + BucketSize}
	end,
	(maps:merge(RangeAttrs, packing_map(Packing)))#{defrag => false}.

-spec config_to_storage_module(map()) -> {pos_integer(), non_neg_integer(), term()}.
config_to_storage_module(Module) ->
	{BucketSize, Bucket} = range_from_map(Module),
	Packing = packing_from_map(Module),
	{BucketSize, Bucket, Packing}.

range_from_map(#{partition := Bucket}) ->
	{?PARTITION_SIZE, Bucket};
range_from_map(#{range_start := Start, range_end := End}) ->
	BucketSize = End - Start,
	Bucket = case BucketSize of
		0 -> 0;
		_ -> Start div BucketSize
	end,
	{BucketSize, Bucket}.

packing_map(unpacked) ->
	#{packing_format => unpacked};
packing_map({Format, Addr}) when Format =:= spora_2_6; Format =:= replica_2_9 ->
	#{packing_format => Format, packing_address => Addr}.

packing_from_map(#{packing_format := unpacked}) ->
	unpacked;
packing_from_map(#{packing_format := Format, packing_address := Addr})
		when Format =:= spora_2_6; Format =:= replica_2_9 ->
	{Format, Addr}.

%% Storage-module-set validator.

-spec validate() -> ok | {error, binary()}.
validate() ->
	case validate_shape() of
		ok ->
			case validate_no_duplicates() of
				ok -> validate_unique_replication_type();
				{error, _} = Err -> Err
			end;
		{error, _} = Err ->
			Err
	end.

validate_shape() ->
	case arweave_config_store:get([storage_modules]) of
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
		Tuple = config_to_storage_module(Module),
		validate_tuple_shape(Tuple)
	catch
		_:_ ->
			{error, <<"storage_modules: invalid module shape">>}
	end.

validate_tuple_shape({BucketSize, _Bucket, Packing})
		when is_integer(BucketSize), BucketSize >= 0 ->
	validate_tuple_packing(Packing);
validate_tuple_shape(_) ->
	{error, <<"storage_modules: invalid module range">>}.

validate_tuple_packing(unpacked) ->
	ok;
validate_tuple_packing({Format, Addr})
		when (Format =:= spora_2_6 orelse Format =:= replica_2_9),
		     is_binary(Addr), byte_size(Addr) =:= 32 ->
ok;
validate_tuple_packing(_) ->
	{error, <<"storage_modules: invalid packing">>}.

validate_no_duplicates() ->
	Modules = legacy_list(),
	case length(Modules) =:= length(lists:usort(Modules)) of
		true ->
			ok;
		false ->
			{error, <<"Duplicate value detected in the storage_modules option.">>}
	end.

%% @doc Cross-cutting: also reads [mining, enabled] and [mining, address].
validate_unique_replication_type() ->
	case arweave_config:get([mining, enabled]) of
		true ->
			MiningAddr = arweave_config:get([mining, address]),
			Unique = lists:foldl(
				fun({_, _, {spora_2_6, Addr}}, Acc) when Addr =:= MiningAddr ->
					sets:add_element(spora_2_6, Acc);
				({_, _, {replica_2_9, Addr}}, Acc) when Addr =:= MiningAddr ->
					sets:add_element(replica_2_9, Acc);
				(_, Acc) ->
					Acc
				end,
				sets:new(),
				legacy_list()
			),
			case sets:size(Unique) =< 1 of
				true ->
					ok;
				false ->
					{error, <<"The node cannot mine multiple replication types "
							"for the same mining address.">>}
			end;
		_ ->
			ok
	end.
