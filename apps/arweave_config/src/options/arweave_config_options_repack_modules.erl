%%% @doc Specs for the `repack_modules` option group. Options for
%%% declaring storage ranges that should be repacked in place.
%%%
%%% Each repack module is stored under `[repack_modules, ID, ...]`.
%%% The leaves for a module cover
%%% `partition`, `range.{start, end}`, `from.{format, address}`,
%%% `to.{format, address}`. Each leaf reads from
%%% `arweave_config_store` with no per-spec default.
%%%
%%% The module-level validator enforces each module's shape,
%%% derived-id invariants, and rejects ID collisions with the
%%% `[storage_modules, ...]` namespace.
%%%
%%% Shape per module:
%%%
%%%   [repack_modules, <id>, partition] :: non_neg_integer
%%%   [repack_modules, <id>, range, start] :: non_neg_integer
%%%   [repack_modules, <id>, range, end] :: pos_integer
%%%   [repack_modules, <id>, from, format] :: unpacked | spora_2_6 | replica_2_9
%%%   [repack_modules, <id>, from, address] :: 32-byte binary
%%%   [repack_modules, <id>, to, format] :: unpacked | spora_2_6 | replica_2_9
%%%   [repack_modules, <id>, to, address] :: 32-byte binary
%%%
%%% A module has either `partition` or an explicit `range.{start, end}`.
%%% Both `from` and `to` packings are required.
%%%
%%% Only `unpacked`, `spora_2_6`, and `replica_2_9` formats are
%%% accepted; composite and spora_2_5 are rejected by the validator.
-module(arweave_config_options_repack_modules).
-behaviour(arweave_config_options).
-export([
	specs/0,
	group_description/0,
	list/0,
	write_list/1,
	validate/0
]).
-include("arweave_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

specs() ->
	[
		partition_spec(),
		range_start_spec(),
		range_end_spec(),
		from_format_spec(),
		from_address_spec(),
		to_format_spec(),
		to_address_spec()
	].

group_description() ->
	<<"Define and run repack-in-place modules.">>.

%% Per-leaf specs.

partition_spec() ->
	#{
		enabled => true,
		option_key => [repack_modules, {id}, partition],
		type => pos_integer,
		short_description =>
			<<"Partition number this repack module covers.">>,
		long_description =>
			<<"The repack-in-place module operates on the same "
			  "on-disk slot as the source storage module. Use "
			  "`partition` for the default partition size; for a "
			  "custom-sized range, set `range.{start, end}' "
			  "instead. Mutually exclusive with "
			  "`range.{start, end}'.">>,
		handle_get => fun store_only_get/2
	}.

range_start_spec() ->
	#{
		enabled => true,
		option_key => [repack_modules, {id}, range, start],
		type => pos_integer,
		short_description =>
			<<"Inclusive start byte offset of the range to repack.">>,
		long_description =>
			<<"Required when `partition' is not set. The range must "
			  "match the source storage module's on-disk layout.">>,
		handle_get => fun store_only_get/2
	}.

range_end_spec() ->
	#{
		enabled => true,
		option_key => [repack_modules, {id}, range, 'end'],
		type => pos_integer,
		short_description =>
			<<"Exclusive end byte offset of the range to repack.">>,
		long_description =>
			<<"Required when `partition' is not set.">>,
		handle_get => fun store_only_get/2
	}.

from_format_spec() ->
	#{
		enabled => true,
		option_key => [repack_modules, {id}, from, format],
		type => atom,
		short_description =>
			<<"Source packing format: unpacked, spora_2_6, or "
			  "replica_2_9.">>,
		handle_get => fun store_only_get/2
	}.

from_address_spec() ->
	#{
		enabled => true,
		option_key => [repack_modules, {id}, from, address],
		short_description =>
			<<"Source mining address. Required unless "
			  "`from.format = unpacked'.">>,
		handle_get => fun store_only_get/2
	}.

to_format_spec() ->
	#{
		enabled => true,
		option_key => [repack_modules, {id}, to, format],
		type => atom,
		short_description =>
			<<"Target packing format: unpacked, spora_2_6, or "
			  "replica_2_9.">>,
		long_description =>
			<<"As of 2.9.1 only repack to replica_2_9 is "
			  "supported.">>,
		handle_get => fun store_only_get/2
	}.

to_address_spec() ->
	#{
		enabled => true,
		option_key => [repack_modules, {id}, to, address],
		short_description =>
			<<"Target mining address. Required unless "
			  "`to.format = unpacked'.">>,
		handle_get => fun store_only_get/2
	}.

%% @doc Store-only reader for `[repack_modules, ID, ...]` leaves:
%% returns the stored value with no fallback default. Repack modules
%% only exist when explicitly configured.
store_only_get(Option, _S) ->
	case arweave_config_store:get(Option) of
		{ok, V} -> {ok, V};
		_ -> {error, not_found}
	end.

%% @doc Aggregate `[repack_modules, <id>, ...]` entries back into the
%% legacy repack-in-place tuple list.
-spec list() -> [term()].
list() ->
	[tuple_for(ID) || ID <- module_ids()].

%% @doc Take the legacy repack-in-place list and write each entry's
%% attributes — both the source `from` and the target `to` — into
%% `[repack_modules, <id>, ...]`.
-spec write_list([term()]) -> ok.
write_list(L) when is_list(L) ->
	clear_all(),
	lists:foreach(fun write_one/1, L),
	ok.

write_one({{_BS, _B, _Packing} = Source, ToPacking}) ->
	ID = arweave_config_options_storage_modules:derived_id(Source),
	write_range_attrs(ID, Source),
	write_from(ID, Source),
	write_to(ID, ToPacking),
	ok.

write_range_attrs(ID, {BucketSize, Bucket, _Packing})
		when BucketSize =:= ?PARTITION_SIZE ->
	set_local([repack_modules, ID, partition], Bucket);
write_range_attrs(ID, {BucketSize, Bucket, _Packing}) ->
	Start = Bucket * BucketSize,
	End = Start + BucketSize,
	set_local([repack_modules, ID, range, start], Start),
	set_local([repack_modules, ID, range, 'end'], End).

write_from(ID, {_BS, _B, unpacked}) ->
	set_local([repack_modules, ID, from, format], unpacked);
write_from(ID, {_BS, _B, {Format, Addr}})
		when Format =:= spora_2_6; Format =:= replica_2_9 ->
	set_local([repack_modules, ID, from, format], Format),
	set_local([repack_modules, ID, from, address], Addr).

write_to(ID, unpacked) ->
	set_local([repack_modules, ID, to, format], unpacked);
write_to(ID, {Format, Addr})
		when Format =:= spora_2_6; Format =:= replica_2_9 ->
	set_local([repack_modules, ID, to, format], Format),
	set_local([repack_modules, ID, to, address], Addr).

set_local(Key, Value) ->
	_ = arweave_config_options_registry:set_local(Key, Value),
	ok.

%% Internal read helpers.

module_ids() ->
	Items = arweave_config:get_all_with_prefix([repack_modules]),
	lists:usort([ID || {[repack_modules, ID | _], _Value} <- Items]).

tuple_for(ID) ->
	Source = source_tuple_for(ID),
	To = read_packing_for(ID, to),
	{Source, To}.

source_tuple_for(ID) ->
	{BucketSize, Bucket} = read_range_for(ID),
	From = read_packing_for(ID, from),
	{BucketSize, Bucket, From}.

read_range_for(ID) ->
	case arweave_config_store:get([repack_modules, ID, partition]) of
		{ok, Bucket} ->
			{?PARTITION_SIZE, Bucket};
		_ ->
			{ok, Start} = arweave_config_store:get(
				[repack_modules, ID, range, start]),
			{ok, End} = arweave_config_store:get(
				[repack_modules, ID, range, 'end']),
			BucketSize = End - Start,
			Bucket = case BucketSize of
				0 -> 0;
				_ -> Start div BucketSize
			end,
			{BucketSize, Bucket}
	end.

read_packing_for(ID, Sub) ->
	case arweave_config_store:get([repack_modules, ID, Sub, format]) of
		{ok, unpacked} ->
			unpacked;
		{ok, Format} when Format =:= spora_2_6; Format =:= replica_2_9 ->
			{ok, Addr} = arweave_config_store:get(
				[repack_modules, ID, Sub, address]),
			{Format, Addr}
	end.

%% The repack bridge owns the entire [repack_modules] namespace, so a
%% rewrite drops everything before re-fanning.
clear_all() ->
	[drop_module(ID) || ID <- module_ids()],
	ok.

drop_module(ID) ->
	Items = arweave_config:get_all_with_prefix([repack_modules, ID]),
	[arweave_config_store:delete(Key) || {Key, _Value} <- Items],
	ok.

%% Module-level validator. Validates each repack module's shape and
%% additionally checks that no repack module's ID collides with a
%% storage_modules entry — a single StoreID can't be both repacked and
%% used as a regular sync target.

-spec validate() -> ok | {error, binary()}.
validate() ->
	IDs = module_ids(),
	case validate_each(IDs) of
		ok ->
			case validate_no_collision(IDs) of
				ok -> validate_no_in_place_overlap();
				{error, _} = Err -> Err
			end;
		{error, _} = Err ->
			Err
	end.

%% @doc Cross-cutting: also reads arweave_config_options_storage_modules:list/0.
%% A repack-in-place module must not also be a regular storage module.
validate_no_in_place_overlap() ->
	StorageIDs =
		[ar_storage_module:id(M)
			|| M <- arweave_config_options_storage_modules:list()],
	validate_no_in_place_overlap(list(), StorageIDs).

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

validate_each([]) ->
	ok;
validate_each([ID | Rest]) ->
	case validate_one(ID) of
		ok -> validate_each(Rest);
		{error, _} = Err -> Err
	end.

validate_one(ID) ->
	with_steps(ID, [
		fun validate_range_shape/1,
		fun validate_from/1,
		fun validate_to/1,
		fun validate_derived_id/1
	]).

with_steps(_ID, []) ->
	ok;
with_steps(ID, [Step | Rest]) ->
	case Step(ID) of
		ok -> with_steps(ID, Rest);
		{error, _} = Err -> Err
	end.

validate_range_shape(ID) ->
	HasPartition = is_set([repack_modules, ID, partition]),
	HasRangeStart = is_set([repack_modules, ID, range, start]),
	HasRangeEnd = is_set([repack_modules, ID, range, 'end']),
	case {HasPartition, HasRangeStart, HasRangeEnd} of
		{true,  false, false} -> ok;
		{false, true,  true} -> ok;
		{true,  _,     _} ->
			err(<<"partition and range are mutually exclusive">>, ID);
		{false, false, false} ->
			err(<<"missing partition or range">>, ID);
		_ ->
			err(<<"range requires both start and end">>, ID)
	end.

validate_from(ID) -> validate_packing_subnamespace(ID, from).
validate_to(ID) -> validate_packing_subnamespace(ID, to).

validate_packing_subnamespace(ID, Sub) ->
	case arweave_config_store:get([repack_modules, ID, Sub, format]) of
		{ok, unpacked} ->
			ok;
		{ok, Format} when Format =:= spora_2_6; Format =:= replica_2_9 ->
			case arweave_config_store:get([repack_modules, ID, Sub, address]) of
				{ok, A} when is_binary(A), byte_size(A) =:= 32 -> ok;
				{ok, _} ->
					err_sub(<<"address must be a 32-byte binary">>, ID, Sub);
				_ ->
					err_sub(<<"format requires address">>, ID, Sub)
			end;
		{ok, _} ->
			err_sub(<<"unsupported format">>, ID, Sub);
		_ ->
			err_sub(<<"missing format">>, ID, Sub)
	end.

validate_derived_id(ID) ->
	Source = source_tuple_for(ID),
	Expected = arweave_config_options_storage_modules:derived_id(Source),
	case ID =:= Expected of
		true -> ok;
		false ->
			err(<<"derived id mismatch (expected ", Expected/binary, ")">>, ID)
	end.

%% @doc Cross-namespace check: a repack module's ID cannot also exist in
%% the storage_modules namespace — the workers would fight over the
%% same on-disk slot.
validate_no_collision(IDs) ->
	StorageItems = arweave_config:get_all_with_prefix([storage_modules]),
	StorageIDs = ordsets:from_list(
		[ID || {[storage_modules, ID | _], _Value} <- StorageItems]),
	Collisions = [ID || ID <- IDs, ordsets:is_element(ID, StorageIDs)],
	case Collisions of
		[] -> ok;
		[ID | _] ->
			err(<<"id collides with [storage_modules, ...]; "
			      "a module cannot be both repacked and synced">>, ID)
	end.

is_set(Key) ->
	case arweave_config_store:get(Key) of
		{ok, _} -> true;
		_ -> false
	end.

err(What, ID) when is_binary(What) ->
	{error, <<"repack_modules: ", What/binary, " (",
	          (io_format_id(ID))/binary, ")">>}.

err_sub(What, ID, Sub) when is_binary(What), is_atom(Sub) ->
	{error, <<"repack_modules: ", What/binary, " (",
	          (io_format_id(ID))/binary, "/",
	          (atom_to_binary(Sub))/binary, ")">>}.

io_format_id(ID) when is_binary(ID) -> ID;
io_format_id(ID) -> list_to_binary(io_lib:format("~p", [ID])).
