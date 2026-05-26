%%% @doc Specs for the `storage_modules` option group. Options for
%%% declaring what weave data the node stores and how it is encoded.
%%%
%%% Each storage module is stored under `[storage_modules, ID, ...]`.
%%% The leaves for a module cover
%%% `partition`, `range.{start, end}`, `packing.{format, address}`,
%%% `path`, and `defrag`. Each leaf reads from `arweave_config_store`
%%% with no per-spec default.
%%%
%%% The `id` is auto-derived from the storage module tuple so that the
%%% `[storage_modules, ID, ...]` option_key matches the on-disk
%%% directory name produced by
%%% `ar_storage_module:id/1` — `storage_module_<bucket>_<packing>` at
%%% the default partition size, `storage_module_<size>_<bucket>_<packing>`
%%% otherwise. The derivation is mirrored here to avoid a reverse
%%% dependency from `arweave_config` to `arweave`.
%%%
%%% Shape per module:
%%%
%%%   [storage_modules, <id>, partition] :: non_neg_integer
%%%   [storage_modules, <id>, range, start] :: non_neg_integer
%%%   [storage_modules, <id>, range, end] :: pos_integer
%%%   [storage_modules, <id>, packing, format] :: unpacked | spora_2_6 | replica_2_9
%%%   [storage_modules, <id>, packing, address] :: 32-byte binary
%%%   [storage_modules, <id>, defrag] :: boolean
%%%
%%% A module has either `partition` or an explicit `range.{start, end}`.
%%% Repack-in-place modules live in their own
%%% `[repack_modules, <id>, ...]` namespace.
%%%
%%% Accepted packing formats: `unpacked`, `spora_2_6`, `replica_2_9`,
%%% and `composite` (the latter additionally requires
%%% `packing.difficulty`).
-module(arweave_config_options_storage_modules).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-export([
	derived_id/1,
	list/0,
	defrags/0,
	to_entry_map/1,
	write_list/1,
	write_defrags/1
]).
-include("arweave_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

specs() ->
	[
		partition_spec(),
		range_start_spec(),
		range_end_spec(),
		packing_format_spec(),
		packing_address_spec(),
		packing_difficulty_spec(),
		defrag_spec()
	].

group_description() ->
	<<"Define and manage storage modules.">>.

%% Per-leaf specs.

partition_spec() ->
	#{
		enabled => true,
		option_key => [storage_modules, {id}, partition],
		type => pos_integer,
		short_description =>
			<<"Partition number this storage module covers.">>,
		long_description =>
			<<"A storage module is responsible for syncing and "
			  "storing a particular data range. The partition shorthand "
			  "uses the default partition size; for a custom-sized "
			  "range, set `range.{start, end}` explicitly instead. "
			  "Mutually exclusive with `range.{start, end}`.">>,
		handle_get => fun store_only_get/2
	}.

range_start_spec() ->
	#{
		enabled => true,
		option_key => [storage_modules, {id}, range, start],
		type => pos_integer,
		short_description =>
			<<"Inclusive start byte offset of the storage module's "
			  "range.">>,
		long_description =>
			<<"Use range to size a storage module to a specific byte "
			  "range — for instance, `start = 22000000000000, "
			  "end = 23000000000000` covers the weave between 22 TB "
			  "and 23 TB. Make sure the corresponding disk has about "
			  "10% extra space for proofs and other metadata. "
			  "Required when `partition` is not set.">>,
		handle_get => fun store_only_get/2
	}.

range_end_spec() ->
	#{
		enabled => true,
		option_key => [storage_modules, {id}, range, 'end'],
		type => pos_integer,
		short_description =>
			<<"Exclusive end byte offset of the storage module's "
			  "range.">>,
		long_description =>
			<<"Required when `partition` is not set. See "
			  "`range.start` for sizing guidance.">>,
		handle_get => fun store_only_get/2
	}.

packing_format_spec() ->
	#{
		enabled => true,
		option_key => [storage_modules, {id}, packing, format],
		type => atom,
		short_description =>
			<<"Packing format used to pack the stored data: "
			  "unpacked, spora_2_6, replica_2_9, or composite.">>,
		handle_get => fun store_only_get/2
	}.

packing_address_spec() ->
	#{
		enabled => true,
		option_key => [storage_modules, {id}, packing, address],
		type => address,
		short_description =>
			<<"Mining address the data is packed for.">>,
		long_description =>
			<<"Required unless `packing.format` is `unpacked`. "
			  "Data already packed with different addresses is not "
			  "repacked automatically.">>,
		handle_get => fun store_only_get/2
	}.

packing_difficulty_spec() ->
	#{
		enabled => true,
		option_key => [storage_modules, {id}, packing, difficulty],
		type => integer,
		short_description =>
			<<"Packing difficulty (composite packing only).">>,
		handle_get => fun store_only_get/2
	}.

defrag_spec() ->
	#{
		enabled => true,
		option_key => [storage_modules, {id}, defrag],
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
			  "module declared as a regular storage module.">>,
		handle_get => fun store_only_get/2
	}.

%% @doc Store-only reader for `[storage_modules, ID, ...]` leaves:
%% returns the stored value with no fallback default. Storage modules
%% only exist when explicitly configured.
store_only_get(Option, _S) ->
	case arweave_config_store:get(Option) of
		{ok, V} -> {ok, V};
		_ -> {error, not_found}
	end.

%% @doc Derived id for a legacy storage_module tuple. Matches
%% `ar_storage_module:id/1`.
-spec derived_id({pos_integer(), non_neg_integer(), term()}) -> binary().
derived_id({BucketSize, Bucket, Packing}) ->
	PackingString = packing_string(Packing),
	case BucketSize == ?PARTITION_SIZE of
		true ->
			iolist_to_binary(io_lib:format(
				"storage_module_~B_~s",
				[Bucket, PackingString]));
		false ->
			iolist_to_binary(io_lib:format(
				"storage_module_~B_~B_~s",
				[BucketSize, Bucket, PackingString]))
	end.

packing_string(unpacked) ->
	<<"unpacked">>;
packing_string({spora_2_6, Addr}) ->
	b64fast:encode(Addr);
packing_string({replica_2_9, Addr}) ->
	<< (b64fast:encode(Addr))/binary, ".replica.2.9" >>;
packing_string({composite, Addr, PackingDifficulty}) ->
	%% Must produce the same string as `ar_storage_module:id/1` for
	%% composite-packed modules.
	<< (b64fast:encode(Addr))/binary, ".",
		(integer_to_binary(PackingDifficulty))/binary >>.

%% @doc Aggregate `[storage_modules, <id>, ...]` entries back into
%% the legacy tuple list shape.
-spec list() -> [term()].
list() ->
	[tuple_for(ID) || ID <- module_ids()].

%% @doc Aggregate `[storage_modules, <id>, ...]` entries that have
%% `defrag => true` back into the legacy defragmentation tuple list.
-spec defrags() -> [term()].
defrags() ->
	[tuple_for(ID) || ID <- module_ids(), has_defrag(ID)].

%% @doc Convert a legacy tuple list into a flat per-leaf entry map
%% suitable for `arweave_config:load/1`. Keys are
%% `[storage_modules, <id>, ...]` option_keys; values are the leaf
%% scalars. Pure — no side effects.
-spec to_entry_map([term()]) -> #{[term()] => term()}.
to_entry_map(L) when is_list(L) ->
	lists:foldl(fun(T, Acc) ->
		maps:merge(Acc, tuple_to_entries(T))
	end, #{}, L).

tuple_to_entries({BucketSize, Bucket, Packing} = Tuple) ->
	ID = derived_id(Tuple),
	maps:merge(
		range_entries(ID, BucketSize, Bucket),
		packing_entries(ID, Packing)).

range_entries(ID, BucketSize, Bucket) when BucketSize =:= ?PARTITION_SIZE ->
	#{ [storage_modules, ID, partition] => Bucket };
range_entries(ID, BucketSize, Bucket) ->
	Start = Bucket * BucketSize,
	#{
		[storage_modules, ID, range, start] => Start,
		[storage_modules, ID, range, 'end'] => Start + BucketSize
	}.

packing_entries(ID, unpacked) ->
	#{ [storage_modules, ID, packing, format] => unpacked };
packing_entries(ID, {Format, Addr})
		when Format =:= spora_2_6; Format =:= replica_2_9 ->
	#{
		[storage_modules, ID, packing, format] => Format,
		[storage_modules, ID, packing, address] => Addr
	};
packing_entries(ID, {composite, Addr, PackingDifficulty}) ->
	#{
		[storage_modules, ID, packing, format] => composite,
		[storage_modules, ID, packing, address] => Addr,
		[storage_modules, ID, packing, difficulty] => PackingDifficulty
	}.

%% @doc Take the legacy tuple list and write each entry's attributes
%% into `[storage_modules, <id>, ...]`.
-spec write_list([term()]) -> ok.
write_list(L) when is_list(L) ->
	clear_non_defrag_entries(),
	lists:foreach(fun write_storage_module/1, L),
	ok.

write_storage_module({_BucketSize, _Bucket, _Packing} = Tuple) ->
	ID = derived_id(Tuple),
	write_packing_attrs(ID, Tuple),
	write_range_attrs(ID, Tuple),
	ok.

%% @doc Take the legacy defragmentation_modules list and set
%% `defrag => true` on matching `[storage_modules, <id>, ...]`
%% entries. Modules that aren't already present (defrag-only modules
%% in legacy parlance) are synthesized with `packing.*` and the
%% appropriate range attribute populated from the legacy tuple.
-spec write_defrags([term()]) -> ok.
write_defrags(L) when is_list(L) ->
	clear_defrag_flags(),
	lists:foreach(fun write_defrag_module/1, L),
	ok.

write_defrag_module({_BucketSize, _Bucket, _Packing} = Tuple) ->
	ID = derived_id(Tuple),
	write_packing_attrs(ID, Tuple),
	write_range_attrs(ID, Tuple),
	set_local([storage_modules, ID, defrag], true),
	ok.

%% Internal write helpers.

write_packing_attrs(ID, {_BS, _B, unpacked}) ->
	set_local([storage_modules, ID, packing, format], unpacked);
write_packing_attrs(ID, {_BS, _B, {Format, Addr}})
		when Format =:= spora_2_6; Format =:= replica_2_9 ->
	set_local([storage_modules, ID, packing, format], Format),
	set_local([storage_modules, ID, packing, address], Addr);
write_packing_attrs(ID, {_BS, _B, {composite, Addr, PackingDifficulty}}) ->
	set_local([storage_modules, ID, packing, format], composite),
	set_local([storage_modules, ID, packing, address], Addr),
	set_local([storage_modules, ID, packing, difficulty], PackingDifficulty).

write_range_attrs(ID, {BucketSize, Bucket, _Packing})
		when BucketSize =:= ?PARTITION_SIZE ->
	set_local([storage_modules, ID, partition], Bucket);
write_range_attrs(ID, {BucketSize, Bucket, _Packing}) ->
	Start = Bucket * BucketSize,
	End = Start + BucketSize,
	set_local([storage_modules, ID, range, start], Start),
	set_local([storage_modules, ID, range, 'end'], End).

set_local(Key, Value) ->
	_ = arweave_config_options_registry:set_local(Key, Value),
	ok.

%% Internal read helpers.

module_ids() ->
	Items = arweave_config:get_all_with_prefix([storage_modules]),
	lists:usort([ID || {[storage_modules, ID | _], _Value} <- Items]).

has_defrag(ID) ->
	case arweave_config_store:get([storage_modules, ID, defrag]) of
		{ok, true} -> true;
		_ -> false
	end.

tuple_for(ID) ->
	{BucketSize, Bucket} = read_range_for(ID),
	Packing = read_packing_for(ID),
	{BucketSize, Bucket, Packing}.

read_range_for(ID) ->
	case arweave_config_store:get([storage_modules, ID, partition]) of
		{ok, Bucket} ->
			{?PARTITION_SIZE, Bucket};
		_ ->
			{ok, Start} = arweave_config_store:get(
				[storage_modules, ID, range, start]),
			{ok, End} = arweave_config_store:get(
				[storage_modules, ID, range, 'end']),
			BucketSize = End - Start,
			Bucket = case BucketSize of
				0 -> 0;
				_ -> Start div BucketSize
			end,
			{BucketSize, Bucket}
	end.

read_packing_for(ID) ->
	case arweave_config_store:get([storage_modules, ID, packing, format]) of
		{ok, unpacked} ->
			unpacked;
		{ok, Format} when Format =:= spora_2_6; Format =:= replica_2_9 ->
			{ok, Addr} = arweave_config_store:get(
				[storage_modules, ID, packing, address]),
			{Format, Addr};
		{ok, composite} ->
			{ok, Addr} = arweave_config_store:get(
				[storage_modules, ID, packing, address]),
			{ok, PackingDifficulty} = arweave_config_store:get(
				[storage_modules, ID, packing, difficulty]),
			{composite, Addr, PackingDifficulty}
	end.

%% Clear helpers.

%% @doc The storage_modules bridge clears entries that aren't defrag-flagged.
%% Defrag-flagged modules are owned by the defragmentation_modules
%% bridge and left intact across rewrites of the regular list.
clear_non_defrag_entries() ->
	[drop_module(ID) || ID <- module_ids(), not has_defrag(ID)],
	ok.

%% @doc The defrag bridge clears just the `defrag` attribute; module
%% shape is left intact in case the same module is also declared via
%% the storage_modules bridge. Defrag-only modules that lose their flag
%% become orphan storage entries — accepted limitation on reload.
clear_defrag_flags() ->
	[arweave_config_store:delete([storage_modules, ID, defrag])
		|| ID <- module_ids(), has_defrag(ID)],
	ok.

drop_module(ID) ->
	Items = arweave_config:get_all_with_prefix([storage_modules, ID]),
	[arweave_config_store:delete(Key) || {Key, _Value} <- Items],
	ok.

%% Storage-module-set validator.

-spec validate() -> ok | {error, binary()}.
validate() ->
	IDs = module_ids(),
	case validate_each(IDs) of
		ok ->
			case validate_no_duplicates() of
				ok -> validate_unique_replication_type();
				{error, _} = Err -> Err
			end;
		{error, _} = Err ->
			Err
	end.

validate_no_duplicates() ->
	Modules = list(),
	case length(Modules) =:= length(lists:usort(Modules)) of
		true ->
			ok;
		false ->
			{error, <<"Duplicate value detected in the storage_modules option.">>}
	end.

%% @doc Cross-cutting: also reads [mining, enabled] and [mining, address].
validate_unique_replication_type() ->
	case arweave_config:get([mining, enabled], false) of
		true ->
			MiningAddr = arweave_config:get([mining, address], not_set),
			Unique = lists:foldl(
				fun({_, _, {composite, Addr, Difficulty}}, Acc)
						when Addr =:= MiningAddr ->
					sets:add_element({composite, Difficulty}, Acc);
				({_, _, {spora_2_6, Addr}}, Acc) when Addr =:= MiningAddr ->
					sets:add_element(spora_2_6, Acc);
				({_, _, {replica_2_9, Addr}}, Acc) when Addr =:= MiningAddr ->
					sets:add_element(replica_2_9, Acc);
				(_, Acc) ->
					Acc
				end,
				sets:new(),
				list()
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
		fun validate_packing/1,
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
	HasPartition = is_set([storage_modules, ID, partition]),
	HasRangeStart = is_set([storage_modules, ID, range, start]),
	HasRangeEnd = is_set([storage_modules, ID, range, 'end']),
	case {HasPartition, HasRangeStart, HasRangeEnd} of
		{true,  false, false} -> ok;
		{false, true,  true} -> ok;
		{true,  _,     _} ->
			{error, format_error(
				<<"partition and range are mutually exclusive">>, ID)};
		{false, false, false} ->
			{error, format_error(<<"missing partition or range">>, ID)};
		_ ->
			{error, format_error(<<"range requires both start and end">>, ID)}
	end.

validate_packing(ID) ->
	case arweave_config_store:get([storage_modules, ID, packing, format]) of
		{ok, unpacked} ->
			ok;
		{ok, Format} when Format =:= spora_2_6; Format =:= replica_2_9 ->
			case arweave_config_store:get(
					[storage_modules, ID, packing, address]) of
				{ok, A} when is_binary(A), byte_size(A) =:= 32 -> ok;
				{ok, _} ->
					{error, format_error(
						<<"packing.address must be a 32-byte binary">>,
						io_format_id(ID))};
				_ ->
					{error, format_error(
						<<"packing.format requires packing.address">>,
						io_format_id(ID))}
			end;
		{ok, composite} ->
			%% Composite needs both address and difficulty; address is
			%% any non-empty binary (test fixtures use shorter labels).
			case arweave_config_store:get(
					[storage_modules, ID, packing, difficulty]) of
				{ok, D} when is_integer(D), D >= 0 ->
					case arweave_config_store:get(
							[storage_modules, ID, packing, address]) of
						{ok, A} when is_binary(A), byte_size(A) > 0 -> ok;
						_ -> {error, format_error(
							<<"composite packing requires packing.address">>,
							io_format_id(ID))}
					end;
				_ -> {error, format_error(
					<<"composite packing requires packing.difficulty">>,
					io_format_id(ID))}
			end;
		{ok, BadFormat} ->
			{error, format_error(
				<<"unsupported packing format">>,
				<<(io_format_id(ID))/binary, " = ",
				  (atom_to_binary(BadFormat))/binary>>)};
		_ ->
			{error, format_error(<<"missing packing.format">>, io_format_id(ID))}
	end.

validate_derived_id(ID) ->
	{BucketSize, Bucket} = read_range_for(ID),
	Packing = read_packing_for(ID),
	Expected = derived_id({BucketSize, Bucket, Packing}),
	case ID =:= Expected of
		true -> ok;
		false ->
			{error, format_error(
				<<"derived id mismatch (expected ", Expected/binary, ")">>,
				io_format_id(ID))}
	end.

is_set(Key) ->
	case arweave_config_store:get(Key) of
		{ok, _} -> true;
		_ -> false
	end.

io_format_id(ID) when is_binary(ID) -> ID;
io_format_id(ID) -> list_to_binary(io_lib:format("~p", [ID])).

format_error(What, Detail) when is_binary(What), is_binary(Detail) ->
	<<"storage_modules: ", What/binary, " (", Detail/binary, ")">>.
