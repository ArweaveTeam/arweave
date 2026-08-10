%%% @doc Specs for the `storage_modules` option group. Options for
%%% declaring what weave data the node stores and how it is encoded.
%%%
%%% Storage modules are canonically stored as `[storage_modules]`, a
%%% list of maps (the form config files, `config get/set` and the
%%% serializers speak). Leaf specs use `{list_item}` to declare the
%%% fields available inside each list element.
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
%%% `range_end` (mutually exclusive). Ranges are arbitrary byte
%%% ranges: any `range_end > range_start` is valid.
%%% Repack-in-place modules live in their own `[repack_modules]`
%%% list value.
%%%
%%% Everything outside `arweave_config` speaks the runtime dialect
%%% `{RangeStart, RangeEnd, Packing}`: `storage_modules/0` /
%%% `defrag_storage_modules/0` read the store as runtime tuples, and
%%% `set` accepts runtime tuples alongside maps (normalized by the
%%% `storage_modules` type via `normalize_entry/1`). The map<->tuple
%%% conversions live here and nowhere else. The pre-2.9.6 bucket
%%% notation (`{BucketSize, Bucket}`) exists only inside
%%% `arweave_config_format_legacy_json:parse_storage_module', which
%%% converts it to a runtime range while parsing.
%%%
%%% Accepted packing formats: `unpacked`, `spora_2_6`, and `replica_2_9`.
-module(arweave_config_options_storage_modules).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-export([
    storage_modules/0,
    defrag_storage_modules/0,
    normalize_entry/1,
    config_to_storage_module/1,
    runtime_to_config/1,
    write_legacy_storage_module/1,
    write_legacy_list/1,
    write_legacy_defrags/1
]).
%% Shared helpers, also used by arweave_config_options_repack_modules
%% (same per-module shape, different field names).
-export([
    range_from_map/1,
    range_to_config/2,
    packing_map/3,
    packing_from_map/3,
    validate_range_fields/2,
    validate_module_packing/4,
    write_module_maps/2
]).
-include("arweave_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

specs() ->
    [
        #{
            enabled => true,
            option_key => [storage_modules],
            type => storage_modules,
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
                  "covers exactly one partition; for any other range, set "
                  "`range_start` and `range_end` explicitly instead. "
                  "Mutually exclusive with those range fields.">>
        },
        #{
            enabled => true,
            option_key => [storage_modules, {list_item}, range_start],
            type => pos_integer,
            short_description =>
                <<"Inclusive start byte offset of the storage module's "
                  "range.">>,
            long_description =>
                <<"Use range to size a storage module to an arbitrary "
                  "byte range: any range with `range_end > range_start` "
                  "is valid; it does not need to be aligned to a "
                  "partition or to its own length. Make sure the "
                  "corresponding disk has about 10% extra space for "
                  "proofs and other metadata. Required when `partition` "
                  "is not set.">>
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

%% @doc Return the configured storage modules as runtime tuples
%% `{RangeStart, RangeEnd, Packing}` - the representation everything
%% outside `arweave_config` uses.
storage_modules() ->
    [config_to_storage_module(Module) || Module <- stored_maps()].

%% @doc Return the storage modules flagged `defrag => true`, as
%% runtime tuples.
defrag_storage_modules() ->
    [config_to_storage_module(Module) || Module <- stored_maps(),
        maps:get(defrag, Module, false) =:= true].

stored_maps() ->
    case arweave_config_store:get([storage_modules]) of
        {ok, Modules} when is_list(Modules) -> Modules;
        _ -> []
    end.

%% @doc Normalize one `[storage_modules]` list entry at set time:
%% canonical maps pass through, runtime tuples convert to their
%% canonical map. Called by the `storage_modules` type
%% (`arweave_config_type:storage_modules/1`).
normalize_entry(Module) when is_map(Module) ->
    Module;
normalize_entry({Start, End, _Packing} = Module)
        when is_integer(Start), is_integer(End) ->
    runtime_to_config(Module).

%% @doc Write the parsed legacy `storage_module` list (already
%% converted to runtime tuples by the legacy parser) as the canonical
%% `[storage_modules]' list of maps.
write_legacy_list([]) ->
	ok;
write_legacy_list(L) when is_list(L) ->
    ExistingDefrags = defrag_storage_modules(),
    write_modules(lists:usort(L ++ ExistingDefrags)),
    ok.

write_legacy_storage_module({_Start, _End, _Packing} = Module) ->
    write_modules(lists:usort([Module | storage_modules()])),
    ok.

%% @doc Take the legacy defragmentation_modules list (as runtime
%% tuples) and set `defrag => true` on matching storage module
%% entries. Modules that aren't already present (defrag-only modules
%% in legacy parlance) are synthesized from the tuple.
write_legacy_defrags(L) when is_list(L) ->
    DefragSet = sets:from_list(L),
    Existing = storage_modules(),
    Combined = lists:usort(Existing ++ L),
    write_module_maps([storage_modules], [
        (runtime_to_config(Module))#{
            defrag => sets:is_element(Module, DefragSet)}
        || Module <- Combined
    ]),
    ok.

write_modules(Modules) ->
    write_module_maps([storage_modules],
        [runtime_to_config(Module) || Module <- Modules]).

%% @doc Replace the stored module list under `Key' (`[storage_modules]'
%% or `[repack_modules]') with the given canonical maps.
write_module_maps(Key, Modules) ->
    _ = arweave_config_store:delete_prefix(Key),
    _ = arweave_config_options_registry:set_local(Key, Modules),
    ok.

%% Internal map <-> runtime tuple conversions.

%% @doc Convert a canonical map into the runtime tuple
%% `{RangeStart, RangeEnd, Packing}'.
config_to_storage_module(Module) ->
    {Start, End} = range_from_map(Module),
    Packing = packing_from_map(packing_format, packing_address, Module),
    {Start, End, Packing}.

%% @doc Convert a runtime tuple `{RangeStart, RangeEnd, Packing}'
%% into the canonical map - the inverse of
%% `config_to_storage_module/1'.
runtime_to_config({Start, End, Packing}) ->
    RangeAttrs = range_to_config(Start, End),
    Packing2 = packing_map(packing_format, packing_address, Packing),
    (maps:merge(RangeAttrs, Packing2))#{defrag => false}.

range_from_map(#{partition := Bucket}) ->
    Start = Bucket * ?PARTITION_SIZE,
    {Start, Start + ?PARTITION_SIZE};
range_from_map(#{range_start := Start, range_end := End}) ->
    {Start, End}.

%% @doc Build the range fields of a canonical module map: a range
%% covering exactly one aligned partition becomes the `partition`
%% shorthand; any other range keeps its explicit offsets.
range_to_config(Start, End) ->
    case End - Start =:= ?PARTITION_SIZE
            andalso Start rem ?PARTITION_SIZE =:= 0 of
        true ->
            #{partition => Start div ?PARTITION_SIZE};
        false ->
            #{range_start => Start, range_end => End}
    end.

%% @doc Build the packing fields of a canonical module map, using the
%% given field names (`packing_format`/`packing_address` here,
%% `from_*`/`to_*` in repack modules).
packing_map(FormatField, _AddressField, unpacked) ->
    #{FormatField => unpacked};
packing_map(FormatField, AddressField, {Format, Addr})
        when Format =:= spora_2_6; Format =:= replica_2_9 ->
    #{FormatField => Format, AddressField => Addr}.

%% @doc Read a packing (`unpacked' or `{Format, Addr}') out of a
%% canonical module map, using the given field names.
packing_from_map(FormatField, AddressField, Module) ->
    packing_from_map2(maps:get(FormatField, Module), AddressField, Module).

packing_from_map2(unpacked, _AddressField, _Module) ->
    unpacked;
packing_from_map2(Format, AddressField, Module)
        when Format =:= spora_2_6; Format =:= replica_2_9 ->
    {Format, maps:get(AddressField, Module)}.

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
    validate_modules(stored_maps()).

validate_modules([]) ->
    ok;
validate_modules([Module | Rest]) ->
    case validate_module(Module) of
        ok -> validate_modules(Rest);
        {error, _} = Err -> Err
    end.

validate_module(Module) when is_map(Module) ->
    case validate_range_fields(<<"storage_modules">>, Module) of
        ok ->
            validate_module_packing(
                <<"storage_modules">>, packing_format, packing_address,
                Module);
        {error, _} = Err ->
            Err
    end;
validate_module(_) ->
    {error, <<"storage_modules: invalid module shape">>}.

%% @doc Validate the range fields of a canonical module map. `Group'
%% (e.g. `<<"storage_modules">>') prefixes the error messages.
validate_range_fields(Group, #{partition := _} = Module)
        when is_map_key(range_start, Module);
             is_map_key(range_end, Module) ->
    {error, <<Group/binary, ": partition and range_start/range_end "
              "are mutually exclusive">>};
validate_range_fields(_Group, #{partition := Partition})
        when is_integer(Partition), Partition >= 0 ->
    ok;
validate_range_fields(_Group, #{range_start := Start, range_end := End})
        when is_integer(Start), Start >= 0, is_integer(End), End > Start ->
    ok;
validate_range_fields(Group, #{range_start := Start, range_end := End})
        when is_integer(Start), is_integer(End) ->
    {error, <<Group/binary, ": range_end must be greater than "
              "range_start">>};
validate_range_fields(Group, _) ->
    {error, <<Group/binary, ": set either partition or both "
              "range_start and range_end">>}.

%% @doc Validate the packing declared under the given field names of a
%% canonical module map.
validate_module_packing(Group, FormatField, AddressField, Module) ->
    try packing_from_map(FormatField, AddressField, Module) of
        unpacked ->
            ok;
        {Format, Addr}
                when (Format =:= spora_2_6 orelse Format =:= replica_2_9),
                     is_binary(Addr), byte_size(Addr) =:= 32 ->
            ok;
        _ ->
            {error, <<Group/binary, ": invalid packing">>}
    catch
        _:_ ->
            {error, <<Group/binary, ": invalid packing">>}
    end.

validate_no_duplicates() ->
    Modules = storage_modules(),
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
                storage_modules()
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
