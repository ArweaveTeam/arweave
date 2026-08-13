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
    repack_modules/1,
    normalize_entry/1,
    config_to_repack_module/1,
    runtime_to_config/1,
    write_legacy_repack_module/1,
    write_legacy_list/1,
    validate/0
]).
-include("arweave_config.hrl").

specs() ->
    [
        #{
            enabled => true,
            option_key => [repack_modules],
            type => repack_modules,
            default => [],
            short_description =>
                <<"Repack-in-place module declarations.">>,
            long_description =>
                <<"For replica.2.9 repacks the read batch size is derived "
                  "from [packing, entropy, cache_size]; set that cache as "
                  "large as you can without running out of memory (a good "
                  "starting point is ~40% of available RAM) to improve "
                  "repacking throughput.">>
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
                <<"Required when `partition' is not set. Any range with "
                  "`range_end > range_start' is valid, but it must match "
                  "the source storage module's on-disk layout.">>
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

%% @doc Return the configured repack-in-place modules. With
%% `module_only' only the source storage module of each entry is
%% returned (`{RangeStart, RangeEnd, FromPacking}`); with `full' the
%% full repack spec pairs
%% `{{RangeStart, RangeEnd, FromPacking}, ToPacking}`.
repack_modules(Shape) ->
    Pairs =
        case arweave_config_store:get([repack_modules]) of
            {ok, Modules} when is_list(Modules) ->
                [config_to_repack_module(Module) || Module <- Modules];
            _ ->
                []
        end,
    case Shape of
        module_only -> [Module || {Module, _ToPacking} <- Pairs];
        full -> Pairs
    end.

%% @doc Normalize one `[repack_modules]` list entry at set time:
%% canonical maps pass through, runtime pairs convert to their
%% canonical map. Called by the `repack_modules` type
%% (`arweave_config_type:repack_modules/1`).
normalize_entry(Module) when is_map(Module) ->
    Module;
normalize_entry({{Start, End, _From}, _To} = Module)
        when is_integer(Start), is_integer(End) ->
    runtime_to_config(Module).

%% @doc Write the parsed legacy repack-in-place list (already
%% converted to runtime pairs by the legacy parser) into the
%% canonical `[repack_modules]' list of maps.
-spec write_legacy_list([term()]) -> ok.
write_legacy_list([]) ->
    ok;
write_legacy_list(L) when is_list(L) ->
    write_modules(L),
    ok.

write_legacy_repack_module({{_Start, _End, _Packing}, _ToPacking} = Tuple) ->
    write_modules(lists:usort([Tuple | repack_modules(full)])),
    ok.

write_modules(Modules) ->
    arweave_config_options_storage_modules:write_module_maps(
        [repack_modules], [runtime_to_config(Module) || Module <- Modules]).

%% Map <-> runtime pair conversions. The shared per-side helpers live
%% in arweave_config_options_storage_modules; here each side (`from'
%% and `to') carries its own field names.

%% @doc Convert a canonical map into the RUNTIME-dialect pair
%% `{{RangeStart, RangeEnd, FromPacking}, ToPacking}'.
config_to_repack_module(Module) ->
    {Start, End} =
        arweave_config_options_storage_modules:range_from_map(Module),
    From = packing_from_map(from, Module),
    To = packing_from_map(to, Module),
    {{Start, End, From}, To}.

%% @doc Convert a RUNTIME-dialect pair `{{RangeStart, RangeEnd,
%% FromPacking}, ToPacking}' into the canonical map - the inverse of
%% `config_to_repack_module/1'.
runtime_to_config({{Start, End, FromPacking}, ToPacking}) ->
    maps:merge(
        arweave_config_options_storage_modules:range_to_config(Start, End),
        maps:merge(
            packing_map(from, FromPacking),
            packing_map(to, ToPacking)
        )
    ).

packing_map(Prefix, Packing) ->
    arweave_config_options_storage_modules:packing_map(
        format_field(Prefix), address_field(Prefix), Packing).

packing_from_map(Prefix, Module) ->
    arweave_config_options_storage_modules:packing_from_map(
        format_field(Prefix), address_field(Prefix), Module).

format_field(from) -> from_format;
format_field(to) -> to_format.

address_field(from) -> from_address;
address_field(to) -> to_address.

%% Module-level validator. Validates each repack module's shape and
%% additionally checks that no repack module overlaps a regular
%% storage module.

-spec validate() -> ok | {error, binary()}.
validate() ->
    run_checks([
        fun validate_shape/0,
        fun validate_no_regular_storage_modules/0,
        fun validate_uniform_operation/0
    ]).

run_checks([]) ->
    ok;
run_checks([Check | Rest]) ->
    case Check() of
        ok -> run_checks(Rest);
        {error, _} = Err -> Err
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

validate_module(Module) when is_map(Module) ->
    case arweave_config_options_storage_modules:validate_range_fields(
            <<"repack_modules">>, Module) of
        ok ->
            case validate_module_packing(from, Module) of
                ok -> validate_module_packing(to, Module);
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end;
validate_module(_) ->
    {error, <<"repack_modules: invalid module shape">>}.

validate_module_packing(Prefix, Module) ->
    arweave_config_options_storage_modules:validate_module_packing(
        <<"repack_modules">>, format_field(Prefix), address_field(Prefix),
        Module).

%% @doc While any module is being repacked in place, every storage module must be a repack
%% module. Repacking, syncing and mining are all memory-heavy processes - we do not support
%% efficient memory utilization when they are run simultaneously so we demand repacking is executed first.
validate_no_regular_storage_modules() ->
    case arweave_config:get([repack_modules]) of
        [] ->
            ok;
        _ ->
            case arweave_config:get([storage_modules]) of
                [] ->
                    ok;
                _ ->
                    {error, <<"repack_modules: all storage modules must be repacked in "
                        "place. Remove regular storage_modules entries while repacking.">>}
            end
    end.

%% @doc All repack modules must use the same archetype: replica.2.9 vs not on each side -
%% the four combinations of {unpacked|spora_2_6, replica.2.9}. Addresses may differ between
%% modules. This keeps the per-chunk entropy count uniform, which the batch-size derivation
%% relies on.
validate_uniform_operation() ->
    case arweave_config:get([repack_modules]) of
        [] ->
            ok;
        Modules ->
            case lists:usort([repack_archetype(M) || M <- Modules]) of
                [_] ->
                    ok;
                _ ->
                    {error, <<"repack_modules: all repack modules must use the same "
                        "from/to packing archetype (replica.2.9 vs not, on each side); "
                        "addresses may differ.">>}
            end
    end.

%% @doc The repack archetype ignores the mining address - only whether each side is the
%% replica.2.9 format matters.
repack_archetype(Module) ->
    {is_replica_2_9(packing_from_map(from, Module)),
        is_replica_2_9(packing_from_map(to, Module))}.

is_replica_2_9({replica_2_9, _}) -> true;
is_replica_2_9(_) -> false.
