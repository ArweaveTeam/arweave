%%% @doc The prefix of each sector a storage module with a footprint_limit
%%% keeps, counted in replica.2.9 entropy footprints. Every question about
%%% the limit is answered here; nothing else compares a footprint to it.
-module(ar_footprint_limit).
-test_category([fast]).
%% get/1 is the module's main export; the process-dictionary BIF of the
%% same name is not used here.
-compile({no_auto_import, [get/1]}).

-export([get/1, is_unlimited/1, is_beyond/2, clip/3, kept_intervals/3]).

-include("ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc How many footprints of each sector the module keeps: its
%% footprint_limit clamped to at least one and at most every footprint of
%% a partition, or every footprint without a limit.
get(StoreID) ->
    All = arweave_constants:get_replica_2_9_footprints_per_partition(),
    case arweave_storage:store_info(StoreID) of
        #store_info{
            configured_range = {Start, End}, packing = Packing
        } when is_integer(End) ->
            Module = {Start, End, Packing},
            clamp(arweave_config:storage_module_footprint_limit(Module), All);
        _ ->
            All
    end.

clamp(not_set, All) ->
    All;
clamp(Limit, All) ->
    min(max(1, Limit), All).

%% @doc Whether the limit keeps every footprint of a partition.
is_unlimited(Limit) ->
    Limit >= arweave_constants:get_replica_2_9_footprints_per_partition().

%% @doc Whether the chunk with the given bucket end offset lies past the
%% limit, in the part of its sector the module does not keep.
is_beyond(BucketEndOffset, Limit) ->
    arweave_storage:get_footprint(BucketEndOffset) >= Limit.

%% @doc The number of buckets from the given one to the end of the kept
%% prefix of its sector, at most Count and at least one. Without a limit
%% Count is returned as is: the sector end is the caller's cap.
clip(BucketEndOffset, Count, Limit) ->
    case is_unlimited(Limit) of
        true ->
            Count;
        false ->
            Footprint = arweave_storage:get_footprint(BucketEndOffset),
            max(1, min(Count, Limit - Footprint))
    end.

%% @doc The byte intervals of [Start, End) within the first Limit footprints
%% of their sectors, as an ar_intervals set: what a module with the limit
%% keeps of the range.
kept_intervals(Start, End, Limit) ->
    case is_unlimited(Limit) of
        true ->
            ar_intervals:add(ar_intervals:new(), End, Start);
        false ->
            kept_intervals(Start, End, Limit, ar_intervals:new())
    end.

kept_intervals(Start, End, _Limit, Intervals) when Start >= End ->
    Intervals;
kept_intervals(Start, End, Limit, Intervals) ->
    ChunkEndOffset = Start + ?DATA_CHUNK_SIZE,
    SectorStart =
        arweave_storage:get_sector_bucket_start(ChunkEndOffset, 0),
    AllowedEnd = min(SectorStart + Limit * ?DATA_CHUNK_SIZE, End),
    Intervals2 =
        case AllowedEnd > Start of
            true -> ar_intervals:add(Intervals, AllowedEnd, Start);
            false -> Intervals
        end,
    NextSectorStart =
        arweave_storage:get_sector_bucket_start(ChunkEndOffset, 1),
    kept_intervals(NextSectorStart, End, Limit, Intervals2).

%%%===================================================================
%%% Tests. Test geometry: 512 KiB sectors holding two chunks, so two
%%% footprints per partition; partitions of 2,000,000 bytes.
%%%===================================================================

get_test() ->
    arweave_config:internal_with_test_config(fun() ->
        P = arweave_constants:partition_size(),
        Addr = crypto:strong_rand_bytes(32),
        All = arweave_constants:get_replica_2_9_footprints_per_partition(),
        ok = arweave_config:internal_force_config(#{
            [storage_modules] => [
                #{partition => 0, packing_format => replica_2_9,
                    packing_address => Addr, footprint_limit => 1},
                #{partition => 1, packing_format => unpacked},
                #{partition => 2, packing_format => unpacked,
                    footprint_limit => 10 * All}
            ]}),
        ?assertEqual(
            1,
            get(
                (arweave_storage:store_info({0, P, {replica_2_9, Addr}}))#store_info.id
            )
        ),
        ?assertEqual(
            All,
            get((arweave_storage:store_info({P, 2 * P, unpacked}))#store_info.id)
        ),
        %% A limit above the partition keeps every footprint.
        ?assertEqual(
            All,
            get(
                (arweave_storage:store_info({2 * P, 3 * P, unpacked}))#store_info.id
            )
        ),
        ?assertEqual(All, get(?DEFAULT_MODULE)),
        ?assertEqual(All, get("storage_module_9_unpacked")),
        ok = arweave_config:internal_force_config(#{
            [storage_modules] => [],
            [repack_modules] => [
                #{partition => 3, from_format => unpacked,
                    to_format => replica_2_9, to_address => Addr,
                    footprint_limit => 1}
            ]}),
        %% A repack source is limited by its repack entry.
        ?assertEqual(
            1,
            get(
                (arweave_storage:store_info({3 * P, 4 * P, unpacked}))#store_info.id
            )
        )
    end).

is_beyond_test() ->
    %% The first chunk of every sector has footprint 0, the second 1.
    ?assertNot(is_beyond(262144, 1)),
    ?assert(is_beyond(524288, 1)),
    ?assertNot(is_beyond(524288, 2)),
    ?assertNot(is_beyond(786432, 1)),
    ?assert(is_beyond(1048576, 1)).

clip_test() ->
    %% One bucket from the first footprint with the limit at 1, never below
    %% one past the limit, and the count as is without a limit (two
    %% footprints per partition here).
    ?assertEqual(1, clip(262144, 10, 1)),
    ?assertEqual(1, clip(524288, 10, 1)),
    ?assertEqual(10, clip(262144, 10, 2)),
    ?assertEqual(10, clip(524288, 10, 2)),
    ?assertEqual(1, clip(262144, 1, 1)).

kept_intervals_test() ->
    Whole = kept_intervals(0, 2097152, 5),
    ?assertEqual([{2097152, 0}], ar_intervals:to_list(Whole)),
    One = kept_intervals(0, 2097152, 1),
    ?assertEqual([{262144, 0}, {786432, 524288}, {1310720, 1048576},
            {1835008, 1572864}], ar_intervals:to_list(One)),
    Two = kept_intervals(0, 2097152, 2),
    ?assertEqual([{2097152, 0}], ar_intervals:to_list(Two)),
    %% A window starting inside a kept prefix keeps only its tail.
    Tail = kept_intervals(131072, 1000000, 1),
    ?assertEqual([{262144, 131072}, {786432, 524288}],
            ar_intervals:to_list(Tail)),
    %% A window starting past the prefix gets the next sector's prefix.
    Skip = kept_intervals(262144, 1048576, 1),
    ?assertEqual([{786432, 524288}], ar_intervals:to_list(Skip)).
