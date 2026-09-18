%%% Public storage API; implementation and process ownership stay internal.
-module(arweave_storage).
-behaviour(application).

-export_type([storage_module/0]).
-type storage_module() :: arweave_storage_module:storage_module().

-ifdef(AR_TEST).
-export([internal_write_chunk/3, internal_erase_chunk/2]).
-endif.

%% Lifecycle.
-export([
    start/2,
    stop/1,
    child_spec/0,
    activate/0,
    activate/1,
    deactivate/0
]).

%% Storage module metadata.
-export([
    store_info/1,
    get_overlap/1,
    packing_label/1,
    covering_store/2,
    covering_stores/2,
    intersecting_stores/3,
    covers_offset/3,
    covers_range/4,
    covering_ranges/3
]).

%% Availability records.
-export([
    get_data_sizes/0,
    sync_record_exists/3,
    get_sync_record/3,
    add_sync_record/5,
    delete_sync_record/4,
    cut_sync_record/3,
    is_recorded/4,
    is_recorded_any/3,
    get_next_interval/6,
    get_intervals/6,
    get_interval/4,
    get_intersection_size/5
]).

%% Chunk I/O and maintenance.
-export([
    is_storage_supported/3,
    put_chunk/4,
    open_chunk_files/1,
    get_chunk/2,
    locate_chunk_on_disk/2,
    get_chunk_range/3,
    delete_chunk/2,
    set_entropy_complete/1,
    chunk_filepath/2,
    close_chunk_files/1,
    list_chunk_files/1,
    run_defragmentation/0,
    read_offset/2
]).

%% Entropy storage.
-export([
    entropy_context/2,
    read_entropy_cursor/2,
    write_entropy_cursor/2,
    await_entropy_writes/1,
    entropy_sync_record_id/0,
    is_entropy_recorded/3,
    get_next_unsynced_entropy_interval/3,
    add_entropy_record/3,
    delete_entropy_record/2,
    delete_entropy_record/3,
    store_entropy_footprint/2,
    store_entropy/4
]).

%% Aggregate availability.
-export([
    get_serialized_sync_record/1,
    get_serialized_buckets/1
]).

%% Footprint records.
-export([
    add_footprint/3,
    delete_footprint/2
]).

%% Chunk and footprint geometry.
-export([
    get_position_and_relative_chunk_offset/2,
    get_chunk_bucket_start/1,
    get_chunk_bucket_end/1,
    get_chunk_byte_from_bucket_end/1,
    get_chunk_seek_offset/1,
    get_chunk_file_start/1,
    get_footprint_offset/1,
    get_footprint_range/2,
    get_padded_offset_from_footprint_offset/1,
    get_footprint/1,
    get_footprint_location/1,
    get_footprint_bucket/1,
    footprint_intervals_to_byte_intervals/1,
    footprint_intervals_to_byte_intervals/3,
    max_footprint_offset/1,
    get_next_sector_start/1,
    get_sector_bucket_start/2
]).

%%====================================================================
%% Lifecycle
%%====================================================================

start(normal, []) ->
    arweave_storage_sup:start_link().

stop(_State) ->
    ok.

%% @doc Return the host lifecycle child that activates storage after its dependencies.
child_spec() ->
    arweave_storage_lifecycle:child_spec().

%% @doc Start storage services once the host dependencies are available.
activate() ->
    arweave_storage_sup:activate().

%% @doc Activate node or standalone storage after its dependencies start.
activate(Mode) ->
    arweave_storage_sup:activate(Mode).

%% @doc Stop storage services after all host users have stopped.
deactivate() ->
    arweave_storage_sup:deactivate().

%%====================================================================
%% Storage module metadata
%%====================================================================

%% @doc Return storage module metadata for a tuple or ID, or not_found.
store_info(ModuleOrID) ->
    arweave_storage_module:info(ModuleOrID).

get_overlap(Packing) ->
    arweave_storage_module:get_overlap(Packing).

packing_label(Packing) ->
    arweave_storage_module:packing_label(Packing).

%% @doc Select a covering module, preferring the requested packing.
covering_store(Offset, PreferredPacking) ->
    arweave_storage_module:covering_store(Offset, PreferredPacking).

covering_stores(Offset, Packing) ->
    arweave_storage_module:covering_stores(Offset, Packing).

intersecting_stores(Start, End, Packing) ->
    arweave_storage_module:intersecting_stores(Start, End, Packing).

%% @doc Check configured offset coverage, including packing overlap.
covers_offset(Offset, Packing, StoreID) ->
    arweave_storage_module:covers_offset(Offset, Packing, StoreID).

%% @doc Check complete configured range coverage, excluding packing overlap.
covers_range(Start, End, Packing, StoreID) ->
    arweave_storage_module:covers_range(Start, End, Packing, StoreID).

%% @doc Return {Start, End, StoreID} ranges covering the requested interval,
%% or not_found if configured storage modules cannot cover it.
covering_ranges(Start, End, PreferredStore) ->
    arweave_storage_module:covering_ranges(Start, End, PreferredStore).

%%====================================================================
%% Availability records
%%====================================================================

get_data_sizes() ->
    arweave_storage_sync_record:get_data_sizes().

%% @doc Check whether a matching record exists, even if it is empty.
sync_record_exists(Packing, Record, StoreID) ->
    arweave_storage_sync_record:sync_record_exists(Packing, Record, StoreID).

get_sync_record(Packing, Record, StoreID) ->
    arweave_storage_sync_record:get(Packing, Record, StoreID).

%% @doc Add an interval, using any_packing for an untyped record.
add_sync_record(End, Start, Packing, ID, StoreID) ->
    arweave_storage_sync_record:add(End, Start, Packing, ID, StoreID).

delete_sync_record(End, Start, ID, StoreID) ->
    arweave_storage_sync_record:delete(End, Start, ID, StoreID).

%% @doc Remove recorded coverage above the given offset.
cut_sync_record(Offset, ID, StoreID) ->
    arweave_storage_sync_record:cut(Offset, ID, StoreID).

%% @doc Check an index offset, selecting any_packing and/or any_store if needed.
is_recorded(Offset, Packing, ID, StoreID) ->
    arweave_storage_sync_record:is_recorded(Offset, Packing, ID, StoreID).

is_recorded_any(Offset, ID, StorageModules) ->
    arweave_storage_sync_record:is_recorded_any(Offset, ID, StorageModules).

%% @doc Return the next synced or unsynced interval for the given selectors.
get_next_interval(Status, Offset, EndOffsetUpperBound, Packing, ID, StoreID) ->
    arweave_storage_sync_record:get_next_interval(
        Status, Offset, EndOffsetUpperBound, Packing, ID, StoreID
    ).

%% @doc Collect intervals in the selected index's native coordinates.
get_intervals(Status, Start, End, Packing, Record, StoreID) ->
    arweave_storage_sync_record:get_intervals(
        Status, Start, End, Packing, Record, StoreID
    ).

get_interval(Offset, Packing, Record, StoreID) ->
    arweave_storage_sync_record:get_interval(Offset, Packing, Record, StoreID).

get_intersection_size(End, Start, Packing, Record, StoreID) ->
    arweave_storage_sync_record:get_intersection_size(
        End, Start, Packing, Record, StoreID
    ).

%%====================================================================
%% Chunk I/O and maintenance
%%====================================================================

is_storage_supported(Offset, ChunkSize, Packing) ->
    arweave_storage_chunk_storage:is_storage_supported(
        Offset, ChunkSize, Packing
    ).

%% @doc Write a chunk, optionally paired with entropy for missing-entropy repair.
%% {chunk_with_entropy, Chunk, Entropy} is accepted for unpacked_padded only;
%% storage rechecks the bucket under its write lock before using the entropy.
put_chunk(PaddedOffset, Chunk, Packing, StoreID) ->
    arweave_storage_chunk_storage:put(PaddedOffset, Chunk, Packing, StoreID).

open_chunk_files(StoreID) ->
    arweave_storage_chunk_storage:open_files(StoreID).

get_chunk(Byte, StoreID) ->
    arweave_storage_chunk_storage:get(Byte, StoreID).

locate_chunk_on_disk(PaddedEndOffset, StoreID) ->
    arweave_storage_chunk_storage:locate_chunk_on_disk(
        PaddedEndOffset, StoreID
    ).

get_chunk_range(Start, Size, StoreID) ->
    arweave_storage_chunk_storage:get_range(Start, Size, StoreID).

delete_chunk(PaddedOffset, StoreID) ->
    arweave_storage_chunk_storage:delete(PaddedOffset, StoreID).

set_entropy_complete(StoreID) ->
    arweave_storage_chunk_storage:set_entropy_complete(StoreID).

chunk_filepath(Name, StoreID) ->
    arweave_storage_chunk_storage:get_filepath(Name, StoreID).

close_chunk_files(StoreID) ->
    arweave_storage_chunk_storage:close_files(StoreID).

list_chunk_files(StoreID) ->
    arweave_storage_chunk_storage:list_files(StoreID).

run_defragmentation() ->
    arweave_storage_chunk_storage:run_defragmentation().

read_offset(PaddedOffset, StoreID) ->
    arweave_storage_chunk_storage:read_offset(PaddedOffset, StoreID).

%%====================================================================
%% Entropy storage
%%====================================================================

entropy_context(StoreID, Packing) ->
    arweave_storage_entropy_storage:initialize_context(StoreID, Packing).

read_entropy_cursor(StoreID, ModuleStart) ->
    arweave_storage_entropy_storage:read_cursor(StoreID, ModuleStart).

write_entropy_cursor(Cursor, StoreID) ->
    arweave_storage_entropy_storage:write_cursor(Cursor, StoreID).

%% @doc Wait for preceding entropy writes from this caller to be processed.
await_entropy_writes(StoreID) ->
    arweave_storage_entropy_storage:is_ready(StoreID).

entropy_sync_record_id() ->
    arweave_storage_entropy_storage:sync_record_id().

is_entropy_recorded(PaddedEndOffset, Packing, StoreID) ->
    arweave_storage_entropy_storage:is_entropy_recorded(
        PaddedEndOffset, Packing, StoreID
    ).

get_next_unsynced_entropy_interval(Offset, Packing, StoreID) ->
    arweave_storage_entropy_storage:get_next_unsynced_interval(
        Offset, Packing, StoreID
    ).

add_entropy_record(BucketEndOffset, Packing, StoreID) ->
    arweave_storage_entropy_storage:add_record(
        BucketEndOffset, Packing, StoreID
    ).

delete_entropy_record(PaddedEndOffset, StoreID) ->
    arweave_storage_entropy_storage:delete_record(PaddedEndOffset, StoreID).

delete_entropy_record(EndOffset, StartOffset, StoreID) ->
    arweave_storage_entropy_storage:delete_record(
        EndOffset, StartOffset, StoreID
    ).

store_entropy_footprint(StoreID, Fold) ->
    arweave_storage_entropy_storage:store_entropy_footprint(StoreID, Fold).

store_entropy(ChunkEntropy, BucketEndOffset, StoreID, RewardAddr) ->
    arweave_storage_entropy_storage:store_entropy(
        ChunkEntropy, BucketEndOffset, StoreID, RewardAddr
    ).

%%====================================================================
%% Aggregate availability
%%====================================================================

get_serialized_sync_record(Args) ->
    arweave_storage_global_sync_record:get_serialized_sync_record(Args).

get_serialized_buckets(Index) ->
    arweave_storage_global_sync_record:get_serialized_buckets(Index).

%%====================================================================
%% Footprint records
%%====================================================================

add_footprint(Offset, Packing, StoreID) ->
    arweave_storage_footprint_record:add(Offset, Packing, StoreID).

delete_footprint(Offset, StoreID) ->
    arweave_storage_footprint_record:delete(Offset, StoreID).

%%====================================================================
%% Chunk and footprint geometry
%%====================================================================

get_position_and_relative_chunk_offset(ChunkFileStart, Offset) ->
    arweave_storage_chunk_storage:get_position_and_relative_chunk_offset(
        ChunkFileStart, Offset
    ).

get_chunk_bucket_start(Offset) ->
    arweave_storage_chunk_storage:get_chunk_bucket_start(Offset).

get_chunk_bucket_end(Offset) ->
    arweave_storage_chunk_storage:get_chunk_bucket_end(Offset).

get_chunk_byte_from_bucket_end(BucketEndOffset) ->
    arweave_storage_chunk_storage:get_chunk_byte_from_bucket_end(
        BucketEndOffset
    ).

get_chunk_seek_offset(Offset) ->
    arweave_storage_chunk_storage:get_chunk_seek_offset(Offset).

get_chunk_file_start(EndOffset) ->
    arweave_storage_chunk_storage:get_chunk_file_start(EndOffset).

%% @doc Return {Start, End} in footprint-index coordinates.
get_footprint_range(Partition, Footprint) ->
    arweave_storage_footprint_record:footprint_range(Partition, Footprint).

get_footprint_offset(Offset) ->
    arweave_storage_footprint_record:get_offset(Offset).

get_padded_offset_from_footprint_offset(FootprintOffset) ->
    arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(
        FootprintOffset
    ).

get_footprint(Offset) ->
    arweave_storage_footprint_record:get_footprint(Offset).

get_footprint_location(Offset) ->
    arweave_storage_footprint_record:get_location(Offset).

get_footprint_bucket(Offset) ->
    arweave_storage_footprint_record:get_footprint_bucket(Offset).

footprint_intervals_to_byte_intervals(FootprintIntervals) ->
    arweave_storage_footprint_record:footprint_intervals_to_byte_intervals(
        FootprintIntervals
    ).

footprint_intervals_to_byte_intervals(FootprintIntervals, Start, End) ->
    arweave_storage_footprint_record:footprint_intervals_to_byte_intervals(
        FootprintIntervals, Start, End
    ).

max_footprint_offset(WeaveSize) ->
    arweave_storage_footprint_record:max_offset(WeaveSize).

get_next_sector_start(AbsoluteChunkEndOffset) ->
    arweave_storage_footprint_record:get_next_sector_start(
        AbsoluteChunkEndOffset
    ).

get_sector_bucket_start(AbsoluteChunkEndOffset, SectorShift) ->
    arweave_storage_footprint_record:get_sector_bucket_start(
        AbsoluteChunkEndOffset, SectorShift
    ).

-ifdef(AR_TEST).

%% @doc Overwrite raw chunk bytes without updating availability records.
internal_write_chunk(PaddedEndOffset, Chunk, StoreID) ->
    arweave_storage_chunk_storage:write_chunk(
        PaddedEndOffset, Chunk, #{}, StoreID
    ).

%% @doc Erase raw chunk bytes without updating availability records.
internal_erase_chunk(PaddedEndOffset, StoreID) ->
    arweave_storage_chunk_storage:delete_chunk(PaddedEndOffset, StoreID).

-endif.
