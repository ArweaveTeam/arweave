%%% Calls from storage to host-owned persistence, protocol and packing services.
%%% Keeping these calls here makes the remaining extraction boundary explicit.
-module(arweave_storage_deps).
-export([
    db_open/1,
    db_get/2,
    db_put/3,
    send_event/2,
    subscribe/1,
    apply_after/5,
    encode_packing/2,
    get_entropy_index/2,
    get_entropy_partition/1,
    get_footprint_size/0,
    get_footprints_per_partition/0,
    get_partition_offset/1,
    get_entropy_sector_size/0,
    encipher_replica_2_9_chunk/2,
    new_buckets/0,
    new_buckets/1,
    serialize_buckets/2,
    add_bucket_range/3,
    cut_buckets/2,
    delete_bucket_range/3,
    buckets_from_intervals/2,
    console/1,
    console/2
]).

db_open(Options) ->
    ar_kv:open(Options).

db_get(Database, Key) ->
    ar_kv:get(Database, Key).

db_put(Database, Key, Value) ->
    ar_kv:put(Database, Key, Value).

send_event(Topic, Event) ->
    ar_events:send(Topic, Event).

subscribe(Topic) ->
    ar_events:subscribe(Topic).

apply_after(Timeout, Module, Function, Args, Options) ->
    ar_timer:apply_after(Timeout, Module, Function, Args, Options).

encode_packing(Packing, Format) ->
    ar_serialize:encode_packing(Packing, Format).

get_entropy_index(Offset, SubChunkIndex) ->
    ar_replica_2_9:get_entropy_index(Offset, SubChunkIndex).

get_entropy_partition(Offset) ->
    ar_replica_2_9:get_entropy_partition(Offset).

get_footprint_size() ->
    arweave_constants:get_sub_chunks_per_replica_2_9_entropy().

get_footprints_per_partition() ->
    arweave_constants:get_replica_2_9_footprints_per_partition().

get_partition_offset(Offset) ->
    ar_replica_2_9:get_partition_offset(Offset).

get_entropy_sector_size() ->
    arweave_constants:get_replica_2_9_entropy_sector_size().

encipher_replica_2_9_chunk(Chunk, Entropy) ->
    ar_packing_server:encipher_replica_2_9_chunk(Chunk, Entropy).

new_buckets() ->
    ar_sync_buckets:new().

new_buckets(BucketSize) ->
    ar_sync_buckets:new(BucketSize).

serialize_buckets(Buckets, MaxSize) ->
    ar_sync_buckets:serialize(Buckets, MaxSize).

add_bucket_range(End, Start, Buckets) ->
    ar_sync_buckets:add(End, Start, Buckets).

cut_buckets(Offset, Buckets) ->
    ar_sync_buckets:cut(Offset, Buckets).

delete_bucket_range(End, Start, Buckets) ->
    ar_sync_buckets:delete(End, Start, Buckets).

buckets_from_intervals(Intervals, Buckets) ->
    ar_sync_buckets:from_intervals(Intervals, Buckets).

console(Format) ->
    ar:console(Format).

console(Format, Args) ->
    ar:console(Format, Args).
