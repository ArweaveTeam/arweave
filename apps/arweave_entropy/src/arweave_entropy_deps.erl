%%% Adapter to host scheduling, protocol helpers and the shared RandomX pool.
-module(arweave_entropy_deps).
-export([
    generate_entropy/2,
    request_generation/3,
    get_entropy_key/3,
    get_entropy_partition/1,
    get_entropy_partition_range/1,
    get_slice_index/1,
    acquire_device_lock/3,
    release_device_lock/2,
    set_device_lock_metric/3,
    footprint_limit/1,
    is_beyond_footprint_limit/2,
    encode_packing/2,
    console/2
]).
-include_lib("arweave_constants/include/arweave_constants.hrl").

%% @doc Generate on the caller's existing packing worker using its shared dataset.
generate_entropy(_RewardAddr, Key) ->
    {_, _, RandomXState} = ar_packing_server:get_packing_state(),
    Entropy = ar_mine_randomx:randomx_generate_replica_2_9_entropy(
        RandomXState, Key
    ),
    binary_part(Entropy, 0, ?REPLICA_2_9_ENTROPY_SIZE).

request_generation(Ref, ReplyTo, Args) ->
    ar_packing_server:request_entropy_generation(Ref, ReplyTo, Args).

get_entropy_key(RewardAddr, Offset, SubChunkStart) ->
    ar_replica_2_9:get_entropy_key(RewardAddr, Offset, SubChunkStart).

get_entropy_partition(Offset) ->
    ar_replica_2_9:get_entropy_partition(Offset).

get_entropy_partition_range(Partition) ->
    ar_replica_2_9:get_entropy_partition_range(Partition).

get_slice_index(Offset) ->
    ar_replica_2_9:get_slice_index(Offset).

acquire_device_lock(Type, StoreID, Status) ->
    ar_device_lock:acquire_lock(Type, StoreID, Status).

release_device_lock(Type, StoreID) ->
    ar_device_lock:release_lock(Type, StoreID).

set_device_lock_metric(StoreID, Type, Status) ->
    ar_device_lock:set_device_lock_metric(StoreID, Type, Status).

footprint_limit(StoreID) ->
    ar_footprint_limit:get(StoreID).

is_beyond_footprint_limit(Offset, Limit) ->
    ar_footprint_limit:is_beyond(Offset, Limit).

encode_packing(Packing, Format) ->
    ar_serialize:encode_packing(Packing, Format).

console(Format, Args) ->
    ar:console(Format, Args).
