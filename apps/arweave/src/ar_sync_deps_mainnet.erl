%%% @doc The production implementation of ar_sync_deps: one-line
%%% delegations to the modules the sync pipeline depends on.
-module(ar_sync_deps_mainnet).
-behaviour(ar_sync_deps).

-export([get_peers_for_offset/1, pick_peers/2, is_throttled/2,
        get_peer_ranges_for_peers/5,
        rate_fetched_data/5, get_chunk_binary/3,
        is_chunk_cache_full/0, chunk_cache_size/0, chunk_cache_size/1,
        chunk_cache_size_limit/0,
        increment_chunk_cache_size/1,
        is_disk_space_sufficient/1, is_footprint_record_initialized/1,
        store_fetched_chunk/5, unsynced_intervals/3,
        unsynced_footprint_intervals/3,
        get_next_not_blacklisted_byte/1, get_next_synced_interval/4,
        get_peer_release/1, get_peers/1, is_joined/0, get_weave_size/0,
        get_sync_buckets/2, fetch_chunk_intervals/2]).

get_peers_for_offset(Offset) ->
    ar_sync_discovery:get_peers_for_offset(Offset).

pick_peers(Peers, Count) ->
    ar_peers:pick_peers(Peers, Count).

is_throttled(Peer, Path) ->
    arweave_throttling:is_throttled(Peer, Path).

get_peer_ranges_for_peers(StoreID, Peers, Offset, RangeStart, RangeEnd) ->
    ar_sync_discovery:get_peer_ranges_for_peers(
        StoreID, Peers, Offset, RangeStart, RangeEnd).

rate_fetched_data(Peer, DataType, Result, ElapsedUs, Bytes) ->
    ar_peers:rate_fetched_data(Peer, DataType, Result, ElapsedUs, Bytes).

get_chunk_binary(Peer, Offset, Packing) ->
    ar_http_iface_client:get_chunk_binary(Peer, Offset, Packing).

is_chunk_cache_full() ->
    ar_data_sync:is_chunk_cache_full().

chunk_cache_size() ->
    ar_data_sync:chunk_cache_size().

chunk_cache_size(StoreID) ->
    ar_data_sync:chunk_cache_size(StoreID).

chunk_cache_size_limit() ->
    ar_data_sync:chunk_cache_size_limit().

increment_chunk_cache_size(StoreID) ->
    ar_data_sync:increment_chunk_cache_size(StoreID).

is_disk_space_sufficient(StoreID) ->
    ar_data_sync:is_disk_space_sufficient(StoreID).

is_footprint_record_initialized(StoreID) ->
    ar_data_sync:is_footprint_record_initialized(StoreID).

store_fetched_chunk(StoreID, Peer, Byte, Proof, TaskRef) ->
    ar_data_sync:store_fetched_chunk(StoreID, Peer, Byte, Proof, TaskRef).

unsynced_intervals(Start, End, StoreID) ->
    UnsyncedIntervals =
        ar_sync_record:collect_unsynced_intervals(Start, End, ar_data_sync, StoreID),
    BlacklistedIntervals = ar_tx_blacklist:get_blacklisted_intervals(Start, End),
    ar_intervals:outerjoin(BlacklistedIntervals, UnsyncedIntervals).

unsynced_footprint_intervals(Partition, Footprint, StoreID) ->
    ar_footprint_record:get_unsynced_intervals(Partition, Footprint, StoreID).

get_next_not_blacklisted_byte(Byte) ->
    ar_tx_blacklist:get_next_not_blacklisted_byte(Byte).

get_next_synced_interval(Byte, End, ID, StoreID) ->
    ar_sync_record:get_next_synced_interval(Byte, End, ID, StoreID).

get_peer_release(Peer) ->
    ar_peers:get_peer_release(Peer).

get_peers(Type) ->
    ar_peers:get_peers(Type).

is_joined() ->
    ar_node:is_joined().

get_weave_size() ->
    ar_node:get_weave_size().

get_sync_buckets(Peer, byte) ->
    ar_http_iface_client:get_sync_buckets(Peer);
get_sync_buckets(Peer, footprint) ->
    ar_http_iface_client:get_footprint_buckets(Peer).

fetch_chunk_intervals(Peer, {byte, Start, none, Limit}) ->
    ar_http_iface_client:get_sync_record(Peer, Start, Limit);
fetch_chunk_intervals(Peer, {byte, Start, Right, Limit}) ->
    ar_http_iface_client:get_sync_record(Peer, Start, Right, Limit);
fetch_chunk_intervals(Peer, {footprint, Partition, Footprint}) ->
    ar_http_iface_client:get_footprints(Peer, Partition, Footprint).
