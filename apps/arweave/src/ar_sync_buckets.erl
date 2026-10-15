-module(ar_sync_buckets).
-test_category([fast]).

-export([new/0, new/1, from_intervals/1, from_intervals/2,
        add/3, delete/3, cut/2, get/3, serialize/2, serialize/3,
        deserialize/2, foreach/4,
        get_default_sync_bucket_size/0, get_network_data_bucket_size/0,
        get_network_footprint_bucket_size/0]).

-include_lib("arweave/include/ar_sync_buckets.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Wrappers around the bucket-size macros so tests, where the macros are
%% redefined smaller, can read the mainnet values.
get_default_sync_bucket_size() -> ?DEFAULT_SYNC_BUCKET_SIZE.
get_network_data_bucket_size() -> ?NETWORK_DATA_BUCKET_SIZE.
get_network_footprint_bucket_size() -> ?NETWORK_FOOTPRINT_BUCKET_SIZE.

%% @doc Return an empty set of buckets.
new() ->
    {?DEFAULT_SYNC_BUCKET_SIZE, #{}}.

new(Size) ->
    {Size, #{}}.

%% @doc Initialize buckets from a set of intervals (see arweave_lib_intervals).
%% The bucket size is ?DEFAULT_SYNC_BUCKET_SIZE.
from_intervals(Intervals) ->
    from_intervals(Intervals, new()).

%% @doc Add the data from a set of intervals (see arweave_lib_intervals) to the given buckets.
from_intervals(Intervals, SyncBuckets) ->
    {Size, Map} = SyncBuckets,
    {Size, arweave_lib_intervals:fold(
        fun({End, Start}, Acc) ->
            add(Start, End, Size, Acc)
        end,
        Map,
        Intervals
    )}.

%% @doc Add the interval with the end offset End and start offset Start to the buckets.
add(End, Start, Buckets) ->
    {Size, Map} = Buckets,
    {Size, add(Start, End, Size, Map)}.

%% @doc Remove the interval with the end offset End and start offset Start from the buckets.
delete(End, Start, Buckets) ->
    {Size, Map} = Buckets,
    {Size, delete(Start, End, Size, Map)}.

%% @doc Remove the intervals strictly above Offset from the buckets.
cut(_Offset, {_Size, Map} = Buckets) when map_size(Map) == 0 ->
    Buckets;
cut(Offset, Buckets) ->
    {Size, Map} = Buckets,
    Last = lists:last(lists:sort(maps:keys(Map))),
    End = (Last + 1) * Size,
    {Size, delete(Offset, End, Size, Map)}.

%% @doc Return the percentage of data synced in the given bucket of size BucketSize.
%% If the recorded bucket size is bigger than the given bucket size, return the share
%% corresponding to the bigger bucket (essentially assuming the uniform distribution of data).
%% If the given bucket crosses the border between the two recorded buckets, return
%% the sum of their shares.
get(Bucket, BucketSize, Buckets) ->
    {Size, Map} = Buckets,
    First = Bucket * BucketSize div Size,
    Last = (Bucket * BucketSize + BucketSize - 1) div Size,
    lists:sum([maps:get(Key, Map, 0) || Key <- lists:seq(First, Last)]).

%% @doc Serialize buckets, treating their current bucket size as the base size.
serialize({Size, _Map} = Buckets, MaxSize) ->
    serialize(Buckets, MaxSize, Size).

%% @doc Serialize buckets into Erlang Term Format, doubling the bucket size
%% until they fit in MaxSize, up to the largest size peers accept for
%% BaseBucketSize. Throw uncompressable_buckets if they still do not fit.
serialize(Buckets, MaxSize, BaseBucketSize) ->
    S = term_to_binary(Buckets),
    {Size, Map} = Buckets,
    case byte_size(S) > MaxSize of
        false ->
            {Buckets, S};
        true when Size * 2 > BaseBucketSize * ?MAX_SYNC_BUCKET_SIZE_RATIO ->
            throw(uncompressable_buckets);
        true ->
            %% A doubling that merges no buckets can grow the term, so keep
            %% doubling instead of stopping at the first step that grows.
            serialize({Size * 2, merge_buckets(Map)}, MaxSize, BaseBucketSize)
    end.

%% @doc Deserialize the buckets from Erlang Term Format.
%% The bucket size must be bigger than or equal to ExpectedBucketSize.
deserialize(SerializedBuckets, ExpectedBucketSize) ->
    case ar_serialize:etf_decode(SerializedBuckets) of
        {ok, {BucketSize, Map}} when is_map(Map), is_integer(BucketSize),
                BucketSize >= ExpectedBucketSize,
                BucketSize =< ExpectedBucketSize * ?MAX_SYNC_BUCKET_SIZE_RATIO ->
            {ok, {BucketSize, maps:filter(
                fun    (Bucket, Share) when
                            is_integer(Bucket), Bucket >= 0,
                            is_number(Share), Share > 0, Share =< 1 ->
                        true;
                    (_, _) ->
                        false
                end,
                Map
            )}};
        {error, Reason} ->
            {error, Reason};
        _ ->
            {error, invalid_format}
    end.

%% @doc Iterate the buckets at BucketSize granularity, calling Fun(Bucket, Share)
%% for each bucket that starts before MaxBucketStartOffset. The stored buckets
%% may use a coarser size (Size); each stored entry is broken down into
%% Size div BucketSize buckets at the requested granularity. The bound exists
%% so a peer reporting a coarse Size cannot make us materialize buckets beyond
%% the current weave — most of which would never be queried.
foreach(Fun, BucketSize, MaxBucketStartOffset, {Size, Map})
        when Size >= BucketSize, MaxBucketStartOffset > 0 ->
    Ratio = Size div BucketSize,
    BucketCount = (MaxBucketStartOffset + BucketSize - 1) div BucketSize,
    maps:fold(
        fun(StoredBucket, Share, ok) ->
            Start = StoredBucket * Ratio,
            case Start >= BucketCount of
                true ->
                    ok;
                false ->
                    End = min(Start + Ratio, BucketCount),
                    foreach_range(Fun, Share, Start, End)
            end
        end,
        ok,
        Map
    );
foreach(_Fun, _BucketSize, _MaxBucketStartOffset, _Buckets) ->
    ok.

foreach_range(_Fun, _Share, SubBucket, End) when SubBucket >= End ->
    ok;
foreach_range(Fun, Share, SubBucket, End) ->
    Fun(SubBucket, Share),
    foreach_range(Fun, Share, SubBucket + 1, End).

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Merge each pair of neighbouring buckets into one of twice the size,
%% which holds half of each share.
merge_buckets(Map) ->
    maps:fold(
        fun(Bucket, Share, Acc) ->
            maps:update_with(
                Bucket div 2, fun(Sh) -> Sh + Share / 2 end, Share / 2, Acc
            )
        end,
        #{},
        Map
    ).

add(Start, End, _Size, Map) when Start >= End ->
    Map;
add(Start, End, Size, Map) ->
    Bucket = Start div Size,
    Share = maps:get(Bucket, Map, 0),
    BucketUpperBound = bucket_upper_bound(Start, Size),
    Increase = min(BucketUpperBound, End) - Start,
    add(BucketUpperBound, End, Size,
            maps:put(Bucket, min(1, (Share * Size + Increase) / Size), Map)).

bucket_upper_bound(Offset, Size) ->
    arweave_lib_util:ceil_int(Offset, Size).

delete(Start, End, _Size, Map) when Start >= End ->
    Map;
delete(Start, End, Size, Map) ->
    Bucket = Start div Size,
    Share = maps:get(Bucket, Map, 0),
    BucketUpperBound = bucket_upper_bound(Start, Size),
    Decrease = min(BucketUpperBound, End) - Start,
    delete(BucketUpperBound, End, Size,
            maps:put(Bucket, max(0, Share * (1 - Decrease / Size)), Map)).

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Tests.
%%%===================================================================

compressed_etf_rejected_test() ->
    Map = maps:from_list([{Bucket, 0.5} || Bucket <- lists:seq(0, 99)]),
    Buckets = {?DEFAULT_SYNC_BUCKET_SIZE, Map},
    Compressed = term_to_binary(Buckets, [compressed]),
    ?assertMatch(<< 131, 80, _/binary >>, Compressed),
    ?assertEqual({ok, Buckets},
            deserialize(term_to_binary(Buckets), ?DEFAULT_SYNC_BUCKET_SIZE)),
    ?assertEqual({error, compressed_etf},
            deserialize(Compressed, ?DEFAULT_SYNC_BUCKET_SIZE)).

%% @doc Sparse buckets that one doubling does not shrink keep doubling until
%% neighbouring buckets merge.
sparse_buckets_serialize_past_a_growing_step_test() ->
    %% Bucket size 2^39 gains a byte of ETF at 2^40. Keys four apart, all at
    %% least 256 after two halvings, only merge at the third doubling.
    Size = 1 bsl 39,
    Keys = lists:seq(1024, 1024 + 4 * 99, 4),
    Buckets = {Size, maps:from_list([{Key, 0.5} || Key <- Keys])},
    MaxSize = byte_size(term_to_binary(Buckets)) - 1,
    {{Size2, Map2}, S} = serialize(Buckets, MaxSize, Size),
    ?assert(byte_size(S) =< MaxSize),
    ?assertEqual(8 * Size, Size2),
    ?assertEqual(lists:usort([Key div 8 || Key <- Keys]),
        lists:sort(maps:keys(Map2))),
    %% Each merged bucket spans eight original buckets, two of them half
    %% covered.
    ?assertEqual([0.125], lists:usort(maps:values(Map2))).

%% @doc Serialization stops at the largest bucket size peers accept.
serialize_stops_at_the_accepted_ratio_test() ->
    Size = ?DEFAULT_SYNC_BUCKET_SIZE,
    Buckets = {Size, #{0 => 1, 1 bsl 40 => 1}},
    ?assertException(throw, uncompressable_buckets,
        serialize(Buckets, 10, Size)),
    %% A bucket size already at the largest accepted ratio cannot double.
    MaxBuckets = {Size * ?MAX_SYNC_BUCKET_SIZE_RATIO, #{0 => 1, 1 => 1}},
    ?assertException(throw, uncompressable_buckets,
        serialize(MaxBuckets, 10, Size)).

%% @doc Merging a bucket without a neighbour halves its share.
unpaired_bucket_share_halves_test() ->
    Size = ?DEFAULT_SYNC_BUCKET_SIZE,
    %% Buckets 0 and 1 merge, which shrinks the map, and bucket 3 has no
    %% neighbour.
    Buckets = {Size, #{0 => 0.5, 1 => 0.5, 3 => 0.5}},
    MaxSize = byte_size(term_to_binary(Buckets)) - 1,
    ?assertEqual({2 * Size, #{0 => 0.5, 1 => 0.25}},
        element(1, serialize(Buckets, MaxSize, Size))).

%% @doc Run the bucket coverage checks with the original suite's timeout.
buckets_test_() ->
    {timeout, 60, fun buckets/0}.

%% @doc Bucket updates and size-bounded serialization preserve coverage through
%% round trips.
buckets() ->
    Size = 10000000000,
    B1 = {10000000000, #{}},
    ?assertException(throw, uncompressable_buckets, serialize(B1, 10)),
    {B1, S1} = serialize(B1, 20),
    {ok, B1} = deserialize(S1, ?DEFAULT_SYNC_BUCKET_SIZE),
    B2 = add(5, 0, B1),
    ?assertEqual(5 / Size, get(0, 10, B2)),
    B3 = add(Size * 2, Size, B2),
    ?assertEqual({Size, #{0 => 5 / Size, 1 => 1}}, B3),
    {B3, S3} = serialize(B3, 40),
    {ok, B3} = deserialize(S3, ?DEFAULT_SYNC_BUCKET_SIZE),
    %% The size of the serialized buckets is 31 bytes.
    DoubleSize = 2 * Size,
    ?assertEqual(
        {DoubleSize, #{0 => 0.5 + 5 / Size / 2}}, element(1, serialize(B3, 30))
    ),
    {_, S3_1} = serialize(B3, 30),
    ?assertEqual(
        {ok, {DoubleSize, #{0 => 0.5 + 5 / Size / 2}}},
        deserialize(S3_1, ?DEFAULT_SYNC_BUCKET_SIZE)
    ),
    ?assertEqual(
        {Size, #{0 => 5 / Size, 1 => 0.5}}, cut(Size + Size div 2, B3)
    ),
    ?assertEqual(
        {Size, #{0 => (1 - (Size - 4) / Size) * (5 / Size), 1 => 0}},
        delete(Size * 2, 4, B3)
    ),
    B4 = from_intervals(
        gb_sets:from_list([{5, 0}, {2 * Size, Size}]), {10000000000, #{}}
    ),
    ?assertEqual(B3, B4),
    B5 = from_intervals(gb_sets:from_list([{2 * Size, Size}]), B2),
    ?assertEqual(B4, B5).

-endif.
