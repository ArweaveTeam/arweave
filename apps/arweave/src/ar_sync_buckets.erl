-module(ar_sync_buckets).

-export([new/0, from_intervals/1, from_intervals/2, add/3, delete/3, cut/2, get/3, serialize/2,
		deserialize/1, foreach/3]).

-include_lib("arweave/include/ar_sync_buckets.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return an empty set of buckets.
new() ->
	{?DEFAULT_SYNC_BUCKET_SIZE, #{}}.

%% @doc Initialize buckets from a set of intervals (see ar_intervals).
%% The bucket size is ?DEFAULT_SYNC_BUCKET_SIZE.
from_intervals(Intervals) ->
	from_intervals(Intervals, new()).

%% @doc Add the data from a set of intervals (see ar_intervals) to the given buckets.
from_intervals(Intervals, SyncBuckets) ->
	{Size, Map} = SyncBuckets,
	{Size, ar_intervals:fold(
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

%% @doc Serialize buckets into Erlang Term Format. If the size of the serialized structure
%% exceeds MaxSize, double the bucket size and restructure the buckets.
%% Throw uncompressable_buckets if MaxSize is too low.
serialize(Buckets, MaxSize) ->
	serialize(Buckets, MaxSize, infinity).

serialize(Buckets, MaxSize, PrevSerializedSize) ->
	S = term_to_binary(Buckets),
	SerializedSize = byte_size(S),
	case SerializedSize > MaxSize of
		true ->
			case SerializedSize > PrevSerializedSize of
				true ->
					throw(uncompressable_buckets);
				false ->
					ok
			end,
			{Size, Map} = Buckets,
			%% Double the bucket size until the serialized buckets are smaller than MaxSize.
			serialize({Size * 2, maps:fold(
				fun(Bucket, Share, Acc) ->
					maps:update_with(Bucket div 2, fun(Sh) -> (Sh + Share) / 2 end, Share, Acc)
				end,
				maps:new(),
				Map
			)}, MaxSize, SerializedSize);
		false ->
			{Buckets, S}
	end.

%% @doc Deserialize the buckets from Erlang Term Format.
%% The bucket size must be bigger than or equal to ?DEFAULT_SYNC_BUCKET_SIZE.
deserialize(SerializedBuckets) ->
	case catch binary_to_term(SerializedBuckets, [safe]) of
		{BucketSize, Map} when is_map(Map), is_integer(BucketSize),
				BucketSize >= (?DEFAULT_SYNC_BUCKET_SIZE) ->
			{ok, {BucketSize, maps:filter(
				fun	(Bucket, Share) when
							is_integer(Bucket), Bucket >= 0,
							is_number(Share), Share > 0, Share =< 1 ->
						true;
					(_, _) ->
						false
				end,
				Map
			)}};
		{'EXIT', Reason} ->
			{error, Reason}
	end.

%% @doc Apply the given function of two arguments (Bucket, Share) to each
%% of the given buckets breaking them down according to the given size.
foreach(Fun, BucketSize, {Size, Map}) when Size >= BucketSize, Size rem BucketSize == 0 ->
	maps:fold(
		fun(Bucket, Share, ok) ->
			lists:foreach(
				fun(SubBucket) -> Fun(SubBucket, Share) end,
				lists:seq(Bucket * Size div BucketSize, (Bucket + 1) * Size div BucketSize - 1)
			)
		end,
		ok,
		Map
	);
foreach(_Fun, _BucketSize, _Buckets) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

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
	Offset - Offset rem Size + Size.

delete(Start, End, _Size, Map) when Start >= End ->
	Map;
delete(Start, End, Size, Map) ->
	Bucket = Start div Size,
	Share = maps:get(Bucket, Map, 0),
	BucketUpperBound = bucket_upper_bound(Start, Size),
	Decrease = min(BucketUpperBound, End) - Start,
	delete(BucketUpperBound, End, Size,
			maps:put(Bucket, max(0, Share * (1 - Decrease / Size)), Map)).

%%%===================================================================
%%% Tests.
%%%===================================================================

buckets_test() ->
	Size = 10000000000,
	B1 = {10000000000, #{}},
	?assertException(throw, uncompressable_buckets, serialize(B1, 10)),
	{B1, S1} = serialize(B1, 20),
	{ok, B1} = deserialize(S1),
	B2 = add(5, 0, B1),
	?assertEqual(5 / Size, get(0, 10, B2)),
	B3 = add(Size * 2, Size, B2),
	?assertEqual({Size, #{ 0 => 5 / Size, 1 => 1 }}, B3),
	{B3, S3} = serialize(B3, 40),
	{ok, B3} = deserialize(S3),
	%% The size of the serialized buckets is 31 bytes.
	DoubleSize = 2 * Size,
	?assertEqual({DoubleSize, #{ 0 => 0.5 + 5 / Size / 2 }}, element(1, serialize(B3, 30))),
	{_, S3_1} = serialize(B3, 30),
	?assertEqual({ok, {DoubleSize, #{ 0 => 0.5 + 5 / Size / 2 }}}, deserialize(S3_1)),
	?assertEqual({Size, #{ 0 => 5 / Size, 1 => 0.5 }}, cut(Size + Size div 2, B3)),
	?assertEqual({Size, #{ 0 => (1 - (Size - 4) / Size) * (5 / Size), 1 => 0 }},
			delete(Size * 2, 4, B3)),
	B4 = from_intervals(gb_sets:from_list([{5, 0}, {2 * Size, Size}]), {10000000000, #{}}),
	?assertEqual(B3, B4),
	B5 = from_intervals(gb_sets:from_list([{2 * Size, Size}]), B2),
	?assertEqual(B4, B5).
