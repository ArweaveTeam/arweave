-module(elli_util).
-include("elli.hrl").
-include("elli_util.hrl").

-include_lib("kernel/include/file.hrl").

-export([normalize_range/2
        , encode_range/2
        , file_size/1
        ]).

-export_type([range/0]).

-type range() :: {Offset::non_neg_integer(), Length::non_neg_integer()}.

-spec normalize_range(RangeOrSet, Size) -> Normalized when
      RangeOrSet :: any(),
      Size       :: integer(),
      Normalized :: range() | undefined | invalid_range.
%% @doc: If a valid byte-range, or byte-range-set of size 1
%% is supplied, returns a normalized range in the format
%% {Offset, Length}. Returns undefined when an empty byte-range-set
%% is supplied and the atom `invalid_range' in all other cases.
normalize_range({suffix, Length}, Size)
  when is_integer(Length), Length > 0 ->
    Length0 = erlang:min(Length, Size),
    {Size - Length0, Length0};
normalize_range({offset, Offset}, Size)
  when is_integer(Offset), Offset >= 0, Offset < Size ->
    {Offset, Size - Offset};
normalize_range({bytes, First, Last}, Size)
  when is_integer(First), is_integer(Last), First =< Last ->
    normalize_range({First, Last - First + 1}, Size);
normalize_range({Offset, Length}, Size)
  when is_integer(Offset), is_integer(Length),
       Offset >= 0, Length >= 0, Offset < Size ->
    Length0 = erlang:min(Length, Size - Offset),
    {Offset, Length0};
normalize_range([ByteRange], Size) ->
    normalize_range(ByteRange, Size);
normalize_range([], _Size) -> undefined;
normalize_range(_, _Size)  -> invalid_range.


-spec encode_range(Range::range() | invalid_range,
                   Size::non_neg_integer()) -> ByteRange::iolist().
%% @doc: Encode Range to a Content-Range value.
encode_range(Range, Size) ->
    [<<"bytes ">>, encode_range_bytes(Range),
     <<"/">>, ?I2L(Size)].

encode_range_bytes({Offset, Length}) ->
    [?I2L(Offset), <<"-">>, ?I2L(Offset + Length - 1)];
encode_range_bytes(invalid_range) -> <<"*">>.


-spec file_size(Filename) -> Size | {error, Reason} when
      Filename :: file:name_all(),
      Size     :: non_neg_integer(),
      Reason   :: file:posix() | badarg | invalid_file.
%% @doc: Get the size in bytes of the file.
file_size(Filename) ->
    case file:read_file_info(Filename) of
        {ok, #file_info{type = regular, access = Perm, size = Size}}
          when Perm =:= read orelse Perm =:= read_write ->
            Size;
        {error, Reason} -> {error, Reason};
        _               -> {error, invalid_file}
    end.
