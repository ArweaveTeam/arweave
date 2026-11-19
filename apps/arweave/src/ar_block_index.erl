-module(ar_block_index).
-test_category([fast]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([init/1, update/2, member/1, get_list/1, get_list_by_hash/1, get_element_by_height/1,
         get_block_bounds/1, get_block_bounds_with_height/1, get_intersection/2, get_intersection/1, get_range/2, get_last/0]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Store the given block index in ETS.
init(BI) ->
    init(lists:reverse(BI), 0).

%% @doc Insert the new block index elements from BI and remove the N orphaned ones.
update([], 0) ->
    ok;
update(BI, 0) ->
    {_WeaveSize, Height, _H, _TXRoot} = ets:last(block_index),
    update2(BI, Height + 1);
update(BI, N) ->
    ets:delete(block_index, ets:last(block_index)),
    update(BI, N - 1).

%% @doc Return true if the given block hash is found in the index.
member(H) ->
    member(H, ets:last(block_index)).

%% @doc Return the list of {H, WeaveSize, TXRoot} triplets up to the given Height (including)
%% sorted from latest to earliest.
get_list(Height) ->
    get_list([], ets:first(block_index), -1, Height).

%% @doc Return the list of {H, WeaveSize, TXRoot} triplets up to the block with the given
%% hash H (including) sorted from latest to earliest.
get_list_by_hash(H) ->
    get_list_by_hash([], ets:first(block_index), -1, H).

%% @doc Return the {H, WeaveSize, TXRoot} triplet for the given Height or not_found.
get_element_by_height(Height) ->
    case get_key_by_height(block_index, Height) of
        not_found ->
            not_found;
        {WeaveSize, Height, H, TXRoot} ->
            {H, WeaveSize, TXRoot}
    end.

%% @doc Return {BlockStartOffset, BlockEndOffset, TXRoot} where Offset >= BlockStartOffset,
%% Offset < BlockEndOffset, or not_found if Offset is beyond the block index range.
get_block_bounds(Offset) ->
    case get_block_bounds_with_height(Offset) of
        {BlockStart, BlockEnd, TXRoot, _} ->
            {BlockStart, BlockEnd, TXRoot};
        not_found ->
            not_found
    end.

%% @doc Return {BlockStartOffset, BlockEndOffset, TXRoot, Height} where Offset >= BlockStartOffset,
%% Offset < BlockEndOffset.
get_block_bounds_with_height(Offset) ->
    case ets:next(block_index, {Offset, n, n, n}) of
        '$end_of_table' ->
            ?LOG_ERROR([{event, get_block_bounds_offset_out_of_range},
                        {offset, Offset}]),
            not_found;
        {WeaveSize, Height, _H, TXRoot} = Key ->
            case Height of
                0 ->
                    {0, WeaveSize, TXRoot, 0};
                _ ->
                    {PrevWeaveSize, _, _, _} = ets:prev(block_index, Key),
                    {PrevWeaveSize, WeaveSize, TXRoot, Height}
            end
    end.

%% @doc Return {Height, {H, WeaveSize, TXRoot}} with the triplet present in both
%% the cached block index and the given BI or no_intersection.
get_intersection(Height, _BI) when Height < 0 ->
    no_intersection;
get_intersection(_Height, []) ->
    no_intersection;
get_intersection(Height, BI) ->
    ReverseBI = lists:reverse(BI),
    [{H, _, _} = Elem | ReverseBI2] = ReverseBI,
    case get_key_by_height(block_index, Height) of
        {_, Height, H, _} = Entry ->
            get_intersection(Height + 1, Elem, ReverseBI2, ets:next(block_index, Entry));
        _ ->
            no_intersection
    end.

%% @doc Return the {H, WeaveSize, TXRoot} triplet present in both
%% the cached block index and the given BI or no_intersection.
get_intersection([]) ->
    no_intersection;
get_intersection(BI) ->
    {H, WeaveSize, _TXRoot} = lists:last(BI),
    get_intersection2({H, WeaveSize}, tl(lists:reverse(BI)),
                      ets:next(block_index, {WeaveSize - 1, n, n, n})).

%% @doc Return the list of {H, WeaveSize, TXRoot} for blocks with Height >= Start, =< End,
%% sorted from the largest height to the smallest.
get_range(Start, End) when Start > End ->
    [];
get_range(Start, End) ->
    case get_key_by_height(block_index, Start) of
        {WeaveSize, _Height, H, TXRoot} = Entry ->
            lists:reverse([{H, WeaveSize, TXRoot}
                          | get_range2(Start + 1, End, ets:next(block_index, Entry))]);
        _ ->
            {error, invalid_start}
    end.

%% @doc Return the last element in the block index.
get_last() ->
    ets:last(block_index).

%%%===================================================================
%%% Private functions.
%%%===================================================================

init([], _Height) ->
    ok;
init([{H, WeaveSize, TXRoot} | BI], Height) ->
    ets:insert(block_index, {{WeaveSize, Height, H, TXRoot}}),
    init(BI, Height + 1).

update2([], _Height) ->
    ok;
update2([{H, WeaveSize, TXRoot} | BI], Height) ->
    ets:insert(block_index, {{WeaveSize, Height, H, TXRoot}}),
    update2(BI, Height + 1).

%% @doc Return the key of the block at the given Height in the given block
%% index table or not_found.
get_key_by_height(Table, Height) when is_integer(Height), Height >= 0 ->
    case ets:last(Table) of
        {WeaveSize, LastHeight, _H, _TXRoot} when Height =< LastHeight ->
            %% Every block ends above -1 and not above WeaveSize.
            case get_key_by_height(Table, Height, -1, WeaveSize) of
                {_, Height, _, _} = Key ->
                    Key;
                _ ->
                    %% The blocks were replaced while we searched.
                    not_found
            end;
        _ ->
            not_found
    end;
get_key_by_height(_Table, _Height) ->
    not_found.

%% @doc Search for the block at the given Height by its end offset, which
%% the keys are sorted by and which does not decrease with the height.
%% The block ends above Low and not above High.
get_key_by_height(Table, Height, Low, High) when High - Low =< 1 ->
    %% Blocks without data share the end offset, so name the height too.
    ets:next(Table, {High, Height, n, n});
get_key_by_height(Table, Height, Low, High) ->
    Middle = (Low + High) div 2,
    %% The first block that ends above Middle.
    case ets:next(Table, {Middle, n, n, n}) of
        {_, Height, _, _} = Key ->
            Key;
        {WeaveSize, Height2, _, _} when Height2 < Height ->
            get_key_by_height(Table, Height, WeaveSize - 1, High);
        _ ->
            get_key_by_height(Table, Height, Low, Middle)
    end.

member(H, {_, _, H, _}) ->
    true;
member(_H, '$end_of_table') ->
    false;
member(H, Key) ->
    member(H, ets:prev(block_index, Key)).

get_list(BI, '$end_of_table', _Height, _MaxHeight) ->
    BI;
get_list(BI, _Elem, Height, MaxHeight) when Height >= MaxHeight ->
    BI;
get_list(BI, {WeaveSize, NextHeight, H, TXRoot} = Key, Height, MaxHeight)
  when NextHeight == Height + 1 ->
    get_list([{H, WeaveSize, TXRoot} | BI], ets:next(block_index, Key), Height + 1, MaxHeight);
get_list(_BI, _Key, _Height, MaxHeight) ->
    %% An extremely unlikely race condition should have occured where some blocks were
    %% orphaned right after we passed some of them here, and new blocks have been added
    %% right before we reached the end of the table.
    get_list(MaxHeight).

get_list_by_hash(BI, '$end_of_table', _Height, _H) ->
    BI;
get_list_by_hash(BI, {WeaveSize, NextHeight, H, TXRoot}, Height, H)
  when NextHeight == Height + 1 ->
    [{H, WeaveSize, TXRoot} | BI];
get_list_by_hash(BI, {WeaveSize, NextHeight, H, TXRoot} = Key, Height, H2)
  when NextHeight == Height + 1 ->
    get_list_by_hash([{H, WeaveSize, TXRoot} | BI], ets:next(block_index, Key), Height + 1,
                     H2);
get_list_by_hash(_BI, _Key, _Height, H) ->
    %% An extremely unlikely race condition should have occured where some blocks were
    %% orphaned right after we passed some of them here, and new blocks have been added
    %% right before we reached the end of the table.
    get_list_by_hash(H).

get_intersection(Height, Entry, _ReverseBI, '$end_of_table') ->
    {Height - 1, Entry};
get_intersection(Height, Entry, [], _Entry) ->
    {Height - 1, Entry};
get_intersection(Height, _Entry, [{H, _, _} = Elem | ReverseBI], {_, Height, H, _} = Entry) ->
    get_intersection(Height + 1, Elem, ReverseBI, ets:next(block_index, Entry));
get_intersection(Height, Entry, _ReverseBI, _TableEntry) ->
    {Height - 1, Entry}.

get_intersection2(_, _, '$end_of_table') ->
    no_intersection;
get_intersection2({_, WeaveSize}, _, {WeaveSize2, _, _, _}) when WeaveSize2 > WeaveSize ->
    no_intersection;
get_intersection2({H, WeaveSize}, BI, {WeaveSize, _, H, TXRoot} = Elem) ->
    get_intersection3(ets:next(block_index, Elem), BI, {H, WeaveSize, TXRoot});
get_intersection2({H, WeaveSize}, BI, {WeaveSize, _, _, _} = Elem) ->
    get_intersection2({H, WeaveSize}, BI, ets:next(block_index, Elem)).

get_intersection3({WeaveSize, _, H, TXRoot} = Key, [{H, WeaveSize, TXRoot} | BI], _Elem) ->
    get_intersection3(ets:next(block_index, Key), BI, {H, WeaveSize, TXRoot});
get_intersection3(_, _, {H, WeaveSize, TXRoot}) ->
    {H, WeaveSize, TXRoot}.

get_range2(Start, End, _Elem) when Start > End ->
    [];
get_range2(_Start, _End, '$end_of_table') ->
    [];
get_range2(Start, End, {WeaveSize, _Height, H, TXRoot} = Elem) ->
    [{H, WeaveSize, TXRoot} | get_range2(Start + 1, End, ets:next(block_index, Elem))].

%%%===================================================================
%%% Tests.
%%%===================================================================

get_key_by_height_test() ->
    %% Every third block has no data, and so do the first two, so that
    %% blocks share an end offset at the start, in the middle and at the end.
    %% The 200 blocks with data add up to 30,000,000 bytes, so the search
    %% halves its range up to 25 times.
    Sizes = [0, 0 | [case I rem 3 of 0 -> 0; _ -> I * 1000 end
            || I <- lists:seq(1, 300)]],
    Table = new_test_index(Sizes),
    Count = length(Sizes),
    Keys = [ets:slot(Table, Height) || Height <- lists:seq(0, Count - 1)],
    ?assertEqual(Keys, [[{get_key_by_height(Table, Height)}]
            || Height <- lists:seq(0, Count - 1)]),
    ?assertEqual(not_found, get_key_by_height(Table, Count)),
    ?assertEqual(not_found, get_key_by_height(Table, -1)),
    ?assertEqual(not_found, get_key_by_height(Table, <<"1">>)),
    ets:delete(Table).

get_key_by_height_one_block_test() ->
    Table = new_test_index([0]),
    ?assertEqual({0, 0, <<0:256>>, <<>>}, get_key_by_height(Table, 0)),
    ?assertEqual(not_found, get_key_by_height(Table, 1)),
    ets:delete(Table).

get_key_by_height_empty_index_test() ->
    Table = new_test_index([]),
    ?assertEqual(not_found, get_key_by_height(Table, 0)),
    ets:delete(Table).

%% @doc Return a block index table with a block per given block size.
new_test_index(Sizes) ->
    Table = ets:new(test_block_index, [ordered_set]),
    lists:foldl(
        fun(Size, {Height, WeaveSize}) ->
            WeaveSize2 = WeaveSize + Size,
            ets:insert(Table, {{WeaveSize2, Height, <<Height:256>>, <<>>}}),
            {Height + 1, WeaveSize2}
        end,
        {0, 0},
        Sizes
    ),
    Table.
