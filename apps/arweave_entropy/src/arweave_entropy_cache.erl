-module(arweave_entropy_cache).
-include_lib("arweave_entropy/include/arweave_entropy_deps.hrl").
-ifdef(AR_TEST).
-export([total_size/1, get/2, clean_up_space/4, get_fetched_key_count/2, put/5]).
-endif.



-export([get/1, clean_up_space/2, put/3, total_size/0]).


-include_lib("arweave/include/ar.hrl").


-include_lib("eunit/include/eunit.hrl").


%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return the stored value, if any, for the given Key.
-spec get(Key :: string()) -> {ok, term()} | not_found.

get(Key) ->
    get(Key, ar_entropy_cache).


%% @doc Make sure the cache has enough space (i.e., clean up the oldest records, if any)
%% to store Size worth of elements such that the total size does not exceed MaxSize.
%% In other words, if you want to store new elements with the total size Size,
%% call clean_up_space(Size, MaxSize) then call put/3 to store new elements.
-spec clean_up_space(
        Size :: non_neg_integer(),
        MaxSize :: non_neg_integer()
       ) -> ok.

clean_up_space(Size, MaxSize) ->
    Table = ar_entropy_cache,
    OrderedKeyTable = ar_entropy_cache_ordered_keys,
    clean_up_space(Size, MaxSize, Table, OrderedKeyTable).


%% @doc Store the given Value in the cache. Associate it with the given Size and
%% increase the total cache size accordingly.
-spec put(
        Key :: string(),
        Value :: term(),
        Size :: non_neg_integer()
       ) -> ok.

put(Key, Value, Size) ->
    Table = ar_entropy_cache,
    OrderedKeyTable = ar_entropy_cache_ordered_keys,
    put(Key, Value, Size, Table, OrderedKeyTable).


%% @doc Return the size of the cache.
-spec total_size() -> non_neg_integer().

total_size() ->
    Table = ar_entropy_cache,
    total_size(Table).


%%%===================================================================
%%% Private functions.
%%%===================================================================

total_size(Table) ->
    ets:lookup_element(Table, total_size, 2, 0).


get(Key, Table) ->
    case ets:lookup(Table, {key, Key}) of
        [] ->
            not_found;
        [{_, Value}] ->
            %% Track the number of used keys per entropy to estimate the efficiency
            %% of the cache.
            ets:update_counter(Table, {fetched_key_count, Key}, 1,
                               {{fetched_key_count, Key}, 0}),
            {ok, Value}
    end.


clean_up_space(Size, MaxSize, Table, OrderedKeyTable) ->
    TotalSize = total_size(Table),
    case TotalSize + Size > MaxSize of
        true ->
            case ets:first(OrderedKeyTable) of
                '$end_of_table' ->
                    ok;
                {_Timestamp, Key, ElementSize} = EarliestKey ->
                    ets:delete(Table, {key, Key}),
                    ets:update_counter(Table, total_size, -ElementSize, {total_size, 0}),
                    ets:delete(OrderedKeyTable, EarliestKey),
                    ets:delete(Table, {fetched_key_count, Key}),
                    clean_up_space(Size, MaxSize, Table, OrderedKeyTable)
            end;
        false ->
            ?DEP(metrics):gauge_set(replica_2_9_entropy_cache, TotalSize + Size),
            ok
    end.


get_fetched_key_count(Table, Key) ->
    ets:lookup_element(Table, {fetched_key_count, Key}, 2, 0).


put(Key, Value, Size, Table, OrderedKeyTable) ->
    ets:insert(Table, {{key, Key}, Value}),
    Timestamp = os:system_time(microsecond),
    ets:insert(OrderedKeyTable, {{Timestamp, Key, Size}}),
    ets:update_counter(Table, total_size, Size, {total_size, 0}).


