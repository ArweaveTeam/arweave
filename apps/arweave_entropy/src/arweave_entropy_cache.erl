-module(arweave_entropy_cache).

-export([get/1, clean_up_space/2, put/3, put_with_limit/4, total_size/0]).

-ifdef(AR_TEST).
-export([get/2, put/5, clean_up_space/4, get_fetched_key_count/2]).
-endif.

-include_lib("arweave_constants/include/arweave_constants.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return the stored value, if any, for the given Key.
get(Key) ->
    get(Key, ar_entropy_cache).

%% @doc Make sure the cache has enough space (i.e., clean up the oldest records, if any)
%% to store Size worth of elements such that the total size does not exceed MaxSize.
%% In other words, if you want to store new elements with the total size Size,
%% call clean_up_space(Size, MaxSize) then call put/3 to store new elements.
clean_up_space(Size, MaxSize) ->
    Table = ar_entropy_cache,
    OrderedKeyTable = ar_entropy_cache_ordered_keys,
    clean_up_space(Size, MaxSize, Table, OrderedKeyTable).

%% @doc Evict the oldest entries needed to fit Size bytes, then store Value.
put_with_limit(Key, Value, Size, MaxSize) ->
    clean_up_space(Size, MaxSize),
    put(Key, Value, Size).

%% @doc Store the given Value in the cache. Associate it with the given Size and
%% increase the total cache size accordingly.
put(Key, Value, Size) ->
    Table = ar_entropy_cache,
    OrderedKeyTable = ar_entropy_cache_ordered_keys,
    put(Key, Value, Size, Table, OrderedKeyTable).

%% @doc Return the size of the cache.
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
        [{{key, Key}, Value}] ->
            %% Track the number of used keys per entropy to estimate the efficiency
            %% of the cache.
            ets:update_counter(
                Table,
                {fetched_key_count, Key},
                1,
                {{fetched_key_count, Key}, 0}
            ),
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
                    ets:delete(OrderedKeyTable, EarliestKey),
                    %% Eviction is the first point at which this entropy's final
                    %% reuse count is known.
                    arweave_metrics:histogram_observe(
                        replica_2_9_entropy_reuse_count,
                        get_fetched_key_count(Table, Key)
                    ),
                    ets:update_counter(
                        Table,
                        total_size,
                        -ElementSize,
                        {total_size, 0}
                    ),
                    ets:delete(Table, {fetched_key_count, Key}),
                    clean_up_space(Size, MaxSize, Table, OrderedKeyTable)
            end;
        false ->
            arweave_metrics:gauge_set(replica_2_9_entropy_cache, TotalSize + Size),
            ok
    end.

get_fetched_key_count(Table, Key) ->
    ets:lookup_element(Table, {fetched_key_count, Key}, 2, 0).

put(Key, Value, Size, Table, OrderedKeyTable) ->
    Timestamp = erlang:unique_integer([monotonic, positive]),
    ets:insert(Table, {{key, Key}, Value}),
    ets:insert(OrderedKeyTable, {{Timestamp, Key, Size}}),
    _ = ets:update_counter(Table, total_size, Size, {total_size, 0}),
    ok.
